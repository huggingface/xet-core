use std::future::Future;
use std::sync::{Arc, Mutex, Weak};

use tokio_util::sync::CancellationToken;
use xet_runtime::core::XetRuntime;

use crate::error::XetError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum TaskRuntimeState {
    Running,
    Finalizing,
    Completed,
    Error(String),
    UserCancelled,
}

pub(super) struct TaskRuntime {
    runtime: Arc<XetRuntime>,
    cancellation_token: CancellationToken,
    state: Mutex<TaskRuntimeState>,
    children: Mutex<Vec<Weak<TaskRuntime>>>,
}

impl TaskRuntime {
    pub(super) fn new_root(runtime: Arc<XetRuntime>) -> Arc<Self> {
        Arc::new(Self {
            runtime,
            cancellation_token: CancellationToken::new(),
            state: Mutex::new(TaskRuntimeState::Running),
            children: Mutex::new(Vec::new()),
        })
    }

    pub(super) fn child(self: &Arc<Self>) -> Result<Arc<Self>, XetError> {
        let child = Arc::new(Self {
            runtime: self.runtime.clone(),
            cancellation_token: self.cancellation_token.child_token(),
            state: Mutex::new(self.state()?),
            children: Mutex::new(Vec::new()),
        });

        self.children.lock()?.push(Arc::downgrade(&child));
        Ok(child)
    }

    pub(super) fn state(&self) -> Result<TaskRuntimeState, XetError> {
        Ok(self.state.lock()?.clone())
    }

    fn set_state(&self, new_state: TaskRuntimeState) -> Result<(), XetError> {
        *self.state.lock()? = new_state;
        Ok(())
    }

    fn set_state_recursive(&self, new_state: TaskRuntimeState) -> Result<(), XetError> {
        self.set_state(new_state.clone())?;
        for child in self.live_children()? {
            child.set_state_recursive(new_state.clone())?;
        }
        Ok(())
    }

    pub(super) fn cancel_subtree(&self) -> Result<(), XetError> {
        self.cancellation_token.cancel();
        self.set_state_recursive(TaskRuntimeState::UserCancelled)
    }

    fn check_state(&self, task_name: &'static str) -> Result<(), XetError> {
        match self.state()? {
            TaskRuntimeState::Running | TaskRuntimeState::Finalizing => Ok(()),
            TaskRuntimeState::UserCancelled => Err(XetError::UserCancelled(format!("{task_name} cancelled by user"))),
            TaskRuntimeState::Completed => Err(XetError::AlreadyCompleted),
            TaskRuntimeState::Error(msg) => Err(XetError::PreviousTaskError(msg)),
        }
    }

    fn update_state_on_error(&self, err: &XetError) -> Result<(), XetError> {
        match err {
            XetError::UserCancelled(_) => self.set_state(TaskRuntimeState::UserCancelled),
            other => self.set_state(TaskRuntimeState::Error(other.to_string())),
        }
    }

    fn run_inner_async<T, F>(
        &self,
        task_name: &'static str,
        fut: F,
    ) -> impl Future<Output = Result<T, XetError>> + Send + 'static
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        let token = self.cancellation_token.clone();
        let runtime = self.runtime.clone();
        async move {
            runtime
                .bridge_async(task_name, async move {
                    tokio::select! {
                        _ = token.cancelled() => Err(XetError::UserCancelled(
                            format!("{task_name} cancelled by user"),
                        )),
                        result = fut => result,
                    }
                })
                .await
                .map_err(XetError::from)?
        }
    }

    pub(super) async fn bridge_async<T, F>(&self, task_name: &'static str, fut: F) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        self.check_state(task_name)?;
        let result = self.run_inner_async(task_name, fut).await;
        if let Err(ref e) = result {
            self.update_state_on_error(e)?;
        }
        result
    }

    pub(super) fn bridge_sync<T, F>(&self, task_name: &'static str, fut: F) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        self.check_state(task_name)?;
        let token = self.cancellation_token.clone();
        let result = self
            .runtime
            .bridge_sync(async move {
                tokio::select! {
                    _ = token.cancelled() => Err(XetError::UserCancelled(
                        format!("{task_name} cancelled by user"),
                    )),
                    result = fut => result,
                }
            })
            .map_err(XetError::from)?;
        if let Err(ref e) = result {
            self.update_state_on_error(e)?;
        }
        result
    }

    pub(super) async fn bridge_async_finalizing<T, F>(&self, task_name: &'static str, fut: F) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        match self.state()? {
            TaskRuntimeState::Running => self.set_state(TaskRuntimeState::Finalizing)?,
            TaskRuntimeState::Finalizing | TaskRuntimeState::Completed => {
                return Err(XetError::AlreadyCompleted);
            },
            TaskRuntimeState::UserCancelled => {
                return Err(XetError::UserCancelled(format!("{task_name} cancelled by user")));
            },
            TaskRuntimeState::Error(msg) => return Err(XetError::PreviousTaskError(msg)),
        }

        let result = self.run_inner_async(task_name, fut).await;
        match &result {
            Ok(_) => self.set_state(TaskRuntimeState::Completed)?,
            Err(XetError::UserCancelled(_)) => {
                self.set_state(TaskRuntimeState::UserCancelled)?;
            },
            Err(e) => self.set_state(TaskRuntimeState::Error(e.to_string()))?,
        }
        result
    }

    pub(super) fn bridge_sync_finalizing<T, F>(&self, task_name: &'static str, fut: F) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        match self.state()? {
            TaskRuntimeState::Running => self.set_state(TaskRuntimeState::Finalizing)?,
            TaskRuntimeState::Finalizing | TaskRuntimeState::Completed => {
                return Err(XetError::AlreadyCompleted);
            },
            TaskRuntimeState::UserCancelled => {
                return Err(XetError::UserCancelled(format!("{task_name} cancelled by user")));
            },
            TaskRuntimeState::Error(msg) => return Err(XetError::PreviousTaskError(msg)),
        }

        let token = self.cancellation_token.clone();
        let result = self
            .runtime
            .bridge_sync(async move {
                tokio::select! {
                    _ = token.cancelled() => Err(XetError::UserCancelled(
                        format!("{task_name} cancelled by user"),
                    )),
                    result = fut => result,
                }
            })
            .map_err(XetError::from)?;
        match &result {
            Ok(_) => self.set_state(TaskRuntimeState::Completed)?,
            Err(XetError::UserCancelled(_)) => {
                self.set_state(TaskRuntimeState::UserCancelled)?;
            },
            Err(e) => self.set_state(TaskRuntimeState::Error(e.to_string()))?,
        }
        result
    }

    fn live_children(&self) -> Result<Vec<Arc<TaskRuntime>>, XetError> {
        let mut guard = self.children.lock()?;
        let mut live = Vec::with_capacity(guard.len());
        guard.retain(|weak| {
            if let Some(child) = weak.upgrade() {
                live.push(child);
                true
            } else {
                false
            }
        });
        Ok(live)
    }
}
