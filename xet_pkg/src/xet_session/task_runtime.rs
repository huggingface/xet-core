use std::future::Future;
use std::sync::{Arc, Mutex, Weak};

use tokio_util::sync::CancellationToken;
use xet_runtime::core::XetRuntime;

use crate::error::XetError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum XetTaskState {
    Running,
    Finalizing,
    Completed,
    Error(String),
    UserCancelled,
}

#[derive(Debug)]
pub(super) enum BackgroundTaskState<T, J> {
    Running { join_handle: Option<J> },
    Success(T),
    Error(String),
}

pub(super) enum BackgroundTaskResolution<T, J> {
    Cached(T),
    JoinHandle(J),
}

pub(super) struct TaskRuntime {
    runtime: Arc<XetRuntime>,
    cancellation_token: CancellationToken,
    state: Mutex<XetTaskState>,
    children: Mutex<Vec<Weak<TaskRuntime>>>,
}

impl TaskRuntime {
    pub(super) fn new_root(runtime: Arc<XetRuntime>) -> Arc<Self> {
        Arc::new(Self {
            runtime,
            cancellation_token: CancellationToken::new(),
            state: Mutex::new(XetTaskState::Running),
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

    pub(super) fn status(&self) -> Result<XetTaskState, XetError> {
        Ok(self.state.lock()?.clone())
    }

    pub(super) fn state(&self) -> Result<XetTaskState, XetError> {
        self.status()
    }

    fn set_state(&self, new_state: XetTaskState) -> Result<(), XetError> {
        *self.state.lock()? = new_state;
        Ok(())
    }

    fn set_state_recursive(&self, new_state: XetTaskState) -> Result<(), XetError> {
        self.set_state(new_state.clone())?;
        for child in self.live_children()? {
            child.set_state_recursive(new_state.clone())?;
        }
        Ok(())
    }

    pub(super) fn cancel_subtree(&self) -> Result<(), XetError> {
        self.cancellation_token.cancel();
        self.set_state_recursive(XetTaskState::UserCancelled)
    }

    fn check_state(&self, task_name: &'static str) -> Result<(), XetError> {
        match self.status()? {
            XetTaskState::Running | XetTaskState::Finalizing => Ok(()),
            XetTaskState::UserCancelled => Err(XetError::UserCancelled(format!("{task_name} cancelled by user"))),
            XetTaskState::Completed => Err(XetError::AlreadyCompleted),
            XetTaskState::Error(msg) => Err(XetError::PreviousTaskError(msg)),
        }
    }

    fn update_state_on_error(&self, err: &XetError) -> Result<(), XetError> {
        match err {
            XetError::UserCancelled(_) => self.set_state(XetTaskState::UserCancelled),
            other => self.set_state(XetTaskState::Error(other.to_string())),
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
        match self.status()? {
            XetTaskState::Running => self.set_state(XetTaskState::Finalizing)?,
            XetTaskState::Finalizing | XetTaskState::Completed => {
                return Err(XetError::AlreadyCompleted);
            },
            XetTaskState::UserCancelled => {
                return Err(XetError::UserCancelled(format!("{task_name} cancelled by user")));
            },
            XetTaskState::Error(msg) => return Err(XetError::PreviousTaskError(msg)),
        }

        let result = self.run_inner_async(task_name, fut).await;
        match &result {
            Ok(_) => self.set_state(XetTaskState::Completed)?,
            Err(XetError::UserCancelled(_)) => {
                self.set_state(XetTaskState::UserCancelled)?;
            },
            Err(e) => self.set_state(XetTaskState::Error(e.to_string()))?,
        }
        result
    }

    pub(super) fn bridge_sync_finalizing<T, F>(&self, task_name: &'static str, fut: F) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        match self.status()? {
            XetTaskState::Running => self.set_state(XetTaskState::Finalizing)?,
            XetTaskState::Finalizing | XetTaskState::Completed => {
                return Err(XetError::AlreadyCompleted);
            },
            XetTaskState::UserCancelled => {
                return Err(XetError::UserCancelled(format!("{task_name} cancelled by user")));
            },
            XetTaskState::Error(msg) => return Err(XetError::PreviousTaskError(msg)),
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
            Ok(_) => self.set_state(XetTaskState::Completed)?,
            Err(XetError::UserCancelled(_)) => {
                self.set_state(XetTaskState::UserCancelled)?;
            },
            Err(e) => self.set_state(XetTaskState::Error(e.to_string()))?,
        }
        result
    }

    pub(super) fn status_from_background_task<T, J>(
        &self,
        state: &tokio::sync::Mutex<BackgroundTaskState<T, J>>,
    ) -> Result<XetTaskState, XetError> {
        let runtime_state = self.status()?;
        if !matches!(runtime_state, XetTaskState::Running | XetTaskState::Finalizing) {
            return Ok(runtime_state);
        }
        let state_guard = match state.try_lock() {
            Ok(guard) => guard,
            Err(_) => return Ok(XetTaskState::Running),
        };
        let status = match &*state_guard {
            BackgroundTaskState::Running { .. } => XetTaskState::Running,
            BackgroundTaskState::Success(_) => XetTaskState::Completed,
            BackgroundTaskState::Error(msg) => XetTaskState::Error(msg.clone()),
        };
        Ok(status)
    }

    pub(super) async fn begin_background_task_resolution<T: Clone, J>(
        &self,
        state: &tokio::sync::Mutex<BackgroundTaskState<T, J>>,
        already_resolved_message: &'static str,
    ) -> Result<BackgroundTaskResolution<T, J>, XetError> {
        let mut guard = state.lock().await;
        match &mut *guard {
            BackgroundTaskState::Success(value) => Ok(BackgroundTaskResolution::Cached(value.clone())),
            BackgroundTaskState::Error(msg) => Err(XetError::TaskError(msg.clone())),
            BackgroundTaskState::Running { join_handle } => {
                let handle = join_handle
                    .take()
                    .ok_or_else(|| XetError::TaskError(already_resolved_message.to_string()))?;
                Ok(BackgroundTaskResolution::JoinHandle(handle))
            },
        }
    }

    pub(super) fn begin_background_task_resolution_blocking<T: Clone, J>(
        &self,
        state: &tokio::sync::Mutex<BackgroundTaskState<T, J>>,
        already_resolved_message: &'static str,
    ) -> Result<BackgroundTaskResolution<T, J>, XetError> {
        let mut guard = state.blocking_lock();
        match &mut *guard {
            BackgroundTaskState::Success(value) => Ok(BackgroundTaskResolution::Cached(value.clone())),
            BackgroundTaskState::Error(msg) => Err(XetError::TaskError(msg.clone())),
            BackgroundTaskState::Running { join_handle } => {
                let handle = join_handle
                    .take()
                    .ok_or_else(|| XetError::TaskError(already_resolved_message.to_string()))?;
                Ok(BackgroundTaskResolution::JoinHandle(handle))
            },
        }
    }

    pub(super) async fn finish_background_task_resolution<T: Clone, J>(
        &self,
        state: &tokio::sync::Mutex<BackgroundTaskState<T, J>>,
        result: &Result<T, XetError>,
    ) {
        let mut guard = state.lock().await;
        match result {
            Ok(value) => {
                *guard = BackgroundTaskState::Success(value.clone());
            },
            Err(err) => {
                *guard = BackgroundTaskState::Error(Self::task_error_message(err));
            },
        }
    }

    pub(super) fn finish_background_task_resolution_blocking<T: Clone, J>(
        &self,
        state: &tokio::sync::Mutex<BackgroundTaskState<T, J>>,
        result: &Result<T, XetError>,
    ) {
        let mut guard = state.blocking_lock();
        match result {
            Ok(value) => {
                *guard = BackgroundTaskState::Success(value.clone());
            },
            Err(err) => {
                *guard = BackgroundTaskState::Error(Self::task_error_message(err));
            },
        }
    }

    pub(super) fn background_success<T: Clone, J>(
        &self,
        state: &tokio::sync::Mutex<BackgroundTaskState<T, J>>,
    ) -> Option<T> {
        let guard = state.try_lock().ok()?;
        match &*guard {
            BackgroundTaskState::Success(value) => Some(value.clone()),
            _ => None,
        }
    }

    pub(super) fn background_result<T: Clone, J>(
        &self,
        state: &tokio::sync::Mutex<BackgroundTaskState<T, J>>,
    ) -> Option<Result<T, XetError>> {
        let guard = state.try_lock().ok()?;
        match &*guard {
            BackgroundTaskState::Success(value) => Some(Ok(value.clone())),
            BackgroundTaskState::Error(msg) => Some(Err(XetError::TaskError(msg.clone()))),
            BackgroundTaskState::Running { .. } => None,
        }
    }

    pub(super) fn cancel_background_task<T, J, F>(
        &self,
        state: &tokio::sync::Mutex<BackgroundTaskState<T, J>>,
        cancel_message: &'static str,
        mut abort_join_handle: F,
    ) where
        F: FnMut(&mut J),
    {
        let _ = self.cancel_subtree();
        if let Ok(mut guard) = state.try_lock()
            && let BackgroundTaskState::Running { join_handle } = &mut *guard
        {
            if let Some(mut handle) = join_handle.take() {
                abort_join_handle(&mut handle);
            }
            *guard = BackgroundTaskState::Error(cancel_message.to_string());
        }
    }

    fn task_error_message(err: &XetError) -> String {
        match err {
            XetError::TaskError(msg)
            | XetError::PreviousTaskError(msg)
            | XetError::UserCancelled(msg)
            | XetError::Cancelled(msg) => msg.clone(),
            _ => err.to_string(),
        }
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
