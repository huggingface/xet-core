use std::future::Future;
use std::sync::{Arc, Mutex, Weak};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
#[cfg(target_family = "wasm")]
use tokio_with_wasm::alias as tokio;
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
pub(super) enum BackgroundTaskState<T> {
    Running {
        join_handle: Option<JoinHandle<Result<T, XetError>>>,
    },
    Success(T),
    Error(String),
}

impl<T: Clone> BackgroundTaskState<T> {
    pub(super) async fn finish(&mut self) -> Result<T, XetError> {
        match self {
            BackgroundTaskState::Success(value) => Ok(value.clone()),
            BackgroundTaskState::Error(msg) => Err(XetError::PreviousTaskError(msg.clone())),
            BackgroundTaskState::Running { join_handle } => {
                let handle = join_handle
                    .take()
                    .ok_or_else(|| XetError::TaskError("task already being resolved".into()))?;
                match handle.await {
                    Ok(Ok(value)) => {
                        *self = BackgroundTaskState::Success(value.clone());
                        Ok(value)
                    },
                    Ok(Err(e)) => {
                        let msg = e.to_string();
                        *self = BackgroundTaskState::Error(msg);
                        Err(e)
                    },
                    Err(join_err) => {
                        if join_err.is_cancelled() {
                            let msg = "background task cancelled by user".to_string();
                            *self = BackgroundTaskState::Error(msg.clone());
                            return Err(XetError::UserCancelled(msg));
                        }
                        let msg = join_err.to_string();
                        *self = BackgroundTaskState::Error(msg.clone());
                        Err(XetError::TaskError(msg))
                    },
                }
            },
        }
    }
}

pub(super) struct TaskRuntime {
    #[cfg_attr(target_family = "wasm", allow(dead_code))]
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
            state: Mutex::new(self.status()?),
            children: Mutex::new(Vec::new()),
        });

        self.children.lock()?.push(Arc::downgrade(&child));
        Ok(child)
    }

    pub(super) fn status(&self) -> Result<XetTaskState, XetError> {
        Ok(self.state.lock()?.clone())
    }

    fn set_state(&self, new_state: XetTaskState) -> Result<(), XetError> {
        *self.state.lock()? = new_state;
        Ok(())
    }

    fn transition_to_finalizing(&self, task_name: &'static str, allow_repeat: bool) -> Result<(), XetError> {
        let mut state = self.state.lock()?;
        match &*state {
            XetTaskState::Running => {
                *state = XetTaskState::Finalizing;
                Ok(())
            },
            XetTaskState::Finalizing | XetTaskState::Completed => {
                if allow_repeat {
                    Ok(())
                } else {
                    Err(XetError::AlreadyCompleted)
                }
            },
            XetTaskState::UserCancelled => Err(XetError::UserCancelled(format!("{task_name} cancelled by user"))),
            XetTaskState::Error(msg) => Err(XetError::PreviousTaskError(msg.clone())),
        }
    }

    fn set_state_recursive(&self, new_state: XetTaskState) -> Result<(), XetError> {
        self.set_state(new_state.clone())?;
        for child in self.live_children()? {
            child.set_state_recursive(new_state.clone())?;
        }
        Ok(())
    }

    pub(super) fn cancel_subtree(&self) -> Result<(), XetError> {
        // TaskRuntime cancellation is token-driven: cancel the subtree token,
        // then mark the local state tree as UserCancelled. Child tasks observe
        // the token in bridge paths and exit cooperatively.
        self.cancellation_token.cancel();
        self.set_state_recursive(XetTaskState::UserCancelled)
    }

    pub(super) fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    pub(super) fn check_state(&self, task_name: &'static str) -> Result<(), XetError> {
        match self.status()? {
            XetTaskState::Running => Ok(()),
            XetTaskState::Finalizing => Err(XetError::AlreadyCompleted),
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

    // ── Background task helpers ──────────────────────────────────────────────

    pub(super) fn status_from_background_task<T>(
        &self,
        state: &tokio::sync::Mutex<BackgroundTaskState<T>>,
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

    pub(super) fn background_success<T: Clone>(&self, state: &tokio::sync::Mutex<BackgroundTaskState<T>>) -> Option<T> {
        let guard = state.try_lock().ok()?;
        match &*guard {
            BackgroundTaskState::Success(value) => Some(value.clone()),
            _ => None,
        }
    }

    // Used only by native-only handle accessors (e.g. `_blocking` variants);
    // intentionally retained on wasm so handles keep the same shape.
    #[cfg_attr(target_family = "wasm", allow(dead_code))]
    pub(super) fn background_result<T: Clone>(
        &self,
        state: &tokio::sync::Mutex<BackgroundTaskState<T>>,
    ) -> Option<Result<T, XetError>> {
        let guard = state.try_lock().ok()?;
        match &*guard {
            BackgroundTaskState::Success(value) => Some(Ok(value.clone())),
            BackgroundTaskState::Error(msg) => Some(Err(XetError::TaskError(msg.clone()))),
            BackgroundTaskState::Running { .. } => None,
        }
    }

    // Cancellation entrypoint for per-handle abort methods.
    // We intentionally rely on subtree token propagation (plus bridge select
    // points) instead of mutating per-handle background state directly.
    pub(super) fn cancel_background_task(&self) {
        let _ = self.cancel_subtree();
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

// Native task bridging: routes futures through XetRuntime's multithreaded
// executor and requires Send + 'static bounds. Sync bridging only exists
// here — wasm has no blocking model.
#[cfg(not(target_family = "wasm"))]
impl TaskRuntime {
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

    pub(super) async fn bridge_async_finalizing<T, F>(
        &self,
        task_name: &'static str,
        allow_repeat: bool,
        fut: F,
    ) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        self.transition_to_finalizing(task_name, allow_repeat)?;

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

    pub(super) fn bridge_sync_finalizing<T, F>(
        &self,
        task_name: &'static str,
        allow_repeat: bool,
        fut: F,
    ) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        self.transition_to_finalizing(task_name, allow_repeat)?;

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
}

// Wasm task bridging: runs futures inline on the single-threaded executor
// (no XetRuntime offload), dropping the Send bound. There is no sync
// counterpart on wasm.
#[cfg(target_family = "wasm")]
impl TaskRuntime {
    fn run_inner_async<T, F>(
        &self,
        task_name: &'static str,
        fut: F,
    ) -> impl Future<Output = Result<T, XetError>> + 'static
    where
        F: Future<Output = Result<T, XetError>> + 'static,
        T: 'static,
    {
        let token = self.cancellation_token.clone();
        async move {
            tokio::select! {
                _ = token.cancelled() => Err(XetError::UserCancelled(
                    format!("{task_name} cancelled by user"),
                )),
                result = fut => result,
            }
        }
    }

    pub(super) async fn bridge_async<T, F>(&self, task_name: &'static str, fut: F) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + 'static,
        T: 'static,
    {
        self.check_state(task_name)?;
        let result = self.run_inner_async(task_name, fut).await;
        if let Err(ref e) = result {
            self.update_state_on_error(e)?;
        }
        result
    }

    pub(super) async fn bridge_async_finalizing<T, F>(
        &self,
        task_name: &'static str,
        allow_repeat: bool,
        fut: F,
    ) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + 'static,
        T: 'static,
    {
        self.transition_to_finalizing(task_name, allow_repeat)?;

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
}
