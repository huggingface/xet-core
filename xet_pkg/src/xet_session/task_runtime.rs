use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;
use xet_runtime::RuntimeError;
use xet_runtime::core::XetRuntime;

use crate::error::XetError;

#[derive(Clone, Debug)]
pub(super) enum TaskState<T> {
    Running,
    Finished(Result<T, XetError>),
    Cancelled,
}

pub(super) type TaskRuntimeState = TaskState<()>;

pub(super) struct TaskRuntime {
    runtime: Arc<XetRuntime>,
    cancellation_token: CancellationToken,
    sigint_cancellation_token: CancellationToken,
    state: Mutex<TaskRuntimeState>,
    terminal_gate: Arc<AsyncMutex<()>>,
    children: Mutex<Vec<Weak<TaskRuntime>>>,
}

impl TaskRuntime {
    pub(super) fn new_root(runtime: Arc<XetRuntime>) -> Arc<Self> {
        Arc::new(Self {
            runtime,
            cancellation_token: CancellationToken::new(),
            sigint_cancellation_token: CancellationToken::new(),
            state: Mutex::new(TaskRuntimeState::Running),
            terminal_gate: Arc::new(AsyncMutex::new(())),
            children: Mutex::new(Vec::new()),
        })
    }

    pub(super) fn child(self: &Arc<Self>) -> Result<Arc<Self>, XetError> {
        let child = Arc::new(Self {
            runtime: self.runtime.clone(),
            cancellation_token: self.cancellation_token.child_token(),
            sigint_cancellation_token: self.sigint_cancellation_token.child_token(),
            state: Mutex::new(self.state()?),
            terminal_gate: Arc::new(AsyncMutex::new(())),
            children: Mutex::new(Vec::new()),
        });

        self.children.lock()?.push(Arc::downgrade(&child));
        Ok(child)
    }

    pub(super) fn state(&self) -> Result<TaskRuntimeState, XetError> {
        Ok(self.state.lock()?.clone())
    }

    pub(super) fn set_state(&self, new_state: TaskRuntimeState) -> Result<(), XetError> {
        *self.state.lock()? = new_state;
        Ok(())
    }

    pub(super) fn set_state_recursive(&self, new_state: TaskRuntimeState) -> Result<(), XetError> {
        self.set_state(new_state.clone())?;
        let children = self.live_children()?;
        for child in children {
            child.set_state_recursive(new_state.clone())?;
        }
        Ok(())
    }

    pub(super) fn cancel_subtree(&self) -> Result<(), XetError> {
        self.cancellation_token.cancel();
        self.set_state_recursive(TaskRuntimeState::Finished(Err(XetError::Aborted)))
    }

    pub(super) fn sigint_cancel_subtree(&self) -> Result<(), XetError> {
        self.sigint_cancellation_token.cancel();
        self.set_state_recursive(TaskRuntimeState::Cancelled)
    }

    pub(super) fn mark_runtime_finished_ok(&self) -> Result<(), XetError> {
        self.set_state_recursive(TaskRuntimeState::Finished(Ok(())))
    }

    pub(super) fn mark_runtime_finished_err(&self, err: XetError) -> Result<(), XetError> {
        self.set_state_recursive(TaskRuntimeState::Finished(Err(err)))
    }

    pub(super) fn check_runtime_running(
        &self,
        finished_error: XetError,
        cancelled_message: &'static str,
    ) -> Result<(), XetError> {
        match self.state()? {
            TaskRuntimeState::Running => Ok(()),
            TaskRuntimeState::Cancelled => Err(XetError::Cancelled(cancelled_message.to_string())),
            TaskRuntimeState::Finished(Err(XetError::Aborted)) => Err(XetError::Aborted),
            TaskRuntimeState::Finished(_) => Err(finished_error),
        }
    }

    pub(super) async fn bridge_async_runtime_checked<T, F>(
        &self,
        task_name: &'static str,
        finished_error: XetError,
        cancelled_message: &'static str,
        fut: F,
    ) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        self.check_runtime_running(finished_error, cancelled_message)?;
        self.bridge_async(task_name, fut).await
    }

    pub(super) fn bridge_sync_runtime_checked<T, F>(
        &self,
        task_name: &'static str,
        finished_error: XetError,
        cancelled_message: &'static str,
        fut: F,
    ) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        self.check_runtime_running(finished_error, cancelled_message)?;
        self.bridge_sync(task_name, fut)
    }

    /// Checks state before a terminal operation; if not `Running`, caches and returns the
    /// appropriate error.  Returns `Ok(())` when the caller should proceed with the bridge call.
    fn check_terminal_state<T: Clone>(
        &self,
        task_name: &'static str,
        result_cell: &OnceLock<Result<T, XetError>>,
    ) -> Result<(), XetError> {
        match self.state()? {
            TaskRuntimeState::Running => Ok(()),
            TaskRuntimeState::Cancelled => {
                let result = Err(XetError::Cancelled(format!("{task_name} cancelled")));
                let _ = result_cell.set(result.clone());
                result.map(|_| ())
            },
            TaskRuntimeState::Finished(Err(XetError::Aborted)) => {
                let result = Err(XetError::Aborted);
                let _ = result_cell.set(result.clone());
                result.map(|_| ())
            },
            TaskRuntimeState::Finished(_) => {
                let result = Err(XetError::other(format!("{task_name} already finished")));
                let _ = result_cell.set(result.clone());
                result.map(|_| ())
            },
        }
    }

    /// Records the bridge result into `result_cell` and updates internal state to match.
    fn record_terminal_result<T: Clone>(
        &self,
        result: &Result<T, XetError>,
        result_cell: &OnceLock<Result<T, XetError>>,
    ) -> Result<(), XetError> {
        self.set_state(match result {
            Ok(_) => TaskRuntimeState::Finished(Ok(())),
            Err(XetError::Cancelled(_)) => TaskRuntimeState::Cancelled,
            Err(e) => TaskRuntimeState::Finished(Err(e.clone())),
        })?;
        let _ = result_cell.set(result.clone());
        Ok(())
    }

    pub(super) async fn bridge_async_terminal<T, F>(
        &self,
        task_name: &'static str,
        result_cell: Arc<OnceLock<Result<T, XetError>>>,
        fut: F,
    ) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Clone + Send + 'static,
    {
        if let Some(cached) = result_cell.get() {
            return cached.clone();
        }
        let _gate = self.terminal_gate.clone().lock_owned().await;
        if let Some(cached) = result_cell.get() {
            return cached.clone();
        }

        self.check_terminal_state(task_name, &result_cell)?;
        let result = self.bridge_async(task_name, fut).await;
        self.record_terminal_result(&result, &result_cell)?;
        result
    }

    pub(super) async fn bridge_async_checked<T, F>(
        &self,
        task_name: &'static str,
        finished_error: XetError,
        cancelled_message: &'static str,
        fut: F,
    ) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        self.check_runtime_running(finished_error, cancelled_message)?;
        let result = self.bridge_async(task_name, fut).await;
        if matches!(result, Err(XetError::Cancelled(_))) {
            self.set_state(TaskRuntimeState::Cancelled)?;
        }
        result
    }

    pub(super) fn bridge_sync_checked<T, F>(
        &self,
        task_name: &'static str,
        finished_error: XetError,
        cancelled_message: &'static str,
        fut: F,
    ) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        self.check_runtime_running(finished_error, cancelled_message)?;
        let result = self.bridge_sync(task_name, fut);
        if matches!(result, Err(XetError::Cancelled(_))) {
            self.set_state(TaskRuntimeState::Cancelled)?;
        }
        result
    }

    pub(super) fn bridge_sync_terminal<T, F>(
        &self,
        task_name: &'static str,
        result_cell: Arc<OnceLock<Result<T, XetError>>>,
        fut: F,
    ) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Clone + Send + 'static,
    {
        if let Some(cached) = result_cell.get() {
            return cached.clone();
        }
        let _gate = self.terminal_gate.blocking_lock();
        if let Some(cached) = result_cell.get() {
            return cached.clone();
        }

        self.check_terminal_state(task_name, &result_cell)?;
        let result = self.bridge_sync(task_name, fut);
        self.record_terminal_result(&result, &result_cell)?;
        result
    }

    pub(super) async fn bridge_async<T, F>(&self, task_name: &'static str, fut: F) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        let token = self.cancellation_token.clone();
        let sigint_token = self.sigint_cancellation_token.clone();
        self.runtime
            .bridge_async(task_name, async move {
                tokio::select! {
                    _ = sigint_token.cancelled() => Err(XetError::from(RuntimeError::TaskCanceled(task_name.to_string()))),
                    _ = token.cancelled() => Err(XetError::from(RuntimeError::TaskCanceled(task_name.to_string()))),
                    result = fut => result,
                }
            })
            .await?
    }

    pub(super) fn bridge_sync<T, F>(&self, task_name: &'static str, fut: F) -> Result<T, XetError>
    where
        F: Future<Output = Result<T, XetError>> + Send + 'static,
        T: Send + 'static,
    {
        let token = self.cancellation_token.clone();
        let sigint_token = self.sigint_cancellation_token.clone();
        self.runtime.bridge_sync(async move {
            tokio::select! {
                _ = sigint_token.cancelled() => Err(XetError::from(RuntimeError::TaskCanceled(task_name.to_string()))),
                _ = token.cancelled() => Err(XetError::from(RuntimeError::TaskCanceled(task_name.to_string()))),
                result = fut => result,
            }
        })?
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
