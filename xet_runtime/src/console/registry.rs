use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, RwLock, Weak};

use super::model::{SessionState, SessionSummary};
use super::ring::TimestampedRing;
use super::state::SessionConsole;

pub const ENDED_SESSIONS_CAPACITY: usize = 16;

pub struct ConsoleRegistry {
    sessions: RwLock<HashMap<String, Weak<SessionConsole>>>,
    ended_sessions: Mutex<TimestampedRing<SessionSummary>>,
}

impl Default for ConsoleRegistry {
    fn default() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            ended_sessions: Mutex::new(TimestampedRing::new(ENDED_SESSIONS_CAPACITY)),
        }
    }
}

static CONSOLE_REGISTRY: LazyLock<Arc<ConsoleRegistry>> =
    LazyLock::new(|| Arc::new(ConsoleRegistry::default()));

pub fn registry() -> Arc<ConsoleRegistry> {
    CONSOLE_REGISTRY.clone()
}

/// Owns a registered session scope. Held by XetCommon.console_session; on drop
/// (session graph released) it reports the ended summary and prunes the weak entry.
pub struct SessionHandle {
    pub scope: Arc<SessionConsole>,
    registry: Option<Arc<ConsoleRegistry>>, // None for detached test scopes
}

impl SessionHandle {
    /// Test-only scope with no registry reporting.
    pub fn detached(scope: Arc<SessionConsole>) -> Self {
        Self { scope, registry: None }
    }
}

impl Drop for SessionHandle {
    fn drop(&mut self) {
        let Some(reg) = &self.registry else { return };
        reg.record_ended_session(self.scope.summary(SessionState::Ended));
        if let Ok(mut s) = reg.sessions.write() {
            s.remove(&self.scope.id);
        }
    }
}

impl ConsoleRegistry {
    /// Register a new session. Stores a `Weak`, prunes dead entries, returns
    /// a `SessionHandle` that reports the session as ended when dropped.
    pub fn register_session(
        self: &Arc<Self>,
        id: String,
        config: Vec<(String, String)>,
    ) -> SessionHandle {
        let scope = SessionConsole::new(id.clone(), config);
        if let Ok(mut sessions) = self.sessions.write() {
            sessions.retain(|_, w| w.upgrade().is_some());
            sessions.insert(id, Arc::downgrade(&scope));
        }
        SessionHandle { scope, registry: Some(self.clone()) }
    }

    /// Look up a live session by id.
    pub fn session(&self, id: &str) -> Option<Arc<SessionConsole>> {
        let sessions = self.sessions.read().ok()?;
        sessions.get(id)?.upgrade()
    }

    /// All currently live sessions; prunes dead weaks in place.
    pub fn live_sessions(&self) -> Vec<Arc<SessionConsole>> {
        let Ok(mut sessions) = self.sessions.write() else {
            return Vec::new();
        };
        let mut result = Vec::new();
        sessions.retain(|_, w| {
            if let Some(arc) = w.upgrade() {
                result.push(arc);
                true
            } else {
                false
            }
        });
        result
    }

    /// Returns (active summaries, ended summaries). Prunes dead weaks from
    /// the active map in place.
    pub fn session_summaries(&self) -> (Vec<SessionSummary>, Vec<SessionSummary>) {
        let active = {
            let Ok(mut sessions) = self.sessions.write() else {
                return (Vec::new(), Vec::new());
            };
            let mut result = Vec::new();
            sessions.retain(|_, w| {
                if let Some(arc) = w.upgrade() {
                    result.push(arc.summary(SessionState::Active));
                    true
                } else {
                    false
                }
            });
            result
        };
        let ended = self
            .ended_sessions
            .lock()
            .map(|ring| ring.snapshot().into_iter().map(|(_, s)| s).collect())
            .unwrap_or_default();
        (active, ended)
    }

    pub fn record_ended_session(&self, summary: SessionSummary) {
        let Ok(ring) = self.ended_sessions.lock() else { return };
        ring.push(summary);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::console::model::SessionState;

    #[test]
    fn register_list_and_end_session() {
        let reg = Arc::new(ConsoleRegistry::default()); // tests use a local instance, not the global
        let handle = reg.register_session("sess-1".to_string(), vec![]);
        assert_eq!(reg.session_summaries().0.len(), 1);
        assert_eq!(reg.session("sess-1").unwrap().id, "sess-1");
        drop(handle);
        let (active, ended) = reg.session_summaries();
        assert!(active.is_empty());
        assert_eq!(ended.len(), 1);
        assert_eq!(ended[0].state, SessionState::Ended);
    }

    #[test]
    fn ended_sessions_are_bounded() {
        let reg = Arc::new(ConsoleRegistry::default());
        for i in 0..(ENDED_SESSIONS_CAPACITY + 4) {
            let handle = reg.register_session(format!("s{i}"), vec![]);
            drop(handle);
        }
        assert_eq!(reg.session_summaries().1.len(), ENDED_SESSIONS_CAPACITY);
    }
}
