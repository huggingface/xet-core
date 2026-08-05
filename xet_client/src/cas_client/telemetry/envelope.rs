use chrono::SecondsFormat;
use serde::Serialize;

/// The wire body of `POST /v1/telemetry`.
///
/// The server validates exactly these five keys and ignores any others; the field names below are
/// its contract, not a stylistic choice. **Every key is snake_case**, matching the agreed naming
/// across the whole document - envelope, metrics, and the fields the server stamps itself.
///
/// The server accepts a camelCase `userAgent` as a serde alias for compatibility with clients that
/// predate the convention. Emit only `user_agent`: a body carrying both spellings is a
/// duplicate-field 400.
///
/// Identity is intentionally absent. The server derives `repo_id`/`user_id` from the request's JWT
/// and stamps `client_ip`, `server_time`, `env`, and `cas_version` itself, so anything this struct
/// added would be redundant at best.
#[derive(Debug, Clone, Serialize)]
pub struct TelemetryEnvelope {
    /// RFC3339 with millisecond precision, UTC. The server re-normalizes to UTC but rejects
    /// anything it cannot parse as ISO-8601.
    pub time: String,
    pub event: &'static str,
    pub session_id: String,
    pub user_agent: String,
    /// A flat object of scalars. Built in `xet_data`, which owns the metric definitions.
    pub metrics: serde_json::Value,
}

impl TelemetryEnvelope {
    /// Stamps `time` with the current wall clock.
    pub fn new(event: &'static str, session_id: String, user_agent: String, metrics: serde_json::Value) -> Self {
        Self {
            time: chrono::Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            event,
            session_id,
            user_agent,
            metrics,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn sample() -> TelemetryEnvelope {
        TelemetryEnvelope::new("xet_upload_summary", "sess-1".into(), "hf_xet/1.5.4".into(), json!({"a": 1}))
    }

    #[test]
    fn test_envelope_has_exactly_the_five_contract_keys() {
        let v = serde_json::to_value(sample()).unwrap();
        let mut keys: Vec<_> = v.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, vec!["event", "metrics", "session_id", "time", "user_agent"]);
    }

    /// Every envelope key is snake_case. The camelCase spelling is only a server-side alias for
    /// older clients, and a body carrying both is rejected as a duplicate field - so exactly one of
    /// the two must appear, and it must be the snake_case one.
    #[test]
    fn test_envelope_emits_only_the_snake_case_user_agent() {
        let v = serde_json::to_value(sample()).unwrap();
        assert_eq!(v["user_agent"], "hf_xet/1.5.4");
        assert!(v.get("userAgent").is_none());
    }

    /// No key anywhere in the envelope carries a capital letter.
    #[test]
    fn test_every_envelope_key_is_snake_case() {
        let v = serde_json::to_value(sample()).unwrap();
        for key in v.as_object().unwrap().keys() {
            assert!(!key.chars().any(char::is_uppercase), "envelope key '{key}' is not snake_case");
        }
    }

    #[test]
    fn test_time_is_parseable_rfc3339_utc() {
        let v = serde_json::to_value(sample()).unwrap();
        let time = v["time"].as_str().unwrap();
        assert!(time.ends_with('Z'), "expected a UTC 'Z' suffix, got {time}");
        chrono::DateTime::parse_from_rfc3339(time).expect("server parses this with parse_from_rfc3339");
    }
}
