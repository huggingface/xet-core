use chrono::SecondsFormat;
use serde::Serialize;

/// The wire body of `POST /v1/telemetry`.
///
/// The server validates exactly these five keys and ignores any others; the field names and
/// casing below are its contract, not a stylistic choice. Note the deliberate mix: `session_id`
/// is snake_case while `userAgent` is camelCase. The server also accepts `user_agent`, but
/// sending both spellings is a 400, so only ever emit the camelCase one.
///
/// Identity is intentionally absent. The server derives `repoId`/`userId` from the request's JWT
/// and stamps `clientIp`, `serverTime`, `env`, and `casVersion` itself, so anything this struct
/// added would be redundant at best.
#[derive(Debug, Clone, Serialize)]
pub struct TelemetryEnvelope {
    /// RFC3339 with millisecond precision, UTC. The server re-normalizes to UTC but rejects
    /// anything it cannot parse as ISO-8601.
    pub time: String,
    pub event: &'static str,
    pub session_id: String,
    #[serde(rename = "userAgent")]
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
        assert_eq!(keys, vec!["event", "metrics", "session_id", "time", "userAgent"]);
    }

    /// The server rejects a body carrying both `userAgent` and `user_agent` as a duplicate field.
    #[test]
    fn test_envelope_emits_only_the_camel_case_user_agent() {
        let v = serde_json::to_value(sample()).unwrap();
        assert_eq!(v["userAgent"], "hf_xet/1.5.4");
        assert!(v.get("user_agent").is_none());
    }

    #[test]
    fn test_time_is_parseable_rfc3339_utc() {
        let v = serde_json::to_value(sample()).unwrap();
        let time = v["time"].as_str().unwrap();
        assert!(time.ends_with('Z'), "expected a UTC 'Z' suffix, got {time}");
        chrono::DateTime::parse_from_rfc3339(time).expect("server parses this with parse_from_rfc3339");
    }
}
