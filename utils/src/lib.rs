#![cfg_attr(feature = "strict", deny(warnings))]

use std::ops::Range;

use tonic::Status;

// The auto code generated by tonic has clippy warnings. Disabling until those are
// resolved in tonic/prost.
pub mod common {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("common");
}

pub mod cas {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("cas");
}

pub mod infra {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("infra");
}

pub mod alb {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("aws");
}

pub mod shard {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("shard");
}

pub mod consistenthash;
pub mod constants;
pub mod errors;
pub mod gitbaretools;
pub mod key;
pub mod safeio;
pub mod singleflight;
pub mod version;
pub mod auth;

mod output_bytes;

use crate::common::{CompressionScheme, InitiateResponse};
pub use output_bytes::output_bytes;

impl TryFrom<cas::Range> for Range<u64> {
    type Error = Status;

    fn try_from(range_proto: cas::Range) -> Result<Self, Self::Error> {
        if range_proto.start > range_proto.end {
            return Err(Status::failed_precondition(format!(
                "Range: {range_proto:?} has an end smaller than the start"
            )));
        }
        Ok(range_proto.start..range_proto.end)
    }
}

impl TryFrom<&cas::Range> for Range<u64> {
    type Error = Status;

    fn try_from(range_proto: &cas::Range) -> Result<Self, Self::Error> {
        if range_proto.start > range_proto.end {
            return Err(Status::failed_precondition(format!(
                "Range: {range_proto:?} has an end smaller than the start"
            )));
        }
        Ok(range_proto.start..range_proto.end)
    }
}

impl std::fmt::Display for common::Scheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let as_str = match self {
            common::Scheme::Http => "http",
            common::Scheme::Https => "https",
        };
        write!(f, "{as_str}")
    }
}

impl std::fmt::Display for common::EndpointConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let common::EndpointConfig {
            host, port, scheme, ..
        } = self;
        let scheme_parsed = common::Scheme::try_from(*scheme).unwrap_or_default();
        write!(f, "{scheme_parsed}://{host}:{port}")
    }
}

impl InitiateResponse {
    pub fn get_accepted_encodings_parsed(&self) -> Vec<CompressionScheme> {
        self.accepted_encodings
            .iter()
            .filter_map(|i| CompressionScheme::try_from(*i).ok())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::common::EndpointConfig;

    use super::*;

    #[test]
    fn test_range_conversion() {
        let r = cas::Range { start: 0, end: 10 };
        let range = Range::try_from(r).unwrap();
        assert_eq!(range.start, 0);
        assert_eq!(range.end, 10);
    }

    #[test]
    fn test_range_conversion_zero_len() {
        let r = cas::Range { start: 10, end: 10 };
        let range = Range::try_from(r).unwrap();
        assert_eq!(range.start, 10);
        assert_eq!(range.end, 10);
    }

    #[test]
    fn test_range_conversion_failed() {
        let r = cas::Range { start: 20, end: 10 };
        let res = Range::try_from(r);
        assert!(res.is_err());
    }

    #[test]
    fn test_endpoint_config_to_endpoint_string() {
        let host = "xetxet";
        let port = 443;
        let e1 = EndpointConfig {
            host: host.to_string(),
            port,
            scheme: common::Scheme::Http.into(),
            root_ca_certificate: String::new(),
        };

        assert_eq!(e1.to_string(), format!("http://{host}:{port}"));

        let e2 = EndpointConfig {
            host: host.to_string(),
            port,
            scheme: common::Scheme::Https.into(),
            root_ca_certificate: String::from("abcd"),
        };

        assert_eq!(e2.to_string(), format!("https://{host}:{port}"));
    }
}
