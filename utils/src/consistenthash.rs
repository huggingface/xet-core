use crate::infra::LoadStatus;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use hashring::HashRing;
use tracing::debug;

#[derive(Debug, Clone, Hash, PartialEq)]
struct VNode {
    addr: String,
}

impl VNode {
    fn new(ip: &str) -> Self {
        VNode {
            addr: ip.to_string(),
        }
    }
}

pub struct ConsistentHash {
    ring: HashRing<VNode>,
    pub ts: DateTime<Utc>,
}

impl ConsistentHash {
    pub fn new(load_status_vec: Vec<LoadStatus>) -> Result<Self> {
        let mut ring: HashRing<VNode> = HashRing::new();
        let mut oldest_ts = Utc::now();
        let mut valid_hosts = 0;
        for load_status in &load_status_vec {
            if let Some(load_stat) = &load_status.status {
                debug!("Host: {}", &load_status.address);
                debug!("Load Stats: {:?}", load_stat);
                let ts = DateTime::parse_from_rfc3339(&load_stat.timestamp)?.with_timezone(&Utc);
                if oldest_ts > ts {
                    oldest_ts = ts;
                }
                ring.add(VNode::new(&load_status.address));
                valid_hosts += 1;
            }
        }
        if valid_hosts == 0 {
            return Err(anyhow!(
                "Unable to create ConsistentHash with empty host set {:?}",
                load_status_vec
            ));
        }
        Ok(Self {
            ring,
            ts: oldest_ts,
        })
    }

    pub fn server(&self, key: &str) -> Option<String> {
        self.ring.get(&key).map(|val| val.addr.to_string())
    }
}
#[cfg(test)]
mod tests {
    use crate::consistenthash::ConsistentHash;
    use crate::infra::{LoadStatus, SystemStatus};
    use chrono::{FixedOffset, TimeZone};
    use itertools::Itertools;
    use rand::SeedableRng;
    use rand::{distributions::Alphanumeric, Rng};
    use std::collections::HashMap;
    #[test]
    fn test_empty_errors() {
        let res = ConsistentHash::new(vec![]);
        assert!(res.is_err());

        let infra_input = vec![LoadStatus {
            address: "localhost".to_string(),
            status: None,
        }];
        let res = ConsistentHash::new(infra_input);
        assert!(res.is_err());
    }

    #[test]
    fn test_time_parsing() {
        let infra_input = vec![LoadStatus {
            address: "localhost".to_string(),
            status: Some(SystemStatus {
                timestamp: "junk".to_string(),
                cpu_utilization: 1.0,
            }),
        }];
        let res = ConsistentHash::new(infra_input);
        assert!(res.is_err());

        let infra_input = vec![
            LoadStatus {
                address: "1.1.1.1".to_string(),
                status: Some(SystemStatus {
                    timestamp: "2022-07-06T19:15:00Z".to_string(),
                    cpu_utilization: 1.0,
                }),
            },
            LoadStatus {
                address: "2.1.1.1".to_string(),
                status: Some(SystemStatus {
                    timestamp: "2022-07-07T19:15:00Z".to_string(),
                    cpu_utilization: 1.0,
                }),
            },
        ];
        let res = ConsistentHash::new(infra_input);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert_eq!(
            res.ts,
            FixedOffset::east_opt(0)
                .unwrap()
                .with_ymd_and_hms(2022, 7, 6, 19, 15, 0)
                .unwrap()
        );
    }
    #[test]
    fn test_distribution() {
        let infra_input = vec![
            LoadStatus {
                address: "35.89.208.89".to_string(),
                status: Some(SystemStatus {
                    timestamp: "2022-07-06T19:15:00Z".to_string(),
                    cpu_utilization: 1.0,
                }),
            },
            LoadStatus {
                address: "54.245.178.249".to_string(),
                status: Some(SystemStatus {
                    timestamp: "2022-07-07T19:15:00Z".to_string(),
                    cpu_utilization: 1.0,
                }),
            },
        ];
        let ch = ConsistentHash::new(infra_input).unwrap();
        let mut rng = rand::rngs::SmallRng::from_seed([
            40, 219, 206, 212, 254, 181, 162, 148, 15, 114, 37, 56, 217, 149, 76, 254,
        ]);
        let server_counts: HashMap<String, usize> = (0..20)
            .map(|_| {
                let key: String = rng
                    .sample_iter(&Alphanumeric)
                    .take(7)
                    .map(char::from)
                    .collect();
                ch.server(&key).unwrap()
            })
            .counts();
        // wide range for testing, I am seeing typical outputs like
        // Server is 35.89.208.89 and count is 11
        // Server is 54.245.178.249 and count is 9
        let expected_range = 3..18;
        for (server, count) in server_counts.iter() {
            println!("Server is {} and count is {}", server, count);
            assert!(expected_range.contains(count));
        }
    }
}
