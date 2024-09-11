use crate::{error::Result, CasClientError};
use async_trait::async_trait;
use deadpool::{
    managed::{self, Object, PoolConfig, PoolError, Timeouts},
    Runtime,
};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::{collections::HashMap, marker::PhantomData};
use tokio::sync::RwLock;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::RetryIf;
use tracing::{debug, error, info};
use xet_error::Error;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum CasConnectionPoolError {
    #[error("Invalid Range Read")]
    InvalidRange,
    #[error("Locking primitives error")]
    LockCannotBeAcquired,
    #[error("Connection pool acquisition error: {0}")]
    ConnectionPoolAcquisition(String),
    #[error("Connection pool creation error: {0}")]
    ConnectionPoolCreation(String),
}

// Magic number for the number of concurrent connections we
// want to support.
const POOL_SIZE: usize = 16;

const CONNECTION_RETRIES_ON_TIMEOUT: usize = 3;
const CONNECT_TIMEOUT_MS: u64 = 20000;
const CONNECTION_RETRY_BACKOFF_MS: u64 = 10;
const ASYNC_RUNTIME: Runtime = Runtime::Tokio1;

/// Container for information required to set up and handle
/// CAS connections (both gRPC and H2)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CasConnectionConfig {
    // this endpoint contains the scheme info (http/https)
    // ideally we'd have the scheme separately.
    pub endpoint: String,
    pub user_id: String,
    pub auth: String,
    pub repo_paths: String,
    pub git_xet_version: String,
    pub root_ca: Option<Arc<String>>,
}

impl CasConnectionConfig {
    /// creates a new CasConnectionConfig with given endpoint and user_id
    pub fn new(
        endpoint: String,
        user_id: String,
        auth: String,
        repo_paths: Vec<String>,
        git_xet_version: String,
    ) -> CasConnectionConfig {
        CasConnectionConfig {
            endpoint,
            user_id,
            auth,
            repo_paths: serde_json::to_string(&repo_paths).unwrap_or_else(|_| "[]".to_string()),
            git_xet_version,
            root_ca: None,
        }
    }

    pub fn with_root_ca<T: Into<String>>(mut self, root_ca: T) -> Self {
        self.root_ca = Some(Arc::new(root_ca.into()));
        self
    }
}

/// to be impl'ed by Connection types (DataTransport, GrpcClient)so that
/// connection pool managers could instantiate them using CasConnectionConfig
#[async_trait]
pub trait FromConnectionConfig: Sized {
    async fn new_from_connection_config(config: CasConnectionConfig) -> Result<Self>;
}

#[derive(Debug)]
pub struct PoolManager<T>
where
    T: ?Sized,
{
    connection_type: PhantomData<T>,
    cas_connection_config: CasConnectionConfig,
}

#[async_trait]
impl<T> managed::Manager for PoolManager<T>
where
    T: FromConnectionConfig + Sync + Send,
{
    type Type = T;
    type Error = CasClientError;

    async fn create(&self) -> std::result::Result<Self::Type, Self::Error> {
        // Currently recreating the GrpcClient itself. In my limited testing,
        // this gets slightly better overall performance than cloning the prototype.
        Ok(T::new_from_connection_config(self.cas_connection_config.clone()).await?)
    }

    async fn recycle(&self, _conn: &mut Self::Type) -> managed::RecycleResult<Self::Error> {
        Ok(())
    }
}

// A mapping between IP address and connection pool. Each IP maps to a
// CAS instance, and we keep a fixed pool with the data plane connections.
#[derive(Debug)]
pub struct ConnectionPoolMap<T>
where
    T: FromConnectionConfig + Send + Sync,
{
    pool_map: RwLock<HashMap<String, Arc<managed::Pool<PoolManager<T>>>>>,
    max_pool_size: usize,
}

impl<T> ConnectionPoolMap<T>
where
    T: FromConnectionConfig + Send + Sync,
{
    pub fn new() -> Self {
        ConnectionPoolMap {
            pool_map: RwLock::new(HashMap::default()),
            max_pool_size: POOL_SIZE,
        }
    }

    pub fn new_with_pool_size(max_pool_size: usize) -> Self {
        ConnectionPoolMap {
            pool_map: RwLock::new(HashMap::default()),
            max_pool_size,
        }
    }

    // Creates a connection pool for the given IP address.
    async fn create_pool_for_endpoint_impl(
        cas_connection_config: CasConnectionConfig,
        max_pool_size: usize,
    ) -> std::result::Result<managed::Pool<PoolManager<T>>, CasConnectionPoolError> {
        let endpoint = cas_connection_config.endpoint.clone();

        let mgr = PoolManager {
            connection_type: PhantomData,
            cas_connection_config,
        };

        info!("Creating pool for {endpoint}");

        let pool = managed::Pool::<PoolManager<T>>::builder(mgr)
            .config(PoolConfig {
                max_size: max_pool_size,
                timeouts: Timeouts {
                    create: Some(Duration::from_millis(CONNECT_TIMEOUT_MS)),
                    wait: None,
                    recycle: Some(Duration::from_millis(0)),
                },
            })
            .runtime(ASYNC_RUNTIME)
            .build()
            .map_err(|e| {
                error!(
                    "Error creating connection pool: {:?} server: {}",
                    e, endpoint
                );
                CasConnectionPoolError::ConnectionPoolCreation(format!("{e:?}"))
            })?;

        Ok(pool)
    }

    // // Gets a connection object for the given endpoint. This will
    // // create a connection pool for the endpoint if none exists already.
    pub async fn get_connection_for_config(
        &self,
        cas_connection_config: CasConnectionConfig,
    ) -> std::result::Result<Object<PoolManager<T>>, CasConnectionPoolError> {
        let strategy = ExponentialBackoff::from_millis(CONNECTION_RETRY_BACKOFF_MS)
            .map(jitter)
            .take(CONNECTION_RETRIES_ON_TIMEOUT);

        let endpoint = cas_connection_config.endpoint.clone();
        let pool = self.get_pool_for_config(cas_connection_config).await?;
        let result = RetryIf::spawn(
            strategy,
            || async {
                debug!("Trying to get connection for endpoint: {}", endpoint);
                pool.get().await
            },
            is_pool_connection_error_retriable,
        )
        .await
        .map_err(|e| {
            error!(
                "Error acquiring connection for {:?} from pool: {:?} after {} retries",
                endpoint, e, CONNECTION_RETRIES_ON_TIMEOUT
            );
            CasConnectionPoolError::ConnectionPoolCreation(format!("{e:?}"))
        })?;

        Ok(result)
    }

    // Utility function to get a connection pool for the given endpoint. If none already
    // exists, it will create it and insert it into the map.
    async fn get_pool_for_config(
        &self,
        cas_connection_config: CasConnectionConfig,
    ) -> std::result::Result<Arc<managed::Pool<PoolManager<T>>>, CasConnectionPoolError> {
        debug!("Using connection pool");

        // handle the typical case up front where we are connecting to an
        // endpoint that already has its pool initialized. Scopes are meant
        // to keep the
        {
            let now = Instant::now();
            debug!("Acquiring connection map read lock");
            let map = self.pool_map.read().await;
            debug!(
                "Connection map read lock acquired in {} ms",
                now.elapsed().as_millis()
            );

            if let Some(pool) = map.get(cas_connection_config.endpoint.as_str()) {
                return Ok(pool.clone());
            };
        }

        let endpoint = cas_connection_config.endpoint.clone();
        // If the connection isn't in the map, create it and insert.
        // At worst, we'll briefly have multiple pools overwriting the hashmap, but this
        // is needed so as not to carry the lock across an await
        let new_pool = Arc::new(
            Self::create_pool_for_endpoint_impl(cas_connection_config, self.max_pool_size).await?,
        );
        {
            let now = Instant::now();
            debug!("Acquiring connection map write lock");
            let mut map = self.pool_map.write().await;

            debug!(
                "Connection map write lock acquired in {} ms",
                now.elapsed().as_millis()
            );

            map.insert(endpoint, new_pool.clone());
        }
        Ok(new_pool)
    }

    // Utility to see how many connections are avialable for the IP address.
    // This currently requires a lock, so it's not a good idea to use this for
    // testing if you should get the lock.
    pub async fn get_pool_status_for_endpoint(
        &self,
        ip_address: String,
    ) -> Option<managed::Status> {
        let map = self.pool_map.read().await;

        map.get(&ip_address).map(|s| s.status())
    }
}

fn is_pool_connection_error_retriable(err: &PoolError<CasClientError>) -> bool {
    matches!(err, PoolError::Timeout(_))
}

impl<T> Default for ConnectionPoolMap<T>
where
    T: FromConnectionConfig + Send + Sync,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::cas_connection_pool::{ConnectionPoolMap, FromConnectionConfig};
    use crate::error::Result;
    use async_trait::async_trait;

    use super::CasConnectionConfig;

    const USER_ID: &str = "XET USER";
    const AUTH: &str = "XET SECRET";
    static REPO_PATHS: &str = "/XET/REPO";
    const GIT_XET_VERSION: &str = "0.1.0";

    #[derive(Debug, Clone)]
    struct PoolTestData {
        cas_connection_config: CasConnectionConfig,
    }

    #[async_trait]
    impl FromConnectionConfig for PoolTestData {
        async fn new_from_connection_config(
            cas_connection_config: CasConnectionConfig,
        ) -> Result<PoolTestData> {
            Ok(PoolTestData {
                cas_connection_config,
            })
        }
    }

    #[tokio::test]
    async fn test_get_creates_pool() {
        let cas_connection_pool = ConnectionPoolMap::<PoolTestData>::new();

        let server1 = "foo".to_string();
        let server2 = "bar".to_string();

        let config1 = CasConnectionConfig::new(
            server1.clone(),
            USER_ID.to_string(),
            AUTH.to_string(),
            vec![REPO_PATHS.to_string()],
            GIT_XET_VERSION.to_string(),
        );
        let config2 = CasConnectionConfig::new(
            server2.clone(),
            USER_ID.to_string(),
            AUTH.to_string(),
            vec![REPO_PATHS.to_string()],
            GIT_XET_VERSION.to_string(),
        );

        let conn0 = cas_connection_pool
            .get_connection_for_config(config1.clone())
            .await
            .unwrap();
        assert_eq!(conn0.cas_connection_config, config1.clone());
        let stat = cas_connection_pool
            .get_pool_status_for_endpoint(server1.clone())
            .await
            .unwrap();
        assert_eq!(stat.size, 1);
        assert_eq!(stat.available, 0);

        let conn1 = cas_connection_pool
            .get_connection_for_config(config1.clone())
            .await
            .unwrap();
        assert_eq!(conn1.cas_connection_config, config1.clone());

        let stat = cas_connection_pool
            .get_pool_status_for_endpoint(server1.clone())
            .await
            .unwrap();
        assert_eq!(stat.size, 2);
        assert_eq!(stat.available, 0);

        let conn2 = cas_connection_pool
            .get_connection_for_config(config1.clone())
            .await
            .unwrap();
        assert_eq!(conn2.cas_connection_config, config1.clone());

        let stat = cas_connection_pool
            .get_pool_status_for_endpoint(server1.clone())
            .await
            .unwrap();
        assert_eq!(stat.size, 3);
        assert_eq!(stat.available, 0);

        let conn3 = cas_connection_pool
            .get_connection_for_config(config1.clone())
            .await
            .unwrap();
        assert_eq!(conn3.cas_connection_config, config1.clone());

        let stat = cas_connection_pool
            .get_pool_status_for_endpoint(server1.clone())
            .await
            .unwrap();
        assert_eq!(stat.size, 4);
        assert_eq!(stat.available, 0);

        drop(conn0);

        let stat = cas_connection_pool
            .get_pool_status_for_endpoint(server1.clone())
            .await
            .unwrap();

        println!("{}, {}", stat.size, stat.available);
        assert_eq!(stat.size, 4);
        assert_eq!(stat.available, 1);

        drop(conn1);

        let stat = cas_connection_pool
            .get_pool_status_for_endpoint(server1.clone())
            .await
            .unwrap();

        println!("{}, {}", stat.size, stat.available);
        assert_eq!(stat.size, 4);
        assert_eq!(stat.available, 2);

        // ensure there's no cross pollination between different server strings
        let conn0 = cas_connection_pool
            .get_connection_for_config(config2.clone())
            .await
            .unwrap();
        assert_eq!(conn0.cas_connection_config, config2.clone());
        let stat = cas_connection_pool
            .get_pool_status_for_endpoint(server2.clone())
            .await
            .unwrap();
        assert_eq!(stat.size, 1);
        assert_eq!(stat.available, 0);

        // TODO: Add more tests here
    }

    #[tokio::test]
    async fn test_repo_name_encoding() {
        let data: Vec<Vec<String>> = vec![
            vec!["user1/repo-😀".to_string(), "user1/répô_123".to_string()],
            vec![
                "user2/👾_repo".to_string(),
                "user2/üникод".to_string(),
                "user2/foobar!@#".to_string(),
            ],
            vec!["user3/sømè_repo".to_string(), "user3/你好-世界".to_string()],
            vec!["user4/✨🌈repo".to_string()],
            vec!["user5/Ω≈ç√repo".to_string()],
            vec!["user6/42°_repo".to_string()],
            vec![
                "user7/äëïöü_repo".to_string(),
                "user7/ĀāĒēĪīŌōŪū".to_string(),
            ],
        ];
        for inner_vec in data {
            let config = CasConnectionConfig::new(
                "".to_string(),
                "".to_string(),
                "".to_string(),
                inner_vec.clone(),
                "".to_string(),
            );
            let vec_of_strings: Vec<String> = serde_json::from_str(config.repo_paths.as_ref())
                .expect("Failed to deserialize JSON");
            assert_eq!(vec_of_strings.clone(), inner_vec.clone());
        }
    }
}
