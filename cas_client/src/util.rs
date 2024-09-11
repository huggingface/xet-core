#[cfg(test)]
pub(crate) mod grpc_mock {
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use cas::infra::infra_utils_server::InfraUtils;
    use oneshot::{channel, Receiver};
    use tokio::sync::oneshot;
    use tokio::sync::oneshot::Sender;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;
    use tonic::transport::{Error, Server};
    use tonic::{Request, Response, Status};

    use crate::cas_connection_pool::CasConnectionConfig;
    use crate::grpc::get_client;
    use crate::grpc::GrpcClient;
    use cas::cas::cas_server::{Cas, CasServer};
    use cas::cas::{
        GetRangeRequest, GetRangeResponse, GetRequest, GetResponse, HeadRequest, HeadResponse,
        PutCompleteRequest, PutCompleteResponse, PutRequest, PutResponse,
    };
    use cas::common::{Empty, InitiateRequest, InitiateResponse};
    use cas::infra::EndpointLoadResponse;
    use retry_strategy::RetryStrategy;

    const TEST_PORT_START: u16 = 64400;

    lazy_static::lazy_static! {
        static ref CURRENT_PORT: AtomicU16 = AtomicU16::new(TEST_PORT_START);
    }

    trait_set::trait_set! {
        pub trait PutFn = Fn(Request<PutRequest>) -> Result<Response<PutResponse>, Status> + 'static;
        pub trait InitiateFn = Fn(Request<InitiateRequest>) -> Result<Response<InitiateResponse>, Status> + 'static;
        pub trait PutCompleteFn = Fn(Request<PutCompleteRequest>) -> Result<Response<PutCompleteResponse>, Status> + 'static;
        pub trait GetFn = Fn(Request<GetRequest>) -> Result<Response<GetResponse>, Status> + 'static;
        pub trait GetRangeFn = Fn(Request<GetRangeRequest>) -> Result<Response<GetRangeResponse>, Status> + 'static;
        pub trait HeadFn = Fn(Request<HeadRequest>) -> Result<Response<HeadResponse>, Status> + 'static;
    }

    /// "Mocks" the grpc service for CAS. This is implemented by allowing the test writer
    /// to define the functionality needed for the server and then calling `#start()` to
    /// run the server on some port. A GrpcClient will be returned to test with as well
    /// as a shutdown hook that can be called to shutdown the mock service.
    #[derive(Default)]
    pub struct MockService {
        put_fn: Option<Arc<dyn PutFn>>,
        initiate_fn: Option<Arc<dyn InitiateFn>>,
        put_complete_fn: Option<Arc<dyn PutCompleteFn>>,
        get_fn: Option<Arc<dyn GetFn>>,
        get_range_fn: Option<Arc<dyn GetRangeFn>>,
        head_fn: Option<Arc<dyn HeadFn>>,
    }

    impl MockService {
        #[allow(dead_code)]
        pub fn with_initiate<F: InitiateFn>(self, f: F) -> Self {
            Self {
                initiate_fn: Some(Arc::new(f)),
                ..self
            }
        }
        #[allow(dead_code)]
        pub fn with_put_complete<F: PutCompleteFn>(self, f: F) -> Self {
            Self {
                put_complete_fn: Some(Arc::new(f)),
                ..self
            }
        }

        pub fn with_put<F: PutFn>(self, f: F) -> Self {
            Self {
                put_fn: Some(Arc::new(f)),
                ..self
            }
        }

        #[allow(dead_code)]
        pub fn with_get<F: GetFn>(self, f: F) -> Self {
            Self {
                get_fn: Some(Arc::new(f)),
                ..self
            }
        }

        #[allow(dead_code)]
        pub fn with_get_range<F: GetRangeFn>(self, f: F) -> Self {
            Self {
                get_range_fn: Some(Arc::new(f)),
                ..self
            }
        }

        #[allow(dead_code)]
        pub fn with_head<F: HeadFn>(self, f: F) -> Self {
            Self {
                head_fn: Some(Arc::new(f)),
                ..self
            }
        }

        pub async fn start(self) -> (ShutdownHook, GrpcClient) {
            self.start_with_retry_strategy(RetryStrategy::new(2, 1))
                .await
        }

        pub async fn start_with_retry_strategy(
            self,
            strategy: RetryStrategy,
        ) -> (ShutdownHook, GrpcClient) {
            // Get next port
            let port = CURRENT_PORT.fetch_add(1, Ordering::SeqCst);
            let addr = format!("127.0.0.1:{}", port).parse().unwrap();

            // Start up the server
            let (tx, rx) = channel::<()>();
            let handle = tokio::spawn(
                Server::builder()
                    .add_service(CasServer::new(self))
                    .serve_with_shutdown(addr, shutdown(rx)),
            );
            let shutdown_hook = ShutdownHook::new(tx, handle);

            // Wait for server to start up
            sleep(Duration::from_millis(10)).await;

            // Create dedicated client for server
            let endpoint = format!("127.0.0.1:{}", port);
            let user_id = "xet_user".to_string();
            let auth = "xet_auth".to_string();
            let repo_paths = vec!["example".to_string()];
            let version = "0.1.0".to_string();
            let cas_client = get_client(CasConnectionConfig::new(
                endpoint, user_id, auth, repo_paths, version,
            ))
            .await
            .unwrap();
            let client = GrpcClient::new("127.0.0.1".to_string(), cas_client, strategy);
            (shutdown_hook, client)
        }
    }

    // Unsafe hacks so that we can dynamically add in overrides to the mock functionality
    // (Fn isn't sync/send). There's probably a better way to do this that isn't so blunt/fragile.
    unsafe impl Send for MockService {}
    unsafe impl Sync for MockService {}

    #[async_trait::async_trait]
    impl InfraUtils for MockService {
        async fn endpoint_load(
            &self,
            _request: Request<Empty>,
        ) -> Result<Response<EndpointLoadResponse>, Status> {
            unimplemented!()
        }
        async fn initiate(
            &self,
            request: Request<InitiateRequest>,
        ) -> Result<Response<InitiateResponse>, Status> {
            self.initiate_fn.as_ref().unwrap()(request)
        }
    }
    #[async_trait::async_trait]
    impl Cas for MockService {
        async fn initiate(
            &self,
            request: Request<InitiateRequest>,
        ) -> Result<Response<InitiateResponse>, Status> {
            self.initiate_fn.as_ref().unwrap()(request)
        }

        async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
            self.put_fn.as_ref().unwrap()(request)
        }

        async fn put_complete(
            &self,
            request: Request<PutCompleteRequest>,
        ) -> Result<Response<PutCompleteResponse>, Status> {
            self.put_complete_fn.as_ref().unwrap()(request)
        }

        async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
            self.get_fn.as_ref().unwrap()(request)
        }

        async fn get_range(
            &self,
            request: Request<GetRangeRequest>,
        ) -> Result<Response<GetRangeResponse>, Status> {
            self.get_range_fn.as_ref().unwrap()(request)
        }

        async fn head(
            &self,
            request: Request<HeadRequest>,
        ) -> Result<Response<HeadResponse>, Status> {
            self.head_fn.as_ref().unwrap()(request)
        }
    }

    async fn shutdown(rx: Receiver<()>) {
        let _ = rx.await;
    }

    /// Encapsulates logic to shutdown a running tonic Server. This is done through
    /// sending a message on a channel that the server is listening on for shutdown.
    /// Once the message has been sent, the spawned task is awaited using its JoinHandle.
    ///
    /// TODO: implementing `Drop` with async is difficult and the naïve implementation
    ///       ends up blocking the test completion. There is likely some deadlock somewhere.
    pub struct ShutdownHook {
        tx: Option<Sender<()>>,
        join_handle: Option<JoinHandle<Result<(), Error>>>,
    }

    impl ShutdownHook {
        pub fn new(tx: Sender<()>, join_handle: JoinHandle<Result<(), Error>>) -> Self {
            Self {
                tx: Some(tx),
                join_handle: Some(join_handle),
            }
        }

        pub async fn async_drop(&mut self) {
            let tx = self.tx.take();
            let handle = self.join_handle.take();
            let _ = tx.unwrap().send(());
            let _ = handle.unwrap().await;
        }
    }
}
