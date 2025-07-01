use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use reqwest::{Response, StatusCode};
use reqwest_retry::{default_on_request_failure, default_on_request_success, Retryable};
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::RetryIf;
use tracing::{error, info};

use crate::constants::{CLIENT_RETRY_BASE_DELAY_MS, CLIENT_RETRY_MAX_ATTEMPTS};
use crate::error::CasClientError;
use crate::http_client::request_id_from_response;

#[derive(Debug)]
pub enum RetryableReqwestError {
    FatalError(CasClientError),
    RetryableError(CasClientError),
}

pub struct RetryWrapper {
    max_attempts: usize,
    base_delay_ms: u64,
    no_retry_on_429: bool,
    log_errors_as_info: bool,
    api_tag: &'static str,
}

impl RetryWrapper {
    pub fn new(api_tag: &'static str) -> Self {
        Self {
            max_attempts: *CLIENT_RETRY_MAX_ATTEMPTS,
            base_delay_ms: *CLIENT_RETRY_BASE_DELAY_MS,
            no_retry_on_429: false,
            log_errors_as_info: false,
            api_tag,
        }
    }

    pub fn with_max_attempts(mut self, attempts: usize) -> Self {
        self.max_attempts = attempts;
        self
    }

    pub fn with_base_delay_ms(mut self, delay: u64) -> Self {
        self.base_delay_ms = delay;
        self
    }

    pub fn with_429_no_retry(mut self) -> Self {
        self.no_retry_on_429 = true;
        self
    }

    pub fn log_errors_as_info(mut self) -> Self {
        self.log_errors_as_info = true;
        self
    }

    fn process_error_response(&self, try_idx: usize, err: reqwest_middleware::Error) -> RetryableReqwestError {
        let api = &self.api_tag;

        let process_error = |txt, log_as_info| {
            let msg = {
                if try_idx > 0 {
                    format!("{txt}: {api} api call failed (retry {try_idx}): {err}")
                } else {
                    format!("{txt}: {api} api call failed: {err}")
                }
            };

            if self.log_errors_as_info || log_as_info {
                info!("{msg}");
            } else {
                error!("{msg}");
            }

            // Turn this into a client connection error
            CasClientError::ClientConnectionError(msg)
        };

        // Here's the retry logic.
        match default_on_request_failure(&err) {
            Some(Retryable::Fatal) => {
                let cas_err = process_error("Fatal Client Error", false);
                RetryableReqwestError::FatalError(cas_err)
            },
            Some(Retryable::Transient) => {
                let cas_err = process_error("Retryable Client Error", true);
                RetryableReqwestError::RetryableError(cas_err)
            },
            None => {
                let cas_err = process_error("Unknown Client Error", true);
                RetryableReqwestError::FatalError(cas_err)
            },
        }
    }

    fn process_ok_response(&self, try_idx: usize, resp: Response) -> Result<Response, RetryableReqwestError> {
        let request_id = request_id_from_response(&resp).to_owned();

        let retry_str = if try_idx == 0 {
            String::default()
        } else {
            format!(", retry {try_idx}")
        };

        let api = &self.api_tag;

        // Log the errors and create a message string with the information in it to expose to users.
        let process_error = |context, err, log_as_info| {
            let msg = format!("{context}: {api:?} api call failed (request id {request_id}{retry_str}): {err}");

            if self.log_errors_as_info || log_as_info {
                info!("{msg}");
            } else {
                error!("{msg}");
            }
            CasClientError::ServerConnectionError(msg)
        };

        let retriability = default_on_request_success(&resp);

        match (resp.error_for_status(), retriability) {
            (Err(e), Some(Retryable::Fatal)) => {
                let cas_err = process_error("Fatal Server Error", e, false);
                Err(RetryableReqwestError::FatalError(cas_err))
            },
            (Err(e), Some(Retryable::Transient)) => {
                // Intercept the too many requests condition in the case of no retrying on 429.
                if e.status() == Some(StatusCode::TOO_MANY_REQUESTS) && self.no_retry_on_429 {
                    let cas_err = process_error("Too Many Requests (retry on 429 disabled)", e, false);
                    Err(RetryableReqwestError::FatalError(cas_err))
                } else {
                    let cas_err = process_error("Retryable Server Error", e, true);
                    Err(RetryableReqwestError::RetryableError(cas_err))
                }
            },
            (Err(e), None) => {
                // I don't believe this case should ever happen, but it's an external library
                // so let's handle it semigracefully.
                let cas_err = process_error("Unknown Server Error", e, false);
                Err(RetryableReqwestError::FatalError(cas_err))
            },

            (Ok(result), _) => {
                // Not an error, so just log a successful request.
                info!("Request Success: {api} api call succeeded (request id {request_id}{retry_str}).");
                Ok(result)
            },
        }
    }

    /// Run a connection and process the result, retrying on transient errors or if the process_fn returns a retryable
    /// error.
    ///
    /// The `make_request` function returns a future that resolves to a Result<Response> object as is returned by the
    /// client middleware.  For example, `|| client.clone().get(url).send()` returns a future (as `send()` is async)
    /// that will then be evaluatated to get the response.
    ///
    /// The process_fn takes a successful response and returns a future that evaluates that response.  A successful
    /// response is defined as the make_request future evaluating to an Ok() result and the enclosed Response having
    /// OK status. Given such a response, the process_fn returns a future that processes the response into an object
    /// of type `Result<T, RetryableRequestError>`
    ///
    /// RetryableRequestError is an enum containing either a FatalError, in which case it cannot be retried and is
    /// passed on, or a RetryableError, in which case the entire request is retried from the start.
    pub async fn run_and_process<T, ReqFut, ReqFn, ProcFut, ProcFn>(
        self,
        make_request: ReqFn,
        process_fn: ProcFn,
    ) -> Result<T, CasClientError>
    where
        ReqFn: Fn() -> ReqFut + Send + 'static,
        ReqFut: std::future::Future<Output = Result<Response, reqwest_middleware::Error>> + 'static,
        ProcFn: Fn(Response) -> ProcFut + Send + 'static,
        ProcFut: std::future::Future<Output = Result<T, RetryableReqwestError>> + 'static,
    {
        let strategy = ExponentialBackoff::from_millis(self.base_delay_ms)
            .map(jitter)
            .take(self.max_attempts);

        // Move self (which is consumable) into an arc that can be passed into this.
        // This allows the code to be a bit better.
        let self_ = Arc::new(self);
        let try_count = AtomicUsize::new(0);

        let retry_info = Arc::new((make_request, process_fn, try_count, self_.clone()));

        let result = RetryIf::spawn(
            strategy,
            move || {
                let retry_info = retry_info.clone();

                async move {
                    let (make_request, process_fn, try_count, self_) = retry_info.as_ref();

                    let resp_result = make_request().await;
                    let try_idx = try_count.fetch_add(1, Ordering::Relaxed);

                    // Process the result to check status codes for error conditions, and
                    // possibly retry.
                    let checked_result = match resp_result {
                        Err(e) => Err(self_.process_error_response(try_idx, e)),
                        Ok(resp) => self_.process_ok_response(try_idx, resp),
                    };

                    match checked_result {
                        Ok(ok_response) => process_fn(ok_response).await,
                        Err(e) => Err(e),
                    }
                }
            },
            |err: &RetryableReqwestError| matches!(err, RetryableReqwestError::RetryableError(_)),
        )
        .await;

        match result {
            Ok(r) => Ok(r),
            Err(RetryableReqwestError::FatalError(e)) => {
                // This would already have been logged, so no need to report on it here.
                Err(e)
            },
            Err(RetryableReqwestError::RetryableError(e)) => {
                // Log this here, as this is aborting things.
                if self_.log_errors_as_info {
                    info!("No more retries; aborting: {e}");
                } else {
                    error!("No more retries; aborting: {e}");
                }

                Err(e)
            },
        }
    }

    /// Run a connection and process the result as a json blob, retrying on transient errors or on issues parsing the
    /// json blob.
    ///
    /// The `make_request` function returns a future that resolves to a Result<Response> object as is returned by the
    /// client middleware.  For example, `|| client.clone().get(url).send()` returns a future (as `send()` is async)
    /// that will then be evaluatated to get the response.
    ///
    /// This functions acts just like the json() function on a client response, but retries the entire connection on
    /// transient errors.  
    pub async fn run_and_extract_json<ReqFut, ReqFn, JsonDest>(
        self,
        make_request: ReqFn,
    ) -> Result<JsonDest, CasClientError>
    where
        ReqFn: Fn() -> ReqFut + Send + 'static,
        ReqFut: std::future::Future<Output = Result<Response, reqwest_middleware::Error>> + 'static,
        JsonDest: for<'de> serde::Deserialize<'de>,
    {
        self.run_and_process(make_request, |resp: Response| {
            async move {
                // Extract the json from the final result.
                let r: Result<JsonDest, reqwest::Error> = resp.json().await;

                match r {
                    Ok(v) => Ok(v),
                    Err(e) => {
                        #[cfg(not(target_arch = "wasm32"))]
                        let is_connect = e.is_connect();
                        #[cfg(target_arch = "wasm32")]
                        let is_connect = false;

                        if is_connect || e.is_decode() || e.is_body() || e.is_timeout() {
                            // We got an incomplete or corrupted response from the server, possibly due to a dropped
                            // connection.  Presumably this error is transient.
                            Err(RetryableReqwestError::RetryableError(e.into()))
                        } else {
                            Err(RetryableReqwestError::FatalError(e.into()))
                        }
                    },
                }
            }
        })
        .await
    }

    /// Run a connection and process the result object, retrying on transient errors.
    ///
    /// The `make_request` function returns a future that resolves to a Result<Response> object as is returned by the
    /// client middleware.  For example, `|| client.clone().get(url).send()` returns a future (as `send()` is async)
    /// that will then be evaluatated to get the response.
    pub async fn run<ReqFut, ReqFn>(self, make_request: ReqFn) -> Result<Response, CasClientError>
    where
        ReqFn: Fn() -> ReqFut + Send + 'static,
        ReqFut: std::future::Future<Output = Result<Response, reqwest_middleware::Error>> + 'static,
    {
        // Just have the process_fn pass through the response.
        self.run_and_process(make_request, |resp| async move { Ok(resp) }).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
    use serde::{Deserialize, Serialize};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn connection_wrapper(api: &'static str) -> RetryWrapper {
        RetryWrapper::new(api).with_base_delay_ms(5).with_max_attempts(3)
    }

    fn make_client() -> ClientWithMiddleware {
        ClientBuilder::new(reqwest::Client::new()).build()
    }

    #[tokio::test]
    async fn test_success_first_try() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/success"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();

        let result = connection_wrapper("test_success_first_try")
            .run(move || {
                let url = format!("{}/success", server.uri());
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(&url).send()
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_then_success() {
        let server = MockServer::start().await;

        // First two return 500
        Mock::given(method("GET"))
            .and(path("/flaky"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(2)
            .mount(&server)
            .await;

        // Third returns 200
        Mock::given(method("GET"))
            .and(path("/flaky"))
            .respond_with(ResponseTemplate::new(200).set_body_string("Recovered"))
            .mount(&server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();

        let result = connection_wrapper("test_retry_then_success")
            .run(move || {
                let url = format!("{}/flaky", server.uri());
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(url).send()
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(&result.unwrap().bytes().await.unwrap()[..], b"Recovered");
        assert_eq!(counter.load(Ordering::SeqCst), 3); // handle() only called on retry attempts
    }

    #[tokio::test]
    async fn test_retry_limit_exceeded() {
        let server = MockServer::start().await;

        // Always return 500
        Mock::given(method("GET"))
            .and(path("/fail"))
            .respond_with(ResponseTemplate::new(500))
            .expect(4) // 1 initial + 3 retries
            .mount(&server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();

        let result = connection_wrapper("test_retry_limit_exceeded")
            .with_max_attempts(3)
            .run(move || {
                let url = format!("{}/fail", server.uri());
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(&url).send()
            })
            .await;

        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 4); // 3 retries attempted
    }

    #[tokio::test]
    async fn test_non_retryable_status() {
        let server = MockServer::start().await;

        // Respond with a 400 Bad Request
        Mock::given(method("GET"))
            .and(path("/bad"))
            .respond_with(ResponseTemplate::new(400))
            .expect(1)
            .mount(&server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();

        let result = connection_wrapper("test_non_retryable_status")
            .run(move || {
                let url = format!("{}/bad", server.uri());
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(&url).send()
            })
            .await;

        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 1); // strategy called once
    }

    #[tokio::test]
    async fn test_429_retry_if_specified() {
        // Ensures that 429 does in fact retry unless told not to.

        let server = MockServer::start().await;

        // Respond with a 429 too many requests
        Mock::given(method("GET"))
            .and(path("/bad"))
            .respond_with(ResponseTemplate::new(429))
            .expect(4)
            .mount(&server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();

        let result = connection_wrapper("test_429_retry_if_specified")
            .with_max_attempts(3)
            .run(move || {
                let url = format!("{}/bad", server.uri());
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(&url).send()
            })
            .await;

        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 4); // strategy called once
    }

    #[tokio::test]
    async fn test_429_retry_no_retry() {
        // Ensures that 429 does in fact retry unless told not to.

        let server = MockServer::start().await;

        // Respond with a 429 too many requests
        Mock::given(method("GET"))
            .and(path("/bad"))
            .respond_with(ResponseTemplate::new(429))
            .expect(1)
            .mount(&server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();

        let result = connection_wrapper("test_429_no_retry")
            .with_max_attempts(3)
            .with_429_no_retry()
            .run(move || {
                let url = format!("{}/bad", server.uri());
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(&url).send()
            })
            .await;

        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 1); // strategy called once
    }

    // Testing the JSON parsing
    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct JsonData {
        text: String,
        number: u64,
    }

    #[tokio::test]
    async fn test_json_reserialization() {
        // Ensures that 429 does in fact retry unless told not to.
        let data = JsonData {
            text: "test".into(),
            number: 42,
        };

        let server = MockServer::start().await;

        // Respond with a 429 too many requests
        Mock::given(method("GET"))
            .and(path("/bad"))
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_json(&data))
            .expect(1)
            .mount(&server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();

        let ret_data: JsonData = connection_wrapper("test_json")
            .run_and_extract_json(move || {
                let url = format!("{}/bad", server.uri());
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(&url).send()
            })
            .await
            .unwrap();

        assert_eq!(ret_data, data);
        assert_eq!(counter.load(Ordering::SeqCst), 1); // strategy called once
    }

    #[tokio::test]
    async fn test_json_unexpected_eof_retry() {
        // Ensures that 429 does in fact retry unless told not to.
        let data = JsonData {
            text: "test".into(),
            number: 42,
        };

        let json_data = serde_json::to_string(&data).unwrap();

        let server = MockServer::start().await;

        // Respond with a 429 too many requests
        Mock::given(method("GET"))
            .and(path("/json_flaky"))
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_string(&json_data[..json_data.len() - 5])) // Truncate to simulate unexpected EOF
            .up_to_n_times(1)
            .mount(&server)
            .await;

        // Respond with a 429 too many requests
        Mock::given(method("GET"))
            .and(path("/json_flaky"))
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_string(&json_data)) // Full length
            .expect(1)
            .mount(&server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();

        let ret_data: JsonData = connection_wrapper("test_json_unexpected_eof")
            .run_and_extract_json(move || {
                let url = format!("{}/json_flaky", server.uri());
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(&url).send()
            })
            .await
            .unwrap();

        assert_eq!(ret_data, data);
        assert_eq!(counter.load(Ordering::SeqCst), 2); // strategy called twice
    }
}
