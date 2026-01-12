use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use reqwest::{Error as ReqwestError, Response, StatusCode};
use reqwest_retry::{Retryable, default_on_request_success};
use tokio::sync::Mutex;
use tokio_retry::RetryIf;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tracing::{error, info};
use xet_runtime::xet_config;

use crate::adaptive_concurrency::ConnectionPermit;
use crate::error::CasClientError;
use crate::http_client::request_id_from_response;

#[derive(Debug)]
pub enum RetryableReqwestError {
    FatalError(CasClientError),
    RetryableError(CasClientError),
}

struct ConnectionPermitInfo {
    permit: Option<ConnectionPermit>,
    transfer_size_if_known: Option<u64>,
}

pub struct RetryWrapper {
    max_attempts: usize,
    base_delay: Duration,
    no_retry_on_429: bool,
    retry_on_403: bool,
    log_errors_as_info: bool,
    api_tag: &'static str,
    connection_permit: Option<Mutex<ConnectionPermitInfo>>,
}

impl RetryWrapper {
    pub fn new(api_tag: &'static str) -> Self {
        Self {
            max_attempts: xet_config().client.retry_max_attempts,
            base_delay: xet_config().client.retry_base_delay,
            no_retry_on_429: false,
            retry_on_403: false,
            log_errors_as_info: false,
            api_tag,
            connection_permit: None,
        }
    }

    pub fn with_max_attempts(mut self, attempts: usize) -> Self {
        self.max_attempts = attempts;
        self
    }

    pub fn with_base_delay(mut self, delay: Duration) -> Self {
        self.base_delay = delay;
        self
    }

    pub fn with_429_no_retry(mut self) -> Self {
        self.no_retry_on_429 = true;
        self
    }

    pub fn with_retry_on_403(mut self) -> Self {
        self.retry_on_403 = true;
        self
    }

    pub fn log_errors_as_info(mut self) -> Self {
        self.log_errors_as_info = true;
        self
    }

    pub fn with_connection_permit(mut self, permit: ConnectionPermit, transfer_size_if_known: Option<u64>) -> Self {
        self.connection_permit = Some(Mutex::new(ConnectionPermitInfo {
            permit: Some(permit),
            transfer_size_if_known,
        }));
        self
    }

    fn process_error_response(&self, try_idx: usize, err: reqwest_middleware::Error) -> RetryableReqwestError {
        let api = &self.api_tag;

        let process_error = |txt, log_as_info, err: reqwest_middleware::Error| {
            let msg = {
                if try_idx > 0 {
                    format!("{txt}: {api} api call failed (retry {try_idx}): {err}")
                } else {
                    format!("{txt}: {api} api call failed: {err}")
                }
            };

            info!(api = self.api_tag, "Connection attempt {}/{}", try_idx + 1, self.max_attempts);

            if self.log_errors_as_info || log_as_info {
                info!("{msg}");
            } else {
                error!("{msg}");
            }

            CasClientError::from(err)
        };

        // Here's the retry logic.
        match on_request_failure(&err) {
            Some(Retryable::Fatal) => {
                let cas_err = process_error("Fatal Client Error", false, err);
                RetryableReqwestError::FatalError(cas_err)
            },
            Some(Retryable::Transient) => {
                let cas_err = process_error("Retryable Client Error", true, err);
                RetryableReqwestError::RetryableError(cas_err)
            },
            None => {
                let cas_err = process_error("Unknown Client Error", true, err);
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
        let process_error = |context, err: ReqwestError, log_as_info| {
            if self.log_errors_as_info || log_as_info {
                info!("{context}: {api:?} api call failed (request id {request_id}{retry_str}): {err}");
            } else {
                error!("{context}: {api:?} api call failed (request id {request_id}{retry_str}): {err}");
            }
            CasClientError::from(err)
        };

        let retriability = default_on_request_success(&resp);

        match (resp.error_for_status(), retriability) {
            (Err(e), Some(Retryable::Fatal)) => {
                // Intercept the forbidden condition if retry on 403 is enabled.
                if e.status() == Some(StatusCode::FORBIDDEN) && self.retry_on_403 {
                    let cas_err = process_error("Retry on 403 (Forbidden) enabled)", e, true);
                    Err(RetryableReqwestError::RetryableError(cas_err))
                } else {
                    let cas_err = process_error("Fatal Error", e, false);
                    Err(RetryableReqwestError::FatalError(cas_err))
                }
            },
            (Err(e), Some(Retryable::Transient)) => {
                // Intercept the too many requests condition in the case of no retrying on 429.
                if e.status() == Some(StatusCode::TOO_MANY_REQUESTS) && self.no_retry_on_429 {
                    let cas_err = process_error("Too Many Requests (retry on 429 disabled)", e, false);
                    Err(RetryableReqwestError::FatalError(cas_err))
                } else {
                    let cas_err = process_error("Retryable Error", e, true);
                    Err(RetryableReqwestError::RetryableError(cas_err))
                }
            },
            (Err(e), None) => {
                // I don't believe this case should ever happen, but it's an external library
                // so let's handle it semi-gracefully.
                let cas_err = process_error("Unknown Error", e, false);
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
    /// The `make_request` function takes an optional partial progress reporting function and returns a future that
    /// resolves to a Result<Response> object as is returned by the client middleware.  For example,
    /// `|report_fn| client.clone().get(url).send()` returns a future (as `send()` is async) that will then be
    /// evaluated to get the response.
    ///
    /// The process_fn takes a successful response and returns a future that evaluates that response.  A successful
    /// response is defined as the make_request future evaluating to an Ok() result and the enclosed Response having
    /// OK status. Given such a response, the process_fn returns a future that processes the response into an object
    /// of type `Result<T, RetryableRequestError>`
    ///
    /// RetryableRequestError is an enum containing either a FatalError, in which case it cannot be retried and is
    /// passed on, or a RetryableError, in which case the entire request is retried from the start.
    ///
    /// If a connection permit is provided, a partial progress reporting function will be extracted from it and passed
    /// to `make_request`. This function can be used to report partial progress during the request.
    pub async fn run_and_process<T, ReqFut, ReqFn, ProcFut, ProcFn>(
        self,
        make_request: ReqFn,
        process_fn: ProcFn,
    ) -> Result<T, CasClientError>
    where
        ReqFn: Fn(Option<Arc<dyn Fn(f64, u64) + Send + Sync>>) -> ReqFut + Send + Sync + 'static,
        ReqFut: std::future::Future<Output = Result<Response, reqwest_middleware::Error>> + 'static,
        ProcFn: Fn(Response) -> ProcFut + Send + 'static,
        ProcFut: Future<Output = Result<T, RetryableReqwestError>> + 'static,
    {
        let strategy = ExponentialBackoff::from_millis(self.base_delay.as_millis().min(u64::MAX as u128) as u64)
            .map(jitter)
            .take(self.max_attempts);

        info!(
            max_attempts = self.max_attempts,
            base_delay=?self.base_delay,
            no_retry_on_429=self.no_retry_on_429,
            "Retry strategy",
        );

        // Move self (which is consumable) into an arc that can be passed into this.
        // This allows the code to be a bit better.
        let self_ = Arc::new(self);
        let try_count = AtomicUsize::new(0);

        // Extract the partial progress reporting function from the connection permit if it exists
        let partial_report_fn = if let Some(permit_holder) = &self_.connection_permit {
            permit_holder
                .lock()
                .await
                .permit
                .as_ref()
                .map(|p| p.get_partial_completion_reporting_function())
        } else {
            None
        };

        let retry_info = Arc::new((make_request, process_fn, try_count, self_.clone(), partial_report_fn.clone()));

        let result = RetryIf::spawn(
            strategy,
            move || {
                let retry_info = retry_info.clone();

                async move {
                    let (make_request, process_fn, try_count, self_, partial_report_fn) = retry_info.as_ref();

                    if let Some(p) = &self_.connection_permit
                        && let Some(p) = p.lock().await.permit.as_ref()
                    {
                        p.transfer_starting().await;
                    }

                    let resp_result = make_request(partial_report_fn.clone()).await;
                    let try_idx = try_count.fetch_add(1, Ordering::Relaxed);

                    // Process the result to check status codes for error conditions, and
                    // possibly retry.
                    let checked_result = match resp_result {
                        Err(e) => Err(self_.process_error_response(try_idx, e)),
                        Ok(resp) => self_.process_ok_response(try_idx, resp),
                    };

                    // reply_bytes is ignored if the size was specified earlier, as in the case for upload.
                    let (reply_bytes, processing_result) = match checked_result {
                        Ok(ok_response) => {
                            // Got an okay response, so now we can run the processing function.
                            let reply_bytes = ok_response.content_length().unwrap_or(0);
                            let prosess_fn_result = process_fn(ok_response).await;
                            (reply_bytes, prosess_fn_result)
                        },
                        Err(e) => (0, Err(e)),
                    };

                    // Now, possibly adjust the connection permit.
                    if let Some(permit_holder) = &self_.connection_permit {
                        let mut permit_info = permit_holder.lock().await;

                        match &processing_result {
                            Ok(_) => {
                                if let Some(permit) = permit_info.permit.take() {
                                    permit
                                        .report_completion(
                                            permit_info.transfer_size_if_known.unwrap_or(reply_bytes),
                                            true,
                                        )
                                        .await;
                                }
                            },
                            Err(RetryableReqwestError::FatalError(_)) => {
                                if let Some(permit) = permit_info.permit.take() {
                                    permit.report_completion(0, false).await;
                                }
                            },
                            Err(RetryableReqwestError::RetryableError(_)) => {
                                if let Some(permit) = permit_info.permit.as_ref() {
                                    permit.report_retryable_failure().await;
                                }
                            },
                        }
                    }

                    processing_result
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
                // Retries exhausted - report failure on the permit if present
                if let Some(permit_holder) = &self_.connection_permit {
                    let mut permit_info = permit_holder.lock().await;
                    if let Some(permit) = permit_info.permit.take() {
                        permit.report_completion(0, false).await;
                    }
                }

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
    /// The `make_request` function takes an optional partial progress reporting function and returns a future that
    /// resolves to a Result<Response> object as is returned by the client middleware.  For example,
    /// `|partial_report_fn| client.clone().get(url).send()` returns a future (as `send()` is async) that will then be
    /// evaluated to get the response.
    ///
    /// This functions acts just like the json() function on a client response, but retries the entire connection on
    /// transient errors.
    pub async fn run_and_extract_json<JsonDest, ReqFn, ReqFut>(
        self,
        make_request: ReqFn,
    ) -> Result<JsonDest, CasClientError>
    where
        JsonDest: for<'de> serde::Deserialize<'de>,
        ReqFn: Fn(Option<Arc<dyn Fn(f64, u64) + Send + Sync>>) -> ReqFut + Send + Sync + 'static,
        ReqFut: std::future::Future<Output = Result<Response, reqwest_middleware::Error>> + 'static,
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

    /// Run a connection and process the result as bytes, retrying on transient errors or on issues not getting the
    /// full object.
    ///
    /// The `make_request` function returns a future that resolves to a Result<Response> object as is returned by the
    /// client middleware.  For example, `|| client.clone().get(url).send()` returns a future (as `send()` is async)
    /// that will then be evaluatated to get the response.
    ///
    /// This functions acts just like the json() function on a client response, but retries the entire connection on
    /// transient errors.  
    pub async fn run_and_extract_bytes<ReqFut, ReqFn>(self, make_request: ReqFn) -> Result<Bytes, CasClientError>
    where
        ReqFn: Fn(Option<Arc<dyn Fn(f64, u64) + Send + Sync>>) -> ReqFut + Send + Sync + 'static,
        ReqFut: std::future::Future<Output = Result<Response, reqwest_middleware::Error>> + 'static,
    {
        self.run_and_process(make_request, |resp: Response| {
            async move {
                // Extract the bytes from the final result.
                let r: Result<Bytes, reqwest::Error> = resp.bytes().await;

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

    /// Run a connection and process the result as a using the specified function parameter blob,
    /// retrying on transient errors or on issues parsing the blob with the custom function.
    ///
    /// The `make_request` function returns a future that resolves to a Result<Response> object as is returned by the
    /// client middleware.  For example, `|| client.clone().get(url).send()` returns a future (as `send()` is async)
    /// that will then be evaluated to get the response.
    ///
    /// The `parse` function is a custom parser that takes a Response and returns a future that resolves to
    /// Result<Dest (your desired output type), ParseErr>. This allows for custom parsing logic beyond simple JSON
    /// deserialization.
    ///
    /// The `map_parse_err` function maps parsing errors to RetryableReqwestError, determining whether parsing
    /// failures should be treated as fatal or retryable error
    pub async fn run_and_extract_custom<ReqFn, ReqFut, Parse, ParseFut, Dest>(
        self,
        make_request: ReqFn,
        parse: Parse,
    ) -> Result<Dest, CasClientError>
    where
        ReqFn: Fn(Option<Arc<dyn Fn(f64, u64) + Send + Sync>>) -> ReqFut + Send + Sync + 'static,
        ReqFut: std::future::Future<Output = Result<Response, reqwest_middleware::Error>> + 'static,
        Parse: Fn(Response) -> ParseFut + Send + Sync + 'static,
        ParseFut: std::future::Future<Output = Result<Dest, RetryableReqwestError>> + 'static,
    {
        self.run_and_process(make_request, parse).await
    }

    /// Run a connection and process the result object, retrying on transient errors.
    ///
    /// The `make_request` function returns a future that resolves to a Result<Response> object as is returned by the
    /// client middleware.  For example, `|| client.clone().get(url).send()` returns a future (as `send()` is async)
    /// that will then be evaluated to get the response.
    pub async fn run<ReqFut, ReqFn>(self, make_request: ReqFn) -> Result<Response, CasClientError>
    where
        ReqFn: Fn(Option<Arc<dyn Fn(f64, u64) + Send + Sync>>) -> ReqFut + Send + Sync + 'static,
        ReqFut: std::future::Future<Output = Result<Response, reqwest_middleware::Error>> + 'static,
    {
        // Just have the process_fn pass through the response.
        self.run_and_process(make_request, |resp| async move { Ok(resp) }).await
    }
}

/// Like [request_middleware::default_on_request_failure], but retries all IOErrors instead of a
/// subset. There are a few errors that can occur on certain systems that we will want to retry
/// (e.g. `No buffer space available: (os error 55)`).
///
/// Unfortunately, those errors don't translate to a defined [std::io::ErrorKind], and are thus
/// not able to be effectively filtered.
pub fn on_request_failure(error: &reqwest_middleware::Error) -> Option<Retryable> {
    let reqwest_middleware::Error::Reqwest(error) = error else {
        // If something fails in the middleware we're screwed.
        return Some(Retryable::Fatal);
    };
    // reqwest error
    #[cfg(not(target_arch = "wasm32"))]
    let is_connect = error.is_connect();
    #[cfg(target_arch = "wasm32")]
    let is_connect = false;
    if error.is_timeout() || is_connect {
        Some(Retryable::Transient)
    } else if error.is_body() || error.is_decode() || error.is_builder() || error.is_redirect() {
        Some(Retryable::Fatal)
    } else if error.is_request() {
        // It seems that hyper::Error(IncompleteMessage) is not correctly handled by reqwest.
        // Here we check if the Reqwest error was originated by hyper and map it consistently.
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(hyper_error) = get_source_error_type::<hyper::Error>(&error) {
            // The hyper::Error(IncompleteMessage) is raised if the HTTP response is well formatted but does not
            // contain all the bytes. This can happen when the server has started sending
            // back the response but the connection is cut halfway through. We can safely
            // retry the call, hence marking this error as [`Retryable::Transient`]. Instead
            // hyper::Error(Canceled) is raised when the connection is gracefully closed on
            // the server side.
            let is_io_error = get_source_error_type::<std::io::Error>(hyper_error).is_some();
            if hyper_error.is_incomplete_message() || hyper_error.is_canceled() || is_io_error {
                Some(Retryable::Transient)
            } else {
                Some(Retryable::Fatal)
            }
        } else {
            Some(Retryable::Fatal)
        }
        #[cfg(target_arch = "wasm32")]
        Some(Retryable::Fatal)
    } else {
        // We omit checking if error.is_status() since we check that already.
        // However, if Response::error_for_status is used the status will still
        // remain in the response object.
        None
    }
}

/// Downcasts the given err source into T.
///
/// Note: copied from [request_middleware::get_source_error_type] since that is not
/// publicly exported.
#[cfg(not(target_arch = "wasm32"))]
fn get_source_error_type<T: std::error::Error + 'static>(err: &dyn std::error::Error) -> Option<&T> {
    let mut source = err.source();

    while let Some(err) = source {
        if let Some(err) = err.downcast_ref::<T>() {
            return Some(err);
        }

        source = err.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
    use serde::{Deserialize, Serialize};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn connection_wrapper(api: &'static str) -> RetryWrapper {
        RetryWrapper::new(api)
            .with_base_delay(Duration::from_millis(5))
            .with_max_attempts(3)
    }

    fn make_client() -> ClientWithMiddleware {
        ClientBuilder::new(reqwest::Client::new()).build()
    }

    async fn check_success_first_try(server: &MockServer) {
        let _guard = Mock::given(method("GET"))
            .and(path("/success"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount_as_scoped(server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();
        let server_uri = server.uri();

        let result = connection_wrapper("check_success_first_try")
            .run(move |_partial_report_fn| {
                let url = format!("{}/success", server_uri);
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(&url).send()
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    async fn check_retry_then_success(server: &MockServer) {
        // First two return 500
        let _guard1 = Mock::given(method("GET"))
            .and(path("/flaky"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(2)
            .mount_as_scoped(server)
            .await;

        // Third returns 200
        let _guard2 = Mock::given(method("GET"))
            .and(path("/flaky"))
            .respond_with(ResponseTemplate::new(200).set_body_string("Recovered"))
            .mount_as_scoped(server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();
        let server_uri = server.uri();

        let result = connection_wrapper("check_retry_then_success")
            .run(move |_partial_report_fn| {
                let url = format!("{}/flaky", server_uri);
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(url).send()
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(&result.unwrap().bytes().await.unwrap()[..], b"Recovered");
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    async fn check_retry_limit_exceeded(server: &MockServer) {
        // Always return 500
        let _guard = Mock::given(method("GET"))
            .and(path("/fail"))
            .respond_with(ResponseTemplate::new(500))
            .expect(4) // 1 initial + 3 retries
            .mount_as_scoped(server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();
        let server_uri = server.uri();

        let result = connection_wrapper("check_retry_limit_exceeded")
            .with_max_attempts(3)
            .run(move |_partial_report_fn| {
                let url = format!("{}/fail", server_uri);
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(&url).send()
            })
            .await;

        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 4);
    }

    async fn check_non_retryable_status(server: &MockServer) {
        // Respond with a 400 Bad Request
        let _guard = Mock::given(method("GET"))
            .and(path("/bad_request"))
            .respond_with(ResponseTemplate::new(400))
            .expect(1)
            .mount_as_scoped(server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();
        let server_uri = server.uri();

        let result = connection_wrapper("check_non_retryable_status")
            .run(move |_partial_report_fn| {
                let url = format!("{}/bad_request", server_uri);
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(&url).send()
            })
            .await;

        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    async fn check_429_retry_if_specified(server: &MockServer) {
        // Respond with a 429 too many requests
        let _guard = Mock::given(method("GET"))
            .and(path("/rate_limit"))
            .respond_with(ResponseTemplate::new(429))
            .expect(4)
            .mount_as_scoped(server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();
        let server_uri = server.uri();

        let result = connection_wrapper("check_429_retry_if_specified")
            .with_max_attempts(3)
            .run(move |_partial_report_fn| {
                let url = format!("{}/rate_limit", server_uri);
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(&url).send()
            })
            .await;

        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 4);
    }

    async fn check_429_no_retry(server: &MockServer) {
        // Respond with a 429 too many requests
        let _guard = Mock::given(method("GET"))
            .and(path("/rate_limit_no_retry"))
            .respond_with(ResponseTemplate::new(429))
            .expect(1)
            .mount_as_scoped(server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();
        let server_uri = server.uri();

        let result = connection_wrapper("check_429_no_retry")
            .with_max_attempts(3)
            .with_429_no_retry()
            .run(move |_partial_report_fn| {
                let url = format!("{}/rate_limit_no_retry", server_uri);
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(&url).send()
            })
            .await;

        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct JsonData {
        text: String,
        number: u64,
    }

    async fn check_json_reserialization(server: &MockServer) {
        let data = JsonData {
            text: "test".into(),
            number: 42,
        };

        let _guard = Mock::given(method("GET"))
            .and(path("/json"))
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_json(&data))
            .expect(1)
            .mount_as_scoped(server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();
        let server_uri = server.uri();

        let ret_data: JsonData = connection_wrapper("check_json_reserialization")
            .run_and_extract_json(move |_partial_report_fn| {
                let url = format!("{}/json", server_uri);
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(&url).send()
            })
            .await
            .unwrap();

        assert_eq!(ret_data, data);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    async fn check_json_unexpected_eof_retry(server: &MockServer) {
        let data = JsonData {
            text: "test".into(),
            number: 42,
        };

        let json_data = serde_json::to_string(&data).unwrap();

        // First response truncated to simulate unexpected EOF
        let _guard1 = Mock::given(method("GET"))
            .and(path("/json_flaky"))
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_string(&json_data[..json_data.len() - 5]))
            .up_to_n_times(1)
            .mount_as_scoped(server)
            .await;

        // Second response with full data
        let _guard2 = Mock::given(method("GET"))
            .and(path("/json_flaky"))
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_string(&json_data))
            .expect(1)
            .mount_as_scoped(server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();
        let server_uri = server.uri();

        let ret_data: JsonData = connection_wrapper("check_json_unexpected_eof_retry")
            .run_and_extract_json(move |_partial_report_fn| {
                let url = format!("{}/json_flaky", server_uri);
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(&url).send()
            })
            .await
            .unwrap();

        assert_eq!(ret_data, data);
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_retry_wrapper() {
        let server = MockServer::start().await;

        // To avoid "Too many open files" error, we start one server here
        // and then have each check below use the same server with scoped
        // mocks.  When running each of these tests on its own, it seemed
        // we would hit sporadic "Too many open files" errors when the
        // wiremock server was starting.

        check_success_first_try(&server).await;
        check_retry_then_success(&server).await;
        check_retry_limit_exceeded(&server).await;
        check_non_retryable_status(&server).await;
        check_429_retry_if_specified(&server).await;
        check_429_no_retry(&server).await;
        check_json_reserialization(&server).await;
        check_json_unexpected_eof_retry(&server).await;
    }

    #[tokio::test]
    async fn test_403_no_retry_by_default() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/forbidden"))
            .respond_with(ResponseTemplate::new(403))
            .expect(1)
            .mount(&server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();

        let result = connection_wrapper("test_403_no_retry_by_default")
            .with_max_attempts(3)
            .run(move |_partial_report_fn| {
                let url = format!("{}/forbidden", server.uri());
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(&url).send()
            })
            .await;

        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_403_retry_when_enabled() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/forbidden"))
            .respond_with(ResponseTemplate::new(403))
            .expect(4)
            .mount(&server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();

        let result = connection_wrapper("test_403_retry_when_enabled")
            .with_max_attempts(3)
            .with_retry_on_403()
            .run(move |_partial_report_fn| {
                let url = format!("{}/forbidden", server.uri());
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(&url).send()
            })
            .await;

        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn test_403_retry_then_success() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/forbidden_then_ok"))
            .respond_with(ResponseTemplate::new(403))
            .up_to_n_times(2)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/forbidden_then_ok"))
            .respond_with(ResponseTemplate::new(200).set_body_string("Success"))
            .mount(&server)
            .await;

        let client = make_client();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_ = counter.clone();

        let result = connection_wrapper("test_403_retry_then_success")
            .with_max_attempts(3)
            .with_retry_on_403()
            .run(move |_partial_report_fn| {
                let url = format!("{}/forbidden_then_ok", server.uri());
                counter_.fetch_add(1, Ordering::Relaxed);
                client.clone().get(&url).send()
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(&result.unwrap().bytes().await.unwrap()[..], b"Success");
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }
}
