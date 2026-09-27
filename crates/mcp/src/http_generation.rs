//! Guards applied before the SDK HTTP worker can retain metadata or retry I/O.

use crate::{BoundedHttpClient, McpHttpError, McpHttpLimits, ProtocolVersion};
use futures_util::{StreamExt, stream::BoxStream};
use http::{HeaderName, HeaderValue};
use rmcp::{
    RoleClient,
    model::{ClientJsonRpcMessage, ClientRequest, RequestId, ServerJsonRpcMessage, ServerResult},
    transport::{
        StreamableHttpClientTransport, Transport,
        common::client_side_sse::{FixedInterval, NeverRetry},
        streamable_http_client::{
            StreamableHttpClient, StreamableHttpClientTransportConfig, StreamableHttpError,
            StreamableHttpPostResponse,
        },
    },
};
use sse_stream::{Error as SseError, Sse};
use std::{
    collections::HashMap,
    io::Write,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

/// Already-authorized, host-local inputs for one protocol-pinned HTTP generation.
/// No Debug/serialization: endpoint, headers and bearer token may be sensitive.
pub struct McpHttpGenerationConfig {
    pub endpoint: String,
    pub protocol: ProtocolVersion,
    pub io: McpHttpLimits,
    pub bearer_token: Option<String>,
    pub headers: HashMap<HeaderName, HeaderValue>,
    pub channel_capacity: NonZeroUsize,
    /// Maximum attempted POST/GET exchanges over this generation's lifetime.
    /// One separate DELETE attempt remains available for legacy cleanup.
    pub max_exchanges: NonZeroUsize,
    /// Cumulative encoded tools/list results admitted to the SDK. The SDK retains
    /// schemas across refreshes; charging every result conservatively bounds that
    /// cache even when names change. Repeated catalogues also consume this budget.
    pub max_catalog_lifetime_bytes: NonZeroUsize,
    /// Explicit reconnect delay for legacy SSE; None disables reconnects.
    /// Modern HTTP must use None. The exchange budget also bounds reconnects.
    pub legacy_resume_delay: Option<std::time::Duration>,
}

/// Build transport I/O only; the caller owns startup negotiation, durable scope,
/// invocation claims and retirement. No automatic downgrade or reinitialization.
/// The 2024 HTTP+SSE adapter is separate and is not supplied by this constructor.
pub fn http_generation_transport(
    config: McpHttpGenerationConfig,
) -> Result<impl Transport<RoleClient, Error = StreamableHttpError<McpHttpError>>, McpHttpError> {
    if config.protocol == ProtocolVersion::V20241105
        || config.protocol == ProtocolVersion::V20260728 && config.legacy_resume_delay.is_some()
    {
        return Err(McpHttpError::ProtocolViolation);
    }
    let inner = BoundedHttpClient::new(&config.endpoint, config.io)?;
    let guard = Arc::new(GenerationGuard {
        protocol: config.protocol,
        closed: AtomicBool::new(false),
        initialized: AtomicBool::new(false),
        deleted: AtomicBool::new(false),
        get_started: AtomicBool::new(false),
        legacy_resume: config.legacy_resume_delay.is_some(),
        exchanges: AtomicUsize::new(config.max_exchanges.get()),
        catalog_bytes: AtomicUsize::new(config.max_catalog_lifetime_bytes.get()),
    });
    let client = GenerationClient {
        inner,
        guard,
        max_event_bytes: config.io.max_event_bytes.get(),
    };
    let mut sdk = StreamableHttpClientTransportConfig::with_uri(config.endpoint);
    sdk.channel_buffer_capacity = config.channel_capacity.get();
    // The broker serializes ordinary invocations. The SDK reserves one separate
    // control slot so cancellation/replies can progress while an invocation waits.
    sdk.max_concurrent_requests = 1;
    sdk.control_request_timeout = config.io.request_timeout;
    sdk.session_recovery_timeout = config.io.request_timeout;
    sdk.reinit_on_expired_session = false;
    sdk.retry_config = Arc::new(NeverRetry::default());
    if let Some(delay) = config.legacy_resume_delay {
        let mut retry = FixedInterval::default();
        retry.duration = delay;
        retry.max_times = Some(config.max_exchanges.get());
        sdk.retry_config = Arc::new(retry);
    }
    sdk.max_sse_event_size = config.io.max_event_bytes.get();
    sdk.auth_header = config.bearer_token;
    sdk.custom_headers = config.headers;
    if sdk
        .custom_headers
        .contains_key(&HeaderName::from_static("mcp-protocol-version"))
    {
        return Err(McpHttpError::InvalidHeader);
    }
    sdk.custom_headers.insert(
        HeaderName::from_static("mcp-protocol-version"),
        HeaderValue::from_static(config.protocol.as_str()),
    );
    Ok(StreamableHttpClientTransport::with_client(client, sdk))
}

struct GenerationGuard {
    protocol: ProtocolVersion,
    closed: AtomicBool,
    initialized: AtomicBool,
    deleted: AtomicBool,
    get_started: AtomicBool,
    legacy_resume: bool,
    exchanges: AtomicUsize,
    catalog_bytes: AtomicUsize,
}
type HttpError = StreamableHttpError<McpHttpError>;
fn failure(error: McpHttpError) -> HttpError {
    StreamableHttpError::Client(error)
}

impl GenerationGuard {
    fn retire(&self, error: McpHttpError) -> HttpError {
        self.closed.store(true, Ordering::Release);
        failure(error)
    }
    fn admit(&self, headers: &HashMap<HeaderName, HeaderValue>) -> Result<(), HttpError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(failure(McpHttpError::GenerationClosed));
        }
        if headers
            .get(&HeaderName::from_static("mcp-protocol-version"))
            .and_then(|h| h.to_str().ok())
            != Some(self.protocol.as_str())
        {
            return Err(self.retire(McpHttpError::ProtocolViolation));
        }
        if self
            .exchanges
            .try_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                left.checked_sub(1)
            })
            .is_err()
        {
            return Err(self.retire(McpHttpError::GenerationLimit));
        }
        Ok(())
    }
    fn observe(&self, message: &ServerJsonRpcMessage) -> Result<(), HttpError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(failure(McpHttpError::GenerationClosed));
        }
        if let ServerJsonRpcMessage::Response(response) = message {
            if let ServerResult::InitializeResult(result) = &response.result {
                if self.protocol == ProtocolVersion::V20260728
                    || result.protocol_version.as_str() != self.protocol.as_str()
                {
                    return Err(self.retire(McpHttpError::ProtocolViolation));
                }
            }
            if let ServerResult::ListToolsResult(result) = &response.result {
                let mut counter = CountBytes {
                    count: 0,
                    limit: self.catalog_bytes.load(Ordering::Acquire),
                };
                if serde_json::to_writer(&mut counter, result).is_err()
                    || self
                        .catalog_bytes
                        .try_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                            left.checked_sub(counter.count)
                        })
                        .is_err()
                {
                    return Err(self.retire(McpHttpError::GenerationLimit));
                }
            }
        }
        Ok(())
    }
    fn finish<T>(&self, result: Result<T, HttpError>) -> Result<T, HttpError> {
        if result.is_err() {
            self.closed.store(true, Ordering::Release);
        }
        result
    }
    fn stream(
        self: &Arc<Self>,
        source: BoxStream<'static, Result<Sse, SseError>>,
        expected: Option<RequestId>,
    ) -> BoxStream<'static, Result<Sse, SseError>> {
        let guard = self.clone();
        futures_util::stream::unfold(
            (source, guard, expected, false, false),
            |(mut source, guard, expected, mut completed, ended)| async move {
                if ended {
                    return None;
                }
                match source.next().await {
                    Some(Ok(event)) => {
                        if let Some(data) = event.data.as_deref() {
                            let result = serde_json::from_str::<ServerJsonRpcMessage>(data)
                                .map_err(|_| guard.retire(McpHttpError::InvalidResponse))
                                .and_then(|message| {
                                    guard.observe(&message)?;
                                    let id = match &message {
                                        ServerJsonRpcMessage::Response(response) => {
                                            Some(&response.id)
                                        }
                                        ServerJsonRpcMessage::Error(error) => error.id.as_ref(),
                                        _ => None,
                                    };
                                    completed |= expected
                                        .as_ref()
                                        .is_some_and(|expected| id == Some(expected));
                                    Ok(())
                                });
                            if let Err(error) = result {
                                return Some((
                                    Err(SseError::Body(Box::new(error))),
                                    (source, guard, expected, completed, true),
                                ));
                            }
                        }
                        Some((Ok(event), (source, guard, expected, completed, false)))
                    }
                Some(Err(error)) => {
                    // Only a legacy network interruption can resume. Limit and
                    // parser failures are terminal, never reasons to read more.
                    let network = matches!(&error, SseError::Body(error) if error.downcast_ref::<McpHttpError>() == Some(&McpHttpError::Network));
                    if !guard.legacy_resume || !network {
                        guard.closed.store(true, Ordering::Release);
                    }
                        Some((Err(error), (source, guard, expected, completed, true)))
                    }
                    None => {
                        if guard.protocol == ProtocolVersion::V20260728
                            && expected.is_some()
                            && !completed
                        {
                            let error = guard.retire(McpHttpError::InvalidResponse);
                            return Some((
                                Err(SseError::Body(Box::new(error))),
                                (source, guard, expected, completed, true),
                            ));
                        }
                        None
                    }
                }
            },
        )
        .boxed()
    }
}

struct CountBytes {
    count: usize,
    limit: usize,
}
impl Write for CountBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.count) {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        self.count += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct GenerationClient {
    inner: BoundedHttpClient,
    guard: Arc<GenerationGuard>,
    max_event_bytes: usize,
}
impl StreamableHttpClient for GenerationClient {
    type Error = McpHttpError;
    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<StreamableHttpPostResponse, HttpError> {
        self.post_message_with_max_sse_event_size(
            uri,
            message,
            session,
            auth,
            headers,
            self.max_event_bytes,
        )
        .await
    }
    async fn post_message_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
        max: usize,
    ) -> Result<StreamableHttpPostResponse, HttpError> {
        self.guard.admit(&headers)?;
        if self.guard.protocol == ProtocolVersion::V20260728 && session.is_some() {
            return Err(self.guard.retire(McpHttpError::ProtocolViolation));
        }
        let expected = if let ClientJsonRpcMessage::Request(request) = &message {
            if let ClientRequest::InitializeRequest(initialize) = &request.request {
                if self.guard.protocol == ProtocolVersion::V20260728
                    || initialize.params.protocol_version.as_str() != self.guard.protocol.as_str()
                    || self.guard.initialized.swap(true, Ordering::AcqRel)
                {
                    return Err(self.guard.retire(McpHttpError::ProtocolViolation));
                }
            }
            Some(request.id.clone())
        } else {
            None
        };
        let response = self.guard.finish(
            self.inner
                .post_message_with_max_sse_event_size(uri, message, session, auth, headers, max)
                .await,
        )?;
        match response {
            StreamableHttpPostResponse::Json(message, session) => {
                if self.guard.protocol == ProtocolVersion::V20260728 && session.is_some() {
                    return Err(self.guard.retire(McpHttpError::ProtocolViolation));
                }
                // A request's JSON response must complete that exact request;
                // otherwise the SDK can leave it pending without any stream.
                let correlated = expected.as_ref().is_some_and(|expected| match &message {
                    ServerJsonRpcMessage::Response(response) => &response.id == expected,
                    ServerJsonRpcMessage::Error(error) => error.id.as_ref() == Some(expected),
                    _ => false,
                });
                if !correlated {
                    return Err(self.guard.retire(McpHttpError::ProtocolViolation));
                }
                self.guard.observe(&message)?;
                Ok(StreamableHttpPostResponse::Json(message, session))
            }
            StreamableHttpPostResponse::Sse(stream, session) => {
                if expected.is_none()
                    || self.guard.protocol == ProtocolVersion::V20260728 && session.is_some()
                {
                    return Err(self.guard.retire(McpHttpError::ProtocolViolation));
                }
                Ok(StreamableHttpPostResponse::Sse(
                    self.guard.stream(stream, expected),
                    session,
                ))
            }
            StreamableHttpPostResponse::Accepted if expected.is_none() => {
                Ok(StreamableHttpPostResponse::Accepted)
            }
            _ => Err(self.guard.retire(McpHttpError::ProtocolViolation)),
        }
    }
    async fn get_stream(
        &self,
        uri: Arc<str>,
        session: Option<Arc<str>>,
        last_event: Option<String>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, HttpError> {
        self.get_stream_with_max_sse_event_size(
            uri,
            session,
            last_event,
            auth,
            headers,
            self.max_event_bytes,
        )
        .await
    }
    async fn get_stream_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        session: Option<Arc<str>>,
        last_event: Option<String>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
        max: usize,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, HttpError> {
        // NeverRetry alone is insufficient in rmcp 3.4.1: server retry fields and
        // the first reconnect after a stream error can bypass that policy.
        if self.guard.protocol == ProtocolVersion::V20260728 {
            return Err(self.guard.retire(McpHttpError::ProtocolViolation));
        }
        if (self.guard.get_started.swap(true, Ordering::AcqRel) || last_event.is_some())
            && !self.guard.legacy_resume
        {
            return Err(self.guard.retire(McpHttpError::ProtocolViolation));
        }
        self.guard.admit(&headers)?;
        let result = self
            .inner
            .get_stream_with_max_sse_event_size(uri, session, last_event, auth, headers, max)
            .await;
        if matches!(result, Err(StreamableHttpError::ServerDoesNotSupportSse)) {
            return result;
        }
        self.guard
            .finish(result)
            .map(|stream| self.guard.stream(stream, None))
    }
    async fn delete_session(
        &self,
        uri: Arc<str>,
        session: Arc<str>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), HttpError> {
        if self.guard.protocol == ProtocolVersion::V20260728
            || self.guard.deleted.swap(true, Ordering::AcqRel)
        {
            return Err(failure(McpHttpError::ProtocolViolation));
        }
        // Cleanup remains possible after admission/response failure, once only.
        self.inner.delete_session(uri, session, auth, headers).await
    }
}
