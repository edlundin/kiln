//! HTTP I/O only. This adapter grants no launch, credential or invocation authority.

use std::{collections::HashMap, io::Write, num::NonZeroUsize, sync::Arc, time::Duration};

use futures_util::{StreamExt, stream::BoxStream};
use http::{HeaderName, HeaderValue};
use reqwest::{Method, StatusCode, Url, header};
use rmcp::{
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
    transport::streamable_http_client::{
        StreamableHttpClient, StreamableHttpError, StreamableHttpPostResponse,
    },
};
use sse_stream::{Error as SseError, Sse};

/// Host-owned budgets, chosen for the server workload and permitted latency.
#[derive(Clone, Copy)]
pub struct McpHttpLimits {
    pub max_request_bytes: NonZeroUsize,
    pub max_response_bytes: NonZeroUsize,
    /// Whole SSE response byte allowance, including comments and unknown fields.
    pub max_stream_bytes: NonZeroUsize,
    /// Per-event bytes including fields/comments; CRLF counts as one terminator.
    pub max_event_bytes: NonZeroUsize,
    /// Sum of header names and values accepted/sent by this adapter.
    /// The HTTP stack parses response headers before this application check.
    pub max_header_bytes: NonZeroUsize,
    pub request_timeout: Duration,
}

/// Sanitized transport errors never retain response bodies, URLs or header values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum McpHttpError {
    InvalidEndpoint,
    InvalidLimits,
    EndpointMismatch,
    InvalidHeader,
    HeaderLimit,
    BodyLimit,
    InvalidResponse,
    HttpStatus(u16),
    Network,
    GenerationClosed,
    GenerationLimit,
    ProtocolViolation,
}

impl std::fmt::Display for McpHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HttpStatus(status) => write!(f, "MCP HTTP status {status}"),
            error => write!(f, "MCP HTTP {error:?}"),
        }
    }
}
impl std::error::Error for McpHttpError {}

type HttpError = StreamableHttpError<McpHttpError>;
fn failure(error: McpHttpError) -> HttpError {
    StreamableHttpError::Client(error)
}

/// One fixed endpoint, no redirects, ambient proxies or automatic HTTP retries.
/// SDK session recovery/reconnect policy belongs to the eventual lifecycle owner;
/// constructing an SDK worker around this adapter does not disable its retries.
/// No Debug implementation: the endpoint may contain private query parameters.
#[derive(Clone)]
pub struct BoundedHttpClient {
    endpoint: Url,
    client: reqwest::Client,
    limits: McpHttpLimits,
}

impl BoundedHttpClient {
    pub fn new(endpoint: &str, limits: McpHttpLimits) -> Result<Self, McpHttpError> {
        let endpoint = Url::parse(endpoint).map_err(|_| McpHttpError::InvalidEndpoint)?;
        let loopback = endpoint.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && loopback)
            || endpoint.host().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(McpHttpError::InvalidEndpoint);
        }
        if limits.request_timeout.is_zero() {
            return Err(McpHttpError::InvalidLimits);
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .timeout(limits.request_timeout)
            .build()
            .map_err(|_| McpHttpError::Network)?;
        Ok(Self {
            endpoint,
            client,
            limits,
        })
    }

    fn request(
        &self,
        method: Method,
        uri: &str,
        session: Option<&str>,
        auth: Option<String>,
        custom: HashMap<HeaderName, HeaderValue>,
        last_event: Option<String>,
    ) -> Result<reqwest::RequestBuilder, HttpError> {
        if Url::parse(uri).ok().as_ref() != Some(&self.endpoint) {
            return Err(failure(McpHttpError::EndpointMismatch));
        }
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static("application/json, text/event-stream"),
        );
        if method == Method::POST {
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
        }
        if let Some(session) = session {
            if !valid_session(session) {
                return Err(failure(McpHttpError::InvalidHeader));
            }
            headers.insert(
                "mcp-session-id",
                HeaderValue::from_str(session).map_err(|_| failure(McpHttpError::InvalidHeader))?,
            );
        }
        if let Some(auth) = auth {
            if auth.len() > self.limits.max_header_bytes.get().saturating_sub(7) {
                return Err(failure(McpHttpError::HeaderLimit));
            }
            let mut value = HeaderValue::from_str(&format!("Bearer {auth}"))
                .map_err(|_| failure(McpHttpError::InvalidHeader))?;
            value.set_sensitive(true);
            headers.insert(header::AUTHORIZATION, value);
        }
        if let Some(last_event) = last_event {
            headers.insert(
                "last-event-id",
                HeaderValue::from_str(&last_event)
                    .map_err(|_| failure(McpHttpError::InvalidHeader))?,
            );
        }
        for (name, mut value) in custom {
            // These are owned by HTTP framing, endpoint selection or this adapter.
            // Protocol-version and modern MCP request/parameter headers remain allowed.
            if matches!(
                name.as_str(),
                "host"
                    | "authorization"
                    | "proxy-authorization"
                    | "cookie"
                    | "accept"
                    | "content-type"
                    | "content-length"
                    | "transfer-encoding"
                    | "connection"
                    | "trailer"
                    | "upgrade"
                    | "te"
                    | "mcp-session-id"
                    | "last-event-id"
            ) {
                return Err(failure(McpHttpError::InvalidHeader));
            }
            value.set_sensitive(true);
            headers.insert(name, value);
        }
        self.check_headers(&headers)?;
        Ok(self
            .client
            .request(method, self.endpoint.clone())
            .headers(headers))
    }

    fn check_headers(&self, headers: &header::HeaderMap) -> Result<(), HttpError> {
        let mut remaining = self.limits.max_header_bytes.get();
        for (name, value) in headers {
            remaining = remaining
                .checked_sub(name.as_str().len())
                .and_then(|n| n.checked_sub(value.as_bytes().len()))
                .ok_or_else(|| failure(McpHttpError::HeaderLimit))?;
        }
        Ok(())
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> Result<reqwest::Response, HttpError> {
        let response = request
            .send()
            .await
            .map_err(|_| failure(McpHttpError::Network))?;
        self.check_headers(response.headers())?;
        Ok(response)
    }

    pub(crate) async fn post(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        auth: Option<String>,
        custom: HashMap<HeaderName, HeaderValue>,
        event_limit: usize,
    ) -> Result<(StreamableHttpPostResponse, StatusCode), HttpError> {
        let mut body = BoundedBody {
            bytes: Vec::new(),
            limit: self.limits.max_request_bytes.get(),
        };
        serde_json::to_writer(&mut body, &message).map_err(|_| failure(McpHttpError::BodyLimit))?;
        let request = self
            .request(Method::POST, &uri, session.as_deref(), auth, custom, None)?
            .body(body.bytes);
        let response = self.send(request).await?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND && session.is_some() {
            return Err(StreamableHttpError::SessionExpired);
        }
        if status == StatusCode::ACCEPTED {
            return Ok((StreamableHttpPostResponse::Accepted, status));
        }
        let session = response
            .headers()
            .get("mcp-session-id")
            .map(|value| {
                value
                    .to_str()
                    .ok()
                    .filter(|s| valid_session(s))
                    .map(str::to_owned)
                    .ok_or_else(|| failure(McpHttpError::InvalidHeader))
            })
            .transpose()?;
        if content_type(&response, "text/event-stream") && status.is_success() {
            return Ok((
                StreamableHttpPostResponse::Sse(
                    crate::http_sse::response_stream(
                        response,
                        event_limit.min(self.limits.max_event_bytes.get()),
                        self.limits.max_stream_bytes.get(),
                    ),
                    session,
                ),
                status,
            ));
        }
        if !content_type(&response, "application/json") {
            return Err(failure(if status.is_success() {
                McpHttpError::InvalidResponse
            } else {
                McpHttpError::HttpStatus(status.as_u16())
            }));
        }
        let bytes = read_json(response, self.limits.max_response_bytes.get()).await?;
        let parsed: ServerJsonRpcMessage =
            serde_json::from_slice(&bytes).map_err(|_| failure(McpHttpError::InvalidResponse))?;
        if !status.is_success() {
            // Preserve only a genuinely correlated protocol error. Never synthesize
            // a discovery rejection or rewrite an untrusted response ID for downgrade.
            let correlated = matches!((&message, &parsed), (ClientJsonRpcMessage::Request(request), ServerJsonRpcMessage::Error(error)) if error.id.as_ref() == Some(&request.id));
            if !correlated || matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
                return Err(failure(McpHttpError::HttpStatus(status.as_u16())));
            }
        }
        Ok((StreamableHttpPostResponse::Json(parsed, session), status))
    }

    async fn stream(
        &self,
        uri: Arc<str>,
        session: Option<Arc<str>>,
        last_event: Option<String>,
        auth: Option<String>,
        custom: HashMap<HeaderName, HeaderValue>,
        event_limit: usize,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, HttpError> {
        let response = self
            .send(self.request(
                Method::GET,
                &uri,
                session.as_deref(),
                auth,
                custom,
                last_event,
            )?)
            .await?;
        if response.status() == StatusCode::METHOD_NOT_ALLOWED {
            return Err(StreamableHttpError::ServerDoesNotSupportSse);
        }
        if response.status() == StatusCode::NOT_FOUND && session.is_some() {
            return Err(StreamableHttpError::SessionExpired);
        }
        if !response.status().is_success() {
            return Err(failure(McpHttpError::HttpStatus(
                response.status().as_u16(),
            )));
        }
        if !content_type(&response, "text/event-stream") {
            return Err(failure(McpHttpError::InvalidResponse));
        }
        Ok(crate::http_sse::response_stream(
            response,
            event_limit.min(self.limits.max_event_bytes.get()),
            self.limits.max_stream_bytes.get(),
        ))
    }
}

impl StreamableHttpClient for BoundedHttpClient {
    type Error = McpHttpError;
    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        auth: Option<String>,
        custom: HashMap<HeaderName, HeaderValue>,
    ) -> Result<StreamableHttpPostResponse, HttpError> {
        self.post(
            uri,
            message,
            session,
            auth,
            custom,
            self.limits.max_event_bytes.get(),
        )
        .await
        .map(|(response, _)| response)
    }
    async fn post_message_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        auth: Option<String>,
        custom: HashMap<HeaderName, HeaderValue>,
        max: usize,
    ) -> Result<StreamableHttpPostResponse, HttpError> {
        self.post(uri, message, session, auth, custom, max)
            .await
            .map(|(response, _)| response)
    }
    async fn get_stream(
        &self,
        uri: Arc<str>,
        session: Option<Arc<str>>,
        last_event: Option<String>,
        auth: Option<String>,
        custom: HashMap<HeaderName, HeaderValue>,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, HttpError> {
        self.stream(
            uri,
            session,
            last_event,
            auth,
            custom,
            self.limits.max_event_bytes.get(),
        )
        .await
    }
    async fn get_stream_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        session: Option<Arc<str>>,
        last_event: Option<String>,
        auth: Option<String>,
        custom: HashMap<HeaderName, HeaderValue>,
        max: usize,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, HttpError> {
        self.stream(uri, session, last_event, auth, custom, max)
            .await
    }
    async fn delete_session(
        &self,
        uri: Arc<str>,
        session: Arc<str>,
        auth: Option<String>,
        custom: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), HttpError> {
        let response = self
            .send(self.request(Method::DELETE, &uri, Some(&session), auth, custom, None)?)
            .await?;
        if response.status().is_success() || response.status() == StatusCode::METHOD_NOT_ALLOWED {
            return Ok(());
        }
        Err(failure(McpHttpError::HttpStatus(
            response.status().as_u16(),
        )))
    }
}

fn valid_session(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
}

fn content_type(response: &reqwest::Response, expected: &str) -> bool {
    response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(';')
                .next()
                .unwrap_or("")
                .trim()
                .eq_ignore_ascii_case(expected)
        })
}

async fn read_json(response: reqwest::Response, limit: usize) -> Result<Vec<u8>, HttpError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(failure(McpHttpError::BodyLimit));
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| failure(McpHttpError::Network))?;
        if chunk.len() > limit.saturating_sub(bytes.len()) {
            return Err(failure(McpHttpError::BodyLimit));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

struct BoundedBody {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for BoundedBody {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
