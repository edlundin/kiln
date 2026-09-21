use std::{
    collections::VecDeque,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use kiln_core::{
    ModelInvocation, ModelInvocationOutcome, ModelProviderOperation, ProviderContext,
    ProviderError, ProviderRequest, ProviderUpdate, ProviderUsageMetadata, ProviderUsageUpdate,
    ResolvedModelCredential, UsageAccounting, UsageCompleteness, UsageFinality, UsageSource,
};
use reqwest::{
    Client, StatusCode,
    header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue},
    redirect::Policy,
};

use crate::{
    OpenAiApiKey, ResponsesRequestBody, ResponsesRequestLimits, ResponsesStream,
    ResponsesStreamLimits,
};

const RESPONSES_ENDPOINT: &str = "https://api.openai.com/v1/responses";

#[derive(Debug, Clone, Copy)]
pub struct OpenAiApiTransportLimits {
    pub connect_timeout: Duration,
    /// Overall HTTP request/body deadline, not an idle timeout or model latency estimate.
    pub request_timeout: Duration,
    pub request: ResponsesRequestLimits,
    pub stream: ResponsesStreamLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAiApiTransportError {
    InvalidLimits,
    Unavailable,
    CredentialMismatch,
    InvalidCredential,
    InvalidRequest,
}

/// Fixed public-API transport with no redirects, automatic retries or ambient
/// proxies. Construction/preparation perform no network or vault operations.
/// Subscription transport and native provider registration are separate.
pub struct OpenAiApiTransport {
    client: Client,
    limits: OpenAiApiTransportLimits,
}

impl OpenAiApiTransport {
    pub fn new(limits: OpenAiApiTransportLimits) -> Result<Self, OpenAiApiTransportError> {
        use OpenAiApiTransportError as Error;
        if limits.connect_timeout.is_zero()
            || limits.request_timeout.is_zero()
            || limits.connect_timeout > limits.request_timeout
            || Instant::now().checked_add(limits.request_timeout).is_none()
            || limits.request.max_request_bytes == 0
            || limits.request.max_input_items == 0
            || limits.request.replay.max_output_bytes == 0
            || limits.request.replay.max_item_bytes == 0
            || limits.request.replay.max_items == 0
            || limits.stream.max_retained_bytes == 0
            || limits.stream.framing.max_frame_bytes == 0
            || limits.stream.framing.max_stream_bytes == 0
            || limits.stream.framing.max_events == 0
        {
            return Err(Error::InvalidLimits);
        }
        limits
            .stream
            .completion
            .validate()
            .map_err(|_| Error::InvalidLimits)?;
        let client = Client::builder()
            .https_only(true)
            .no_proxy()
            .redirect(Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(limits.connect_timeout)
            .timeout(limits.request_timeout)
            .build()
            .map_err(|_| Error::Unavailable)?;
        Ok(Self { client, limits })
    }

    /// Consume one claimed request and its resolved credential proof. Dispatch
    /// starts on the operation's first poll, allowing cancellation before send.
    pub fn prepare(
        &self,
        request: ProviderRequest,
        context: &ProviderContext,
        credential: ResolvedModelCredential,
    ) -> Result<OpenAiApiOperation, OpenAiApiTransportError> {
        use OpenAiApiTransportError as Error;
        if !credential.matches(&request) {
            return Err(Error::CredentialMismatch);
        }
        let body = ResponsesRequestBody::from_context(&request, context, self.limits.request)
            .map_err(|_| Error::InvalidRequest)?;
        let key = OpenAiApiKey::from_credential(
            request.invocation().provider_account_id(),
            credential.into_secret(),
        )
        .map_err(|_| Error::InvalidCredential)?
        .into_secret();
        let mut bearer = b"Bearer ".to_vec();
        bearer.extend_from_slice(key.as_bytes());
        let mut authorization =
            HeaderValue::from_bytes(&bearer).map_err(|_| Error::InvalidCredential)?;
        authorization.set_sensitive(true);
        bearer.fill(0);
        let http_request = self
            .client
            .post(RESPONSES_ENDPOINT)
            .header(AUTHORIZATION, authorization)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "text/event-stream")
            .body(body.as_json().to_vec())
            .build()
            .map_err(|_| Error::InvalidRequest)?;
        let invocation = request.invocation().clone();
        let stream =
            ResponsesStream::new(request, self.limits.stream).map_err(|_| Error::InvalidRequest)?;
        Ok(OpenAiApiOperation {
            client: self.client.clone(),
            request: Some(http_request),
            response: None,
            stream: Some(stream),
            invocation,
            updates: VecDeque::new(),
            network_finished: false,
            terminal_emitted: false,
            failed: false,
        })
    }
}

/// No Debug: request headers and provider-private body must remain private.
/// Dropping/cancelling stops local transport; it cannot promise remote work or
/// billing stopped. There is no automatic replay of an ambiguous request.
pub struct OpenAiApiOperation {
    client: Client,
    request: Option<reqwest::Request>,
    response: Option<reqwest::Response>,
    stream: Option<ResponsesStream>,
    invocation: ModelInvocation,
    updates: VecDeque<ProviderUpdate>,
    network_finished: bool,
    terminal_emitted: bool,
    failed: bool,
}

impl OpenAiApiOperation {
    async fn collect(&mut self) -> Result<(), ProviderError> {
        if self.response.is_none() {
            // Taking once prevents a dropped execute future from causing a resend
            // if a caller later polls the same operation again.
            let request = self
                .request
                .take()
                .ok_or(ProviderError::ProviderStreamInterrupted)?;
            let response = self
                .client
                .execute(request)
                .await
                .map_err(|_| ProviderError::ProviderUnavailable)?;
            if !response.status().is_success() {
                return Err(http_error(response.status()));
            }
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.split(';').next())
                .map(str::trim);
            if !content_type.is_some_and(|v| v.eq_ignore_ascii_case("text/event-stream")) {
                return Err(ProviderError::ProviderProtocolChanged);
            }
            self.response = Some(response);
        }
        loop {
            let chunk = self
                .response
                .as_mut()
                .ok_or(ProviderError::ProviderStreamInterrupted)?
                .chunk()
                .await
                .map_err(|_| ProviderError::ProviderStreamInterrupted)?;
            let stream = self
                .stream
                .as_mut()
                .ok_or(ProviderError::ProviderResponseInvalid)?;
            if let Some(chunk) = chunk {
                stream
                    .push(&chunk, now()?)
                    .map_err(|_| ProviderError::ProviderResponseInvalid)?;
            } else {
                let completion = stream
                    .finish()
                    .map_err(|_| ProviderError::ProviderResponseInvalid)?;
                let (output, terminal) = completion.into_parts();
                self.updates
                    .extend(output.into_iter().map(ProviderUpdate::Output));
                self.updates.push_back(terminal);
                self.stream = None;
                self.response = None;
                self.network_finished = true;
                return Ok(());
            }
        }
    }

    fn pop_update(&mut self) -> Option<ProviderUpdate> {
        let update = self.updates.pop_front()?;
        if matches!(
            &update,
            ProviderUpdate::Finished { .. } | ProviderUpdate::CompletedWithContinuation { .. }
        ) {
            self.terminal_emitted = true;
        }
        Some(update)
    }
}

impl ModelProviderOperation for OpenAiApiOperation {
    async fn next_update(&mut self) -> Result<Option<ProviderUpdate>, ProviderError> {
        if let Some(update) = self.pop_update() {
            return Ok(Some(update));
        }
        if self.network_finished {
            return Ok(None);
        }
        if self.failed {
            return Err(ProviderError::ProviderStreamInterrupted);
        }
        if let Err(error) = self.collect().await {
            self.failed = true;
            return Err(error);
        }
        Ok(self.pop_update())
    }

    async fn cancel(&mut self) -> Result<(), ProviderError> {
        if self.terminal_emitted {
            return Ok(());
        }
        // Drop local network handles before synthesizing a terminal update.
        self.request = None;
        self.response = None;
        let mut usage = self
            .stream
            .as_mut()
            .and_then(ResponsesStream::take_terminal_usage);
        self.stream = None;
        for update in self.updates.drain(..) {
            match update {
                ProviderUpdate::Finished {
                    usage: final_usage, ..
                }
                | ProviderUpdate::CompletedWithContinuation {
                    usage: final_usage, ..
                } => usage = Some(final_usage),
                _ => {}
            }
        }
        let usage = match usage {
            Some(usage) => usage,
            None => ProviderUsageUpdate::new(
                ProviderUsageMetadata {
                    update_id: format!(
                        "responses:{}:terminal",
                        self.invocation.invocation_id().as_str()
                    ),
                    provider_account_id: self.invocation.provider_account_id().clone(),
                    work_id: self.invocation.work_id().clone(),
                    model_invocation_id: self.invocation.invocation_id().clone(),
                    accounting: UsageAccounting::Cumulative,
                    finality: UsageFinality::Final,
                    completeness: UsageCompleteness::Unknown,
                    observed_at_unix_ms: now()?,
                    request_id: None,
                    resolved_model: None,
                    service_tier: None,
                    source: UsageSource::NativeProvider,
                },
                Vec::new(),
            )
            .map_err(|_| ProviderError::ProviderResponseInvalid)?,
        };
        let update = ProviderUpdate::Finished {
            outcome: ModelInvocationOutcome::cancelled(),
            usage,
        };
        update.validate_for(&self.invocation)?;
        self.updates.push_back(update);
        self.network_finished = true;
        Ok(())
    }
}

fn http_error(status: StatusCode) -> ProviderError {
    match status {
        StatusCode::UNAUTHORIZED => ProviderError::AuthenticationRequired,
        StatusCode::FORBIDDEN => ProviderError::ModelUnavailable,
        StatusCode::TOO_MANY_REQUESTS => ProviderError::RateLimited,
        _ if status.is_server_error() => ProviderError::ProviderUnavailable,
        _ if status.is_redirection() => ProviderError::ProviderProtocolChanged,
        _ => ProviderError::ProviderResponseInvalid,
    }
}

fn now() -> Result<u64, ProviderError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|time| u64::try_from(time.as_millis()).ok())
        .ok_or(ProviderError::ProviderUnavailable)
}
