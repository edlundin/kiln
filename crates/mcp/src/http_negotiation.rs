//! HTTP startup uses exact structured evidence, never the SDK's broad Auto mode.

use std::sync::{Arc, Mutex};

use rmcp::{
    ClientHandler, RoleClient,
    model::{ClientJsonRpcMessage, ClientRequest, ErrorCode, RequestId, ServerJsonRpcMessage},
    service::RunningService,
    transport::Transport,
};
use tokio::time::Instant;

use crate::http_generation::wait_http_cleanup;
use crate::{
    McpHttpError, McpHttpGenerationConfig, ProtocolPolicy, ProtocolVersion,
    http_generation_transport, start_stdio_client,
};

/// Sanitized failure classification; no remote frames, session IDs or credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpHttpStartError {
    Configuration(McpHttpError),
    Deadline,
    Negotiation,
    Cleanup,
}

impl std::fmt::Display for McpHttpStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MCP HTTP startup {self:?}")
    }
}
impl std::error::Error for McpHttpStartError {}

/// Negotiate an already-authorized HTTP snapshot, without invocation authority.
/// For Auto, config.protocol must be modern; legacy_resume_delay applies only to
/// a permitted legacy attempt. Pinned policy must match config.protocol exactly.
/// The deadline covers negotiation across attempts. Worker cleanup is awaited
/// even after that deadline; local shutdown does not prove remote session deletion.
pub async fn start_http_client<S: ClientHandler>(
    service: S,
    mut config: McpHttpGenerationConfig,
    policy: ProtocolPolicy,
    deadline: Instant,
) -> Result<RunningService<RoleClient, Arc<S>>, McpHttpStartError> {
    let initial = match policy {
        ProtocolPolicy::Auto => ProtocolVersion::V20260728,
        ProtocolPolicy::Pinned(version) => version,
    };
    if config.protocol != initial {
        return Err(McpHttpStartError::Configuration(
            McpHttpError::ProtocolViolation,
        ));
    }
    let legacy_resume = if policy == ProtocolPolicy::Auto {
        config.legacy_resume_delay.take()
    } else {
        config.legacy_resume_delay
    };
    let service = Arc::new(service);
    let evidence = Arc::new(Mutex::new(None));
    let mut allow_fallback = policy == ProtocolPolicy::Auto;
    loop {
        if Instant::now() >= deadline {
            return Err(McpHttpStartError::Deadline);
        }
        let transport =
            http_generation_transport(config.clone()).map_err(McpHttpStartError::Configuration)?;
        let cleanup = transport.cleanup_receiver();
        let observed = ObservedStartup {
            inner: transport,
            discover_id: None,
            fallback: evidence.clone(),
        };
        // The shared handshake implements exact pins for every transport. Its
        // stdio Auto fallback rule is deliberately never used on HTTP.
        let result = tokio::time::timeout_at(
            deadline,
            start_stdio_client(
                service.clone(),
                observed,
                ProtocolPolicy::Pinned(config.protocol),
            ),
        )
        .await;
        match result {
            Ok(Ok(client)) => return Ok(client),
            result => {
                if !wait_http_cleanup(cleanup).await {
                    return Err(McpHttpStartError::Cleanup);
                }
                if result.is_err() {
                    return Err(McpHttpStartError::Deadline);
                }
                let selected = evidence
                    .lock()
                    .map_err(|_| McpHttpStartError::Negotiation)?
                    .take();
                if !allow_fallback {
                    return Err(McpHttpStartError::Negotiation);
                }
                let Some(selected) = selected else {
                    return Err(McpHttpStartError::Negotiation);
                };
                allow_fallback = false;
                config.protocol = selected;
                config.legacy_resume_delay = legacy_resume;
            }
        }
    }
}

struct ObservedStartup<T> {
    inner: T,
    discover_id: Option<RequestId>,
    fallback: Arc<Mutex<Option<ProtocolVersion>>>,
}

impl<T: Transport<RoleClient>> Transport<RoleClient> for ObservedStartup<T> {
    type Error = T::Error;
    fn send(
        &mut self,
        message: ClientJsonRpcMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        if let ClientJsonRpcMessage::Request(request) = &message {
            if matches!(&request.request, ClientRequest::DiscoverRequest(_)) {
                self.discover_id = Some(request.id.clone());
                if let Ok(mut evidence) = self.fallback.lock() {
                    *evidence = None;
                }
            }
        }
        self.inner.send(message)
    }
    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        let message = self.inner.receive().await?;
        if let ServerJsonRpcMessage::Error(error) = &message {
            if error
                .id
                .as_ref()
                .is_some_and(|id| Some(id) == self.discover_id.as_ref())
                && error.error.code == ErrorCode::UNSUPPORTED_PROTOCOL_VERSION
            {
                let selected = error.error.data.as_ref().and_then(legacy_version);
                if let Ok(mut evidence) = self.fallback.lock() {
                    *evidence = selected;
                }
            }
        }
        Some(message)
    }
    async fn close(&mut self) -> Result<(), Self::Error> {
        self.inner.close().await
    }
}

fn legacy_version(data: &serde_json::Value) -> Option<ProtocolVersion> {
    if data.get("requested")?.as_str()? != ProtocolVersion::V20260728.as_str() {
        return None;
    }
    let supported = data.get("supported")?.as_array()?;
    if supported.iter().any(|version| {
        !version.is_string() || version.as_str() == Some(ProtocolVersion::V20260728.as_str())
    }) {
        return None;
    }
    [
        ProtocolVersion::V20251125,
        ProtocolVersion::V20250618,
        ProtocolVersion::V20250326,
    ]
    .into_iter()
    .find(|version| {
        supported
            .iter()
            .any(|value| value.as_str() == Some(version.as_str()))
    })
}
