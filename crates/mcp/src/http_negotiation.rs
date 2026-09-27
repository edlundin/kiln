//! HTTP startup uses exact structured evidence, never the SDK's broad Auto mode.

use std::sync::{Arc, Mutex};

use rmcp::{
    ClientHandler, RoleClient,
    model::{ClientJsonRpcMessage, ClientRequest, ErrorCode, RequestId, ServerJsonRpcMessage},
    service::RunningService,
    transport::Transport,
};
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};

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
    config: McpHttpGenerationConfig,
    policy: ProtocolPolicy,
    deadline: Instant,
) -> Result<RunningService<RoleClient, Arc<S>>, McpHttpStartError> {
    let (startup, _cleanup) = start_managed_http_client(service, config, policy, deadline);
    startup.await
}

/// Owns completion evidence for every attempt, including a fallback worker and
/// a successfully started client. Stop/drop the startup future or cancel the
/// running client before awaiting cleanup. Local worker completion does not
/// prove successful remote session deletion.
pub struct McpHttpClientCleanup {
    attempts: mpsc::Receiver<watch::Receiver<Option<bool>>>,
    current: Option<watch::Receiver<Option<bool>>>,
    failed: bool,
}

impl McpHttpClientCleanup {
    /// Cancellation-safe while this handle is retained: another call resumes
    /// waiting for the same worker, never losing a received completion handle.
    pub async fn finish(&mut self) -> Result<(), McpHttpStartError> {
        loop {
            if self.current.is_none() {
                self.current = self.attempts.recv().await;
                if self.current.is_none() {
                    return if self.failed {
                        Err(McpHttpStartError::Cleanup)
                    } else {
                        Ok(())
                    };
                }
            }
            self.failed |=
                !wait_http_cleanup(self.current.as_ref().expect("received worker").clone()).await;
            self.current = None;
        }
    }
}

/// Return startup and its cleanup owner before polling can start any I/O.
/// Dropping an unpolled startup creates no worker. Dropping a polled startup
/// closes its transport; `finish` waits for the owned close task. After success,
/// the same handle waits for the running client's worker termination. The caller
/// must retain it through durable lifecycle retirement and runtime shutdown.
pub fn start_managed_http_client<S: ClientHandler>(
    service: S,
    config: McpHttpGenerationConfig,
    policy: ProtocolPolicy,
    deadline: Instant,
) -> (
    impl Future<Output = Result<RunningService<RoleClient, Arc<S>>, McpHttpStartError>> + Send,
    McpHttpClientCleanup,
) {
    // At most one preferred attempt and one evidence-authorized fallback exist.
    // Registration never waits for cleanup to be polled by the lifecycle owner.
    let (attempts, receiver) = mpsc::channel(2);
    let startup =
        async move { start_http_client_inner(service, config, policy, deadline, &attempts).await };
    (
        startup,
        McpHttpClientCleanup {
            attempts: receiver,
            current: None,
            failed: false,
        },
    )
}

async fn start_http_client_inner<S: ClientHandler>(
    service: S,
    mut config: McpHttpGenerationConfig,
    policy: ProtocolPolicy,
    deadline: Instant,
    attempts: &mpsc::Sender<watch::Receiver<Option<bool>>>,
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
        // A dropped observer must not prevent the transport's own close task.
        // Capacity cannot fill: negotiation admits at most two total attempts.
        let _ = attempts.try_send(cleanup.clone());
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
        if let ClientJsonRpcMessage::Request(request) = &message
            && matches!(&request.request, ClientRequest::DiscoverRequest(_))
        {
            self.discover_id = Some(request.id.clone());
            if let Ok(mut evidence) = self.fallback.lock() {
                *evidence = None;
            }
        }
        self.inner.send(message)
    }
    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        let message = self.inner.receive().await?;
        if let ServerJsonRpcMessage::Error(error) = &message
            && error
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
