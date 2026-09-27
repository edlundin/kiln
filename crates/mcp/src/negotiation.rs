use rmcp::{
    ClientLifecycleMode, RoleClient, Service,
    model::{
        ClientJsonRpcMessage, ClientRequest, ErrorCode, RequestId, ServerJsonRpcMessage,
        ServerResult,
    },
    serve_client_with_lifecycle,
    service::{ClientInitializeError, RunningService},
    transport::Transport,
};

pub use kiln_core::{McpProtocolPolicy as ProtocolPolicy, McpProtocolVersion as ProtocolVersion};

/// Negotiate on an already-owned stdio transport. The caller owns the process,
/// scope, startup deadline and cancellation. This does not start a process or
/// grant tool authority. HTTP requires a separate unsupported-version policy.
pub async fn start_stdio_client<S, T>(
    service: S,
    transport: T,
    policy: ProtocolPolicy,
) -> Result<RunningService<RoleClient, S>, ClientInitializeError>
where
    S: Service<RoleClient>,
    T: Transport<RoleClient> + 'static,
{
    let lifecycle = match policy {
        ProtocolPolicy::Auto => ClientLifecycleMode::Auto {
            preferred_versions: vec![rmcp::model::ProtocolVersion::V_2026_07_28],
            legacy_version: Some(rmcp::model::ProtocolVersion::V_2025_11_25),
        },
        ProtocolPolicy::Pinned(ProtocolVersion::V20260728) => ClientLifecycleMode::Discover {
            preferred_versions: vec![rmcp::model::ProtocolVersion::V_2026_07_28],
        },
        ProtocolPolicy::Pinned(_) => ClientLifecycleMode::Initialize,
    };
    let legacy_permitted = matches!(
        policy,
        ProtocolPolicy::Pinned(version) if version != ProtocolVersion::V20260728
    );
    let guarded = GuardedTransport {
        inner: transport,
        policy,
        discover_id: None,
        legacy_permitted,
    };
    serve_client_with_lifecycle(service, guarded, lifecycle).await
}

struct GuardedTransport<T> {
    inner: T,
    policy: ProtocolPolicy,
    discover_id: Option<RequestId>,
    legacy_permitted: bool,
}

enum GuardError<E> {
    Transport(E),
    UnsupportedFallback,
}

impl<E: std::error::Error> std::fmt::Debug for GuardError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl<E: std::error::Error> std::fmt::Display for GuardError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Raw transport text may contain server-controlled content or secrets.
        f.write_str(match self {
            Self::Transport(_) => "MCP transport failed",
            Self::UnsupportedFallback => {
                "MCP legacy fallback lacks a correlated unsupported-method response"
            }
        })
    }
}
impl<E: std::error::Error + 'static> std::error::Error for GuardError<E> {}

impl<T: Transport<RoleClient>> Transport<RoleClient> for GuardedTransport<T> {
    type Error = GuardError<T::Error>;

    fn send(
        &mut self,
        mut item: ClientJsonRpcMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let mut permitted = true;
        if let ClientJsonRpcMessage::Request(request) = &mut item {
            match &mut request.request {
                ClientRequest::DiscoverRequest(_) => {
                    self.discover_id = Some(request.id.clone());
                    self.legacy_permitted = false;
                }
                ClientRequest::InitializeRequest(initialize) => {
                    permitted = self.legacy_permitted;
                    self.legacy_permitted = false;
                    if let ProtocolPolicy::Pinned(version) = self.policy {
                        initialize.params.protocol_version = match version {
                            ProtocolVersion::V20241105 => {
                                rmcp::model::ProtocolVersion::V_2024_11_05
                            }
                            ProtocolVersion::V20250326 => {
                                rmcp::model::ProtocolVersion::V_2025_03_26
                            }
                            ProtocolVersion::V20250618 => {
                                rmcp::model::ProtocolVersion::V_2025_06_18
                            }
                            ProtocolVersion::V20251125 => {
                                rmcp::model::ProtocolVersion::V_2025_11_25
                            }
                            ProtocolVersion::V20260728 => {
                                rmcp::model::ProtocolVersion::V_2026_07_28
                            }
                        };
                    }
                }
                _ => {}
            }
        }
        // rmcp 3.4.1 Auto attempts initialize after its discovery timeout. Never
        // put that request on the wire without explicit unsupported evidence.
        let send = permitted.then(|| self.inner.send(item));
        async move {
            match send {
                Some(send) => send.await.map_err(GuardError::Transport),
                None => Err(GuardError::UnsupportedFallback),
            }
        }
    }

    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        let message = self.inner.receive().await?;
        if let ServerJsonRpcMessage::Error(error) = &message {
            self.legacy_permitted = self.policy == ProtocolPolicy::Auto
                && error.error.code == ErrorCode::METHOD_NOT_FOUND
                && error
                    .id
                    .as_ref()
                    .zip(self.discover_id.as_ref())
                    .is_some_and(|(received, expected)| expected == received);
        }
        if let ServerJsonRpcMessage::Response(response) = &message {
            if let ServerResult::InitializeResult(result) = &response.result {
                let version = result.protocol_version.as_str();
                let accepted = match self.policy {
                    ProtocolPolicy::Auto => {
                        ProtocolVersion::parse(version).is_ok() && version != "2026-07-28"
                    }
                    ProtocolPolicy::Pinned(pin) => version == pin.as_str(),
                };
                if !accepted {
                    let _ = self.inner.close().await;
                    return None;
                }
            }
        }
        Some(message)
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.inner.close().await.map_err(GuardError::Transport)
    }
}
