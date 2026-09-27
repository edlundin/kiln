//! Approved-call composition used by the opt-in daemon native MCP coordinator.

use std::{num::NonZeroUsize, time::Duration};

use kiln_core::{
    Artifact, McpCommand, McpDefinitionLimits, McpDefinitionStore, McpDispatchClaim,
    McpGenerationId, McpInstanceStore, McpInvocationError, McpInvocationStore, McpLaunchStore,
    McpSecretStore, ModelToolExecutionRequest, RunError, ToolCallResult, WorkspaceCheckout,
    claim_mcp_dispatch,
};
use tokio::{sync::oneshot, time::Instant};

use crate::{
    StdioBindingError, StdioCallLimits, StdioLaunchResources, StdioRegistry, StdioRegistryError,
    resolve_persisted_stdio_launch,
};

/// Explicit host budgets; no model-supplied launch resources or product defaults.
pub struct McpBrokerLimits {
    pub generation: McpGenerationId,
    pub definition: McpDefinitionLimits,
    pub max_resolved_bytes: NonZeroUsize,
    pub max_frame_bytes: NonZeroUsize,
    pub shutdown_grace: Duration,
    pub startup_deadline: Instant,
    pub call: StdioCallLimits,
}

/// Explicit HTTP allowances; absence disables HTTP broker startup.
#[derive(Clone, Copy)]
pub struct HttpBrokerLimits {
    pub io: crate::McpHttpLimits,
    pub channel_capacity: NonZeroUsize,
    pub max_exchanges: NonZeroUsize,
    pub max_catalog_lifetime_bytes: NonZeroUsize,
    pub legacy_resume_delay: Option<Duration>,
}

pub type StdioBrokerLimits = McpBrokerLimits;
pub type StdioBrokerError = McpBrokerError;

enum PreparedTransport {
    Stdio(crate::ResolvedStdioLaunch),
    Http(crate::ResolvedHttpLaunch, rustix::fd::OwnedFd),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpBrokerError {
    Preparation(McpInvocationError),
    Directory,
    Binding(StdioBindingError),
    HttpBinding(crate::HttpBindingError),
    UnsupportedTransport,
    Registry(StdioRegistryError),
    DispatchClaim(McpInvocationError),
    Completion(RunError),
    CancelledBeforeDispatch,
    DeadlineBeforeDispatch,
}

/// Consume one fresh approved native claim. The pin callback must verify root
/// identity/containment and return an owned directory descriptor for the supplied
/// checkout (infrastructure provides `pin_mcp_working_directory`). Archive output
/// using TOOL_OUTPUT_MEDIA_TYPE. The caller persists completion and its events.
///
/// Preparation cancellation can leave shared startup running, but never sends an
/// invocation. Once claimed, dispatch owns cancellation and uncertain outcomes.
/// No error or dropped future is permission to replay this request.
#[expect(
    clippy::too_many_arguments,
    reason = "The broker keeps independent launch, dispatch and artifact authority explicit in its public API."
)]
pub async fn execute_mcp_call<S, V, P, PF, A, AF, E>(
    registry: &StdioRegistry<S>,
    request: ModelToolExecutionRequest<McpCommand>,
    vault: &V,
    limits: McpBrokerLimits,
    http: Option<HttpBrokerLimits>,
    mut cancellation: oneshot::Receiver<()>,
    pin: P,
    archive: A,
) -> Result<ToolCallResult, McpBrokerError>
where
    S: McpInstanceStore
        + McpDefinitionStore
        + McpInvocationStore
        + kiln_core::McpInputStore
        + kiln_core::McpElicitationDecisionStore
        + kiln_core::McpElicitationUrlStore
        + McpLaunchStore
        + 'static,
    V: McpSecretStore,
    P: FnOnce(WorkspaceCheckout) -> PF,
    PF: Future<Output = Result<rustix::fd::OwnedFd, RunError>>,
    A: FnOnce(Vec<u8>) -> AF,
    AF: Future<Output = Result<Artifact, E>>,
{
    let store = registry.store();
    let prepare = async {
        let context = store
            .inspect_mcp_launch(&request, limits.definition)
            .await
            .map_err(McpBrokerError::Preparation)?;
        let directory = pin(context.directory.clone())
            .await
            .map_err(|_| McpBrokerError::Directory)?;
        let revision = context.host.revision;
        let metadata = context.host.bindings.metadata_json().to_owned();
        let prepared = if matches!(
            context.definition.definition.server().transport,
            kiln_core::SharedMcpTransport::Stdio { .. }
        ) {
            PreparedTransport::Stdio(
                resolve_persisted_stdio_launch(
                    &context.definition,
                    context.host,
                    &context.directory,
                    StdioLaunchResources {
                        generation: limits.generation,
                        working_directory: directory,
                        definition_limits: limits.definition,
                        max_resolved_bytes: limits.max_resolved_bytes,
                        max_frame_bytes: limits.max_frame_bytes,
                        shutdown_grace: limits.shutdown_grace,
                        startup_deadline: limits.startup_deadline,
                    },
                    vault,
                )
                .await
                .map_err(McpBrokerError::Binding)?,
            )
        } else {
            let http = http.ok_or(McpBrokerError::UnsupportedTransport)?;
            let instance = context.host.bindings.instance_id().clone();
            let key = context.host.bindings.key().clone();
            let resolved = crate::resolve_persisted_http_launch(
                &context.definition,
                context.host,
                crate::HttpLaunchAuthorization {
                    instance_id: &instance,
                    key: &key,
                    directory: Some(&context.directory),
                },
                crate::HttpLaunchResources {
                    generation: limits.generation,
                    definition_limits: limits.definition,
                    max_resolved_bytes: limits.max_resolved_bytes,
                    io: http.io,
                    channel_capacity: http.channel_capacity,
                    max_exchanges: http.max_exchanges,
                    max_catalog_lifetime_bytes: http.max_catalog_lifetime_bytes,
                    legacy_resume_delay: http.legacy_resume_delay,
                    startup_deadline: limits.startup_deadline,
                },
                vault,
            )
            .await
            .map_err(McpBrokerError::HttpBinding)?;
            PreparedTransport::Http(resolved, directory)
        };
        // Vault reads and filesystem work can suspend. A previously valid
        // snapshot must not bypass a cancellation or rotation during that work.
        let current = store
            .inspect_mcp_launch(&request, limits.definition)
            .await
            .map_err(McpBrokerError::Preparation)?;
        if current.host.revision != revision || current.host.bindings.metadata_json() != metadata {
            return Err(McpBrokerError::Preparation(McpInvocationError::Conflict));
        }
        let (key, ready) = match prepared {
            PreparedTransport::Stdio(resolved) => {
                let key = resolved.launch.key.clone();
                let ready = registry
                    .ensure_ready(resolved.launch, resolved.binding_revision)
                    .await
                    .map_err(McpBrokerError::Registry)?;
                (key, ready)
            }
            PreparedTransport::Http(resolved, directory) => {
                let key = resolved.key.clone();
                let ready = registry
                    .ensure_http_ready(resolved, directory)
                    .await
                    .map_err(McpBrokerError::Registry)?;
                (key, ready)
            }
        };
        Ok((key, ready))
    };
    let (key, ready) = tokio::select! {
        biased;
        _ = &mut cancellation => return Err(McpBrokerError::CancelledBeforeDispatch),
        _ = tokio::time::sleep_until(limits.call.deadline) => return Err(McpBrokerError::DeadlineBeforeDispatch),
        result = prepare => result?,
    };
    let permit = match claim_mcp_dispatch(store, request, &ready, limits.definition)
        .await
        .map_err(McpBrokerError::DispatchClaim)?
    {
        McpDispatchClaim::Acquired(permit) => permit,
        McpDispatchClaim::Existing(_) => {
            return Err(McpBrokerError::DispatchClaim(McpInvocationError::Conflict));
        }
    };
    registry
        .dispatch_tool_call(&key, permit, limits.call, cancellation, archive)
        .await
        .map_err(McpBrokerError::Completion)
}

/// Compatibility entry point: HTTP remains disabled for stdio-only callers.
pub async fn execute_stdio_call<S, V, P, PF, A, AF, E>(
    registry: &StdioRegistry<S>,
    request: ModelToolExecutionRequest<McpCommand>,
    vault: &V,
    limits: StdioBrokerLimits,
    cancellation: oneshot::Receiver<()>,
    pin: P,
    archive: A,
) -> Result<ToolCallResult, StdioBrokerError>
where
    S: McpInstanceStore
        + McpDefinitionStore
        + McpInvocationStore
        + kiln_core::McpInputStore
        + kiln_core::McpElicitationDecisionStore
        + kiln_core::McpElicitationUrlStore
        + McpLaunchStore
        + 'static,
    V: McpSecretStore,
    P: FnOnce(WorkspaceCheckout) -> PF,
    PF: Future<Output = Result<rustix::fd::OwnedFd, RunError>>,
    A: FnOnce(Vec<u8>) -> AF,
    AF: Future<Output = Result<Artifact, E>>,
{
    execute_mcp_call(
        registry,
        request,
        vault,
        limits,
        None,
        cancellation,
        pin,
        archive,
    )
    .await
}
