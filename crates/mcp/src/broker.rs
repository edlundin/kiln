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
pub struct StdioBrokerLimits {
    pub generation: McpGenerationId,
    pub definition: McpDefinitionLimits,
    pub max_resolved_bytes: NonZeroUsize,
    pub max_frame_bytes: NonZeroUsize,
    pub shutdown_grace: Duration,
    pub startup_deadline: Instant,
    pub call: StdioCallLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StdioBrokerError {
    Preparation(McpInvocationError),
    Directory,
    Binding(StdioBindingError),
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
pub async fn execute_stdio_call<S, V, P, PF, A, AF, E>(
    registry: &StdioRegistry<S>,
    request: ModelToolExecutionRequest<McpCommand>,
    vault: &V,
    limits: StdioBrokerLimits,
    mut cancellation: oneshot::Receiver<()>,
    pin: P,
    archive: A,
) -> Result<ToolCallResult, StdioBrokerError>
where
    S: McpInstanceStore + McpDefinitionStore + McpInvocationStore + McpLaunchStore + 'static,
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
            .map_err(StdioBrokerError::Preparation)?;
        let directory = pin(context.directory.clone())
            .await
            .map_err(|_| StdioBrokerError::Directory)?;
        let revision = context.host.revision;
        let metadata = context.host.bindings.metadata_json().to_owned();
        let resolved = resolve_persisted_stdio_launch(
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
        .map_err(StdioBrokerError::Binding)?;
        // Vault reads and filesystem work can suspend. A previously valid
        // snapshot must not bypass a cancellation or rotation during that work.
        let current = store
            .inspect_mcp_launch(&request, limits.definition)
            .await
            .map_err(StdioBrokerError::Preparation)?;
        if current.host.revision != revision || current.host.bindings.metadata_json() != metadata {
            return Err(StdioBrokerError::Preparation(McpInvocationError::Conflict));
        }
        let key = resolved.launch.key.clone();
        let ready = registry
            .ensure_ready(resolved.launch, resolved.binding_revision)
            .await
            .map_err(StdioBrokerError::Registry)?;
        Ok((key, ready))
    };
    let (key, ready) = tokio::select! {
        biased;
        _ = &mut cancellation => return Err(StdioBrokerError::CancelledBeforeDispatch),
        _ = tokio::time::sleep_until(limits.call.deadline) => return Err(StdioBrokerError::DeadlineBeforeDispatch),
        result = prepare => result?,
    };
    let permit = match claim_mcp_dispatch(store, request, &ready, limits.definition)
        .await
        .map_err(StdioBrokerError::DispatchClaim)?
    {
        McpDispatchClaim::Acquired(permit) => permit,
        McpDispatchClaim::Existing(_) => {
            return Err(StdioBrokerError::DispatchClaim(
                McpInvocationError::Conflict,
            ));
        }
    };
    registry
        .dispatch_tool_call(&key, permit, limits.call, cancellation, archive)
        .await
        .map_err(StdioBrokerError::Completion)
}
