//! Durable dispatch ownership layered on the ordinary native ToolCall claim.

use std::num::NonZeroUsize;

use crate::{
    MCP_CALL_CAPABILITY, McpCallCommand, McpDefinitionLimits, McpGenerationId, McpInstanceRecord,
    ModelToolExecutionRequest, ToolCallId, ToolCallState,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpInvocationState {
    Dispatching,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}
impl McpInvocationState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dispatching => "dispatching",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
        }
    }
    pub fn parse(value: &str) -> Result<Self, McpInvocationError> {
        match value {
            "dispatching" => Ok(Self::Dispatching),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "interrupted" => Ok(Self::Interrupted),
            _ => Err(McpInvocationError::IntegrityViolation),
        }
    }
}

/// A receipt is not dispatch authority. Request/result bodies stay behind the
/// ordinary native ToolCall source and artifact boundaries, never in these events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpInvocationRecord {
    pub tool_call_id: ToolCallId,
    pub generation: McpGenerationId,
    pub state: McpInvocationState,
}

pub enum McpInvocationMutation {
    Applied(McpInvocationRecord),
    Existing(McpInvocationRecord),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpInvocationError {
    InvalidRequest,
    ScopeMismatch,
    DefinitionChanged,
    GenerationChanged,
    Busy,
    Conflict,
    NotFound,
    IntegrityViolation,
    Unavailable,
}

pub trait McpInvocationStore: Send + Sync {
    /// Revalidate the current ToolCall, frozen source, definition and generation
    /// in the same transaction as the unique dispatch record. Existing records
    /// never authorize sending, even if their outcome is interrupted or unknown.
    fn begin_mcp_invocation(
        &self,
        request: &ModelToolExecutionRequest<McpCallCommand>,
        target: &McpInstanceRecord,
        limits: McpDefinitionLimits,
    ) -> impl Future<Output = Result<McpInvocationMutation, McpInvocationError>> + Send;

    fn finish_mcp_invocation(
        &self,
        expected: &McpInvocationRecord,
        state: McpInvocationState,
    ) -> impl Future<Output = Result<McpInvocationRecord, McpInvocationError>> + Send;

    /// Invoke only after the generation has lost readiness, or under exclusive
    /// startup ownership before dispatch. None interrupts all unfinished calls.
    /// This records an unknown external outcome and never supplies a retry permit.
    fn interrupt_mcp_invocations(
        &self,
        generation: Option<&McpGenerationId>,
        batch_size: NonZeroUsize,
    ) -> impl Future<Output = Result<usize, McpInvocationError>> + Send;
}

/// One fresh claim, consumed by the broker. No Clone, deserialization, public
/// constructor or recovery path. Dropping it cannot authorize another dispatch.
pub struct McpDispatchPermit {
    request: ModelToolExecutionRequest<McpCallCommand>,
    record: McpInvocationRecord,
}
impl McpDispatchPermit {
    pub fn request(&self) -> &ModelToolExecutionRequest<McpCallCommand> {
        &self.request
    }
    pub fn record(&self) -> &McpInvocationRecord {
        &self.record
    }
}

pub enum McpDispatchClaim {
    Acquired(McpDispatchPermit),
    Existing(McpInvocationRecord),
}

pub async fn claim_mcp_dispatch<S: McpInvocationStore>(
    store: &S,
    request: ModelToolExecutionRequest<McpCallCommand>,
    target: &McpInstanceRecord,
    limits: McpDefinitionLimits,
) -> Result<McpDispatchClaim, McpInvocationError> {
    if request.tool_call().state() != ToolCallState::Running
        || request.tool_call().capability() != MCP_CALL_CAPABILITY
        || request.command().server_id() != target.key.definition_id()
        || request.command().definition_version() != target.definition_version
    {
        return Err(McpInvocationError::InvalidRequest);
    }
    match store.begin_mcp_invocation(&request, target, limits).await? {
        McpInvocationMutation::Applied(record) => {
            if record.tool_call_id != *request.tool_call().tool_call_id()
                || record.generation != target.generation
                || record.state != McpInvocationState::Dispatching
            {
                return Err(McpInvocationError::IntegrityViolation);
            }
            Ok(McpDispatchClaim::Acquired(McpDispatchPermit {
                request,
                record,
            }))
        }
        McpInvocationMutation::Existing(record) => Ok(McpDispatchClaim::Existing(record)),
    }
}
