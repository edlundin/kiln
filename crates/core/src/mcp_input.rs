//! Private mediation receipts. These never authorize replay or contain MCP bodies.

use std::num::NonZeroU64;

use crate::{McpInvocationError, McpInvocationRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpInputKind {
    Roots,
    Sampling,
    Elicitation,
}
impl McpInputKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Roots => "roots",
            Self::Sampling => "sampling",
            Self::Elicitation => "elicitation",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpInputState {
    Required,
    Resolved,
    Interrupted,
}
impl McpInputState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Required => "required",
            Self::Resolved => "resolved",
            Self::Interrupted => "interrupted",
        }
    }
}

/// An ordinal belongs to one invocation, across all legacy requests/MRTR rounds.
/// Resolution records completed mediation, not permission to retry the operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpInputRecord {
    pub invocation: McpInvocationRecord,
    pub ordinal: NonZeroU64,
    pub kind: McpInputKind,
    pub state: McpInputState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpInputMutation {
    Applied(McpInputRecord),
    Existing(McpInputRecord),
}

impl McpInputMutation {
    pub fn record(&self) -> &McpInputRecord {
        match self {
            Self::Applied(record) | Self::Existing(record) => record,
        }
    }
}

pub trait McpInputStore: Send + Sync {
    /// Identify the live interaction owner without creating or approving input.
    /// Interactive Runs own their input; read-only children route to their
    /// interactive root in the same Session. This snapshot is not authority to
    /// publish a prompt or execute sampling; writers must revalidate ownership.
    fn mcp_input_interaction_run(
        &self,
        expected: &McpInputRecord,
    ) -> impl Future<Output = Result<crate::RunId, McpInvocationError>> + Send;

    /// Resolve the one directory already approved for this live roots input.
    /// Includes the approved relative scope, never the broader workspace root.
    /// A path is metadata, not filesystem access or authority to send a response;
    /// resolution must still be journaled against the live claim before sending.
    fn mcp_input_root(
        &self,
        expected: &McpInputRecord,
        limits: crate::McpDefinitionLimits,
    ) -> impl Future<Output = Result<std::path::PathBuf, McpInvocationError>> + Send;

    /// Record the next input before invoking any provider or interaction path.
    /// Only one input can remain pending per invocation; exact retries return a
    /// receipt, including after interruption, and never grant execution authority.
    fn require_mcp_input(
        &self,
        invocation: &McpInvocationRecord,
        ordinal: NonZeroU64,
        kind: McpInputKind,
    ) -> impl Future<Output = Result<McpInputMutation, McpInvocationError>> + Send;

    /// Journal mediation before sending its response. The same live invocation
    /// must still own dispatch. Restart/termination interrupts pending inputs.
    fn resolve_mcp_input(
        &self,
        expected: &McpInputRecord,
    ) -> impl Future<Output = Result<McpInputMutation, McpInvocationError>> + Send;
}
