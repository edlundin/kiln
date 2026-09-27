//! Read-only provenance for reviewing a durable ToolCall before approval.

use std::num::NonZeroUsize;

use crate::{ModelInvocationId, ModelToolDefinition, ModelToolRequest, RunId, ToolCallId};

/// Frozen source data, never execution authority. No Debug: arguments may be
/// private user/model data. Resolved credentials and host bindings are absent.
pub struct NativeToolSource {
    pub invocation_id: ModelInvocationId,
    pub request: ModelToolRequest,
    pub definition: ModelToolDefinition,
}

pub struct ToolCallInspection {
    pub tool_call_id: ToolCallId,
    pub run_id: RunId,
    pub capability: String,
    /// None for a ToolCall without a native model-request adoption.
    pub source: Option<NativeToolSource>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCallInspectionError {
    NotFound,
    LimitExceeded,
    IntegrityViolation,
    Unavailable,
}

pub trait ToolCallInspectionStore: Send + Sync {
    /// Budget covers the frozen request batch and catalogue needed to validate
    /// their hashes, plus the ToolCall capability. No partial source is returned.
    fn inspect_tool_call(
        &self,
        tool_call_id: &ToolCallId,
        max_source_bytes: NonZeroUsize,
    ) -> impl Future<Output = Result<ToolCallInspection, ToolCallInspectionError>> + Send;
}
