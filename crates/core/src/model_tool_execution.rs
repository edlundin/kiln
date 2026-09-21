use std::future::Future;

use crate::{
    AdoptModelToolRequest, EventId, ModelInvocationId, ModelToolAdoptionError, StoredSessionEvent,
    ToolCall, ToolCallId, WorkspacePathScope,
};

/// A fresh durable claim, consumed by a Kiln-owned executor. No Clone or Debug.
/// Dropping this request does not authorize redispatch; running work requires
/// explicit reconciliation after an ambiguous failure or process restart.
pub struct ModelToolExecutionRequest<C> {
    invocation_id: ModelInvocationId,
    provider_call_id: String,
    tool_call: ToolCall,
    scope: WorkspacePathScope,
    command: C,
}

impl<C> ModelToolExecutionRequest<C> {
    pub(crate) fn new(
        source: &AdoptModelToolRequest,
        tool_call: ToolCall,
        command: C,
    ) -> Result<Self, ModelToolAdoptionError> {
        let scope = tool_call
            .effective_scope()
            .cloned()
            .ok_or(ModelToolAdoptionError::IntegrityViolation)?;
        let definition = source
            .catalog()
            .find(source.request().name())
            .ok_or(ModelToolAdoptionError::IntegrityViolation)?;
        if tool_call.state() != crate::ToolCallState::Running
            || tool_call.run_id() != source.invocation().run_id()
            || tool_call.capability() != definition.capability()
            || tool_call.requested_scope() != Some(source.requested_scope())
            || scope.workspace_root_id() != source.requested_scope().workspace_root_id()
            || !crate::scope_is_within(source.requested_scope(), &scope)
        {
            return Err(ModelToolAdoptionError::IntegrityViolation);
        }
        Ok(Self {
            invocation_id: source.invocation().invocation_id().clone(),
            provider_call_id: source.request().provider_call_id().to_owned(),
            tool_call,
            scope,
            command,
        })
    }
    pub fn invocation_id(&self) -> &ModelInvocationId {
        &self.invocation_id
    }
    pub fn provider_call_id(&self) -> &str {
        &self.provider_call_id
    }
    pub fn tool_call(&self) -> &ToolCall {
        &self.tool_call
    }
    pub fn scope(&self) -> &WorkspacePathScope {
        &self.scope
    }
    pub fn command(&self) -> &C {
        &self.command
    }
}

pub enum ModelToolExecutionClaim<C> {
    Applied {
        request: ModelToolExecutionRequest<C>,
        events: Vec<StoredSessionEvent>,
    },
    /// No execution request is returned for running or terminal work.
    Duplicate { tool_call: ToolCall },
}

pub struct ModelToolExecutionMutation {
    pub tool_call: ToolCall,
    pub events: Vec<StoredSessionEvent>,
    pub disposition: ModelToolExecutionDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelToolExecutionDisposition {
    Applied,
    Duplicate,
}

pub trait ModelToolExecutionStore: Send + Sync {
    fn claim_model_tool_call(
        &self,
        source: &AdoptModelToolRequest,
        tool_call_id: &ToolCallId,
        event_id: EventId,
    ) -> impl Future<Output = Result<ModelToolExecutionMutation, ModelToolAdoptionError>> + Send;
}
