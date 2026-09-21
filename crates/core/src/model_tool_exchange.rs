use std::{fmt, future::Future};

use serde_json::{Value, json};

use crate::{
    Artifact, CapabilitySupport, ModelInvocation, ModelInvocationCompletionKind, ModelInvocationId,
    ModelInvocationOutcome, ModelInvocationPurpose, ModelInvocationStoreError, ModelToolCatalog,
    ModelToolCatalogError, ModelToolDefinition, ModelToolRequest, ModelToolRequestBatch,
    ModelToolRequestError, Run, RunId, RunStoreError, SessionId, ToolCall, ToolCallId,
};

/// An immutable projection of a completed Kiln tool exchange. It retains source
/// identities, arguments, status, inline output, and artifact metadata, never
/// an executor handle, credential, or host-path binding.
#[derive(Clone, PartialEq, Eq)]
pub struct ModelToolExchange {
    session_id: SessionId,
    invocation_id: ModelInvocationId,
    request: ModelToolRequest,
    definition: ModelToolDefinition,
    tool_call: ToolCall,
    content_json: String,
}

impl fmt::Debug for ModelToolExchange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelToolExchange")
            .field("invocation_id", &self.invocation_id)
            .field("tool_call_id", self.tool_call.tool_call_id())
            .field("state", &self.tool_call.state())
            .finish_non_exhaustive()
    }
}

impl ModelToolExchange {
    pub fn new(
        run: &Run,
        invocation: &ModelInvocation,
        requests: &ModelToolRequestBatch,
        catalog: &ModelToolCatalog,
        position: usize,
        tool_call: ToolCall,
    ) -> Result<Self, ModelToolExchangeError> {
        if invocation.run_id() != run.run_id()
            || tool_call.run_id() != run.run_id()
            || requests.invocation_id() != invocation.invocation_id()
            || invocation.outcome()
                != Some(ModelInvocationOutcome::completed(
                    ModelInvocationCompletionKind::ToolRequests,
                ))
            || invocation.purpose() != ModelInvocationPurpose::Generation
            || invocation.capabilities().tool_calls() != CapabilitySupport::Supported
        {
            return Err(ModelToolExchangeError::IntegrityViolation);
        }
        if !tool_call.state().is_terminal() {
            return Err(ModelToolExchangeError::ToolNotComplete);
        }
        let stdout_present = tool_call.stdout().is_some() || tool_call.stdout_artifact().is_some();
        let stderr_present = tool_call.stderr().is_some() || tool_call.stderr_artifact().is_some();
        if tool_call.stdout().is_some() && tool_call.stdout_artifact().is_some()
            || tool_call.stderr().is_some() && tool_call.stderr_artifact().is_some()
            || match tool_call.state() {
                crate::ToolCallState::Completed | crate::ToolCallState::Failed => {
                    !stdout_present
                        || !stderr_present
                        || !crate::terminal_exit_matches(tool_call.state(), tool_call.exit_code())
                }
                crate::ToolCallState::Cancelled => stdout_present != stderr_present,
                crate::ToolCallState::Denied => {
                    stdout_present || stderr_present || tool_call.exit_code().is_some()
                }
                _ => true,
            }
        {
            return Err(ModelToolExchangeError::IntegrityViolation);
        }
        let request = requests
            .requests()
            .get(position)
            .ok_or(ModelToolExchangeError::IntegrityViolation)?
            .clone();
        let definition = catalog
            .find(request.name())
            .ok_or(ModelToolExchangeError::IntegrityViolation)?
            .clone();
        if definition.capability() != tool_call.capability() {
            return Err(ModelToolExchangeError::IntegrityViolation);
        }
        let arguments: Value = serde_json::from_str(request.arguments_json())
            .map_err(|_| ModelToolExchangeError::IntegrityViolation)?;
        let mut content = json!({
            "version": 1,
            "session_id": run.session_id().as_str(),
            "run_id": run.run_id().as_str(),
            "model_invocation_id": invocation.invocation_id().as_str(),
            "tool_call_id": tool_call.tool_call_id().as_str(),
            "provider_call_id": request.provider_call_id(),
            "name": request.name(),
            "capability": definition.capability(),
            "revision": definition.revision(),
            "arguments": arguments,
            "state": tool_call.state().as_str(),
            "stdout": tool_call.stdout(),
            "stderr": tool_call.stderr(),
            "stdout_artifact": artifact_json(tool_call.stdout_artifact()),
            "stderr_artifact": artifact_json(tool_call.stderr_artifact()),
            "exit_code": tool_call.exit_code(),
        });
        content.sort_all_objects();
        let content_json = serde_json::to_string(&content)
            .map_err(|_| ModelToolExchangeError::IntegrityViolation)?;
        Ok(Self {
            session_id: run.session_id().clone(),
            invocation_id: invocation.invocation_id().clone(),
            request,
            definition,
            tool_call,
            content_json,
        })
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }
    pub fn run_id(&self) -> &RunId {
        self.tool_call.run_id()
    }
    pub fn invocation_id(&self) -> &ModelInvocationId {
        &self.invocation_id
    }
    pub fn request(&self) -> &ModelToolRequest {
        &self.request
    }
    pub fn definition(&self) -> &ModelToolDefinition {
        &self.definition
    }
    pub fn tool_call(&self) -> &ToolCall {
        &self.tool_call
    }
    pub fn content_json(&self) -> &str {
        &self.content_json
    }
}

fn artifact_json(artifact: Option<&Artifact>) -> Value {
    artifact.map_or(Value::Null, |artifact| {
        json!({
            "content_hash": artifact.content_hash().as_str(),
            "media_type": artifact.media_type(),
            "size": artifact.size(),
        })
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelToolExchangeError {
    NotFound,
    RunMismatch,
    ToolNotComplete,
    IntegrityViolation,
    Unavailable,
    Invocation(ModelInvocationStoreError),
    Catalog(ModelToolCatalogError),
    Requests(ModelToolRequestError),
    Store(RunStoreError),
}

pub trait ModelToolExchangeStore: Send + Sync {
    fn get_model_tool_exchange(
        &self,
        run_id: &RunId,
        tool_call_id: &ToolCallId,
    ) -> impl Future<Output = Result<ModelToolExchange, ModelToolExchangeError>> + Send;
}
