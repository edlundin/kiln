use std::future::Future;

use crate::{
    EventId, INLINE_TOOL_OUTPUT_LIMIT, ModelInvocationStoreError, ModelToolCatalogError,
    ModelToolRequestError, RunError, RunSnapshot, RunStoreError, SessionEvent, SessionId,
    StoredSessionEvent, ToolCall, ToolCallId, ToolCallResult, ToolOutputStream,
};

pub struct ModelToolCompletionIds {
    pub stdout_event_id: EventId,
    pub stderr_event_id: EventId,
    pub state_event_id: EventId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelToolCompletionDisposition {
    Applied,
    Duplicate,
}

pub struct ModelToolCompletionMutation {
    pub value: RunSnapshot,
    pub events: Vec<StoredSessionEvent>,
    pub disposition: ModelToolCompletionDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelToolCompletionError {
    ToolNotAdopted,
    InvalidTransition,
    OutputRequiresArtifact,
    IdempotencyConflict,
    IntegrityViolation,
    Unavailable,
    Invocation(ModelInvocationStoreError),
    Catalog(ModelToolCatalogError),
    Requests(ModelToolRequestError),
    Run(RunError),
    Store(RunStoreError),
}

pub trait ModelToolCompletionStore: Send + Sync {
    /// The owning executor must have stopped external work. Commit output and
    /// terminal tool state without finishing the Run or dispatching more work.
    fn finish_model_tool_call(
        &self,
        tool_call_id: &ToolCallId,
        result: &ToolCallResult,
        ids: ModelToolCompletionIds,
    ) -> impl Future<Output = Result<ModelToolCompletionMutation, ModelToolCompletionError>> + Send;
}

impl ToolCallResult {
    pub fn validate_native_output(&self) -> Result<(), ModelToolCompletionError> {
        if [self.stdout(), self.stderr()]
            .into_iter()
            .flatten()
            .any(|text| text.len() > INLINE_TOOL_OUTPUT_LIMIT)
        {
            return Err(ModelToolCompletionError::OutputRequiresArtifact);
        }
        Ok(())
    }

    pub fn matches_tool_call(&self, tool: &ToolCall) -> bool {
        self.state() == tool.state()
            && self.stdout() == tool.stdout()
            && self.stderr() == tool.stderr()
            && self.stdout_artifact() == tool.stdout_artifact()
            && self.stderr_artifact() == tool.stderr_artifact()
            && self.exit_code() == tool.exit_code()
    }
}

/// Events contain exactly the persisted terminal output. Artifact bytes must
/// already have been written by the executor before metadata publication.
pub fn model_tool_completion_events(
    session_id: &SessionId,
    terminal: &ToolCall,
    ids: ModelToolCompletionIds,
) -> Vec<SessionEvent> {
    let mut events = Vec::new();
    for (stream, text, artifact, event_id) in [
        (
            ToolOutputStream::Stdout,
            terminal.stdout(),
            terminal.stdout_artifact(),
            ids.stdout_event_id,
        ),
        (
            ToolOutputStream::Stderr,
            terminal.stderr(),
            terminal.stderr_artifact(),
            ids.stderr_event_id,
        ),
    ] {
        if let Some(artifact) = artifact {
            events.push(SessionEvent::artifact_registered(
                event_id,
                session_id.clone(),
                terminal.run_id().clone(),
                terminal.tool_call_id().clone(),
                stream,
                artifact.clone(),
            ));
        } else if let Some(text) = text.filter(|text| !text.is_empty()) {
            events.push(SessionEvent::tool_call_output(
                event_id,
                session_id.clone(),
                terminal.run_id().clone(),
                terminal.tool_call_id().clone(),
                stream,
                text.to_owned(),
            ));
        }
    }
    events.push(SessionEvent::tool_call_state_changed(
        ids.state_event_id,
        session_id.clone(),
        terminal.clone(),
    ));
    events
}
