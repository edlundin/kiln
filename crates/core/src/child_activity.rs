use crate::{
    ChildActivityReference, ContextManifestEntry, MessageId, SessionEventPayload,
    StoredSessionEvent,
};

/// Project only the selected durable event as untrusted context data.
/// Artifact bytes, full child context, and related events are not traversed.
pub fn project_child_activity(
    event: &StoredSessionEvent,
    reaction_message_id: MessageId,
) -> Option<ContextManifestEntry> {
    let reference = ChildActivityReference {
        run_id: event.activity_run_id()?.clone(),
        event_id: event.event_id().clone(),
    };
    let content = match event.payload() {
        SessionEventPayload::SessionCreated { .. } => return None,
        SessionEventPayload::MessageAppended { message } => format!(
            "Child message ({}, {}):\n{}",
            message.role().as_str(),
            message.status().as_str(),
            message.content(),
        ),
        SessionEventPayload::ModelOutputRecorded { chunk } => {
            format!("Child {} update:\n{}", chunk.stream.as_str(), chunk.content,)
        }
        SessionEventPayload::ToolCallOutput {
            stream,
            tool_call_id,
            content,
            ..
        } => format!(
            "Child ToolCall {} {}:\n{}",
            tool_call_id.as_str(),
            stream.as_str(),
            content,
        ),
        SessionEventPayload::ArtifactRegistered {
            artifact,
            tool_call_id,
            stream,
            ..
        } => format!(
            "Child artifact: {}\nMedia type: {}\nBytes: {}\nToolCall: {}\nStream: {}",
            artifact.content_hash().as_str(),
            artifact.media_type(),
            artifact.size(),
            tool_call_id.as_str(),
            stream.as_str(),
        ),
        SessionEventPayload::ToolCallRequested { tool_call }
        | SessionEventPayload::ToolCallDenied { tool_call }
        | SessionEventPayload::ToolCallStateChanged { tool_call } => format!(
            "Child ToolCall {}: {}\nCapability: {}\nExit code: {}",
            tool_call.tool_call_id().as_str(),
            tool_call.state().as_str(),
            tool_call.capability(),
            tool_call
                .exit_code()
                .map_or("none".to_owned(), |code| code.to_string()),
        ),
        SessionEventPayload::ApprovalRequested { approval }
        | SessionEventPayload::ApprovalDecided { approval } => format!(
            "Child approval {}: {}\nToolCall: {}",
            approval.approval_id().as_str(),
            approval.state().as_str(),
            approval.tool_call_id().as_str(),
        ),
        SessionEventPayload::TaskCreated { task }
        | SessionEventPayload::TaskUpdated { task }
        | SessionEventPayload::TaskStateChanged { task }
        | SessionEventPayload::TaskAssigned { task } => format!(
            "Child Task {}: {}\nObjective: {}",
            task.task_id().as_str(),
            task.state().as_str(),
            task.objective(),
        ),
        SessionEventPayload::RunCreated { state, .. }
        | SessionEventPayload::RunStateChanged { state, .. } => {
            format!("Child Run: {}", state.as_str())
        }
        SessionEventPayload::RunQueued { .. } => "Child Run queued.".to_owned(),
        SessionEventPayload::RunCancellationRequested { .. } => {
            "Child cancellation requested.".to_owned()
        }
        SessionEventPayload::RunChildAdded {
            parent_run_id,
            child_run_id,
        } => format!(
            "Child Run {} added under {}.",
            child_run_id.as_str(),
            parent_run_id.as_str(),
        ),
        SessionEventPayload::RunInputQueued { message_id, .. } => {
            format!("Child guidance {} queued.", message_id.as_str())
        }
        SessionEventPayload::RunInterruptRequested { message_id, .. } => {
            format!("Child interrupt requested for {}.", message_id.as_str())
        }
        SessionEventPayload::RunInputDelivered { message_id, .. } => {
            format!("Child guidance {} delivered.", message_id.as_str())
        }
        SessionEventPayload::RunInputFailed { message_id, .. } => {
            format!("Child guidance {} failed.", message_id.as_str())
        }
        SessionEventPayload::RunInputCancelled { message_id, .. } => {
            format!("Child guidance {} cancelled.", message_id.as_str())
        }
        SessionEventPayload::ContextManifestCreated {
            context_manifest_id,
            content_hash,
            entry_count,
            ..
        } => format!(
            "Child ContextManifest {} created.\nHash: {}\nEntries: {}",
            context_manifest_id.as_str(),
            content_hash.as_str(),
            entry_count,
        ),
        SessionEventPayload::ModelInvocationCreated { invocation }
        | SessionEventPayload::ModelInvocationStateChanged { invocation } => format!(
            "Child ModelInvocation {}: {}",
            invocation.invocation_id().as_str(),
            invocation.state().as_str(),
        ),
        SessionEventPayload::UsageObserved { .. } => "Child usage observation recorded.".to_owned(),
    };
    ContextManifestEntry::child_activity_snapshot(reaction_message_id, reference, content).ok()
}
