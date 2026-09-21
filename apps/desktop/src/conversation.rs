use std::collections::{BTreeMap, HashMap, HashSet};

use kiln_protocol::{
    ApprovalResponse, ArtifactResponse, MessageDeliveryState, MessageRole, MessageStatus,
    ModelOutputStream, RunInputMode, RunResponse, RunState, SessionEventDataResponse as Event,
    SessionEventResponse, TaskResponse, ToolOutputStream, WorkspaceScopeResponse,
};

pub struct TranscriptItem {
    pub id: String,
    pub source_event_id: String,
    pub actor: String,
    pub content: String,
    pub detail: Option<String>,
    pub run_id: Option<String>,
    pub delivery: Option<MessageDeliveryState>,
    pub attachments: Vec<ArtifactResponse>,
}

#[derive(Clone)]
pub struct RunItem {
    pub run_id: String,
    pub parent_run_id: Option<String>,
    pub task_id: Option<String>,
    pub user_input_mode: RunInputMode,
    pub state: RunState,
    pub requested_scope: Option<WorkspaceScopeResponse>,
}

#[derive(Default)]
pub struct Conversation {
    pub transcript: Vec<TranscriptItem>,
    pub artifacts: BTreeMap<String, ArtifactResponse>,
    pub approvals: BTreeMap<String, ApprovalResponse>,
    pub root_run_id: Option<String>,
    pub runs: BTreeMap<String, RunItem>,
    pub tasks: BTreeMap<String, TaskResponse>,
    entries: HashMap<String, usize>,
    seen_events: HashSet<String>,
}

impl Conversation {
    pub fn root_state(&self) -> Option<RunState> {
        self.root_run_id
            .as_ref()
            .and_then(|id| self.runs.get(id))
            .map(|run| run.state.clone())
    }

    pub fn apply_run(&mut self, run: RunResponse) {
        let item = RunItem {
            run_id: run.run_id.clone(),
            parent_run_id: run.parent_run_id,
            task_id: run.task_id,
            user_input_mode: run.user_input_mode,
            state: run.state,
            requested_scope: run.requested_scope,
        };
        if item.parent_run_id.is_none() {
            self.root_run_id = Some(item.run_id.clone());
        }
        self.runs.insert(item.run_id.clone(), item);
    }

    pub fn child_runs(&self) -> Vec<(&RunItem, usize)> {
        let Some(root) = self.root_run_id.as_deref() else {
            return Vec::new();
        };
        let mut children = BTreeMap::<&str, Vec<&RunItem>>::new();
        for run in self.runs.values() {
            if let Some(parent) = run.parent_run_id.as_deref() {
                children.entry(parent).or_default().push(run);
            }
        }
        let mut tree = Vec::new();
        let mut stack = vec![(root, 0)];
        let mut seen = HashSet::new();
        while let Some((run_id, depth)) = stack.pop() {
            if !seen.insert(run_id) {
                continue;
            }
            if depth > 0 {
                if let Some(run) = self.runs.get(run_id) {
                    tree.push((run, depth));
                }
            }
            if let Some(descendants) = children.get(run_id) {
                stack.extend(
                    descendants
                        .iter()
                        .rev()
                        .map(|run| (run.run_id.as_str(), depth + 1)),
                );
            }
        }
        tree
    }

    pub fn run(&self, run_id: &str) -> Option<&RunItem> {
        self.runs.get(run_id)
    }

    pub fn root_for_run(&self, run_id: &str) -> Option<&str> {
        let mut run = self.run(run_id)?;
        let mut seen = HashSet::new();
        while let Some(parent) = &run.parent_run_id {
            if !seen.insert(run.run_id.as_str()) {
                return None;
            }
            run = self.run(parent)?;
        }
        Some(&run.run_id)
    }

    pub fn task_for_run(&self, run: &RunItem) -> Option<&TaskResponse> {
        run.task_id
            .as_ref()
            .and_then(|task_id| self.tasks.get(task_id))
    }

    pub fn tasks_for_run<'a>(&'a self, run: &'a RunItem) -> impl Iterator<Item = &'a TaskResponse> {
        self.tasks.values().filter(move |task| {
            task.assigned_run_id.as_deref() == Some(run.run_id.as_str())
                || (task.assigned_run_id.is_none()
                    && run.task_id.as_deref() == Some(task.task_id.as_str()))
        })
    }

    pub fn run_label(&self, run_id: &str) -> String {
        let title = self.run(run_id).map_or("Run", |run| {
            self.task_for_run(run).map_or(
                if run.parent_run_id.is_none() {
                    "Root Run"
                } else {
                    "Child Run"
                },
                |task| task.objective.as_str(),
            )
        });
        format!("{title} · {run_id}")
    }

    pub fn transcript_detail(&self, item: &TranscriptItem) -> Option<String> {
        let mut parts = Vec::new();
        if let Some(run_id) = &item.run_id {
            parts.push(self.run_label(run_id));
        }
        if let Some(delivery) = item.delivery {
            parts.push(
                match delivery {
                    MessageDeliveryState::Queued => "Guidance queued",
                    MessageDeliveryState::Delivered => "Guidance delivered",
                    MessageDeliveryState::Failed => "Guidance failed",
                    MessageDeliveryState::Cancelled => "Guidance cancelled",
                }
                .to_owned(),
            );
        }
        parts.extend(item.detail.iter().cloned());
        (!parts.is_empty()).then(|| parts.join(" · "))
    }

    pub fn active_root(&self) -> Option<&str> {
        match self.root_state() {
            Some(
                RunState::Queued
                | RunState::Running
                | RunState::WaitingForApproval
                | RunState::Cancelling,
            ) => self.root_run_id.as_deref(),
            _ => None,
        }
    }

    pub fn apply(&mut self, event: SessionEventResponse) {
        if !self.seen_events.insert(event.event_id.clone()) {
            return;
        }
        let source_event_id = event.event_id;
        match event.event {
            Event::MessageAppended { message } => {
                let key = message.model_invocation_id.as_ref().map_or_else(
                    || format!("message:{}", message.message_id),
                    |id| format!("model:{id}"),
                );
                let actor = match message.role {
                    MessageRole::User => "You",
                    MessageRole::Assistant => "Kiln",
                };
                for attachment in &message.attachments {
                    self.artifacts
                        .insert(attachment.content_hash.clone(), attachment.clone());
                }
                let content = if message.content.is_empty() {
                    format!("{} attachment(s)", message.attachments.len())
                } else {
                    message.content.clone()
                };
                self.replace_entry(TranscriptItem {
                    id: key,
                    source_event_id,
                    actor: actor.to_owned(),
                    content,
                    run_id: message.target_run_id.or(message.origin_run_id),
                    delivery: None,
                    attachments: message.attachments,
                    detail: match message.status {
                        MessageStatus::Complete => message.child_activity.map(|reference| {
                            format!(
                                "Regarding child {} · Event {}",
                                reference.run_id, reference.event_id
                            )
                        }),
                        MessageStatus::Incomplete => Some("Incomplete response".to_owned()),
                    },
                });
            }
            Event::ModelOutputRecorded(output) => {
                let (prefix, actor) = match output.stream {
                    ModelOutputStream::AssistantText => ("model", "Kiln"),
                    ModelOutputStream::ReasoningSummary => ("reasoning", "Reasoning summary"),
                };
                self.append_entry(TranscriptItem {
                    id: format!("{prefix}:{}", output.model_invocation_id),
                    source_event_id,
                    actor: actor.to_owned(),
                    content: output.content,
                    run_id: Some(output.run_id),
                    detail: None,
                    delivery: None,
                    attachments: Vec::new(),
                });
            }
            Event::RunCreated {
                run_id,
                state,
                parent_run_id,
                task_id,
                user_input_mode,
                requested_scope,
                ..
            } => {
                if parent_run_id.is_none() {
                    self.root_run_id = Some(run_id.clone());
                }
                self.runs.insert(
                    run_id.clone(),
                    RunItem {
                        run_id,
                        parent_run_id,
                        task_id,
                        user_input_mode,
                        state,
                        requested_scope,
                    },
                );
            }
            Event::RunStateChanged { run_id, state } => {
                if let Some(run) = self.runs.get_mut(&run_id) {
                    run.state = state;
                }
            }
            Event::RunCancellationRequested { run_id } => {
                if let Some(run) = self.runs.get_mut(&run_id) {
                    run.state = RunState::Cancelling;
                }
            }
            Event::RunInputQueued { message_id, .. } => {
                self.set_delivery(&message_id, MessageDeliveryState::Queued);
            }
            Event::RunInterruptRequested { message_id, .. } => {
                self.set_delivery(&message_id, MessageDeliveryState::Queued);
                if let Some(index) = self.entries.get(&format!("message:{message_id}")) {
                    self.transcript[*index].detail = Some("Interrupt requested".to_owned());
                }
            }
            Event::RunInputDelivered { message_id, .. } => {
                self.set_delivery(&message_id, MessageDeliveryState::Delivered);
            }
            Event::RunInputFailed { message_id, .. } => {
                self.set_delivery(&message_id, MessageDeliveryState::Failed);
            }
            Event::RunInputCancelled { message_id, .. } => {
                self.set_delivery(&message_id, MessageDeliveryState::Cancelled);
            }
            Event::TaskCreated { task }
            | Event::TaskUpdated { task }
            | Event::TaskAssigned { task }
            | Event::TaskStateChanged { task } => {
                self.tasks.insert(task.task_id.clone(), task);
            }
            Event::ApprovalRequested { approval } | Event::ApprovalDecided { approval } => {
                self.approvals
                    .insert(approval.approval_id.clone(), approval);
            }
            Event::ToolCallOutput {
                tool_call_id,
                stream,
                content,
                run_id,
            } => {
                let actor = match stream {
                    ToolOutputStream::Stdout => "Tool output",
                    ToolOutputStream::Stderr => "Tool error output",
                };
                self.append_entry(TranscriptItem {
                    id: format!("tool:{tool_call_id}:{actor}"),
                    source_event_id,
                    actor: actor.to_owned(),
                    content,
                    run_id: Some(run_id),
                    detail: None,
                    delivery: None,
                    attachments: Vec::new(),
                });
            }
            Event::ArtifactRegistered {
                artifact,
                run_id,
                tool_call_id,
                stream,
            } => {
                self.artifacts
                    .insert(artifact.content_hash.clone(), artifact.clone());
                self.replace_entry(TranscriptItem {
                    source_event_id,
                    id: format!(
                        "artifact:{run_id}:{tool_call_id}:{stream:?}:{}",
                        artifact.content_hash
                    ),
                    actor: "Artifact".to_owned(),
                    content: artifact.content_hash.clone(),
                    run_id: Some(run_id),
                    detail: Some("Stored by the daemon".to_owned()),
                    delivery: None,
                    attachments: Vec::new(),
                });
            }
            _ => {}
        }
    }

    fn set_delivery(&mut self, message_id: &str, state: MessageDeliveryState) {
        if let Some(index) = self.entries.get(&format!("message:{message_id}")) {
            self.transcript[*index].delivery = Some(state);
        }
    }

    fn append_entry(&mut self, item: TranscriptItem) {
        if let Some(index) = self.entries.get(&item.id) {
            self.transcript[*index].content.push_str(&item.content);
            self.transcript[*index].source_event_id = item.source_event_id;
        } else {
            self.replace_entry(item);
        }
    }

    fn replace_entry(&mut self, item: TranscriptItem) {
        if let Some(index) = self.entries.get(&item.id) {
            self.transcript[*index] = item;
        } else {
            self.entries.insert(item.id.clone(), self.transcript.len());
            self.transcript.push(item);
        }
    }
}
