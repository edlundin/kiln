use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use gpui::{
    ClipboardEntry, Context, Entity, ExternalPaths, Focusable, Image, IntoElement,
    PathPromptOptions, Render, SharedString, Subscription, Window, div, prelude::*, px,
};
use gpui_component::{
    Disableable, IconName, Selectable, Sizable,
    button::{Button, ButtonVariants},
    input::{Input, InputEvent, InputState, Paste, TextareaState},
};
use kiln_protocol::{
    ApprovalDecision, ApprovalDecisionRequest, ApprovalState, ArtifactResponse,
    MAX_ARTIFACT_UPLOAD_BYTES, MessageDeliveryMode, RunInputMode, RunState, SendRunInputRequest,
    SessionChangeDiffContent, SessionChangeDiffResponse, SessionChangeDiffUnavailableReason,
    SessionChangesResponse, SessionEventResponse, SessionResponse, StartChildRunRequest,
    UsageAccounting, UsageCompleteness, UsageFinality, UsageLedgerEntryResponse,
    UsageLedgerResponse, UsageQuantityRelation, UsageSource, WebSocketFrame, WorkspaceResponse,
};
use tokio::{runtime::Runtime, sync::mpsc, task::JoinHandle};

use crate::{
    accounts::AccountSettings,
    components::{
        ApprovalPanel, AttachmentChip, AttachmentThumbnail, ChildRunRow, ClickHandler, Composer,
        GuidanceComposer, RunStatus, TranscriptRow,
    },
    connection::{self, AttachmentUpload, Connected, ConnectionConfig, Submission},
    conversation::Conversation,
    theme,
};

enum Update {
    ApprovalInspected {
        connection_generation: u64,
        session_id: String,
        tool_call_id: String,
        result: Result<kiln_protocol::ToolCallInspectionResponse, String>,
    },
    Connected {
        generation: u64,
        result: Result<connection::ConnectionResult, String>,
    },
    Event {
        generation: u64,
        session_id: String,
        event: SessionEventResponse,
    },
    Disconnected {
        generation: u64,
        error: String,
    },
    Submitted {
        session_id: String,
        submission: Submission,
        result: Result<(), String>,
    },
    ChildStarted {
        session_id: String,
        result: Result<(), String>,
    },
    Guided {
        session_id: String,
        run_id: String,
        result: Result<(), String>,
    },
    Command {
        session_id: String,
        result: Result<(), String>,
    },
    SessionsLoaded {
        request_id: u64,
        workspace_id: String,
        result: Result<Vec<SessionResponse>, String>,
    },
    SessionOpened {
        request_id: u64,
        workspace_id: String,
        session_id: String,
        result: Result<Connected, String>,
    },
    SessionCreated {
        request_id: u64,
        workspace_id: String,
        result: Result<Connected, String>,
    },
    ArtifactLoaded {
        request_id: u64,
        session_id: String,
        content_hash: String,
        result: Result<kiln_client::ArtifactPreviewDownload, String>,
    },
    ChangesLoaded {
        request_id: u64,
        session_id: String,
        result: Result<SessionChangesResponse, String>,
    },
    ChangeDiffLoaded {
        request_id: u64,
        session_id: String,
        path: String,
        result: Result<SessionChangeDiffResponse, String>,
    },
    UsageLoaded {
        request_id: u64,
        connection_generation: u64,
        destination_generation: u64,
        after: Option<String>,
        result: Result<UsageLedgerResponse, String>,
    },
}

#[derive(Clone)]
struct ChildStart {
    session_id: String,
    parent_run_id: String,
    idempotency_key: String,
    request: StartChildRunRequest,
}

struct GuidanceDraft {
    input: Entity<TextareaState>,
    pending: Option<(String, SendRunInputRequest)>,
    error: Option<String>,
}

#[derive(Clone)]
struct ReactionDraft {
    root_run_id: String,
    reference: kiln_protocol::ChildActivityReference,
}

struct DraftState {
    content: String,
    reaction: Option<ReactionDraft>,
    attachments: Vec<DraftAttachment>,
}

#[derive(Clone)]
enum DraftAttachmentState {
    Local,
    Uploading,
    Ready,
    Failed(String),
}

#[derive(Clone)]
enum DraftAttachmentSource {
    Path(PathBuf),
    Bytes(Vec<u8>),
}

#[derive(Clone)]
struct DraftAttachment {
    source: DraftAttachmentSource,
    name: String,
    media_type: String,
    size: u64,
    state: DraftAttachmentState,
    artifact: Option<ArtifactResponse>,
    thumbnail: Option<AttachmentThumbnail>,
}

const ARTIFACT_PREVIEW_LIMIT: usize = 64 * 1024;

enum ApprovalInspection {
    Loading,
    Loaded(kiln_protocol::ToolCallInspectionResponse),
    Failed(String),
}
const USAGE_PAGE_LIMIT: u64 = 100;

struct ArtifactPreview {
    session_id: String,
    metadata: ArtifactResponse,
    state: ArtifactPreviewState,
}

enum ArtifactPreviewState {
    Loading,
    Ready {
        media_type: String,
        content: String,
        truncated: bool,
    },
    Failed(String),
    Unsupported {
        media_type: String,
        reason: String,
    },
}

enum ChangesState {
    Loading {
        session_id: String,
    },
    Ready {
        session_id: String,
        summary: SessionChangesResponse,
    },
    Failed {
        session_id: String,
        error: String,
    },
}

enum ChangeDiffState {
    Loading {
        session_id: String,
        path: String,
    },
    Ready {
        session_id: String,
        path: String,
        diff: SessionChangeDiffResponse,
    },
    Failed {
        session_id: String,
        path: String,
        error: String,
    },
}

#[derive(Default)]
struct UsageState {
    entries: Vec<UsageLedgerEntryResponse>,
    next_cursor: Option<String>,
    loading: bool,
    loading_more: bool,
    has_loaded: bool,
    error: Option<String>,
    retry_after: Option<String>,
    request_id: u64,
    destination_generation: u64,
    expanded: std::collections::BTreeSet<String>,
}

impl UsageState {
    fn invalidate_request(&mut self) {
        self.request_id = self.request_id.wrapping_add(1);
        self.loading = false;
        self.loading_more = false;
    }

    fn destination_changed(&mut self) {
        self.destination_generation = self.destination_generation.wrapping_add(1);
        self.invalidate_request();
        self.error = None;
        self.retry_after = None;
        self.expanded.clear();
    }

    fn clear_for_store(&mut self) {
        self.invalidate_request();
        self.entries.clear();
        self.next_cursor = None;
        self.has_loaded = false;
        self.error = None;
        self.retry_after = None;
        self.expanded.clear();
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InspectorTab {
    Changes,
    Artifact,
}

pub struct Desktop {
    address: Entity<InputState>,
    token_file: Entity<InputState>,
    repository: Entity<InputState>,
    session: Entity<InputState>,
    workspace_query: Entity<InputState>,
    session_query: Entity<InputState>,
    composer: Entity<TextareaState>,
    daemon: Option<connection::DaemonConnection>,
    connection: Option<Connected>,
    conversation: Conversation,
    approval_inspections: BTreeMap<String, ApprovalInspection>,
    elicitations: BTreeMap<
        (crate::mcp_elicitation::InputId, String),
        Entity<crate::mcp_elicitation::ElicitationPanel>,
    >,
    runtime: Arc<Runtime>,
    updates: mpsc::UnboundedSender<Update>,
    stream: Option<JoinHandle<()>>,
    pending: Option<Submission>,
    pending_child_start: Option<ChildStart>,
    pending_by_session: BTreeMap<String, Submission>,
    pending_child_by_session: BTreeMap<String, ChildStart>,
    reaction: Option<ReactionDraft>,
    guidance: BTreeMap<String, GuidanceDraft>,
    guidance_by_session: BTreeMap<String, BTreeMap<String, GuidanceDraft>>,
    drafts: BTreeMap<String, DraftState>,
    attachments: Vec<DraftAttachment>,
    workspaces: Vec<WorkspaceResponse>,
    sessions: Vec<SessionResponse>,
    selected_workspace_id: Option<String>,
    selected_session_id: Option<String>,
    sessions_loading: bool,
    sessions_error: Option<String>,
    browser_open: bool,
    request_id: u64,
    connection_generation: u64,
    event_generation: u64,
    switching_session: Option<u64>,
    online: bool,
    busy: bool,
    show_connection: bool,
    selected_run: Option<String>,
    focused_run: Option<String>,
    show_runs: bool,
    error: Option<String>,
    artifact_request_id: u64,
    artifact_preview: Option<ArtifactPreview>,
    changes_request_id: u64,
    changes_state: Option<ChangesState>,
    change_diff_request_id: u64,
    change_diff_state: Option<ChangeDiffState>,
    inspector_tab: InspectorTab,
    show_usage: bool,
    show_settings: bool,
    account_settings: Option<Entity<AccountSettings>>,
    usage: UsageState,
    _subscriptions: Vec<Subscription>,
}

impl Desktop {
    pub fn new(
        config: ConnectionConfig,
        runtime: Arc<Runtime>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let autoconnect = !config.address.is_empty() && !config.token_file.is_empty();
        let address = text_input(config.address, "Loopback address from kilnd", window, cx);
        let token_file = text_input(
            config.token_file,
            "Path to the daemon credential file",
            window,
            cx,
        );
        let repository = text_input(
            config.repository_path,
            "Git repository for a new session",
            window,
            cx,
        );
        let session = text_input(
            config.session_id,
            "Leave empty to create a session",
            window,
            cx,
        );
        let workspace_query = text_input(String::new(), "Search workspaces", window, cx);
        let session_query = text_input(String::new(), "Search sessions", window, cx);
        let composer = cx.new(|cx| {
            TextareaState::new(window, cx)
                .placeholder("Message Kiln…")
                .submit_on_enter(true)
                .auto_grow(2, 5)
        });
        let subscription = cx.subscribe_in(&composer, window, |this, _, event, window, cx| {
            if matches!(event, InputEvent::PressEnter { shift: false, .. }) {
                this.submit(window, cx);
            }
        });
        let workspace_query_subscription =
            cx.subscribe_in(&workspace_query, window, |_this, _, event, _, cx| {
                if matches!(event, InputEvent::Change) {
                    cx.notify();
                }
            });
        let session_query_subscription =
            cx.subscribe_in(&session_query, window, |_this, _, event, _, cx| {
                if matches!(event, InputEvent::Change) {
                    cx.notify();
                }
            });
        let (updates, mut receiver) = mpsc::unbounded_channel();
        cx.spawn_in(window, async move |this, cx| {
            while let Some(update) = receiver.recv().await {
                if this
                    .update_in(cx, |this, window, cx| this.apply(update, window, cx))
                    .is_err()
                {
                    break;
                }
            }
        })
        .detach();
        let mut desktop = Self {
            address,
            token_file,
            repository,
            session,
            workspace_query,
            session_query,
            composer,
            daemon: None,
            connection: None,
            conversation: Conversation::default(),
            approval_inspections: BTreeMap::new(),
            elicitations: BTreeMap::new(),
            runtime,
            updates,
            stream: None,
            pending: None,
            pending_child_start: None,
            pending_by_session: BTreeMap::new(),
            pending_child_by_session: BTreeMap::new(),
            reaction: None,
            guidance: BTreeMap::new(),
            guidance_by_session: BTreeMap::new(),
            drafts: BTreeMap::new(),
            attachments: Vec::new(),
            workspaces: Vec::new(),
            sessions: Vec::new(),
            selected_workspace_id: None,
            selected_session_id: None,
            sessions_loading: false,
            sessions_error: None,
            browser_open: true,
            request_id: 0,
            connection_generation: 0,
            event_generation: 0,
            switching_session: None,
            online: false,
            busy: false,
            show_connection: true,
            selected_run: None,
            focused_run: None,
            show_runs: false,
            error: None,
            artifact_request_id: 0,
            artifact_preview: None,
            changes_request_id: 0,
            changes_state: None,
            change_diff_request_id: 0,
            change_diff_state: None,
            inspector_tab: InspectorTab::Changes,
            show_usage: false,
            show_settings: false,
            account_settings: None,
            usage: UsageState::default(),
            _subscriptions: vec![
                subscription,
                workspace_query_subscription,
                session_query_subscription,
            ],
        };
        if autoconnect {
            desktop.connect(cx);
        }
        desktop
    }

    fn connect(&mut self, cx: &mut Context<Self>) {
        if self.busy || self.switching_session.is_some() {
            return;
        }
        self.stash_session_state(cx);
        if let Some(stream) = self.stream.take() {
            stream.abort();
        }
        self.connection_generation = self.connection_generation.wrapping_add(1);
        // A settings entity belongs to one authenticated daemon connection. In-flight
        // responses cannot populate a replacement daemon's account view.
        self.account_settings = None;
        self.clear_elicitations(cx);
        self.event_generation = self.event_generation.wrapping_add(1);
        self.usage.invalidate_request();
        self.usage.error = None;
        self.usage.retry_after = None;
        let generation = self.connection_generation;
        self.online = false;
        self.busy = true;
        self.error = None;
        let config = ConnectionConfig {
            address: self.address.read(cx).value().to_string(),
            token_file: self.token_file.read(cx).value().to_string(),
            repository_path: self.repository.read(cx).value().to_string(),
            session_id: self.session.read(cx).value().to_string(),
        };
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let _ = updates.send(Update::Connected {
                generation,
                result: connection::connect(config).await,
            });
        });
        cx.notify();
    }

    fn subscribe(&mut self, generation: u64) {
        let Some(connected) = &self.connection else {
            return;
        };
        let client = connected.client.clone();
        let negotiated = connected.negotiated.clone();
        let after = connected.initial_events.current_event_cursor.clone();
        let session_id = connected.session.session_id.clone();
        let updates = self.updates.clone();
        self.stream = Some(self.runtime.spawn(async move {
            let mut stream = match client.subscribe_events(&negotiated, Some(&after)).await {
                Ok(stream) => stream,
                Err(error) => {
                    let _ = updates.send(Update::Disconnected {
                        generation,
                        error: connection::error_message("Connect event stream", &error),
                    });
                    return;
                }
            };
            loop {
                match stream.next_frame().await {
                    Ok(Some(WebSocketFrame::Event { event })) if event.session_id == session_id => {
                        if updates
                            .send(Update::Event {
                                generation,
                                session_id: session_id.clone(),
                                event,
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(Some(WebSocketFrame::Error { code, .. })) => {
                        let _ = updates.send(Update::Disconnected {
                            generation,
                            error: format!("Event stream: {code}. Reconnect to resume."),
                        });
                        break;
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        let _ = updates.send(Update::Disconnected {
                            generation,
                            error: "Connection closed. Reconnect to resume.".to_owned(),
                        });
                        break;
                    }
                    Err(error) => {
                        let _ = updates.send(Update::Disconnected {
                            generation,
                            error: connection::error_message("Read event stream", &error),
                        });
                        break;
                    }
                }
            }
        }));
    }

    fn submit(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if self.busy
            || !self.online
            || self.switching_session.is_some()
            || self.conversation.root_state() == Some(RunState::Cancelling)
        {
            return;
        }
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.append_uncertain && !pending.append_retry_ready)
        {
            return;
        }
        let Some(connected) = &self.connection else {
            return;
        };
        let session_id = connected.session.session_id.clone();
        let content = self.composer.read(cx).value().to_string();
        if content.trim().is_empty() && self.attachments.is_empty() && self.pending.is_none() {
            return;
        }
        let mut submission = self.pending.take().unwrap_or_else(|| Submission {
            session_id: session_id.clone(),
            content,
            attachments: self
                .attachments
                .iter()
                .filter_map(|attachment| attachment.artifact.clone())
                .collect(),
            uploads: self
                .attachments
                .iter()
                .filter(|attachment| {
                    matches!(
                        attachment.state,
                        DraftAttachmentState::Local | DraftAttachmentState::Failed(_)
                    )
                })
                .map(|attachment| {
                    let (path, bytes) = match &attachment.source {
                        DraftAttachmentSource::Path(path) => {
                            (Some(path.to_string_lossy().into_owned()), None)
                        }
                        DraftAttachmentSource::Bytes(bytes) => (None, Some(bytes.clone())),
                    };
                    AttachmentUpload {
                        path,
                        bytes,
                        media_type: attachment.media_type.clone(),
                    }
                })
                .collect(),
            idempotency_key: ulid::Ulid::generate().to_string(),
            active_run_id: self
                .reaction
                .as_ref()
                .map(|draft| draft.root_run_id.clone())
                .or_else(|| self.conversation.active_root().map(str::to_owned)),
            child_activity: self.reaction.as_ref().map(|draft| draft.reference.clone()),
            message_appended: false,
            append_uncertain: false,
            append_retry_ready: false,
        });
        // Each uncertain append needs a fresh history load before an exact retry.
        submission.append_retry_ready = false;
        let client = connected.client.clone();
        let session = session_id.clone();
        let root = connected.workspace.roots[0].workspace_root_id.clone();
        let updates = self.updates.clone();
        if !submission.uploads.is_empty() {
            for attachment in &mut self.attachments {
                if matches!(
                    attachment.state,
                    DraftAttachmentState::Local | DraftAttachmentState::Failed(_)
                ) {
                    attachment.state = DraftAttachmentState::Uploading;
                }
            }
        }
        self.busy = true;
        self.error = None;
        self.runtime.spawn(async move {
            let result = connection::submit(&client, &session, &root, &mut submission).await;
            let _ = updates.send(Update::Submitted {
                session_id: session,
                submission,
                result,
            });
        });
        cx.notify();
    }

    fn pick_attachments(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy || !self.online || self.switching_session.is_some() || self.pending.is_some() {
            return;
        }
        let Some(session_id) = self.active_session_id().map(str::to_owned) else {
            return;
        };
        let generation = self.event_generation;
        let receiver = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: true,
            prompt: Some("Attach files".into()),
        });
        cx.spawn_in(window, async move |this, cx| {
            let Ok(Ok(Some(paths))) = receiver.await else {
                return;
            };
            let _ = this.update_in(cx, |this, _window, cx| {
                // The native dialog can outlive its Session or daemon connection.
                if this.event_generation != generation
                    || this.active_session_id() != Some(session_id.as_str())
                    || this.busy
                    || !this.online
                    || this.switching_session.is_some()
                    || this.pending.is_some()
                {
                    return;
                }
                this.add_attachment_paths(paths);
                cx.notify();
            });
        })
        .detach();
    }

    fn add_attachment_paths(&mut self, paths: Vec<PathBuf>) {
        if self.busy || self.switching_session.is_some() || self.pending.is_some() {
            return;
        }
        for path in paths {
            if self.attachments.iter().any(|attachment| {
                matches!(
                    &attachment.source,
                    DraftAttachmentSource::Path(existing) if existing == &path
                )
            }) {
                continue;
            }
            let Ok(metadata) = std::fs::symlink_metadata(&path) else {
                continue;
            };
            if !metadata.file_type().is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let media_type = media_type_for_path(&path);
            let (state, thumbnail) = if metadata.len() > MAX_ARTIFACT_UPLOAD_BYTES as u64 {
                (
                    DraftAttachmentState::Failed(format!(
                        "attachment exceeds the {} MiB upload limit",
                        MAX_ARTIFACT_UPLOAD_BYTES / (1024 * 1024)
                    )),
                    None,
                )
            } else {
                (
                    DraftAttachmentState::Local,
                    media_type
                        .starts_with("image/")
                        .then_some(AttachmentThumbnail::Icon(IconName::File)),
                )
            };
            self.attachments.push(DraftAttachment {
                name: name.to_owned(),
                size: metadata.len(),
                source: DraftAttachmentSource::Path(path),
                media_type,
                state,
                artifact: None,
                thumbnail,
            });
        }
    }

    fn add_clipboard_image(&mut self, image: &Image) {
        if image.bytes.len() > MAX_ARTIFACT_UPLOAD_BYTES {
            return;
        }
        if self.attachments.iter().any(|attachment| {
            matches!(&attachment.source, DraftAttachmentSource::Bytes(bytes) if bytes == &image.bytes)
        }) {
            return;
        }
        self.attachments.push(DraftAttachment {
            source: DraftAttachmentSource::Bytes(image.bytes.clone()),
            name: format!("pasted-image.{}", image.format.extension()),
            media_type: image.format.mime_type().to_owned(),
            size: image.bytes.len() as u64,
            state: DraftAttachmentState::Local,
            artifact: None,
            thumbnail: Some(AttachmentThumbnail::Icon(IconName::File)),
        });
    }

    fn paste_from_clipboard(
        &mut self,
        _action: &Paste,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.busy || !self.online || self.switching_session.is_some() || self.pending.is_some() {
            cx.stop_propagation();
            return;
        }
        let Some(item) = cx.read_from_clipboard() else {
            return;
        };
        let mut attached = false;
        for entry in &item.entries {
            match entry {
                ClipboardEntry::ExternalPaths(paths) => {
                    self.add_attachment_paths(paths.paths().to_vec());
                    attached = true;
                }
                ClipboardEntry::Image(image) => {
                    self.add_clipboard_image(image);
                    attached = true;
                }
                ClipboardEntry::String(_) => {}
            }
        }
        if attached {
            cx.stop_propagation();
            cx.notify();
        }
    }

    fn attachment_edits_blocked(&self) -> bool {
        self.busy
            || self.switching_session.is_some()
            || self.pending.as_ref().is_some_and(|submission| {
                submission.message_appended || submission.append_uncertain
            })
    }

    fn remove_attachment(&mut self, index: usize, cx: &mut Context<Self>) {
        if self.attachment_edits_blocked() || index >= self.attachments.len() {
            return;
        }
        let removed = self.attachments.remove(index);
        if let Some(submission) = self
            .pending
            .as_mut()
            .filter(|submission| !submission.message_appended)
        {
            if removed.artifact.is_some() {
                let artifact_index = self
                    .attachments
                    .iter()
                    .take(index)
                    .filter(|attachment| attachment.artifact.is_some())
                    .count();
                if artifact_index < submission.attachments.len() {
                    submission.attachments.remove(artifact_index);
                }
            } else {
                let upload_index = self
                    .attachments
                    .iter()
                    .take(index)
                    .filter(|attachment| attachment.artifact.is_none())
                    .count();
                if upload_index < submission.uploads.len() {
                    submission.uploads.remove(upload_index);
                }
            }
        }
        if self.attachments.is_empty()
            && self
                .pending
                .as_ref()
                .is_some_and(|submission| !submission.message_appended)
            && self.composer.read(cx).value().trim().is_empty()
        {
            self.pending = None;
            self.error = None;
        }
        cx.notify();
    }

    fn retry_attachment(&mut self, index: usize, window: &mut Window, cx: &mut Context<Self>) {
        if self.attachment_edits_blocked() || !self.online {
            return;
        }
        if let Some(attachment) = self.attachments.get_mut(index)
            && matches!(attachment.state, DraftAttachmentState::Failed(_))
        {
            attachment.state = DraftAttachmentState::Local;
        }
        self.submit(window, cx);
    }

    fn attachment_labels(&self, cx: &mut Context<Self>) -> Vec<AttachmentChip> {
        self.attachments
            .iter()
            .enumerate()
            .map(|(index, attachment)| {
                let state = match &attachment.state {
                    DraftAttachmentState::Local => "local".to_owned(),
                    DraftAttachmentState::Uploading => "uploading".to_owned(),
                    DraftAttachmentState::Ready => "ready".to_owned(),
                    DraftAttachmentState::Failed(error) => format!("failed: {error}"),
                };
                let label = format!(
                    "{} · {} · {} · {}",
                    attachment.name, attachment.media_type, attachment.size, state
                );
                let retry =
                    matches!(attachment.state, DraftAttachmentState::Failed(_)).then(|| {
                        let retry = cx.listener(move |this, _, window, cx| {
                            this.retry_attachment(index, window, cx);
                        });
                        std::rc::Rc::new(retry) as ClickHandler
                    });
                let on_remove = cx.listener(move |this, _, _, cx| {
                    this.remove_attachment(index, cx);
                });
                let on_remove: ClickHandler = std::rc::Rc::new(move |event, window, app| {
                    on_remove(event, window, app);
                });
                AttachmentChip {
                    label: label.into(),
                    thumbnail: attachment.thumbnail.clone(),
                    retry,
                    retry_disabled: self.attachment_edits_blocked() || !self.online,
                    remove: on_remove,
                    remove_disabled: self.attachment_edits_blocked(),
                }
            })
            .collect()
    }

    fn cancel(&mut self, cx: &mut Context<Self>) {
        let Some(run) = self.conversation.active_root().map(str::to_owned) else {
            return;
        };
        self.cancel_run(run, "Stop Run", cx);
    }

    fn select_reaction(
        &mut self,
        run_id: String,
        event_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.busy || !self.online || self.switching_session.is_some() || self.pending.is_some() {
            return;
        }
        let Some(root) = self
            .conversation
            .root_for_run(&run_id)
            .filter(|root| *root != run_id && self.conversation.active_root() == Some(*root))
        else {
            return;
        };
        self.reaction = Some(ReactionDraft {
            root_run_id: root.to_owned(),
            reference: kiln_protocol::ChildActivityReference { run_id, event_id },
        });
        self.show_runs = false;
        self.focused_run = None;
        self.composer.read(cx).focus_handle(cx).focus(window, cx);
        cx.notify();
    }

    fn invalidate_change_diff(&mut self) {
        self.change_diff_request_id = self.change_diff_request_id.wrapping_add(1);
        self.change_diff_state = None;
    }

    fn open_artifact(&mut self, content_hash: String, cx: &mut Context<Self>) {
        if !self.online || self.switching_session.is_some() {
            return;
        }
        let Some(connected) = &self.connection else {
            return;
        };
        let client = connected.client.clone();
        let Some(metadata) = self.conversation.artifacts.get(&content_hash).cloned() else {
            return;
        };
        let Some(session_id) = self.active_session_id().map(str::to_owned) else {
            return;
        };

        self.artifact_request_id = self.artifact_request_id.wrapping_add(1);
        let request_id = self.artifact_request_id;
        self.inspector_tab = InspectorTab::Artifact;
        self.artifact_preview = Some(ArtifactPreview {
            session_id: session_id.clone(),
            metadata,
            state: ArtifactPreviewState::Loading,
        });

        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let result = client
                .get_artifact_preview(&content_hash, ARTIFACT_PREVIEW_LIMIT)
                .await
                .map_err(|error| connection::error_message("Load artifact", &error));
            let _ = updates.send(Update::ArtifactLoaded {
                request_id,
                session_id,
                content_hash,
                result,
            });
        });
        cx.notify();
    }

    fn close_artifact_preview(&mut self) {
        self.artifact_request_id = self.artifact_request_id.wrapping_add(1);
        self.artifact_preview = None;
        if self.changes_state.is_some() {
            self.inspector_tab = InspectorTab::Changes;
        }
    }

    fn close_inspector(&mut self) {
        self.artifact_request_id = self.artifact_request_id.wrapping_add(1);
        self.changes_request_id = self.changes_request_id.wrapping_add(1);
        self.invalidate_change_diff();
        self.artifact_preview = None;
        self.changes_state = None;
        self.inspector_tab = InspectorTab::Changes;
    }

    fn request_changes(&mut self, session_id: String, cx: &mut Context<Self>) {
        if !self.online || self.switching_session.is_some() {
            return;
        }
        let Some(client) = self
            .connection
            .as_ref()
            .map(|connected| connected.client.clone())
        else {
            return;
        };

        self.invalidate_change_diff();
        self.changes_request_id = self.changes_request_id.wrapping_add(1);
        let request_id = self.changes_request_id;
        self.inspector_tab = InspectorTab::Changes;
        self.changes_state = Some(ChangesState::Loading {
            session_id: session_id.clone(),
        });
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let result = client
                .list_session_changes(&session_id)
                .await
                .map_err(|error| connection::error_message("Load changes", &error));
            let _ = updates.send(Update::ChangesLoaded {
                request_id,
                session_id,
                result,
            });
        });
        cx.notify();
    }

    fn request_change_diff(&mut self, session_id: String, path: String, cx: &mut Context<Self>) {
        if !self.online
            || self.switching_session.is_some()
            || self.active_session_id() != Some(session_id.as_str())
        {
            return;
        }
        let Some(client) = self
            .connection
            .as_ref()
            .map(|connected| connected.client.clone())
        else {
            return;
        };

        self.change_diff_request_id = self.change_diff_request_id.wrapping_add(1);
        let request_id = self.change_diff_request_id;
        self.inspector_tab = InspectorTab::Changes;
        self.change_diff_state = Some(ChangeDiffState::Loading {
            session_id: session_id.clone(),
            path: path.clone(),
        });
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let result = client
                .get_session_change_diff(&session_id, &path)
                .await
                .map_err(|error| connection::error_message("Load change diff", &error));
            let _ = updates.send(Update::ChangeDiffLoaded {
                request_id,
                session_id,
                path,
                result,
            });
        });
        cx.notify();
    }

    fn retry_change_diff(&mut self, cx: &mut Context<Self>) {
        let Some((session_id, path)) =
            self.change_diff_state
                .as_ref()
                .and_then(|state| match state {
                    ChangeDiffState::Loading { .. } => None,
                    ChangeDiffState::Ready {
                        session_id, path, ..
                    }
                    | ChangeDiffState::Failed {
                        session_id, path, ..
                    } => Some((session_id.clone(), path.clone())),
                })
        else {
            return;
        };
        self.request_change_diff(session_id, path, cx);
    }

    fn back_from_change_diff(&mut self, cx: &mut Context<Self>) {
        self.invalidate_change_diff();
        self.inspector_tab = InspectorTab::Changes;
        cx.notify();
    }

    fn request_active_changes(&mut self, cx: &mut Context<Self>) {
        let Some(session_id) = self.active_session_id().map(str::to_owned) else {
            return;
        };
        self.request_changes(session_id, cx);
    }

    fn toggle_changes(&mut self, cx: &mut Context<Self>) {
        if self.changes_state.is_some() {
            self.close_inspector();
            cx.notify();
        } else {
            self.request_active_changes(cx);
        }
    }

    fn cancel_run(&mut self, run: String, operation: &'static str, cx: &mut Context<Self>) {
        if self.busy
            || !self.online
            || self.switching_session.is_some()
            || !self
                .conversation
                .run(&run)
                .is_some_and(|run| run_accepts_commands(&run.state))
        {
            return;
        }
        let Some(connected) = &self.connection else {
            return;
        };
        let session_id = connected.session.session_id.clone();
        let client = connected.client.clone();
        let updates = self.updates.clone();
        self.busy = true;
        self.runtime.spawn(async move {
            let result = client
                .cancel_run(&run)
                .await
                .map(|_| ())
                .map_err(|error| connection::error_message(operation, &error));
            let _ = updates.send(Update::Command { session_id, result });
        });
        cx.notify();
    }

    fn start_child(&mut self, cx: &mut Context<Self>) {
        if self.busy || !self.online || self.switching_session.is_some() {
            return;
        }
        let Some(connected) = &self.connection else {
            return;
        };
        let session_id = connected.session.session_id.clone();
        let command = if let Some(command) = self.pending_child_start.clone() {
            command
        } else {
            let Some(parent_run_id) = self.conversation.active_root().map(str::to_owned) else {
                return;
            };
            if !self
                .conversation
                .run(&parent_run_id)
                .is_some_and(|run| run_accepts_commands(&run.state))
            {
                return;
            }
            let Some(scope) = self
                .conversation
                .run(&parent_run_id)
                .and_then(|run| run.requested_scope.as_ref())
            else {
                self.error = Some(
                    "The root Run has no repository scope. Reconnect to reload its details."
                        .to_owned(),
                );
                cx.notify();
                return;
            };
            ChildStart {
                session_id,
                parent_run_id,
                idempotency_key: ulid::Ulid::generate().to_string(),
                request: StartChildRunRequest {
                    approval_policy: kiln_protocol::ApprovalPolicy::Ask,
                    workspace_root_id: scope.workspace_root_id.clone(),
                    relative_directory: scope.relative_directory.clone(),
                    user_input_mode: RunInputMode::Interactive,
                    task_id: None,
                },
            }
        };
        let session_id = command.session_id.clone();
        let client = connected.client.clone();
        let updates = self.updates.clone();
        self.pending_child_start = Some(command.clone());
        self.busy = true;
        self.error = None;
        self.runtime.spawn(async move {
            let result = client
                .start_child_run(
                    &command.parent_run_id,
                    &command.idempotency_key,
                    &command.request,
                )
                .await
                .map(|_| ())
                .map_err(|error| connection::error_message("Start child Run", &error));
            let _ = updates.send(Update::ChildStarted { session_id, result });
        });
        cx.notify();
    }

    fn retry(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.pending.is_some() {
            self.submit(window, cx);
        } else if self.pending_child_start.is_some() {
            self.start_child(cx);
        }
    }

    fn send_guidance(
        &mut self,
        run_id: String,
        delivery_mode: MessageDeliveryMode,
        cx: &mut Context<Self>,
    ) {
        if self.busy || !self.online || self.switching_session.is_some() {
            return;
        }
        let Some(connected) = &self.connection else {
            return;
        };
        let session_id = connected.session.session_id.clone();
        let Some(draft) = self.guidance.get_mut(&run_id) else {
            return;
        };
        let command = if let Some(command) = &draft.pending {
            command.clone()
        } else {
            if !self.conversation.run(&run_id).is_some_and(|run| {
                run.parent_run_id.is_some()
                    && run.user_input_mode == RunInputMode::Interactive
                    && run_accepts_commands(&run.state)
            }) {
                return;
            }
            let content = draft.input.read(cx).value().to_string();
            if content.trim().is_empty() {
                return;
            }
            (
                ulid::Ulid::generate().to_string(),
                SendRunInputRequest {
                    content,
                    attachments: Vec::new(),
                    delivery_mode,
                },
            )
        };
        draft.pending = Some(command.clone());
        draft.error = None;
        self.busy = true;
        let client = connected.client.clone();
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let result = client
                .send_run_input(&run_id, &command.0, &command.1)
                .await
                .map(|_| ())
                .map_err(|error| connection::error_message("Child guidance", &error));
            let _ = updates.send(Update::Guided {
                session_id,
                run_id,
                result,
            });
        });
        cx.notify();
    }

    fn decide(&mut self, tool_call: String, decision: ApprovalDecision, cx: &mut Context<Self>) {
        if decision == ApprovalDecision::Approved
            && !matches!(
                self.approval_inspections.get(&tool_call),
                Some(ApprovalInspection::Loaded(_))
            )
        {
            return;
        }
        if self.busy || !self.online || self.switching_session.is_some() {
            return;
        }
        let Some(connected) = &self.connection else {
            return;
        };
        let session_id = connected.session.session_id.clone();
        let client = connected.client.clone();
        let updates = self.updates.clone();
        let key = ulid::Ulid::generate().to_string();
        self.busy = true;
        self.runtime.spawn(async move {
            let result = client
                .decide_approval(&tool_call, &key, &ApprovalDecisionRequest { decision })
                .await
                .map(|_| ())
                .map_err(|error| connection::error_message("Decide approval", &error));
            let _ = updates.send(Update::Command { session_id, result });
        });
        cx.notify();
    }

    fn clear_elicitations(&mut self, cx: &mut Context<Self>) {
        for (_, panel) in std::mem::take(&mut self.elicitations) {
            panel.update(cx, |panel, cx| panel.invalidate(cx));
        }
    }

    fn ensure_elicitations(&mut self, cx: &mut Context<Self>) {
        if !self.online || self.switching_session.is_some() || self.connection.is_none() {
            self.clear_elicitations(cx);
            return;
        }
        let pending = self
            .conversation
            .elicitation_inputs
            .iter()
            .filter_map(|(id, source)| {
                let owner = self.conversation.interaction_owner(source)?.to_owned();
                Some(((id.clone(), owner), source.clone()))
            })
            .collect::<BTreeMap<_, _>>();
        self.elicitations.retain(|key, panel| {
            if pending.contains_key(key) {
                true
            } else {
                panel.update(cx, |panel, cx| panel.invalidate(cx));
                false
            }
        });
        for ((id, owner), source) in pending {
            let key = (id.clone(), owner.clone());
            if self.elicitations.contains_key(&key) {
                continue;
            }
            let client = self.connection.as_ref().unwrap().client.clone();
            let runtime = self.runtime.clone();
            let provenance = format!(
                "{} · Tool {}",
                self.conversation.run_label(&source),
                id.tool_call_id
            );
            let panel = cx.new(|cx| {
                crate::mcp_elicitation::ElicitationPanel::new(
                    client,
                    runtime,
                    id,
                    owner,
                    provenance,
                    std::num::NonZeroUsize::new(ARTIFACT_PREVIEW_LIMIT).unwrap(),
                    cx,
                )
            });
            self.elicitations.insert(key, panel);
        }
    }

    fn ensure_approval_inspections(&mut self) {
        if !self.online || self.switching_session.is_some() {
            return;
        }
        let Some(connected) = &self.connection else {
            return;
        };
        let pending = self
            .conversation
            .approvals
            .values()
            .filter(|approval| approval.state == ApprovalState::Pending)
            .map(|approval| approval.tool_call_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        self.approval_inspections
            .retain(|id, _| pending.contains(id));
        for id in pending {
            if self.approval_inspections.contains_key(&id) {
                continue;
            }
            self.approval_inspections
                .insert(id.clone(), ApprovalInspection::Loading);
            let client = connected.client.clone();
            let session_id = connected.session.session_id.clone();
            let connection_generation = self.connection_generation;
            let updates = self.updates.clone();
            self.runtime.spawn(async move {
                // Reuse the desktop's existing bounded text-preview allowance.
                // Oversized source fails; approval never uses truncated arguments.
                let budget = std::num::NonZeroUsize::new(ARTIFACT_PREVIEW_LIMIT).unwrap();
                let result = client
                    .inspect_tool_call(&id, budget)
                    .await
                    .map_err(|error| {
                        connection::error_message("Inspect requested operation", &error)
                    });
                let _ = updates.send(Update::ApprovalInspected {
                    connection_generation,
                    session_id,
                    tool_call_id: id,
                    result,
                });
            });
        }
    }

    fn toggle_run_details(&mut self, run_id: String, window: &mut Window, cx: &mut Context<Self>) {
        if self.switching_session.is_some() {
            return;
        }
        if self.selected_run.as_deref() == Some(&run_id) {
            self.selected_run = None;
        } else {
            self.guidance
                .entry(run_id.clone())
                .or_insert_with(|| GuidanceDraft {
                    input: cx.new(|cx| {
                        TextareaState::new(window, cx)
                            .placeholder("Guide this child…")
                            .auto_grow(2, 5)
                    }),
                    pending: None,
                    error: None,
                });
            self.selected_run = Some(run_id);
        }
        cx.notify();
    }

    fn active_session_id(&self) -> Option<&str> {
        self.connection
            .as_ref()
            .map(|connected| connected.session.session_id.as_str())
    }

    fn leave_usage(&mut self) {
        self.show_settings = false;
        if self.show_usage {
            self.show_usage = false;
            self.usage.destination_changed();
        }
    }

    fn toggle_usage(&mut self, cx: &mut Context<Self>) {
        if self.daemon.is_none() {
            return;
        }
        if self.show_usage {
            self.leave_usage();
        } else {
            self.show_settings = false;
            self.show_usage = true;
            self.show_runs = false;
            self.focused_run = None;
            self.close_inspector();
            self.usage.destination_changed();
            self.refresh_usage(cx);
        }
        cx.notify();
    }

    fn toggle_settings(&mut self, cx: &mut Context<Self>) {
        if self.daemon.is_none() {
            return;
        }
        if self.show_settings {
            self.show_settings = false;
        } else {
            self.leave_usage();
            self.show_settings = true;
            self.show_connection = false;
            self.show_runs = false;
            self.focused_run = None;
            self.close_inspector();
            self.ensure_account_settings(cx);
        }
        cx.notify();
    }

    fn ensure_account_settings(&mut self, cx: &mut Context<Self>) {
        if self.account_settings.is_none()
            && self.online
            && let Some(daemon) = &self.daemon
        {
            let client = daemon.client.clone();
            let runtime = self.runtime.clone();
            self.account_settings = Some(cx.new(|cx| AccountSettings::new(client, runtime, cx)));
        }
    }

    fn start_usage_request(&mut self, after: Option<String>, cx: &mut Context<Self>) {
        if !self.online {
            self.usage.invalidate_request();
            self.usage.error = Some("Reconnect to load the global Usage ledger.".to_owned());
            self.usage.retry_after = after;
            cx.notify();
            return;
        }
        let Some(daemon) = self.daemon.as_ref() else {
            self.usage.invalidate_request();
            self.usage.error =
                Some("Connect to a daemon to load the global Usage ledger.".to_owned());
            self.usage.retry_after = after;
            cx.notify();
            return;
        };

        self.usage.request_id = self.usage.request_id.wrapping_add(1);
        let request_id = self.usage.request_id;
        let connection_generation = self.connection_generation;
        let destination_generation = self.usage.destination_generation;
        let is_continuation = after.is_some();
        self.usage.loading = !is_continuation;
        self.usage.loading_more = is_continuation;
        self.usage.error = None;
        self.usage.retry_after = None;

        let client = daemon.client.clone();
        let call_after = after.clone();
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let result = client
                .list_usage(call_after.as_deref(), Some(USAGE_PAGE_LIMIT))
                .await
                .map_err(|error| connection::error_message("Load Usage ledger", &error));
            let _ = updates.send(Update::UsageLoaded {
                request_id,
                connection_generation,
                destination_generation,
                after,
                result,
            });
        });
        cx.notify();
    }

    fn refresh_usage(&mut self, cx: &mut Context<Self>) {
        self.start_usage_request(None, cx);
    }

    fn load_more_usage(&mut self, cx: &mut Context<Self>) {
        if self.usage.loading || self.usage.loading_more {
            return;
        }
        let Some(after) = self.usage.next_cursor.clone() else {
            return;
        };
        self.start_usage_request(Some(after), cx);
    }

    fn retry_usage(&mut self, cx: &mut Context<Self>) {
        self.start_usage_request(self.usage.retry_after.clone(), cx);
    }

    fn toggle_usage_entry(&mut self, observation_id: String, cx: &mut Context<Self>) {
        if !self.usage.expanded.insert(observation_id.clone()) {
            self.usage.expanded.remove(&observation_id);
        }
        cx.notify();
    }

    fn request_sessions(&mut self, workspace_id: String) {
        let Some(daemon) = &self.daemon else {
            return;
        };
        self.request_id = self.request_id.wrapping_add(1);
        let request_id = self.request_id;
        let updates = self.updates.clone();
        let daemon = daemon.clone();
        self.sessions.clear();
        self.sessions_loading = true;
        self.sessions_error = None;
        self.runtime.spawn(async move {
            let result = connection::list_sessions(&daemon, &workspace_id).await;
            let _ = updates.send(Update::SessionsLoaded {
                request_id,
                workspace_id,
                result,
            });
        });
    }

    fn select_workspace(&mut self, workspace_id: String, cx: &mut Context<Self>) {
        if self.busy || self.switching_session.is_some() {
            return;
        }
        if !self
            .workspaces
            .iter()
            .any(|workspace| workspace.workspace_id == workspace_id)
        {
            return;
        }
        self.selected_workspace_id = Some(workspace_id.clone());
        if self.show_usage {
            self.usage.destination_changed();
        }
        self.selected_session_id = self
            .active_session_id()
            .filter(|session_id| {
                self.connection.as_ref().is_some_and(|connected| {
                    connected.workspace.workspace_id == workspace_id
                        && *session_id == connected.session.session_id
                })
            })
            .map(str::to_owned);
        self.request_sessions(workspace_id);
        if self.show_usage && self.online {
            self.refresh_usage(cx);
        }
        cx.notify();
    }

    fn stash_session_state(&mut self, cx: &mut Context<Self>) {
        self.close_inspector();
        let Some(session_id) = self.active_session_id().map(str::to_owned) else {
            return;
        };
        let content = self.composer.read(cx).value().to_string();
        let reaction = self.reaction.take();
        if !content.is_empty() || reaction.is_some() || !self.attachments.is_empty() {
            self.drafts.insert(
                session_id.clone(),
                DraftState {
                    content,
                    reaction,
                    attachments: std::mem::take(&mut self.attachments),
                },
            );
        } else {
            self.drafts.remove(&session_id);
            self.attachments.clear();
        }
        if let Some(pending) = self.pending.take() {
            self.pending_by_session.insert(session_id.clone(), pending);
        }
        if let Some(pending) = self.pending_child_start.take() {
            self.pending_child_by_session
                .insert(session_id.clone(), pending);
        }
        if !self.guidance.is_empty() {
            self.guidance_by_session
                .insert(session_id, std::mem::take(&mut self.guidance));
        }
    }

    fn restore_session_state(
        &mut self,
        session_id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(draft) = self.drafts.remove(session_id) {
            self.composer
                .update(cx, |input, cx| input.set_value(draft.content, window, cx));
            self.reaction = draft.reaction;
            self.attachments = draft.attachments;
        } else {
            self.composer
                .update(cx, |input, cx| input.set_value("", window, cx));
            self.reaction = None;
            self.attachments.clear();
        }
        self.pending = self.pending_by_session.remove(session_id);
        self.pending_child_start = self.pending_child_by_session.remove(session_id);
        self.guidance = self
            .guidance_by_session
            .remove(session_id)
            .unwrap_or_default();
    }

    fn clear_saved_session_state(&mut self) {
        self.close_inspector();
        self.usage.clear_for_store();
        self.pending = None;
        self.pending_child_start = None;
        self.pending_by_session.clear();
        self.pending_child_by_session.clear();
        self.reaction = None;
        self.attachments.clear();
        self.guidance.clear();
        self.guidance_by_session.clear();
        self.drafts.clear();
    }

    fn select_session(&mut self, session_id: String, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy || self.switching_session.is_some() {
            return;
        }
        let Some(workspace_id) = self.selected_workspace_id.clone() else {
            return;
        };
        if !self
            .sessions
            .iter()
            .any(|session| session.session_id == session_id)
        {
            return;
        }
        self.leave_usage();
        if self.active_session_id() == Some(session_id.as_str()) {
            self.composer.read(cx).focus_handle(cx).focus(window, cx);
            return;
        }
        let Some(daemon) = self.daemon.clone() else {
            return;
        };
        self.stash_session_state(cx);
        if let Some(stream) = self.stream.take() {
            stream.abort();
        }
        self.event_generation = self.event_generation.wrapping_add(1);
        self.request_id = self.request_id.wrapping_add(1);
        let request_id = self.request_id;
        self.switching_session = Some(request_id);
        self.selected_session_id = Some(session_id.clone());
        self.sessions_loading = true;
        self.sessions_error = None;
        let updates = self.updates.clone();
        let target_session_id = session_id.clone();
        self.runtime.spawn(async move {
            let result = connection::open_session(&daemon, &target_session_id).await;
            let _ = updates.send(Update::SessionOpened {
                request_id,
                workspace_id,
                session_id: target_session_id,
                result,
            });
        });
        cx.notify();
    }

    fn create_session(&mut self, cx: &mut Context<Self>) {
        if self.busy || self.switching_session.is_some() {
            return;
        }
        let Some(workspace_id) = self.selected_workspace_id.clone() else {
            return;
        };
        let Some(daemon) = self.daemon.clone() else {
            return;
        };
        self.leave_usage();
        self.stash_session_state(cx);
        if let Some(stream) = self.stream.take() {
            stream.abort();
        }
        self.event_generation = self.event_generation.wrapping_add(1);
        self.request_id = self.request_id.wrapping_add(1);
        let request_id = self.request_id;
        self.switching_session = Some(request_id);
        self.selected_session_id = None;
        self.sessions_loading = true;
        self.sessions_error = None;
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let result = connection::create_session(&daemon, &workspace_id).await;
            let _ = updates.send(Update::SessionCreated {
                request_id,
                workspace_id,
                result,
            });
        });
        cx.notify();
    }

    fn install_connected(
        &mut self,
        connected: Connected,
        restore_saved_state: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.close_inspector();
        let session_id = connected.session.session_id.clone();
        let workspace_id = connected.workspace.workspace_id.clone();
        self.daemon = Some(connected.daemon.clone());
        self.workspaces = connected.daemon.workspaces.clone();
        if !self
            .workspaces
            .iter()
            .any(|workspace| workspace.workspace_id == connected.workspace.workspace_id)
        {
            self.workspaces.push(connected.workspace.clone());
        }
        self.selected_workspace_id = Some(connected.workspace.workspace_id.clone());
        self.selected_session_id = Some(session_id.clone());
        self.conversation = Conversation::default();
        self.approval_inspections.clear();
        self.clear_elicitations(cx);
        for run in connected.initial_runs.runs.iter().cloned() {
            self.conversation.apply_run(run);
        }
        for event in connected.initial_events.events.iter().cloned() {
            self.conversation.apply(event);
        }
        self.session.update(cx, |input, cx| {
            input.set_value(session_id.clone(), window, cx)
        });
        self.connection = Some(connected);
        if restore_saved_state {
            self.restore_session_state(&session_id, window, cx);
            self.composer.read(cx).focus_handle(cx).focus(window, cx);
        } else {
            self.pending = None;
            self.reaction = None;
        }
        self.online = true;
        self.busy = false;
        self.switching_session = None;
        if let Some(settings) = &self.account_settings {
            settings.update(cx, |settings, cx| settings.set_online(true, cx));
        }
        self.show_connection = false;
        self.selected_run = None;
        self.focused_run = None;
        self.show_runs = false;
        self.error = None;
        if let Some(pending) = self
            .pending
            .as_mut()
            .filter(|pending| pending.append_uncertain)
        {
            pending.append_retry_ready = true;
            self.error = Some(
                "Message submission was uncertain. History is refreshed; review it, then Retry the same submission."
                    .to_owned(),
            );
        }
        self.event_generation = self.event_generation.wrapping_add(1);
        self.request_sessions(workspace_id);
        self.subscribe(self.event_generation);
        self.request_active_changes(cx);
        if self.show_usage {
            self.refresh_usage(cx);
        }
    }

    fn workspace_navigator(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let query = self
            .workspace_query
            .read(cx)
            .value()
            .to_string()
            .trim()
            .to_ascii_lowercase();
        let selected = self.selected_workspace_id.as_deref();
        let disabled = self.busy || self.switching_session.is_some();
        let mut panel = div()
            .id("workspace-navigator")
            .w(px(280.0))
            .h_full()
            .flex_none()
            .flex()
            .flex_col()
            .gap_3()
            .p_3()
            .bg(theme::CHROME)
            .border_r_1()
            .border_color(theme::BORDER)
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .child(
                        div()
                            .font_family(theme::MONO_FONT)
                            .text_size(px(12.0))
                            .text_color(theme::TEXT)
                            .child("WORKSPACES"),
                    )
                    .child(
                        Button::new("add-workspace")
                            .label("Add")
                            .small()
                            .disabled(self.busy)
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.show_connection = true;
                                cx.notify();
                            })),
                    ),
            )
            .child(Input::new(&self.workspace_query).aria_label("Search workspaces"));

        if self.workspaces.is_empty() {
            panel = panel.child(
                div()
                    .id("workspace-empty")
                    .role(gpui::Role::Status)
                    .text_sm()
                    .text_color(theme::MUTED)
                    .child("No saved workspaces. Add a repository to create one."),
            );
            return panel;
        }

        let mut list = div()
            .id("workspace-list")
            .min_h_0()
            .flex_1()
            .overflow_y_scroll()
            .flex()
            .flex_col()
            .gap_2();
        let mut matching = 0usize;
        for workspace in &self.workspaces {
            let root_text = workspace.roots.first().map_or_else(
                || "No repository root".to_owned(),
                |root| root.display_path.clone(),
            );
            let searchable = format!(
                "{} {} {}",
                workspace.name, workspace.workspace_id, root_text
            )
            .to_ascii_lowercase();
            if !query.is_empty() && !searchable.contains(&query) {
                continue;
            }
            matching += 1;
            let workspace_id = workspace.workspace_id.clone();
            let workspace_name = workspace.name.clone();
            let root_count = workspace.roots.len();
            list = list.child(
                Button::new(SharedString::from(format!("workspace-{workspace_id}")))
                    .ghost()
                    .selected(selected == Some(workspace_id.as_str()))
                    .disabled(disabled)
                    .accessibility_label(format!(
                        "Open workspace {}. {} repository roots.",
                        workspace_name, root_count
                    ))
                    .w_full()
                    .h_auto()
                    .flex_col()
                    .items_start()
                    .gap_1()
                    .px_2()
                    .py_2()
                    .child(div().w_full().text_sm().child(workspace_name))
                    .child(
                        div()
                            .w_full()
                            .font_family(theme::MONO_FONT)
                            .text_size(px(11.0))
                            .line_height(px(16.0))
                            .text_color(theme::MUTED)
                            .child(format!("{root_count} roots · {root_text}")),
                    )
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.select_workspace(workspace_id.clone(), cx);
                    })),
            );
        }
        if matching == 0 {
            list = list.child(
                div()
                    .id("workspace-no-results")
                    .role(gpui::Role::Status)
                    .text_sm()
                    .text_color(theme::MUTED)
                    .child("No workspace matches this search."),
            );
        }
        panel.child(list)
    }

    fn session_drawer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let query = self
            .session_query
            .read(cx)
            .value()
            .to_string()
            .trim()
            .to_ascii_lowercase();
        let selected = self.selected_session_id.as_deref();
        let disabled = self.busy || self.switching_session.is_some();
        let workspace_name = self
            .selected_workspace_id
            .as_deref()
            .and_then(|id| {
                self.workspaces
                    .iter()
                    .find(|workspace| workspace.workspace_id == id)
            })
            .map_or("No workspace selected".to_owned(), |workspace| {
                workspace.name.clone()
            });
        let workspace_context = self.connection.as_ref().map_or_else(
            || workspace_name.clone(),
            |connected| {
                let current = compact_session_id(&connected.session.session_id);
                if connected.workspace.workspace_id
                    == self.selected_workspace_id.as_deref().unwrap_or_default()
                {
                    format!("{workspace_name} · Current {current}")
                } else {
                    format!("Browsing {workspace_name} · Current {current}")
                }
            },
        );
        let mut panel = div()
            .id("session-drawer")
            .w(px(260.0))
            .h_full()
            .flex_none()
            .flex()
            .flex_col()
            .gap_3()
            .p_3()
            .bg(theme::CHROME)
            .border_r_1()
            .border_color(theme::BORDER)
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .child(
                        div()
                            .font_family(theme::MONO_FONT)
                            .text_size(px(12.0))
                            .text_color(theme::TEXT)
                            .child("SESSIONS"),
                    )
                    .child(
                        Button::new("new-session")
                            .label("New")
                            .small()
                            .disabled(disabled || self.selected_workspace_id.is_none())
                            .on_click(cx.listener(|this, _, _, cx| this.create_session(cx))),
                    ),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(theme::MUTED)
                    .child(workspace_context),
            )
            .child(Input::new(&self.session_query).aria_label("Search sessions"));

        if self.sessions_loading {
            panel = panel.child(
                div()
                    .id("session-loading")
                    .role(gpui::Role::Status)
                    .text_sm()
                    .text_color(theme::MUTED)
                    .child(if self.switching_session.is_some() {
                        "Opening session…"
                    } else {
                        "Loading sessions…"
                    }),
            );
        }
        if let Some(error) = self.sessions_error.clone() {
            panel = panel.child(
                div()
                    .id("session-query-error")
                    .role(gpui::Role::Alert)
                    .aria_label(error.clone())
                    .text_sm()
                    .text_color(theme::DANGER)
                    .child(error),
            );
        }
        if !self.sessions_loading && self.sessions_error.is_none() && self.sessions.is_empty() {
            panel = panel.child(
                div()
                    .id("session-empty")
                    .role(gpui::Role::Status)
                    .text_sm()
                    .text_color(theme::MUTED)
                    .child(if self.selected_workspace_id.is_some() {
                        "No saved sessions in this workspace."
                    } else {
                        "Select a workspace to browse sessions."
                    }),
            );
        }

        let mut list = div()
            .id("session-list")
            .min_h_0()
            .flex_1()
            .overflow_y_scroll()
            .flex()
            .flex_col()
            .gap_2();
        let mut matching = 0usize;
        for session in &self.sessions {
            let session_id = session.session_id.clone();
            if !query.is_empty() && !session_id.to_ascii_lowercase().contains(&query) {
                continue;
            }
            matching += 1;
            let is_active = self.active_session_id() == Some(session_id.as_str());
            let state = if is_active {
                self.conversation
                    .root_state()
                    .map(run_state_label)
                    .unwrap_or("Open")
            } else {
                "Saved"
            };
            let label = if is_active {
                format!("Current · {}", compact_session_id(&session_id))
            } else {
                format!("Session · {}", compact_session_id(&session_id))
            };
            let session_id_for_click = session_id.clone();
            list = list.child(
                Button::new(SharedString::from(format!("session-{session_id}")))
                    .ghost()
                    .selected(selected == Some(session_id.as_str()))
                    .disabled(disabled)
                    .accessibility_label(format!("Open Session {session_id}. Status: {state}."))
                    .w_full()
                    .h_auto()
                    .flex_col()
                    .items_start()
                    .gap_1()
                    .px_2()
                    .py_2()
                    .child(div().w_full().text_sm().child(label))
                    .child(
                        div()
                            .w_full()
                            .font_family(theme::MONO_FONT)
                            .text_size(px(11.0))
                            .line_height(px(16.0))
                            .text_color(if is_active {
                                theme::ACCENT
                            } else {
                                theme::MUTED
                            })
                            .child(state),
                    )
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.select_session(session_id_for_click.clone(), window, cx);
                    })),
            );
        }
        if matching == 0 && !self.sessions.is_empty() && !self.sessions_loading {
            list = list.child(
                div()
                    .id("session-no-results")
                    .role(gpui::Role::Status)
                    .text_sm()
                    .text_color(theme::MUTED)
                    .child("No session matches this search."),
            );
        }
        panel.child(list)
    }

    fn apply(&mut self, update: Update, window: &mut Window, cx: &mut Context<Self>) {
        match update {
            Update::Connected { generation, result } => {
                if generation != self.connection_generation {
                    return;
                }
                self.busy = false;
                match result {
                    Ok(connection::ConnectionResult::Browse(daemon)) => {
                        let same_store = self.daemon.as_ref().is_some_and(|previous| {
                            previous.negotiated.store_identity.id
                                == daemon.negotiated.store_identity.id
                        });
                        if !same_store {
                            self.clear_saved_session_state();
                        }
                        self.daemon = Some(daemon.clone());
                        self.workspaces = daemon.workspaces;
                        self.connection = None;
                        self.conversation = Conversation::default();
                        self.approval_inspections.clear();
                        self.clear_elicitations(cx);
                        self.online = true;
                        self.show_connection = false;
                        self.selected_run = None;
                        self.focused_run = None;
                        self.selected_session_id = None;
                        self.sessions.clear();
                        self.sessions_loading = false;
                        self.sessions_error = None;
                        let workspace_id = self
                            .selected_workspace_id
                            .clone()
                            .filter(|id| {
                                self.workspaces
                                    .iter()
                                    .any(|workspace| &workspace.workspace_id == id)
                            })
                            .or_else(|| {
                                self.workspaces
                                    .first()
                                    .map(|workspace| workspace.workspace_id.clone())
                            });
                        self.selected_workspace_id = workspace_id.clone();
                        if let Some(workspace_id) = workspace_id {
                            self.request_sessions(workspace_id);
                        }
                        self.error = None;
                        if self.show_usage {
                            self.refresh_usage(cx);
                        }
                    }
                    Ok(connection::ConnectionResult::Session(connected)) => {
                        let same_session = self.connection.as_ref().is_some_and(|previous| {
                            previous.session.session_id == connected.session.session_id
                                && previous.negotiated.store_identity.id
                                    == connected.negotiated.store_identity.id
                        });
                        let same_store = self.daemon.as_ref().is_some_and(|previous| {
                            previous.negotiated.store_identity.id
                                == connected.negotiated.store_identity.id
                        });
                        if !same_store {
                            self.clear_saved_session_state();
                        }
                        if !same_session {
                            self.pending_child_start = None;
                            self.guidance.clear();
                        }
                        self.install_connected(connected, true, window, cx);
                    }
                    Err(error) => {
                        if let Some(session_id) = self.active_session_id().map(str::to_owned) {
                            self.restore_session_state(&session_id, window, cx);
                        }
                        self.usage.invalidate_request();
                        self.usage.error = Some(format!("Usage unavailable: {error}"));
                        self.usage.retry_after = None;
                        self.show_connection = true;
                        self.error = Some(error);
                    }
                }
            }
            Update::Event {
                generation,
                session_id,
                event,
            } => {
                if generation == self.event_generation
                    && self.online
                    && self.active_session_id() == Some(session_id.as_str())
                {
                    self.conversation.apply(event);
                }
            }
            Update::Disconnected { generation, error } => {
                if generation == self.event_generation {
                    self.clear_elicitations(cx);
                    if let Some(settings) = &self.account_settings {
                        settings.update(cx, |settings, cx| settings.set_online(false, cx));
                    }
                    self.close_inspector();
                    self.usage.invalidate_request();
                    self.usage.error = Some(
                        "Connection lost. Reconnect to refresh the global Usage ledger.".to_owned(),
                    );
                    self.usage.retry_after = None;
                    self.online = false;
                    self.error = Some(error);
                }
            }
            Update::Submitted {
                session_id,
                submission,
                result,
            } => {
                if submission.session_id != session_id
                    || self.active_session_id() != Some(session_id.as_str())
                {
                    if result.is_err() {
                        self.pending_by_session.insert(session_id, submission);
                    }
                    return;
                }
                self.busy = false;
                match &result {
                    Ok(()) => self.attachments.clear(),
                    Err(error) => {
                        let uploaded: Vec<_> = submission
                            .attachments
                            .iter()
                            .filter(|candidate| {
                                !self.attachments.iter().any(|attachment| {
                                    attachment.artifact.as_ref().is_some_and(|artifact| {
                                        artifact.content_hash == candidate.content_hash
                                    })
                                })
                            })
                            .cloned()
                            .collect();
                        let mut uploaded = uploaded.into_iter();
                        for attachment in self.attachments.iter_mut().filter(|attachment| {
                            matches!(
                                attachment.state,
                                DraftAttachmentState::Uploading | DraftAttachmentState::Local
                            )
                        }) {
                            if let Some(artifact) = uploaded.next() {
                                attachment.artifact = Some(artifact);
                                attachment.state = DraftAttachmentState::Ready;
                            } else {
                                attachment.state = DraftAttachmentState::Failed(error.clone());
                            }
                        }
                    }
                }
                match result {
                    Ok(()) => {
                        self.composer
                            .update(cx, |input, cx| input.set_value("", window, cx));
                        self.pending = None;
                        self.reaction = None;
                        self.error = None;
                    }
                    Err(error) => {
                        self.pending = Some(submission);
                        self.error = Some(error);
                    }
                }
            }
            Update::ChildStarted { session_id, result } => {
                if self.active_session_id() != Some(session_id.as_str()) {
                    return;
                }
                self.busy = false;
                match result {
                    Ok(()) => {
                        self.pending_child_start = None;
                        self.error = None;
                    }
                    Err(error) => self.error = Some(error),
                }
            }
            Update::Guided {
                session_id,
                run_id,
                result,
            } => {
                if self.active_session_id() != Some(session_id.as_str()) {
                    return;
                }
                self.busy = false;
                if let Some(draft) = self.guidance.get_mut(&run_id) {
                    match result {
                        Ok(()) => {
                            draft
                                .input
                                .update(cx, |input, cx| input.set_value("", window, cx));
                            draft.pending = None;
                            draft.error = None;
                        }
                        Err(error) => draft.error = Some(error),
                    }
                }
            }
            Update::ApprovalInspected {
                connection_generation,
                session_id,
                tool_call_id,
                result,
            } => {
                if connection_generation != self.connection_generation
                    || self.active_session_id() != Some(session_id.as_str())
                {
                    return;
                }
                let Some(approval) = self.conversation.approvals.values().find(|approval| {
                    approval.tool_call_id == tool_call_id
                        && approval.state == ApprovalState::Pending
                }) else {
                    return;
                };
                let state = match result {
                    Ok(source)
                        if source.tool_call_id == tool_call_id
                            && source.run_id == approval.run_id =>
                    {
                        ApprovalInspection::Loaded(source)
                    }
                    Ok(_) => ApprovalInspection::Failed(
                        "The returned operation does not match this approval.".into(),
                    ),
                    Err(error) => ApprovalInspection::Failed(error),
                };
                self.approval_inspections.insert(tool_call_id, state);
            }
            Update::Command { session_id, result } => {
                if self.active_session_id() != Some(session_id.as_str()) {
                    return;
                }
                self.busy = false;
                self.error = result.err();
            }
            Update::SessionsLoaded {
                request_id,
                workspace_id,
                result,
            } => {
                if request_id != self.request_id
                    || self.selected_workspace_id.as_deref() != Some(workspace_id.as_str())
                {
                    return;
                }
                self.sessions_loading = false;
                match result {
                    Ok(sessions) => {
                        self.sessions = sessions;
                        self.sessions_error = None;
                    }
                    Err(error) => {
                        self.sessions.clear();
                        self.sessions_error = Some(error);
                    }
                }
            }
            Update::SessionOpened {
                request_id,
                workspace_id,
                session_id,
                result,
            } => {
                if self.switching_session != Some(request_id)
                    || self.selected_workspace_id.as_deref() != Some(workspace_id.as_str())
                    || self.selected_session_id.as_deref() != Some(session_id.as_str())
                {
                    return;
                }
                self.sessions_loading = false;
                let result = match result {
                    Ok(connected) if connected.workspace.workspace_id == workspace_id => {
                        Ok(connected)
                    }
                    Ok(_) => Err("session belongs to a different Workspace".to_owned()),
                    Err(error) => Err(error),
                };
                match result {
                    Ok(connected) => self.install_connected(connected, true, window, cx),
                    Err(error) => {
                        self.switching_session = None;
                        self.online = self.connection.is_some();
                        self.selected_session_id = self.active_session_id().map(str::to_owned);
                        if let Some(session_id) = self.active_session_id().map(str::to_owned) {
                            self.restore_session_state(&session_id, window, cx);
                            self.event_generation = self.event_generation.wrapping_add(1);
                            self.subscribe(self.event_generation);
                        }
                        self.error = Some(error);
                    }
                }
            }
            Update::SessionCreated {
                request_id,
                workspace_id,
                result,
            } => {
                if self.switching_session != Some(request_id)
                    || self.selected_workspace_id.as_deref() != Some(workspace_id.as_str())
                {
                    return;
                }
                self.sessions_loading = false;
                let result = match result {
                    Ok(connected) if connected.workspace.workspace_id == workspace_id => {
                        Ok(connected)
                    }
                    Ok(_) => Err("new Session belongs to a different Workspace".to_owned()),
                    Err(error) => Err(error),
                };
                match result {
                    Ok(connected) => self.install_connected(connected, true, window, cx),
                    Err(error) => {
                        self.switching_session = None;
                        self.online = self.connection.is_some();
                        self.selected_session_id = self.active_session_id().map(str::to_owned);
                        if let Some(session_id) = self.active_session_id().map(str::to_owned) {
                            self.restore_session_state(&session_id, window, cx);
                            self.event_generation = self.event_generation.wrapping_add(1);
                            self.subscribe(self.event_generation);
                        }
                        self.error = Some(error);
                    }
                }
            }
            Update::ArtifactLoaded {
                request_id,
                session_id,
                content_hash,
                result,
            } => {
                if request_id != self.artifact_request_id
                    || self.active_session_id() != Some(session_id.as_str())
                {
                    return;
                }
                let Some(preview) = self.artifact_preview.as_mut().filter(|preview| {
                    preview.session_id == session_id
                        && preview.metadata.content_hash == content_hash
                }) else {
                    return;
                };
                match result {
                    Ok(download) => {
                        let media_type = download
                            .media_type
                            .unwrap_or_else(|| preview.metadata.media_type.clone());
                        if !supports_artifact_preview(&media_type) {
                            preview.state = ArtifactPreviewState::Unsupported {
                                reason: "This file type is kept as a download-only artifact."
                                    .to_owned(),
                                media_type,
                            };
                        } else {
                            match text_preview(&download.bytes, download.truncated) {
                                Ok(content) => {
                                    preview.state = ArtifactPreviewState::Ready {
                                        media_type,
                                        content,
                                        truncated: download.truncated,
                                    };
                                }
                                Err(reason) => {
                                    preview.state = ArtifactPreviewState::Unsupported {
                                        media_type,
                                        reason: reason.to_owned(),
                                    };
                                }
                            }
                        }
                    }
                    Err(error) => preview.state = ArtifactPreviewState::Failed(error),
                }
            }
            Update::ChangesLoaded {
                request_id,
                session_id,
                result,
            } => {
                if request_id != self.changes_request_id
                    || self.active_session_id() != Some(session_id.as_str())
                {
                    return;
                }
                let state = match result {
                    Ok(summary) => ChangesState::Ready {
                        session_id: session_id.clone(),
                        summary,
                    },
                    Err(error) => ChangesState::Failed {
                        session_id: session_id.clone(),
                        error,
                    },
                };
                let current_session_id = self.changes_state.as_ref().map(changes_state_session_id);
                if current_session_id == Some(session_id.as_str()) {
                    self.changes_state = Some(state);
                }
            }
            Update::ChangeDiffLoaded {
                request_id,
                session_id,
                path,
                result,
            } => {
                if request_id != self.change_diff_request_id
                    || self.active_session_id() != Some(session_id.as_str())
                {
                    return;
                }
                let state = match result {
                    Ok(diff) if diff.path == path => ChangeDiffState::Ready {
                        session_id: session_id.clone(),
                        path: path.clone(),
                        diff,
                    },
                    Ok(_) => return,
                    Err(error) => ChangeDiffState::Failed {
                        session_id: session_id.clone(),
                        path: path.clone(),
                        error,
                    },
                };
                let current = self.change_diff_state.as_ref().map(|state| match state {
                    ChangeDiffState::Loading { path, .. }
                    | ChangeDiffState::Ready { path, .. }
                    | ChangeDiffState::Failed { path, .. } => path.as_str(),
                });
                if current == Some(path.as_str()) {
                    self.change_diff_state = Some(state);
                }
            }
            Update::UsageLoaded {
                request_id,
                connection_generation,
                destination_generation,
                after,
                result,
            } => {
                if request_id != self.usage.request_id
                    || connection_generation != self.connection_generation
                    || destination_generation != self.usage.destination_generation
                {
                    return;
                }
                self.usage.loading = false;
                self.usage.loading_more = false;
                match result {
                    Ok(response) => {
                        if after.is_some() {
                            self.usage.entries.extend(response.entries);
                        } else {
                            self.usage.entries = response.entries;
                        }
                        self.usage.next_cursor = response.next_cursor;
                        self.usage.has_loaded = true;
                        self.usage.error = None;
                        self.usage.retry_after = None;
                        if after.is_none() {
                            self.usage.expanded.retain(|entry_id| {
                                self.usage
                                    .entries
                                    .iter()
                                    .any(|entry| &entry.usage_observation_id == entry_id)
                            });
                        }
                    }
                    Err(error) => {
                        self.usage.error = Some(error);
                        self.usage.retry_after = after;
                    }
                }
            }
        }
        cx.notify();
    }

    fn run_rail(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let child_runs = self.conversation.child_runs();
        let can_start_child = self.online
            && !self.busy
            && self.switching_session.is_none()
            && (self.pending_child_start.is_some()
                || self
                    .conversation
                    .root_state()
                    .as_ref()
                    .is_some_and(run_accepts_commands));
        let mut rail = div()
            .id("run-rail")
            .w_full()
            .min_h_0()
            .overflow_y_scroll()
            .flex()
            .flex_col()
            .gap_2()
            .px_3()
            .py_4()
            .bg(theme::CHROME)
            .border_r_1()
            .border_color(theme::BORDER)
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .gap_2()
                    .child(
                        div()
                            .font_family(theme::MONO_FONT)
                            .text_size(px(12.0))
                            .text_color(theme::FAINT)
                            .child("RUNS"),
                    )
                    .child(
                        Button::new("start-child-run")
                            .label(if self.busy && self.pending_child_start.is_some() {
                                "Starting…"
                            } else if self.pending_child_start.is_some() {
                                "Retry child"
                            } else {
                                "New child"
                            })
                            .small()
                            .ghost()
                            .disabled(!can_start_child)
                            .on_click(cx.listener(|this, _, _, cx| this.start_child(cx))),
                    ),
            );

        if let Some(state) = self.conversation.root_state() {
            rail = rail.child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .gap_2()
                    .px_2()
                    .py_2()
                    .child(div().text_sm().text_color(theme::TEXT).child("Root Run"))
                    .child(RunStatus::new(state)),
            );
        } else {
            rail = rail.child(
                div()
                    .px_2()
                    .py_2()
                    .text_sm()
                    .text_color(theme::MUTED)
                    .child("No Run started"),
            );
        }

        rail = rail.child(
            div()
                .mt_2()
                .font_family(theme::MONO_FONT)
                .text_size(px(12.0))
                .text_color(theme::FAINT)
                .child(format!("DELEGATED · {}", child_runs.len())),
        );

        if child_runs.is_empty() {
            rail = rail.child(
                div()
                    .px_2()
                    .py_2()
                    .text_sm()
                    .text_color(theme::MUTED)
                    .child("Child Runs appear here as the root delegates work."),
            );
        }

        for (run, depth) in child_runs {
            let run_id = run.run_id.clone();
            let open_run_id = run_id.clone();
            let title = self
                .conversation
                .task_for_run(run)
                .map_or("Delegated Run".to_owned(), |task| task.objective.clone());
            let detail = format!(
                "{} · {} · Level {}",
                compact_run_id(&run_id),
                run_input_mode_label(run.user_input_mode),
                depth,
            );
            rail = rail.child(
                ChildRunRow::new(
                    run_id.clone(),
                    title,
                    detail,
                    run.state.clone(),
                    self.selected_run.as_deref() == Some(&run_id),
                    cx.listener(move |this, _, window, cx| {
                        this.toggle_run_details(open_run_id.clone(), window, cx)
                    }),
                )
                .tasks(
                    self.conversation
                        .tasks_for_run(run)
                        .map(|task| (task.objective.clone(), task.state))
                        .collect(),
                ),
            );
        }

        if let Some(run) = self
            .selected_run
            .as_deref()
            .and_then(|run_id| self.conversation.run(run_id))
        {
            let title = self
                .conversation
                .task_for_run(run)
                .map_or("Delegated Run".to_owned(), |task| task.objective.clone());
            let task_detail = self
                .conversation
                .task_for_run(run)
                .map(|task| format!("Task {} · {:?}", compact_run_id(&task.task_id), task.state));
            let cancel_run_id = run.run_id.clone();
            let focus_run_id = run.run_id.clone();
            let can_cancel = self.online && !self.busy && run_accepts_commands(&run.state);
            rail = rail.child(
                div()
                    .id("run-detail-drawer")
                    .mt_3()
                    .pt_3()
                    .border_t_1()
                    .border_color(theme::BORDER)
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .gap_2()
                            .child(div().text_sm().text_color(theme::TEXT).child(title))
                            .child(
                                Button::new("close-run-detail")
                                    .label("Close")
                                    .small()
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.selected_run = None;
                                        cx.notify();
                                    })),
                            ),
                    )
                    .child(RunStatus::new(run.state.clone()))
                    .child(
                        Button::new("focus-child-run")
                            .label("View child activity")
                            .small()
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.focused_run = Some(focus_run_id.clone());
                                this.show_runs = false;
                                cx.notify();
                            })),
                    )
                    .when_some(run.parent_run_id.clone(), |drawer, parent| {
                        drawer.child(
                            div()
                                .text_xs()
                                .text_color(theme::MUTED)
                                .child(format!("Parent · {parent}")),
                        )
                    })
                    .child(
                        div()
                            .font_family(theme::MONO_FONT)
                            .text_size(px(12.0))
                            .line_height(px(18.0))
                            .text_color(theme::FAINT)
                            .child(run.run_id.clone()),
                    )
                    .child(div().text_sm().text_color(theme::MUTED).child(format!(
                        "Input · {}",
                        run_input_mode_label(run.user_input_mode)
                    )))
                    .when_some(task_detail, |drawer, detail| {
                        drawer.child(div().text_sm().text_color(theme::MUTED).child(detail))
                    })
                    .child(
                        Button::new("stop-child-run")
                            .label("Stop child")
                            .small()
                            .danger()
                            .disabled(!can_cancel)
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.cancel_run(cancel_run_id.clone(), "Stop child Run", cx)
                            })),
                    ),
            );
            if let Some(draft) = self.guidance.get(&run.run_id) {
                let queue_id = run.run_id.clone();
                let interrupt_id = queue_id.clone();
                rail = rail.child(
                    GuidanceComposer::new(
                        &draft.input,
                        self.conversation.run_label(&run.run_id),
                        cx.listener(move |this, _, _, cx| {
                            this.send_guidance(queue_id.clone(), MessageDeliveryMode::Queued, cx)
                        }),
                        cx.listener(move |this, _, _, cx| {
                            this.send_guidance(
                                interrupt_id.clone(),
                                MessageDeliveryMode::Interrupt,
                                cx,
                            )
                        }),
                    )
                    .availability(
                        self.busy || !self.online,
                        run.user_input_mode == RunInputMode::Interactive
                            && run_accepts_commands(&run.state),
                        draft.pending.is_some(),
                    )
                    .error(draft.error.clone()),
                );
            }
        }

        rail
    }

    fn artifact_inspector(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let Some(preview) = self.artifact_preview.as_ref() else {
            return div().id("artifact-inspector");
        };

        let content_hash = preview.metadata.content_hash.clone();
        let hash_label = compact_content_hash(&content_hash);
        let metadata = preview.metadata.clone();
        let media_type = match &preview.state {
            ArtifactPreviewState::Ready { media_type, .. }
            | ArtifactPreviewState::Unsupported { media_type, .. } => media_type.clone(),
            _ => metadata.media_type.clone(),
        };
        let mut panel = div()
            .id("artifact-inspector")
            .w(px(340.0))
            .h_full()
            .flex_none()
            .flex()
            .flex_col()
            .bg(theme::CHROME)
            .border_l_1()
            .border_color(theme::BORDER)
            .child(
                div()
                    .w_full()
                    .h(px(44.0))
                    .flex()
                    .items_center()
                    .justify_between()
                    .px_3()
                    .border_b_1()
                    .border_color(theme::BORDER)
                    .child(
                        div()
                            .font_family(theme::MONO_FONT)
                            .text_size(px(12.0))
                            .text_color(theme::TEXT)
                            .child("ARTIFACT PREVIEW"),
                    )
                    .child(
                        Button::new("close-artifact-preview")
                            .label("Close")
                            .small()
                            .ghost()
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.close_artifact_preview();
                                cx.notify();
                            })),
                    ),
            )
            .child(
                div()
                    .w_full()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .p_3()
                    .border_b_1()
                    .border_color(theme::BORDER)
                    .child(
                        div()
                            .font_family(theme::MONO_FONT)
                            .text_size(px(12.0))
                            .text_color(theme::TEXT)
                            .child(hash_label),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme::MUTED)
                            .child(format!("{} · {} bytes", media_type, metadata.size)),
                    ),
            );

        let body = match &preview.state {
            ArtifactPreviewState::Loading => div()
                .id("artifact-preview-loading")
                .role(gpui::Role::Status)
                .flex_1()
                .flex()
                .items_center()
                .justify_center()
                .text_sm()
                .text_color(theme::MUTED)
                .child("Loading artifact…"),
            ArtifactPreviewState::Ready {
                content, truncated, ..
            } => {
                let mut body = div()
                    .id("artifact-preview-content")
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scroll()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .p_3();
                if *truncated {
                    body = body.child(
                        div()
                            .id("artifact-preview-truncation")
                            .role(gpui::Role::Status)
                            .text_xs()
                            .text_color(theme::ATTENTION)
                            .child("Preview limited to the first 64 KiB."),
                    );
                }
                body.child(
                    div()
                        .font_family(theme::MONO_FONT)
                        .text_size(px(12.0))
                        .line_height(px(18.0))
                        .text_color(theme::TEXT_SOFT)
                        .child(content.clone()),
                )
            }
            ArtifactPreviewState::Failed(error) => {
                let retry_hash = content_hash.clone();
                div()
                    .id("artifact-preview-error")
                    .role(gpui::Role::Alert)
                    .flex_1()
                    .flex()
                    .flex_col()
                    .items_center()
                    .justify_center()
                    .gap_3()
                    .p_3()
                    .text_sm()
                    .text_color(theme::DANGER)
                    .child(error.clone())
                    .child(
                        Button::new("retry-artifact-preview")
                            .label("Retry")
                            .small()
                            .disabled(!self.online || self.switching_session.is_some())
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.open_artifact(retry_hash.clone(), cx);
                            })),
                    )
            }
            ArtifactPreviewState::Unsupported { reason, .. } => div()
                .id("artifact-preview-unsupported")
                .role(gpui::Role::Status)
                .flex_1()
                .flex()
                .flex_col()
                .items_center()
                .justify_center()
                .gap_2()
                .p_3()
                .text_sm()
                .text_color(theme::MUTED)
                .child("Preview unavailable")
                .child(
                    div()
                        .text_xs()
                        .text_color(theme::FAINT)
                        .child(reason.clone()),
                ),
        };
        panel = panel.child(body);
        panel
    }

    fn changes_inspector(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let Some(state) = self.changes_state.as_ref() else {
            return div().id("changes-inspector");
        };
        let session_id = changes_state_session_id(state).to_owned();
        let diff_open = self.change_diff_state.is_some();
        let heading = if diff_open { "CHANGE DIFF" } else { "CHANGES" };

        let mut tabs = div()
            .w_full()
            .flex()
            .items_center()
            .gap_1()
            .px_2()
            .py_1()
            .border_b_1()
            .border_color(theme::BORDER);
        if diff_open {
            tabs = tabs.child(
                Button::new("back-change-diff")
                    .label("Back")
                    .small()
                    .ghost()
                    .accessibility_label("Back to changed files")
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.back_from_change_diff(cx);
                    })),
            );
        } else {
            tabs = tabs.child(
                Button::new("changes-tab")
                    .label("Changes")
                    .small()
                    .ghost()
                    .selected(self.inspector_tab == InspectorTab::Changes)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.inspector_tab = InspectorTab::Changes;
                        cx.notify();
                    })),
            );
        }
        if self.artifact_preview.is_some() {
            tabs = tabs.child(
                Button::new("artifact-tab")
                    .label("Artifact")
                    .small()
                    .ghost()
                    .selected(self.inspector_tab == InspectorTab::Artifact)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.inspector_tab = InspectorTab::Artifact;
                        cx.notify();
                    })),
            );
        }

        let panel = div()
            .id("changes-inspector")
            .w(px(340.0))
            .h_full()
            .flex_none()
            .flex()
            .flex_col()
            .bg(theme::CHROME)
            .border_l_1()
            .border_color(theme::BORDER)
            .child(
                div()
                    .w_full()
                    .h(px(44.0))
                    .flex()
                    .items_center()
                    .justify_between()
                    .px_3()
                    .border_b_1()
                    .border_color(theme::BORDER)
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(
                                div()
                                    .font_family(theme::MONO_FONT)
                                    .text_size(px(12.0))
                                    .text_color(theme::TEXT)
                                    .child(heading),
                            )
                            .child(
                                div()
                                    .font_family(theme::MONO_FONT)
                                    .text_size(px(10.0))
                                    .text_color(theme::FAINT)
                                    .child(format!("Session {}", compact_session_id(&session_id))),
                            ),
                    )
                    .child(
                        Button::new("close-changes-inspector")
                            .label("Close")
                            .small()
                            .ghost()
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.close_inspector();
                                cx.notify();
                            })),
                    ),
            )
            .child(tabs);

        let body = if let Some(diff_state) = self.change_diff_state.as_ref() {
            match diff_state {
                ChangeDiffState::Loading { path, session_id } => div()
                    .id("change-diff-loading")
                    .role(gpui::Role::Status)
                    .flex_1()
                    .flex()
                    .flex_col()
                    .items_center()
                    .justify_center()
                    .gap_2()
                    .p_3()
                    .text_sm()
                    .text_color(theme::MUTED)
                    .child(format!(
                        "Loading diff for Session {}…",
                        compact_session_id(session_id)
                    ))
                    .child(
                        div()
                            .font_family(theme::MONO_FONT)
                            .text_xs()
                            .text_color(theme::FAINT)
                            .child(path.clone()),
                    ),
                ChangeDiffState::Failed { path, error, .. } => {
                    let retry_error = error.clone();
                    div()
                        .id("change-diff-error")
                        .role(gpui::Role::Alert)
                        .flex_1()
                        .flex()
                        .flex_col()
                        .items_center()
                        .justify_center()
                        .gap_3()
                        .p_3()
                        .text_sm()
                        .text_color(theme::DANGER)
                        .child(format!("Could not load diff for {path}."))
                        .child(retry_error)
                        .child(
                            Button::new("retry-change-diff")
                                .label("Retry")
                                .small()
                                .disabled(!self.online || self.switching_session.is_some())
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.retry_change_diff(cx);
                                })),
                        )
                }
                ChangeDiffState::Ready { diff, .. } => {
                    let mut body = div()
                        .id("change-diff-content")
                        .flex_1()
                        .min_h_0()
                        .overflow_y_scroll()
                        .flex()
                        .flex_col()
                        .gap_2()
                        .p_3()
                        .child(
                            div()
                                .id("change-diff-summary")
                                .role(gpui::Role::Status)
                                .flex()
                                .flex_col()
                                .gap_1()
                                .pb_2()
                                .border_b_1()
                                .border_color(theme::BORDER)
                                .child(
                                    div()
                                        .font_family(theme::MONO_FONT)
                                        .text_size(px(12.0))
                                        .line_height(px(17.0))
                                        .text_color(theme::TEXT)
                                        .child(diff.path.clone()),
                                )
                                .child(div().text_xs().text_color(theme::MUTED).child(format!(
                                    "{} · {}",
                                    change_kind_label(&diff.kind),
                                    diff.relative_directory
                                ))),
                        );
                    match &diff.content {
                        SessionChangeDiffContent::Ready { patch, truncated } => {
                            if patch.is_empty() {
                                body = body.child(
                                    div()
                                        .id("change-diff-empty")
                                        .role(gpui::Role::Status)
                                        .py_8()
                                        .text_center()
                                        .text_sm()
                                        .text_color(theme::MUTED)
                                        .child("No textual changes to display."),
                                );
                            } else {
                                body = body.child(
                                    div()
                                        .id("change-diff-patch")
                                        .w_full()
                                        .font_family(theme::MONO_FONT)
                                        .text_size(px(11.0))
                                        .line_height(px(16.0))
                                        .text_color(theme::TEXT_SOFT)
                                        .child(patch.clone()),
                                );
                            }
                            if *truncated {
                                body = body.child(
                                    div()
                                        .id("change-diff-truncation")
                                        .role(gpui::Role::Status)
                                        .text_xs()
                                        .text_color(theme::ATTENTION)
                                        .child("Diff preview truncated at 256 KiB."),
                                );
                            }
                        }
                        SessionChangeDiffContent::Unavailable { reason } => {
                            body = body.child(
                                div()
                                    .id("change-diff-unsupported")
                                    .role(gpui::Role::Status)
                                    .flex()
                                    .flex_col()
                                    .gap_2()
                                    .p_3()
                                    .text_sm()
                                    .text_color(theme::MUTED)
                                    .child("Diff unavailable")
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(theme::FAINT)
                                            .child(change_diff_unavailable_reason_label(*reason)),
                                    ),
                            );
                        }
                    }
                    body
                }
            }
        } else {
            match state {
                ChangesState::Loading { .. } => div()
                    .id("changes-loading")
                    .role(gpui::Role::Status)
                    .flex_1()
                    .flex()
                    .items_center()
                    .justify_center()
                    .text_sm()
                    .text_color(theme::MUTED)
                    .child("Loading changes…"),
                ChangesState::Failed { error, .. } => {
                    let retry_session_id = session_id.clone();
                    div()
                        .id("changes-error")
                        .role(gpui::Role::Alert)
                        .flex_1()
                        .flex()
                        .flex_col()
                        .items_center()
                        .justify_center()
                        .gap_3()
                        .p_3()
                        .text_sm()
                        .text_color(theme::DANGER)
                        .child(error.clone())
                        .child(
                            Button::new("retry-changes")
                                .label("Retry")
                                .small()
                                .disabled(!self.online || self.switching_session.is_some())
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.request_changes(retry_session_id.clone(), cx);
                                })),
                        )
                }
                ChangesState::Ready { summary, .. } => {
                    let (additions, deletions, binary, untracked) = change_summary_counts(summary);
                    let mut body = div()
                        .id("changes-content")
                        .flex_1()
                        .min_h_0()
                        .overflow_y_scroll()
                        .flex()
                        .flex_col()
                        .gap_2()
                        .p_3()
                        .child(
                            div()
                                .id("changes-summary")
                                .role(gpui::Role::Status)
                                .flex()
                                .flex_col()
                                .gap_1()
                                .pb_2()
                                .border_b_1()
                                .border_color(theme::BORDER)
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(theme::TEXT)
                                        .child(format!(
                                            "{} changed file{}",
                                            summary.files.len(),
                                            if summary.files.len() == 1 { "" } else { "s" }
                                        )),
                                )
                                .child(
                                    div()
                                        .text_xs()
                                        .text_color(theme::MUTED)
                                        .child(format!(
                                            "Added {additions} · Deleted {deletions} · Binary {binary} · Untracked {untracked}"
                                        )),
                                )
                                .child(
                                    div()
                                        .font_family(theme::MONO_FONT)
                                        .text_size(px(11.0))
                                        .line_height(px(16.0))
                                        .text_color(theme::FAINT)
                                        .child(format!(
                                            "Checkout · {} · root {}",
                                            summary.relative_directory,
                                            compact_session_id(&summary.workspace_root_id)
                                        )),
                                ),
                        );

                    if summary.files.is_empty() {
                        body = body.child(
                            div()
                                .id("changes-empty")
                                .role(gpui::Role::Status)
                                .py_8()
                                .text_center()
                                .text_sm()
                                .text_color(theme::MUTED)
                                .child("No changes in this checkout."),
                        );
                    } else {
                        let mut files = div().id("changes-file-list").flex().flex_col().gap_1();
                        let selected_path =
                            self.change_diff_state.as_ref().map(change_diff_state_path);
                        for (index, file) in summary.files.iter().enumerate() {
                            let status = change_kind_label(&file.kind);
                            let file_counts = change_file_counts(file);
                            let path = file.path.clone();
                            let selected = selected_path == Some(file.path.as_str());
                            let path_for_click = path.clone();
                            let session_id_for_click = session_id.clone();
                            files = files.child(
                                Button::new(SharedString::from(format!("change-file-{index}")))
                                    .ghost()
                                    .selected(selected)
                                    .disabled(!self.online || self.switching_session.is_some())
                                    .accessibility_label(format!(
                                        "Open diff for {}. Status: {}. {}.",
                                        path, status, file_counts
                                    ))
                                    .w_full()
                                    .h_auto()
                                    .flex()
                                    .items_start()
                                    .justify_between()
                                    .gap_2()
                                    .px_2()
                                    .py_2()
                                    .border_b_1()
                                    .border_color(theme::BORDER)
                                    .child(
                                        div()
                                            .min_w_0()
                                            .flex_1()
                                            .flex()
                                            .flex_col()
                                            .gap_1()
                                            .child(
                                                div()
                                                    .font_family(theme::MONO_FONT)
                                                    .text_size(px(12.0))
                                                    .line_height(px(17.0))
                                                    .text_color(theme::TEXT)
                                                    .child(file.path.clone()),
                                            )
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(theme::MUTED)
                                                    .child(format!("{status} · {file_counts}")),
                                            ),
                                    )
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.request_change_diff(
                                            session_id_for_click.clone(),
                                            path_for_click.clone(),
                                            cx,
                                        );
                                    })),
                            );
                        }
                        body = body.child(files);
                    }
                    body
                }
            }
        };
        panel.child(body)
    }

    fn usage_entry(
        &self,
        entry: &UsageLedgerEntryResponse,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let observation_id = entry.usage_observation_id.clone();
        let expanded = self.usage.expanded.contains(&observation_id);
        let requested_model = entry.requested_model.clone();
        let resolved_model = entry
            .resolved_model
            .clone()
            .unwrap_or_else(|| "Not reported".to_owned());
        let source = usage_source_label(entry.source);
        let status = format!(
            "Revision {} · {} · {}",
            entry.revision,
            usage_finality_label(entry.finality),
            usage_completeness_label(entry.completeness),
        );
        let provider = format!(
            "Provider account · {} · Source · {source}",
            entry.provider_account_id
        );
        let toggle_label = if expanded {
            "Hide details"
        } else {
            "Show details"
        };
        let accessibility_label = format!(
            "Usage observation for requested model {}. Resolved model {}. {}. {}.",
            requested_model, resolved_model, provider, status
        );
        let observation_id_for_click = observation_id.clone();
        let mut quantities = div()
            .id(SharedString::from(format!(
                "usage-quantities-{observation_id}"
            )))
            .flex()
            .flex_col()
            .gap_1();
        if entry.quantities.is_empty() {
            quantities = quantities.child(
                div()
                    .text_xs()
                    .text_color(theme::FAINT)
                    .child("No quantities reported; missing dimensions remain unreported."),
            );
        } else {
            for quantity in &entry.quantities {
                quantities = quantities.child(
                    div()
                        .font_family(theme::MONO_FONT)
                        .text_size(px(12.0))
                        .line_height(px(18.0))
                        .text_color(theme::TEXT_SOFT)
                        .child(usage_quantity_label(quantity)),
                );
            }
        }

        let mut row = div()
            .id(SharedString::from(format!("usage-entry-{observation_id}")))
            .role(gpui::Role::ListItem)
            .aria_label(accessibility_label)
            .w_full()
            .flex()
            .flex_col()
            .border_b_1()
            .border_color(theme::BORDER)
            .child(
                Button::new(SharedString::from(format!(
                    "usage-entry-toggle-{observation_id}"
                )))
                .ghost()
                .accessibility_label(toggle_label)
                .w_full()
                .h_auto()
                .items_start()
                .px_3()
                .py_3()
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.toggle_usage_entry(observation_id_for_click.clone(), cx);
                }))
                .child(
                    div()
                        .w_full()
                        .flex()
                        .items_start()
                        .justify_between()
                        .gap_4()
                        .child(
                            div()
                                .min_w_0()
                                .flex_1()
                                .flex()
                                .flex_col()
                                .items_start()
                                .gap_1()
                                .child(
                                    div()
                                        .w_full()
                                        .text_sm()
                                        .text_color(theme::TEXT)
                                        .child(requested_model),
                                )
                                .child(
                                    div()
                                        .w_full()
                                        .text_xs()
                                        .text_color(theme::MUTED)
                                        .child(format!("Resolved model · {resolved_model}")),
                                )
                                .child(
                                    div()
                                        .w_full()
                                        .text_xs()
                                        .text_color(theme::MUTED)
                                        .child(provider),
                                ),
                        )
                        .child(
                            div()
                                .flex_none()
                                .flex()
                                .flex_col()
                                .items_end()
                                .gap_1()
                                .child(
                                    div()
                                        .font_family(theme::MONO_FONT)
                                        .text_size(px(11.0))
                                        .text_color(theme::TEXT_SOFT)
                                        .child(status),
                                )
                                .child(
                                    div()
                                        .text_xs()
                                        .text_color(theme::ACCENT)
                                        .child(toggle_label),
                                ),
                        ),
                ),
            )
            .child(
                div()
                    .w_full()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .px_3()
                    .pb_3()
                    .child(
                        div()
                            .font_family(theme::MONO_FONT)
                            .text_size(px(10.0))
                            .text_color(theme::FAINT)
                            .child("QUANTITIES · PRESERVED AS REPORTED"),
                    )
                    .child(quantities)
                    .child(div().text_xs().text_color(theme::MUTED).child(format!(
                        "Observed at Unix ms {} · Accounting · {} · {}",
                        entry.observed_at_unix_ms,
                        usage_accounting_label(entry.accounting),
                        if entry.is_terminal {
                            "Terminal invocation"
                        } else {
                            "Invocation still open"
                        }
                    ))),
            );

        if expanded {
            let mut details = div()
                .id(SharedString::from(format!(
                    "usage-details-{observation_id}"
                )))
                .w_full()
                .flex()
                .flex_col()
                .gap_1()
                .px_3()
                .pb_3()
                .pt_1()
                .border_t_1()
                .border_color(theme::BORDER)
                .child(
                    div()
                        .font_family(theme::MONO_FONT)
                        .text_size(px(10.0))
                        .text_color(theme::FAINT)
                        .child("IDENTIFIERS · DURABLE OWNERSHIP"),
                )
                .child(usage_detail(
                    "Invocation",
                    entry.model_invocation_id.clone(),
                ))
                .child(usage_detail(
                    "Usage observation",
                    entry.usage_observation_id.clone(),
                ))
                .child(usage_detail("Update", entry.update_id.clone()))
                .child(usage_detail("Work", entry.work_id.clone()))
                .child(usage_detail("Run", entry.run_id.clone()))
                .child(usage_detail("Session", entry.session_id.clone()))
                .child(usage_detail("Workspace", entry.workspace_id.clone()))
                .child(usage_detail(
                    "Supersedes",
                    entry
                        .supersedes_usage_observation_id
                        .clone()
                        .unwrap_or_else(|| "Not reported".to_owned()),
                ))
                .child(usage_detail(
                    "Request",
                    entry
                        .request_id
                        .clone()
                        .unwrap_or_else(|| "Not reported".to_owned()),
                ))
                .child(usage_detail(
                    "Service tier",
                    entry
                        .service_tier
                        .clone()
                        .unwrap_or_else(|| "Not reported".to_owned()),
                ));
            if let Some(resolved_model) = &entry.resolved_model {
                details = details.child(usage_detail("Resolved model", resolved_model.clone()));
            } else {
                details = details.child(usage_detail("Resolved model", "Not reported".to_owned()));
            }
            row = row.child(details);
        }
        row
    }

    fn usage_screen(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let refresh_disabled =
            !self.online || self.usage.loading || self.usage.loading_more || self.daemon.is_none();
        let refresh_label = if self.usage.loading {
            "Refreshing…"
        } else if self.usage.loading_more {
            "Loading…"
        } else {
            "Refresh"
        };
        let loaded_count = self.usage.entries.len();
        let mut workbench = div()
            .id("usage-workbench")
            .w_full()
            .max_w(px(1104.0))
            .flex()
            .flex_col()
            .gap_4()
            .px_6()
            .py_6()
            .child(
                div()
                    .w_full()
                    .flex()
                    .items_start()
                    .justify_between()
                    .gap_4()
                    .child(
                        div()
                            .min_w_0()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(div().text_xl().child("Usage"))
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(theme::MUTED)
                                    .child("Global daemon ledger · latest revision per physical invocation"),
                            ),
                    )
                    .child(
                        Button::new("refresh-usage")
                            .label(refresh_label)
                            .small()
                            .disabled(refresh_disabled)
                            .on_click(cx.listener(|this, _, _, cx| this.refresh_usage(cx))),
                    ),
            )
            .child(
                div()
                    .id("usage-ledger-note")
                    .role(gpui::Role::Status)
                    .w_full()
                    .text_xs()
                    .text_color(theme::FAINT)
                    .child("Entries are observations only. Quantities remain separate; this view does not calculate totals or valuation."),
            );

        if let Some(error) = self.usage.error.clone() {
            let retry_label = if self.usage.retry_after.is_some() {
                "Retry load more"
            } else {
                "Retry"
            };
            workbench = workbench.child(
                div()
                    .id("usage-error")
                    .role(gpui::Role::Alert)
                    .aria_label(error.clone())
                    .w_full()
                    .flex()
                    .items_center()
                    .gap_3()
                    .px_3()
                    .py_3()
                    .border_l_2()
                    .border_color(theme::DANGER)
                    .child(
                        div()
                            .min_w_0()
                            .flex_1()
                            .text_sm()
                            .text_color(theme::DANGER)
                            .child(error),
                    )
                    .child(
                        Button::new("retry-usage")
                            .label(retry_label)
                            .small()
                            .disabled(!self.online || self.usage.loading || self.usage.loading_more)
                            .on_click(cx.listener(|this, _, _, cx| this.retry_usage(cx))),
                    ),
            );
        }

        if self.usage.loading && self.usage.entries.is_empty() {
            workbench = workbench.child(
                div()
                    .id("usage-loading")
                    .role(gpui::Role::Status)
                    .w_full()
                    .py_16()
                    .text_center()
                    .text_sm()
                    .text_color(theme::MUTED)
                    .child("Loading global Usage ledger…"),
            );
        } else if self.usage.entries.is_empty() && self.usage.error.is_none() {
            let message = if self.daemon.is_none() {
                "Connect to a daemon to view the global Usage ledger."
            } else if !self.online {
                "Reconnect to view the global Usage ledger."
            } else if self.usage.has_loaded {
                "No usage observations are available in this daemon ledger."
            } else {
                "Open Usage to load the global daemon ledger."
            };
            workbench = workbench.child(
                div()
                    .id("usage-empty")
                    .role(gpui::Role::Status)
                    .w_full()
                    .py_16()
                    .text_center()
                    .text_sm()
                    .text_color(theme::MUTED)
                    .child(message),
            );
        } else if !self.usage.entries.is_empty() {
            let summary = if self.usage.loading {
                format!("Refreshing · {loaded_count} loaded ledger entries")
            } else if self.usage.loading_more {
                format!("Loading next page · {loaded_count} loaded ledger entries")
            } else {
                format!("{loaded_count} loaded ledger entries · global daemon scope")
            };
            let mut rows = div()
                .id("usage-ledger-rows")
                .role(gpui::Role::List)
                .w_full()
                .flex()
                .flex_col()
                .border_t_1()
                .border_color(theme::BORDER);
            for entry in &self.usage.entries {
                rows = rows.child(self.usage_entry(entry, cx));
            }
            workbench = workbench
                .child(
                    div()
                        .id("usage-summary")
                        .role(gpui::Role::Status)
                        .w_full()
                        .text_xs()
                        .text_color(theme::MUTED)
                        .child(summary),
                )
                .child(rows);
            if let Some(after) = self.usage.next_cursor.clone() {
                let load_more_disabled = !self.online
                    || self.usage.loading
                    || self.usage.loading_more
                    || self.usage.error.is_some();
                workbench = workbench.child(
                    div()
                        .id("usage-load-more-row")
                        .w_full()
                        .flex()
                        .justify_center()
                        .pt_2()
                        .child(
                            Button::new("load-more-usage")
                                .label(if self.usage.loading_more {
                                    "Loading next page…"
                                } else {
                                    "Load more"
                                })
                                .small()
                                .disabled(load_more_disabled)
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    if this.usage.next_cursor.as_deref() == Some(after.as_str()) {
                                        this.load_more_usage(cx);
                                    }
                                })),
                        ),
                );
            }
        }

        div()
            .id("usage-screen")
            .w_full()
            .h_full()
            .min_h_0()
            .overflow_y_scroll()
            .flex()
            .justify_center()
            .bg(theme::BG)
            .child(workbench)
    }

    fn connection_form(&self, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap_4()
            .w_full()
            .max_w(px(580.))
            .p_6()
            .child(div().text_xl().child("Connect to Kiln"))
            .child(
                div()
                    .text_sm()
                    .text_color(theme::MUTED)
                    .child("Use the address and credential file reported by your local daemon."),
            )
            .child(field("Daemon address", &self.address))
            .child(field("Credential file", &self.token_file))
            .child(field("Git repository · new session", &self.repository))
            .child(field("Session ID · resume existing work", &self.session))
            .child(
                Button::new("connect")
                    .primary()
                    .label(if self.busy {
                        "Connecting…"
                    } else {
                        "Connect"
                    })
                    .disabled(self.busy)
                    .on_click(cx.listener(|this, _, _, cx| this.connect(cx))),
            )
    }
}

impl Render for Desktop {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.ensure_approval_inspections();
        self.ensure_elicitations(cx);
        let title = self.connection.as_ref().map_or_else(
            || {
                self.selected_workspace_id
                    .as_deref()
                    .and_then(|id| {
                        self.workspaces
                            .iter()
                            .find(|workspace| workspace.workspace_id == id)
                    })
                    .map_or("Kiln".to_owned(), |workspace| workspace.name.clone())
            },
            |connected| connected.workspace.name.clone(),
        );
        let scope =
            self.connection
                .as_ref()
                .map_or("No workspace selected".to_owned(), |connected| {
                    connected
                        .workspace
                        .roots
                        .first()
                        .map_or("No repository root".to_owned(), |root| {
                            root.display_path.clone()
                        })
                });
        let active = self.conversation.active_root().is_some();
        let disabled = self.busy
            || !self.online
            || self.connection.is_none()
            || self.switching_session.is_some()
            || self.pending.is_some()
            || self.conversation.root_state() == Some(RunState::Cancelling);
        let mut transcript = div()
            .flex()
            .flex_col()
            .gap_5()
            .w_full()
            .max_w(theme::TRANSCRIPT_WIDTH)
            .py_6();
        if self.focused_run.as_deref().is_some_and(|run| {
            !self
                .conversation
                .transcript
                .iter()
                .any(|item| item.run_id.as_deref() == Some(run))
        }) {
            transcript = transcript.child(
                div()
                    .text_sm()
                    .text_color(theme::MUTED)
                    .child("No child activity yet. Live updates will appear here."),
            );
        }
        if self.focused_run.is_none() && self.conversation.transcript.is_empty() {
            transcript = transcript.child(div().py_16().text_center().text_xl().child("What should we build?"))
                .child(div().text_center().text_sm().text_color(theme::MUTED).child("This daemon uses a deterministic executor. Live model providers are not connected."));
        }
        for item in &self.conversation.transcript {
            if self
                .focused_run
                .as_deref()
                .is_some_and(|run| item.run_id.as_deref() != Some(run))
            {
                continue;
            }
            let mut row = TranscriptRow::new(item.actor.clone(), item.content.clone());
            if let Some(detail) = self.conversation.transcript_detail(item) {
                row = row.detail(detail);
            }
            let artifact_disabled = !self.online || self.switching_session.is_some();
            for (position, attachment) in item.attachments.iter().enumerate() {
                let content_hash = attachment.content_hash.clone();
                let label = format!(
                    "Open attachment {} ({}, {} bytes)",
                    position + 1,
                    attachment.media_type,
                    attachment.size
                );
                row = row.attachment_action(
                    label,
                    artifact_disabled,
                    cx.listener(move |this, _, _, cx| {
                        this.open_artifact(content_hash.clone(), cx);
                    }),
                );
            }
            if item.attachments.is_empty()
                && self.conversation.artifacts.contains_key(&item.content)
            {
                let content_hash = item.content.clone();
                row = row.artifact(
                    artifact_disabled,
                    cx.listener(move |this, _, _, cx| {
                        this.open_artifact(content_hash.clone(), cx);
                    }),
                );
            }
            if let Some(run_id) = item.run_id.as_deref().filter(|run_id| {
                self.conversation.root_for_run(run_id).is_some_and(|root| {
                    root != *run_id && self.conversation.active_root() == Some(root)
                })
            }) {
                let run_id = run_id.to_owned();
                let event_id = item.source_event_id.clone();
                row = row.reaction(
                    disabled,
                    cx.listener(move |this, _, window, cx| {
                        this.select_reaction(run_id.clone(), event_id.clone(), window, cx);
                    }),
                );
            }
            transcript = transcript.child(div().id(SharedString::from(item.id.clone())).child(row));
        }
        for approval in self
            .conversation
            .approvals
            .values()
            .filter(|approval| approval.state == ApprovalState::Pending)
            .filter(|approval| {
                self.focused_run
                    .as_ref()
                    .is_none_or(|run| *run == approval.run_id)
            })
        {
            let approve_id = approval.tool_call_id.clone();
            let reject_id = approve_id.clone();
            let retry_id = approve_id.clone();
            let (details, inspected, failed) = match self.approval_inspections.get(&approve_id) {
                Some(ApprovalInspection::Loaded(inspection)) => {
                    let details = match &inspection.source {
                        Some(source) => {
                            let arguments =
                                serde_json::from_str::<serde_json::Value>(&source.arguments_json)
                                    .ok()
                                    .and_then(|value| serde_json::to_string_pretty(&value).ok())
                                    .unwrap_or_else(|| source.arguments_json.clone());
                            format!(
                                "Tool: {} (revision {})\nCapability: {}\n\nModel-supplied arguments:\n{}",
                                source.name, source.revision, inspection.capability, arguments
                            )
                        }
                        None => format!(
                            "Capability: {}\nThis ToolCall has no model-supplied arguments.",
                            inspection.capability
                        ),
                    };
                    (details, true, false)
                }
                Some(ApprovalInspection::Failed(error)) => (
                    format!(
                        "Request details unavailable: {error}\nApproval is disabled until the complete request can be inspected."
                    ),
                    false,
                    true,
                ),
                _ => ("Loading requested operation…".to_owned(), false, false),
            };
            let approval_root = self
                .connection
                .as_ref()
                .and_then(|connected| {
                    connected.workspace.roots.iter().find(|root| {
                        root.workspace_root_id == approval.requested_scope.workspace_root_id
                    })
                })
                .map_or(
                    approval.requested_scope.workspace_root_id.as_str(),
                    |root| root.display_path.as_str(),
                );
            let approval_scope = format!(
                "{} · {} · {}",
                self.conversation.run_label(&approval.run_id),
                approval_root,
                approval.requested_scope.relative_directory
            );
            transcript = transcript.child(
                div()
                    .id(SharedString::from(approval.approval_id.clone()))
                    .child(
                        ApprovalPanel::new(
                            approval_scope,
                            cx.listener(move |this, _, _, cx| {
                                this.decide(approve_id.clone(), ApprovalDecision::Approved, cx)
                            }),
                            cx.listener(move |this, _, _, cx| {
                                this.decide(reject_id.clone(), ApprovalDecision::Rejected, cx)
                            }),
                        )
                        .details(details)
                        .approve_disabled(!inspected)
                        .when(failed, |panel| {
                            panel.retry(cx.listener(move |this, _, _, cx| {
                                this.approval_inspections.remove(&retry_id);
                                cx.notify();
                            }))
                        })
                        .disabled(self.busy || !self.online),
                    ),
            );
        }
        for ((id, owner), panel) in &self.elicitations {
            let source = self.conversation.elicitation_inputs.get(id);
            if self
                .focused_run
                .as_ref()
                .is_none_or(|focused| focused == owner || source == Some(focused))
            {
                transcript = transcript.child(panel.clone());
            }
        }
        if self.show_settings {
            self.ensure_account_settings(cx);
        }
        let heading = if self.show_settings {
            "Settings".to_owned()
        } else if self.show_usage {
            "Usage".to_owned()
        } else {
            title
        };
        let mut content = div()
            .capture_key_down(cx.listener(|this, event: &gpui::KeyDownEvent, window, cx| {
                if event.keystroke.key == "escape" && !this.show_connection {
                    let workspace_query = this.workspace_query.read(cx).value().to_string();
                    let session_query = this.session_query.read(cx).value().to_string();
                    if !workspace_query.is_empty() {
                        this.workspace_query
                            .update(cx, |input, cx| input.set_value("", window, cx));
                    } else if !session_query.is_empty() {
                        this.session_query
                            .update(cx, |input, cx| input.set_value("", window, cx));
                    } else if this.show_settings {
                        this.show_settings = false;
                        this.composer.read(cx).focus_handle(cx).focus(window, cx);
                    } else if this.show_usage {
                        this.leave_usage();
                        this.composer.read(cx).focus_handle(cx).focus(window, cx);
                    } else if this.artifact_preview.is_some() || this.changes_state.is_some() {
                        this.close_inspector();
                        this.composer.read(cx).focus_handle(cx).focus(window, cx);
                    } else if this.show_runs || this.focused_run.is_some() {
                        this.show_runs = false;
                        this.selected_run = None;
                        this.focused_run = None;
                        this.composer.read(cx).focus_handle(cx).focus(window, cx);
                    } else {
                        return;
                    }
                    cx.stop_propagation();
                    cx.notify();
                }
            }))
            .flex()
            .flex_col()
            .min_w_0()
            .size_full()
            .bg(theme::BG)
            .text_color(theme::TEXT)
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .items_center()
                    .justify_between()
                    .px_5()
                    .py_3()
                    .border_b_1()
                    .border_color(theme::BORDER)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(div().text_sm().child(heading))
                            .when(self.show_usage || self.show_settings, |view| {
                                view.child(
                                    div()
                                        .font_family(theme::MONO_FONT)
                                        .text_size(px(10.0))
                                        .text_color(theme::FAINT)
                                        .child("global"),
                                )
                            }),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_wrap()
                            .items_center()
                            .gap_3()
                            .when(!self.show_usage && !self.show_settings, |view| {
                                view.when_some(self.conversation.root_state(), |view, state| {
                                    view.child(RunStatus::new(state))
                                })
                            })
                            .child(
                                Button::new("toggle-browser")
                                    .label(if self.browser_open {
                                        "Hide browser"
                                    } else {
                                        "Browse"
                                    })
                                    .disabled(self.daemon.is_none() || self.show_settings)
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.browser_open = !this.browser_open;
                                        cx.notify();
                                    })),
                            )
                            .child(
                                Button::new("toggle-usage")
                                    .label(if self.show_usage {
                                        "Close Usage"
                                    } else {
                                        "Usage"
                                    })
                                    .selected(self.show_usage)
                                    .disabled(self.daemon.is_none())
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.toggle_usage(cx);
                                    })),
                            )
                            .child(
                                Button::new("toggle-settings")
                                    .label(if self.show_settings {
                                        "Close Settings"
                                    } else {
                                        "Settings"
                                    })
                                    .selected(self.show_settings)
                                    .disabled(self.daemon.is_none())
                                    .on_click(
                                        cx.listener(|this, _, _, cx| this.toggle_settings(cx)),
                                    ),
                            )
                            .child(
                                Button::new("toggle-runs")
                                    .label(if self.show_runs {
                                        "Close Runs".to_owned()
                                    } else {
                                        format!("Runs · {}", self.conversation.runs.len())
                                    })
                                    .disabled(
                                        self.show_usage
                                            || self.show_settings
                                            || self.connection.is_none()
                                            || self.switching_session.is_some(),
                                    )
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.show_runs = !this.show_runs;
                                        cx.notify();
                                    })),
                            )
                            .child(
                                Button::new("toggle-changes")
                                    .label(if self.changes_state.is_some() {
                                        "Close Changes".to_owned()
                                    } else {
                                        "Changes".to_owned()
                                    })
                                    .disabled(
                                        self.show_usage
                                            || self.show_settings
                                            || self.connection.is_none()
                                            || !self.online
                                            || self.switching_session.is_some(),
                                    )
                                    .on_click(
                                        cx.listener(|this, _, _, cx| this.toggle_changes(cx)),
                                    ),
                            )
                            .child(
                                Button::new("connection-settings")
                                    .label("Connection")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.show_connection = !this.show_connection;
                                        if this.show_connection {
                                            this.leave_usage();
                                        }
                                        cx.notify();
                                    })),
                            ),
                    ),
            )
            .when_some(self.error.clone(), |view, error| {
                view.child(
                    div()
                        .id("connection-error")
                        .role(gpui::Role::Alert)
                        .aria_label(error.clone())
                        .flex()
                        .items_center()
                        .gap_3()
                        .px_5()
                        .py_3()
                        .text_sm()
                        .text_color(theme::DANGER)
                        .child(div().flex_1().child(error))
                        .when(
                            self.pending.as_ref().is_some_and(|pending| {
                                !pending.append_uncertain || pending.append_retry_ready
                            }) || self.pending_child_start.is_some(),
                            |row| {
                                row.child(
                                    Button::new("retry-command")
                                        .label(
                                            if self
                                                .pending
                                                .as_ref()
                                                .is_some_and(|pending| pending.append_uncertain)
                                            {
                                                "Retry same submission"
                                            } else {
                                                "Retry"
                                            },
                                        )
                                        .disabled(self.busy || !self.online)
                                        .on_click(cx.listener(|this, _, window, cx| {
                                            this.retry(window, cx)
                                        })),
                                )
                            },
                        )
                        .child(
                            Button::new("reconnect")
                                .label("Reconnect")
                                .disabled(self.busy)
                                .on_click(cx.listener(|this, _, _, cx| this.connect(cx))),
                        ),
                )
            });
        if self.show_connection || self.daemon.is_none() {
            content = content.child(
                div()
                    .id("connection-scroll")
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scroll()
                    .flex()
                    .justify_center()
                    .child(self.connection_form(cx)),
            );
        } else if self.show_settings {
            content = content.child(
                div()
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_h_0()
                    .when_some(self.account_settings.clone(), |view, settings| {
                        view.child(settings)
                    })
                    .when(self.account_settings.is_none(), |view| {
                        view.child(
                            div()
                                .px_6()
                                .py_6()
                                .child("Reconnect to manage provider accounts."),
                        )
                    }),
            );
        } else if self.show_usage {
            content = content.child(self.usage_screen(cx));
        } else {
            content = content.child(
                div()
                    .flex()
                    .flex_1()
                    .min_h_0()
                    .child(
                        div()
                            .min_w_0()
                            .flex_1()
                            .flex()
                            .flex_col()
                            .child(
                                div()
                                    .id("transcript-scroll")
                                    .flex_1()
                                    .min_h_0()
                                    .overflow_y_scroll()
                                    .flex()
                                    .flex_col()
                                    .items_center()
                                    .px_6()
                                    .when(self.show_runs, |view| view.child(
                                        div().w_full().max_w(theme::TRANSCRIPT_WIDTH).child(self.run_rail(cx))))
                                    .when(!self.show_runs, |view| view
                                        .when_some(self.focused_run.clone(), |view, run| view.child(
                                            div().w_full().max_w(theme::TRANSCRIPT_WIDTH).pt_3().flex().flex_col().gap_2()
                                                .child(Button::new("back-to-root").label("Back to conversation").small()
                                                    .on_click(cx.listener(|this, _, _, cx| { this.focused_run = None; cx.notify(); })))
                                                .child(div().text_sm().child(self.conversation.run_label(&run)))
                                                .child(div().text_xs().text_color(theme::MUTED).child("Viewing child activity · the composer below sends to the root"))
                                        ))
                                        .child(transcript)),
                            )
                            .child(
                                div()
                                    .flex()
                                    .flex_col()
                                    .items_center()
                                    .px_6()
                                    .pb_5()
                                    .gap_2()
                                    .child(
                                        div().w_full().max_w(theme::TRANSCRIPT_WIDTH).child(
                                            Composer::new(
                                                &self.composer,
                                                scope,
                                                active,
                                                cx.listener(|this, _, window, cx| {
                                                    this.submit(window, cx)
                                                }),
            cx.listener(|this, _, _, cx| this.cancel(cx)),
        )
        .disabled(disabled)
        .attachments(self.attachment_labels(cx))
        .on_attach(cx.listener(|this, _, window, cx| this.pick_attachments(window, cx)))
            .on_drop(cx.listener(|this, paths: &ExternalPaths, _, cx| {
                this.add_attachment_paths(paths.paths().to_vec());
            cx.notify();
        }))
        .on_paste(cx.listener(|this, action: &Paste, window, cx| {
            this.paste_from_clipboard(action, window, cx);
        }))
                .when_some(self.reaction.as_ref(), |composer, draft| composer.reference(
                                                format!("Reply to root about child {} · Event {}", draft.reference.run_id, draft.reference.event_id),
                                                cx.listener(|this, _, window, cx| {
                                                    this.reaction = None;
                                                    this.composer.read(cx).focus_handle(cx).focus(window, cx);
                                                    cx.notify();
                                                }),
                                            )),
                                        ),
                                    )
                                    .child(
                                        div().text_xs().text_color(theme::MUTED).child(
                                            if self.online {
                                                "Local daemon · deterministic executor · Enter sends or queues · Shift+Enter adds a line"
                                            } else {
                                                "Disconnected · the daemon continues independently"
                                            },
                                        ),
                                    ),
                            ),
                    ),
            );
        }
        if !self.show_connection
            && !self.show_settings
            && self.browser_open
            && self.daemon.is_some()
        {
            let mut frame = div()
                .flex()
                .size_full()
                .child(self.workspace_navigator(cx))
                .child(self.session_drawer(cx))
                .child(content);
            if self.inspector_tab == InspectorTab::Changes && self.changes_state.is_some() {
                frame = frame.child(self.changes_inspector(cx));
            } else if self.artifact_preview.is_some() {
                frame = frame.child(self.artifact_inspector(cx));
            }
            frame
        } else if !self.show_connection
            && (self.changes_state.is_some() || self.artifact_preview.is_some())
        {
            let mut frame = div().flex().size_full().child(content);
            if self.inspector_tab == InspectorTab::Changes && self.changes_state.is_some() {
                frame = frame.child(self.changes_inspector(cx));
            } else if self.artifact_preview.is_some() {
                frame = frame.child(self.artifact_inspector(cx));
            }
            frame
        } else {
            content
        }
    }
}

fn usage_detail(
    label: impl Into<SharedString>,
    value: impl Into<SharedString>,
) -> impl IntoElement {
    div()
        .w_full()
        .flex()
        .items_start()
        .gap_3()
        .child(
            div()
                .w(px(112.0))
                .flex_none()
                .text_xs()
                .text_color(theme::MUTED)
                .child(label.into()),
        )
        .child(
            div()
                .min_w_0()
                .flex_1()
                .font_family(theme::MONO_FONT)
                .text_size(px(11.0))
                .line_height(px(16.0))
                .text_color(theme::TEXT_SOFT)
                .child(value.into()),
        )
}

fn usage_quantity_label(quantity: &kiln_protocol::UsageQuantityResponse) -> String {
    let relation = match quantity.relation {
        UsageQuantityRelation::Additive => "additive".to_owned(),
        UsageQuantityRelation::Informational => "informational".to_owned(),
        UsageQuantityRelation::Subset => quantity.subset_of.as_deref().map_or_else(
            || "subset · parent not reported".to_owned(),
            |parent| format!("subset of {parent}"),
        ),
    };
    format!(
        "{} · {} {} · {relation}",
        quantity.dimension, quantity.amount, quantity.unit
    )
}

fn usage_accounting_label(accounting: UsageAccounting) -> &'static str {
    match accounting {
        UsageAccounting::Delta => "delta",
        UsageAccounting::Cumulative => "cumulative",
    }
}

fn usage_finality_label(finality: UsageFinality) -> &'static str {
    match finality {
        UsageFinality::Partial => "partial",
        UsageFinality::Final => "final",
        UsageFinality::Correction => "correction",
    }
}

fn usage_completeness_label(completeness: UsageCompleteness) -> &'static str {
    match completeness {
        UsageCompleteness::Complete => "complete",
        UsageCompleteness::Partial => "partial completeness",
        UsageCompleteness::Unknown => "unknown completeness",
    }
}

fn usage_source_label(source: UsageSource) -> &'static str {
    match source {
        UsageSource::NativeProvider => "native provider",
    }
}

fn compact_run_id(run_id: &str) -> String {
    run_id.chars().take(14).collect()
}

fn compact_session_id(session_id: &str) -> String {
    session_id.chars().take(18).collect()
}

fn compact_content_hash(content_hash: &str) -> String {
    let length = content_hash.chars().count();
    if length <= 24 {
        return content_hash.to_owned();
    }
    let prefix: String = content_hash.chars().take(12).collect();
    let suffix: String = content_hash
        .chars()
        .rev()
        .take(12)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    format!("{prefix}…{suffix}")
}

fn changes_state_session_id(state: &ChangesState) -> &str {
    match state {
        ChangesState::Loading { session_id }
        | ChangesState::Ready { session_id, .. }
        | ChangesState::Failed { session_id, .. } => session_id,
    }
}

fn change_diff_state_path(state: &ChangeDiffState) -> &str {
    match state {
        ChangeDiffState::Loading { path, .. }
        | ChangeDiffState::Ready { path, .. }
        | ChangeDiffState::Failed { path, .. } => path,
    }
}

fn change_diff_unavailable_reason_label(
    reason: SessionChangeDiffUnavailableReason,
) -> &'static str {
    match reason {
        SessionChangeDiffUnavailableReason::Untracked => {
            "This file is untracked, so there is no committed baseline diff."
        }
        SessionChangeDiffUnavailableReason::Binary => {
            "Binary files are listed in the summary but are not rendered as text."
        }
        SessionChangeDiffUnavailableReason::Conflicted => {
            "Conflicted files are not rendered until the conflict is resolved."
        }
        SessionChangeDiffUnavailableReason::Renamed => {
            "Renamed files do not have a single text diff in this view."
        }
        SessionChangeDiffUnavailableReason::UnsupportedFileType => {
            "This file type is not supported for a safe text preview."
        }
        SessionChangeDiffUnavailableReason::UnsupportedEncoding => {
            "This file is not encoded as supported UTF-8 text."
        }
    }
}

fn change_summary_counts(summary: &SessionChangesResponse) -> (u64, u64, usize, usize) {
    let mut additions = 0;
    let mut deletions = 0;
    let mut binary = 0;
    let mut untracked = 0;
    for file in &summary.files {
        additions += file.additions.unwrap_or(0);
        deletions += file.deletions.unwrap_or(0);
        binary += usize::from(file.kind == "binary");
        untracked += usize::from(file.kind == "untracked");
    }
    (additions, deletions, binary, untracked)
}

fn change_file_counts(file: &kiln_protocol::ChangedFileResponse) -> String {
    match (file.additions, file.deletions) {
        (Some(additions), Some(deletions)) => format!("+{additions} · −{deletions}"),
        (Some(additions), None) => format!("+{additions} · deletion count unavailable"),
        (None, Some(deletions)) => format!("addition count unavailable · −{deletions}"),
        (None, None) => match file.kind.as_str() {
            "binary" => "Binary · line counts unavailable".to_owned(),
            "untracked" => "Untracked · line counts unavailable".to_owned(),
            _ => "Line counts unavailable".to_owned(),
        },
    }
}

fn change_kind_label(kind: &str) -> &'static str {
    match kind {
        "added" => "Added",
        "modified" => "Modified",
        "deleted" => "Deleted",
        "type_changed" => "Type changed",
        "conflicted" => "Conflicted",
        "renamed" => "Renamed",
        "untracked" => "Untracked",
        "binary" => "Binary",
        _ => "Changed",
    }
}

fn supports_artifact_preview(media_type: &str) -> bool {
    let media_type = media_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    media_type.starts_with("text/")
        || media_type == "application/json"
        || media_type.ends_with("+json")
        || media_type == "application/xml"
        || media_type.ends_with("+xml")
        || matches!(
            media_type.as_str(),
            "application/javascript"
                | "application/x-javascript"
                | "application/sql"
                | "application/toml"
                | "application/yaml"
                | "application/x-yaml"
                | "application/csv"
        )
}

fn text_preview(bytes: &[u8], truncated: bool) -> Result<String, &'static str> {
    let limit = bytes.len().min(ARTIFACT_PREVIEW_LIMIT);
    let slice = &bytes[..limit];
    match std::str::from_utf8(slice) {
        Ok(content) => Ok(content.to_owned()),
        Err(error) if error.error_len().is_none() => {
            if truncated {
                Ok(String::from_utf8_lossy(&slice[..error.valid_up_to()]).into_owned())
            } else {
                Err("This artifact is not valid UTF-8 text.")
            }
        }
        Err(_) => Err("This artifact is not valid UTF-8 text."),
    }
}

fn run_input_mode_label(mode: RunInputMode) -> &'static str {
    match mode {
        RunInputMode::Interactive => "interactive",
        RunInputMode::ReadOnly => "read only",
    }
}

fn run_accepts_commands(state: &RunState) -> bool {
    matches!(
        state,
        RunState::Queued | RunState::Running | RunState::WaitingForApproval
    )
}

fn run_state_label(state: RunState) -> &'static str {
    match state {
        RunState::Queued => "Queued",
        RunState::Running => "Working",
        RunState::WaitingForApproval => "Needs approval",
        RunState::Cancelling => "Stopping",
        RunState::Completed => "Completed",
        RunState::Failed => "Failed",
        RunState::Cancelled => "Cancelled",
    }
}

fn media_type_for_path(path: &std::path::Path) -> String {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.to_ascii_lowercase())
        .as_deref()
    {
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("svg") => "image/svg+xml",
        Some("pdf") => "application/pdf",
        Some("json") => "application/json",
        Some("txt") | Some("md") | Some("rs") | Some("toml") | Some("yaml") | Some("yml") => {
            "text/plain"
        }
        _ => "application/octet-stream",
    }
    .to_owned()
}

fn text_input(
    value: String,
    placeholder: &'static str,
    window: &mut Window,
    cx: &mut Context<Desktop>,
) -> Entity<InputState> {
    cx.new(|cx| {
        InputState::new(window, cx)
            .placeholder(placeholder)
            .default_value(value)
    })
}

fn field(label: &'static str, input: &Entity<InputState>) -> impl IntoElement {
    div()
        .flex()
        .flex_col()
        .gap_2()
        .child(div().text_sm().child(label))
        .child(Input::new(input).aria_label(label))
}

impl Drop for Desktop {
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take() {
            stream.abort();
        }
    }
}
