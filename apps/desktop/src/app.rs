use std::{collections::BTreeMap, sync::Arc};

use gpui::{
    Context, Entity, Focusable, IntoElement, Render, SharedString, Subscription, Window, div,
    prelude::*, px,
};
use gpui_component::{
    Disableable, Selectable, Sizable,
    button::{Button, ButtonVariants},
    input::{Input, InputEvent, InputState, TextareaState},
};
use kiln_protocol::{
    ApprovalDecision, ApprovalDecisionRequest, ApprovalState, MessageDeliveryMode, RunInputMode,
    RunState, SendRunInputRequest, SessionEventResponse, SessionResponse, StartChildRunRequest,
    WebSocketFrame, WorkspaceResponse,
};
use tokio::{runtime::Runtime, sync::mpsc, task::JoinHandle};

use crate::{
    components::{
        ApprovalPanel, ChildRunRow, Composer, GuidanceComposer, RunStatus, TranscriptRow,
    },
    connection::{self, Connected, ConnectionConfig, Submission},
    conversation::Conversation,
    theme,
};

enum Update {
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
        self.event_generation = self.event_generation.wrapping_add(1);
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
            .is_some_and(|pending| pending.append_uncertain)
        {
            return;
        }
        let Some(connected) = &self.connection else {
            return;
        };
        let session_id = connected.session.session_id.clone();
        let content = self.composer.read(cx).value().to_string();
        if content.trim().is_empty() && self.pending.is_none() {
            return;
        }
        let mut submission = self.pending.take().unwrap_or_else(|| Submission {
            session_id: session_id.clone(),
            content,
            idempotency_key: ulid::Ulid::generate().to_string(),
            active_run_id: self
                .reaction
                .as_ref()
                .map(|draft| draft.root_run_id.clone())
                .or_else(|| self.conversation.active_root().map(str::to_owned)),
            child_activity: self.reaction.as_ref().map(|draft| draft.reference.clone()),
            message_appended: false,
            append_uncertain: false,
        });
        let client = connected.client.clone();
        let session = session_id.clone();
        let root = connected.workspace.roots[0].workspace_root_id.clone();
        let updates = self.updates.clone();
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
        cx.notify();
    }

    fn stash_session_state(&mut self, cx: &mut Context<Self>) {
        let Some(session_id) = self.active_session_id().map(str::to_owned) else {
            return;
        };
        let content = self.composer.read(cx).value().to_string();
        let reaction = self.reaction.take();
        if !content.is_empty() || reaction.is_some() {
            self.drafts
                .insert(session_id.clone(), DraftState { content, reaction });
        } else {
            self.drafts.remove(&session_id);
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
        } else {
            self.composer
                .update(cx, |input, cx| input.set_value("", window, cx));
            self.reaction = None;
        }
        self.pending = self.pending_by_session.remove(session_id);
        self.pending_child_start = self.pending_child_by_session.remove(session_id);
        self.guidance = self
            .guidance_by_session
            .remove(session_id)
            .unwrap_or_default();
    }

    fn clear_saved_session_state(&mut self) {
        self.pending = None;
        self.pending_child_start = None;
        self.pending_by_session.clear();
        self.pending_child_by_session.clear();
        self.reaction = None;
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
        self.show_connection = false;
        self.selected_run = None;
        self.focused_run = None;
        self.show_runs = false;
        self.error = None;
        self.event_generation = self.event_generation.wrapping_add(1);
        self.request_sessions(workspace_id);
        self.subscribe(self.event_generation);
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
                .task_for_run(&run)
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
                        .disabled(self.busy || !self.online),
                    ),
            );
        }
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
            .size_full()
            .bg(theme::BG)
            .text_color(theme::TEXT)
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .px_5()
                    .py_3()
                    .border_b_1()
                    .border_color(theme::BORDER)
                    .child(div().text_sm().child(title))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_3()
                            .when_some(self.conversation.root_state(), |view, state| {
                                view.child(RunStatus::new(state))
                            })
                            .child(
                                Button::new("toggle-browser")
                                    .label(if self.browser_open {
                                        "Hide browser"
                                    } else {
                                        "Browse"
                                    })
                                    .disabled(self.daemon.is_none())
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.browser_open = !this.browser_open;
                                        cx.notify();
                                    })),
                            )
                            .child(
                                Button::new("toggle-runs")
                                    .label(if self.show_runs {
                                        "Close Runs".to_owned()
                                    } else {
                                        format!("Runs · {}", self.conversation.runs.len())
                                    })
                                    .disabled(
                                        self.connection.is_none()
                                            || self.switching_session.is_some(),
                                    )
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.show_runs = !this.show_runs;
                                        cx.notify();
                                    })),
                            )
                            .child(
                                Button::new("connection-settings")
                                    .label("Connection")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.show_connection = !this.show_connection;
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
                            self.pending
                                .as_ref()
                                .is_some_and(|pending| !pending.append_uncertain)
                                || self.pending_child_start.is_some(),
                            |row| {
                                row.child(
                                    Button::new("retry-command")
                                        .label("Retry")
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
        if !self.show_connection && self.browser_open && self.daemon.is_some() {
            div()
                .flex()
                .size_full()
                .child(self.workspace_navigator(cx))
                .child(self.session_drawer(cx))
                .child(content)
        } else {
            content
        }
    }
}

fn compact_run_id(run_id: &str) -> String {
    run_id.chars().take(14).collect()
}

fn compact_session_id(session_id: &str) -> String {
    session_id.chars().take(18).collect()
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
