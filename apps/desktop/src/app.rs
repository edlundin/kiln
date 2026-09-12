use std::{collections::BTreeMap, sync::Arc};

use gpui::{
    Context, Entity, Focusable, IntoElement, Render, SharedString, Subscription, Window, div,
    prelude::*, px,
};
use gpui_component::{
    Disableable, Sizable,
    button::{Button, ButtonVariants},
    input::{Input, InputEvent, InputState, TextareaState},
};
use kiln_protocol::{
    ApprovalDecision, ApprovalDecisionRequest, ApprovalState, MessageDeliveryMode, RunInputMode,
    RunState, SendRunInputRequest, SessionEventResponse, StartChildRunRequest, WebSocketFrame,
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
    Connected(Result<Connected, String>),
    Event(SessionEventResponse),
    Disconnected(String),
    Submitted(Submission, Result<(), String>),
    ChildStarted(Result<(), String>),
    Guided {
        run_id: String,
        result: Result<(), String>,
    },
    Command(Result<(), String>),
}

#[derive(Clone)]
struct ChildStart {
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

pub struct Desktop {
    address: Entity<InputState>,
    token_file: Entity<InputState>,
    repository: Entity<InputState>,
    session: Entity<InputState>,
    composer: Entity<TextareaState>,
    connection: Option<Connected>,
    conversation: Conversation,
    runtime: Arc<Runtime>,
    updates: mpsc::UnboundedSender<Update>,
    stream: Option<JoinHandle<()>>,
    pending: Option<Submission>,
    pending_child_start: Option<ChildStart>,
    reaction: Option<ReactionDraft>,
    guidance: BTreeMap<String, GuidanceDraft>,
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
        let autoconnect = !config.address.is_empty()
            && !config.token_file.is_empty()
            && (!config.repository_path.is_empty() || !config.session_id.is_empty());
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
            composer,
            connection: None,
            conversation: Conversation::default(),
            runtime,
            updates,
            stream: None,
            pending: None,
            pending_child_start: None,
            reaction: None,
            guidance: BTreeMap::new(),
            online: false,
            busy: false,
            show_connection: true,
            selected_run: None,
            focused_run: None,
            show_runs: false,
            error: None,
            _subscriptions: vec![subscription],
        };
        if autoconnect {
            desktop.connect(cx);
        }
        desktop
    }

    fn connect(&mut self, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        if let Some(stream) = self.stream.take() {
            stream.abort();
        }
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
            let _ = updates.send(Update::Connected(connection::connect(config).await));
        });
        cx.notify();
    }

    fn subscribe(&mut self) {
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
                    let _ = updates.send(Update::Disconnected(connection::error_message(
                        "Connect event stream",
                        &error,
                    )));
                    return;
                }
            };
            loop {
                match stream.next_frame().await {
                    Ok(Some(WebSocketFrame::Event { event })) if event.session_id == session_id => {
                        if updates.send(Update::Event(event)).is_err() {
                            break;
                        }
                    }
                    Ok(Some(WebSocketFrame::Error { code, .. })) => {
                        let _ = updates.send(Update::Disconnected(format!(
                            "Event stream: {code}. Reconnect to resume."
                        )));
                        break;
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        let _ = updates.send(Update::Disconnected(
                            "Connection closed. Reconnect to resume.".to_owned(),
                        ));
                        break;
                    }
                    Err(error) => {
                        let _ = updates.send(Update::Disconnected(connection::error_message(
                            "Read event stream",
                            &error,
                        )));
                        break;
                    }
                }
            }
        }));
    }

    fn submit(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if self.busy || !self.online || self.conversation.root_state() == Some(RunState::Cancelling)
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
        let content = self.composer.read(cx).value().to_string();
        if content.trim().is_empty() && self.pending.is_none() {
            return;
        }
        let mut submission = self.pending.take().unwrap_or_else(|| Submission {
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
        let session = connected.session.session_id.clone();
        let root = connected.workspace.roots[0].workspace_root_id.clone();
        let updates = self.updates.clone();
        self.busy = true;
        self.error = None;
        self.runtime.spawn(async move {
            let result = connection::submit(&client, &session, &root, &mut submission).await;
            let _ = updates.send(Update::Submitted(submission, result));
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
        if self.busy || !self.online || self.pending.is_some() {
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
        let client = connected.client.clone();
        let updates = self.updates.clone();
        self.busy = true;
        self.runtime.spawn(async move {
            let result = client
                .cancel_run(&run)
                .await
                .map(|_| ())
                .map_err(|error| connection::error_message(operation, &error));
            let _ = updates.send(Update::Command(result));
        });
        cx.notify();
    }

    fn start_child(&mut self, cx: &mut Context<Self>) {
        if self.busy || !self.online {
            return;
        }
        let Some(connected) = &self.connection else {
            return;
        };
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
            let _ = updates.send(Update::ChildStarted(result));
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
        if self.busy || !self.online {
            return;
        }
        let Some(connected) = &self.connection else {
            return;
        };
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
            let _ = updates.send(Update::Guided { run_id, result });
        });
        cx.notify();
    }

    fn decide(&mut self, tool_call: String, decision: ApprovalDecision, cx: &mut Context<Self>) {
        if self.busy || !self.online {
            return;
        }
        let Some(connected) = &self.connection else {
            return;
        };
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
            let _ = updates.send(Update::Command(result));
        });
        cx.notify();
    }

    fn toggle_run_details(&mut self, run_id: String, window: &mut Window, cx: &mut Context<Self>) {
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

    fn apply(&mut self, update: Update, window: &mut Window, cx: &mut Context<Self>) {
        match update {
            Update::Connected(Ok(connected)) => {
                let same_session = self.connection.as_ref().is_some_and(|previous| {
                    previous.session.session_id == connected.session.session_id
                        && previous.negotiated.store_identity.id
                            == connected.negotiated.store_identity.id
                });
                self.conversation = Conversation::default();
                for run in connected.initial_runs.runs.iter().cloned() {
                    self.conversation.apply_run(run);
                }
                for event in connected.initial_events.events.iter().cloned() {
                    self.conversation.apply(event);
                }
                self.session.update(cx, |input, cx| {
                    input.set_value(connected.session.session_id.clone(), window, cx)
                });
                self.connection = Some(connected);
                self.pending = None;
                self.reaction = None;
                if !same_session {
                    self.pending_child_start = None;
                    self.guidance.clear();
                }
                self.online = true;
                self.busy = false;
                self.show_connection = false;
                self.selected_run = None;
                self.focused_run = None;
                self.error = None;
                self.subscribe();
            }
            Update::Connected(Err(error)) => {
                self.busy = false;
                self.show_connection = true;
                self.error = Some(error);
            }
            Update::Event(event) => self.conversation.apply(event),
            Update::Disconnected(error) => {
                self.online = false;
                self.error = Some(error);
            }
            Update::Submitted(submission, result) => {
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
            Update::ChildStarted(result) => {
                self.busy = false;
                match result {
                    Ok(()) => {
                        self.pending_child_start = None;
                        self.error = None;
                    }
                    Err(error) => self.error = Some(error),
                }
            }
            Update::Guided { run_id, result } => {
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
            Update::Command(result) => {
                self.busy = false;
                self.error = result.err();
            }
        }
        cx.notify();
    }

    fn run_rail(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let child_runs = self.conversation.child_runs();
        let can_start_child = self.online
            && !self.busy
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
        let title = self
            .connection
            .as_ref()
            .map_or("Kiln".to_owned(), |connected| {
                connected.workspace.name.clone()
            });
        let scope = self
            .connection
            .as_ref()
            .map_or("No workspace selected".to_owned(), |connected| {
                connected.workspace.roots[0].display_path.clone()
            });
        let active = self.conversation.active_root().is_some();
        let disabled = self.busy
            || !self.online
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
                if event.keystroke.key == "escape"
                    && !this.show_connection
                    && (this.show_runs || this.focused_run.is_some())
                {
                    this.show_runs = false;
                    this.selected_run = None;
                    this.focused_run = None;
                    this.composer.read(cx).focus_handle(cx).focus(window, cx);
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
                                Button::new("toggle-runs")
                                    .label(if self.show_runs {
                                        "Close Runs".to_owned()
                                    } else {
                                        format!("Runs · {}", self.conversation.runs.len())
                                    })
                                    .disabled(self.connection.is_none())
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
        if self.show_connection || self.connection.is_none() {
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
        content
    }
}

fn compact_run_id(run_id: &str) -> String {
    run_id.chars().take(14).collect()
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
