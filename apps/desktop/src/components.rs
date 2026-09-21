use std::rc::Rc;

use gpui::{
    App, ClickEvent, Entity, ExternalPaths, FontWeight, IntoElement, RenderOnce, Role,
    SharedString, Window, div, prelude::*, px,
};
use gpui_component::{
    Disableable, Icon, IconName, Selectable, Sizable,
    button::{Button, ButtonVariants},
    input::{Textarea, TextareaState},
};
use kiln_protocol::{RunState, TaskState};

use crate::{PasteAttachments, theme};

pub type ClickHandler = Rc<dyn Fn(&ClickEvent, &mut Window, &mut App)>;
pub type DropHandler = Rc<dyn Fn(&ExternalPaths, &mut Window, &mut App)>;
pub type PasteHandler = Rc<dyn Fn(&PasteAttachments, &mut Window, &mut App)>;

#[derive(Clone)]
pub enum AttachmentThumbnail {
    Icon(IconName),
}

#[derive(Clone)]
pub struct AttachmentChip {
    pub label: SharedString,
    pub thumbnail: Option<AttachmentThumbnail>,
    pub retry: Option<ClickHandler>,
    pub remove: ClickHandler,
}

#[derive(IntoElement)]
pub struct TranscriptRow {
    actor: SharedString,
    text: SharedString,
    detail: Option<SharedString>,
    reaction: Option<ClickHandler>,
    reaction_disabled: bool,
    artifact: Option<ClickHandler>,
    artifact_disabled: bool,
    attachment_actions: Vec<(SharedString, ClickHandler)>,
    attachment_actions_disabled: bool,
}

impl TranscriptRow {
    pub fn new(actor: impl Into<SharedString>, text: impl Into<SharedString>) -> Self {
        Self {
            actor: actor.into(),
            text: text.into(),
            detail: None,
            reaction: None,
            reaction_disabled: false,
            artifact: None,
            artifact_disabled: false,
            attachment_actions: Vec::new(),
            attachment_actions_disabled: false,
        }
    }

    pub fn detail(mut self, detail: impl Into<SharedString>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn reaction(
        mut self,
        disabled: bool,
        on_react: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.reaction = Some(Rc::new(on_react));
        self.reaction_disabled = disabled;
        self
    }

    pub fn artifact(
        mut self,
        disabled: bool,
        on_open: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.artifact = Some(Rc::new(on_open));
        self.artifact_disabled = disabled;
        self
    }

    pub fn attachment_action(
        mut self,
        label: impl Into<SharedString>,
        disabled: bool,
        on_open: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.attachment_actions
            .push((label.into(), Rc::new(on_open)));
        self.attachment_actions_disabled = disabled;
        self
    }
}

impl RenderOnce for TranscriptRow {
    fn render(self, _: &mut Window, _: &mut App) -> impl IntoElement {
        let role = if self.reaction.is_some()
            || self.artifact.is_some()
            || !self.attachment_actions.is_empty()
        {
            Role::Group
        } else {
            Role::Label
        };
        let label = format!(
            "{}: {}{}",
            self.actor,
            self.text,
            self.detail
                .as_ref()
                .map_or(String::new(), |detail| format!(". {detail}"))
        );
        let content = div()
            .min_w_0()
            .flex_1()
            .text_size(px(14.0))
            .line_height(px(21.0))
            .text_color(theme::TEXT)
            .child(self.text)
            .when_some(self.detail, |content, detail| {
                content.child(
                    div()
                        .pt_1()
                        .font_family(theme::MONO_FONT)
                        .text_size(px(12.0))
                        .line_height(px(18.0))
                        .text_color(theme::FAINT)
                        .child(detail),
                )
            })
            .when_some(self.reaction, |content, on_react| {
                content.child(
                    Button::new("react-to-activity")
                        .label("Discuss latest update with root")
                        .small()
                        .ghost()
                        .disabled(self.reaction_disabled)
                        .on_click(move |event, window, cx| on_react(event, window, cx)),
                )
            })
            .when_some(self.artifact, |content, on_open| {
                content.child(
                    Button::new("open-artifact")
                        .label("Open artifact")
                        .small()
                        .ghost()
                        .disabled(self.artifact_disabled)
                        .on_click(move |event, window, cx| on_open(event, window, cx)),
                )
            })
            .children(self.attachment_actions.into_iter().enumerate().map(
                |(index, (label, on_open))| {
                    Button::new(format!("open-attachment-{index}"))
                        .label(label.clone())
                        .accessibility_label(label)
                        .small()
                        .ghost()
                        .disabled(self.attachment_actions_disabled)
                        .on_click(move |event, window, cx| on_open(event, window, cx))
                },
            ));

        div()
            .id("transcript-row")
            .role(role)
            .aria_label(label)
            .w_full()
            .flex()
            .flex_row()
            .items_start()
            .gap_4()
            .py_3()
            .child(
                div()
                    .w(px(96.0))
                    .flex_none()
                    .font_family(theme::MONO_FONT)
                    .text_size(px(12.0))
                    .line_height(px(21.0))
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(theme::MUTED)
                    .child(self.actor),
            )
            .child(content)
    }
}

#[derive(IntoElement)]
pub struct RunStatus {
    state: RunState,
}

impl RunStatus {
    pub fn new(state: RunState) -> Self {
        Self { state }
    }
}

impl RenderOnce for RunStatus {
    fn render(self, _: &mut Window, _: &mut App) -> impl IntoElement {
        let (label, color) = run_state_presentation(&self.state);

        div()
            .id("run-status")
            .role(Role::Label)
            .aria_label(label)
            .font_family(theme::MONO_FONT)
            .text_size(px(12.0))
            .line_height(px(18.0))
            .font_weight(FontWeight::MEDIUM)
            .text_color(color)
            .child(label)
    }
}

fn run_state_presentation(state: &RunState) -> (&'static str, gpui::Hsla) {
    match state {
        RunState::Queued => ("Queued", theme::MUTED),
        RunState::Running => ("Running", theme::ACCENT),
        RunState::WaitingForApproval => ("Waiting for approval", theme::ATTENTION),
        RunState::Cancelling => ("Cancelling", theme::ATTENTION),
        RunState::Completed => ("Completed", theme::SUCCESS),
        RunState::Failed => ("Failed", theme::DANGER),
        RunState::Cancelled => ("Cancelled", theme::MUTED),
    }
}

#[derive(IntoElement)]
pub struct ChildRunRow {
    id: SharedString,
    title: SharedString,
    detail: SharedString,
    state: RunState,
    selected: bool,
    on_open: ClickHandler,
    tasks: Vec<(String, TaskState)>,
}

impl ChildRunRow {
    pub fn new(
        id: impl Into<SharedString>,
        title: impl Into<SharedString>,
        detail: impl Into<SharedString>,
        state: RunState,
        selected: bool,
        on_open: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        Self {
            id: id.into(),
            title: title.into(),
            detail: detail.into(),
            state,
            selected,
            on_open: Rc::new(on_open),
            tasks: Vec::new(),
        }
    }

    pub fn tasks(mut self, tasks: Vec<(String, TaskState)>) -> Self {
        self.tasks = tasks;
        self
    }
}

impl RenderOnce for ChildRunRow {
    fn render(self, _: &mut Window, _: &mut App) -> impl IntoElement {
        let (state_label, _) = run_state_presentation(&self.state);
        let accessibility_label = format!(
            "Open child Run details for {}. {}. {state_label}",
            self.title, self.detail
        );
        let on_open = self.on_open;

        Button::new(self.id)
            .ghost()
            .selected(self.selected)
            .accessibility_label(accessibility_label)
            .w_full()
            .h_auto()
            .flex_col()
            .items_start()
            .px_2()
            .py_2()
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
                            .child(self.title),
                    )
                    .child(
                        div()
                            .w_full()
                            .font_family(theme::MONO_FONT)
                            .text_size(px(12.0))
                            .line_height(px(18.0))
                            .text_color(theme::FAINT)
                            .child(self.detail),
                    ),
            )
            .child(RunStatus::new(self.state))
            .when(!self.tasks.is_empty(), |row| {
                row.child(TaskProgress {
                    tasks: self.tasks,
                    expanded: self.selected,
                })
            })
            .on_click(move |event, window, cx| on_open(event, window, cx))
    }
}

#[derive(IntoElement)]
struct TaskProgress {
    tasks: Vec<(String, TaskState)>,
    expanded: bool,
}

impl RenderOnce for TaskProgress {
    fn render(self, _: &mut Window, _: &mut App) -> impl IntoElement {
        let completed = self
            .tasks
            .iter()
            .filter(|(_, state)| *state == TaskState::Completed)
            .count();
        let summary = format!("{completed}/{} Tasks completed", self.tasks.len());
        let mut segments = div().w_full().flex().gap_1();
        let mut tasks = div().w_full().flex().flex_col().gap_1();
        for (title, state) in self.tasks {
            let (label, color) = match state {
                TaskState::Pending => ("Pending", theme::MUTED),
                TaskState::Ready => ("Ready", theme::MUTED),
                TaskState::Running => ("Running", theme::ACCENT),
                TaskState::Blocked => ("Blocked", theme::ATTENTION),
                TaskState::Completed => ("Completed", theme::SUCCESS),
                TaskState::Failed => ("Failed", theme::DANGER),
                TaskState::Cancelled => ("Cancelled", theme::MUTED),
            };
            segments = segments.child(div().flex_1().h(px(3.0)).bg(color));
            tasks = tasks.child(
                div()
                    .text_xs()
                    .text_color(color)
                    .child(format!("{label} · {title}")),
            );
        }
        div()
            .w_full()
            .flex()
            .flex_col()
            .gap_2()
            .child(div().text_xs().text_color(theme::MUTED).child(summary))
            .child(segments)
            .when(self.expanded, |row| row.child(tasks))
    }
}

#[derive(IntoElement)]
pub struct ApprovalPanel {
    scope: SharedString,
    on_approve: ClickHandler,
    on_reject: ClickHandler,
    disabled: bool,
}

impl ApprovalPanel {
    pub fn new(
        scope: impl Into<SharedString>,
        on_approve: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
        on_reject: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        Self {
            scope: scope.into(),
            on_approve: Rc::new(on_approve),
            on_reject: Rc::new(on_reject),
            disabled: false,
        }
    }

    pub fn disabled(mut self, disabled: bool) -> Self {
        self.disabled = disabled;
        self
    }
}

impl RenderOnce for ApprovalPanel {
    fn render(self, _: &mut Window, _: &mut App) -> impl IntoElement {
        let on_approve = self.on_approve;
        let on_reject = self.on_reject;

        div()
            .w_full()
            .flex()
            .flex_col()
            .gap_3()
            .p_3()
            .bg(theme::SURFACE)
            .border_l_2()
            .border_color(theme::ATTENTION)
            .rounded(theme::RADIUS_SMALL)
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .child(
                        div()
                            .id("approval-heading")
                            .role(Role::Label)
                            .aria_label("Approval required")
                            .font_family(theme::MONO_FONT)
                            .text_size(px(12.0))
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(theme::ATTENTION)
                            .child("Approval required"),
                    )
                    .child(
                        div()
                            .id("approval-scope")
                            .role(Role::Label)
                            .aria_label(self.scope.clone())
                            .text_size(px(14.0))
                            .line_height(px(21.0))
                            .text_color(theme::TEXT_SOFT)
                            .child(self.scope),
                    ),
            )
            .child(
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .gap_2()
                    .child(
                        Button::new("approval-approve")
                            .label("Approve")
                            .primary()
                            .small()
                            .disabled(self.disabled)
                            .on_click(move |event, window, cx| {
                                on_approve(event, window, cx);
                            }),
                    )
                    .child(
                        Button::new("approval-reject")
                            .label("Reject")
                            .danger()
                            .small()
                            .disabled(self.disabled)
                            .on_click(move |event, window, cx| {
                                on_reject(event, window, cx);
                            }),
                    ),
            )
    }
}

#[derive(IntoElement)]
pub struct GuidanceComposer {
    input: Entity<TextareaState>,
    target: SharedString,
    on_queue: ClickHandler,
    on_interrupt: ClickHandler,
    busy: bool,
    accepts_input: bool,
    pending: bool,
    error: Option<String>,
}

impl GuidanceComposer {
    pub fn new(
        input: &Entity<TextareaState>,
        target: impl Into<SharedString>,
        on_queue: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
        on_interrupt: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        Self {
            input: input.clone(),
            target: target.into(),
            on_queue: Rc::new(on_queue),
            on_interrupt: Rc::new(on_interrupt),
            busy: false,
            accepts_input: false,
            pending: false,
            error: None,
        }
    }

    pub fn availability(mut self, busy: bool, accepts_input: bool, pending: bool) -> Self {
        self.busy = busy;
        self.accepts_input = accepts_input;
        self.pending = pending;
        self
    }

    pub fn error(mut self, error: Option<String>) -> Self {
        self.error = error;
        self
    }
}

impl RenderOnce for GuidanceComposer {
    fn render(self, _: &mut Window, _: &mut App) -> impl IntoElement {
        let on_queue = self.on_queue;
        let on_interrupt = self.on_interrupt;
        div().w_full().flex().flex_col().gap_2()
            .child(div().text_sm().child("Child guidance"))
            .child(Textarea::new(&self.input)
                .aria_label(format!("Guidance to {}", self.target))
                .h(px(100.0))
                .disabled(self.busy || self.pending || !self.accepts_input))
            .child(div().flex().flex_wrap().gap_2()
                .child(Button::new("queue-guidance")
                    .label(if self.pending { "Retry guidance" } else { "Queue guidance" })
                    .small()
                    .disabled(self.busy || (!self.pending && !self.accepts_input))
                    .on_click(move |event, window, cx| on_queue(event, window, cx)))
                .child(Button::new("interrupt-guidance")
                    .label("Interrupt child")
                    .small()
                    .disabled(self.busy || self.pending || !self.accepts_input)
                    .on_click(move |event, window, cx| on_interrupt(event, window, cx))))
            .child(div().text_xs().text_color(theme::MUTED).child(
                if self.pending {
                    "Retry sends the same guidance to the same child."
                } else if !self.accepts_input {
                    "This child cannot accept new input. You can still message the root."
                } else {
                    "Queue delivers at the next safe boundary. Interrupt requests a stop of current child work, then delivers guidance."
                }))
            .when_some(self.error, |view, error| view.child(
                div().id("guidance-error").role(Role::Alert).aria_label(error.clone())
                    .text_sm().text_color(theme::DANGER).child(error)))
    }
}

#[derive(IntoElement)]
pub struct Composer {
    input: Entity<TextareaState>,
    scope: SharedString,
    is_running: bool,
    on_submit: ClickHandler,
    on_stop: ClickHandler,
    disabled: bool,
    reference: Option<(SharedString, ClickHandler)>,
    attachments: Vec<AttachmentChip>,
    on_attach: Option<ClickHandler>,
    on_drop: Option<DropHandler>,
    on_paste: Option<PasteHandler>,
}

impl Composer {
    pub fn new(
        input: &Entity<TextareaState>,
        scope: impl Into<SharedString>,
        is_running: bool,
        on_submit: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
        on_stop: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        Self {
            input: input.clone(),
            scope: scope.into(),
            is_running,
            on_submit: Rc::new(on_submit),
            on_stop: Rc::new(on_stop),
            disabled: false,
            reference: None,
            attachments: Vec::new(),
            on_attach: None,
            on_drop: None,
            on_paste: None,
        }
    }

    pub fn disabled(mut self, disabled: bool) -> Self {
        self.disabled = disabled;
        self
    }

    pub fn reference(
        mut self,
        label: impl Into<SharedString>,
        on_clear: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.reference = Some((label.into(), Rc::new(on_clear)));
        self
    }

    pub fn attachments(mut self, attachments: Vec<AttachmentChip>) -> Self {
        self.attachments = attachments;
        self
    }

    pub fn on_attach(
        mut self,
        on_attach: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.on_attach = Some(Rc::new(on_attach));
        self
    }

    pub fn on_drop(
        mut self,
        on_drop: impl Fn(&ExternalPaths, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.on_drop = Some(Rc::new(on_drop));
        self
    }

    pub fn on_paste(
        mut self,
        on_paste: impl Fn(&PasteAttachments, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.on_paste = Some(Rc::new(on_paste));
        self
    }
}

impl RenderOnce for Composer {
    fn render(self, _: &mut Window, _: &mut App) -> impl IntoElement {
        let action = if self.is_running {
            let on_stop = self.on_stop;
            Button::new("composer-stop")
                .label("Stop")
                .danger()
                .small()
                .disabled(self.disabled)
                .on_click(move |event, window, cx| {
                    on_stop(event, window, cx);
                })
        } else {
            let on_submit = self.on_submit;
            Button::new("composer-submit")
                .label("Submit")
                .primary()
                .small()
                .disabled(self.disabled)
                .on_click(move |event, window, cx| {
                    on_submit(event, window, cx);
                })
        };

        let attachment_button = self.on_attach.map(|on_attach| {
            Button::new("composer-attach")
                .label("Attach")
                .small()
                .disabled(self.disabled)
                .on_click(move |event, window, cx| on_attach(event, window, cx))
        });
        let attachment_list =
            self.attachments
                .iter()
                .fold(div().flex().flex_wrap().gap_1(), |row, attachment| {
                    let remove = attachment.remove.clone();
                    let thumbnail = attachment
                        .thumbnail
                        .clone()
                        .map(|thumbnail| match thumbnail {
                            AttachmentThumbnail::Icon(icon) => Icon::new(icon),
                        });
                    row.child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .px_1()
                            .border_1()
                            .border_color(theme::BORDER)
                            .rounded(theme::RADIUS_SMALL)
                            .when_some(thumbnail, |chip, thumbnail| chip.child(thumbnail.size_8()))
                            .child(attachment.label.clone())
                            .when_some(attachment.retry.clone(), |chip, retry| {
                                chip.child(
                                    Button::new("retry-attachment")
                                        .label("Retry")
                                        .small()
                                        .on_click(move |event, window, cx| {
                                            retry(event, window, cx)
                                        }),
                                )
                            })
                            .child(
                                Button::new("remove-attachment")
                                    .label("Remove")
                                    .small()
                                    .on_click(move |event, window, cx| remove(event, window, cx)),
                            ),
                    )
                });
        let on_drop = self.on_drop;
        let on_paste = self.on_paste;
        let composer = div()
            .key_context("KilnComposer")
            .when_some(on_drop, |composer, on_drop| {
                composer.on_drop(move |paths: &ExternalPaths, window, cx| {
                    on_drop(paths, window, cx);
                })
            })
            .when_some(on_paste, |composer, on_paste| {
                composer.on_action(move |action: &PasteAttachments, window, cx| {
                    on_paste(action, window, cx);
                })
            })
            .w_full()
            .max_w(theme::TRANSCRIPT_WIDTH)
            .h(px(164.0))
            .flex()
            .flex_col()
            .gap_2()
            .p_3()
            .bg(theme::SURFACE)
            .border_1()
            .border_color(theme::BORDER)
            .rounded(theme::RADIUS_MEDIUM)
            .child(
                Textarea::new(&self.input)
                    .min_h_0()
                    .flex_1()
                    .aria_label("Message to root Run")
                    .disabled(self.disabled),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .when_some(attachment_button, |row, button| row.child(button))
                    .child(attachment_list),
            )
            .child(
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .justify_between()
                    .gap_3()
                    .child(
                        div()
                            .id("composer-scope")
                            .role(Role::Label)
                            .aria_label(format!("Target: Root Run. Scope: {}", self.scope))
                            .min_w_0()
                            .font_family(theme::MONO_FONT)
                            .text_size(px(12.0))
                            .line_height(px(18.0))
                            .text_color(theme::FAINT)
                            .child(format!("Root Run · Scope: {}", self.scope)),
                    )
                    .child(action),
            );
        div()
            .w_full()
            .max_w(theme::TRANSCRIPT_WIDTH)
            .flex()
            .flex_col()
            .gap_2()
            .when_some(self.reference, |view, (label, on_clear)| {
                view.child(
                    div()
                        .flex()
                        .items_start()
                        .gap_2()
                        .child(
                            div()
                                .id("reaction-reference")
                                .role(Role::Label)
                                .aria_label(label.clone())
                                .flex_1()
                                .min_w_0()
                                .text_xs()
                                .child(label),
                        )
                        .child(
                            Button::new("clear-reaction")
                                .label("Clear reference")
                                .small()
                                .disabled(self.disabled)
                                .on_click(move |event, window, cx| on_clear(event, window, cx)),
                        ),
                )
            })
            .child(composer)
    }
}
