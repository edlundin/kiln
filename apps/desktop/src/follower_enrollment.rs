//! Explicit operations on durable follower enrollment attempts.

use std::sync::Arc;

use gpui::{ClipboardItem, Context, Entity, Render, SharedString, Window, div, prelude::*};
use gpui_component::{
    Disableable,
    button::{Button, ButtonVariants},
    input::{Input, InputState},
};
use kiln_client::Client;
use kiln_protocol::{
    ConfigurationFollowerEnrollmentExchangeResult as ExchangeResult,
    ConfigurationFollowerEnrollmentListResponse as Page,
    ConfigurationFollowerEnrollmentPhase as Phase,
    ConfigurationFollowerEnrollmentResponse as Enrollment, ConfigurationSyncStatusResponse,
    ExchangeConfigurationFollowerEnrollmentRequest as Exchange,
    RetireConfigurationFollowerEnrollmentRequest as Retire,
};
use tokio::{runtime::Runtime, sync::mpsc};
use ulid::Ulid;

use crate::{connection, theme};

enum Update {
    Page(Result<Page, String>),
    Loaded(Result<(Enrollment, ConfigurationSyncStatusResponse), String>),
    Exchanged(Result<Enrollment, String>),
    Retired(Result<(), String>),
}

#[derive(Clone)]
enum Confirmation {
    Exchange(String, Exchange),
    Retire(String, Retire),
}

pub struct FollowerEnrollment {
    client: Client,
    runtime: Arc<Runtime>,
    updates: mpsc::UnboundedSender<(Ulid, Update)>,
    operation: Option<Ulid>,
    online: bool,
    page: Vec<Enrollment>,
    next_cursor: Option<String>,
    selected: Option<(Enrollment, ConfigurationSyncStatusResponse)>,
    confirmation: Option<Confirmation>,
    origin: Option<Entity<InputState>>,
    connect_ms: Option<Entity<InputState>>,
    request_ms: Option<Entity<InputState>>,
    error: Option<String>,
    notice: Option<String>,
}

impl FollowerEnrollment {
    pub fn new(client: Client, runtime: Arc<Runtime>, cx: &mut Context<Self>) -> Self {
        let (updates, mut receiver) = mpsc::unbounded_channel();
        cx.spawn(async move |this, cx| {
            while let Some((id, update)) = receiver.recv().await {
                if this
                    .update(cx, |this, cx| this.apply(id, update, cx))
                    .is_err()
                {
                    break;
                }
            }
        })
        .detach();
        Self {
            client,
            runtime,
            updates,
            operation: None,
            online: true,
            page: Vec::new(),
            next_cursor: None,
            selected: None,
            confirmation: None,
            origin: None,
            connect_ms: None,
            request_ms: None,
            error: None,
            notice: None,
        }
    }

    pub fn set_online(&mut self, online: bool, cx: &mut Context<Self>) {
        if self.online != online {
            self.online = online;
            self.operation = None;
            self.selected = None;
            self.confirmation = None;
            self.page.clear();
            self.next_cursor = None;
            self.error = None;
            self.notice = None;
        }
        cx.notify();
    }

    fn begin(&mut self, cx: &mut Context<Self>) -> Option<Ulid> {
        if !self.online || self.operation.is_some() {
            return None;
        }
        let id = Ulid::generate();
        self.operation = Some(id);
        self.confirmation = None;
        self.error = None;
        self.notice = None;
        cx.notify();
        Some(id)
    }

    fn list(&mut self, next: bool, cx: &mut Context<Self>) {
        let after = if next { self.next_cursor.clone() } else { None };
        if next && after.is_none() {
            return;
        }
        let Some(id) = self.begin(cx) else {
            return;
        };
        self.selected = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            // Matches the endpoint's documented default page size.
            let result = client
                .list_configuration_follower_enrollments(50, after.as_deref())
                .await
                .map_err(|e| connection::error_message("Load follower enrollments", &e));
            let _ = updates.send((id, Update::Page(result)));
        });
    }

    fn load(&mut self, attempt: String, cx: &mut Context<Self>) {
        let Some(id) = self.begin(cx) else {
            return;
        };
        self.selected = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let result = async {
                let status = client.get_configuration_sync_status().await?;
                let enrollment = client
                    .get_configuration_follower_enrollment(&attempt)
                    .await?;
                Ok::<_, kiln_client::Error>((enrollment, status))
            }
            .await
            .map_err(|e| connection::error_message("Load follower enrollment", &e));
            let _ = updates.send((id, Update::Loaded(result)));
        });
    }

    fn prepare_exchange(&mut self, cx: &mut Context<Self>) {
        if !self.online || self.operation.is_some() {
            return;
        }
        let Some((enrollment, status)) = &self.selected else {
            return;
        };
        if enrollment.phase != Phase::Prepared
            || enrollment.follower_instance_id != status.instance_id
        {
            return;
        }
        let origin = self
            .origin
            .as_ref()
            .map(|v| v.read(cx).value().to_string())
            .unwrap_or_default();
        let connect = self
            .connect_ms
            .as_ref()
            .and_then(|v| v.read(cx).value().parse::<u64>().ok());
        let request = self
            .request_ms
            .as_ref()
            .and_then(|v| v.read(cx).value().parse::<u64>().ok());
        let valid_origin = url::Url::parse(&origin).is_ok_and(|url| {
            url.scheme() == "https"
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && url.path() == "/"
                && !origin
                    .bytes()
                    .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
        });
        let Some((connect_timeout_ms, request_timeout_ms)) =
            connect.zip(request).filter(|(c, r)| *c > 0 && *r >= *c)
        else {
            self.error = Some(
                "Enter positive whole-millisecond deadlines, with connect no greater than request."
                    .into(),
            );
            cx.notify();
            return;
        };
        if !valid_origin {
            self.error = Some(
                "Enter an HTTPS origin without credentials, a path, query, or fragment.".into(),
            );
            cx.notify();
            return;
        }
        self.error = None;
        self.confirmation = Some(Confirmation::Exchange(
            enrollment.attempt_id.clone(),
            Exchange {
                expected_instance_id: status.instance_id.clone(),
                origin,
                connect_timeout_ms,
                request_timeout_ms,
            },
        ));
        cx.notify();
    }

    fn prepare_retire(&mut self, cx: &mut Context<Self>) {
        if !self.online || self.operation.is_some() {
            return;
        }
        let Some((enrollment, status)) = &self.selected else {
            return;
        };
        if enrollment.follower_instance_id != status.instance_id {
            return;
        }
        self.confirmation = Some(Confirmation::Retire(
            enrollment.attempt_id.clone(),
            Retire {
                expected_instance_id: status.instance_id.clone(),
            },
        ));
        cx.notify();
    }

    fn execute(&mut self, cx: &mut Context<Self>) {
        let Some(confirmation) = self.confirmation.clone() else {
            return;
        };
        let Some(id) = self.begin(cx) else {
            return;
        };
        self.selected = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let update = match confirmation {
                Confirmation::Exchange(attempt, request) => Update::Exchanged(client.exchange_configuration_follower_enrollment(&attempt, &request).await
                    .map_err(|e| format!("{} Reload this attempt before retrying; the daemon may have completed it.", connection::error_message("Exchange follower enrollment", &e)))),
                Confirmation::Retire(attempt, request) => Update::Retired(client.retire_configuration_follower_enrollment(&attempt, &request).await
                    .map_err(|e| format!("{} Reload this attempt and retry retirement to finish credential cleanup.", connection::error_message("Retire follower enrollment", &e)))),
            };
            let _ = updates.send((id, update));
        });
    }

    fn apply(&mut self, id: Ulid, update: Update, cx: &mut Context<Self>) {
        if !self.online || self.operation != Some(id) {
            return;
        }
        self.operation = None;
        match update {
            Update::Page(Ok(page)) => {
                self.page = page.enrollments;
                self.next_cursor = page.next_cursor;
                if self.page.is_empty() {
                    self.notice = Some("No follower attempts on this page. Prepare an enrollment through the local API first.".into());
                }
            }
            Update::Loaded(Ok(selected)) => self.selected = Some(selected),
            Update::Exchanged(Ok(enrollment)) => {
                if let Some(row) = self
                    .page
                    .iter_mut()
                    .find(|row| row.attempt_id == enrollment.attempt_id)
                {
                    *row = enrollment.clone();
                }
                self.notice = Some(format!(
                    "Exchange recorded for {}: {}. Reload the attempt to inspect its receipt and fingerprint. Refresh configuration status after approval; enrollment does not fetch a snapshot.",
                    enrollment.attempt_id,
                    exchange_label(enrollment.exchange_result)
                ));
            }
            Update::Retired(Ok(())) => {
                self.page.clear();
                self.next_cursor = None;
                self.notice = Some("Enrollment retired and credential cleanup completed. An existing follower role and stored snapshot are unchanged. Refresh configuration status and attempts.".into());
            }
            Update::Page(Err(error))
            | Update::Loaded(Err(error))
            | Update::Exchanged(Err(error))
            | Update::Retired(Err(error)) => self.error = Some(error),
        }
        cx.notify();
    }
}

fn phase_label(phase: Phase) -> &'static str {
    match phase {
        Phase::Reserved => "Preparation incomplete",
        Phase::Prepared => "Prepared",
        Phase::Retired => "Retired",
    }
}

fn exchange_label(result: Option<ExchangeResult>) -> &'static str {
    match result {
        None => "Not exchanged",
        Some(ExchangeResult::Pending) => "Awaiting master approval",
        Some(ExchangeResult::Approved) => "Approved",
        Some(ExchangeResult::Rejected) => "Rejected",
        Some(ExchangeResult::Revoked) => "Grant revoked",
        Some(ExchangeResult::RoleConflict) => "Local role changed before approval",
    }
}

impl Render for FollowerEnrollment {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let origin = self
            .origin
            .get_or_insert_with(|| {
                cx.new(|cx| {
                    InputState::new(window, cx).placeholder("https://master.example.com:port")
                })
            })
            .clone();
        let connect_ms = self
            .connect_ms
            .get_or_insert_with(|| {
                cx.new(|cx| {
                    InputState::new(window, cx).placeholder("Connect deadline in milliseconds")
                })
            })
            .clone();
        let request_ms = self
            .request_ms
            .get_or_insert_with(|| {
                cx.new(|cx| {
                    InputState::new(window, cx)
                        .placeholder("Whole request deadline in milliseconds")
                })
            })
            .clone();
        let disabled = !self.online || self.operation.is_some();
        let mut view = div().flex().flex_col().gap_3().min_w_0()
            .child(div().text_lg().child("Follower enrollment"))
            .child(div().text_sm().text_color(theme::MUTED).child("Submit a prepared enrollment to its pinned master, check approval, or retire its local credential. The daemon keeps the bearer in its OS vault. Receipt status is historical, not a live connectivity or grant check."))
            .when(!self.online, |v| v.child(div().text_sm().text_color(theme::ATTENTION).child("Reconnect and reload the enrollment before continuing.")))
            .child(div().flex().flex_wrap().gap_2()
                .child(Button::new("load-follower-attempts").label(if self.operation.is_some() { "Working…" } else { "Refresh enrollments" }).disabled(disabled)
                    .on_click(cx.listener(|this, _, _, cx| this.list(false, cx))))
                .when(self.next_cursor.is_some(), |v| v.child(Button::new("next-follower-attempts").label("Next page").disabled(disabled)
                    .on_click(cx.listener(|this, _, _, cx| this.list(true, cx))))))
            .when_some(self.error.clone(), |v, error| v.child(div().id("follower-enrollment-error").role(gpui::Role::Alert).aria_label(error.clone()).text_sm().text_color(theme::DANGER).child(error)))
            .when_some(self.notice.clone(), |v, notice| v.child(div().text_sm().child(notice)));
        for enrollment in &self.page {
            let attempt = enrollment.attempt_id.clone();
            view = view.child(
                Button::new(SharedString::from(attempt.clone()))
                    .label(format!(
                        "{} · {} · {}",
                        enrollment.server_name,
                        phase_label(enrollment.phase),
                        attempt
                    ))
                    .disabled(disabled)
                    .on_click(cx.listener(move |this, _, _, cx| this.load(attempt.clone(), cx))),
            );
        }
        if let Some((enrollment, status)) = &self.selected {
            for (label, value) in [
                ("Attempt", enrollment.attempt_id.clone()),
                ("Follower instance", enrollment.follower_instance_id.clone()),
                (
                    "Enrollment state version",
                    enrollment.expected_state_version.to_string(),
                ),
                ("Master instance", enrollment.master_instance_id.clone()),
                ("Authority group", enrollment.group_id.clone()),
                ("Pinned server name", enrollment.server_name.clone()),
                (
                    "Pinned CA fingerprint",
                    enrollment.certificate_authority_fingerprint.clone(),
                ),
                ("Enrollment phase", phase_label(enrollment.phase).to_owned()),
                (
                    "Last exchange result",
                    exchange_label(enrollment.exchange_result).to_owned(),
                ),
            ] {
                view = view.child(div().text_sm().child(format!("{label}: {value}")));
            }
            if let Some(receipt) = &enrollment.last_observed_receipt {
                let fingerprint = receipt.credential_fingerprint.clone();
                view = view
                    .child(div().text_sm().child(format!(
                        "Master request: {} · received master version {}",
                        receipt.request_id, receipt.received_master_state_version
                    )))
                    .child(
                        div()
                            .text_sm()
                            .child(format!("Request fingerprint: {fingerprint}")),
                    )
                    .child(div().text_sm().text_color(theme::MUTED).child(
                        "Compare this full fingerprint on the master before approving the request.",
                    ))
                    .child(
                        Button::new("copy-follower-request-fingerprint")
                            .label("Copy request fingerprint")
                            .disabled(disabled)
                            .on_click(cx.listener(move |this, _, _, cx| {
                                if this.online && this.operation.is_none() {
                                    cx.write_to_clipboard(ClipboardItem::new_string(
                                        fingerprint.clone(),
                                    ));
                                }
                            })),
                    );
                if receipt.grant.as_ref().is_some_and(|grant| grant.revoked) {
                    view = view.child(
                        div()
                            .text_sm()
                            .text_color(theme::ATTENTION)
                            .child("The last receipt reports a revoked grant."),
                    );
                }
            }
            if enrollment.follower_instance_id == status.instance_id {
                if enrollment.phase == Phase::Prepared {
                    let edit_disabled = disabled || self.confirmation.is_some();
                    view = view
                        .child(div().text_sm().child("Master HTTPS origin"))
                        .child(
                            Input::new(&origin)
                                .aria_label("Enrollment master HTTPS origin")
                                .disabled(edit_disabled),
                        )
                        .child(div().text_sm().child("Connect deadline (milliseconds)"))
                        .child(
                            Input::new(&connect_ms)
                                .aria_label("Enrollment connect deadline in milliseconds")
                                .disabled(edit_disabled),
                        )
                        .child(
                            div()
                                .text_sm()
                                .child("Whole request deadline (milliseconds)"),
                        )
                        .child(
                            Input::new(&request_ms)
                                .aria_label("Enrollment request deadline in milliseconds")
                                .disabled(edit_disabled),
                        )
                        .child(
                            Button::new("review-follower-exchange")
                                .label("Review submission / approval check…")
                                .disabled(edit_disabled)
                                .on_click(cx.listener(|this, _, _, cx| this.prepare_exchange(cx))),
                        );
                }
                view = view.child(
                    Button::new("retire-follower-enrollment")
                        .label(if enrollment.phase == Phase::Retired {
                            "Retry credential cleanup…"
                        } else {
                            "Retire enrollment…"
                        })
                        .disabled(disabled || self.confirmation.is_some())
                        .on_click(cx.listener(|this, _, _, cx| this.prepare_retire(cx))),
                );
            }
        }
        if let Some(confirmation) = &self.confirmation {
            let message = match confirmation {
                Confirmation::Exchange(attempt, request) => format!(
                    "Contact {} for attempt {} with connect / whole-request deadlines {} / {} ms? The saved CA and server name remain pinned. An approved response can join this instance as a follower. No snapshot is fetched.",
                    request.origin, attempt, request.connect_timeout_ms, request.request_timeout_ms
                ),
                Confirmation::Retire(attempt, _) => format!(
                    "Permanently retire {attempt} and remove its local vault credential? Future fetches using it will fail. This does not leave an existing follower role or remove its stored snapshot."
                ),
            };
            view = view
                .child(div().text_sm().text_color(theme::ATTENTION).child(message))
                .child(
                    div()
                        .flex()
                        .flex_wrap()
                        .gap_2()
                        .child(
                            Button::new("confirm-follower-operation")
                                .label("Confirm")
                                .primary()
                                .disabled(disabled)
                                .on_click(cx.listener(|this, _, _, cx| this.execute(cx))),
                        )
                        .child(
                            Button::new("cancel-follower-operation")
                                .label("Cancel")
                                .disabled(disabled)
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.confirmation = None;
                                    cx.notify();
                                })),
                        ),
                );
        }
        view
    }
}
