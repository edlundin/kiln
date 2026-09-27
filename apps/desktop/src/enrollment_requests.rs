//! Master-side decisions bind to the complete displayed enrollment request.

use std::sync::Arc;

use gpui::{ClipboardItem, Context, Render, SharedString, Window, div, prelude::*};
use gpui_component::{
    Disableable,
    button::{Button, ButtonVariants},
};
use kiln_client::Client;
use kiln_protocol::{
    ConfigurationFollowerEnrollmentDecisionRequest as Decision,
    ConfigurationFollowerEnrollmentRequestListResponse as Page,
    ConfigurationFollowerEnrollmentRequestPhase as Phase,
    ConfigurationFollowerEnrollmentRequestResponse as Request, ConfigurationSyncRole,
    ConfigurationSyncStatusResponse, RevokeConfigurationReadGrantRequest,
};
use tokio::{runtime::Runtime, sync::mpsc};
use ulid::Ulid;

use crate::{connection, theme};

#[expect(
    clippy::large_enum_variant,
    reason = "The UI event owns its background operation result until the matching state transition consumes it."
)]
enum Update {
    Page(Result<Page, String>),
    Loaded(Result<(Request, ConfigurationSyncStatusResponse), String>),
    Decided(Result<Request, String>),
    Revoked(Result<(), String>),
}

#[derive(Clone)]
enum Confirmation {
    Decision(bool, Decision),
    Revoke(String, RevokeConfigurationReadGrantRequest),
}

pub struct EnrollmentRequests {
    client: Client,
    runtime: Arc<Runtime>,
    updates: mpsc::UnboundedSender<(Ulid, Update)>,
    operation: Option<Ulid>,
    online: bool,
    page: Vec<Request>,
    next_cursor: Option<String>,
    selected: Option<(Request, ConfigurationSyncStatusResponse)>,
    confirmation: Option<Confirmation>,
    error: Option<String>,
    notice: Option<String>,
}

impl EnrollmentRequests {
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
        self.selected = None;
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
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let result = client
                .list_configuration_follower_enrollment_requests(
                    kiln_protocol::CONFIGURATION_FOLLOWER_ENROLLMENT_REQUEST_DEFAULT_PAGE_SIZE,
                    after.as_deref(),
                )
                .await
                .map_err(|e| connection::error_message("Load follower requests", &e));
            let _ = updates.send((id, Update::Page(result)));
        });
    }

    fn load(&mut self, request_id: String, cx: &mut Context<Self>) {
        let Some(id) = self.begin(cx) else {
            return;
        };
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let result = async {
                let status = client.get_configuration_sync_status().await?;
                let request = client
                    .get_configuration_follower_enrollment_request(&request_id)
                    .await?;
                Ok::<_, kiln_client::Error>((request, status))
            }
            .await
            .map_err(|e| connection::error_message("Load enrollment confirmation", &e));
            let _ = updates.send((id, Update::Loaded(result)));
        });
    }

    fn prepare(&mut self, approve: bool, cx: &mut Context<Self>) {
        if !self.online || self.operation.is_some() {
            return;
        }
        let Some((request, status)) = &self.selected else {
            return;
        };
        let Some(decision) = decision(request, status) else {
            return;
        };
        self.confirmation = Some(Confirmation::Decision(approve, decision));
        cx.notify();
    }

    fn prepare_revocation(&mut self, cx: &mut Context<Self>) {
        if !self.online || self.operation.is_some() {
            return;
        }
        let Some((request, status)) = &self.selected else {
            return;
        };
        if request.phase != Phase::Approved || !current_master(request, status) {
            return;
        }
        let Some(grant) = request.grant.as_ref().filter(|grant| !grant.revoked) else {
            return;
        };
        self.confirmation = Some(Confirmation::Revoke(
            grant.grant_id.clone(),
            RevokeConfigurationReadGrantRequest {
                expected_instance_id: status.instance_id.clone(),
                expected_state_version: status.state_version,
            },
        ));
        cx.notify();
    }

    fn decide(&mut self, cx: &mut Context<Self>) {
        let Some(confirmation) = self.confirmation.clone() else {
            return;
        };
        let Some(id) = self.begin(cx) else {
            return;
        };
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let update = match confirmation {
                Confirmation::Revoke(grant, request) => Update::Revoked(
                    client
                        .revoke_configuration_read_grant(&grant, &request)
                        .await
                        .map_err(|error| {
                            format!(
                                "{} Reload the request to inspect its grant before retrying.",
                                connection::error_message("Revoke follower access", &error),
                            )
                        }),
                ),
                Confirmation::Decision(approve, decision) => {
                    let result = if approve {
                            client
                                .approve_configuration_follower_enrollment_request(
                                    &decision.request_id,
                                    &decision,
                                )
                                .await
                        } else {
                            client
                                .reject_configuration_follower_enrollment_request(
                                    &decision.request_id,
                                    &decision,
                                )
                                .await
                        }
                        .map_err(|e| {
                            format!(
                                "{} Reload the request to learn its current state before deciding again.",
                                connection::error_message("Decide follower enrollment", &e)
                            )
                        });
                    Update::Decided(result)
                }
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
                self.page = page.requests;
                self.next_cursor = page.next_cursor;
                if self.page.is_empty() {
                    self.notice = Some("No enrollment requests on this page.".into());
                }
            }
            Update::Loaded(Ok(selected)) => self.selected = Some(selected),
            Update::Revoked(Ok(())) => {
                self.page.clear();
                self.next_cursor = None;
                self.notice = Some("Follower read access revoked permanently. Previously fetched data remains on the follower. Refresh requests to inspect the grant.".into());
            }
            Update::Decided(Ok(request)) => {
                if let Some(row) = self
                    .page
                    .iter_mut()
                    .find(|row| row.request_id == request.request_id)
                {
                    *row = request.clone();
                }
                self.notice = Some(format!(
                    "Request {}: {}. Reload it for current grant status.",
                    request.request_id,
                    phase(&request)
                ));
            }
            Update::Page(Err(error))
            | Update::Loaded(Err(error))
            | Update::Decided(Err(error))
            | Update::Revoked(Err(error)) => self.error = Some(error),
        }
        cx.notify();
    }
}

fn phase(request: &Request) -> &'static str {
    match request.phase {
        Phase::Pending => "Pending",
        Phase::Rejected => "Rejected permanently",
        Phase::Approved if request.grant.as_ref().is_some_and(|grant| grant.revoked) => {
            "Approved · grant revoked"
        }
        Phase::Approved => "Approved",
    }
}

// Metadata from a historical authority cannot authorize a current decision.
fn current_master(request: &Request, status: &ConfigurationSyncStatusResponse) -> bool {
    status.role == ConfigurationSyncRole::Master
        && status.instance_id == request.master_instance_id
        && status.master_instance_id.as_deref() == Some(request.master_instance_id.as_str())
        && status.group_id.as_deref() == Some(request.group_id.as_str())
}

fn decision(request: &Request, status: &ConfigurationSyncStatusResponse) -> Option<Decision> {
    if request.phase != Phase::Pending || !current_master(request, status) {
        return None;
    }
    Some(Decision {
        expected_instance_id: status.instance_id.clone(),
        expected_state_version: status.state_version,
        request_id: request.request_id.clone(),
        attempt_id: request.attempt_id.clone(),
        follower_id: request.follower_id.clone(),
        follower_state_version: request.follower_state_version,
        group_id: request.group_id.clone(),
        master_instance_id: request.master_instance_id.clone(),
        server_name: request.server_name.clone(),
        master_ca_fingerprint: request.master_ca_fingerprint.clone(),
        received_master_state_version: request.received_master_state_version,
        credential_fingerprint: request.credential_fingerprint.clone(),
    })
}

impl Render for EnrollmentRequests {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let disabled = !self.online || self.operation.is_some();
        let mut view = div().flex().flex_col().gap_3().min_w_0()
            .child(div().text_lg().child("Follower enrollment requests"))
            .child(div().text_sm().text_color(theme::MUTED).child("On the master, review requests received from followers. A follower ID is a claim, not proof of identity. Before approving, compare the complete request fingerprint with the intended follower through a trusted channel."))
            .when(!self.online, |v| v.child(div().text_sm().text_color(theme::ATTENTION).child("Reconnect and reload requests before making a decision.")))
            .child(div().flex().flex_wrap().gap_2()
                .child(Button::new("load-enrollment-requests").label(if self.operation.is_some() { "Working…" } else { "Refresh requests" }).disabled(disabled)
                    .on_click(cx.listener(|this, _, _, cx| this.list(false, cx))))
                .when(self.next_cursor.is_some(), |v| v.child(Button::new("next-enrollment-requests").label("Next page").disabled(disabled)
                    .on_click(cx.listener(|this, _, _, cx| this.list(true, cx))))))
            .when_some(self.error.clone(), |v, error| v.child(div().id("enrollment-request-error").role(gpui::Role::Alert).aria_label(error.clone()).text_sm().text_color(theme::DANGER).child(error)))
            .when_some(self.notice.clone(), |v, notice| v.child(div().text_sm().child(notice)));
        for request in &self.page {
            let id = request.request_id.clone();
            view = view.child(
                Button::new(SharedString::from(id.clone()))
                    .label(format!(
                        "{} · {} · {}",
                        request.follower_id,
                        phase(request),
                        id
                    ))
                    .disabled(disabled)
                    .on_click(cx.listener(move |this, _, _, cx| this.load(id.clone(), cx))),
            );
        }
        if let Some((request, status)) = &self.selected {
            for (label, value) in [
                ("Request", request.request_id.clone()),
                ("State", phase(request).to_owned()),
                ("Follower claim", request.follower_id.clone()),
                (
                    "Follower state version",
                    request.follower_state_version.to_string(),
                ),
                ("Enrollment attempt", request.attempt_id.clone()),
                ("Authority group", request.group_id.clone()),
                ("Master instance", request.master_instance_id.clone()),
                ("Server name", request.server_name.clone()),
                (
                    "Master CA fingerprint",
                    request.master_ca_fingerprint.clone(),
                ),
                (
                    "Request fingerprint",
                    request.credential_fingerprint.clone(),
                ),
                (
                    "Master version at receipt",
                    request.received_master_state_version.to_string(),
                ),
                ("Current local instance", status.instance_id.clone()),
                ("Current local version", status.state_version.to_string()),
            ] {
                view = view.child(div().text_sm().child(format!("{label}: {value}")));
            }
            let fingerprint = request.credential_fingerprint.clone();
            view = view.child(
                Button::new("copy-enrollment-fingerprint")
                    .label("Copy request fingerprint")
                    .disabled(disabled)
                    .on_click(cx.listener(move |this, _, _, cx| {
                        if this.online && this.operation.is_none() {
                            cx.write_to_clipboard(ClipboardItem::new_string(fingerprint.clone()));
                        }
                    })),
            );
            if let Some(grant) = &request.grant {
                view = view.child(div().text_sm().child(format!(
                    "Read grant: {} · {}",
                    grant.grant_id,
                    if grant.revoked {
                        "Revoked permanently"
                    } else {
                        "Not revoked in this response"
                    }
                )));
                if request.phase == Phase::Approved
                    && !grant.revoked
                    && current_master(request, status)
                {
                    view = view.child(
                        Button::new("revoke-follower-grant")
                            .label("Revoke follower access…")
                            .disabled(disabled || self.confirmation.is_some())
                            .on_click(cx.listener(|this, _, _, cx| this.prepare_revocation(cx))),
                    );
                }
            }
            if decision(request, status).is_some() {
                view = view.child(
                    div()
                        .flex()
                        .flex_wrap()
                        .gap_2()
                        .child(
                            Button::new("approve-enrollment-request")
                                .label("Approve follower…")
                                .disabled(disabled || self.confirmation.is_some())
                                .on_click(cx.listener(|this, _, _, cx| this.prepare(true, cx))),
                        )
                        .child(
                            Button::new("reject-enrollment-request")
                                .label("Reject request…")
                                .disabled(disabled || self.confirmation.is_some())
                                .on_click(cx.listener(|this, _, _, cx| this.prepare(false, cx))),
                        ),
                );
            } else if request.phase == Phase::Pending {
                view = view.child(div().text_sm().text_color(theme::ATTENTION).child("This pending request does not belong to the current master authority. It cannot be decided here."));
            }
        }
        if let Some(confirmation) = &self.confirmation {
            let (message, label) = match confirmation {
                Confirmation::Decision(true, _) => (
                    "Approve this exact request only after comparing its fingerprint with the intended follower. Approval grants read access to this authority’s shared configuration.".to_owned(),
                    "Fingerprint verified — approve",
                ),
                Confirmation::Decision(false, _) => ("Reject this exact request permanently? It cannot later be approved.".to_owned(), "Confirm permanent rejection"),
                Confirmation::Revoke(grant, _) => (format!("Permanently revoke grant {grant}? Future snapshot reads using it will be denied. This does not delete configuration already fetched by the follower or erase its local credential."), "Confirm permanent revocation"),
            };
            view = view
                .child(div().text_sm().text_color(theme::ATTENTION).child(message))
                .child(
                    div()
                        .flex()
                        .flex_wrap()
                        .gap_2()
                        .child(
                            Button::new("confirm-enrollment-decision")
                                .label(label)
                                .primary()
                                .disabled(disabled)
                                .on_click(cx.listener(|this, _, _, cx| this.decide(cx))),
                        )
                        .child(
                            Button::new("cancel-enrollment-decision")
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
