//! Configuration authority controls, scoped to one daemon connection.

use std::sync::Arc;

use gpui::{Context, Render, Window, div, prelude::*};
use gpui_component::{Disableable, button::Button};
use kiln_client::Client;
use kiln_protocol::{
    ConfigurationRevisionResponse, ConfigurationSyncRole, ConfigurationSyncStatusResponse,
    ConfigurationSyncTransportState, DesignateConfigurationMasterRequest,
};
use tokio::{runtime::Runtime, sync::mpsc, task::JoinHandle};
use ulid::Ulid;

use crate::{connection, theme};

enum Update {
    Status(Result<ConfigurationSyncStatusResponse, String>),
    Designated(Result<kiln_protocol::ConfigurationMasterDesignationResponse, kiln_client::Error>),
}

pub struct ConfigurationSyncSettings {
    client: Client,
    runtime: Arc<Runtime>,
    updates: mpsc::UnboundedSender<(Ulid, Update)>,
    request: Option<Ulid>,
    task: Option<JoinHandle<()>>,
    online: bool,
    status: Option<ConfigurationSyncStatusResponse>,
    error: Option<String>,
    confirmation: Option<DesignateConfigurationMasterRequest>,
    // Keep the exact request/key after an ambiguous failure, including disconnect.
    pending_designation: Option<(String, DesignateConfigurationMasterRequest)>,
    notice: Option<String>,
}

impl ConfigurationSyncSettings {
    pub fn new(client: Client, runtime: Arc<Runtime>, cx: &mut Context<Self>) -> Self {
        let (updates, mut receiver) = mpsc::unbounded_channel();
        cx.spawn(async move |this, cx| {
            while let Some((request, result)) = receiver.recv().await {
                if this
                    .update(cx, |this, cx| {
                        // Aborting I/O cannot remove an already queued response.
                        if !this.online || this.request != Some(request) {
                            return;
                        }
                        this.request = None;
                        this.task = None;
                        this.apply(result, cx);
                        cx.notify();
                    })
                    .is_err()
                {
                    break;
                }
            }
        })
        .detach();
        let mut settings = Self {
            client,
            runtime,
            updates,
            request: None,
            task: None,
            online: true,
            status: None,
            error: None,
            confirmation: None,
            pending_designation: None,
            notice: None,
        };
        settings.refresh(cx);
        settings
    }

    pub fn set_online(&mut self, online: bool, cx: &mut Context<Self>) {
        if self.online == online {
            return;
        }
        self.online = online;
        self.request = None;
        self.status = None;
        self.error = None;
        self.confirmation = None;
        self.notice = None;
        if let Some(task) = self.task.take() {
            task.abort();
        }
        if online {
            self.refresh(cx);
        }
        cx.notify();
    }

    fn refresh(&mut self, cx: &mut Context<Self>) {
        if !self.online || self.request.is_some() {
            return;
        }
        let request = Ulid::generate();
        self.request = Some(request);
        // A failed refresh must not leave old metadata looking current.
        self.status = None;
        self.error = None;
        self.confirmation = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.task = Some(self.runtime.spawn(async move {
            let result = client
                .get_configuration_sync_status()
                .await
                .map_err(|error| connection::error_message("Load synchronization status", &error));
            let _ = updates.send((request, Update::Status(result)));
        }));
        cx.notify();
    }

    fn confirm_master(&mut self, cx: &mut Context<Self>) {
        if !self.online || self.request.is_some() || self.pending_designation.is_some() {
            return;
        }
        let Some(status) = &self.status else {
            return;
        };
        if status.role != ConfigurationSyncRole::Unassigned {
            return;
        }
        self.confirmation = Some(DesignateConfigurationMasterRequest {
            expected_instance_id: status.instance_id.clone(),
            expected_state_version: status.state_version,
        });
        self.notice = None;
        cx.notify();
    }

    fn designate(&mut self, cx: &mut Context<Self>) {
        if !self.online || self.request.is_some() {
            return;
        }
        if self.pending_designation.is_none() {
            let Some(command) = self.confirmation.take() else {
                return;
            };
            self.pending_designation = Some((Ulid::generate().to_string(), command));
        }
        let Some((key, command)) = self.pending_designation.clone() else {
            return;
        };
        let request = Ulid::generate();
        self.request = Some(request);
        self.status = None;
        self.error = None;
        self.notice = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.task = Some(self.runtime.spawn(async move {
            let result = client.designate_configuration_master(&key, &command).await;
            let _ = updates.send((request, Update::Designated(result)));
        }));
        cx.notify();
    }

    fn apply(&mut self, update: Update, cx: &mut Context<Self>) {
        match update {
            Update::Status(Ok(status)) => self.status = Some(status),
            Update::Status(Err(error)) => self.error = Some(error),
            Update::Designated(Ok(_receipt)) => {
                self.pending_designation = None;
                self.notice = Some("Master designation recorded.".into());
                // An idempotent receipt can predate later role changes.
                self.refresh(cx);
            }
            Update::Designated(Err(error)) => {
                let rejected = matches!(&error, kiln_client::Error::Api { problem, .. }
                    if matches!(problem.code.as_str(),
                        kiln_protocol::error_code::CONFIGURATION_SYNC_CONFLICT
                        | kiln_protocol::error_code::CONFIGURATION_SYNC_INVALID_REQUEST
                        | kiln_protocol::error_code::IDEMPOTENCY_CONFLICT));
                if rejected {
                    self.pending_designation = None;
                }
                let action = if rejected {
                    "Refresh status before making another choice."
                } else {
                    "The outcome is unconfirmed. Retry designation to recover the original result."
                };
                self.error = Some(format!(
                    "{} {action}",
                    connection::error_message("Designate master", &error)
                ));
            }
        }
    }
}

impl Drop for ConfigurationSyncSettings {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn revision_label(revision: Option<&ConfigurationRevisionResponse>) -> String {
    revision.map_or_else(
        || "None".to_owned(),
        |revision| format!("{} (schema {})", revision.revision, revision.schema_version),
    )
}

impl Render for ConfigurationSyncSettings {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let mut content = div()
            .flex()
            .flex_col()
            .gap_3()
            .w_full()
            .min_w_0()
            .pb_4()
            .border_b_1()
            .border_color(theme::BORDER)
            .child(div().text_lg().child("Configuration synchronization"))
            .child(div().text_sm().text_color(theme::MUTED).child(
                "Shared settings, global MCP servers, and skills use one designated master instance.",
            ))
            .child(
                Button::new("refresh-configuration-sync")
                    .label(if self.request.is_some() {
                        "Working…"
                    } else if self.error.is_some() {
                        "Retry status"
                    } else {
                        "Refresh status"
                    })
                    .disabled(!self.online || self.request.is_some())
                    .on_click(cx.listener(|this, _, _, cx| this.refresh(cx))),
            );
        if !self.online {
            content = content.child(
                div()
                    .text_sm()
                    .text_color(theme::ATTENTION)
                    .child("Reconnect to the daemon to load synchronization status."),
            );
        }
        if let Some(error) = &self.error {
            content = content.child(
                div()
                    .id("configuration-sync-error")
                    .role(gpui::Role::Alert)
                    .aria_label(error.clone())
                    .text_sm()
                    .text_color(theme::DANGER)
                    .child(error.clone()),
            );
        }
        if let Some(notice) = &self.notice {
            content = content.child(div().text_sm().child(notice.clone()));
        }
        if self.pending_designation.is_some() {
            content = content.child(div().text_sm().text_color(theme::ATTENTION)
                .child("A designation request is awaiting confirmation. Retrying uses the same request."))
                .child(Button::new("retry-master-designation").label("Retry designation")
                    .disabled(!self.online || self.request.is_some())
                    .on_click(cx.listener(|this, _, _, cx| this.designate(cx))));
        }
        if let Some(status) = &self.status {
            let role = match status.role {
                ConfigurationSyncRole::Unassigned => "Unassigned — no master designated",
                ConfigurationSyncRole::Master => "Master",
                ConfigurationSyncRole::Follower => "Follower",
            };
            let transport = match status.transport {
                ConfigurationSyncTransportState::Unconfigured => {
                    "Remote synchronization is not configured. Configuration is not yet distributed between instances."
                }
            };
            content = content
                .child(div().text_sm().child(format!("Role: {role}")))
                .child(
                    div()
                        .text_sm()
                        .text_color(theme::ATTENTION)
                        .child(transport),
                );
            for (label, value) in [
                ("Instance", status.instance_id.clone()),
                (
                    "Group",
                    status.group_id.clone().unwrap_or_else(|| "None".into()),
                ),
                (
                    "Master",
                    status
                        .master_instance_id
                        .clone()
                        .unwrap_or_else(|| "None".into()),
                ),
                (
                    "Stored snapshot",
                    revision_label(status.applied_revision.as_ref()),
                ),
                (
                    "Highest observed revision",
                    revision_label(status.observed_revision.as_ref()),
                ),
            ] {
                content = content.child(
                    div()
                        .flex()
                        .flex_wrap()
                        .gap_x_2()
                        .text_sm()
                        .child(div().text_color(theme::MUTED).child(format!("{label}:")))
                        .child(value),
                );
            }
            content = content.child(div().text_sm().text_color(theme::MUTED).child(
                "Last loaded from this daemon. Stored revisions do not confirm remote connectivity or activation in running sessions.",
            ));
            if status.role == ConfigurationSyncRole::Unassigned
                && self.pending_designation.is_none()
            {
                if self.confirmation.is_some() {
                    content = content.child(div().text_sm().child(format!(
                        "Designate instance {} as master for a new configuration group? This records its role; remote synchronization is not yet available.", status.instance_id)))
                        .child(div().flex().flex_wrap().gap_2()
                            .child(Button::new("confirm-master-designation").label("Confirm master")
                                .disabled(!self.online || self.request.is_some())
                                .on_click(cx.listener(|this, _, _, cx| this.designate(cx))))
                            .child(Button::new("cancel-master-designation").label("Keep unassigned")
                                .disabled(!self.online || self.request.is_some())
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.confirmation = None;
                                    cx.notify();
                                }))));
                } else {
                    content = content.child(
                        Button::new("designate-master")
                            .label("Designate as master")
                            .disabled(!self.online || self.request.is_some())
                            .on_click(cx.listener(|this, _, _, cx| this.confirm_master(cx))),
                    );
                }
            }
        }
        content
    }
}
