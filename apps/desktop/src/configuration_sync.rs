//! Read-only configuration authority status, scoped to one daemon connection.

use std::sync::Arc;

use gpui::{Context, Render, Window, div, prelude::*};
use gpui_component::{Disableable, button::Button};
use kiln_client::Client;
use kiln_protocol::{
    ConfigurationRevisionResponse, ConfigurationSyncRole, ConfigurationSyncStatusResponse,
    ConfigurationSyncTransportState,
};
use tokio::{runtime::Runtime, sync::mpsc, task::JoinHandle};
use ulid::Ulid;

use crate::{connection, theme};

pub struct ConfigurationSyncSettings {
    client: Client,
    runtime: Arc<Runtime>,
    updates: mpsc::UnboundedSender<(Ulid, Result<ConfigurationSyncStatusResponse, String>)>,
    request: Option<Ulid>,
    task: Option<JoinHandle<()>>,
    online: bool,
    status: Option<ConfigurationSyncStatusResponse>,
    error: Option<String>,
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
                        match result {
                            Ok(status) => this.status = Some(status),
                            Err(error) => this.error = Some(error),
                        }
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
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.task = Some(self.runtime.spawn(async move {
            let result = client
                .get_configuration_sync_status()
                .await
                .map_err(|error| connection::error_message("Load synchronization status", &error));
            let _ = updates.send((request, result));
        }));
        cx.notify();
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
                        "Loading status…"
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
        }
        content
    }
}
