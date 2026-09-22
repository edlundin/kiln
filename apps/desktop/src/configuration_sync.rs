//! Configuration authority controls, scoped to one daemon connection.

use std::{path::PathBuf, sync::Arc};

use gpui::{Context, PathPromptOptions, Render, Window, div, prelude::*};
use gpui_component::{Disableable, button::Button};
use kiln_client::Client;
use kiln_protocol::{
    ConfigurationRevisionResponse, ConfigurationSyncRole, ConfigurationSyncStatusResponse,
    ConfigurationSyncTransportState, DesignateConfigurationMasterRequest,
    PublishConfigurationSnapshotRequest, SharedConfigurationBundle,
};
use tokio::{runtime::Runtime, sync::mpsc, task::JoinHandle};
use ulid::Ulid;

use crate::{
    configuration_bundle::{self, BundleSummary},
    connection, theme,
};

struct PublicationDraft {
    command: PublishConfigurationSnapshotRequest,
    summary: BundleSummary,
}

enum Update {
    Status(Result<ConfigurationSyncStatusResponse, String>),
    Designated(Result<kiln_protocol::ConfigurationMasterDesignationResponse, kiln_client::Error>),
    Imported(Result<PublicationDraft, String>),
    Published(Result<kiln_protocol::ConfigurationPublicationResponse, kiln_client::Error>),
    ExportReady(Result<(PathBuf, SharedConfigurationBundle), String>),
    Saved(Result<(), String>),
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
    draft: Option<PublicationDraft>,
    pending_publication: Option<(String, PublishConfigurationSnapshotRequest)>,
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
            draft: None,
            pending_publication: None,
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
        self.draft = None;
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
        self.draft = None;
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
        if !self.online
            || self.request.is_some()
            || self.pending_designation.is_some()
            || self.pending_publication.is_some()
        {
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
            Update::Imported(Ok(draft)) => self.draft = Some(draft),
            Update::Imported(Err(error))
            | Update::Saved(Err(error))
            | Update::ExportReady(Err(error)) => self.error = Some(error),
            Update::Saved(Ok(())) => self.notice = Some("Configuration bundle exported.".into()),
            Update::ExportReady(Ok((path, bundle))) => {
                let request = Ulid::generate();
                self.request = Some(request);
                let updates = self.updates.clone();
                // Once started, this explicitly chosen complete-file save finishes
                // even if the connection view is dropped while blocking I/O runs.
                self.task = Some(self.runtime.spawn(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        configuration_bundle::save_bundle(&path, &bundle)
                    })
                    .await
                    .unwrap_or_else(|_| Err("Could not save the configuration bundle.".into()));
                    let _ = updates.send((request, Update::Saved(result)));
                }));
            }
            Update::Published(Ok(receipt)) => {
                self.pending_publication = None;
                self.notice = Some(format!(
                    "Publication recorded at revision {}.",
                    receipt.revision.revision
                ));
                self.refresh(cx);
            }
            Update::Published(Err(error)) => {
                let rejected = matches!(&error, kiln_client::Error::Api { problem, .. }
                    if matches!(problem.code.as_str(), kiln_protocol::error_code::CONFIGURATION_SYNC_CONFLICT
                        | kiln_protocol::error_code::CONFIGURATION_SYNC_INVALID_REQUEST
                        | kiln_protocol::error_code::IDEMPOTENCY_CONFLICT
                        | kiln_protocol::error_code::INVALID_JSON | kiln_protocol::error_code::INVALID_REQUEST));
                if rejected {
                    self.pending_publication = None;
                }
                let action = if rejected {
                    "Refresh status and import the bundle again before publishing."
                } else {
                    "The outcome is unconfirmed. Retry publication to recover the original result."
                };
                self.error = Some(format!(
                    "{} {action}",
                    connection::error_message("Publish configuration", &error)
                ));
            }
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

    fn pick_import(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.online
            || self.request.is_some()
            || self.pending_publication.is_some()
            || self.pending_designation.is_some()
        {
            return;
        }
        let Some(status) = &self.status else {
            return;
        };
        if status.role != ConfigurationSyncRole::Master {
            return;
        }
        let Some(group) = status.group_id.clone() else {
            return;
        };
        let instance = status.instance_id.clone();
        let version = status.state_version;
        let request = Ulid::generate();
        self.request = Some(request);
        self.error = None;
        self.notice = None;
        self.draft = None;
        let picker = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: false,
            prompt: Some("Import configuration bundle".into()),
        });
        cx.spawn_in(window, async move |this, cx| {
            let selection = picker.await;
            let _ = this.update_in(cx, |this, _, cx| {
                if !this.online || this.request != Some(request) {
                    return;
                }
                let path = match selection {
                    Ok(Ok(Some(mut paths))) if paths.len() == 1 => paths.remove(0),
                    Ok(Ok(None)) => {
                        this.request = None;
                        cx.notify();
                        return;
                    }
                    _ => {
                        this.request = None;
                        this.error = Some("Could not select a configuration bundle.".into());
                        cx.notify();
                        return;
                    }
                };
                let updates = this.updates.clone();
                this.task = Some(this.runtime.spawn(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        let (snapshot, summary) = configuration_bundle::read_bundle(&path)?;
                        let command = PublishConfigurationSnapshotRequest {
                            expected_instance_id: instance,
                            expected_group_id: group,
                            expected_state_version: version,
                            snapshot,
                        };
                        let encoded = serde_json::to_vec(&command)
                            .map_err(|_| "Could not prepare the publication.".to_owned())?;
                        if encoded.len() > kiln_protocol::CONFIGURATION_PUBLICATION_MAX_BYTES {
                            return Err("The bundle and publication metadata exceed the 2 MiB transfer limit.".into());
                        }
                        Ok(PublicationDraft { command, summary })
                    })
                    .await
                    .unwrap_or_else(|_| Err("Could not read the configuration bundle.".into()));
                    let _ = updates.send((request, Update::Imported(result)));
                }));
            });
        })
        .detach();
        cx.notify();
    }

    fn publish(&mut self, cx: &mut Context<Self>) {
        if !self.online || self.request.is_some() || self.pending_designation.is_some() {
            return;
        }
        if self.pending_publication.is_none() {
            let Some(draft) = self.draft.take() else {
                return;
            };
            self.pending_publication = Some((Ulid::generate().to_string(), draft.command));
        }
        let Some((key, command)) = self.pending_publication.clone() else {
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
            let result = client.publish_configuration_snapshot(&key, &command).await;
            let _ = updates.send((request, Update::Published(result)));
        }));
        cx.notify();
    }

    fn pick_export(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.online || self.request.is_some() {
            return;
        }
        let Some(status) = &self.status else {
            return;
        };
        if status.applied_revision.is_none() {
            return;
        }
        let Some(group) = status.group_id.clone() else {
            return;
        };
        let instance = status.instance_id.clone();
        let version = status.state_version;
        let request = Ulid::generate();
        self.request = Some(request);
        self.error = None;
        self.notice = None;
        let directory = std::env::var_os("HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| PathBuf::from("."));
        let picker = cx.prompt_for_new_path(&directory, Some("kiln-configuration.json"));
        cx.spawn_in(window, async move |this, cx| {
            let selection = picker.await;
            let _ = this.update_in(cx, |this, _, cx| {
                if !this.online || this.request != Some(request) {
                    return;
                }
                let path = match selection {
                    Ok(Ok(Some(path))) => path,
                    Ok(Ok(None)) => {
                        this.request = None;
                        cx.notify();
                        return;
                    }
                    _ => {
                        this.request = None;
                        this.error = Some("Could not choose an export destination.".into());
                        cx.notify();
                        return;
                    }
                };
                let client = this.client.clone();
                let updates = this.updates.clone();
                this.task = Some(this.runtime.spawn(async move {
                    let result = client
                        .get_configuration_snapshot()
                        .await
                        .map_err(|error| connection::error_message("Export configuration", &error))
                        .and_then(|response| {
                            if response.instance_id != instance
                                || response.group_id != group
                                || response.state_version != version
                            {
                                Err("Configuration changed. Refresh status before exporting."
                                    .into())
                            } else {
                                Ok((path, response.snapshot))
                            }
                        });
                    let _ = updates.send((request, Update::ExportReady(result)));
                }));
            });
        })
        .detach();
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
        if self.pending_publication.is_some() {
            content = content
                .child(div().text_sm().text_color(theme::ATTENTION).child(
                    "A publication result is unconfirmed. Retrying sends the same complete bundle.",
                ))
                .child(
                    Button::new("retry-configuration-publication")
                        .label("Retry publication")
                        .disabled(!self.online || self.request.is_some())
                        .on_click(cx.listener(|this, _, _, cx| this.publish(cx))),
                );
        }
        if let Some(draft) = &self.draft {
            content = content.child(div().text_sm().child(format!(
                "Import preview: {} MCP servers, {} skills, {} files. Model defaults: {}.",
                draft.summary.mcp_servers, draft.summary.skills, draft.summary.files,
                if draft.summary.model_defaults { "included" } else { "none" })))
                .child(div().text_sm().text_color(theme::ATTENTION).child(format!(
                    "Replace all shared configuration for instance {} in group {}? Entries absent from this bundle will be removed. The daemon validates the bundle before storing it. Remote distribution remains unavailable.",
                    draft.command.expected_instance_id, draft.command.expected_group_id)))
                .child(div().flex().flex_wrap().gap_2()
                    .child(Button::new("confirm-configuration-publication").label("Publish replacement")
                        .disabled(!self.online || self.request.is_some())
                        .on_click(cx.listener(|this, _, _, cx| this.publish(cx))))
                    .child(Button::new("discard-configuration-import").label("Discard import")
                        .disabled(self.request.is_some())
                        .on_click(cx.listener(|this, _, _, cx| { this.draft = None; cx.notify(); }))));
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
            content = content.child(
                div()
                    .flex()
                    .flex_wrap()
                    .gap_2()
                    .when(status.role == ConfigurationSyncRole::Master, |view| {
                        view.child(
                            Button::new("import-configuration-bundle")
                                .label("Import bundle…")
                                .disabled(
                                    !self.online
                                        || self.request.is_some()
                                        || self.pending_publication.is_some()
                                        || self.pending_designation.is_some(),
                                )
                                .on_click(
                                    cx.listener(|this, _, window, cx| this.pick_import(window, cx)),
                                ),
                        )
                    })
                    .when(status.applied_revision.is_some(), |view| {
                        view.child(
                            Button::new("export-configuration-bundle")
                                .label("Export stored bundle…")
                                .disabled(!self.online || self.request.is_some())
                                .on_click(
                                    cx.listener(|this, _, window, cx| this.pick_export(window, cx)),
                                ),
                        )
                    }),
            );
            if status.role == ConfigurationSyncRole::Unassigned
                && self.pending_designation.is_none()
                && self.pending_publication.is_none()
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
