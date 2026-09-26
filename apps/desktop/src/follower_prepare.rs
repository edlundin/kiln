//! Explicit public-CA trust selection before the daemon reserves a vault credential.

use std::{fs::File, io::Read, path::Path, sync::Arc};

use gpui::{Context, Entity, PathPromptOptions, Render, Window, div, prelude::*};
use gpui_component::{
    Disableable,
    button::{Button, ButtonVariants},
    input::{Input, InputState},
};
use kiln_client::Client;
use kiln_protocol::{
    ConfigurationSyncRole, PrepareConfigurationFollowerEnrollmentRequest as Prepare,
};
use sha2::{Digest, Sha256};
use tokio::{runtime::Runtime, sync::mpsc};
use ulid::Ulid;

use crate::{connection, theme};

enum Update {
    Draft(Result<(Prepare, String), String>),
    Prepared(Result<kiln_protocol::ConfigurationFollowerEnrollmentResponse, String>),
}

pub struct FollowerPreparation {
    client: Client,
    runtime: Arc<Runtime>,
    updates: mpsc::UnboundedSender<(Ulid, Update)>,
    operation: Option<Ulid>,
    online: bool,
    group: Option<Entity<InputState>>,
    master: Option<Entity<InputState>>,
    server: Option<Entity<InputState>>,
    draft: Option<Prepare>,
    // Retain exact bytes/attempt across uncertain replies and reconnects. No bearer
    // is ever returned; durable recovery and cleanup remain in the daemon journal.
    pending: Option<Prepare>,
    fingerprint: Option<String>,
    error: Option<String>,
    notice: Option<String>,
}

impl FollowerPreparation {
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
            group: None,
            master: None,
            server: None,
            draft: None,
            pending: None,
            fingerprint: None,
            error: None,
            notice: None,
        }
    }

    pub fn set_online(&mut self, online: bool, cx: &mut Context<Self>) {
        if self.online != online {
            self.online = online;
            self.operation = None;
            self.draft = None;
            self.error = None;
            self.notice = None;
        }
        cx.notify();
    }

    fn choose(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.online || self.operation.is_some() || self.pending.is_some() {
            return;
        }
        let group = self
            .group
            .as_ref()
            .map(|v| v.read(cx).value().to_string())
            .unwrap_or_default();
        let master = self
            .master
            .as_ref()
            .map(|v| v.read(cx).value().to_string())
            .unwrap_or_default();
        let server = self
            .server
            .as_ref()
            .map(|v| v.read(cx).value().to_string())
            .unwrap_or_default();
        if !valid_id(&group, "cfg_") || !valid_id(&master, "ins_") || server.is_empty() {
            self.error = Some("Enter the master’s exact group ID, instance ID, and server name from its settings.".into());
            cx.notify();
            return;
        }
        let id = Ulid::generate();
        self.operation = Some(id);
        self.draft = None;
        self.error = None;
        self.notice = None;
        let picker = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: false,
            prompt: Some("Select public master CA certificate (DER)".into()),
        });
        cx.spawn_in(window, async move |this, cx| {
            let selection = picker.await;
            let _ = this.update_in(cx, |this, _, cx| {
                if !this.online || this.operation != Some(id) { return; }
                let path = match selection {
                    Ok(Ok(Some(mut paths))) if paths.len() == 1 => paths.remove(0),
                    Ok(Ok(None)) => { this.operation = None; cx.notify(); return; }
                    _ => { this.operation = None; this.error = Some("Could not select a public CA certificate.".into()); cx.notify(); return; }
                };
                let client = this.client.clone();
                let updates = this.updates.clone();
                this.runtime.spawn(async move {
                    let result = async {
                        let certificate_authority_der = tokio::task::spawn_blocking(move || read_certificate(&path)).await
                            .map_err(|_| "Could not read the public CA certificate.".to_owned())??;
                        let status = client.get_configuration_sync_status().await
                            .map_err(|e| connection::error_message("Load follower preparation state", &e))?;
                        if status.role != ConfigurationSyncRole::Unassigned {
                            return Err("Only an unassigned instance can prepare a new follower enrollment.".into());
                        }
                        let request = Prepare { attempt_id: format!("cra_{}", Ulid::generate()), expected_instance_id: status.instance_id,
                            expected_state_version: status.state_version, group_id: group, master_instance_id: master,
                            server_name: server, certificate_authority_der };
                        let bytes = serde_json::to_vec(&request).map_err(|_| "Could not encode the enrollment request.".to_owned())?;
                        if bytes.len() > kiln_protocol::CONFIGURATION_FOLLOWER_ENROLLMENT_MAX_BYTES {
                            return Err("The certificate and request metadata exceed the 2 MiB transfer limit.".into());
                        }
                        let fingerprint = Sha256::digest(&request.certificate_authority_der)
                            .iter().map(|byte| format!("{byte:02x}")).collect();
                        Ok((request, fingerprint))
                    }.await;
                    let _ = updates.send((id, Update::Draft(result)));
                });
            });
        }).detach();
        cx.notify();
    }

    fn prepare(&mut self, cx: &mut Context<Self>) {
        if !self.online || self.operation.is_some() {
            return;
        }
        if self.pending.is_none() {
            self.pending = self.draft.take();
        }
        let Some(request) = self.pending.clone() else {
            return;
        };
        let id = Ulid::generate();
        self.operation = Some(id);
        self.error = None;
        self.notice = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let result = client.prepare_configuration_follower_enrollment(&request).await
                .map_err(|e| format!("{} Retry the exact request, or recover and retire its attempt in the enrollment list before starting over.", connection::error_message("Prepare follower enrollment", &e)));
            let _ = updates.send((id, Update::Prepared(result)));
        });
        cx.notify();
    }

    fn apply(&mut self, id: Ulid, update: Update, cx: &mut Context<Self>) {
        if !self.online || self.operation != Some(id) {
            return;
        }
        self.operation = None;
        match update {
            Update::Draft(Ok((draft, fingerprint))) => {
                self.draft = Some(draft);
                self.fingerprint = Some(fingerprint);
            }
            Update::Prepared(Ok(enrollment)) => {
                self.pending = None;
                self.notice = Some(format!(
                    "Attempt {} returned from the daemon. Refresh enrollments below to inspect its phase and continue. Preparation alone does not contact the master or join its authority.",
                    enrollment.attempt_id
                ));
            }
            Update::Draft(Err(error)) | Update::Prepared(Err(error)) => self.error = Some(error),
        }
        cx.notify();
    }
}

fn valid_id(value: &str, prefix: &str) -> bool {
    value
        .strip_prefix(prefix)
        .and_then(|v| v.parse::<Ulid>().ok())
        .is_some_and(|id| format!("{prefix}{id}") == value)
}

fn read_certificate(path: &Path) -> Result<Vec<u8>, String> {
    let metadata =
        std::fs::symlink_metadata(path).map_err(|_| "Could not inspect the public CA file.")?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err("Choose a regular DER certificate file, not a link or device.".into());
    }
    #[cfg(unix)]
    let file = File::from(
        rustix::fs::open(
            path,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(|_| "Could not open the public CA as a regular file.")?,
    );
    #[cfg(not(unix))]
    let file = File::open(path).map_err(|_| "Could not open the public CA file.")?;
    let cap = kiln_protocol::CONFIGURATION_FOLLOWER_ENROLLMENT_MAX_BYTES as u64;
    let metadata = file
        .metadata()
        .map_err(|_| "Could not inspect the open public CA file.")?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > cap {
        return Err("Choose a nonempty regular DER certificate within the 2 MiB limit.".into());
    }
    let mut bytes = Vec::new();
    file.take(cap + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "Could not read the public CA file.")?;
    if bytes.is_empty() || bytes.len() as u64 > cap {
        return Err("The public CA file is empty or exceeds the 2 MiB limit.".into());
    }
    // Certificate usability is checked by the pinned transport before exchange.
    Ok(bytes)
}

impl Render for FollowerPreparation {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let group = self
            .group
            .get_or_insert_with(|| cx.new(|cx| InputState::new(window, cx).placeholder("cfg_…")))
            .clone();
        let master = self
            .master
            .get_or_insert_with(|| cx.new(|cx| InputState::new(window, cx).placeholder("ins_…")))
            .clone();
        let server = self
            .server
            .get_or_insert_with(|| {
                cx.new(|cx| InputState::new(window, cx).placeholder("master.example.com"))
            })
            .clone();
        let disabled = !self.online || self.operation.is_some();
        let locked = disabled || self.pending.is_some() || self.draft.is_some();
        let mut view = div().flex().flex_col().gap_3().min_w_0()
            .child(div().text_sm().child("Prepare a new follower"))
            .child(div().text_sm().text_color(theme::MUTED).child("On an unassigned instance, select the master’s public CA certificate in DER format. Obtain it and the authority IDs through a trusted channel; never select a private key. Compare the displayed fingerprint with the master before confirming."))
            .child(div().text_sm().child("Master authority group ID"))
            .child(Input::new(&group).aria_label("Master authority group ID").disabled(locked))
            .child(div().text_sm().child("Master instance ID"))
            .child(Input::new(&master).aria_label("Master instance ID").disabled(locked))
            .child(div().text_sm().child("Pinned master server name"))
            .child(Input::new(&server).aria_label("Pinned master server name").disabled(locked))
            .child(Button::new("choose-enrollment-ca").label(if self.operation.is_some() { "Working…" } else { "Choose public CA certificate…" }).disabled(locked)
                .on_click(cx.listener(|this, _, window, cx| this.choose(window, cx))))
            .when_some(self.error.clone(), |v, error| v.child(div().id("follower-preparation-error").role(gpui::Role::Alert).aria_label(error.clone()).text_sm().text_color(theme::DANGER).child(error)))
            .when_some(self.notice.clone(), |v, notice| v.child(div().text_sm().child(notice)));
        if let Some(request) = self.pending.as_ref().or(self.draft.as_ref()) {
            let fingerprint = self.fingerprint.clone().unwrap_or_default();
            for (label, value) in [
                ("Local follower", request.expected_instance_id.clone()),
                (
                    "Local state version",
                    request.expected_state_version.to_string(),
                ),
                ("Attempt", request.attempt_id.clone()),
                ("Master", request.master_instance_id.clone()),
                ("Group", request.group_id.clone()),
                ("Server name", request.server_name.clone()),
                ("Public CA SHA-256", fingerprint),
            ] {
                view = view.child(div().text_sm().child(format!("{label}: {value}")));
            }
            view = view.child(div().text_sm().text_color(theme::ATTENTION).child("Confirming reserves this exact pin and creates a follower credential in this daemon’s OS vault. It does not contact the master or change the local role."))
                .child(Button::new("confirm-follower-preparation").label(if self.pending.is_some() { "Retry exact preparation" } else { "Fingerprint verified — prepare" }).primary().disabled(disabled)
                    .on_click(cx.listener(|this, _, _, cx| this.prepare(cx))));
            if self.pending.is_some() {
                view = view.child(div().text_sm().text_color(theme::MUTED).child("The exact retry request is retained only in this Settings connection. After an app restart, use the enrollment list to recover or retire an incomplete attempt."))
                    .child(Button::new("forget-follower-preparation").label("Discard local retry (keeps daemon attempt)").disabled(disabled)
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.pending = None;
                            this.error = None;
                            this.notice = Some("Local retry discarded. Refresh enrollments and retire any live attempt before preparing another one. This did not remove a credential.".into());
                            cx.notify();
                        })));
            } else {
                view = view.child(
                    Button::new("cancel-follower-preparation")
                        .label("Cancel preparation")
                        .disabled(disabled)
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.draft = None;
                            cx.notify();
                        })),
                );
            }
        }
        view
    }
}
