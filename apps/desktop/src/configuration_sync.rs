//! Configuration authority controls, scoped to one daemon connection.

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use gpui::{Context, Entity, PathPromptOptions, Render, Window, div, prelude::*};
use gpui_component::{
    Disableable,
    button::{Button, ButtonVariants},
    input::{Input, InputState},
};
use kiln_client::Client;
use kiln_protocol::{
    ConfigurationFollowerEnrollmentExchangeResult, ConfigurationFollowerEnrollmentListResponse,
    ConfigurationFollowerEnrollmentResponse, ConfigurationIdentityStatusResponse,
    ConfigurationRevisionResponse, ConfigurationSyncRefreshOutcome,
    ConfigurationSyncRefreshRecency, ConfigurationSyncRefreshState, ConfigurationSyncRole,
    ConfigurationSyncStatusResponse, ConfigurationSyncTransportState,
    ConfigureMasterIdentityRequest, DesignateConfigurationMasterRequest,
    FetchConfigurationFollowerSnapshotRequest, FetchConfigurationFollowerSnapshotResponse,
    PublishConfigurationSnapshotRequest, RetireMasterIdentityByIdRequest,
    RetireMasterIdentityRequest, SharedConfigurationBundle,
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct FollowerStateContext {
    instance_id: String,
    state_version: u64,
    group_id: String,
    master_instance_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SnapshotFetchDraft {
    follower: FollowerStateContext,
    attempt_id: String,
    server_name: String,
    request: FetchConfigurationFollowerSnapshotRequest,
}

enum Update {
    Status(
        Result<
            (
                ConfigurationSyncStatusResponse,
                ConfigurationIdentityStatusResponse,
            ),
            String,
        >,
    ),
    EnrollmentPage {
        follower: FollowerStateContext,
        after: Option<String>,
        result: Result<ConfigurationFollowerEnrollmentListResponse, String>,
    },
    SnapshotFetched {
        draft: SnapshotFetchDraft,
        result: Result<FetchConfigurationFollowerSnapshotResponse, String>,
        status: Result<
            (
                ConfigurationSyncStatusResponse,
                ConfigurationIdentityStatusResponse,
            ),
            String,
        >,
    },
    Designated(Result<kiln_protocol::ConfigurationMasterDesignationResponse, kiln_client::Error>),
    IdentityConfigured(
        Result<kiln_protocol::ConfigurationIdentitySetupResponse, kiln_client::Error>,
    ),
    IdentityRetired(Result<(), kiln_client::Error>),
    IdentityCleanup(Result<(), kiln_client::Error>),
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
    identity_status: Option<ConfigurationIdentityStatusResponse>,
    follower_enrollments: Vec<ConfigurationFollowerEnrollmentResponse>,
    enrollment_next_cursor: Option<String>,
    enrollment_page_error: Option<String>,
    enrollment_loading: bool,
    selected_enrollment_attempt: Option<String>,
    snapshot_fetch_confirmation: Option<SnapshotFetchDraft>,
    active_snapshot_fetch: Option<SnapshotFetchDraft>,
    error: Option<String>,
    confirmation: Option<DesignateConfigurationMasterRequest>,
    // Keep the exact request/key after an ambiguous failure, including disconnect.
    pending_designation: Option<(String, DesignateConfigurationMasterRequest)>,
    identity_confirmation: Option<ConfigureMasterIdentityRequest>,
    pending_identity_setup: Option<(String, ConfigureMasterIdentityRequest)>,
    pending_identity_cleanup: Option<RetireMasterIdentityRequest>,
    identity_retirement_confirmation: Option<RetireMasterIdentityByIdRequest>,
    pending_identity_retirement: Option<RetireMasterIdentityByIdRequest>,
    server_name_input: Option<Entity<InputState>>,
    leaf_validity_days_input: Option<Entity<InputState>>,
    ca_validity_days_input: Option<Entity<InputState>>,
    snapshot_origin_input: Option<Entity<InputState>>,
    snapshot_connect_timeout_input: Option<Entity<InputState>>,
    snapshot_request_timeout_input: Option<Entity<InputState>>,
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
            identity_status: None,
            follower_enrollments: Vec::new(),
            enrollment_next_cursor: None,
            enrollment_page_error: None,
            enrollment_loading: false,
            selected_enrollment_attempt: None,
            snapshot_fetch_confirmation: None,
            active_snapshot_fetch: None,
            error: None,
            confirmation: None,
            pending_designation: None,
            identity_confirmation: None,
            pending_identity_setup: None,
            pending_identity_cleanup: None,
            identity_retirement_confirmation: None,
            pending_identity_retirement: None,
            server_name_input: None,
            leaf_validity_days_input: None,
            ca_validity_days_input: None,
            snapshot_origin_input: None,
            snapshot_connect_timeout_input: None,
            snapshot_request_timeout_input: None,
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
        self.identity_status = None;
        self.follower_enrollments.clear();
        self.enrollment_next_cursor = None;
        self.enrollment_page_error = None;
        self.enrollment_loading = false;
        self.selected_enrollment_attempt = None;
        self.snapshot_fetch_confirmation = None;
        self.active_snapshot_fetch = None;
        self.error = None;
        self.confirmation = None;
        self.identity_confirmation = None;
        self.identity_retirement_confirmation = None;
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
        self.identity_status = None;
        self.follower_enrollments.clear();
        self.enrollment_next_cursor = None;
        self.enrollment_page_error = None;
        self.enrollment_loading = false;
        self.selected_enrollment_attempt = None;
        self.snapshot_fetch_confirmation = None;
        self.active_snapshot_fetch = None;
        self.error = None;
        self.confirmation = None;
        self.identity_confirmation = None;
        self.identity_retirement_confirmation = None;
        self.draft = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.task = Some(self.runtime.spawn(async move {
            let (status, identity_status) = tokio::join!(
                client.get_configuration_sync_status(),
                client.get_configuration_identity_status(),
            );
            let result = match (status, identity_status) {
                (Ok(status), Ok(identity_status)) => Ok((status, identity_status)),
                (Err(error), _) => Err(connection::error_message(
                    "Load synchronization status",
                    &error,
                )),
                (_, Err(error)) => Err(connection::error_message(
                    "Load managed identity status",
                    &error,
                )),
            };
            let _ = updates.send((request, Update::Status(result)));
        }));
        cx.notify();
    }

    fn load_follower_enrollment_page(&mut self, after: Option<String>, cx: &mut Context<Self>) {
        if !self.online || self.request.is_some() {
            return;
        }
        let Some(status) = self.status.as_ref() else {
            return;
        };
        let Some(follower) = follower_state_context(status) else {
            self.follower_enrollments.clear();
            self.enrollment_next_cursor = None;
            self.selected_enrollment_attempt = None;
            self.enrollment_loading = false;
            return;
        };
        if after.is_some() && after != self.enrollment_next_cursor {
            return;
        }
        if after.is_none() {
            self.follower_enrollments.clear();
            self.enrollment_next_cursor = None;
            self.selected_enrollment_attempt = None;
        }
        let request = Ulid::generate();
        self.request = Some(request);
        self.enrollment_loading = true;
        self.enrollment_page_error = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.task = Some(self.runtime.spawn(async move {
            let result = client
                .list_configuration_follower_enrollments(100, after.as_deref())
                .await
                .map_err(|error| {
                    connection::error_message("Load approved follower attempts", &error)
                });
            let _ = updates.send((
                request,
                Update::EnrollmentPage {
                    follower,
                    after,
                    result,
                },
            ));
        }));
        cx.notify();
    }

    fn review_snapshot_fetch(&mut self, cx: &mut Context<Self>) {
        if !self.online || self.request.is_some() {
            return;
        }
        let Some(status) = self.status.as_ref() else {
            self.error = Some("Refresh status before fetching a snapshot.".into());
            cx.notify();
            return;
        };
        let Some(follower) = follower_state_context(status) else {
            self.error = Some("A snapshot can be fetched only by the current follower.".into());
            cx.notify();
            return;
        };
        let Some(attempt_id) = self.selected_enrollment_attempt.clone() else {
            self.error = Some("Select an approved attempt before reviewing the fetch.".into());
            cx.notify();
            return;
        };
        let Some(enrollment) = self.follower_enrollments.iter().find(|enrollment| {
            enrollment.attempt_id == attempt_id
                && enrollment_matches_follower(enrollment, status)
                && enrollment.exchange_result
                    == Some(ConfigurationFollowerEnrollmentExchangeResult::Approved)
        }) else {
            self.error =
                Some("Refresh status and select an approved attempt for this follower.".into());
            cx.notify();
            return;
        };
        let (Some(origin_input), Some(connect_timeout_input), Some(request_timeout_input)) = (
            self.snapshot_origin_input.as_ref(),
            self.snapshot_connect_timeout_input.as_ref(),
            self.snapshot_request_timeout_input.as_ref(),
        ) else {
            return;
        };
        let origin = origin_input.read(cx).value().to_string();
        let connect_timeout_ms = match positive_timeout_milliseconds(
            connect_timeout_input.read(cx).value().as_ref(),
            "Connection timeout",
        ) {
            Ok(value) => value,
            Err(error) => {
                self.error = Some(error);
                cx.notify();
                return;
            }
        };
        let request_timeout_ms = match positive_timeout_milliseconds(
            request_timeout_input.read(cx).value().as_ref(),
            "Request timeout",
        ) {
            Ok(value) => value,
            Err(error) => {
                self.error = Some(error);
                cx.notify();
                return;
            }
        };
        if origin
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            self.error =
                Some("Enter an HTTPS origin that matches the approved server name.".into());
            cx.notify();
            return;
        }
        let parsed_origin = match url::Url::parse(&origin) {
            Ok(url) => url,
            Err(_) => {
                self.error =
                    Some("Enter an HTTPS origin that matches the approved server name.".into());
                cx.notify();
                return;
            }
        };
        let origin_host = parsed_origin
            .host_str()
            .unwrap_or_default()
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or_else(|| parsed_origin.host_str().unwrap_or_default());
        if parsed_origin.scheme() != "https"
            || !origin_host.eq_ignore_ascii_case(&enrollment.server_name)
            || !parsed_origin.username().is_empty()
            || parsed_origin.password().is_some()
            || parsed_origin.path() != "/"
            || parsed_origin.query().is_some()
            || parsed_origin.fragment().is_some()
            || parsed_origin.port_or_known_default() == Some(0)
        {
            self.error = Some(
                "The origin must use HTTPS, match the approved server name, and have no credentials or path.".into(),
            );
            cx.notify();
            return;
        }
        if connect_timeout_ms > request_timeout_ms {
            self.error =
                Some("The request timeout must be at least the connection timeout.".into());
            cx.notify();
            return;
        }
        if Instant::now()
            .checked_add(Duration::from_millis(request_timeout_ms))
            .is_none()
        {
            self.error = Some("The request timeout is outside the supported range.".into());
            cx.notify();
            return;
        }
        self.snapshot_fetch_confirmation = Some(SnapshotFetchDraft {
            follower,
            attempt_id,
            server_name: enrollment.server_name.clone(),
            request: FetchConfigurationFollowerSnapshotRequest {
                expected_instance_id: status.instance_id.clone(),
                origin,
                connect_timeout_ms,
                request_timeout_ms,
            },
        });
        self.error = None;
        self.notice = None;
        cx.notify();
    }

    fn fetch_snapshot(&mut self, cx: &mut Context<Self>) {
        if !self.online || self.request.is_some() {
            return;
        }
        let Some(draft) = self.snapshot_fetch_confirmation.clone() else {
            return;
        };
        let current_selection_matches =
            self.selected_enrollment_attempt.as_deref() == Some(draft.attempt_id.as_str());
        let current_follower_matches = self
            .status
            .as_ref()
            .is_some_and(|status| follower_status_matches_state(status, &draft.follower));
        if !current_selection_matches || !current_follower_matches {
            self.snapshot_fetch_confirmation = None;
            self.error = Some(
                "The follower or attempt changed. Refresh status and review the fetch again."
                    .into(),
            );
            cx.notify();
            return;
        }
        let request = Ulid::generate();
        self.request = Some(request);
        self.snapshot_fetch_confirmation = None;
        self.active_snapshot_fetch = Some(draft.clone());
        self.error = None;
        self.notice = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.task = Some(self.runtime.spawn(async move {
            let result = client
                .fetch_configuration_follower_snapshot(&draft.attempt_id, &draft.request)
                .await
                .map_err(|error| connection::error_message("Fetch follower snapshot", &error));
            let (status, identity_status) = tokio::join!(
                client.get_configuration_sync_status(),
                client.get_configuration_identity_status(),
            );
            let status = match (status, identity_status) {
                (Ok(status), Ok(identity_status))
                    if status.instance_id == draft.follower.instance_id
                        && same_configuration_state(&status, &identity_status) =>
                {
                    Ok((status, identity_status))
                }
                (Err(error), _) => Err(connection::error_message(
                    "Reload synchronization status",
                    &error,
                )),
                (_, Err(error)) => Err(connection::error_message(
                    "Reload managed identity status",
                    &error,
                )),
                _ => Err(
                    "The connected follower changed while reloading status. Refresh before continuing."
                        .into(),
                ),
            };
            let _ = updates.send((request, Update::SnapshotFetched { draft, result, status }));
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

    fn prepare_identity_setup(&mut self, cx: &mut Context<Self>) {
        if !self.online
            || self.request.is_some()
            || self.pending_identity_setup.is_some()
            || self.pending_identity_cleanup.is_some()
            || self.pending_identity_retirement.is_some()
        {
            return;
        }
        let Some(status) = &self.identity_status else {
            return;
        };
        if status.role != ConfigurationSyncRole::Master || status.identity.is_some() {
            return;
        }
        let (Some(server_name), Some(leaf_days), Some(ca_days)) = (
            self.server_name_input.as_ref(),
            self.leaf_validity_days_input.as_ref(),
            self.ca_validity_days_input.as_ref(),
        ) else {
            return;
        };
        let server_name = server_name.read(cx).value().to_string();
        if server_name.is_empty() {
            self.error = Some("Enter the exact DNS name or IP address followers will use.".into());
            cx.notify();
            return;
        }
        let leaf_seconds = match validity_seconds_from_days(leaf_days.read(cx).value().as_ref()) {
            Some(seconds) => seconds,
            None => {
                self.error = Some("Leaf validity must be a positive whole number of days.".into());
                cx.notify();
                return;
            }
        };
        let ca_seconds = match validity_seconds_from_days(ca_days.read(cx).value().as_ref()) {
            Some(seconds) if seconds >= leaf_seconds => seconds,
            _ => {
                self.error = Some(
                    "CA validity must be a positive number of days at least as long as leaf validity."
                        .into(),
                );
                cx.notify();
                return;
            }
        };
        let now = match std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        {
            Some(now) => now,
            None => {
                self.error =
                    Some("The system clock cannot provide a valid certificate date.".into());
                cx.notify();
                return;
            }
        };
        let Some(leaf_not_after_unix_seconds) = now.checked_add(leaf_seconds) else {
            self.error = Some("Leaf validity is outside the supported date range.".into());
            cx.notify();
            return;
        };
        let Some(ca_not_after_unix_seconds) = now.checked_add(ca_seconds) else {
            self.error = Some("CA validity is outside the supported date range.".into());
            cx.notify();
            return;
        };
        let Some(group_id) = status.group_id.clone() else {
            self.error = Some("The master has no configuration group. Refresh status.".into());
            cx.notify();
            return;
        };
        self.identity_confirmation = Some(ConfigureMasterIdentityRequest {
            expected_instance_id: status.instance_id.clone(),
            expected_group_id: group_id,
            expected_state_version: status.state_version,
            server_name,
            not_before_unix_seconds: now,
            leaf_not_after_unix_seconds,
            ca_not_after_unix_seconds,
        });
        self.error = None;
        self.notice = None;
        cx.notify();
    }

    fn configure_identity(&mut self, cx: &mut Context<Self>) {
        if !self.online || self.request.is_some() {
            return;
        }
        if self.pending_identity_setup.is_none() {
            let Some(command) = self.identity_confirmation.take() else {
                return;
            };
            self.pending_identity_setup = Some((Ulid::generate().to_string(), command));
        }
        let Some((key, command)) = self.pending_identity_setup.clone() else {
            return;
        };
        let request = Ulid::generate();
        self.request = Some(request);
        self.status = None;
        self.identity_status = None;
        self.error = None;
        self.notice = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.task = Some(self.runtime.spawn(async move {
            let result = client.configure_master_identity(&key, &command).await;
            let _ = updates.send((request, Update::IdentityConfigured(result)));
        }));
        cx.notify();
    }

    fn prepare_identity_retirement(&mut self, cx: &mut Context<Self>) {
        if !self.online
            || self.request.is_some()
            || self.pending_identity_setup.is_some()
            || self.pending_identity_cleanup.is_some()
            || self.pending_identity_retirement.is_some()
        {
            return;
        }
        let Some(status) = &self.identity_status else {
            return;
        };
        let Some(identity) = &status.identity else {
            return;
        };
        self.identity_retirement_confirmation = Some(RetireMasterIdentityByIdRequest {
            expected_instance_id: status.instance_id.clone(),
            identity_id: identity.identity_id.clone(),
        });
        self.error = None;
        self.notice = None;
        cx.notify();
    }

    fn retire_identity(&mut self, cx: &mut Context<Self>) {
        if !self.online || self.request.is_some() {
            return;
        }
        if self.pending_identity_retirement.is_none() {
            let Some(request) = self.identity_retirement_confirmation.take() else {
                return;
            };
            self.pending_identity_retirement = Some(request);
        }
        let Some(command) = self.pending_identity_retirement.clone() else {
            return;
        };
        let request = Ulid::generate();
        self.request = Some(request);
        self.status = None;
        self.identity_status = None;
        self.error = None;
        self.notice = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.task = Some(self.runtime.spawn(async move {
            let result = client.retire_master_identity_by_id(&command).await;
            let _ = updates.send((request, Update::IdentityRetired(result)));
        }));
        cx.notify();
    }

    fn cleanup_incomplete_identity(&mut self, cx: &mut Context<Self>) {
        if !self.online || self.request.is_some() {
            return;
        }
        let Some(command) = self.pending_identity_cleanup.clone() else {
            return;
        };
        let request = Ulid::generate();
        self.request = Some(request);
        self.error = None;
        self.notice = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.task = Some(self.runtime.spawn(async move {
            let result = client.retire_master_identity(&command).await;
            let _ = updates.send((request, Update::IdentityCleanup(result)));
        }));
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
        self.identity_status = None;
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
            Update::EnrollmentPage {
                follower,
                after,
                result,
            } => {
                self.enrollment_loading = false;
                if !self
                    .status
                    .as_ref()
                    .is_some_and(|status| follower_status_matches_state(status, &follower))
                {
                    self.follower_enrollments.clear();
                    self.enrollment_next_cursor = None;
                    self.selected_enrollment_attempt = None;
                    self.enrollment_page_error = None;
                    return;
                }
                if self.enrollment_next_cursor != after {
                    return;
                }
                match result {
                    Ok(page) => {
                        for enrollment in page.enrollments {
                            if !self
                                .follower_enrollments
                                .iter()
                                .any(|current| current.attempt_id == enrollment.attempt_id)
                            {
                                self.follower_enrollments.push(enrollment);
                            }
                        }
                        self.enrollment_next_cursor = page.next_cursor;
                        self.enrollment_page_error = None;
                    }
                    Err(error) => self.enrollment_page_error = Some(error),
                }
            }
            Update::SnapshotFetched {
                draft,
                result,
                status,
            } => {
                self.active_snapshot_fetch = None;
                let fetch_error = match result {
                    Ok(response) => {
                        let disposition = match response.disposition {
                            kiln_protocol::ConfigurationSnapshotApplyDisposition::Applied => {
                                "applied"
                            }
                            kiln_protocol::ConfigurationSnapshotApplyDisposition::AlreadyApplied => {
                                "already current"
                            }
                        };
                        self.notice = Some(format!(
                            "Snapshot {disposition} from {} at revision {}.",
                            draft.server_name, response.revision.revision
                        ));
                        None
                    }
                    Err(error) => Some(error),
                };
                let status_error = match status {
                    Ok((status, identity_status))
                        if same_configuration_state(&status, &identity_status)
                            && follower_context_matches_status(&status, &draft.follower) =>
                    {
                        self.status = Some(status);
                        self.identity_status = Some(identity_status);
                        self.follower_enrollments.clear();
                        self.enrollment_next_cursor = None;
                        self.selected_enrollment_attempt = None;
                        self.load_follower_enrollment_page(None, cx);
                        None
                    }
                    Ok(_) => {
                        self.status = None;
                        self.identity_status = None;
                        self.follower_enrollments.clear();
                        self.enrollment_next_cursor = None;
                        Some(
                            "The connected follower changed while reloading status. Refresh before continuing."
                                .to_owned(),
                        )
                    }
                    Err(error) => {
                        self.status = None;
                        self.identity_status = None;
                        self.follower_enrollments.clear();
                        self.enrollment_next_cursor = None;
                        Some(error)
                    }
                };
                self.error = match (fetch_error, status_error) {
                    (Some(fetch), Some(status)) => Some(format!(
                        "{fetch} Current status could not be reloaded: {status}"
                    )),
                    (Some(error), None) | (None, Some(error)) => Some(error),
                    (None, None) => None,
                };
            }
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
            Update::Status(Ok((status, identity_status))) => {
                if same_configuration_state(&status, &identity_status) {
                    let is_follower = status.role == ConfigurationSyncRole::Follower;
                    self.status = Some(status);
                    self.identity_status = Some(identity_status);
                    if is_follower {
                        self.load_follower_enrollment_page(None, cx);
                    }
                } else {
                    self.status = None;
                    self.identity_status = None;
                    self.error = Some(
                        "Configuration changed while loading status. Refresh before continuing."
                            .into(),
                    );
                }
            }
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
            Update::IdentityConfigured(Ok(_receipt)) => {
                self.pending_identity_setup = None;
                self.identity_confirmation = None;
                self.notice =
                    Some("Managed identity setup completed. Reloading current status.".into());
                self.refresh(cx);
            }
            Update::IdentityConfigured(Err(error)) => {
                let recovery_required = api_problem_code(&error)
                    == Some(kiln_protocol::error_code::CONFIGURATION_IDENTITY_RECOVERY_REQUIRED);
                let rejected = recovery_required
                    || matches!(
                        api_problem_code(&error),
                        Some(
                            kiln_protocol::error_code::CONFIGURATION_SYNC_CONFLICT
                                | kiln_protocol::error_code::CONFIGURATION_SYNC_INVALID_REQUEST
                                | kiln_protocol::error_code::IDEMPOTENCY_CONFLICT
                                | kiln_protocol::error_code::INVALID_JSON
                                | kiln_protocol::error_code::INVALID_REQUEST
                        )
                    );
                if recovery_required {
                    if let Some((key, command)) = self.pending_identity_setup.take() {
                        self.pending_identity_cleanup = Some(RetireMasterIdentityRequest {
                            expected_instance_id: command.expected_instance_id,
                            setup_idempotency_key: key,
                        });
                    }
                } else if rejected {
                    self.pending_identity_setup = None;
                }
                let action = if recovery_required {
                    "Retry cleanup for the incomplete setup before creating a new identity."
                } else if rejected {
                    "Refresh identity status and review the request before trying setup again."
                } else {
                    "The outcome is unconfirmed. Retry setup with the same request to recover its result."
                };
                self.error = Some(format!(
                    "{} {action}",
                    connection::error_message("Set up managed identity", &error)
                ));
            }
            Update::IdentityRetired(Ok(())) => {
                self.pending_identity_retirement = None;
                self.identity_retirement_confirmation = None;
                self.notice = Some("Managed identity retired and vault cleanup completed.".into());
                self.refresh(cx);
            }
            Update::IdentityRetired(Err(error)) => {
                let rejected = matches!(
                    api_problem_code(&error),
                    Some(
                        kiln_protocol::error_code::CONFIGURATION_SYNC_CONFLICT
                            | kiln_protocol::error_code::CONFIGURATION_SYNC_INVALID_REQUEST
                            | kiln_protocol::error_code::INVALID_JSON
                            | kiln_protocol::error_code::INVALID_REQUEST
                    )
                );
                if rejected {
                    self.pending_identity_retirement = None;
                    self.identity_retirement_confirmation = None;
                }
                let action = if rejected {
                    "Refresh status before choosing an identity to retire."
                } else {
                    "The outcome is unconfirmed. Retry retirement for the same identity."
                };
                self.error = Some(format!(
                    "{} {action}",
                    connection::error_message("Retire managed identity", &error)
                ));
            }
            Update::IdentityCleanup(Ok(())) => {
                self.pending_identity_cleanup = None;
                self.notice = Some("Incomplete identity keys were removed.".into());
                self.refresh(cx);
            }
            Update::IdentityCleanup(Err(error)) => {
                let rejected = matches!(
                    api_problem_code(&error),
                    Some(
                        kiln_protocol::error_code::CONFIGURATION_SYNC_CONFLICT
                            | kiln_protocol::error_code::CONFIGURATION_SYNC_INVALID_REQUEST
                            | kiln_protocol::error_code::INVALID_JSON
                            | kiln_protocol::error_code::INVALID_REQUEST
                    )
                );
                if rejected {
                    self.pending_identity_cleanup = None;
                }
                let action = if rejected {
                    "Refresh identity status before continuing setup."
                } else {
                    "The outcome is unconfirmed. Retry cleanup with the same setup key."
                };
                self.error = Some(format!(
                    "{} {action}",
                    connection::error_message("Clean up incomplete identity", &error)
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
        self.identity_status = None;
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

fn validity_seconds_from_days(value: &str) -> Option<i64> {
    let days = value.parse::<i64>().ok()?;
    if days <= 0 {
        return None;
    }
    days.checked_mul(86_400)
}

fn validity_days(not_after: i64, not_before: i64) -> i64 {
    not_after.saturating_sub(not_before).div_euclid(86_400)
}

fn positive_timeout_milliseconds(value: &str, name: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{name} must be a positive whole number of milliseconds."))
}

fn follower_state_context(
    status: &ConfigurationSyncStatusResponse,
) -> Option<FollowerStateContext> {
    if status.role != ConfigurationSyncRole::Follower {
        return None;
    }
    Some(FollowerStateContext {
        instance_id: status.instance_id.clone(),
        state_version: status.state_version,
        group_id: status.group_id.clone()?,
        master_instance_id: status.master_instance_id.clone()?,
    })
}

fn follower_context_matches_status(
    status: &ConfigurationSyncStatusResponse,
    follower: &FollowerStateContext,
) -> bool {
    status.instance_id == follower.instance_id
        && status.role == ConfigurationSyncRole::Follower
        && status.group_id.as_deref() == Some(follower.group_id.as_str())
        && status.master_instance_id.as_deref() == Some(follower.master_instance_id.as_str())
}

fn follower_status_matches_state(
    status: &ConfigurationSyncStatusResponse,
    follower: &FollowerStateContext,
) -> bool {
    follower_context_matches_status(status, follower)
        && status.state_version == follower.state_version
}

fn enrollment_matches_follower(
    enrollment: &ConfigurationFollowerEnrollmentResponse,
    status: &ConfigurationSyncStatusResponse,
) -> bool {
    status.role == ConfigurationSyncRole::Follower
        && enrollment.follower_instance_id == status.instance_id
        && status.group_id.as_deref() == Some(enrollment.group_id.as_str())
        && status.master_instance_id.as_deref() == Some(enrollment.master_instance_id.as_str())
}

fn refresh_state_label(state: &ConfigurationSyncRefreshState) -> &'static str {
    match state {
        ConfigurationSyncRefreshState::Disabled => "Disabled",
        ConfigurationSyncRefreshState::Waiting => "Waiting for first check",
        ConfigurationSyncRefreshState::Checking => "Checking",
        ConfigurationSyncRefreshState::Idle => "Waiting for next scheduled check",
        ConfigurationSyncRefreshState::Failed => "Last check failed",
        ConfigurationSyncRefreshState::EnrollmentInactive => "Enrollment inactive",
        ConfigurationSyncRefreshState::InvalidConfiguration => "Invalid refresh configuration",
    }
}

fn refresh_outcome_label(outcome: Option<&ConfigurationSyncRefreshOutcome>) -> &'static str {
    match outcome {
        Some(ConfigurationSyncRefreshOutcome::Applied) => "Applied",
        Some(ConfigurationSyncRefreshOutcome::AlreadyApplied) => "Already applied",
        Some(ConfigurationSyncRefreshOutcome::Failed) => "Failed",
        Some(ConfigurationSyncRefreshOutcome::EnrollmentInactive) => "Enrollment inactive",
        Some(ConfigurationSyncRefreshOutcome::InvalidConfiguration) => "Invalid configuration",
        None => "No completed check",
    }
}

fn refresh_recency_label(recency: &ConfigurationSyncRefreshRecency) -> &'static str {
    match recency {
        ConfigurationSyncRefreshRecency::Unknown => "Unknown",
        ConfigurationSyncRefreshRecency::Fresh => "Fresh",
        ConfigurationSyncRefreshRecency::Stale => "Stale",
    }
}

fn age_label(age_ms: Option<u64>) -> String {
    let Some(age_ms) = age_ms else {
        return "Unknown".into();
    };
    let seconds = age_ms / 1_000;
    if seconds < 60 {
        format!("{seconds}s ago")
    } else if seconds < 3_600 {
        format!("{}m ago", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h ago", seconds / 3_600)
    } else {
        format!("{}d ago", seconds / 86_400)
    }
}

fn enrollment_phase_label(
    phase: kiln_protocol::ConfigurationFollowerEnrollmentPhase,
) -> &'static str {
    match phase {
        kiln_protocol::ConfigurationFollowerEnrollmentPhase::Reserved => "Reserved",
        kiln_protocol::ConfigurationFollowerEnrollmentPhase::Prepared => "Prepared",
        kiln_protocol::ConfigurationFollowerEnrollmentPhase::Retired => "Retired",
    }
}

fn same_configuration_state(
    status: &ConfigurationSyncStatusResponse,
    identity_status: &ConfigurationIdentityStatusResponse,
) -> bool {
    status.instance_id == identity_status.instance_id
        && status.state_version == identity_status.state_version
        && status.role == identity_status.role
        && status.group_id == identity_status.group_id
        && status.master_instance_id == identity_status.master_instance_id
}

fn api_problem_code(error: &kiln_client::Error) -> Option<&str> {
    match error {
        kiln_client::Error::Api { problem, .. } => Some(problem.code.as_str()),
        _ => None,
    }
}

impl Render for ConfigurationSyncSettings {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.server_name_input.is_none() {
            self.server_name_input = Some(cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder("sync.example.com")
                    .default_value("")
            }));
        }
        if self.leaf_validity_days_input.is_none() {
            self.leaf_validity_days_input = Some(cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder("90")
                    .default_value("90")
            }));
        }
        if self.ca_validity_days_input.is_none() {
            self.ca_validity_days_input = Some(cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder("365")
                    .default_value("365")
            }));
        }
        if self.snapshot_origin_input.is_none() {
            self.snapshot_origin_input = Some(cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder("https://sync.example.com")
                    .default_value("")
            }));
        }
        if self.snapshot_connect_timeout_input.is_none() {
            self.snapshot_connect_timeout_input = Some(cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder("Positive integer milliseconds")
                    .default_value("")
            }));
        }
        if self.snapshot_request_timeout_input.is_none() {
            self.snapshot_request_timeout_input = Some(cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder("Positive integer milliseconds")
                    .default_value("")
            }));
        }
        let server_name_input = self.server_name_input.clone().expect("created above");
        let leaf_validity_days_input = self
            .leaf_validity_days_input
            .clone()
            .expect("created above");
        let ca_validity_days_input = self.ca_validity_days_input.clone().expect("created above");
        let snapshot_origin_input = self.snapshot_origin_input.clone().expect("created above");
        let snapshot_connect_timeout_input = self
            .snapshot_connect_timeout_input
            .clone()
            .expect("created above");
        let snapshot_request_timeout_input = self
            .snapshot_request_timeout_input
            .clone()
            .expect("created above");
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
                    } else if self.error.is_some() && self.status.is_none() {
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
        if let Some((_, command)) = &self.pending_identity_setup {
            content = content
                .child(
                    div()
                        .text_sm()
                        .text_color(theme::ATTENTION)
                        .child(format!(
                            "Managed identity setup is unconfirmed for {}. Retry uses the same authority, name, validity and key.",
                            command.server_name
                        )),
                )
                .child(
                    Button::new("retry-managed-identity-setup")
                        .label("Retry identity setup")
                        .disabled(!self.online || self.request.is_some())
                        .on_click(cx.listener(|this, _, _, cx| this.configure_identity(cx))),
                );
        }
        if self.pending_identity_cleanup.is_some() {
            content = content
                .child(
                    div()
                        .text_sm()
                        .text_color(theme::ATTENTION)
                        .child("An incomplete identity needs cleanup. Retry removes keys reserved by that exact setup request."),
                )
                .child(
                    Button::new("retry-managed-identity-cleanup")
                        .label("Retry identity cleanup")
                        .disabled(!self.online || self.request.is_some())
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.cleanup_incomplete_identity(cx)
                        })),
                );
        }
        if let Some(command) = &self.pending_identity_retirement {
            content = content
                .child(div().text_sm().text_color(theme::ATTENTION).child(format!(
                    "Retirement is unconfirmed for identity {}. Retry targets this same identity.",
                    command.identity_id
                )))
                .child(
                    Button::new("retry-managed-identity-retirement")
                        .label("Retry identity retirement")
                        .disabled(!self.online || self.request.is_some())
                        .on_click(cx.listener(|this, _, _, cx| this.retire_identity(cx))),
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
                    "Automatic follower refresh is disabled. A follower can still fetch manually when an approved attempt record exists."
                }
                ConfigurationSyncTransportState::Configured => {
                    "Automatic follower refresh is enabled. Freshness comes from the last successful authenticated check, not stored revision equality."
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
            let refresh = &status.refresh;
            for (label, value) in [
                (
                    "Automatic refresh",
                    refresh_state_label(&refresh.state).to_owned(),
                ),
                (
                    "Last automatic outcome",
                    refresh_outcome_label(refresh.last_outcome.as_ref()).to_owned(),
                ),
                (
                    "Automatic refresh recency",
                    refresh_recency_label(&refresh.recency).to_owned(),
                ),
                (
                    "Last successful check",
                    age_label(refresh.last_success_age_ms),
                ),
                (
                    "Last successful revision",
                    revision_label(refresh.last_success_revision.as_ref()),
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
            if status.role == ConfigurationSyncRole::Follower {
                content = content
                    .child(div().text_lg().pt_2().child("Follower snapshot fetch"))
                    .child(div().text_sm().text_color(theme::MUTED).child(
                        "Manual fetch remains available when automatic refresh is disabled. Selecting an approval record reads only local metadata and does not contact the master.",
                    ));
                if let Some(active) = &self.active_snapshot_fetch {
                    content = content.child(
                        div()
                            .text_sm()
                            .text_color(theme::ATTENTION)
                            .child(format!(
                                "Fetching and applying the snapshot for attempt {} from {}. Disconnecting clears this UI task; a command already accepted by the daemon may still complete.",
                                active.attempt_id, active.server_name
                            )),
                    );
                } else if let Some(draft) = &self.snapshot_fetch_confirmation {
                    content = content
                        .child(div().text_sm().child(format!(
                            "Fetch into follower {} in group {} from master {} using attempt {} at {}?",
                            draft.follower.instance_id,
                            draft.follower.group_id,
                            draft.follower.master_instance_id,
                            draft.attempt_id,
                            draft.request.origin
                        )))
                        .child(div().text_sm().text_color(theme::MUTED).child(format!(
                            "Connection timeout: {} ms · request timeout: {} ms.",
                            draft.request.connect_timeout_ms, draft.request.request_timeout_ms
                        )))
                        .child(div().text_sm().text_color(theme::ATTENTION).child(
                            "This replaces the follower’s complete shared configuration. Settings, model defaults, global MCP servers, skills, and files absent from the master snapshot will be removed. The daemon validates the full snapshot before applying it atomically.",
                        ))
                        .child(div().text_sm().text_color(theme::MUTED).child(
                            "The approval record does not prove that its credential or master grant is still usable. The master validates this exact attempt during the fetch.",
                        ))
                        .child(
                            div()
                                .flex()
                                .flex_wrap()
                                .gap_2()
                                .child(
                                    Button::new("confirm-follower-snapshot-fetch")
                                        .label("Fetch and replace snapshot")
                                        .primary()
                                        .disabled(!self.online || self.request.is_some())
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.fetch_snapshot(cx)
                                        })),
                                )
                                .child(
                                    Button::new("cancel-follower-snapshot-fetch")
                                        .label("Cancel")
                                        .disabled(self.request.is_some())
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.snapshot_fetch_confirmation = None;
                                            cx.notify();
                                        })),
                                ),
                        );
                } else {
                    let approved_enrollments: Vec<_> = self
                        .follower_enrollments
                        .iter()
                        .filter(|enrollment| {
                            enrollment_matches_follower(enrollment, status)
                                && enrollment.exchange_result
                                    == Some(ConfigurationFollowerEnrollmentExchangeResult::Approved)
                        })
                        .cloned()
                        .collect();
                    if self.enrollment_loading {
                        content = content.child(
                            div()
                                .text_sm()
                                .text_color(theme::MUTED)
                                .child("Loading approved attempt records…"),
                        );
                    } else if approved_enrollments.is_empty()
                        && self.enrollment_page_error.is_none()
                    {
                        let message = if self.enrollment_next_cursor.is_some() {
                            "No approved attempt records on this page. Load more to continue."
                        } else {
                            "No approved attempt records for this follower were found."
                        };
                        content =
                            content.child(div().text_sm().text_color(theme::MUTED).child(message));
                    }
                    for enrollment in approved_enrollments {
                        let attempt_id = enrollment.attempt_id.clone();
                        let selected = self.selected_enrollment_attempt.as_deref()
                            == Some(attempt_id.as_str());
                        let button_label = if selected {
                            "Selected"
                        } else {
                            "Select attempt"
                        };
                        content = content.child(
                            div()
                                .flex()
                                .flex_wrap()
                                .items_center()
                                .justify_between()
                                .gap_2()
                                .border_b_1()
                                .border_color(theme::BORDER)
                                .pb_2()
                                .child(
                                    div()
                                        .flex()
                                        .flex_col()
                                        .min_w_0()
                                        .child(div().text_sm().child(format!(
                                            "Approved exchange record · {} enrollment · {}",
                                            enrollment_phase_label(enrollment.phase),
                                            enrollment.server_name
                                        )))
                                        .child(
                                            div()
                                                .text_xs()
                                                .text_color(theme::MUTED)
                                                .child(attempt_id.clone()),
                                        ),
                                )
                                .child(
                                    Button::new(format!("select-follower-attempt-{attempt_id}"))
                                        .label(button_label)
                                        .disabled(
                                            selected || !self.online || self.request.is_some(),
                                        )
                                        .on_click(cx.listener(move |this, _, _, cx| {
                                            this.selected_enrollment_attempt =
                                                Some(attempt_id.clone());
                                            this.snapshot_fetch_confirmation = None;
                                            this.error = None;
                                            this.notice = None;
                                            cx.notify();
                                        })),
                                ),
                        );
                    }
                    if let Some(error) = &self.enrollment_page_error {
                        content = content.child(
                            div()
                                .id("follower-attempt-list-error")
                                .role(gpui::Role::Alert)
                                .aria_label(error.clone())
                                .text_sm()
                                .text_color(theme::DANGER)
                                .child(error.clone()),
                        );
                        content = content.child(
                            Button::new("retry-follower-attempt-list")
                                .label("Retry attempt list")
                                .disabled(!self.online || self.request.is_some())
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.load_follower_enrollment_page(
                                        this.enrollment_next_cursor.clone(),
                                        cx,
                                    );
                                })),
                        );
                    }
                    if self.enrollment_next_cursor.is_some() {
                        content = content.child(
                            Button::new("load-more-follower-attempts")
                                .label("Load more attempts")
                                .disabled(
                                    !self.online
                                        || self.request.is_some()
                                        || self.enrollment_loading,
                                )
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.load_follower_enrollment_page(
                                        this.enrollment_next_cursor.clone(),
                                        cx,
                                    );
                                })),
                        );
                    }
                    if let Some(attempt_id) = &self.selected_enrollment_attempt {
                        if let Some(enrollment) = self.follower_enrollments.iter().find(|item| {
                            item.attempt_id == *attempt_id
                                && enrollment_matches_follower(item, status)
                                && item.exchange_result
                                    == Some(ConfigurationFollowerEnrollmentExchangeResult::Approved)
                        }) {
                            let blocked = !self.online || self.request.is_some();
                            content = content
                                .child(div().text_sm().child(format!(
                                    "Selected attempt {} is an approval record for {} ({} enrollment).",
                                    enrollment.attempt_id,
                                    enrollment.server_name,
                                    enrollment_phase_label(enrollment.phase)
                                )))
                                .child(div().text_sm().text_color(theme::MUTED).child(
                                    "The current grant is checked by the master during fetch.",
                                ))
                                .child(div().text_sm().child("HTTPS origin"))
                                .child(
                                    Input::new(&snapshot_origin_input)
                                        .aria_label("HTTPS origin for follower snapshot fetch")
                                        .disabled(blocked),
                                )
                                .child(div().text_sm().child("Connection timeout (milliseconds)"))
                                .child(
                                    Input::new(&snapshot_connect_timeout_input)
                                        .aria_label("Follower snapshot connection timeout in milliseconds")
                                        .disabled(blocked),
                                )
                                .child(div().text_sm().child("Request timeout (milliseconds)"))
                                .child(
                                    Input::new(&snapshot_request_timeout_input)
                                        .aria_label("Follower snapshot request timeout in milliseconds")
                                        .disabled(blocked),
                                )
                                .child(
                                    Button::new("review-follower-snapshot-fetch")
                                        .label("Review snapshot fetch…")
                                        .disabled(blocked)
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.review_snapshot_fetch(cx)
                                        })),
                                );
                        }
                    }
                }
            }
            if let Some(identity_status) = &self.identity_status {
                content = content
                    .child(div().text_lg().pt_2().child("Managed master identity"))
                    .child(div().text_sm().text_color(theme::MUTED).child(
                        "This identity is stored in the daemon host’s OS vault. Setup and retirement do not enable a listener or enroll followers.",
                    ));
                if identity_status.role == ConfigurationSyncRole::Master {
                    if let Some(identity) = &identity_status.identity {
                        let phase = match identity.phase {
                            kiln_protocol::ConfigurationIdentityPhase::Pending => {
                                "Pending recovery or activation"
                            }
                            kiln_protocol::ConfigurationIdentityPhase::Active => "Active",
                        };
                        content = content
                            .child(div().text_sm().child(format!(
                                "Identity {} · {phase} · {}",
                                identity.identity_id, identity.server_name
                            )))
                            .child(div().text_sm().text_color(theme::MUTED).child(format!(
                                "Certificate authority fingerprint: {}",
                                identity.certificate_authority_fingerprint
                            )))
                            .child(div().text_sm().text_color(theme::MUTED).child(format!(
                                "Leaf validity: {} days · CA validity: {} days",
                                validity_days(
                                    identity.leaf_not_after_unix_seconds,
                                    identity.not_before_unix_seconds
                                ),
                                validity_days(
                                    identity.ca_not_after_unix_seconds,
                                    identity.not_before_unix_seconds
                                ),
                            )));
                        if self.identity_retirement_confirmation.is_some() {
                            content = content
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(theme::ATTENTION)
                                        .child(format!(
                                            "Permanently retire identity {} for {}? Kiln will retain a tombstone and remove both private keys from the OS vault.",
                                            identity.identity_id, identity.server_name
                                        )),
                                )
                                .child(
                                    div()
                                        .flex()
                                        .flex_wrap()
                                        .gap_2()
                                        .child(
                                            Button::new("confirm-managed-identity-retirement")
                                                .label("Confirm retirement")
                                                .disabled(
                                                    !self.online || self.request.is_some(),
                                                )
                                                .on_click(cx.listener(|this, _, _, cx| {
                                                    this.retire_identity(cx)
                                                })),
                                        )
                                        .child(
                                            Button::new("cancel-managed-identity-retirement")
                                                .label("Keep identity")
                                                .disabled(self.request.is_some())
                                                .on_click(cx.listener(|this, _, _, cx| {
                                                    this.identity_retirement_confirmation = None;
                                                    cx.notify();
                                                })),
                                        ),
                                );
                        } else {
                            content = content.child(
                                Button::new("retire-managed-identity")
                                    .label("Retire managed identity…")
                                    .disabled(
                                        !self.online
                                            || self.request.is_some()
                                            || self.pending_identity_setup.is_some()
                                            || self.pending_identity_cleanup.is_some()
                                            || self.pending_identity_retirement.is_some(),
                                    )
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.prepare_identity_retirement(cx)
                                    })),
                            );
                        }
                    } else if self.identity_confirmation.is_some() {
                        let command = self.identity_confirmation.as_ref().expect("checked above");
                        content = content
                            .child(div().text_sm().child(format!(
                                "Create a managed CA and server certificate for {} in group {}? The leaf is valid for {} days and the CA for {} days, starting now. The daemon stores both private keys in its OS vault. This does not start a remote listener.",
                                command.server_name,
                                command.expected_group_id,
                                validity_days(command.leaf_not_after_unix_seconds,
                                    command.not_before_unix_seconds),
                                validity_days(command.ca_not_after_unix_seconds,
                                    command.not_before_unix_seconds),
                            )))
                            .child(
                                div()
                                    .flex()
                                    .flex_wrap()
                                    .gap_2()
                                    .child(
                                        Button::new("confirm-managed-identity-setup")
                                            .label("Create identity")
                                            .primary()
                                            .disabled(
                                                !self.online
                                                    || self.request.is_some()
                                                    || self.pending_identity_cleanup.is_some(),
                                            )
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.configure_identity(cx)
                                            })),
                                    )
                                    .child(
                                        Button::new("cancel-managed-identity-setup")
                                            .label("Cancel")
                                            .disabled(self.request.is_some())
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.identity_confirmation = None;
                                                cx.notify();
                                            })),
                                    ),
                            );
                    } else {
                        let blocked = !self.online
                            || self.request.is_some()
                            || self.pending_identity_setup.is_some()
                            || self.pending_identity_cleanup.is_some()
                            || self.pending_identity_retirement.is_some();
                        content = content
                            .child(div().text_sm().child("Server name"))
                            .child(Input::new(&server_name_input).aria_label("Server name"))
                            .child(div().text_xs().text_color(theme::MUTED).child(
                                "Enter the exact DNS name or IP address followers will use to reach this master.",
                            ))
                            .child(div().text_sm().child("Leaf certificate validity (days)"))
                            .child(Input::new(&leaf_validity_days_input)
                                .aria_label("Leaf certificate validity in days"))
                            .child(div().text_sm().child("Certificate authority validity (days)"))
                            .child(Input::new(&ca_validity_days_input)
                                .aria_label("Certificate authority validity in days"))
                            .child(div().text_xs().text_color(theme::MUTED).child(
                                "Defaults are 90 days for the server certificate and 365 days for the CA. The CA lifetime must be at least as long as the server certificate. Kiln does not renew identities automatically.",
                            ))
                            .child(
                                Button::new("prepare-managed-identity-setup")
                                    .label("Review identity setup…")
                                    .disabled(blocked)
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.prepare_identity_setup(cx)
                                    })),
                            );
                    }
                } else {
                    content = content.child(div().text_sm().text_color(theme::MUTED).child(
                        "A managed server identity can be configured only on the designated master instance.",
                    ));
                }
            }
        }
        content
    }
}
