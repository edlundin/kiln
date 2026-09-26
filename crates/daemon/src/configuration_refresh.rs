//! Opt-in, daemon-owned scheduling for the existing pinned follower fetch path.

use kiln_client::ConfigurationSyncClient;
use kiln_core::{
    ConfigurationFollowerEnrollmentAdministration, ConfigurationFollowerEnrollmentError,
    ConfigurationFollowerEnrollmentExchangeSettings, ConfigurationReadGrantAttemptId,
    ConfigurationSnapshotDisposition, KilnInstanceId,
};
use kiln_protocol::{
    ConfigurationRevisionResponse, ConfigurationSyncRefreshOutcome,
    ConfigurationSyncRefreshRecency, ConfigurationSyncRefreshState,
    ConfigurationSyncRefreshStatusResponse,
};
use std::{
    env,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::watch;

pub(crate) const CONFIGURATION_FOLLOWER_REFRESH_ENVIRONMENT: &[&str] = &[
    "KILN_CONFIGURATION_FOLLOWER_REFRESH_INSTANCE_ID",
    "KILN_CONFIGURATION_FOLLOWER_REFRESH_ATTEMPT_ID",
    "KILN_CONFIGURATION_FOLLOWER_REFRESH_ORIGIN",
    "KILN_CONFIGURATION_FOLLOWER_REFRESH_CONNECT_TIMEOUT_MS",
    "KILN_CONFIGURATION_FOLLOWER_REFRESH_REQUEST_TIMEOUT_MS",
    "KILN_CONFIGURATION_FOLLOWER_REFRESH_POLL_INTERVAL_MS",
    "KILN_CONFIGURATION_FOLLOWER_REFRESH_FRESHNESS_THRESHOLD_MS",
];

#[derive(Clone)]
pub(crate) struct ConfigurationFollowerRefreshConfig {
    pub(crate) instance_id: KilnInstanceId,
    pub(crate) attempt_id: ConfigurationReadGrantAttemptId,
    pub(crate) exchange: ConfigurationFollowerEnrollmentExchangeSettings,
    pub(crate) polling_interval: Duration,
    pub(crate) freshness_threshold: Duration,
}

impl ConfigurationFollowerRefreshConfig {
    pub(crate) fn from_environment() -> Result<Option<Self>, String> {
        if !CONFIGURATION_FOLLOWER_REFRESH_ENVIRONMENT
            .iter()
            .any(|name| env::var_os(name).is_some())
        {
            return Ok(None);
        }

        let instance_id = required::<String>("KILN_CONFIGURATION_FOLLOWER_REFRESH_INSTANCE_ID")?;
        let instance_id = KilnInstanceId::parse(instance_id).map_err(|_| {
            "KILN_CONFIGURATION_FOLLOWER_REFRESH_INSTANCE_ID must be a valid instance ID".to_owned()
        })?;
        let attempt_id = required::<String>("KILN_CONFIGURATION_FOLLOWER_REFRESH_ATTEMPT_ID")?;
        let attempt_id = ConfigurationReadGrantAttemptId::parse(attempt_id).map_err(|_| {
            "KILN_CONFIGURATION_FOLLOWER_REFRESH_ATTEMPT_ID must be a valid enrollment attempt ID"
                .to_owned()
        })?;
        let origin = required::<String>("KILN_CONFIGURATION_FOLLOWER_REFRESH_ORIGIN")?;
        ConfigurationSyncClient::validate_origin_syntax(&origin).map_err(|_| {
            "KILN_CONFIGURATION_FOLLOWER_REFRESH_ORIGIN must be an HTTPS origin without credentials, path, query, or fragment"
                .to_owned()
        })?;
        let connect_timeout_ms =
            positive_milliseconds("KILN_CONFIGURATION_FOLLOWER_REFRESH_CONNECT_TIMEOUT_MS")?;
        let request_timeout_ms =
            positive_milliseconds("KILN_CONFIGURATION_FOLLOWER_REFRESH_REQUEST_TIMEOUT_MS")?;
        if connect_timeout_ms > request_timeout_ms {
            return Err(
                "KILN_CONFIGURATION_FOLLOWER_REFRESH_CONNECT_TIMEOUT_MS must not exceed KILN_CONFIGURATION_FOLLOWER_REFRESH_REQUEST_TIMEOUT_MS"
                    .to_owned(),
            );
        }
        let polling_interval_ms =
            positive_milliseconds("KILN_CONFIGURATION_FOLLOWER_REFRESH_POLL_INTERVAL_MS")?;
        let freshness_threshold_ms =
            positive_milliseconds("KILN_CONFIGURATION_FOLLOWER_REFRESH_FRESHNESS_THRESHOLD_MS")?;
        let polling_interval = Duration::from_millis(polling_interval_ms);
        let freshness_threshold = Duration::from_millis(freshness_threshold_ms);
        let now = Instant::now();
        if now.checked_add(polling_interval).is_none()
            || now.checked_add(freshness_threshold).is_none()
            || now
                .checked_add(Duration::from_millis(request_timeout_ms))
                .is_none()
        {
            return Err("configuration follower refresh duration is out of range".to_owned());
        }

        Ok(Some(Self {
            instance_id,
            attempt_id,
            exchange: ConfigurationFollowerEnrollmentExchangeSettings {
                origin,
                connect_timeout_ms,
                request_timeout_ms,
            },
            polling_interval,
            freshness_threshold,
        }))
    }
}

#[derive(Clone)]
pub(crate) struct ConfigurationFollowerRefreshStatus(Arc<Mutex<RefreshStatusInner>>);

struct RefreshStatusInner {
    response: ConfigurationSyncRefreshStatusResponse,
    freshness_threshold: Option<Duration>,
    last_success: Option<Instant>,
}

impl ConfigurationFollowerRefreshStatus {
    pub(crate) fn new(config: Option<&ConfigurationFollowerRefreshConfig>) -> Self {
        let Some(config) = config else {
            return Self(Arc::new(Mutex::new(RefreshStatusInner {
                response: ConfigurationSyncRefreshStatusResponse::unconfigured(),
                freshness_threshold: None,
                last_success: None,
            })));
        };
        Self(Arc::new(Mutex::new(RefreshStatusInner {
            response: ConfigurationSyncRefreshStatusResponse {
                enabled: true,
                state: ConfigurationSyncRefreshState::Waiting,
                follower_instance_id: Some(config.instance_id.as_str().to_owned()),
                attempt_id: Some(config.attempt_id.as_str().to_owned()),
                origin: Some(config.exchange.origin.clone()),
                enrolled_group_id: None,
                enrolled_master_instance_id: None,
                connect_timeout_ms: Some(config.exchange.connect_timeout_ms),
                request_timeout_ms: Some(config.exchange.request_timeout_ms),
                polling_interval_ms: Some(config.polling_interval.as_millis() as u64),
                freshness_threshold_ms: Some(config.freshness_threshold.as_millis() as u64),
                last_outcome: None,
                last_check_at_unix_ms: None,
                last_success_at_unix_ms: None,
                last_success_age_ms: None,
                last_success_revision: None,
                recency: ConfigurationSyncRefreshRecency::Unknown,
            },
            freshness_threshold: Some(config.freshness_threshold),
            last_success: None,
        })))
    }

    pub(crate) fn response(&self) -> ConfigurationSyncRefreshStatusResponse {
        let inner = self.0.lock().expect("refresh status lock is not poisoned");
        let mut response = inner.response.clone();
        let age = inner.last_success.map(|last_success| {
            u64::try_from(last_success.elapsed().as_millis()).unwrap_or(u64::MAX)
        });
        response.last_success_age_ms = age;
        response.recency = match (age, inner.freshness_threshold) {
            (Some(age), Some(threshold)) if age <= threshold.as_millis() as u64 => {
                ConfigurationSyncRefreshRecency::Fresh
            }
            (Some(_), Some(_)) => ConfigurationSyncRefreshRecency::Stale,
            _ => ConfigurationSyncRefreshRecency::Unknown,
        };
        response
    }

    pub(crate) fn set_enrollment_authority(&self, group_id: &str, master_instance_id: &str) {
        let mut inner = self.0.lock().expect("refresh status lock is not poisoned");
        inner.response.enrolled_group_id = Some(group_id.to_owned());
        inner.response.enrolled_master_instance_id = Some(master_instance_id.to_owned());
    }

    fn checking(&self) {
        self.0
            .lock()
            .expect("refresh status lock is not poisoned")
            .response
            .state = ConfigurationSyncRefreshState::Checking;
    }

    fn succeeded(
        &self,
        disposition: ConfigurationSnapshotDisposition,
        revision: &kiln_core::ConfigurationRevision,
    ) {
        let mut inner = self.0.lock().expect("refresh status lock is not poisoned");
        let now = Instant::now();
        inner.response.state = ConfigurationSyncRefreshState::Idle;
        inner.response.last_outcome = Some(match disposition {
            ConfigurationSnapshotDisposition::Applied => ConfigurationSyncRefreshOutcome::Applied,
            ConfigurationSnapshotDisposition::Duplicate => {
                ConfigurationSyncRefreshOutcome::AlreadyApplied
            }
        });
        inner.response.last_check_at_unix_ms = unix_time_milliseconds();
        inner.response.last_success_at_unix_ms = inner.response.last_check_at_unix_ms;
        inner.response.last_success_revision = Some(ConfigurationRevisionResponse {
            revision: revision.number(),
            schema_version: revision.schema_version(),
            content_hash: revision.content_hash().as_str().to_owned(),
        });
        inner.last_success = Some(now);
    }

    fn failed(&self) {
        let mut inner = self.0.lock().expect("refresh status lock is not poisoned");
        inner.response.state = ConfigurationSyncRefreshState::Failed;
        inner.response.last_outcome = Some(ConfigurationSyncRefreshOutcome::Failed);
        inner.response.last_check_at_unix_ms = unix_time_milliseconds();
    }

    fn enrollment_inactive(&self) {
        let mut inner = self.0.lock().expect("refresh status lock is not poisoned");
        inner.response.state = ConfigurationSyncRefreshState::EnrollmentInactive;
        inner.response.last_outcome = Some(ConfigurationSyncRefreshOutcome::EnrollmentInactive);
        inner.response.last_check_at_unix_ms = unix_time_milliseconds();
    }

    fn invalid_configuration(&self) {
        let mut inner = self.0.lock().expect("refresh status lock is not poisoned");
        inner.response.state = ConfigurationSyncRefreshState::InvalidConfiguration;
        inner.response.last_outcome = Some(ConfigurationSyncRefreshOutcome::InvalidConfiguration);
        inner.response.last_check_at_unix_ms = unix_time_milliseconds();
    }
}

pub(crate) async fn run(
    manager: kiln_infrastructure::ConfigurationFollowerEnrollmentManager,
    config: ConfigurationFollowerRefreshConfig,
    status: ConfigurationFollowerRefreshStatus,
    lifecycle: kiln_server::LifecycleCoordinator,
    mut configuration_changes: watch::Receiver<u64>,
) {
    loop {
        if lifecycle.is_quiescing() {
            return;
        }
        status.checking();
        let result = manager
            .fetch_and_apply_configuration_follower_snapshot(
                config.instance_id.clone(),
                config.attempt_id.clone(),
                config.exchange.clone(),
                Box::new(kiln_server::validate_follower_snapshot_candidate),
            )
            .await;
        match result {
            Ok(mutation) => status.succeeded(mutation.disposition, &mutation.revision),
            Err(
                ConfigurationFollowerEnrollmentError::NotFound
                | ConfigurationFollowerEnrollmentError::EnrollmentInactive
                | ConfigurationFollowerEnrollmentError::Retired
                | ConfigurationFollowerEnrollmentError::RecoveryRequired,
            ) => {
                status.enrollment_inactive();
                return;
            }
            Err(ConfigurationFollowerEnrollmentError::InvalidRequest) => {
                status.invalid_configuration();
                return;
            }
            Err(_) => status.failed(),
        }

        if lifecycle.is_quiescing() {
            return;
        }
        tokio::select! {
            _ = lifecycle.wait_for_shutdown_request() => return,
            change = configuration_changes.changed() => {
                if change.is_err() {
                    return;
                }
            }
            _ = tokio::time::sleep(config.polling_interval) => {}
        }
    }
}

fn required<T: FromStr>(name: &str) -> Result<T, String> {
    let value = env::var(name).map_err(|_| format!("{name} is required and must be UTF-8"))?;
    value
        .parse()
        .map_err(|_| format!("{name} has an invalid value"))
}

fn positive_milliseconds(name: &str) -> Result<u64, String> {
    let value = required::<u64>(name)?;
    if value == 0 {
        return Err(format!("{name} must be positive"));
    }
    Ok(value)
}

fn unix_time_milliseconds() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
}
