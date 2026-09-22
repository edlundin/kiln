use crate::{
    ConfigurationGroupId, ConfigurationInstanceState, ConfigurationRevision,
    ConfigurationStateError, KilnInstanceId, SharedConfigurationLimits,
    SharedConfigurationSnapshot, SharedSkillLimits,
};
use std::future::Future;

#[derive(Clone, Copy)]
pub struct ConfigurationSnapshotReadLimits {
    pub configuration: SharedConfigurationLimits,
    pub skill: SharedSkillLimits,
    /// Combined canonical, package, dependency, file-path and hash metadata bytes.
    /// Payload bytes remain bounded by configuration.max_total_skill_bytes.
    pub max_total_metadata_bytes: usize,
}

/// Durable metadata only; this does not attest to transport connectivity or
/// revalidate every payload byte. Use get_configuration_snapshot for content.
pub struct ConfigurationSyncStatus {
    pub state: ConfigurationInstanceState,
    pub applied_revision: Option<ConfigurationRevision>,
}

pub struct StoredConfigurationSnapshot {
    pub state: ConfigurationInstanceState,
    pub revision: ConfigurationRevision,
    pub snapshot: SharedConfigurationSnapshot,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationSnapshotDisposition {
    Applied,
    Duplicate,
}
pub struct ConfigurationSnapshotMutation {
    pub state: ConfigurationInstanceState,
    pub revision: ConfigurationRevision,
    pub disposition: ConfigurationSnapshotDisposition,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationSnapshotError {
    State(ConfigurationStateError),
    InvalidSnapshot,
    InvalidRequest,
    IdempotencyConflict,
    InvalidLimits,
    LimitExceeded,
    IntegrityViolation,
    Unavailable,
}

/// Public administrative publication with durable retry identity. Replays return
/// the original state/revision receipt without republishing or changing authority.
pub trait ConfigurationPublicationStore: Send + Sync {
    fn publish_configuration_snapshot_idempotent(
        &self,
        expected_instance: &KilnInstanceId,
        expected_group: &ConfigurationGroupId,
        expected_version: u64,
        idempotency_key: &str,
        snapshot: &SharedConfigurationSnapshot,
    ) -> impl Future<Output = Result<ConfigurationSnapshotMutation, ConfigurationSnapshotError>> + Send;
}

/// Internal authenticated-caller boundary. A validated snapshot is data, not an
/// enrollment proof or permission to activate MCP tools, skills or credentials.
/// Content and the active revision must change in one storage transaction.
pub trait ConfigurationSnapshotStore: Send + Sync {
    fn get_configuration_sync_status(
        &self,
    ) -> impl Future<Output = Result<ConfigurationSyncStatus, ConfigurationSnapshotError>> + Send;
    fn publish_configuration_snapshot(
        &self,
        expected: &ConfigurationInstanceState,
        snapshot: &SharedConfigurationSnapshot,
    ) -> impl Future<Output = Result<ConfigurationSnapshotMutation, ConfigurationSnapshotError>> + Send;
    fn apply_configuration_snapshot(
        &self,
        expected: &ConfigurationInstanceState,
        revision: ConfigurationRevision,
        snapshot: &SharedConfigurationSnapshot,
    ) -> impl Future<Output = Result<ConfigurationSnapshotMutation, ConfigurationSnapshotError>> + Send;
    fn get_configuration_snapshot(
        &self,
        limits: ConfigurationSnapshotReadLimits,
    ) -> impl Future<
        Output = Result<Option<StoredConfigurationSnapshot>, ConfigurationSnapshotError>,
    > + Send;
}
