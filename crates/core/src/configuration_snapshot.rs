use crate::{
    ConfigurationInstanceState, ConfigurationRevision, ConfigurationStateError,
    SharedConfigurationLimits, SharedConfigurationSnapshot, SharedSkillLimits,
};
use std::future::Future;

#[derive(Clone, Copy)]
pub struct ConfigurationSnapshotReadLimits {
    pub configuration: SharedConfigurationLimits,
    pub skill: SharedSkillLimits,
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
    InvalidLimits,
    LimitExceeded,
    IntegrityViolation,
    Unavailable,
}

/// Internal authenticated-caller boundary. A validated snapshot is data, not an
/// enrollment proof or permission to activate MCP tools, skills or credentials.
/// Content and the active revision must change in one storage transaction.
pub trait ConfigurationSnapshotStore: Send + Sync {
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
