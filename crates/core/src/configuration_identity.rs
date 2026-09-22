//! Host-local managed identity metadata; never part of a shared snapshot.

use crate::{ConfigurationInstanceState, ConfigurationStateError, ContentHash, SecretRef};
use std::future::Future;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationMasterIdentityPhase {
    Pending,
    Active,
}

/// Setup status only: no claim about live vault availability, certificate
/// validity at the current time, or an enabled remote listener.
pub struct ConfigurationMasterIdentitySummary {
    pub identity_id: SecretRef,
    pub phase: ConfigurationMasterIdentityPhase,
    pub server_name: String,
    pub certificate_authority_fingerprint: ContentHash,
    pub not_before_unix_seconds: i64,
    pub leaf_not_after_unix_seconds: i64,
    pub ca_not_after_unix_seconds: i64,
}

pub struct ConfigurationMasterIdentityStatus {
    pub state: ConfigurationInstanceState,
    /// Only the current master's pending/active identity; retired identities and
    /// identities belonging to historical authorities are excluded.
    pub identity: Option<ConfigurationMasterIdentitySummary>,
}

pub trait ConfigurationIdentityStatusStore: Send + Sync {
    /// Read authority and public setup metadata in one transaction. No vault
    /// read, recovery mutation, key material or follower trust approval.
    fn get_configuration_identity_status(
        &self,
    ) -> impl Future<Output = Result<ConfigurationMasterIdentityStatus, ConfigurationStateError>> + Send;
}
