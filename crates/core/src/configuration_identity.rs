//! Host-local managed identity metadata; never part of a shared snapshot.

use crate::{
    ConfigurationGroupId, ConfigurationInstanceState, ConfigurationStateError, ContentHash,
    KilnInstanceId, SecretRef,
};
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

/// Caller-selected validity in UTC Unix seconds. Both certificates start at
/// not_before; the leaf must currently be valid and cannot outlive its CA.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigurationCertificateValidity {
    pub not_before: i64,
    pub leaf_not_after: i64,
    pub ca_not_after: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigureMasterIdentity {
    pub expected_instance_id: KilnInstanceId,
    pub expected_group_id: ConfigurationGroupId,
    pub expected_state_version: u64,
    pub server_name: String,
    pub validity: ConfigurationCertificateValidity,
}

/// Receipt of one setup command, not current identity/authority status. Reload
/// status after success. Exact retries never allocate replacement references.
pub struct ConfigurationIdentitySetupReceipt {
    pub instance_id: KilnInstanceId,
    pub group_id: ConfigurationGroupId,
    pub reserved_state_version: u64,
    pub identity_id: SecretRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationIdentityCommandError {
    InvalidRequest,
    Conflict,
    IdempotencyConflict,
    RecoveryRequired,
    Unavailable,
}

/// Locally authenticated and explicitly approved administration only. Setup
/// creates host-local private keys; it never enrolls followers or starts serving.
/// The composition must retain command ownership through cancellation/shutdown.
pub trait ConfigurationIdentityAdministration: Send + Sync {
    fn configure_master_identity(
        &self,
        request: ConfigureMasterIdentity,
        idempotency_key: String,
    ) -> impl Future<
        Output = Result<ConfigurationIdentitySetupReceipt, ConfigurationIdentityCommandError>,
    > + Send;

    /// Irreversibly retire the identity reserved by this setup key and retry
    /// cleanup, including when its original response was lost. Historical
    /// identities may be retired; their tombstones remain forever.
    fn retire_master_identity(
        &self,
        expected_instance_id: KilnInstanceId,
        setup_idempotency_key: String,
    ) -> impl Future<Output = Result<(), ConfigurationIdentityCommandError>> + Send;
}
