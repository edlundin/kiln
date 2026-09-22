//! Host-local managed identity metadata; never part of a shared snapshot.

use crate::{
    ConfigurationGroupId, ConfigurationInstanceState, ConfigurationStateError, ContentHash,
    InvalidKilnId, KilnInstanceId,
};
use std::future::Future;
use ulid::Ulid;

/// Stable public identifier for one managed identity. It is generated
/// independently from the vault references and cannot be converted into one.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConfigurationMasterIdentityId(String);

impl ConfigurationMasterIdentityId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("cmi_{:032x}", value.0))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        let value = value.into();
        let Some(suffix) = value.strip_prefix("cmi_") else {
            return Err(InvalidKilnId);
        };
        if suffix.len() != 32
            || !suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(InvalidKilnId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationMasterIdentityPhase {
    Pending,
    Active,
}

/// Setup status only: no claim about live vault availability, certificate
/// validity at the current time, or an enabled remote listener.
pub struct ConfigurationMasterIdentitySummary {
    pub identity_id: ConfigurationMasterIdentityId,
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
    pub identity_id: ConfigurationMasterIdentityId,
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

    /// Retire a known identity by its stable public ID. This remains usable
    /// after the identity's master authority has been left or replaced.
    fn retire_master_identity_by_id(
        &self,
        expected_instance_id: KilnInstanceId,
        identity_id: ConfigurationMasterIdentityId,
    ) -> impl Future<Output = Result<(), ConfigurationIdentityCommandError>> + Send;
}
