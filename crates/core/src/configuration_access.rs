//! Internal follower credential administration. Digests are identifiers, not
//! bearer credentials or proof of a peer's transport identity.

use crate::{
    ConfigurationAuthority, ConfigurationInstanceState, ConfigurationSnapshotError,
    ConfigurationSnapshotReadLimits, ConfigurationStateError, ContentHash, KilnInstanceId,
    StoredConfigurationSnapshot,
};
use std::future::Future;

/// SHA-256 of the complete, domain-separated synchronization credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationCredentialDigest(ContentHash);
impl ConfigurationCredentialDigest {
    pub fn from_sha256(hash: ContentHash) -> Self {
        Self(hash)
    }
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// One immutable credential binding. Revocation is permanent; replacement
/// requires fresh random credential material and a new registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationReadGrant {
    pub authority: ConfigurationAuthority,
    pub follower_id: KilnInstanceId,
    pub credential_digest: ConfigurationCredentialDigest,
    pub revoked: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationAccessError {
    InvalidRequest,
    Conflict,
    CredentialConflict,
    Denied,
    State(ConfigurationStateError),
    Snapshot(ConfigurationSnapshotError),
    IntegrityViolation,
    Unavailable,
}

/// Called only by locally authenticated administration after explicit master
/// authorization. Registration is not enrollment: encrypted transport, master
/// identity pinning and private delivery of the credential remain mandatory.
pub trait ConfigurationAccessStore: Send + Sync {
    /// Register only against the exact current master state. Exact retries of
    /// the same digest, binding and original state return its current revocation
    /// status without reactivating it, even after an authority change.
    fn register_configuration_reader(
        &self,
        expected: &ConfigurationInstanceState,
        follower: &KilnInstanceId,
        digest: &ConfigurationCredentialDigest,
    ) -> impl Future<Output = Result<ConfigurationReadGrant, ConfigurationAccessError>> + Send;

    /// Permanently revoke this credential under the matching current master.
    /// Missing and mismatched bindings are denied; repeated revocation is safe.
    fn revoke_configuration_reader(
        &self,
        expected: &ConfigurationInstanceState,
        follower: &KilnInstanceId,
        digest: &ConfigurationCredentialDigest,
    ) -> impl Future<Output = Result<(), ConfigurationAccessError>> + Send;

    /// Check the credential binding/current master and read bounded, validated
    /// content in one transaction. Never grants local API or publication access.
    /// The transport must hash a presented bearer, not accept a client digest.
    /// Revocation prevents subsequent reads; already-returned bytes cannot be
    /// recalled. A success does not authenticate the master to the follower.
    fn read_configuration_for_follower(
        &self,
        authority: &ConfigurationAuthority,
        follower: &KilnInstanceId,
        digest: &ConfigurationCredentialDigest,
        limits: ConfigurationSnapshotReadLimits,
    ) -> impl Future<Output = Result<Option<StoredConfigurationSnapshot>, ConfigurationAccessError>> + Send;
}
