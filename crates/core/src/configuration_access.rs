//! Internal follower grant administration. Digests identify bearer credentials;
//! neither a digest nor an instance ID proves the identity of a remote peer.

use crate::{
    ConfigurationAuthority, ConfigurationInstanceState, ConfigurationSnapshotError,
    ConfigurationSnapshotReadLimits, ConfigurationStateError, ContentHash, InvalidKilnId,
    KilnInstanceId, StoredConfigurationSnapshot,
};
use std::future::Future;
use ulid::Ulid;

/// Stable public identifier for one follower read grant. It is independent of
/// both the bearer credential and its digest.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConfigurationReadGrantId(String);

impl ConfigurationReadGrantId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("crg_{:032x}", value.0))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        parse_public_id(value.into(), "crg_").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Durable idempotency key for one grant issuance attempt. It is metadata, not
/// a credential and not proof that a follower owns the claimed instance ID.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConfigurationReadGrantAttemptId(String);

impl ConfigurationReadGrantAttemptId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("cra_{:032x}", value.0))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        parse_public_id(value.into(), "cra_").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn parse_public_id(value: String, prefix: &str) -> Result<String, InvalidKilnId> {
    let Some(suffix) = value.strip_prefix(prefix) else {
        return Err(InvalidKilnId);
    };
    if suffix.len() != 32
        || !suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(InvalidKilnId);
    }
    Ok(value)
}

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

/// One immutable credential binding. Revocation is permanent; a new request
/// cannot create a second active grant for the same group/follower.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationReadGrant {
    pub grant_id: ConfigurationReadGrantId,
    /// Absent only for grants created before issuance attempts were journaled.
    pub issuance_attempt_id: Option<ConfigurationReadGrantAttemptId>,
    pub authority: ConfigurationAuthority,
    pub follower_id: KilnInstanceId,
    pub credential_digest: ConfigurationCredentialDigest,
    pub issued_state_version: u64,
    pub revoked: bool,
}

/// Credential-free metadata suitable for local administrative status APIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationReadGrantSummary {
    pub grant_id: ConfigurationReadGrantId,
    pub issuance_attempt_id: Option<ConfigurationReadGrantAttemptId>,
    pub authority: ConfigurationAuthority,
    pub follower_id: KilnInstanceId,
    pub issued_state_version: u64,
    pub revoked: bool,
}

impl From<&ConfigurationReadGrant> for ConfigurationReadGrantSummary {
    fn from(grant: &ConfigurationReadGrant) -> Self {
        Self {
            grant_id: grant.grant_id.clone(),
            issuance_attempt_id: grant.issuance_attempt_id.clone(),
            authority: grant.authority.clone(),
            follower_id: grant.follower_id.clone(),
            issued_state_version: grant.issued_state_version,
            revoked: grant.revoked,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationAccessError {
    InvalidRequest,
    Conflict,
    IdempotencyConflict,
    CredentialConflict,
    Denied,
    State(ConfigurationStateError),
    Snapshot(ConfigurationSnapshotError),
    IntegrityViolation,
    Unavailable,
}

/// Internal storage boundary. Issuance is not enrollment: a caller must bind the
/// digest to an exact pending request and obtain local approval before passing
/// this boundary. No API in this trait delivers the bearer credential.
pub trait ConfigurationAccessStore: Send + Sync {
    /// Issue only against the exact current master state. Exact attempt retries
    /// return the original grant, including its current revocation state. An
    /// second active grant for the same group/follower is rejected.
    fn register_configuration_reader(
        &self,
        expected: &ConfigurationInstanceState,
        follower: &KilnInstanceId,
        attempt_id: &ConfigurationReadGrantAttemptId,
        proposed_grant_id: &ConfigurationReadGrantId,
        digest: &ConfigurationCredentialDigest,
    ) -> impl Future<Output = Result<ConfigurationReadGrant, ConfigurationAccessError>> + Send;

    /// Read credential-free grant metadata by its stable public identifier.
    fn get_configuration_read_grant(
        &self,
        grant_id: &ConfigurationReadGrantId,
    ) -> impl Future<Output = Result<Option<ConfigurationReadGrant>, ConfigurationAccessError>> + Send;

    /// Recover metadata after an ambiguous issuance response, without recovering
    /// or returning the bearer credential.
    fn get_configuration_read_grant_by_attempt(
        &self,
        attempt_id: &ConfigurationReadGrantAttemptId,
    ) -> impl Future<Output = Result<Option<ConfigurationReadGrant>, ConfigurationAccessError>> + Send;

    /// Stable-ID ordered page of current and historical grants. The caller must
    /// cap `limit`; this API never returns credentials.
    fn list_configuration_read_grants(
        &self,
        after: Option<&ConfigurationReadGrantId>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<ConfigurationReadGrant>, ConfigurationAccessError>> + Send;

    /// Permanently revoke a grant by its stable public identifier under the
    /// matching current master. Missing and mismatched bindings are denied.
    fn revoke_configuration_reader(
        &self,
        expected: &ConfigurationInstanceState,
        grant_id: &ConfigurationReadGrantId,
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
