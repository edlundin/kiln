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

/// Stable master-side identifier for one exact incoming follower request.
/// The follower's attempt ID remains a separate idempotency key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConfigurationFollowerEnrollmentRequestId(String);

impl ConfigurationFollowerEnrollmentRequestId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("cfr_{:032x}", value.0))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        parse_public_id(value.into(), "cfr_").map(Self)
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

/// Follower-claimed input accepted by the internal master-side journal. It
/// carries the one-way credential digest, never the bearer itself. The claimed
/// follower ID and its pinned master endpoint are request data, not proof of
/// identity or transport authentication.
#[derive(Clone, PartialEq, Eq)]
pub struct ConfigurationFollowerEnrollmentRequestSubmission {
    pub attempt_id: ConfigurationReadGrantAttemptId,
    pub follower_id: KilnInstanceId,
    pub follower_state_version: u64,
    pub authority: ConfigurationAuthority,
    pub server_name: String,
    pub master_ca_fingerprint: ContentHash,
    pub credential_digest: ConfigurationCredentialDigest,
}

/// Exact values an administrator must confirm before the master issues a
/// follower read grant. The fingerprint covers every immutable request field
/// plus the full credential digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationFollowerEnrollmentRequestConfirmation {
    pub request_id: ConfigurationFollowerEnrollmentRequestId,
    pub attempt_id: ConfigurationReadGrantAttemptId,
    pub follower_id: KilnInstanceId,
    pub follower_state_version: u64,
    pub authority: ConfigurationAuthority,
    pub server_name: String,
    pub master_ca_fingerprint: ContentHash,
    pub received_master_state_version: u64,
    pub credential_fingerprint: ContentHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationFollowerEnrollmentRequestPhase {
    Pending,
    Approved,
    Rejected,
}

/// Credential-free metadata from the master-side request journal. An approved
/// request points to the existing grant record, including its revoked state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationFollowerEnrollmentRequest {
    pub request_id: ConfigurationFollowerEnrollmentRequestId,
    pub attempt_id: ConfigurationReadGrantAttemptId,
    pub follower_id: KilnInstanceId,
    pub follower_state_version: u64,
    pub authority: ConfigurationAuthority,
    pub server_name: String,
    pub master_ca_fingerprint: ContentHash,
    pub credential_fingerprint: ContentHash,
    pub received_master_state_version: u64,
    pub phase: ConfigurationFollowerEnrollmentRequestPhase,
    pub grant: Option<ConfigurationReadGrantSummary>,
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

/// Internal storage boundary. Grant issuance is only available through an exact
/// pending-request approval transaction. No API in this trait delivers the
/// bearer credential or treats claimed remote IDs as authentication proof.
pub trait ConfigurationAccessStore: Send + Sync {
    /// Durably accept an exact follower claim while this instance is the
    /// matching master. Caller IDs remain claims until local approval; this
    /// method does not authenticate a remote instance or issue a grant.
    fn submit_configuration_follower_enrollment_request(
        &self,
        expected: &ConfigurationInstanceState,
        submission: &ConfigurationFollowerEnrollmentRequestSubmission,
        proposed_request_id: &ConfigurationFollowerEnrollmentRequestId,
    ) -> impl Future<
        Output = Result<ConfigurationFollowerEnrollmentRequest, ConfigurationAccessError>,
    > + Send;

    /// Recover one immutable request by its master-generated stable ID.
    fn get_configuration_follower_enrollment_request(
        &self,
        request_id: &ConfigurationFollowerEnrollmentRequestId,
    ) -> impl Future<
        Output = Result<Option<ConfigurationFollowerEnrollmentRequest>, ConfigurationAccessError>,
    > + Send;

    /// Recover the master request corresponding to one follower attempt. This
    /// is metadata-only and does not assert ownership of the claimed ID.
    fn get_configuration_follower_enrollment_request_by_attempt(
        &self,
        attempt_id: &ConfigurationReadGrantAttemptId,
    ) -> impl Future<
        Output = Result<Option<ConfigurationFollowerEnrollmentRequest>, ConfigurationAccessError>,
    > + Send;

    /// Stable request-ID ordered page of pending and terminal request metadata.
    fn list_configuration_follower_enrollment_requests(
        &self,
        after: Option<&ConfigurationFollowerEnrollmentRequestId>,
        limit: usize,
    ) -> impl Future<
        Output = Result<Vec<ConfigurationFollowerEnrollmentRequest>, ConfigurationAccessError>,
    > + Send;

    /// Atomically confirm the complete request binding and issue its read grant
    /// under the exact current master state. Retries return the existing grant,
    /// including permanent revocation; they never reactivate it.
    fn approve_configuration_follower_enrollment_request(
        &self,
        expected: &ConfigurationInstanceState,
        confirmation: &ConfigurationFollowerEnrollmentRequestConfirmation,
        proposed_grant_id: &ConfigurationReadGrantId,
    ) -> impl Future<
        Output = Result<ConfigurationFollowerEnrollmentRequest, ConfigurationAccessError>,
    > + Send;

    /// Permanently reject one exact pending request. Repeating the same
    /// confirmation is safe; rejected requests cannot later be approved.
    fn reject_configuration_follower_enrollment_request(
        &self,
        expected: &ConfigurationInstanceState,
        confirmation: &ConfigurationFollowerEnrollmentRequestConfirmation,
    ) -> impl Future<
        Output = Result<ConfigurationFollowerEnrollmentRequest, ConfigurationAccessError>,
    > + Send;

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
