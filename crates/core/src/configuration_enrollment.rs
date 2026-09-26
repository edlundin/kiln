//! Host-local follower enrollment preparation and recovery metadata.

use crate::{
    ConfigurationAuthority, ConfigurationCredentialDigest, ConfigurationReadGrantAttemptId,
    ContentHash, KilnInstanceId, SecretValue,
};
use serde::{Deserialize, Serialize};
use std::{future::Future, pin::Pin};

/// Locally approved inputs for reserving one follower enrollment attempt.
/// The authority and follower state are rechecked atomically by the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationFollowerEnrollmentChoice {
    pub expected_instance_id: KilnInstanceId,
    pub expected_state_version: u64,
    pub authority: ConfigurationAuthority,
    pub server_name: String,
    pub certificate_authority_der: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationFollowerEnrollmentPhase {
    Reserved,
    Prepared,
    Retired,
}

/// Public recovery metadata. This intentionally contains no credential digest,
/// vault reference, CA bytes, or bearer credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationFollowerEnrollmentMetadata {
    pub attempt_id: ConfigurationReadGrantAttemptId,
    pub follower_instance_id: KilnInstanceId,
    pub expected_state_version: u64,
    pub authority: ConfigurationAuthority,
    pub server_name: String,
    pub certificate_authority_fingerprint: ContentHash,
    pub phase: ConfigurationFollowerEnrollmentPhase,
    /// Result of the most recent completed exchange. This is historical local
    /// outcome metadata, not proof that the master grant is still live.
    pub exchange_result: Option<ConfigurationFollowerEnrollmentExchangeResult>,
    /// Last receipt observed from the pinned master. The master's revocation
    /// state may change after this receipt was stored.
    pub last_observed_receipt: Option<ConfigurationFollowerEnrollmentRemoteReceipt>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationFollowerEnrollmentExchangeResult {
    Pending,
    Approved,
    Rejected,
    Revoked,
    RoleConflict,
}

/// Credential-free master receipt retained only as last-observed metadata.
/// It is not evidence that the grant remains live after this observation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConfigurationFollowerEnrollmentRemoteReceipt {
    pub request_id: String,
    pub attempt_id: String,
    pub follower_id: String,
    pub follower_state_version: u64,
    pub group_id: String,
    pub master_instance_id: String,
    pub server_name: String,
    pub master_ca_fingerprint: String,
    pub credential_fingerprint: String,
    pub received_master_state_version: u64,
    pub phase: ConfigurationFollowerEnrollmentRemotePhase,
    pub grant: Option<ConfigurationFollowerEnrollmentRemoteGrant>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConfigurationFollowerEnrollmentRemotePhase {
    Pending,
    Approved,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConfigurationFollowerEnrollmentRemoteGrant {
    pub grant_id: String,
    pub issuance_attempt_id: Option<String>,
    pub group_id: String,
    pub master_instance_id: String,
    pub follower_instance_id: String,
    pub issued_state_version: u64,
    pub revoked: bool,
}

/// Caller-supplied transport settings for one explicit exchange. They are not
/// immutable enrollment identity and are never stored with the attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationFollowerEnrollmentExchangeSettings {
    pub origin: String,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
}

/// Exact outbound claim assembled from the durable pin. It contains the
/// one-way digest but never the bearer or a vault reference.
pub struct ConfigurationFollowerEnrollmentSubmission {
    attempt_id: ConfigurationReadGrantAttemptId,
    follower_instance_id: KilnInstanceId,
    authority: ConfigurationAuthority,
    server_name: String,
    certificate_authority_der: Vec<u8>,
    certificate_authority_fingerprint: ContentHash,
    credential_digest: ConfigurationCredentialDigest,
    follower_state_version: u64,
}

impl ConfigurationFollowerEnrollmentSubmission {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        attempt_id: ConfigurationReadGrantAttemptId,
        follower_instance_id: KilnInstanceId,
        follower_state_version: u64,
        authority: ConfigurationAuthority,
        server_name: String,
        certificate_authority_der: Vec<u8>,
        certificate_authority_fingerprint: ContentHash,
        credential_digest: ConfigurationCredentialDigest,
    ) -> Self {
        Self {
            attempt_id,
            follower_instance_id,
            authority,
            server_name,
            certificate_authority_der,
            certificate_authority_fingerprint,
            credential_digest,
            follower_state_version,
        }
    }

    pub fn attempt_id(&self) -> &ConfigurationReadGrantAttemptId {
        &self.attempt_id
    }
    pub fn follower_instance_id(&self) -> &KilnInstanceId {
        &self.follower_instance_id
    }
    pub fn follower_state_version(&self) -> u64 {
        self.follower_state_version
    }
    pub fn authority(&self) -> &ConfigurationAuthority {
        &self.authority
    }
    pub fn server_name(&self) -> &str {
        &self.server_name
    }
    pub fn certificate_authority_der(&self) -> &[u8] {
        &self.certificate_authority_der
    }
    pub fn certificate_authority_fingerprint(&self) -> &ContentHash {
        &self.certificate_authority_fingerprint
    }
    pub fn credential_digest(&self) -> &ConfigurationCredentialDigest {
        &self.credential_digest
    }
}

/// Immutable inputs for one authenticated follower snapshot read. The secret
/// is supplied separately and the reference/digest remain private to storage.
pub struct ConfigurationFollowerSnapshotPeer {
    pub follower_instance_id: KilnInstanceId,
    pub authority: ConfigurationAuthority,
    pub server_name: String,
    pub certificate_authority_der: Vec<u8>,
}

/// Bounded but not yet validated bytes and metadata returned by the pinned
/// transport. This internal candidate is never a local HTTP response.
pub struct ConfigurationFollowerSnapshotCandidate {
    pub instance_id: String,
    pub group_id: String,
    pub master_instance_id: String,
    pub state_version: u64,
    pub revision_number: u64,
    pub schema_version: u32,
    pub content_hash: String,
    pub snapshot: ConfigurationFollowerSnapshotBundle,
}

pub struct ConfigurationFollowerSnapshotBundle {
    pub metadata_json: String,
    pub skills: Vec<ConfigurationFollowerSnapshotSkillPackage>,
}

pub struct ConfigurationFollowerSnapshotSkillPackage {
    pub id: String,
    pub version: String,
    pub enabled: bool,
    pub dependencies: Vec<String>,
    pub files: Vec<ConfigurationFollowerSnapshotSkillFile>,
}

pub struct ConfigurationFollowerSnapshotSkillFile {
    pub path: String,
    pub content: Vec<u8>,
    pub content_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationFollowerSnapshotTransportError {
    InvalidSettings,
    TooLarge,
    Failed,
}

/// Restricted daemon-to-infrastructure transport for a single pinned snapshot
/// fetch. Implementations must not log the credential or remote diagnostics.
pub trait ConfigurationFollowerSnapshotTransport: Send + Sync {
    fn fetch(
        &self,
        peer: ConfigurationFollowerSnapshotPeer,
        credential: SecretValue,
        settings: ConfigurationFollowerEnrollmentExchangeSettings,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        ConfigurationFollowerSnapshotCandidate,
                        ConfigurationFollowerSnapshotTransportError,
                    >,
                > + Send
                + '_,
        >,
    >;
}

/// The server supplies the existing complete bundle validator after the
/// authenticated revision has been durably observed.
pub type ConfigurationFollowerSnapshotValidator = Box<
    dyn FnOnce(
            ConfigurationFollowerSnapshotCandidate,
        )
            -> Result<crate::SharedConfigurationSnapshot, crate::ConfigurationSnapshotError>
        + Send,
>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationFollowerEnrollmentTransportError {
    InvalidSettings,
    Failed,
}

/// Narrow daemon-to-infrastructure boundary for the pinned outbound request.
/// Implementations must not log the credential, origin, or remote diagnostics.
pub trait ConfigurationFollowerEnrollmentTransport: Send + Sync {
    fn submit(
        &self,
        submission: ConfigurationFollowerEnrollmentSubmission,
        credential: SecretValue,
        settings: ConfigurationFollowerEnrollmentExchangeSettings,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        ConfigurationFollowerEnrollmentRemoteReceipt,
                        ConfigurationFollowerEnrollmentTransportError,
                    >,
                > + Send
                + '_,
        >,
    >;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationFollowerEnrollmentError {
    InvalidRequest,
    NotFound,
    /// The exact attempt or its local follower relationship is verified inactive.
    EnrollmentInactive,
    Conflict,
    IdempotencyConflict,
    Retired,
    RecoveryRequired,
    InvalidSnapshot,
    SnapshotTooLarge,
    Unavailable,
}

/// Local, authenticated administrative boundary for durable follower
/// enrollment reservations. It does not expose a request containing the
/// credential digest; outbound exchange remains an internal operation.
pub trait ConfigurationFollowerEnrollmentAdministration: Send + Sync {
    /// Reserve or exactly retry one caller-identified attempt. The store must
    /// preserve the request binding and fence the supplied unassigned state.
    fn prepare_configuration_follower_enrollment(
        &self,
        attempt_id: ConfigurationReadGrantAttemptId,
        choice: ConfigurationFollowerEnrollmentChoice,
    ) -> impl Future<
        Output = Result<
            ConfigurationFollowerEnrollmentMetadata,
            ConfigurationFollowerEnrollmentError,
        >,
    > + Send;

    /// Read bounded metadata in stable attempt-ID order. `limit` includes any
    /// caller-requested lookahead row used to form an accurate next cursor.
    fn list_configuration_follower_enrollments(
        &self,
        after: Option<&ConfigurationReadGrantAttemptId>,
        limit: usize,
    ) -> impl Future<
        Output = Result<
            Vec<ConfigurationFollowerEnrollmentMetadata>,
            ConfigurationFollowerEnrollmentError,
        >,
    > + Send;

    /// Recover metadata for an attempt without reading or returning its secret.
    fn get_configuration_follower_enrollment(
        &self,
        attempt_id: &ConfigurationReadGrantAttemptId,
    ) -> impl Future<
        Output = Result<
            Option<ConfigurationFollowerEnrollmentMetadata>,
            ConfigurationFollowerEnrollmentError,
        >,
    > + Send;

    /// Permanently retire an attempt after verifying the expected follower ID.
    /// Cleanup is retryable and the lifecycle tombstone is permanent.
    fn retire_configuration_follower_enrollment(
        &self,
        expected_instance_id: KilnInstanceId,
        attempt_id: ConfigurationReadGrantAttemptId,
    ) -> impl Future<Output = Result<(), ConfigurationFollowerEnrollmentError>> + Send;

    /// Explicitly exchange one prepared attempt with the caller-selected HTTPS
    /// origin. Exact finalized retries return their durable local outcome and
    /// never attempt to reapply the role transition.
    fn exchange_configuration_follower_enrollment(
        &self,
        expected_instance_id: KilnInstanceId,
        attempt_id: ConfigurationReadGrantAttemptId,
        settings: ConfigurationFollowerEnrollmentExchangeSettings,
    ) -> impl Future<
        Output = Result<
            ConfigurationFollowerEnrollmentMetadata,
            ConfigurationFollowerEnrollmentError,
        >,
    > + Send;

    /// Fetch one candidate over the exact enrollment pin, durably observe its
    /// revision against the state captured before transport, validate its
    /// bundle, then atomically apply it under the same active-enrollment fence.
    fn fetch_and_apply_configuration_follower_snapshot(
        &self,
        expected_instance_id: KilnInstanceId,
        attempt_id: ConfigurationReadGrantAttemptId,
        settings: ConfigurationFollowerEnrollmentExchangeSettings,
        validate: ConfigurationFollowerSnapshotValidator,
    ) -> impl Future<
        Output = Result<crate::ConfigurationSnapshotMutation, ConfigurationFollowerEnrollmentError>,
    > + Send;
}
