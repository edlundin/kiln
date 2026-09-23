//! Host-local follower enrollment preparation and recovery metadata.

use crate::{ConfigurationAuthority, ConfigurationReadGrantAttemptId, ContentHash, KilnInstanceId};
use std::future::Future;

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationFollowerEnrollmentError {
    InvalidRequest,
    NotFound,
    Conflict,
    IdempotencyConflict,
    Retired,
    RecoveryRequired,
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
}
