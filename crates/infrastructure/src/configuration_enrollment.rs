//! Durable follower credential reservation. This prepares an exact outbound
//! request but does not change local role, contact a master, or grant access.

use super::{
    ConfigurationReadCredential, OsConfigurationSecretStore, SqliteStore,
    configuration_identity::valid_server_name, configuration_sync,
};
use kiln_core::{
    ConfigurationAuthority, ConfigurationCredentialDigest,
    ConfigurationFollowerEnrollmentAdministration, ConfigurationFollowerEnrollmentChoice,
    ConfigurationFollowerEnrollmentError as CoreError, ConfigurationFollowerEnrollmentMetadata,
    ConfigurationFollowerEnrollmentPhase, ConfigurationReadGrantAttemptId, ConfigurationRole,
    ConfigurationSecretBinding, ConfigurationSecretPurpose, ConfigurationSecretStore, ContentHash,
    KilnInstanceId, SecretRef, SecretStoreError, SecretValue,
};
use sqlx::{Connection, Row, SqliteConnection};
use std::num::NonZeroU32;

// A 100-row API page fetches one additional row to determine whether a next
// cursor is available.
const MAX_PAGE_SIZE: u32 = 101;
// A TLS Certificate entry has a uint24 length field (RFC 8446, section 4.4.2).
const MAX_CERTIFICATE_DER_BYTES: usize = 0xFF_FFFF;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigurationFollowerEnrollmentManagerError {
    InvalidRequest,
    NotFound,
    Conflict,
    IdempotencyConflict,
    Retired,
    RecoveryRequired,
    IntegrityViolation,
    Vault(SecretStoreError),
    Unavailable,
}
use ConfigurationFollowerEnrollmentManagerError as Error;

/// Exact initial request fields. It carries only the one-way credential digest,
/// never the bearer. Keep this value out of logs and general-purpose events.
#[allow(dead_code, reason = "consumed by the deferred internal exchange slice")]
pub(crate) struct ConfigurationFollowerEnrollmentSubmission {
    attempt_id: ConfigurationReadGrantAttemptId,
    follower_instance_id: KilnInstanceId,
    authority: ConfigurationAuthority,
    server_name: String,
    certificate_authority_der: Vec<u8>,
    certificate_authority_fingerprint: ContentHash,
    credential_digest: ConfigurationCredentialDigest,
}

#[allow(dead_code, reason = "consumed by the deferred internal exchange slice")]
impl ConfigurationFollowerEnrollmentSubmission {
    pub(crate) fn attempt_id(&self) -> &ConfigurationReadGrantAttemptId {
        &self.attempt_id
    }
    pub(crate) fn follower_instance_id(&self) -> &KilnInstanceId {
        &self.follower_instance_id
    }
    pub(crate) fn authority(&self) -> &ConfigurationAuthority {
        &self.authority
    }
    pub(crate) fn server_name(&self) -> &str {
        &self.server_name
    }
    pub(crate) fn certificate_authority_der(&self) -> &[u8] {
        &self.certificate_authority_der
    }
    pub(crate) fn certificate_authority_fingerprint(&self) -> &ContentHash {
        &self.certificate_authority_fingerprint
    }
    pub(crate) fn credential_digest(&self) -> &ConfigurationCredentialDigest {
        &self.credential_digest
    }
}

/// Owns the reservation-to-vault sequence for one SQLite store. Do not create
/// independent owners for the same database; the local operation lock covers
/// vault effects within this owner while SQLite transactions fence state writes.
#[derive(Clone)]
pub struct ConfigurationFollowerEnrollmentManager {
    store: SqliteStore,
    vault: OsConfigurationSecretStore,
}

impl ConfigurationFollowerEnrollmentManager {
    pub fn new(store: SqliteStore, vault: OsConfigurationSecretStore) -> Self {
        Self { store, vault }
    }

    /// Persist a fresh attempt before its first vault write. Exact retries load
    /// and verify the original vault value; they never generate or overwrite it.
    /// The caller supplies an attempt ID so it can retry after an uncertain result.
    async fn prepare(
        &self,
        attempt_id: ConfigurationReadGrantAttemptId,
        choice: ConfigurationFollowerEnrollmentChoice,
    ) -> Result<ConfigurationFollowerEnrollmentMetadata, Error> {
        let owner = self.clone();
        tokio::spawn(async move {
            let _guard = owner.store.configuration_enrollment_operations.lock().await;
            owner.prepare_owned(attempt_id, choice).await
        })
        .await
        .map_err(|_| Error::Unavailable)?
    }

    /// Recover bounded journal metadata, including permanent tombstones. The
    /// caller may resume after the last attempt ID; no secret data is returned.
    async fn list(
        &self,
        after: Option<&ConfigurationReadGrantAttemptId>,
        limit: NonZeroU32,
    ) -> Result<Vec<ConfigurationFollowerEnrollmentMetadata>, Error> {
        if limit.get() > MAX_PAGE_SIZE {
            return Err(Error::InvalidRequest);
        }
        let mut connection = self.store.connection.lock().await;
        let rows = sqlx::query(
            "SELECT r.attempt_id, r.follower_instance_id, r.expected_state_version, r.group_id, r.master_instance_id, r.server_name, r.ca_fingerprint, l.phase FROM configuration_follower_enrollment_requests r JOIN configuration_follower_enrollment_lifecycle l USING (attempt_id) WHERE (? IS NULL OR r.attempt_id > ?) ORDER BY r.attempt_id LIMIT ?",
        )
        .bind(after.map(ConfigurationReadGrantAttemptId::as_str))
        .bind(after.map(ConfigurationReadGrantAttemptId::as_str))
        .bind(i64::from(limit.get()))
        .fetch_all(&mut *connection)
        .await
        .map_err(|_| Error::Unavailable)?;
        rows.into_iter().map(decode_metadata).collect()
    }

    /// Produce the digest-only request after rechecking local state and the
    /// vaulted bearer. A stale or changed reservation is retired before return.
    #[allow(dead_code, reason = "consumed by the deferred internal exchange slice")]
    pub(crate) async fn submission(
        &self,
        attempt_id: &ConfigurationReadGrantAttemptId,
    ) -> Result<ConfigurationFollowerEnrollmentSubmission, CoreError> {
        self.submission_internal(attempt_id)
            .await
            .map_err(map_manager_error)
    }

    #[allow(dead_code, reason = "used only by the internal submission boundary")]
    async fn submission_internal(
        &self,
        attempt_id: &ConfigurationReadGrantAttemptId,
    ) -> Result<ConfigurationFollowerEnrollmentSubmission, Error> {
        let _guard = self.store.configuration_enrollment_operations.lock().await;
        let request = {
            let mut connection = self.store.connection.lock().await;
            load_request(&mut connection, attempt_id)
                .await?
                .ok_or(Error::InvalidRequest)?
        };
        let request = self.recover_request(request).await?;
        self.confirm_submission_ready(&request).await?;
        Ok(ConfigurationFollowerEnrollmentSubmission {
            attempt_id: request.attempt_id,
            follower_instance_id: request.follower_instance_id,
            authority: request.authority,
            server_name: request.server_name,
            certificate_authority_der: request.certificate_authority_der,
            certificate_authority_fingerprint: request.certificate_authority_fingerprint,
            credential_digest: request.credential_digest,
        })
    }

    /// Permanently retire the attempt before retryable vault cleanup. Exact
    /// retries repeat deletion; a tombstoned reference is never reused.
    async fn retire_checked(
        &self,
        expected_instance_id: KilnInstanceId,
        attempt_id: ConfigurationReadGrantAttemptId,
    ) -> Result<(), Error> {
        let owner = self.clone();
        tokio::spawn(async move {
            let _guard = owner.store.configuration_enrollment_operations.lock().await;
            owner
                .retire_and_cleanup_expected_owned(&attempt_id, &expected_instance_id)
                .await
        })
        .await
        .map_err(|_| Error::Unavailable)?
    }

    async fn get_metadata(
        &self,
        attempt_id: &ConfigurationReadGrantAttemptId,
    ) -> Result<Option<ConfigurationFollowerEnrollmentMetadata>, Error> {
        let mut connection = self.store.connection.lock().await;
        sqlx::query(
            "SELECT r.attempt_id, r.follower_instance_id, r.expected_state_version, r.group_id, r.master_instance_id, r.server_name, r.ca_fingerprint, l.phase FROM configuration_follower_enrollment_requests r JOIN configuration_follower_enrollment_lifecycle l USING (attempt_id) WHERE r.attempt_id = ?",
        )
        .bind(attempt_id.as_str())
        .fetch_optional(&mut *connection)
        .await
        .map_err(|_| Error::Unavailable)?
        .map(decode_metadata)
        .transpose()
    }

    async fn prepare_owned(
        &self,
        attempt_id: ConfigurationReadGrantAttemptId,
        choice: ConfigurationFollowerEnrollmentChoice,
    ) -> Result<ConfigurationFollowerEnrollmentMetadata, Error> {
        validate_choice(&choice)?;
        let existing = {
            let mut connection = self.store.connection.lock().await;
            load_request(&mut connection, &attempt_id).await?
        };
        if let Some(request) = existing {
            if !request.matches_choice(&choice) {
                return Err(Error::IdempotencyConflict);
            }
            return self
                .recover_request(request)
                .await
                .map(|request| request.metadata());
        }

        let credential = ConfigurationReadCredential::generate();
        let secret_ref = SecretRef::from_ulid(ulid::Ulid::generate());
        let request = self
            .reserve_request(attempt_id, choice, secret_ref, credential.digest())
            .await?;

        match request {
            ReserveResult::Existing(request) => self
                .recover_request(request)
                .await
                .map(|request| request.metadata()),
            ReserveResult::Created(request) => {
                let binding = request.binding()?;
                let secret = SecretValue::new(credential.expose_secret().to_vec())
                    .map_err(|_| Error::IntegrityViolation)?;
                if let Err(error) = self.vault.put_at(&binding, secret).await {
                    return Err(Error::Vault(error));
                }
                match self.read_and_verify_credential(&request).await {
                    Ok(()) => {}
                    Err(VerifySecretError::Changed) => {
                        self.retire_and_cleanup_owned(&request.attempt_id).await?;
                        return Err(Error::RecoveryRequired);
                    }
                    Err(VerifySecretError::Unavailable(error)) => {
                        return Err(Error::Vault(error));
                    }
                }
                self.mark_prepared(&request).await?;
                let mut request = request;
                request.phase = ConfigurationFollowerEnrollmentPhase::Prepared;
                Ok(request.metadata())
            }
        }
    }

    async fn reserve_request(
        &self,
        attempt_id: ConfigurationReadGrantAttemptId,
        choice: ConfigurationFollowerEnrollmentChoice,
        secret_ref: SecretRef,
        credential_digest: ConfigurationCredentialDigest,
    ) -> Result<ReserveResult, Error> {
        let ca_fingerprint = super::hash_bytes(&choice.certificate_authority_der);
        let mut connection = self.store.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;

        if let Some(existing) = load_request(&mut transaction, &attempt_id).await? {
            if !existing.matches_choice(&choice) {
                return Err(Error::IdempotencyConflict);
            }
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(ReserveResult::Existing(existing));
        }

        let state = configuration_sync::load(&mut transaction)
            .await
            .map_err(state_error)?
            .ok_or(Error::Conflict)?;
        if !matches_choice_state(&state, &choice) {
            return Err(Error::Conflict);
        }
        let live: Option<String> = sqlx::query_scalar(
            "SELECT attempt_id FROM configuration_follower_enrollment_lifecycle WHERE follower_instance_id = ? AND phase != 'retired' LIMIT 1",
        )
        .bind(choice.expected_instance_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| Error::Unavailable)?;
        if live.is_some() {
            return Err(Error::Conflict);
        }

        let request = StoredRequest {
            attempt_id: attempt_id.clone(),
            follower_instance_id: choice.expected_instance_id.clone(),
            expected_state_version: choice.expected_state_version,
            authority: choice.authority.clone(),
            server_name: choice.server_name.clone(),
            certificate_authority_der: choice.certificate_authority_der,
            certificate_authority_fingerprint: ca_fingerprint,
            secret_ref,
            credential_digest,
            phase: ConfigurationFollowerEnrollmentPhase::Reserved,
        };
        sqlx::query(
            "INSERT INTO configuration_follower_enrollment_requests (attempt_id, follower_instance_id, expected_state_version, group_id, master_instance_id, server_name, ca_der, ca_fingerprint, secret_ref, credential_digest) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(request.attempt_id.as_str())
        .bind(request.follower_instance_id.as_str())
        .bind(request.expected_state_version as i64)
        .bind(request.authority.group_id().as_str())
        .bind(request.authority.master_id().as_str())
        .bind(&request.server_name)
        .bind(&request.certificate_authority_der)
        .bind(request.certificate_authority_fingerprint.as_str())
        .bind(request.secret_ref.as_str())
        .bind(request.credential_digest.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| Error::Conflict)?;
        sqlx::query(
            "INSERT INTO configuration_follower_enrollment_lifecycle (attempt_id, follower_instance_id, phase) VALUES (?, ?, 'reserved')",
        )
        .bind(request.attempt_id.as_str())
        .bind(request.follower_instance_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| Error::Conflict)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(ReserveResult::Created(request))
    }

    async fn recover_request(&self, mut request: StoredRequest) -> Result<StoredRequest, Error> {
        if request.phase == ConfigurationFollowerEnrollmentPhase::Retired {
            self.retire_and_cleanup_owned(&request.attempt_id).await?;
            return Err(Error::Retired);
        }
        if !self.current_state_matches(&request).await? {
            self.retire_and_cleanup_owned(&request.attempt_id).await?;
            return Err(Error::Conflict);
        }
        match self.read_and_verify_credential(&request).await {
            Ok(()) => {}
            Err(VerifySecretError::Changed) => {
                self.retire_and_cleanup_owned(&request.attempt_id).await?;
                return Err(Error::RecoveryRequired);
            }
            Err(VerifySecretError::Unavailable(error)) => return Err(Error::Vault(error)),
        }
        if request.phase == ConfigurationFollowerEnrollmentPhase::Reserved {
            self.mark_prepared(&request).await?;
            request.phase = ConfigurationFollowerEnrollmentPhase::Prepared;
        }
        Ok(request)
    }

    async fn current_state_matches(&self, request: &StoredRequest) -> Result<bool, Error> {
        let mut connection = self.store.connection.lock().await;
        let state = configuration_sync::load(&mut connection)
            .await
            .map_err(state_error)?;
        Ok(state.is_some_and(|state| {
            state.instance_id() == &request.follower_instance_id
                && state.version() == request.expected_state_version
                && matches!(state.role(), ConfigurationRole::Unassigned)
        }))
    }

    #[allow(dead_code, reason = "consumed by the deferred internal exchange slice")]
    async fn confirm_submission_ready(&self, request: &StoredRequest) -> Result<(), Error> {
        let mut connection = self.store.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let current_request = load_request(&mut transaction, &request.attempt_id)
            .await?
            .ok_or(Error::IntegrityViolation)?;
        if !request.same_identity(&current_request) {
            return Err(Error::IntegrityViolation);
        }
        if current_request.phase == ConfigurationFollowerEnrollmentPhase::Retired {
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            drop(connection);
            self.retire_and_cleanup_owned(&request.attempt_id).await?;
            return Err(Error::Retired);
        }
        let state = configuration_sync::load(&mut transaction)
            .await
            .map_err(state_error)?;
        let current = state.is_some_and(|state| {
            state.instance_id() == &request.follower_instance_id
                && state.version() == request.expected_state_version
                && matches!(state.role(), ConfigurationRole::Unassigned)
        });
        if !current {
            sqlx::query(
                "UPDATE configuration_follower_enrollment_lifecycle SET phase = 'retired' WHERE attempt_id = ? AND phase != 'retired'",
            )
            .bind(request.attempt_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|_| Error::Unavailable)?;
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            drop(connection);
            self.cleanup_vault(request).await?;
            return Err(Error::Conflict);
        }
        if current_request.phase != ConfigurationFollowerEnrollmentPhase::Prepared {
            return Err(Error::IntegrityViolation);
        }
        transaction.commit().await.map_err(|_| Error::Unavailable)
    }

    async fn mark_prepared(&self, request: &StoredRequest) -> Result<(), Error> {
        let mut connection = self.store.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let state = configuration_sync::load(&mut transaction)
            .await
            .map_err(state_error)?;
        let current = state.is_some_and(|state| {
            state.instance_id() == &request.follower_instance_id
                && state.version() == request.expected_state_version
                && matches!(state.role(), ConfigurationRole::Unassigned)
        });
        let phase = load_phase(&mut transaction, &request.attempt_id).await?;
        if phase == Some(ConfigurationFollowerEnrollmentPhase::Retired) {
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            drop(connection);
            self.retire_and_cleanup_owned(&request.attempt_id).await?;
            return Err(Error::Retired);
        }
        if !current {
            sqlx::query(
                "UPDATE configuration_follower_enrollment_lifecycle SET phase = 'retired' WHERE attempt_id = ? AND phase != 'retired'",
            )
            .bind(request.attempt_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|_| Error::Unavailable)?;
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            drop(connection);
            self.cleanup_vault(request).await?;
            return Err(Error::Conflict);
        }
        if phase != Some(ConfigurationFollowerEnrollmentPhase::Prepared) {
            let result = sqlx::query(
                "UPDATE configuration_follower_enrollment_lifecycle SET phase = 'prepared' WHERE attempt_id = ? AND phase = 'reserved'",
            )
            .bind(request.attempt_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|_| Error::Unavailable)?;
            if result.rows_affected() != 1 {
                return Err(Error::IntegrityViolation);
            }
        }
        transaction.commit().await.map_err(|_| Error::Unavailable)
    }

    async fn read_and_verify_credential(
        &self,
        request: &StoredRequest,
    ) -> Result<(), VerifySecretError> {
        let binding = request.binding().map_err(|_| VerifySecretError::Changed)?;
        let value = match self.vault.get(&binding).await {
            Ok(value) => value,
            Err(
                SecretStoreError::NotFound
                | SecretStoreError::InvalidSecret
                | SecretStoreError::InvalidReference
                | SecretStoreError::ProviderBindingMismatch,
            ) => {
                return Err(VerifySecretError::Changed);
            }
            Err(error) => return Err(VerifySecretError::Unavailable(error)),
        };
        let credential = ConfigurationReadCredential::parse(value.as_bytes())
            .ok_or(VerifySecretError::Changed)?;
        if credential.digest() != request.credential_digest {
            return Err(VerifySecretError::Changed);
        }
        Ok(())
    }

    async fn retire_and_cleanup_owned(
        &self,
        attempt_id: &ConfigurationReadGrantAttemptId,
    ) -> Result<(), Error> {
        self.retire_and_cleanup_owned_with_expected(attempt_id, None)
            .await
    }

    async fn retire_and_cleanup_expected_owned(
        &self,
        attempt_id: &ConfigurationReadGrantAttemptId,
        expected_instance_id: &KilnInstanceId,
    ) -> Result<(), Error> {
        self.retire_and_cleanup_owned_with_expected(attempt_id, Some(expected_instance_id))
            .await
    }

    async fn retire_and_cleanup_owned_with_expected(
        &self,
        attempt_id: &ConfigurationReadGrantAttemptId,
        expected_instance_id: Option<&KilnInstanceId>,
    ) -> Result<(), Error> {
        let request = {
            let mut connection = self.store.connection.lock().await;
            let mut transaction = connection
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(|_| Error::Unavailable)?;
            let request = load_request(&mut transaction, attempt_id)
                .await?
                .ok_or(Error::NotFound)?;
            if expected_instance_id
                .is_some_and(|expected| expected != &request.follower_instance_id)
            {
                return Err(Error::Conflict);
            }
            sqlx::query(
                "UPDATE configuration_follower_enrollment_lifecycle SET phase = 'retired' WHERE attempt_id = ? AND phase != 'retired'",
            )
            .bind(attempt_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|_| Error::Unavailable)?;
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            request
        };
        self.cleanup_vault(&request).await
    }

    async fn cleanup_vault(&self, request: &StoredRequest) -> Result<(), Error> {
        let binding = request.binding()?;
        match self.vault.delete(&binding).await {
            Ok(()) | Err(SecretStoreError::NotFound) => Ok(()),
            Err(error) => Err(Error::Vault(error)),
        }
    }
}

impl ConfigurationFollowerEnrollmentAdministration for ConfigurationFollowerEnrollmentManager {
    fn prepare_configuration_follower_enrollment(
        &self,
        attempt_id: ConfigurationReadGrantAttemptId,
        choice: ConfigurationFollowerEnrollmentChoice,
    ) -> impl std::future::Future<
        Output = Result<ConfigurationFollowerEnrollmentMetadata, CoreError>,
    > + Send {
        async move {
            self.prepare(attempt_id, choice)
                .await
                .map_err(map_manager_error)
        }
    }

    fn list_configuration_follower_enrollments(
        &self,
        after: Option<&ConfigurationReadGrantAttemptId>,
        limit: usize,
    ) -> impl std::future::Future<
        Output = Result<Vec<ConfigurationFollowerEnrollmentMetadata>, CoreError>,
    > + Send {
        async move {
            if limit == 0 || limit > MAX_PAGE_SIZE as usize {
                return Err(CoreError::InvalidRequest);
            }
            let limit = u32::try_from(limit)
                .ok()
                .and_then(NonZeroU32::new)
                .ok_or(CoreError::InvalidRequest)?;
            self.list(after, limit).await.map_err(map_manager_error)
        }
    }

    fn get_configuration_follower_enrollment(
        &self,
        attempt_id: &ConfigurationReadGrantAttemptId,
    ) -> impl std::future::Future<
        Output = Result<Option<ConfigurationFollowerEnrollmentMetadata>, CoreError>,
    > + Send {
        async move {
            self.get_metadata(attempt_id)
                .await
                .map_err(map_manager_error)
        }
    }

    fn retire_configuration_follower_enrollment(
        &self,
        expected_instance_id: KilnInstanceId,
        attempt_id: ConfigurationReadGrantAttemptId,
    ) -> impl std::future::Future<Output = Result<(), CoreError>> + Send {
        async move {
            self.retire_checked(expected_instance_id, attempt_id)
                .await
                .map_err(map_manager_error)
        }
    }
}

fn map_manager_error(error: Error) -> CoreError {
    match error {
        Error::InvalidRequest => CoreError::InvalidRequest,
        Error::NotFound => CoreError::NotFound,
        Error::Conflict => CoreError::Conflict,
        Error::IdempotencyConflict => CoreError::IdempotencyConflict,
        Error::Retired => CoreError::Retired,
        Error::RecoveryRequired => CoreError::RecoveryRequired,
        Error::IntegrityViolation | Error::Vault(_) | Error::Unavailable => CoreError::Unavailable,
    }
}

fn validate_choice(choice: &ConfigurationFollowerEnrollmentChoice) -> Result<(), Error> {
    if choice.expected_state_version == 0
        || choice.expected_state_version >= i64::MAX as u64
        || choice.authority.master_id() == &choice.expected_instance_id
        || !valid_server_name(&choice.server_name)
        || choice.certificate_authority_der.is_empty()
        || choice.certificate_authority_der.len() > MAX_CERTIFICATE_DER_BYTES
    {
        return Err(Error::InvalidRequest);
    }
    Ok(())
}

enum ReserveResult {
    Existing(StoredRequest),
    Created(StoredRequest),
}

enum VerifySecretError {
    Changed,
    Unavailable(SecretStoreError),
}

struct StoredRequest {
    attempt_id: ConfigurationReadGrantAttemptId,
    follower_instance_id: KilnInstanceId,
    expected_state_version: u64,
    authority: ConfigurationAuthority,
    server_name: String,
    certificate_authority_der: Vec<u8>,
    certificate_authority_fingerprint: ContentHash,
    secret_ref: SecretRef,
    credential_digest: ConfigurationCredentialDigest,
    phase: ConfigurationFollowerEnrollmentPhase,
}

impl StoredRequest {
    fn binding(&self) -> Result<ConfigurationSecretBinding, Error> {
        ConfigurationSecretBinding::new(
            self.follower_instance_id.clone(),
            self.authority.clone(),
            ConfigurationSecretPurpose::FollowerReadCredential,
            self.secret_ref.clone(),
        )
        .map_err(|_| Error::IntegrityViolation)
    }

    fn matches_choice(&self, choice: &ConfigurationFollowerEnrollmentChoice) -> bool {
        self.follower_instance_id == choice.expected_instance_id
            && self.expected_state_version == choice.expected_state_version
            && self.authority == choice.authority
            && self.server_name == choice.server_name
            && self.certificate_authority_der == choice.certificate_authority_der
            && self.certificate_authority_fingerprint
                == super::hash_bytes(&choice.certificate_authority_der)
    }

    #[allow(dead_code, reason = "used by the deferred internal exchange slice")]
    fn same_identity(&self, other: &Self) -> bool {
        self.attempt_id == other.attempt_id
            && self.follower_instance_id == other.follower_instance_id
            && self.expected_state_version == other.expected_state_version
            && self.authority == other.authority
            && self.server_name == other.server_name
            && self.certificate_authority_der == other.certificate_authority_der
            && self.certificate_authority_fingerprint == other.certificate_authority_fingerprint
            && self.secret_ref == other.secret_ref
            && self.credential_digest == other.credential_digest
    }

    fn metadata(&self) -> ConfigurationFollowerEnrollmentMetadata {
        ConfigurationFollowerEnrollmentMetadata {
            attempt_id: self.attempt_id.clone(),
            follower_instance_id: self.follower_instance_id.clone(),
            expected_state_version: self.expected_state_version,
            authority: self.authority.clone(),
            server_name: self.server_name.clone(),
            certificate_authority_fingerprint: self.certificate_authority_fingerprint.clone(),
            phase: self.phase,
        }
    }
}

async fn load_request(
    connection: &mut SqliteConnection,
    attempt_id: &ConfigurationReadGrantAttemptId,
) -> Result<Option<StoredRequest>, Error> {
    sqlx::query(
        "SELECT r.attempt_id, r.follower_instance_id, r.expected_state_version, r.group_id, r.master_instance_id, r.server_name, r.ca_der, r.ca_fingerprint, r.secret_ref, r.credential_digest, l.phase FROM configuration_follower_enrollment_requests r JOIN configuration_follower_enrollment_lifecycle l USING (attempt_id) WHERE r.attempt_id = ?",
    )
    .bind(attempt_id.as_str())
    .fetch_optional(connection)
    .await
    .map_err(|_| Error::Unavailable)?
    .map(decode_request)
    .transpose()
}

async fn load_phase(
    connection: &mut SqliteConnection,
    attempt_id: &ConfigurationReadGrantAttemptId,
) -> Result<Option<ConfigurationFollowerEnrollmentPhase>, Error> {
    let phase: Option<String> = sqlx::query_scalar(
        "SELECT phase FROM configuration_follower_enrollment_lifecycle WHERE attempt_id = ?",
    )
    .bind(attempt_id.as_str())
    .fetch_optional(connection)
    .await
    .map_err(|_| Error::Unavailable)?;
    phase.map(|phase| parse_phase(&phase)).transpose()
}

fn decode_request(row: sqlx::sqlite::SqliteRow) -> Result<StoredRequest, Error> {
    let attempt_id = ConfigurationReadGrantAttemptId::parse(
        row.try_get::<String, _>("attempt_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let follower_instance_id = KilnInstanceId::parse(
        row.try_get::<String, _>("follower_instance_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let expected_state_version = u64::try_from(
        row.try_get::<i64, _>("expected_state_version")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let group_id = kiln_core::ConfigurationGroupId::parse(
        row.try_get::<String, _>("group_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let master_instance_id = KilnInstanceId::parse(
        row.try_get::<String, _>("master_instance_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    if master_instance_id == follower_instance_id {
        return Err(Error::IntegrityViolation);
    }
    let authority = ConfigurationAuthority::new(group_id, master_instance_id);
    let server_name: String = row
        .try_get("server_name")
        .map_err(|_| Error::IntegrityViolation)?;
    if !valid_server_name(&server_name) {
        return Err(Error::IntegrityViolation);
    }
    let certificate_authority_der: Vec<u8> = row
        .try_get("ca_der")
        .map_err(|_| Error::IntegrityViolation)?;
    if certificate_authority_der.is_empty()
        || certificate_authority_der.len() > MAX_CERTIFICATE_DER_BYTES
    {
        return Err(Error::IntegrityViolation);
    }
    let certificate_authority_fingerprint = ContentHash::parse(
        row.try_get::<String, _>("ca_fingerprint")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    if super::hash_bytes(&certificate_authority_der) != certificate_authority_fingerprint {
        return Err(Error::IntegrityViolation);
    }
    let secret_ref = SecretRef::parse(
        row.try_get::<String, _>("secret_ref")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let credential_digest = ConfigurationCredentialDigest::from_sha256(
        ContentHash::parse(
            row.try_get::<String, _>("credential_digest")
                .map_err(|_| Error::IntegrityViolation)?,
        )
        .map_err(|_| Error::IntegrityViolation)?,
    );
    let phase = parse_phase(
        &row.try_get::<String, _>("phase")
            .map_err(|_| Error::IntegrityViolation)?,
    )?;
    Ok(StoredRequest {
        attempt_id,
        follower_instance_id,
        expected_state_version,
        authority,
        server_name,
        certificate_authority_der,
        certificate_authority_fingerprint,
        secret_ref,
        credential_digest,
        phase,
    })
}

fn decode_metadata(
    row: sqlx::sqlite::SqliteRow,
) -> Result<ConfigurationFollowerEnrollmentMetadata, Error> {
    let attempt_id = ConfigurationReadGrantAttemptId::parse(
        row.try_get::<String, _>("attempt_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let follower_instance_id = KilnInstanceId::parse(
        row.try_get::<String, _>("follower_instance_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let expected_state_version = u64::try_from(
        row.try_get::<i64, _>("expected_state_version")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let group_id = kiln_core::ConfigurationGroupId::parse(
        row.try_get::<String, _>("group_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let master_instance_id = KilnInstanceId::parse(
        row.try_get::<String, _>("master_instance_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    if master_instance_id == follower_instance_id {
        return Err(Error::IntegrityViolation);
    }
    let authority = ConfigurationAuthority::new(group_id, master_instance_id);
    let server_name: String = row
        .try_get("server_name")
        .map_err(|_| Error::IntegrityViolation)?;
    if !valid_server_name(&server_name) {
        return Err(Error::IntegrityViolation);
    }
    let certificate_authority_fingerprint = ContentHash::parse(
        row.try_get::<String, _>("ca_fingerprint")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let phase = parse_phase(
        &row.try_get::<String, _>("phase")
            .map_err(|_| Error::IntegrityViolation)?,
    )?;
    Ok(ConfigurationFollowerEnrollmentMetadata {
        attempt_id,
        follower_instance_id,
        expected_state_version,
        authority,
        server_name,
        certificate_authority_fingerprint,
        phase,
    })
}

fn parse_phase(phase: &str) -> Result<ConfigurationFollowerEnrollmentPhase, Error> {
    match phase {
        "reserved" => Ok(ConfigurationFollowerEnrollmentPhase::Reserved),
        "prepared" => Ok(ConfigurationFollowerEnrollmentPhase::Prepared),
        "retired" => Ok(ConfigurationFollowerEnrollmentPhase::Retired),
        _ => Err(Error::IntegrityViolation),
    }
}

fn matches_choice_state(
    state: &kiln_core::ConfigurationInstanceState,
    choice: &ConfigurationFollowerEnrollmentChoice,
) -> bool {
    state.instance_id() == &choice.expected_instance_id
        && state.version() == choice.expected_state_version
        && matches!(state.role(), ConfigurationRole::Unassigned)
}

fn state_error(error: kiln_core::ConfigurationStateError) -> Error {
    match error {
        kiln_core::ConfigurationStateError::Unavailable => Error::Unavailable,
        _ => Error::IntegrityViolation,
    }
}
