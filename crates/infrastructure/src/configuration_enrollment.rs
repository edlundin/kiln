//! Durable follower credential reservation, explicit exchange, and recovery.

use super::{
    ConfigurationReadCredential, OsConfigurationSecretStore, SqliteStore,
    configuration_identity::valid_server_name, configuration_sync,
};
use kiln_core::{
    ConfigurationAuthority, ConfigurationCredentialDigest,
    ConfigurationFollowerEnrollmentAdministration, ConfigurationFollowerEnrollmentChoice,
    ConfigurationFollowerEnrollmentError as CoreError,
    ConfigurationFollowerEnrollmentExchangeResult, ConfigurationFollowerEnrollmentExchangeSettings,
    ConfigurationFollowerEnrollmentMetadata, ConfigurationFollowerEnrollmentPhase,
    ConfigurationFollowerEnrollmentRemotePhase, ConfigurationFollowerEnrollmentRemoteReceipt,
    ConfigurationFollowerEnrollmentSubmission, ConfigurationFollowerEnrollmentTransport,
    ConfigurationFollowerEnrollmentTransportError, ConfigurationFollowerSnapshotCandidate,
    ConfigurationFollowerSnapshotPeer, ConfigurationFollowerSnapshotTransport,
    ConfigurationFollowerSnapshotTransportError, ConfigurationFollowerSnapshotValidator,
    ConfigurationInstanceState, ConfigurationReadGrantAttemptId, ConfigurationRevision,
    ConfigurationRole, ConfigurationSecretBinding, ConfigurationSecretPurpose,
    ConfigurationSecretStore, ConfigurationSnapshotError, ConfigurationSnapshotMutation,
    ContentHash, KilnInstanceId, SecretRef, SecretStoreError, SecretValue,
};
use sha2::{Digest as _, Sha256};
use sqlx::{Connection, Row, SqliteConnection};
use std::{num::NonZeroU32, sync::Arc};

// A 100-row API page fetches one additional row to determine whether a next
// cursor is available.
const MAX_PAGE_SIZE: u32 = 101;
// A TLS Certificate entry has a uint24 length field (RFC 8446, section 4.4.2).
const MAX_CERTIFICATE_DER_BYTES: usize = 0xFF_FFFF;
// Kept equal to the protocol's bounded receipt budget; infrastructure does not
// depend on the wire crate.
const MAX_RECEIPT_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigurationFollowerEnrollmentManagerError {
    InvalidRequest,
    NotFound,
    EnrollmentInactive,
    Conflict,
    IdempotencyConflict,
    Retired,
    RecoveryRequired,
    InvalidSnapshot,
    SnapshotTooLarge,
    IntegrityViolation,
    Vault(SecretStoreError),
    Unavailable,
}
use ConfigurationFollowerEnrollmentManagerError as Error;

/// Owns reservation, exchange, receipt and vault lifecycles for one SQLite
/// store. Do not create independent owners for the same database; the local
/// operation lock covers network and vault effects while SQLite transactions
/// fence durable state writes.
#[derive(Clone)]
pub struct ConfigurationFollowerEnrollmentManager {
    store: SqliteStore,
    vault: OsConfigurationSecretStore,
    transport: Option<Arc<dyn ConfigurationFollowerEnrollmentTransport>>,
    snapshot_transport: Option<Arc<dyn ConfigurationFollowerSnapshotTransport>>,
}

impl ConfigurationFollowerEnrollmentManager {
    pub fn new(store: SqliteStore, vault: OsConfigurationSecretStore) -> Self {
        Self {
            store,
            vault,
            transport: None,
            snapshot_transport: None,
        }
    }

    pub fn with_exchange_transport(
        mut self,
        transport: Arc<dyn ConfigurationFollowerEnrollmentTransport>,
    ) -> Self {
        self.transport = Some(transport);
        self
    }

    pub fn with_snapshot_transport(
        mut self,
        transport: Arc<dyn ConfigurationFollowerSnapshotTransport>,
    ) -> Self {
        self.snapshot_transport = Some(transport);
        self
    }

    async fn exchange(
        &self,
        expected_instance_id: KilnInstanceId,
        attempt_id: ConfigurationReadGrantAttemptId,
        settings: ConfigurationFollowerEnrollmentExchangeSettings,
    ) -> Result<ConfigurationFollowerEnrollmentMetadata, Error> {
        let owner = self.clone();
        tokio::spawn(async move {
            let _guard = owner.store.configuration_enrollment_operations.lock().await;
            owner
                .exchange_owned(expected_instance_id, attempt_id, settings)
                .await
        })
        .await
        .map_err(|_| Error::Unavailable)?
    }

    async fn fetch_and_apply_snapshot(
        &self,
        expected_instance_id: KilnInstanceId,
        attempt_id: ConfigurationReadGrantAttemptId,
        settings: ConfigurationFollowerEnrollmentExchangeSettings,
        validate: ConfigurationFollowerSnapshotValidator,
    ) -> Result<ConfigurationSnapshotMutation, Error> {
        let owner = self.clone();
        tokio::spawn(async move {
            let _guard = owner.store.configuration_enrollment_operations.lock().await;
            owner
                .fetch_and_apply_snapshot_owned(
                    expected_instance_id,
                    attempt_id,
                    settings,
                    validate,
                )
                .await
        })
        .await
        .map_err(|_| Error::Unavailable)?
    }

    async fn fetch_and_apply_snapshot_owned(
        &self,
        expected_instance_id: KilnInstanceId,
        attempt_id: ConfigurationReadGrantAttemptId,
        settings: ConfigurationFollowerEnrollmentExchangeSettings,
        validate: ConfigurationFollowerSnapshotValidator,
    ) -> Result<ConfigurationSnapshotMutation, Error> {
        if !valid_exchange_settings(&settings) {
            return Err(Error::InvalidRequest);
        }
        let transport = self
            .snapshot_transport
            .as_ref()
            .ok_or(Error::Unavailable)?
            .clone();
        let (request, pre_fetch_state) = self
            .acquire_active_snapshot_enrollment(&expected_instance_id, &attempt_id)
            .await?;
        let credential = match self.read_and_verify_credential(&request).await {
            Ok(credential) => credential,
            Err(VerifySecretError::Changed) => {
                self.retire_and_cleanup_owned(&request.attempt_id).await?;
                return Err(Error::RecoveryRequired);
            }
            Err(VerifySecretError::Unavailable(error)) => return Err(Error::Vault(error)),
        };
        let candidate = transport
            .fetch(
                ConfigurationFollowerSnapshotPeer {
                    follower_instance_id: request.follower_instance_id.clone(),
                    authority: request.authority.clone(),
                    server_name: request.server_name.clone(),
                    certificate_authority_der: request.certificate_authority_der.clone(),
                },
                credential,
                settings,
            )
            .await
            .map_err(|error| match error {
                ConfigurationFollowerSnapshotTransportError::InvalidSettings => {
                    Error::InvalidRequest
                }
                ConfigurationFollowerSnapshotTransportError::TooLarge => Error::SnapshotTooLarge,
                ConfigurationFollowerSnapshotTransportError::Failed => Error::Unavailable,
            })?;
        let revision = revision_from_candidate(&request, &candidate)?;
        let observed_state = self
            .observe_snapshot_revision(&request, &pre_fetch_state, &revision)
            .await?;
        let snapshot = validate(candidate).map_err(map_snapshot_error)?;
        super::configuration_snapshot::apply_follower_enrollment_snapshot(
            &self.store,
            &observed_state,
            &request.attempt_id,
            revision,
            &snapshot,
        )
        .await
        .map_err(map_snapshot_error)
    }

    async fn acquire_active_snapshot_enrollment(
        &self,
        expected_instance_id: &KilnInstanceId,
        attempt_id: &ConfigurationReadGrantAttemptId,
    ) -> Result<(StoredRequest, ConfigurationInstanceState), Error> {
        let mut connection = self.store.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let request = load_request(&mut transaction, attempt_id)
            .await?
            .ok_or(Error::NotFound)?;
        if &request.follower_instance_id != expected_instance_id {
            return Err(Error::EnrollmentInactive);
        }
        let credential_state: Option<String> = sqlx::query_scalar(
            "SELECT state FROM configuration_follower_enrollment_credentials WHERE attempt_id = ?",
        )
        .bind(attempt_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| Error::Unavailable)?;
        match credential_state.as_deref() {
            Some("active") => {}
            Some("pending" | "retired") => return Err(Error::RecoveryRequired),
            _ => return Err(Error::IntegrityViolation),
        }
        if request.phase != ConfigurationFollowerEnrollmentPhase::Retired
            || request.exchange_result
                != Some(ConfigurationFollowerEnrollmentExchangeResult::Approved)
        {
            return Err(Error::IntegrityViolation);
        }
        let state = configuration_sync::load(&mut transaction)
            .await
            .map_err(state_error)?
            .ok_or(Error::EnrollmentInactive)?;
        if state.instance_id() != expected_instance_id
            || !matches!(state.role(), ConfigurationRole::Follower(authority) if authority == &request.authority)
        {
            return Err(Error::EnrollmentInactive);
        }
        verify_active_snapshot_credential(&mut transaction, attempt_id, &state)
            .await
            .map_err(map_snapshot_error)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok((request, state))
    }

    async fn observe_snapshot_revision(
        &self,
        request: &StoredRequest,
        pre_fetch_state: &ConfigurationInstanceState,
        revision: &ConfigurationRevision,
    ) -> Result<ConfigurationInstanceState, Error> {
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
        let current = configuration_sync::load(&mut transaction)
            .await
            .map_err(state_error)?
            .ok_or(Error::Conflict)?;
        if &current != pre_fetch_state {
            return Err(Error::Conflict);
        }
        verify_active_snapshot_credential(&mut transaction, &request.attempt_id, &current)
            .await
            .map_err(map_snapshot_error)?;
        let next = current
            .observe(revision.clone())
            .map_err(|error| match error {
                kiln_core::ConfigurationStateError::Revision(
                    kiln_core::ConfigurationSyncError::AuthorityMismatch
                    | kiln_core::ConfigurationSyncError::StaleRevision
                    | kiln_core::ConfigurationSyncError::RevisionConflict,
                )
                | kiln_core::ConfigurationStateError::InvalidRole
                | kiln_core::ConfigurationStateError::Conflict => Error::Conflict,
                _ => Error::Unavailable,
            })?;
        configuration_sync::save(&mut transaction, &current, &next)
            .await
            .map_err(state_error)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(next)
    }

    async fn exchange_owned(
        &self,
        expected_instance_id: KilnInstanceId,
        attempt_id: ConfigurationReadGrantAttemptId,
        settings: ConfigurationFollowerEnrollmentExchangeSettings,
    ) -> Result<ConfigurationFollowerEnrollmentMetadata, Error> {
        let mut request = {
            let mut connection = self.store.connection.lock().await;
            load_request(&mut connection, &attempt_id)
                .await?
                .ok_or(Error::NotFound)?
        };
        if request.follower_instance_id != expected_instance_id {
            return Err(Error::Conflict);
        }
        if request.phase == ConfigurationFollowerEnrollmentPhase::Retired {
            // Approval is a finalized result, not a request to clean up the
            // active credential. Explicit retirement owns later deletion.
            if request.exchange_result
                != Some(ConfigurationFollowerEnrollmentExchangeResult::Approved)
            {
                self.cleanup_vault(&request).await?;
            }
            return Ok(request.metadata());
        }
        if !self.current_state_matches(&request).await? {
            self.retire_for_role_conflict(&mut request, None).await?;
            return Ok(request.metadata());
        }
        if !valid_exchange_settings(&settings) {
            return Err(Error::InvalidRequest);
        }
        let transport = self.transport.as_ref().ok_or(Error::Unavailable)?.clone();
        request = match self.recover_request(request).await {
            Ok(request) => request,
            Err(Error::Conflict) => {
                return self
                    .get_metadata(&attempt_id)
                    .await?
                    .ok_or(Error::IntegrityViolation);
            }
            Err(error) => return Err(error),
        };
        if let Err(error) = self.confirm_submission_ready(&request).await {
            if error == Error::Conflict {
                return self
                    .get_metadata(&attempt_id)
                    .await?
                    .ok_or(Error::IntegrityViolation);
            }
            return Err(error);
        }
        let credential = match self.read_and_verify_credential(&request).await {
            Ok(credential) => credential,
            Err(VerifySecretError::Changed) => {
                self.retire_and_cleanup_owned(&request.attempt_id).await?;
                return Err(Error::RecoveryRequired);
            }
            Err(VerifySecretError::Unavailable(error)) => return Err(Error::Vault(error)),
        };
        let receipt = transport
            .submit(submission_from_request(&request), credential, settings)
            .await
            .map_err(|error| match error {
                ConfigurationFollowerEnrollmentTransportError::InvalidSettings => {
                    Error::InvalidRequest
                }
                ConfigurationFollowerEnrollmentTransportError::Failed => Error::Unavailable,
            })?;
        if !valid_remote_receipt(&request, &receipt) {
            return Err(Error::IntegrityViolation);
        }
        self.finalize_exchange(&mut request, receipt).await?;
        Ok(request.metadata())
    }

    async fn finalize_exchange(
        &self,
        request: &mut StoredRequest,
        receipt: ConfigurationFollowerEnrollmentRemoteReceipt,
    ) -> Result<(), Error> {
        use ConfigurationFollowerEnrollmentExchangeResult as ResultPhase;

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
            request.phase = ConfigurationFollowerEnrollmentPhase::Retired;
            request.exchange_result = current_request.exchange_result;
            request.last_observed_receipt = current_request.last_observed_receipt;
            self.cleanup_vault(request).await?;
            return Ok(());
        }
        if current_request.phase != ConfigurationFollowerEnrollmentPhase::Prepared {
            return Err(Error::IntegrityViolation);
        }
        let current_state = configuration_sync::load(&mut transaction)
            .await
            .map_err(state_error)?;
        let current = current_state.as_ref().is_some_and(|state| {
            state.instance_id() == &request.follower_instance_id
                && state.version() == request.expected_state_version
                && matches!(state.role(), ConfigurationRole::Unassigned)
        });
        if !current {
            retire_enrollment(&mut transaction, &request.attempt_id).await?;
            store_observation(
                &mut transaction,
                &request.attempt_id,
                ResultPhase::RoleConflict,
                Some(&receipt),
            )
            .await?;
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            drop(connection);
            request.phase = ConfigurationFollowerEnrollmentPhase::Retired;
            request.exchange_result = Some(ResultPhase::RoleConflict);
            request.last_observed_receipt = Some(receipt);
            self.cleanup_vault(request).await?;
            return Ok(());
        }

        match receipt.phase {
            ConfigurationFollowerEnrollmentRemotePhase::Pending => {
                store_observation(
                    &mut transaction,
                    &request.attempt_id,
                    ResultPhase::Pending,
                    Some(&receipt),
                )
                .await?;
                transaction.commit().await.map_err(|_| Error::Unavailable)?;
                request.exchange_result = Some(ResultPhase::Pending);
                request.last_observed_receipt = Some(receipt);
            }
            ConfigurationFollowerEnrollmentRemotePhase::Rejected => {
                retire_enrollment(&mut transaction, &request.attempt_id).await?;
                store_observation(
                    &mut transaction,
                    &request.attempt_id,
                    ResultPhase::Rejected,
                    Some(&receipt),
                )
                .await?;
                transaction.commit().await.map_err(|_| Error::Unavailable)?;
                drop(connection);
                request.phase = ConfigurationFollowerEnrollmentPhase::Retired;
                request.exchange_result = Some(ResultPhase::Rejected);
                request.last_observed_receipt = Some(receipt);
                self.cleanup_vault(request).await?;
            }
            ConfigurationFollowerEnrollmentRemotePhase::Approved => {
                let grant = receipt.grant.as_ref().ok_or(Error::IntegrityViolation)?;
                if grant.revoked {
                    retire_enrollment(&mut transaction, &request.attempt_id).await?;
                    store_observation(
                        &mut transaction,
                        &request.attempt_id,
                        ResultPhase::Revoked,
                        Some(&receipt),
                    )
                    .await?;
                    transaction.commit().await.map_err(|_| Error::Unavailable)?;
                    drop(connection);
                    request.phase = ConfigurationFollowerEnrollmentPhase::Retired;
                    request.exchange_result = Some(ResultPhase::Revoked);
                    request.last_observed_receipt = Some(receipt);
                    self.cleanup_vault(request).await?;
                } else {
                    let state = current_state.ok_or(Error::IntegrityViolation)?;
                    let known_master: Option<String> = sqlx::query_scalar(
                        "SELECT master_instance_id FROM configuration_authorities WHERE group_id = ?",
                    )
                    .bind(request.authority.group_id().as_str())
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(|_| Error::Unavailable)?;
                    if known_master
                        .as_ref()
                        .is_some_and(|master| master != request.authority.master_id().as_str())
                    {
                        retire_enrollment(&mut transaction, &request.attempt_id).await?;
                        store_observation(
                            &mut transaction,
                            &request.attempt_id,
                            ResultPhase::RoleConflict,
                            Some(&receipt),
                        )
                        .await?;
                        transaction.commit().await.map_err(|_| Error::Unavailable)?;
                        drop(connection);
                        request.phase = ConfigurationFollowerEnrollmentPhase::Retired;
                        request.exchange_result = Some(ResultPhase::RoleConflict);
                        request.last_observed_receipt = Some(receipt);
                        self.cleanup_vault(request).await?;
                        return Ok(());
                    }
                    sqlx::query(
                        "INSERT INTO configuration_authorities (group_id, master_instance_id) VALUES (?, ?) ON CONFLICT(group_id) DO NOTHING",
                    )
                    .bind(request.authority.group_id().as_str())
                    .bind(request.authority.master_id().as_str())
                    .execute(&mut *transaction)
                    .await
                    .map_err(|_| Error::Unavailable)?;
                    let next = state
                        .change_role(ConfigurationRole::Follower(request.authority.clone()))
                        .map_err(state_error)?;
                    configuration_sync::save(&mut transaction, &state, &next)
                        .await
                        .map_err(state_error)?;
                    retire_lifecycle_only(&mut transaction, &request.attempt_id).await?;
                    store_observation(
                        &mut transaction,
                        &request.attempt_id,
                        ResultPhase::Approved,
                        Some(&receipt),
                    )
                    .await?;
                    activate_follower_credential(&mut transaction, &request.attempt_id).await?;
                    let reloaded = configuration_sync::load(&mut transaction)
                        .await
                        .map_err(state_error)?
                        .ok_or(Error::IntegrityViolation)?;
                    if reloaded.role() != &ConfigurationRole::Follower(request.authority.clone()) {
                        return Err(Error::IntegrityViolation);
                    }
                    transaction.commit().await.map_err(|_| Error::Unavailable)?;
                    drop(connection);
                    self.store.notify_configuration_serving_change();
                    request.phase = ConfigurationFollowerEnrollmentPhase::Retired;
                    request.exchange_result = Some(ResultPhase::Approved);
                    request.last_observed_receipt = Some(receipt);
                }
            }
        }
        Ok(())
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
            "SELECT r.attempt_id, r.follower_instance_id, r.expected_state_version, r.group_id, r.master_instance_id, r.server_name, r.ca_fingerprint, l.phase, o.result AS exchange_result, o.receipt_json FROM configuration_follower_enrollment_requests r JOIN configuration_follower_enrollment_lifecycle l USING (attempt_id) LEFT JOIN configuration_follower_enrollment_observations o USING (attempt_id) WHERE (? IS NULL OR r.attempt_id > ?) ORDER BY r.attempt_id LIMIT ?",
        )
        .bind(after.map(ConfigurationReadGrantAttemptId::as_str))
        .bind(after.map(ConfigurationReadGrantAttemptId::as_str))
        .bind(i64::from(limit.get()))
        .fetch_all(&mut *connection)
        .await
        .map_err(|_| Error::Unavailable)?;
        rows.into_iter().map(decode_metadata).collect()
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
            "SELECT r.attempt_id, r.follower_instance_id, r.expected_state_version, r.group_id, r.master_instance_id, r.server_name, r.ca_fingerprint, l.phase, o.result AS exchange_result, o.receipt_json FROM configuration_follower_enrollment_requests r JOIN configuration_follower_enrollment_lifecycle l USING (attempt_id) LEFT JOIN configuration_follower_enrollment_observations o USING (attempt_id) WHERE r.attempt_id = ?",
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
                    Ok(_) => {}
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
            exchange_result: None,
            last_observed_receipt: None,
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
        sqlx::query(
            "INSERT INTO configuration_follower_enrollment_credentials (attempt_id, state) VALUES (?, 'pending')",
        )
        .bind(request.attempt_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| Error::Conflict)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(ReserveResult::Created(request))
    }

    async fn recover_request(&self, mut request: StoredRequest) -> Result<StoredRequest, Error> {
        if request.phase == ConfigurationFollowerEnrollmentPhase::Retired {
            if request.exchange_result
                == Some(ConfigurationFollowerEnrollmentExchangeResult::Approved)
            {
                // Preserve the durable result after approval. A prior explicit
                // retirement may already have deleted the credential; do not
                // recreate it or change the joined role on retry.
                return Ok(request);
            }
            self.retire_and_cleanup_owned(&request.attempt_id).await?;
            return Err(Error::Retired);
        }
        if !self.current_state_matches(&request).await? {
            self.retire_for_role_conflict(&mut request, None).await?;
            return Err(Error::Conflict);
        }
        match self.read_and_verify_credential(&request).await {
            Ok(_) => {}
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

    async fn retire_for_role_conflict(
        &self,
        request: &mut StoredRequest,
        receipt: Option<&ConfigurationFollowerEnrollmentRemoteReceipt>,
    ) -> Result<(), Error> {
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
        retire_enrollment(&mut transaction, &request.attempt_id).await?;
        store_observation(
            &mut transaction,
            &request.attempt_id,
            ConfigurationFollowerEnrollmentExchangeResult::RoleConflict,
            receipt,
        )
        .await?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        drop(connection);
        request.phase = ConfigurationFollowerEnrollmentPhase::Retired;
        request.exchange_result = Some(ConfigurationFollowerEnrollmentExchangeResult::RoleConflict);
        if let Some(receipt) = receipt {
            request.last_observed_receipt = Some(receipt.clone());
        }
        self.cleanup_vault(request).await
    }

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
            retire_enrollment(&mut transaction, &request.attempt_id).await?;
            store_observation(
                &mut transaction,
                &request.attempt_id,
                ConfigurationFollowerEnrollmentExchangeResult::RoleConflict,
                None,
            )
            .await?;
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
            retire_enrollment(&mut transaction, &request.attempt_id).await?;
            store_observation(
                &mut transaction,
                &request.attempt_id,
                ConfigurationFollowerEnrollmentExchangeResult::RoleConflict,
                None,
            )
            .await?;
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
    ) -> Result<SecretValue, VerifySecretError> {
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
        drop(credential);
        Ok(value)
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
            retire_enrollment(&mut transaction, attempt_id).await?;
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

    fn exchange_configuration_follower_enrollment(
        &self,
        expected_instance_id: KilnInstanceId,
        attempt_id: ConfigurationReadGrantAttemptId,
        settings: ConfigurationFollowerEnrollmentExchangeSettings,
    ) -> impl std::future::Future<
        Output = Result<ConfigurationFollowerEnrollmentMetadata, CoreError>,
    > + Send {
        async move {
            self.exchange(expected_instance_id, attempt_id, settings)
                .await
                .map_err(map_manager_error)
        }
    }

    fn fetch_and_apply_configuration_follower_snapshot(
        &self,
        expected_instance_id: KilnInstanceId,
        attempt_id: ConfigurationReadGrantAttemptId,
        settings: ConfigurationFollowerEnrollmentExchangeSettings,
        validate: ConfigurationFollowerSnapshotValidator,
    ) -> impl std::future::Future<Output = Result<ConfigurationSnapshotMutation, CoreError>> + Send
    {
        async move {
            self.fetch_and_apply_snapshot(expected_instance_id, attempt_id, settings, validate)
                .await
                .map_err(map_manager_error)
        }
    }
}

pub(super) async fn verify_active_snapshot_credential(
    connection: &mut SqliteConnection,
    attempt_id: &ConfigurationReadGrantAttemptId,
    state: &ConfigurationInstanceState,
) -> Result<(), ConfigurationSnapshotError> {
    use kiln_core::{ConfigurationRole, ConfigurationStateError};

    let Some(row) = sqlx::query(
        "SELECT r.follower_instance_id, r.group_id, r.master_instance_id, l.phase, o.result, c.state AS credential_state FROM configuration_follower_enrollment_requests r JOIN configuration_follower_enrollment_lifecycle l USING (attempt_id) JOIN configuration_follower_enrollment_credentials c USING (attempt_id) LEFT JOIN configuration_follower_enrollment_observations o USING (attempt_id) WHERE r.attempt_id = ?",
    )
    .bind(attempt_id.as_str())
    .fetch_optional(connection)
    .await
    .map_err(|_| ConfigurationSnapshotError::Unavailable)?
    else {
        return Err(ConfigurationSnapshotError::IntegrityViolation);
    };
    let follower: String = row
        .try_get("follower_instance_id")
        .map_err(|_| ConfigurationSnapshotError::IntegrityViolation)?;
    let group: String = row
        .try_get("group_id")
        .map_err(|_| ConfigurationSnapshotError::IntegrityViolation)?;
    let master: String = row
        .try_get("master_instance_id")
        .map_err(|_| ConfigurationSnapshotError::IntegrityViolation)?;
    let phase: String = row
        .try_get("phase")
        .map_err(|_| ConfigurationSnapshotError::IntegrityViolation)?;
    let result: Option<String> = row
        .try_get("result")
        .map_err(|_| ConfigurationSnapshotError::IntegrityViolation)?;
    let credential_state: String = row
        .try_get("credential_state")
        .map_err(|_| ConfigurationSnapshotError::IntegrityViolation)?;
    let ConfigurationRole::Follower(authority) = state.role() else {
        return Err(ConfigurationSnapshotError::State(
            ConfigurationStateError::Conflict,
        ));
    };
    if follower != state.instance_id().as_str()
        || group != authority.group_id().as_str()
        || master != authority.master_id().as_str()
        || phase != "retired"
        || result.as_deref() != Some("approved")
        || credential_state != "active"
    {
        return Err(ConfigurationSnapshotError::State(
            ConfigurationStateError::Conflict,
        ));
    }
    Ok(())
}

fn revision_from_candidate(
    request: &StoredRequest,
    candidate: &ConfigurationFollowerSnapshotCandidate,
) -> Result<ConfigurationRevision, Error> {
    if candidate.instance_id != request.authority.master_id().as_str()
        || candidate.master_instance_id != request.authority.master_id().as_str()
        || candidate.group_id != request.authority.group_id().as_str()
        || candidate.state_version == 0
        || candidate.state_version > i64::MAX as u64
        || candidate.revision_number == 0
        || candidate.revision_number > i64::MAX as u64
        || candidate.schema_version == 0
    {
        return Err(Error::IntegrityViolation);
    }
    let content_hash = ContentHash::parse(candidate.content_hash.clone())
        .map_err(|_| Error::IntegrityViolation)?;
    ConfigurationRevision::new(
        request.authority.clone(),
        candidate.revision_number,
        candidate.schema_version,
        content_hash,
    )
    .map_err(|_| Error::IntegrityViolation)
}

fn map_snapshot_error(error: ConfigurationSnapshotError) -> Error {
    use kiln_core::{ConfigurationStateError, ConfigurationSyncError};
    match error {
        ConfigurationSnapshotError::InvalidSnapshot => Error::InvalidSnapshot,
        ConfigurationSnapshotError::LimitExceeded => Error::SnapshotTooLarge,
        ConfigurationSnapshotError::InvalidRequest | ConfigurationSnapshotError::InvalidLimits => {
            Error::InvalidRequest
        }
        ConfigurationSnapshotError::State(
            ConfigurationStateError::Conflict
            | ConfigurationStateError::InvalidRole
            | ConfigurationStateError::Revision(
                ConfigurationSyncError::AuthorityMismatch
                | ConfigurationSyncError::StaleRevision
                | ConfigurationSyncError::RevisionConflict
                | ConfigurationSyncError::UnsupportedSchema
                | ConfigurationSyncError::SelfEnrollment
                | ConfigurationSyncError::InvalidRevision,
            ),
        ) => Error::Conflict,
        ConfigurationSnapshotError::IdempotencyConflict => Error::Conflict,
        ConfigurationSnapshotError::State(
            ConfigurationStateError::Uninitialized
            | ConfigurationStateError::AuthorityConflict
            | ConfigurationStateError::VersionExhausted
            | ConfigurationStateError::IntegrityViolation
            | ConfigurationStateError::Unavailable,
        )
        | ConfigurationSnapshotError::IntegrityViolation
        | ConfigurationSnapshotError::Unavailable => Error::Unavailable,
    }
}

fn map_manager_error(error: Error) -> CoreError {
    match error {
        Error::InvalidRequest => CoreError::InvalidRequest,
        Error::NotFound => CoreError::NotFound,
        Error::EnrollmentInactive => CoreError::EnrollmentInactive,
        Error::Conflict => CoreError::Conflict,
        Error::IdempotencyConflict => CoreError::IdempotencyConflict,
        Error::Retired => CoreError::Retired,
        Error::RecoveryRequired => CoreError::RecoveryRequired,
        Error::InvalidSnapshot => CoreError::InvalidSnapshot,
        Error::SnapshotTooLarge => CoreError::SnapshotTooLarge,
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
    exchange_result: Option<ConfigurationFollowerEnrollmentExchangeResult>,
    last_observed_receipt: Option<ConfigurationFollowerEnrollmentRemoteReceipt>,
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
            exchange_result: self.exchange_result,
            last_observed_receipt: self.last_observed_receipt.clone(),
        }
    }
}

async fn load_request(
    connection: &mut SqliteConnection,
    attempt_id: &ConfigurationReadGrantAttemptId,
) -> Result<Option<StoredRequest>, Error> {
    sqlx::query(
        "SELECT r.attempt_id, r.follower_instance_id, r.expected_state_version, r.group_id, r.master_instance_id, r.server_name, r.ca_der, r.ca_fingerprint, r.secret_ref, r.credential_digest, l.phase, o.result AS exchange_result, o.receipt_json FROM configuration_follower_enrollment_requests r JOIN configuration_follower_enrollment_lifecycle l USING (attempt_id) LEFT JOIN configuration_follower_enrollment_observations o USING (attempt_id) WHERE r.attempt_id = ?",
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
    let mut request = StoredRequest {
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
        exchange_result: None,
        last_observed_receipt: None,
    };
    (request.exchange_result, request.last_observed_receipt) = decode_observation(
        &row,
        &request.attempt_id,
        &request.follower_instance_id,
        request.expected_state_version,
        &request.authority,
        &request.server_name,
        &request.certificate_authority_fingerprint,
        request.phase,
    )?;
    Ok(request)
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
    let (exchange_result, last_observed_receipt) = decode_observation(
        &row,
        &attempt_id,
        &follower_instance_id,
        expected_state_version,
        &authority,
        &server_name,
        &certificate_authority_fingerprint,
        phase,
    )?;
    Ok(ConfigurationFollowerEnrollmentMetadata {
        attempt_id,
        follower_instance_id,
        expected_state_version,
        authority,
        server_name,
        certificate_authority_fingerprint,
        phase,
        exchange_result,
        last_observed_receipt,
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

fn decode_observation(
    row: &sqlx::sqlite::SqliteRow,
    attempt_id: &ConfigurationReadGrantAttemptId,
    follower_instance_id: &KilnInstanceId,
    follower_state_version: u64,
    authority: &ConfigurationAuthority,
    server_name: &str,
    ca_fingerprint: &ContentHash,
    phase: ConfigurationFollowerEnrollmentPhase,
) -> Result<
    (
        Option<ConfigurationFollowerEnrollmentExchangeResult>,
        Option<ConfigurationFollowerEnrollmentRemoteReceipt>,
    ),
    Error,
> {
    let result = row
        .try_get::<Option<String>, _>("exchange_result")
        .map_err(|_| Error::IntegrityViolation)?
        .map(|value| parse_exchange_result(&value))
        .transpose()?;
    let receipt_json: Option<String> = row
        .try_get("receipt_json")
        .map_err(|_| Error::IntegrityViolation)?;
    let receipt: Option<ConfigurationFollowerEnrollmentRemoteReceipt> = receipt_json
        .map(|value| serde_json::from_str(&value).map_err(|_| Error::IntegrityViolation))
        .transpose()?;
    match (result, receipt.as_ref()) {
        (None, None) => return Ok((None, None)),
        (None, Some(_)) => return Err(Error::IntegrityViolation),
        (Some(ConfigurationFollowerEnrollmentExchangeResult::RoleConflict), None) => {}
        (Some(_), None) => return Err(Error::IntegrityViolation),
        (Some(_), Some(receipt))
            if !valid_remote_receipt_binding(
                receipt,
                attempt_id,
                follower_instance_id,
                follower_state_version,
                authority,
                server_name,
                ca_fingerprint,
            ) =>
        {
            return Err(Error::IntegrityViolation);
        }
        _ => {}
    }
    let Some(result) = result else {
        return Ok((None, None));
    };
    if matches!(
        result,
        ConfigurationFollowerEnrollmentExchangeResult::Pending
    ) {
        if phase == ConfigurationFollowerEnrollmentPhase::Reserved
            || !receipt.as_ref().is_some_and(|receipt| {
                receipt.phase == ConfigurationFollowerEnrollmentRemotePhase::Pending
            })
        {
            return Err(Error::IntegrityViolation);
        }
    } else if phase != ConfigurationFollowerEnrollmentPhase::Retired {
        return Err(Error::IntegrityViolation);
    }
    if let Some(receipt) = &receipt {
        let outcome_matches = match result {
            ConfigurationFollowerEnrollmentExchangeResult::Pending => {
                receipt.phase == ConfigurationFollowerEnrollmentRemotePhase::Pending
                    && receipt.grant.is_none()
            }
            ConfigurationFollowerEnrollmentExchangeResult::Approved => {
                receipt.phase == ConfigurationFollowerEnrollmentRemotePhase::Approved
                    && receipt.grant.as_ref().is_some_and(|grant| !grant.revoked)
            }
            ConfigurationFollowerEnrollmentExchangeResult::Rejected => {
                receipt.phase == ConfigurationFollowerEnrollmentRemotePhase::Rejected
                    && receipt.grant.is_none()
            }
            ConfigurationFollowerEnrollmentExchangeResult::Revoked => {
                receipt.phase == ConfigurationFollowerEnrollmentRemotePhase::Approved
                    && receipt.grant.as_ref().is_some_and(|grant| grant.revoked)
            }
            ConfigurationFollowerEnrollmentExchangeResult::RoleConflict => true,
        };
        if !outcome_matches {
            return Err(Error::IntegrityViolation);
        }
    }
    Ok((Some(result), receipt))
}

fn parse_exchange_result(
    result: &str,
) -> Result<ConfigurationFollowerEnrollmentExchangeResult, Error> {
    match result {
        "pending" => Ok(ConfigurationFollowerEnrollmentExchangeResult::Pending),
        "approved" => Ok(ConfigurationFollowerEnrollmentExchangeResult::Approved),
        "rejected" => Ok(ConfigurationFollowerEnrollmentExchangeResult::Rejected),
        "revoked" => Ok(ConfigurationFollowerEnrollmentExchangeResult::Revoked),
        "role_conflict" => Ok(ConfigurationFollowerEnrollmentExchangeResult::RoleConflict),
        _ => Err(Error::IntegrityViolation),
    }
}

async fn retire_enrollment(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    attempt_id: &ConfigurationReadGrantAttemptId,
) -> Result<(), Error> {
    let credential_state: Option<String> = sqlx::query_scalar(
        "SELECT state FROM configuration_follower_enrollment_credentials WHERE attempt_id = ?",
    )
    .bind(attempt_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| Error::Unavailable)?;
    match credential_state.as_deref() {
        Some("retired") => {}
        Some("pending" | "active") => {
            let result = sqlx::query(
                "UPDATE configuration_follower_enrollment_credentials SET state = 'retired' WHERE attempt_id = ? AND state != 'retired'",
            )
            .bind(attempt_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(|_| Error::Unavailable)?;
            if result.rows_affected() != 1 {
                return Err(Error::IntegrityViolation);
            }
        }
        _ => return Err(Error::IntegrityViolation),
    }

    let phase: Option<String> = sqlx::query_scalar(
        "SELECT phase FROM configuration_follower_enrollment_lifecycle WHERE attempt_id = ?",
    )
    .bind(attempt_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| Error::Unavailable)?;
    match phase.as_deref() {
        Some("retired") => Ok(()),
        Some("reserved" | "prepared") => retire_lifecycle_only(transaction, attempt_id).await,
        _ => Err(Error::IntegrityViolation),
    }
}

async fn retire_lifecycle_only(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    attempt_id: &ConfigurationReadGrantAttemptId,
) -> Result<(), Error> {
    let result = sqlx::query(
        "UPDATE configuration_follower_enrollment_lifecycle SET phase = 'retired' WHERE attempt_id = ? AND phase != 'retired'",
    )
    .bind(attempt_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(|_| Error::Unavailable)?;
    if result.rows_affected() != 1 {
        return Err(Error::IntegrityViolation);
    }
    Ok(())
}

async fn activate_follower_credential(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    attempt_id: &ConfigurationReadGrantAttemptId,
) -> Result<(), Error> {
    let result = sqlx::query(
        "UPDATE configuration_follower_enrollment_credentials SET state = 'active' WHERE attempt_id = ? AND state = 'pending'",
    )
    .bind(attempt_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(|_| Error::Unavailable)?;
    if result.rows_affected() != 1 {
        return Err(Error::IntegrityViolation);
    }
    Ok(())
}

async fn store_observation(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    attempt_id: &ConfigurationReadGrantAttemptId,
    result: ConfigurationFollowerEnrollmentExchangeResult,
    receipt: Option<&ConfigurationFollowerEnrollmentRemoteReceipt>,
) -> Result<(), Error> {
    let existing: Option<String> = sqlx::query_scalar(
        "SELECT receipt_json FROM configuration_follower_enrollment_observations WHERE attempt_id = ?",
    )
    .bind(attempt_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| Error::Unavailable)?
    .flatten();
    let receipt_json = match receipt {
        Some(receipt) => {
            Some(serde_json::to_string(receipt).map_err(|_| Error::IntegrityViolation)?)
        }
        None => existing,
    };
    if receipt_json
        .as_ref()
        .is_some_and(|value| value.len() > MAX_RECEIPT_BYTES)
    {
        return Err(Error::IntegrityViolation);
    }
    if !matches!(
        result,
        ConfigurationFollowerEnrollmentExchangeResult::RoleConflict
    ) && receipt_json.is_none()
    {
        return Err(Error::IntegrityViolation);
    }
    sqlx::query(
        "INSERT INTO configuration_follower_enrollment_observations (attempt_id, result, receipt_json) VALUES (?, ?, ?) ON CONFLICT(attempt_id) DO UPDATE SET result = excluded.result, receipt_json = excluded.receipt_json",
    )
    .bind(attempt_id.as_str())
    .bind(exchange_result_label(result))
    .bind(receipt_json)
    .execute(&mut **transaction)
    .await
    .map_err(|_| Error::IntegrityViolation)?;
    Ok(())
}

fn exchange_result_label(result: ConfigurationFollowerEnrollmentExchangeResult) -> &'static str {
    match result {
        ConfigurationFollowerEnrollmentExchangeResult::Pending => "pending",
        ConfigurationFollowerEnrollmentExchangeResult::Approved => "approved",
        ConfigurationFollowerEnrollmentExchangeResult::Rejected => "rejected",
        ConfigurationFollowerEnrollmentExchangeResult::Revoked => "revoked",
        ConfigurationFollowerEnrollmentExchangeResult::RoleConflict => "role_conflict",
    }
}

fn valid_exchange_settings(settings: &ConfigurationFollowerEnrollmentExchangeSettings) -> bool {
    if settings.origin.is_empty()
        || settings
            .origin
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        || settings.connect_timeout_ms == 0
        || settings.request_timeout_ms == 0
        || settings.connect_timeout_ms > settings.request_timeout_ms
    {
        return false;
    }
    std::time::Instant::now()
        .checked_add(std::time::Duration::from_millis(
            settings.request_timeout_ms,
        ))
        .is_some()
}

fn submission_from_request(request: &StoredRequest) -> ConfigurationFollowerEnrollmentSubmission {
    ConfigurationFollowerEnrollmentSubmission::new(
        request.attempt_id.clone(),
        request.follower_instance_id.clone(),
        request.expected_state_version,
        request.authority.clone(),
        request.server_name.clone(),
        request.certificate_authority_der.clone(),
        request.certificate_authority_fingerprint.clone(),
        request.credential_digest.clone(),
    )
}

fn valid_remote_receipt(
    request: &StoredRequest,
    receipt: &ConfigurationFollowerEnrollmentRemoteReceipt,
) -> bool {
    if !valid_remote_receipt_binding(
        receipt,
        &request.attempt_id,
        &request.follower_instance_id,
        request.expected_state_version,
        &request.authority,
        &request.server_name,
        &request.certificate_authority_fingerprint,
    ) {
        return false;
    }
    receipt.credential_fingerprint == enrollment_confirmation_fingerprint(request, receipt)
}

fn enrollment_confirmation_fingerprint(
    request: &StoredRequest,
    receipt: &ConfigurationFollowerEnrollmentRemoteReceipt,
) -> String {
    let mut hash = Sha256::new();
    hash.update(b"kiln configuration follower enrollment confirmation v1\0");
    for value in [
        receipt.request_id.as_str(),
        request.attempt_id.as_str(),
        request.follower_instance_id.as_str(),
        request.authority.group_id().as_str(),
        request.authority.master_id().as_str(),
        request.server_name.as_str(),
        request.certificate_authority_fingerprint.as_str(),
        request.credential_digest.as_str(),
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hash.update(request.expected_state_version.to_be_bytes());
    hash.update(receipt.received_master_state_version.to_be_bytes());
    lower_hex(&hash.finalize())
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(HEX[(byte >> 4) as usize] as char);
        value.push(HEX[(byte & 0x0f) as usize] as char);
    }
    value
}

fn valid_remote_receipt_binding(
    receipt: &ConfigurationFollowerEnrollmentRemoteReceipt,
    attempt_id: &ConfigurationReadGrantAttemptId,
    follower_instance_id: &KilnInstanceId,
    follower_state_version: u64,
    authority: &ConfigurationAuthority,
    server_name: &str,
    ca_fingerprint: &ContentHash,
) -> bool {
    let valid_hex_32 = |value: &str| {
        value.len() == 32
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    };
    let valid_fingerprint = |value: &str| {
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    };
    if !receipt.request_id.starts_with("cfr_")
        || !valid_hex_32(&receipt.request_id[4..])
        || receipt.attempt_id != attempt_id.as_str()
        || receipt.follower_id != follower_instance_id.as_str()
        || receipt.follower_state_version != follower_state_version
        || receipt.group_id != authority.group_id().as_str()
        || receipt.master_instance_id != authority.master_id().as_str()
        || receipt.server_name != server_name
        || receipt.master_ca_fingerprint != ca_fingerprint.as_str()
        || !valid_fingerprint(&receipt.credential_fingerprint)
        || receipt.received_master_state_version == 0
        || receipt.received_master_state_version > i64::MAX as u64
    {
        return false;
    }
    match (receipt.phase, receipt.grant.as_ref()) {
        (ConfigurationFollowerEnrollmentRemotePhase::Approved, Some(grant)) => {
            grant.grant_id.starts_with("crg_")
                && valid_hex_32(&grant.grant_id[4..])
                && grant.issuance_attempt_id.as_deref() == Some(attempt_id.as_str())
                && grant.group_id == authority.group_id().as_str()
                && grant.master_instance_id == authority.master_id().as_str()
                && grant.follower_instance_id == follower_instance_id.as_str()
                && grant.issued_state_version > 0
                && grant.issued_state_version <= i64::MAX as u64
        }
        (
            ConfigurationFollowerEnrollmentRemotePhase::Pending
            | ConfigurationFollowerEnrollmentRemotePhase::Rejected,
            None,
        ) => true,
        _ => false,
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
