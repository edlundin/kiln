use super::{SqliteStore, configuration_snapshot, configuration_sync, hash_bytes};
use kiln_core::{
    ConfigurationAccessError as Error, ConfigurationAccessStore, ConfigurationAuthority,
    ConfigurationCredentialDigest, ConfigurationFollowerEnrollmentRequest,
    ConfigurationFollowerEnrollmentRequestConfirmation, ConfigurationFollowerEnrollmentRequestId,
    ConfigurationFollowerEnrollmentRequestPhase, ConfigurationFollowerEnrollmentRequestSubmission,
    ConfigurationFollowerServingIdentity, ConfigurationGroupId, ConfigurationInstanceState,
    ConfigurationReadGrant, ConfigurationReadGrantAttemptId, ConfigurationReadGrantId,
    ConfigurationReadGrantSummary, ConfigurationRole, ConfigurationSnapshotReadLimits,
    ConfigurationStateError, ContentHash, KilnInstanceId, StoredConfigurationSnapshot,
};
use sha2::{Digest as _, Sha256};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteRow};
use std::num::NonZeroU32;

const MAX_ENROLLMENT_REQUEST_PAGE_SIZE: usize = 101;

macro_rules! enrollment_request_select {
    ($suffix:literal) => {
        concat!(
            "SELECT r.request_id, r.attempt_id, r.follower_instance_id, r.follower_state_version, r.group_id, r.master_instance_id, r.server_name, r.master_ca_fingerprint, r.credential_digest, r.received_master_state_version, l.phase AS request_phase, l.grant_id AS request_grant_id, g.grant_id AS issued_grant_id, g.credential_digest AS issued_credential_digest, g.issuance_attempt_id AS issued_attempt_id, g.group_id AS issued_group_id, g.master_instance_id AS issued_master_instance_id, g.follower_instance_id AS issued_follower_instance_id, g.issued_state_version AS issued_state_version, g.revoked AS issued_revoked FROM configuration_master_enrollment_requests r JOIN configuration_follower_enrollment_request_lifecycle l USING (request_id) LEFT JOIN configuration_read_grants g ON g.grant_id = l.grant_id",
            $suffix
        )
    };
}

impl ConfigurationAccessStore for SqliteStore {
    async fn submit_configuration_follower_enrollment_request(
        &self,
        serving_identity: &ConfigurationFollowerServingIdentity,
        max_retained_requests_per_authority: NonZeroU32,
        submission: &ConfigurationFollowerEnrollmentRequestSubmission,
        proposed_request_id: &ConfigurationFollowerEnrollmentRequestId,
    ) -> Result<ConfigurationFollowerEnrollmentRequest, Error> {
        let follower_state_version = i64::try_from(submission.follower_state_version)
            .ok()
            .filter(|version| *version > 0)
            .ok_or(Error::InvalidRequest)?;
        if !super::configuration_identity::valid_server_name(&submission.server_name) {
            return Err(Error::InvalidRequest);
        }
        let retention_limit = i64::from(max_retained_requests_per_authority.get());
        if !super::configuration_identity::valid_server_name(&serving_identity.server_name) {
            return Err(Error::InvalidRequest);
        }
        if submission.follower_id == *serving_identity.authority.master_id()
            || submission.authority != serving_identity.authority
            || submission.server_name != serving_identity.server_name
            || submission.master_ca_fingerprint != serving_identity.master_ca_fingerprint
        {
            return Err(Error::Conflict);
        }

        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;

        let current = configuration_sync::load(&mut transaction)
            .await
            .map_err(Error::State)?
            .ok_or(Error::Conflict)?;
        if current.instance_id() != serving_identity.authority.master_id()
            || !matches!(current.role(), ConfigurationRole::Master(authority) if authority == &serving_identity.authority)
        {
            return Err(Error::Conflict);
        }
        require_active_serving_identity_binding(&mut transaction, serving_identity).await?;

        if let Some(existing) =
            load_enrollment_request_by_attempt(&mut transaction, &submission.attempt_id).await?
        {
            if !existing.matches_submission(submission) {
                return Err(Error::IdempotencyConflict);
            }
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(existing.metadata);
        }

        let retained_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM configuration_master_enrollment_requests WHERE group_id = ? AND master_instance_id = ?",
        )
        .bind(serving_identity.authority.group_id().as_str())
        .bind(serving_identity.authority.master_id().as_str())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| Error::Unavailable)?;
        if retained_count >= retention_limit {
            return Err(Error::RetentionLimitReached);
        }

        sqlx::query(
            "INSERT INTO configuration_master_enrollment_requests (request_id, attempt_id, follower_instance_id, follower_state_version, group_id, master_instance_id, server_name, master_ca_fingerprint, credential_digest, received_master_state_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(proposed_request_id.as_str())
        .bind(submission.attempt_id.as_str())
        .bind(submission.follower_id.as_str())
        .bind(follower_state_version)
        .bind(submission.authority.group_id().as_str())
        .bind(submission.authority.master_id().as_str())
        .bind(&submission.server_name)
        .bind(submission.master_ca_fingerprint.as_str())
        .bind(submission.credential_digest.as_str())
        .bind(i64::try_from(current.version()).map_err(|_| Error::IntegrityViolation)?)
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            if error
                .as_database_error()
                .is_some_and(|database| database.is_unique_violation())
            {
                Error::Conflict
            } else {
                Error::Unavailable
            }
        })?;
        sqlx::query(
            "INSERT INTO configuration_follower_enrollment_request_lifecycle (request_id, phase, grant_id) VALUES (?, 'pending', NULL)",
        )
        .bind(proposed_request_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| Error::Unavailable)?;

        let request = load_enrollment_request_by_id(&mut transaction, proposed_request_id)
            .await?
            .ok_or(Error::IntegrityViolation)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(request.metadata)
    }

    async fn get_configuration_follower_enrollment_request(
        &self,
        request_id: &ConfigurationFollowerEnrollmentRequestId,
    ) -> Result<Option<ConfigurationFollowerEnrollmentRequest>, Error> {
        let mut connection = self.connection.lock().await;
        load_enrollment_request_by_id(&mut connection, request_id)
            .await
            .map(|request| request.map(|request| request.metadata))
    }

    async fn get_configuration_follower_enrollment_request_by_attempt(
        &self,
        attempt_id: &ConfigurationReadGrantAttemptId,
    ) -> Result<Option<ConfigurationFollowerEnrollmentRequest>, Error> {
        let mut connection = self.connection.lock().await;
        load_enrollment_request_by_attempt(&mut connection, attempt_id)
            .await
            .map(|request| request.map(|request| request.metadata))
    }

    async fn list_configuration_follower_enrollment_requests(
        &self,
        after: Option<&ConfigurationFollowerEnrollmentRequestId>,
        limit: usize,
    ) -> Result<Vec<ConfigurationFollowerEnrollmentRequest>, Error> {
        if !(1..=MAX_ENROLLMENT_REQUEST_PAGE_SIZE).contains(&limit) {
            return Err(Error::InvalidRequest);
        }
        let mut connection = self.connection.lock().await;
        let rows = sqlx::query(enrollment_request_select!(
            " WHERE (? IS NULL OR r.request_id > ?) ORDER BY r.request_id LIMIT ?"
        ))
        .bind(after.map(ConfigurationFollowerEnrollmentRequestId::as_str))
        .bind(after.map(ConfigurationFollowerEnrollmentRequestId::as_str))
        .bind(limit as i64)
        .fetch_all(&mut *connection)
        .await
        .map_err(|_| Error::Unavailable)?;
        rows.into_iter()
            .map(decode_enrollment_request)
            .map(|result| result.map(|request| request.metadata))
            .collect()
    }

    async fn approve_configuration_follower_enrollment_request(
        &self,
        expected: &ConfigurationInstanceState,
        confirmation: &ConfigurationFollowerEnrollmentRequestConfirmation,
        proposed_grant_id: &ConfigurationReadGrantId,
    ) -> Result<ConfigurationFollowerEnrollmentRequest, Error> {
        let authority = registration_authority(expected, &confirmation.follower_id)?;
        if authority != &confirmation.authority {
            return Err(Error::Conflict);
        }

        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        require_current(&mut transaction, expected).await?;
        let request = load_enrollment_request_by_id(&mut transaction, &confirmation.request_id)
            .await?
            .ok_or(Error::Denied)?;
        if !request.matches_confirmation(confirmation) {
            return Err(Error::IdempotencyConflict);
        }

        match request.metadata.phase {
            ConfigurationFollowerEnrollmentRequestPhase::Approved => {
                transaction.commit().await.map_err(|_| Error::Unavailable)?;
                return Ok(request.metadata);
            }
            ConfigurationFollowerEnrollmentRequestPhase::Rejected => {
                return Err(Error::Conflict);
            }
            ConfigurationFollowerEnrollmentRequestPhase::Pending => {}
        }

        require_active_identity_binding(
            &mut transaction,
            &request.metadata.authority,
            &request.metadata.server_name,
            &request.metadata.master_ca_fingerprint,
        )
        .await?;

        if active_grants_for_follower(
            &mut transaction,
            &request.metadata.authority,
            &request.metadata.follower_id,
        )
        .await?
            != 0
        {
            return Err(Error::CredentialConflict);
        }

        sqlx::query("INSERT INTO configuration_read_grants (credential_digest, group_id, master_instance_id, follower_instance_id, issued_state_version, grant_id, issuance_attempt_id) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(request.credential_digest.as_str())
            .bind(request.metadata.authority.group_id().as_str())
            .bind(request.metadata.authority.master_id().as_str())
            .bind(request.metadata.follower_id.as_str())
            .bind(expected.version() as i64)
            .bind(proposed_grant_id.as_str())
            .bind(request.metadata.attempt_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                if error
                    .as_database_error()
                    .is_some_and(|database| database.is_unique_violation())
                {
                    Error::CredentialConflict
                } else {
                    Error::Unavailable
                }
            })?;
        let updated = sqlx::query(
            "UPDATE configuration_follower_enrollment_request_lifecycle SET phase = 'approved', grant_id = ? WHERE request_id = ? AND phase = 'pending' AND grant_id IS NULL",
        )
        .bind(proposed_grant_id.as_str())
        .bind(confirmation.request_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| Error::Unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(Error::IntegrityViolation);
        }

        let approved = load_enrollment_request_by_id(&mut transaction, &confirmation.request_id)
            .await?
            .ok_or(Error::IntegrityViolation)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(approved.metadata)
    }

    async fn reject_configuration_follower_enrollment_request(
        &self,
        expected: &ConfigurationInstanceState,
        confirmation: &ConfigurationFollowerEnrollmentRequestConfirmation,
    ) -> Result<ConfigurationFollowerEnrollmentRequest, Error> {
        let authority = registration_authority(expected, &confirmation.follower_id)?;
        if authority != &confirmation.authority {
            return Err(Error::Conflict);
        }

        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        require_current(&mut transaction, expected).await?;
        let request = load_enrollment_request_by_id(&mut transaction, &confirmation.request_id)
            .await?
            .ok_or(Error::Denied)?;
        if !request.matches_confirmation(confirmation) {
            return Err(Error::IdempotencyConflict);
        }

        match request.metadata.phase {
            ConfigurationFollowerEnrollmentRequestPhase::Rejected => {
                transaction.commit().await.map_err(|_| Error::Unavailable)?;
                return Ok(request.metadata);
            }
            ConfigurationFollowerEnrollmentRequestPhase::Approved => {
                return Err(Error::Conflict);
            }
            ConfigurationFollowerEnrollmentRequestPhase::Pending => {}
        }

        let updated = sqlx::query(
            "UPDATE configuration_follower_enrollment_request_lifecycle SET phase = 'rejected' WHERE request_id = ? AND phase = 'pending' AND grant_id IS NULL",
        )
        .bind(confirmation.request_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| Error::Unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(Error::IntegrityViolation);
        }

        let rejected = load_enrollment_request_by_id(&mut transaction, &confirmation.request_id)
            .await?
            .ok_or(Error::IntegrityViolation)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(rejected.metadata)
    }

    async fn get_configuration_read_grant(
        &self,
        grant_id: &ConfigurationReadGrantId,
    ) -> Result<Option<ConfigurationReadGrant>, Error> {
        let mut connection = self.connection.lock().await;
        load_by_id(&mut connection, grant_id).await
    }

    async fn get_configuration_read_grant_by_attempt(
        &self,
        attempt_id: &ConfigurationReadGrantAttemptId,
    ) -> Result<Option<ConfigurationReadGrant>, Error> {
        let mut connection = self.connection.lock().await;
        load_by_attempt(&mut connection, attempt_id).await
    }

    async fn list_configuration_read_grants(
        &self,
        after: Option<&ConfigurationReadGrantId>,
        limit: usize,
    ) -> Result<Vec<ConfigurationReadGrant>, Error> {
        if !(1..=101).contains(&limit) {
            return Err(Error::InvalidRequest);
        }
        let mut connection = self.connection.lock().await;
        let rows = if let Some(after) = after {
            sqlx::query("SELECT g.grant_id, g.issuance_attempt_id, g.group_id, g.master_instance_id, g.follower_instance_id, g.credential_digest, g.issued_state_version, g.revoked, a.master_instance_id AS authority_master FROM configuration_read_grants g JOIN configuration_authorities a ON a.group_id = g.group_id WHERE g.grant_id > ? ORDER BY g.grant_id LIMIT ?")
                .bind(after.as_str())
                .bind(limit as i64)
                .fetch_all(&mut *connection)
                .await
                .map_err(|_| Error::Unavailable)?
        } else {
            sqlx::query("SELECT g.grant_id, g.issuance_attempt_id, g.group_id, g.master_instance_id, g.follower_instance_id, g.credential_digest, g.issued_state_version, g.revoked, a.master_instance_id AS authority_master FROM configuration_read_grants g JOIN configuration_authorities a ON a.group_id = g.group_id ORDER BY g.grant_id LIMIT ?")
                .bind(limit as i64)
                .fetch_all(&mut *connection)
                .await
                .map_err(|_| Error::Unavailable)?
        };
        rows.into_iter().map(decode_grant).collect()
    }

    async fn revoke_configuration_reader(
        &self,
        expected: &ConfigurationInstanceState,
        grant_id: &ConfigurationReadGrantId,
    ) -> Result<(), Error> {
        let authority = match expected.role() {
            ConfigurationRole::Master(authority) => authority,
            _ => return Err(Error::InvalidRequest),
        };
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        require_current(&mut transaction, expected).await?;
        let grant = load_by_id(&mut transaction, grant_id)
            .await?
            .ok_or(Error::Denied)?;
        if grant.authority != *authority || grant.follower_id == *authority.master_id() {
            return Err(Error::Denied);
        }
        if !grant.revoked {
            sqlx::query("UPDATE configuration_read_grants SET revoked = 1 WHERE grant_id = ? AND revoked = 0")
                .bind(grant_id.as_str())
                .execute(&mut *transaction)
                .await
                .map_err(|_| Error::Unavailable)?;
        }
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(())
    }

    async fn read_configuration_for_follower(
        &self,
        serving_identity: &ConfigurationFollowerServingIdentity,
        follower: &KilnInstanceId,
        digest: &ConfigurationCredentialDigest,
        limits: ConfigurationSnapshotReadLimits,
    ) -> Result<Option<StoredConfigurationSnapshot>, Error> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection.begin().await.map_err(|_| Error::Unavailable)?;
        let state = configuration_sync::load(&mut transaction)
            .await
            .map_err(Error::State)?
            .ok_or(Error::Denied)?;
        if !matches!(state.role(), ConfigurationRole::Master(current) if current == &serving_identity.authority)
            || state.instance_id() == follower
        {
            return Err(Error::Denied);
        }

        match require_active_serving_identity_binding(&mut transaction, serving_identity).await {
            Ok(()) => {}
            Err(Error::Conflict) => return Err(Error::Denied),
            Err(error) => return Err(error),
        }

        let grant = load_by_digest(&mut transaction, digest)
            .await?
            .ok_or(Error::Denied)?;
        if grant.revoked
            || grant.authority != serving_identity.authority
            || grant.follower_id != *follower
        {
            return Err(Error::Denied);
        }

        let Some(attempt_id) = grant.issuance_attempt_id.as_ref() else {
            return Err(Error::Denied);
        };
        let Some(origin) = load_enrollment_request_by_attempt(&mut transaction, attempt_id).await?
        else {
            return Err(Error::Denied);
        };
        let Some(issued_grant) = origin.metadata.grant.as_ref() else {
            return Err(Error::Denied);
        };
        if origin.metadata.phase != ConfigurationFollowerEnrollmentRequestPhase::Approved
            || origin.metadata.attempt_id != *attempt_id
            || issued_grant.grant_id != grant.grant_id
            || issued_grant.issuance_attempt_id.as_ref() != Some(attempt_id)
            || issued_grant.authority != grant.authority
            || issued_grant.follower_id != grant.follower_id
            || issued_grant.issued_state_version != grant.issued_state_version
            || issued_grant.revoked != grant.revoked
            || origin.credential_digest != grant.credential_digest
            || origin.metadata.authority != serving_identity.authority
            || origin.metadata.follower_id != *follower
            || origin.metadata.server_name != serving_identity.server_name
            || origin.metadata.master_ca_fingerprint != serving_identity.master_ca_fingerprint
        {
            return Err(Error::Denied);
        }

        // Authorization and snapshot acquisition share the transaction and store
        // lock. No reusable authorization proof escapes this boundary.
        let snapshot =
            configuration_snapshot::read_current_snapshot(&mut transaction, state, limits)
                .await
                .map_err(Error::Snapshot)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(snapshot)
    }
}

struct StoredEnrollmentRequest {
    metadata: ConfigurationFollowerEnrollmentRequest,
    credential_digest: ConfigurationCredentialDigest,
}

impl StoredEnrollmentRequest {
    fn matches_submission(
        &self,
        submission: &ConfigurationFollowerEnrollmentRequestSubmission,
    ) -> bool {
        self.metadata.attempt_id == submission.attempt_id
            && self.metadata.follower_id == submission.follower_id
            && self.metadata.follower_state_version == submission.follower_state_version
            && self.metadata.authority == submission.authority
            && self.metadata.server_name == submission.server_name
            && self.metadata.master_ca_fingerprint == submission.master_ca_fingerprint
            && self.credential_digest == submission.credential_digest
    }

    fn matches_confirmation(
        &self,
        confirmation: &ConfigurationFollowerEnrollmentRequestConfirmation,
    ) -> bool {
        self.metadata.request_id == confirmation.request_id
            && self.metadata.attempt_id == confirmation.attempt_id
            && self.metadata.follower_id == confirmation.follower_id
            && self.metadata.follower_state_version == confirmation.follower_state_version
            && self.metadata.authority == confirmation.authority
            && self.metadata.server_name == confirmation.server_name
            && self.metadata.master_ca_fingerprint == confirmation.master_ca_fingerprint
            && self.metadata.received_master_state_version
                == confirmation.received_master_state_version
            && self.metadata.credential_fingerprint == confirmation.credential_fingerprint
    }
}

async fn load_enrollment_request_by_id(
    connection: &mut SqliteConnection,
    request_id: &ConfigurationFollowerEnrollmentRequestId,
) -> Result<Option<StoredEnrollmentRequest>, Error> {
    let row = sqlx::query(enrollment_request_select!(" WHERE r.request_id = ?"))
        .bind(request_id.as_str())
        .fetch_optional(connection)
        .await
        .map_err(|_| Error::Unavailable)?;
    row.map(decode_enrollment_request).transpose()
}

async fn load_enrollment_request_by_attempt(
    connection: &mut SqliteConnection,
    attempt_id: &ConfigurationReadGrantAttemptId,
) -> Result<Option<StoredEnrollmentRequest>, Error> {
    let row = sqlx::query(enrollment_request_select!(" WHERE r.attempt_id = ?"))
        .bind(attempt_id.as_str())
        .fetch_optional(connection)
        .await
        .map_err(|_| Error::Unavailable)?;
    row.map(decode_enrollment_request).transpose()
}

fn decode_enrollment_request(row: SqliteRow) -> Result<StoredEnrollmentRequest, Error> {
    let request_id = ConfigurationFollowerEnrollmentRequestId::parse(
        row.try_get::<String, _>("request_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let attempt_id = ConfigurationReadGrantAttemptId::parse(
        row.try_get::<String, _>("attempt_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let follower_id = KilnInstanceId::parse(
        row.try_get::<String, _>("follower_instance_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let follower_state_version = read_positive_version(&row, "follower_state_version")?;
    let group_id = ConfigurationGroupId::parse(
        row.try_get::<String, _>("group_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let master_id = KilnInstanceId::parse(
        row.try_get::<String, _>("master_instance_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    if follower_id == master_id {
        return Err(Error::IntegrityViolation);
    }
    let authority = ConfigurationAuthority::new(group_id, master_id);
    let server_name: String = row
        .try_get("server_name")
        .map_err(|_| Error::IntegrityViolation)?;
    if !super::configuration_identity::valid_server_name(&server_name) {
        return Err(Error::IntegrityViolation);
    }
    let master_ca_fingerprint = ContentHash::parse(
        row.try_get::<String, _>("master_ca_fingerprint")
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
    let received_master_state_version =
        read_positive_version(&row, "received_master_state_version")?;
    let phase = match row
        .try_get::<String, _>("request_phase")
        .map_err(|_| Error::IntegrityViolation)?
        .as_str()
    {
        "pending" => ConfigurationFollowerEnrollmentRequestPhase::Pending,
        "approved" => ConfigurationFollowerEnrollmentRequestPhase::Approved,
        "rejected" => ConfigurationFollowerEnrollmentRequestPhase::Rejected,
        _ => return Err(Error::IntegrityViolation),
    };
    let grant_id = row
        .try_get::<Option<String>, _>("request_grant_id")
        .map_err(|_| Error::IntegrityViolation)?
        .map(ConfigurationReadGrantId::parse)
        .transpose()
        .map_err(|_| Error::IntegrityViolation)?;

    let grant = match (phase, grant_id) {
        (ConfigurationFollowerEnrollmentRequestPhase::Approved, Some(grant_id)) => {
            let issued_grant_id = ConfigurationReadGrantId::parse(
                row.try_get::<String, _>("issued_grant_id")
                    .map_err(|_| Error::IntegrityViolation)?,
            )
            .map_err(|_| Error::IntegrityViolation)?;
            let issued_digest = row
                .try_get::<String, _>("issued_credential_digest")
                .map_err(|_| Error::IntegrityViolation)?;
            let issued_attempt = ConfigurationReadGrantAttemptId::parse(
                row.try_get::<String, _>("issued_attempt_id")
                    .map_err(|_| Error::IntegrityViolation)?,
            )
            .map_err(|_| Error::IntegrityViolation)?;
            let issued_group = ConfigurationGroupId::parse(
                row.try_get::<String, _>("issued_group_id")
                    .map_err(|_| Error::IntegrityViolation)?,
            )
            .map_err(|_| Error::IntegrityViolation)?;
            let issued_master = KilnInstanceId::parse(
                row.try_get::<String, _>("issued_master_instance_id")
                    .map_err(|_| Error::IntegrityViolation)?,
            )
            .map_err(|_| Error::IntegrityViolation)?;
            let issued_follower = KilnInstanceId::parse(
                row.try_get::<String, _>("issued_follower_instance_id")
                    .map_err(|_| Error::IntegrityViolation)?,
            )
            .map_err(|_| Error::IntegrityViolation)?;
            let issued_state_version = read_positive_version(&row, "issued_state_version")?;
            let issued_revoked = read_revocation_flag(&row, "issued_revoked")?;
            if grant_id != issued_grant_id
                || issued_digest != credential_digest.as_str()
                || issued_attempt != attempt_id
                || issued_group != *authority.group_id()
                || issued_master != *authority.master_id()
                || issued_follower != follower_id
            {
                return Err(Error::IntegrityViolation);
            }
            Some(ConfigurationReadGrantSummary {
                grant_id,
                issuance_attempt_id: Some(attempt_id.clone()),
                authority: authority.clone(),
                follower_id: follower_id.clone(),
                issued_state_version,
                revoked: issued_revoked,
            })
        }
        (ConfigurationFollowerEnrollmentRequestPhase::Approved, None)
        | (ConfigurationFollowerEnrollmentRequestPhase::Pending, Some(_))
        | (ConfigurationFollowerEnrollmentRequestPhase::Rejected, Some(_)) => {
            return Err(Error::IntegrityViolation);
        }
        (_, None) => None,
    };

    let credential_fingerprint = enrollment_request_fingerprint(
        &request_id,
        &attempt_id,
        &follower_id,
        follower_state_version,
        &authority,
        &server_name,
        &master_ca_fingerprint,
        received_master_state_version,
        &credential_digest,
    )?;
    Ok(StoredEnrollmentRequest {
        metadata: ConfigurationFollowerEnrollmentRequest {
            request_id,
            attempt_id,
            follower_id,
            follower_state_version,
            authority,
            server_name,
            master_ca_fingerprint,
            credential_fingerprint,
            received_master_state_version,
            phase,
            grant,
        },
        credential_digest,
    })
}

fn read_positive_version(row: &SqliteRow, column: &str) -> Result<u64, Error> {
    let value: i64 = row.try_get(column).map_err(|_| Error::IntegrityViolation)?;
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(Error::IntegrityViolation)
}

fn read_revocation_flag(row: &SqliteRow, column: &str) -> Result<bool, Error> {
    match row
        .try_get::<i64, _>(column)
        .map_err(|_| Error::IntegrityViolation)?
    {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(Error::IntegrityViolation),
    }
}

fn enrollment_request_fingerprint(
    request_id: &ConfigurationFollowerEnrollmentRequestId,
    attempt_id: &ConfigurationReadGrantAttemptId,
    follower_id: &KilnInstanceId,
    follower_state_version: u64,
    authority: &ConfigurationAuthority,
    server_name: &str,
    master_ca_fingerprint: &ContentHash,
    received_master_state_version: u64,
    credential_digest: &ConfigurationCredentialDigest,
) -> Result<ContentHash, Error> {
    let mut hash = Sha256::new();
    hash.update(b"kiln configuration follower enrollment confirmation v1\0");
    for value in [
        request_id.as_str(),
        attempt_id.as_str(),
        follower_id.as_str(),
        authority.group_id().as_str(),
        authority.master_id().as_str(),
        server_name,
        master_ca_fingerprint.as_str(),
        credential_digest.as_str(),
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hash.update(follower_state_version.to_be_bytes());
    hash.update(received_master_state_version.to_be_bytes());
    let mut encoded = String::with_capacity(64);
    for byte in hash.finalize() {
        encoded.push(b"0123456789abcdef"[(byte >> 4) as usize] as char);
        encoded.push(b"0123456789abcdef"[(byte & 0x0f) as usize] as char);
    }
    ContentHash::parse(encoded).map_err(|_| Error::IntegrityViolation)
}

fn registration_authority<'a>(
    expected: &'a ConfigurationInstanceState,
    follower: &KilnInstanceId,
) -> Result<&'a ConfigurationAuthority, Error> {
    match expected.role() {
        ConfigurationRole::Master(authority) if authority.master_id() != follower => Ok(authority),
        _ => Err(Error::InvalidRequest),
    }
}

async fn require_current(
    connection: &mut SqliteConnection,
    expected: &ConfigurationInstanceState,
) -> Result<(), Error> {
    let current = configuration_sync::load(connection)
        .await
        .map_err(Error::State)?
        .ok_or(Error::State(ConfigurationStateError::Uninitialized))?;
    if current != *expected {
        return Err(Error::Conflict);
    }
    Ok(())
}

async fn require_active_identity_binding(
    connection: &mut SqliteConnection,
    authority: &ConfigurationAuthority,
    server_name: &str,
    ca_fingerprint: &ContentHash,
) -> Result<(), Error> {
    let row = sqlx::query(
        "SELECT server_name, ca_der FROM configuration_master_identities WHERE group_id = ? AND master_instance_id = ? AND status = 'active'",
    )
    .bind(authority.group_id().as_str())
    .bind(authority.master_id().as_str())
    .fetch_optional(&mut *connection)
    .await
    .map_err(|_| Error::Unavailable)?
    .ok_or(Error::Conflict)?;
    let current_server_name: String = row
        .try_get("server_name")
        .map_err(|_| Error::IntegrityViolation)?;
    let ca_der: Vec<u8> = row
        .try_get("ca_der")
        .map_err(|_| Error::IntegrityViolation)?;
    if ca_der.is_empty() {
        return Err(Error::IntegrityViolation);
    }
    if current_server_name != server_name || hash_bytes(&ca_der) != *ca_fingerprint {
        return Err(Error::Conflict);
    }
    Ok(())
}

async fn require_active_serving_identity_binding(
    connection: &mut SqliteConnection,
    serving_identity: &ConfigurationFollowerServingIdentity,
) -> Result<(), Error> {
    let row = sqlx::query(
        "SELECT server_name, ca_der, not_before, leaf_not_after, ca_not_after, unixepoch() AS current_time FROM configuration_master_identities WHERE identity_id = ? AND group_id = ? AND master_instance_id = ? AND status = 'active'",
    )
    .bind(serving_identity.identity_id.as_str())
    .bind(serving_identity.authority.group_id().as_str())
    .bind(serving_identity.authority.master_id().as_str())
    .fetch_optional(&mut *connection)
    .await
    .map_err(|_| Error::Unavailable)?
    .ok_or(Error::Conflict)?;
    let current_server_name: String = row
        .try_get("server_name")
        .map_err(|_| Error::IntegrityViolation)?;
    let ca_der: Vec<u8> = row
        .try_get("ca_der")
        .map_err(|_| Error::IntegrityViolation)?;
    let not_before: i64 = row
        .try_get("not_before")
        .map_err(|_| Error::IntegrityViolation)?;
    let leaf_not_after: i64 = row
        .try_get("leaf_not_after")
        .map_err(|_| Error::IntegrityViolation)?;
    let ca_not_after: i64 = row
        .try_get("ca_not_after")
        .map_err(|_| Error::IntegrityViolation)?;
    let current_time: i64 = row
        .try_get("current_time")
        .map_err(|_| Error::IntegrityViolation)?;
    if ca_der.is_empty() {
        return Err(Error::IntegrityViolation);
    }
    if current_server_name != serving_identity.server_name
        || hash_bytes(&ca_der) != serving_identity.master_ca_fingerprint
        || not_before > current_time
        || leaf_not_after <= current_time
        || ca_not_after <= current_time
    {
        return Err(Error::Conflict);
    }
    Ok(())
}

async fn active_grants_for_follower(
    connection: &mut SqliteConnection,
    authority: &ConfigurationAuthority,
    follower: &KilnInstanceId,
) -> Result<i64, Error> {
    sqlx::query_scalar("SELECT count(*) FROM configuration_read_grants WHERE group_id = ? AND master_instance_id = ? AND follower_instance_id = ? AND revoked = 0")
        .bind(authority.group_id().as_str())
        .bind(authority.master_id().as_str())
        .bind(follower.as_str())
        .fetch_one(connection)
        .await
        .map_err(|_| Error::Unavailable)
}

async fn load_by_digest(
    connection: &mut SqliteConnection,
    digest: &ConfigurationCredentialDigest,
) -> Result<Option<ConfigurationReadGrant>, Error> {
    let row = sqlx::query("SELECT g.grant_id, g.issuance_attempt_id, g.group_id, g.master_instance_id, g.follower_instance_id, g.credential_digest, g.issued_state_version, g.revoked, a.master_instance_id AS authority_master FROM configuration_read_grants g JOIN configuration_authorities a ON a.group_id = g.group_id WHERE g.credential_digest = ?")
        .bind(digest.as_str())
        .fetch_optional(connection)
        .await
        .map_err(|_| Error::Unavailable)?;
    row.map(decode_grant).transpose()
}

async fn load_by_id(
    connection: &mut SqliteConnection,
    grant_id: &ConfigurationReadGrantId,
) -> Result<Option<ConfigurationReadGrant>, Error> {
    let row = sqlx::query("SELECT g.grant_id, g.issuance_attempt_id, g.group_id, g.master_instance_id, g.follower_instance_id, g.credential_digest, g.issued_state_version, g.revoked, a.master_instance_id AS authority_master FROM configuration_read_grants g JOIN configuration_authorities a ON a.group_id = g.group_id WHERE g.grant_id = ?")
        .bind(grant_id.as_str())
        .fetch_optional(connection)
        .await
        .map_err(|_| Error::Unavailable)?;
    row.map(decode_grant).transpose()
}

async fn load_by_attempt(
    connection: &mut SqliteConnection,
    attempt_id: &ConfigurationReadGrantAttemptId,
) -> Result<Option<ConfigurationReadGrant>, Error> {
    let row = sqlx::query("SELECT g.grant_id, g.issuance_attempt_id, g.group_id, g.master_instance_id, g.follower_instance_id, g.credential_digest, g.issued_state_version, g.revoked, a.master_instance_id AS authority_master FROM configuration_read_grants g JOIN configuration_authorities a ON a.group_id = g.group_id WHERE g.issuance_attempt_id = ?")
        .bind(attempt_id.as_str())
        .fetch_optional(connection)
        .await
        .map_err(|_| Error::Unavailable)?;
    row.map(decode_grant).transpose()
}

fn decode_grant(row: SqliteRow) -> Result<ConfigurationReadGrant, Error> {
    let group = ConfigurationGroupId::parse(
        row.try_get::<String, _>("group_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let master = KilnInstanceId::parse(
        row.try_get::<String, _>("master_instance_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let follower = KilnInstanceId::parse(
        row.try_get::<String, _>("follower_instance_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let authority_master: String = row
        .try_get("authority_master")
        .map_err(|_| Error::IntegrityViolation)?;
    let version: i64 = row
        .try_get("issued_state_version")
        .map_err(|_| Error::IntegrityViolation)?;
    let revoked: i64 = row
        .try_get("revoked")
        .map_err(|_| Error::IntegrityViolation)?;
    let attempt = row
        .try_get::<Option<String>, _>("issuance_attempt_id")
        .map_err(|_| Error::IntegrityViolation)?
        .map(ConfigurationReadGrantAttemptId::parse)
        .transpose()
        .map_err(|_| Error::IntegrityViolation)?;
    let grant_id = ConfigurationReadGrantId::parse(
        row.try_get::<String, _>("grant_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let digest = ContentHash::parse(
        row.try_get::<String, _>("credential_digest")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map(ConfigurationCredentialDigest::from_sha256)
    .map_err(|_| Error::IntegrityViolation)?;
    if master.as_str() != authority_master
        || follower == master
        || version <= 0
        || !matches!(revoked, 0 | 1)
    {
        return Err(Error::IntegrityViolation);
    }
    Ok(ConfigurationReadGrant {
        grant_id,
        issuance_attempt_id: attempt,
        authority: ConfigurationAuthority::new(group, master),
        follower_id: follower,
        credential_digest: digest,
        issued_state_version: version as u64,
        revoked: revoked == 1,
    })
}
