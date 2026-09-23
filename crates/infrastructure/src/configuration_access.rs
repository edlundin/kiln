use super::{SqliteStore, configuration_snapshot, configuration_sync};
use kiln_core::{
    ConfigurationAccessError as Error, ConfigurationAccessStore, ConfigurationAuthority,
    ConfigurationCredentialDigest, ConfigurationGroupId, ConfigurationInstanceState,
    ConfigurationReadGrant, ConfigurationReadGrantAttemptId, ConfigurationReadGrantId,
    ConfigurationRole, ConfigurationSnapshotReadLimits, ConfigurationStateError, ContentHash,
    KilnInstanceId, StoredConfigurationSnapshot,
};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteRow};

impl ConfigurationAccessStore for SqliteStore {
    async fn register_configuration_reader(
        &self,
        expected: &ConfigurationInstanceState,
        follower: &KilnInstanceId,
        attempt_id: &ConfigurationReadGrantAttemptId,
        proposed_grant_id: &ConfigurationReadGrantId,
        digest: &ConfigurationCredentialDigest,
    ) -> Result<ConfigurationReadGrant, Error> {
        let authority = registration_authority(expected, follower)?;
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;

        if let Some(grant) = load_by_attempt(&mut transaction, attempt_id).await? {
            if grant.authority != *authority
                || grant.follower_id != *follower
                || grant.credential_digest != *digest
                || grant.issued_state_version != expected.version()
                || grant.issuance_attempt_id.as_ref() != Some(attempt_id)
            {
                return Err(Error::IdempotencyConflict);
            }
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(grant);
        }

        require_current(&mut transaction, expected).await?;
        let active_count =
            active_grants_for_follower(&mut transaction, authority, follower).await?;
        if active_count != 0 {
            // Existing duplicate legacy rows are preserved by migration and
            // must be explicitly revoked before a new single grant is approved.
            return Err(Error::CredentialConflict);
        }

        sqlx::query("INSERT INTO configuration_read_grants (credential_digest, group_id, master_instance_id, follower_instance_id, issued_state_version, grant_id, issuance_attempt_id) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(digest.as_str())
            .bind(authority.group_id().as_str())
            .bind(authority.master_id().as_str())
            .bind(follower.as_str())
            .bind(expected.version() as i64)
            .bind(proposed_grant_id.as_str())
            .bind(attempt_id.as_str())
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

        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(ConfigurationReadGrant {
            grant_id: proposed_grant_id.clone(),
            issuance_attempt_id: Some(attempt_id.clone()),
            authority: authority.clone(),
            follower_id: follower.clone(),
            credential_digest: digest.clone(),
            issued_state_version: expected.version(),
            revoked: false,
        })
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
        authority: &ConfigurationAuthority,
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
        if !matches!(state.role(), ConfigurationRole::Master(current) if current == authority)
            || state.instance_id() == follower
        {
            return Err(Error::Denied);
        }
        let grant = load_by_digest(&mut transaction, digest)
            .await?
            .ok_or(Error::Denied)?;
        if grant.revoked || grant.authority != *authority || grant.follower_id != *follower {
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
