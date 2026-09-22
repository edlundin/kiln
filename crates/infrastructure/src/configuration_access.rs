use super::{SqliteStore, configuration_snapshot, configuration_sync};
use kiln_core::{
    ConfigurationAccessError as Error, ConfigurationAccessStore, ConfigurationAuthority,
    ConfigurationCredentialDigest, ConfigurationInstanceState, ConfigurationReadGrant,
    ConfigurationRole, ConfigurationSnapshotReadLimits, ConfigurationStateError, KilnInstanceId,
    StoredConfigurationSnapshot,
};
use sqlx::{Connection, Row, SqliteConnection};

impl ConfigurationAccessStore for SqliteStore {
    async fn register_configuration_reader(
        &self,
        expected: &ConfigurationInstanceState,
        follower: &KilnInstanceId,
        digest: &ConfigurationCredentialDigest,
    ) -> Result<ConfigurationReadGrant, Error> {
        let authority = registration_authority(expected, follower)?;
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        if let Some((grant, version)) = load(&mut transaction, digest).await? {
            if grant.authority != *authority
                || grant.follower_id != *follower
                || version != expected.version()
            {
                return Err(Error::CredentialConflict);
            }
            // Return the tombstone as well as active records. Never resurrect a
            // revoked grant or reset its original issuance preconditions.
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(grant);
        }
        require_current(&mut transaction, expected).await?;
        sqlx::query("INSERT INTO configuration_read_grants (credential_digest, group_id, master_instance_id, follower_instance_id, issued_state_version) VALUES (?, ?, ?, ?, ?)")
            .bind(digest.as_str()).bind(authority.group_id().as_str()).bind(authority.master_id().as_str())
            .bind(follower.as_str()).bind(expected.version() as i64)
            .execute(&mut *transaction).await.map_err(|_| Error::Unavailable)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(ConfigurationReadGrant {
            authority: authority.clone(),
            follower_id: follower.clone(),
            credential_digest: digest.clone(),
            revoked: false,
        })
    }

    async fn revoke_configuration_reader(
        &self,
        expected: &ConfigurationInstanceState,
        follower: &KilnInstanceId,
        digest: &ConfigurationCredentialDigest,
    ) -> Result<(), Error> {
        let authority = registration_authority(expected, follower)?;
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        require_current(&mut transaction, expected).await?;
        let (grant, _) = load(&mut transaction, digest).await?.ok_or(Error::Denied)?;
        if grant.authority != *authority || grant.follower_id != *follower {
            return Err(Error::Denied);
        }
        sqlx::query("UPDATE configuration_read_grants SET revoked = 1 WHERE credential_digest = ?")
            .bind(digest.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|_| Error::Unavailable)?;
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
        let (grant, _) = load(&mut transaction, digest).await?.ok_or(Error::Denied)?;
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

async fn load(
    connection: &mut SqliteConnection,
    digest: &ConfigurationCredentialDigest,
) -> Result<Option<(ConfigurationReadGrant, u64)>, Error> {
    let row = sqlx::query("SELECT g.group_id, g.master_instance_id, g.follower_instance_id, g.issued_state_version, g.revoked, a.master_instance_id AS authority_master FROM configuration_read_grants g JOIN configuration_authorities a ON a.group_id = g.group_id WHERE g.credential_digest = ?")
        .bind(digest.as_str()).fetch_optional(connection).await.map_err(|_| Error::Unavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let group = kiln_core::ConfigurationGroupId::parse(
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
    if master.as_str() != authority_master
        || follower == master
        || version <= 0
        || !matches!(revoked, 0 | 1)
    {
        return Err(Error::IntegrityViolation);
    }
    Ok(Some((
        ConfigurationReadGrant {
            authority: ConfigurationAuthority::new(group, master),
            follower_id: follower,
            credential_digest: digest.clone(),
            revoked: revoked == 1,
        },
        version as u64,
    )))
}
