use super::SqliteStore;
use kiln_core::{
    ConfigurationAuthority, ConfigurationGroupId, ConfigurationInstanceState,
    ConfigurationRevision, ConfigurationRole, ConfigurationStateError as Error,
    ConfigurationStateStore, ContentHash, KilnInstanceId,
};
use sqlx::{Connection, Row, SqliteConnection};

impl ConfigurationStateStore for SqliteStore {
    async fn initialize_configuration_instance(
        &self,
        proposed_id: KilnInstanceId,
    ) -> Result<ConfigurationInstanceState, Error> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        sqlx::query("INSERT INTO configuration_instance (singleton, instance_id, version, role) VALUES (1, ?, 1, 'unassigned') ON CONFLICT(singleton) DO NOTHING")
            .bind(proposed_id.as_str()).execute(&mut *transaction).await.map_err(|_| Error::Unavailable)?;
        let state = load(&mut transaction)
            .await?
            .ok_or(Error::IntegrityViolation)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(state)
    }

    async fn get_configuration_instance(
        &self,
    ) -> Result<Option<ConfigurationInstanceState>, Error> {
        let mut connection = self.connection.lock().await;
        load(&mut connection).await
    }

    async fn change_configuration_role(
        &self,
        expected: &ConfigurationInstanceState,
        role: ConfigurationRole,
    ) -> Result<ConfigurationInstanceState, Error> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let current = load(&mut transaction).await?.ok_or(Error::Uninitialized)?;
        if &current != expected {
            return Err(Error::Conflict);
        }
        let next = current.change_role(role)?;
        if let Some(authority) = next.role().authority() {
            sqlx::query("INSERT INTO configuration_authorities (group_id, master_instance_id) VALUES (?, ?) ON CONFLICT(group_id) DO NOTHING")
                .bind(authority.group_id().as_str()).bind(authority.master_id().as_str())
                .execute(&mut *transaction).await.map_err(|_| Error::Unavailable)?;
            let master: String = sqlx::query_scalar(
                "SELECT master_instance_id FROM configuration_authorities WHERE group_id = ?",
            )
            .bind(authority.group_id().as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(|_| Error::Unavailable)?;
            if master != authority.master_id().as_str() {
                return Err(Error::AuthorityConflict);
            }
        }
        save(&mut transaction, &current, &next).await?;
        // Rejoining restores the group's durable observation watermark. Role
        // changes never clear another authority's history.
        let next = load(&mut transaction)
            .await?
            .ok_or(Error::IntegrityViolation)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(next)
    }

    async fn observe_configuration_revision(
        &self,
        expected: &ConfigurationInstanceState,
        revision: ConfigurationRevision,
    ) -> Result<ConfigurationInstanceState, Error> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let current = load(&mut transaction).await?.ok_or(Error::Uninitialized)?;
        if &current != expected {
            return Err(Error::Conflict);
        }
        let next = current.observe(revision)?;
        save(&mut transaction, &current, &next).await?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(next)
    }
}

pub(super) async fn load(
    connection: &mut SqliteConnection,
) -> Result<Option<ConfigurationInstanceState>, Error> {
    let row = sqlx::query("SELECT i.instance_id, i.version, i.role, i.group_id, a.master_instance_id, a.observed_revision, a.observed_schema_version, a.observed_content_hash FROM configuration_instance i LEFT JOIN configuration_authorities a ON a.group_id = i.group_id WHERE i.singleton = 1")
        .fetch_optional(connection).await.map_err(|_| Error::Unavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let instance_id = KilnInstanceId::parse(
        row.try_get::<String, _>("instance_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let version = u64::try_from(
        row.try_get::<i64, _>("version")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let group: Option<String> = row
        .try_get("group_id")
        .map_err(|_| Error::IntegrityViolation)?;
    let master: Option<String> = row
        .try_get("master_instance_id")
        .map_err(|_| Error::IntegrityViolation)?;
    let authority = match (group, master) {
        (None, None) => None,
        (Some(group), Some(master)) => Some(ConfigurationAuthority::new(
            ConfigurationGroupId::parse(group).map_err(|_| Error::IntegrityViolation)?,
            KilnInstanceId::parse(master).map_err(|_| Error::IntegrityViolation)?,
        )),
        _ => return Err(Error::IntegrityViolation),
    };
    let role = match row
        .try_get::<String, _>("role")
        .map_err(|_| Error::IntegrityViolation)?
        .as_str()
    {
        "unassigned" if authority.is_none() => ConfigurationRole::Unassigned,
        "master" => ConfigurationRole::Master(authority.clone().ok_or(Error::IntegrityViolation)?),
        "follower" => {
            ConfigurationRole::Follower(authority.clone().ok_or(Error::IntegrityViolation)?)
        }
        _ => return Err(Error::IntegrityViolation),
    };
    let number: Option<i64> = row
        .try_get("observed_revision")
        .map_err(|_| Error::IntegrityViolation)?;
    let schema: Option<i64> = row
        .try_get("observed_schema_version")
        .map_err(|_| Error::IntegrityViolation)?;
    let hash: Option<String> = row
        .try_get("observed_content_hash")
        .map_err(|_| Error::IntegrityViolation)?;
    let observed = match (number, schema, hash) {
        (None, None, None) => None,
        (Some(number), Some(schema), Some(hash)) => Some(
            ConfigurationRevision::new(
                authority.ok_or(Error::IntegrityViolation)?,
                u64::try_from(number).map_err(|_| Error::IntegrityViolation)?,
                u32::try_from(schema).map_err(|_| Error::IntegrityViolation)?,
                ContentHash::parse(hash).map_err(|_| Error::IntegrityViolation)?,
            )
            .map_err(|_| Error::IntegrityViolation)?,
        ),
        _ => return Err(Error::IntegrityViolation),
    };
    ConfigurationInstanceState::from_persisted(instance_id, version, role, observed)
        .map(Some)
        .map_err(|_| Error::IntegrityViolation)
}

pub(super) async fn save(
    connection: &mut SqliteConnection,
    current: &ConfigurationInstanceState,
    next: &ConfigurationInstanceState,
) -> Result<(), Error> {
    if current == next {
        return Ok(());
    }
    let role = match next.role() {
        ConfigurationRole::Unassigned => "unassigned",
        ConfigurationRole::Master(_) => "master",
        ConfigurationRole::Follower(_) => "follower",
    };
    if let Some(revision) = next.observed() {
        let number = i64::try_from(revision.number()).map_err(|_| Error::VersionExhausted)?;
        let updated = sqlx::query("UPDATE configuration_authorities SET observed_revision = ?, observed_schema_version = ?, observed_content_hash = ? WHERE group_id = ? AND master_instance_id = ?")
            .bind(number).bind(i64::from(revision.schema_version())).bind(revision.content_hash().as_str())
            .bind(revision.authority().group_id().as_str()).bind(revision.authority().master_id().as_str())
            .execute(&mut *connection).await.map_err(|_| Error::Unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(Error::IntegrityViolation);
        }
    }
    let updated = sqlx::query("UPDATE configuration_instance SET version = ?, role = ?, group_id = ? WHERE singleton = 1 AND instance_id = ? AND version = ?")
        .bind(i64::try_from(next.version()).map_err(|_| Error::VersionExhausted)?)
        .bind(role).bind(next.role().authority().map(|authority| authority.group_id().as_str()))
        .bind(current.instance_id().as_str()).bind(i64::try_from(current.version()).map_err(|_| Error::VersionExhausted)?)
        .execute(connection).await.map_err(|_| Error::Unavailable)?;
    if updated.rows_affected() != 1 {
        return Err(Error::Conflict);
    }
    Ok(())
}
