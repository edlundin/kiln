use super::{SqliteStore, configuration_sync};
use kiln_core::{
    ConfigurationAuthority, ConfigurationCandidateDisposition, ConfigurationFollowerCursor,
    ConfigurationGroupId, ConfigurationInstanceState, ConfigurationPublicationStore,
    ConfigurationRevision, ConfigurationRole, ConfigurationSnapshotDisposition,
    ConfigurationSnapshotError as Error, ConfigurationSnapshotMutation,
    ConfigurationSnapshotReadLimits, ConfigurationSnapshotStore, ConfigurationStateError,
    ConfigurationSyncStatus, ContentHash, GlobalSkillId, KilnInstanceId,
    SHARED_CONFIGURATION_SCHEMA_VERSION, SharedConfigurationSnapshot, SharedSkillFileInput,
    SharedSkillPackage, SharedSkillPackageInput, StoredConfigurationSnapshot,
};
use sqlx::{Connection, Row, SqliteConnection};

impl ConfigurationPublicationStore for SqliteStore {
    async fn publish_configuration_snapshot_idempotent(
        &self,
        expected_instance: &KilnInstanceId,
        expected_group: &ConfigurationGroupId,
        expected_version: u64,
        idempotency_key: &str,
        snapshot: &SharedConfigurationSnapshot,
    ) -> Result<ConfigurationSnapshotMutation, Error> {
        if idempotency_key.is_empty()
            || expected_version == 0
            || expected_version >= i64::MAX as u64
        {
            return Err(Error::InvalidRequest);
        }
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let receipt = sqlx::query("SELECT p.instance_id, p.group_id, p.expected_version, p.revision, p.schema_version, p.content_hash, a.master_instance_id FROM configuration_publications p JOIN configuration_authorities a ON a.group_id = p.group_id WHERE p.idempotency_key = ?")
            .bind(idempotency_key).fetch_optional(&mut *transaction).await.map_err(|_| Error::Unavailable)?;
        if let Some(receipt) = receipt {
            let instance: String = receipt
                .try_get("instance_id")
                .map_err(|_| Error::IntegrityViolation)?;
            let group: String = receipt
                .try_get("group_id")
                .map_err(|_| Error::IntegrityViolation)?;
            let version: i64 = receipt
                .try_get("expected_version")
                .map_err(|_| Error::IntegrityViolation)?;
            let hash: String = receipt
                .try_get("content_hash")
                .map_err(|_| Error::IntegrityViolation)?;
            if instance != expected_instance.as_str()
                || group != expected_group.as_str()
                || version != expected_version as i64
                || hash != snapshot.content_hash().as_str()
            {
                return Err(Error::IdempotencyConflict);
            }
            let master: String = receipt
                .try_get("master_instance_id")
                .map_err(|_| Error::IntegrityViolation)?;
            if master != instance {
                return Err(Error::IntegrityViolation);
            }
            let authority =
                ConfigurationAuthority::new(expected_group.clone(), expected_instance.clone());
            let revision = ConfigurationRevision::new(
                authority.clone(),
                u64::try_from(
                    receipt
                        .try_get::<i64, _>("revision")
                        .map_err(|_| Error::IntegrityViolation)?,
                )
                .map_err(|_| Error::IntegrityViolation)?,
                u32::try_from(
                    receipt
                        .try_get::<i64, _>("schema_version")
                        .map_err(|_| Error::IntegrityViolation)?,
                )
                .map_err(|_| Error::IntegrityViolation)?,
                ContentHash::parse(hash).map_err(|_| Error::IntegrityViolation)?,
            )
            .map_err(|_| Error::IntegrityViolation)?;
            snapshot
                .verify_revision(&revision)
                .map_err(|_| Error::IntegrityViolation)?;
            let original = ConfigurationInstanceState::from_persisted(
                expected_instance.clone(),
                expected_version + 1,
                ConfigurationRole::Master(authority),
                None,
            )
            .map_err(Error::State)?;
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(ConfigurationSnapshotMutation {
                state: original,
                revision,
                disposition: ConfigurationSnapshotDisposition::Duplicate,
            });
        }
        let current = configuration_sync::load(&mut transaction)
            .await
            .map_err(Error::State)?
            .ok_or(Error::State(ConfigurationStateError::Uninitialized))?;
        if current.instance_id() != expected_instance
            || current.version() != expected_version
            || !matches!(current.role(), ConfigurationRole::Master(authority) if authority.group_id() == expected_group)
        {
            return Err(Error::State(ConfigurationStateError::Conflict));
        }
        let result = publish(&mut transaction, &current, snapshot).await?;
        sqlx::query("INSERT INTO configuration_publications (idempotency_key, instance_id, group_id, expected_version, revision, schema_version, content_hash) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(idempotency_key).bind(expected_instance.as_str()).bind(expected_group.as_str()).bind(expected_version as i64)
            .bind(i64::try_from(result.revision.number()).map_err(|_| Error::InvalidSnapshot)?)
            .bind(i64::from(result.revision.schema_version())).bind(result.revision.content_hash().as_str())
            .execute(&mut *transaction).await.map_err(|_| Error::Unavailable)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(result)
    }
}

impl ConfigurationSnapshotStore for SqliteStore {
    async fn get_configuration_sync_status(&self) -> Result<ConfigurationSyncStatus, Error> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection.begin().await.map_err(|_| Error::Unavailable)?;
        let state = configuration_sync::load(&mut transaction)
            .await
            .map_err(Error::State)?
            .ok_or(Error::State(ConfigurationStateError::Uninitialized))?;
        let applied_revision = match state.role().authority() {
            Some(authority) => load_revision(&mut transaction, authority).await?,
            None => None,
        };
        if let ConfigurationRole::Follower(authority) = state.role() {
            ConfigurationFollowerCursor::from_persisted(
                state.instance_id().clone(),
                authority.clone(),
                applied_revision.clone(),
                state.observed().cloned(),
            )
            .map_err(|_| Error::IntegrityViolation)?;
        }
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(ConfigurationSyncStatus {
            state,
            applied_revision,
        })
    }

    async fn publish_configuration_snapshot(
        &self,
        expected: &ConfigurationInstanceState,
        snapshot: &SharedConfigurationSnapshot,
    ) -> Result<ConfigurationSnapshotMutation, Error> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let current = current(&mut transaction, expected).await?;
        let result = publish(&mut transaction, &current, snapshot).await?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(result)
    }

    async fn apply_configuration_snapshot(
        &self,
        expected: &ConfigurationInstanceState,
        revision: ConfigurationRevision,
        snapshot: &SharedConfigurationSnapshot,
    ) -> Result<ConfigurationSnapshotMutation, Error> {
        snapshot
            .verify_revision(&revision)
            .map_err(|_| Error::InvalidSnapshot)?;
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let current = current(&mut transaction, expected).await?;
        let ConfigurationRole::Follower(authority) = current.role() else {
            return Err(Error::State(ConfigurationStateError::InvalidRole));
        };
        let applied = load_revision(&mut transaction, authority).await?;
        let cursor = ConfigurationFollowerCursor::from_persisted(
            current.instance_id().clone(),
            authority.clone(),
            applied,
            current.observed().cloned(),
        )
        .map_err(|error| Error::State(ConfigurationStateError::Revision(error)))?;
        let disposition = cursor
            .assess_candidate(&revision, SHARED_CONFIGURATION_SCHEMA_VERSION)
            .map_err(|error| Error::State(ConfigurationStateError::Revision(error)))?;
        if disposition == ConfigurationCandidateDisposition::AlreadyApplied {
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(ConfigurationSnapshotMutation {
                state: current,
                revision,
                disposition: ConfigurationSnapshotDisposition::Duplicate,
            });
        }
        let next = advanced_state(&current, Some(revision.clone()))?;
        write_snapshot(&mut transaction, &revision, snapshot).await?;
        configuration_sync::save(&mut transaction, &current, &next)
            .await
            .map_err(Error::State)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(ConfigurationSnapshotMutation {
            state: next,
            revision,
            disposition: ConfigurationSnapshotDisposition::Applied,
        })
    }

    async fn get_configuration_snapshot(
        &self,
        limits: ConfigurationSnapshotReadLimits,
    ) -> Result<Option<StoredConfigurationSnapshot>, Error> {
        if limits.max_total_metadata_bytes == 0 {
            return Err(Error::InvalidLimits);
        }
        limits
            .configuration
            .validate()
            .map_err(|_| Error::InvalidLimits)?;
        limits.skill.validate().map_err(|_| Error::InvalidLimits)?;
        let mut connection = self.connection.lock().await;
        let mut transaction = connection.begin().await.map_err(|_| Error::Unavailable)?;
        let state = configuration_sync::load(&mut transaction)
            .await
            .map_err(Error::State)?
            .ok_or(Error::State(ConfigurationStateError::Uninitialized))?;
        let Some(authority) = state.role().authority() else {
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(None);
        };
        let Some(revision) = load_revision(&mut transaction, authority).await? else {
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(None);
        };
        if let ConfigurationRole::Follower(authority) = state.role() {
            ConfigurationFollowerCursor::from_persisted(
                state.instance_id().clone(),
                authority.clone(),
                Some(revision.clone()),
                state.observed().cloned(),
            )
            .map_err(|_| Error::IntegrityViolation)?;
        }
        let snapshot = read_snapshot(&mut transaction, &revision, limits).await?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(Some(StoredConfigurationSnapshot {
            state,
            revision,
            snapshot,
        }))
    }
}

async fn publish(
    connection: &mut SqliteConnection,
    current: &ConfigurationInstanceState,
    snapshot: &SharedConfigurationSnapshot,
) -> Result<ConfigurationSnapshotMutation, Error> {
    let ConfigurationRole::Master(authority) = current.role() else {
        return Err(Error::State(ConfigurationStateError::InvalidRole));
    };
    let previous = load_revision(connection, authority).await?;
    if previous
        .as_ref()
        .is_some_and(|value| value.schema_version() != SHARED_CONFIGURATION_SCHEMA_VERSION)
    {
        return Err(Error::InvalidSnapshot);
    }
    let number = previous
        .map_or(Some(1), |value| value.number().checked_add(1))
        .filter(|value| *value <= i64::MAX as u64)
        .ok_or(Error::State(ConfigurationStateError::VersionExhausted))?;
    let revision = ConfigurationRevision::new(
        authority.clone(),
        number,
        SHARED_CONFIGURATION_SCHEMA_VERSION,
        snapshot.content_hash().clone(),
    )
    .map_err(|_| Error::InvalidSnapshot)?;
    let next = advanced_state(current, None)?;
    write_snapshot(connection, &revision, snapshot).await?;
    configuration_sync::save(connection, current, &next)
        .await
        .map_err(Error::State)?;
    Ok(ConfigurationSnapshotMutation {
        state: next,
        revision,
        disposition: ConfigurationSnapshotDisposition::Applied,
    })
}

async fn current(
    connection: &mut SqliteConnection,
    expected: &ConfigurationInstanceState,
) -> Result<ConfigurationInstanceState, Error> {
    let state = configuration_sync::load(connection)
        .await
        .map_err(Error::State)?
        .ok_or(Error::State(ConfigurationStateError::Uninitialized))?;
    if &state != expected {
        return Err(Error::State(ConfigurationStateError::Conflict));
    }
    Ok(state)
}
fn advanced_state(
    current: &ConfigurationInstanceState,
    observed: Option<ConfigurationRevision>,
) -> Result<ConfigurationInstanceState, Error> {
    let version = current
        .version()
        .checked_add(1)
        .filter(|value| *value <= i64::MAX as u64)
        .ok_or(Error::State(ConfigurationStateError::VersionExhausted))?;
    ConfigurationInstanceState::from_persisted(
        current.instance_id().clone(),
        version,
        current.role().clone(),
        observed,
    )
    .map_err(Error::State)
}
async fn load_revision(
    connection: &mut SqliteConnection,
    authority: &ConfigurationAuthority,
) -> Result<Option<ConfigurationRevision>, Error> {
    let row = sqlx::query("SELECT revision, schema_version, content_hash FROM configuration_snapshots WHERE group_id = ?")
        .bind(authority.group_id().as_str()).fetch_optional(connection).await.map_err(|_| Error::Unavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    ConfigurationRevision::new(
        authority.clone(),
        u64::try_from(
            row.try_get::<i64, _>("revision")
                .map_err(|_| Error::IntegrityViolation)?,
        )
        .map_err(|_| Error::IntegrityViolation)?,
        u32::try_from(
            row.try_get::<i64, _>("schema_version")
                .map_err(|_| Error::IntegrityViolation)?,
        )
        .map_err(|_| Error::IntegrityViolation)?,
        ContentHash::parse(
            row.try_get::<String, _>("content_hash")
                .map_err(|_| Error::IntegrityViolation)?,
        )
        .map_err(|_| Error::IntegrityViolation)?,
    )
    .map(Some)
    .map_err(|_| Error::IntegrityViolation)
}
async fn write_snapshot(
    connection: &mut SqliteConnection,
    revision: &ConfigurationRevision,
    snapshot: &SharedConfigurationSnapshot,
) -> Result<(), Error> {
    snapshot
        .verify_revision(revision)
        .map_err(|_| Error::InvalidSnapshot)?;
    let group = revision.authority().group_id().as_str();
    sqlx::query("INSERT INTO configuration_snapshots (group_id, revision, schema_version, content_hash, metadata_json) VALUES (?, ?, ?, ?, ?) ON CONFLICT(group_id) DO UPDATE SET revision = excluded.revision, schema_version = excluded.schema_version, content_hash = excluded.content_hash, metadata_json = excluded.metadata_json")
        .bind(group).bind(i64::try_from(revision.number()).map_err(|_| Error::InvalidSnapshot)?)
        .bind(i64::from(revision.schema_version())).bind(revision.content_hash().as_str()).bind(snapshot.metadata_json())
        .execute(&mut *connection).await.map_err(|_| Error::Unavailable)?;
    sqlx::query("DELETE FROM configuration_skill_packages WHERE group_id = ?")
        .bind(group)
        .execute(&mut *connection)
        .await
        .map_err(|_| Error::Unavailable)?;
    for skill in snapshot.skills() {
        let dependencies = serde_json::to_string(
            &skill
                .dependencies()
                .iter()
                .map(GlobalSkillId::as_str)
                .collect::<Vec<_>>(),
        )
        .map_err(|_| Error::InvalidSnapshot)?;
        sqlx::query("INSERT INTO configuration_skill_packages (group_id, skill_id, version, enabled, dependencies_json, content_hash) VALUES (?, ?, ?, ?, ?, ?)")
            .bind(group).bind(skill.id().as_str()).bind(skill.version()).bind(skill.enabled()).bind(dependencies).bind(skill.content_hash().as_str())
            .execute(&mut *connection).await.map_err(|_| Error::Unavailable)?;
        for file in skill.files() {
            sqlx::query("INSERT INTO configuration_skill_files (group_id, skill_id, path, content, content_hash) VALUES (?, ?, ?, ?, ?)")
                .bind(group).bind(skill.id().as_str()).bind(file.path()).bind(file.content()).bind(file.content_hash().as_str())
                .execute(&mut *connection).await.map_err(|_| Error::Unavailable)?;
        }
    }
    Ok(())
}

async fn read_snapshot(
    connection: &mut SqliteConnection,
    revision: &ConfigurationRevision,
    limits: ConfigurationSnapshotReadLimits,
) -> Result<SharedConfigurationSnapshot, Error> {
    let group = revision.authority().group_id().as_str();
    if revision.schema_version() != SHARED_CONFIGURATION_SCHEMA_VERSION {
        return Err(Error::InvalidSnapshot);
    }
    let mut metadata_bytes: i64 = sqlx::query_scalar("SELECT length(CAST(metadata_json AS BLOB)) FROM configuration_snapshots WHERE group_id = ?")
        .bind(group).fetch_one(&mut *connection).await.map_err(|_| Error::Unavailable)?;
    bounded(metadata_bytes, limits.configuration.max_metadata_bytes)?;
    let counts = sqlx::query("SELECT count(*) AS files, coalesce(sum(length(CAST(content AS BLOB))), 0) AS bytes, coalesce(max(length(CAST(content AS BLOB))), 0) AS max_file, coalesce(max(length(CAST(path AS BLOB))), 0) AS max_path, coalesce(sum(length(CAST(path AS BLOB)) + length(CAST(content_hash AS BLOB))), 0) AS metadata_bytes FROM configuration_skill_files WHERE group_id = ?")
        .bind(group).fetch_one(&mut *connection).await.map_err(|_| Error::Unavailable)?;
    for (field, limit) in [
        ("files", limits.configuration.max_total_skill_files),
        ("bytes", limits.configuration.max_total_skill_bytes),
        ("max_file", limits.skill.max_file_bytes),
        ("max_path", limits.skill.max_path_bytes),
    ] {
        bounded(
            counts
                .try_get(field)
                .map_err(|_| Error::IntegrityViolation)?,
            limit,
        )?;
    }
    metadata_bytes = metadata_bytes
        .checked_add(
            counts
                .try_get("metadata_bytes")
                .map_err(|_| Error::IntegrityViolation)?,
        )
        .ok_or(Error::LimitExceeded)?;
    bounded(metadata_bytes, limits.max_total_metadata_bytes)?;
    let dependency_bytes = limits
        .skill
        .max_identifier_bytes
        .saturating_add(3)
        .saturating_mul(limits.skill.max_dependencies)
        .saturating_add(2)
        .min(limits.max_total_metadata_bytes);
    let counts = sqlx::query("SELECT count(*) AS packages, coalesce(max(length(CAST(skill_id AS BLOB))), 0) AS max_id, coalesce(max(length(CAST(version AS BLOB))), 0) AS max_version, coalesce(max(length(CAST(dependencies_json AS BLOB))), 0) AS max_dependencies, coalesce(sum(length(CAST(skill_id AS BLOB)) + length(CAST(version AS BLOB)) + length(CAST(dependencies_json AS BLOB)) + length(CAST(content_hash AS BLOB))), 0) AS metadata_bytes FROM configuration_skill_packages WHERE group_id = ?")
        .bind(group).fetch_one(&mut *connection).await.map_err(|_| Error::Unavailable)?;
    for (field, limit) in [
        ("packages", limits.configuration.max_skills),
        (
            "max_id",
            limits
                .skill
                .max_identifier_bytes
                .min(limits.configuration.max_key_bytes),
        ),
        ("max_version", limits.skill.max_version_bytes),
        ("max_dependencies", dependency_bytes),
    ] {
        bounded(
            counts
                .try_get(field)
                .map_err(|_| Error::IntegrityViolation)?,
            limit,
        )?;
    }
    metadata_bytes = metadata_bytes
        .checked_add(
            counts
                .try_get("metadata_bytes")
                .map_err(|_| Error::IntegrityViolation)?,
        )
        .ok_or(Error::LimitExceeded)?;
    bounded(metadata_bytes, limits.max_total_metadata_bytes)?;
    let rows = sqlx::query("SELECT skill_id, version, enabled, dependencies_json, content_hash FROM configuration_skill_packages WHERE group_id = ? ORDER BY skill_id")
        .bind(group).fetch_all(&mut *connection).await.map_err(|_| Error::Unavailable)?;
    let mut skills = Vec::with_capacity(rows.len());
    for row in rows {
        let id = GlobalSkillId::parse(
            row.try_get::<String, _>("skill_id")
                .map_err(|_| Error::IntegrityViolation)?,
            limits.skill.max_identifier_bytes,
        )
        .map_err(|_| Error::IntegrityViolation)?;
        let counts = sqlx::query("SELECT count(*) AS files, coalesce(sum(length(CAST(content AS BLOB))), 0) AS bytes FROM configuration_skill_files WHERE group_id = ? AND skill_id = ?")
            .bind(group).bind(id.as_str()).fetch_one(&mut *connection).await.map_err(|_| Error::Unavailable)?;
        bounded(
            counts
                .try_get("files")
                .map_err(|_| Error::IntegrityViolation)?,
            limits.skill.max_files,
        )?;
        bounded(
            counts
                .try_get("bytes")
                .map_err(|_| Error::IntegrityViolation)?,
            limits.skill.max_total_file_bytes,
        )?;
        let dependencies: Vec<String> = serde_json::from_str(
            row.try_get::<&str, _>("dependencies_json")
                .map_err(|_| Error::IntegrityViolation)?,
        )
        .map_err(|_| Error::IntegrityViolation)?;
        if dependencies.len() > limits.skill.max_dependencies {
            return Err(Error::LimitExceeded);
        }
        let dependencies = dependencies
            .into_iter()
            .map(|value| {
                GlobalSkillId::parse(value, limits.skill.max_identifier_bytes)
                    .map_err(|_| Error::IntegrityViolation)
            })
            .collect::<Result<_, _>>()?;
        let rows = sqlx::query("SELECT path, content, content_hash FROM configuration_skill_files WHERE group_id = ? AND skill_id = ? ORDER BY path")
            .bind(group).bind(id.as_str()).fetch_all(&mut *connection).await.map_err(|_| Error::Unavailable)?;
        let mut files = Vec::with_capacity(rows.len());
        for file in rows {
            files.push(SharedSkillFileInput {
                path: file
                    .try_get("path")
                    .map_err(|_| Error::IntegrityViolation)?,
                content: file
                    .try_get("content")
                    .map_err(|_| Error::IntegrityViolation)?,
                expected_hash: ContentHash::parse(
                    file.try_get::<String, _>("content_hash")
                        .map_err(|_| Error::IntegrityViolation)?,
                )
                .map_err(|_| Error::IntegrityViolation)?,
            });
        }
        let skill = SharedSkillPackage::validate(
            SharedSkillPackageInput {
                id,
                version: row
                    .try_get("version")
                    .map_err(|_| Error::IntegrityViolation)?,
                enabled: match row
                    .try_get::<i64, _>("enabled")
                    .map_err(|_| Error::IntegrityViolation)?
                {
                    0 => false,
                    1 => true,
                    _ => return Err(Error::IntegrityViolation),
                },
                dependencies,
                files,
            },
            limits.skill,
        )
        .map_err(|error| match error {
            kiln_core::SharedSkillError::LimitExceeded => Error::LimitExceeded,
            _ => Error::IntegrityViolation,
        })?;
        if skill.content_hash().as_str()
            != row
                .try_get::<&str, _>("content_hash")
                .map_err(|_| Error::IntegrityViolation)?
        {
            return Err(Error::IntegrityViolation);
        }
        skills.push(skill);
    }
    let metadata: String =
        sqlx::query_scalar("SELECT metadata_json FROM configuration_snapshots WHERE group_id = ?")
            .bind(group)
            .fetch_one(&mut *connection)
            .await
            .map_err(|_| Error::Unavailable)?;
    let snapshot = SharedConfigurationSnapshot::from_metadata_json(
        metadata.as_bytes(),
        skills,
        limits.configuration,
    )
    .map_err(|error| match error {
        kiln_core::SharedConfigurationError::LimitExceeded => Error::LimitExceeded,
        _ => Error::IntegrityViolation,
    })?;
    snapshot
        .verify_revision(revision)
        .map_err(|_| Error::IntegrityViolation)?;
    Ok(snapshot)
}
fn bounded(value: i64, limit: usize) -> Result<(), Error> {
    let value = usize::try_from(value).map_err(|_| Error::IntegrityViolation)?;
    if value > limit {
        Err(Error::LimitExceeded)
    } else {
        Ok(())
    }
}
