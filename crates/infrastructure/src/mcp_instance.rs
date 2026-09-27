use super::SqliteStore;
use kiln_core::{
    McpDefinitionLimits, McpDesiredState, McpGenerationId, McpInstanceClaim,
    McpInstanceError as Error, McpInstanceKey, McpInstanceOwner, McpInstanceRecord,
    McpInstanceStore, McpInstanceTransition, McpObservedState, McpProtocolPolicy,
    McpProtocolVersion,
};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteRow};

impl McpInstanceStore for SqliteStore {
    async fn claim_mcp_instance(
        &self,
        key: &McpInstanceKey,
        expected_definition_version: u64,
        generation: &McpGenerationId,
        limits: McpDefinitionLimits,
    ) -> Result<McpInstanceClaim, Error> {
        if key.canonical_json().len() > limits.max_metadata_bytes {
            return Err(Error::LimitExceeded);
        }
        limits.validate().map_err(|_| Error::InvalidRequest)?;
        let definition_version = positive(expected_definition_version)?;
        let mut connection = self.connection.lock().await;
        let mut tx = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let current: Option<i64> =
            sqlx::query_scalar("SELECT version FROM mcp_definitions WHERE definition_id = ?")
                .bind(key.definition_id().as_str())
                .fetch_optional(&mut *tx)
                .await
                .map_err(|_| Error::Unavailable)?;
        if current.is_none() {
            return Err(Error::DefinitionNotFound);
        }
        if current != Some(definition_version) {
            return Err(Error::DefinitionChanged);
        }
        let definition = super::mcp_definition::load_version(
            &mut tx,
            key.definition_id(),
            definition_version,
            limits,
        )
        .await
        .map_err(map_definition_error)?
        .definition;
        if !definition.server().enabled {
            return Err(Error::Disabled);
        }
        let canonical =
            McpInstanceKey::new(&definition, key.owner().clone(), limits.max_metadata_bytes)?;
        if canonical.canonical_json() != key.canonical_json() {
            return Err(Error::OwnerMismatch);
        }
        validate_owner(&mut tx, key.owner()).await?;
        if let Some(current) = load_current(&mut tx, key).await? {
            if current.observed.is_active() || current.observed == McpObservedState::Interrupted {
                if current.definition_version != expected_definition_version {
                    return Err(Error::DefinitionChanged);
                }
                tx.commit().await.map_err(|_| Error::Unavailable)?;
                return Ok(McpInstanceClaim::Existing(current));
            }
        }
        let used: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM mcp_instance_generations WHERE generation_id = ?)",
        )
        .bind(generation.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| Error::Unavailable)?;
        if used {
            return Err(Error::GenerationReused);
        }
        sqlx::query("INSERT INTO mcp_instance_generations (generation_id, instance_key, definition_id, definition_version, state_version, desired, observed) VALUES (?, ?, ?, ?, 1, 'running', 'starting')")
            .bind(generation.as_str())
            .bind(key.canonical_json())
            .bind(key.definition_id().as_str())
            .bind(definition_version)
            .execute(&mut *tx).await
            .map_err(|_|Error::Unavailable)?;
        sqlx::query("INSERT INTO mcp_instances (instance_key, generation_id) VALUES (?, ?) ON CONFLICT (instance_key) DO UPDATE SET generation_id = excluded.generation_id")
            .bind(key.canonical_json())
            .bind(generation.as_str())
            .execute(&mut *tx).await
            .map_err(|_|Error::Unavailable)?;
        let record = McpInstanceRecord {
            key: key.clone(),
            generation: generation.clone(),
            definition_version: expected_definition_version,
            state_version: 1,
            desired: McpDesiredState::Running,
            observed: McpObservedState::Starting,
            negotiated_protocol: None,
        };
        append_event(&mut tx, &record, "start").await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(McpInstanceClaim::Acquired(record))
    }

    async fn get_mcp_instance(
        &self,
        key: &McpInstanceKey,
    ) -> Result<Option<McpInstanceRecord>, Error> {
        let mut connection = self.connection.lock().await;
        load_current(&mut connection, key).await
    }

    async fn transition_mcp_instance(
        &self,
        expected: &McpInstanceRecord,
        transition: McpInstanceTransition,
    ) -> Result<McpInstanceRecord, Error> {
        let version = positive(expected.state_version)?;
        let next_version = version.checked_add(1).ok_or(Error::InvalidRequest)?;
        let mut connection = self.connection.lock().await;
        let mut tx = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        // An event is also the immutable response receipt for this generation's
        // state-version transition. Replays never touch the current generation.
        let receipt=sqlx::query("SELECT e.generation_id, g.definition_id, g.definition_version, e.state_version, e.desired, e.observed, e.negotiated_protocol, e.reason FROM mcp_instance_events e JOIN mcp_instance_generations g USING (generation_id) WHERE e.generation_id = ? AND e.state_version = ? AND g.instance_key = ?")
            .bind(expected.generation.as_str())
            .bind(next_version)
            .bind(expected.key.canonical_json())
            .fetch_optional(&mut *tx).await
            .map_err(|_|Error::Unavailable)?;
        if let Some(row) = receipt {
            let stored_reason: String = row
                .try_get("reason")
                .map_err(|_| Error::IntegrityViolation)?;
            let record = parse_record(&row, &expected.key)?;
            if stored_reason != reason(transition)
                || matches!(transition,McpInstanceTransition::Ready(protocol) if record.negotiated_protocol!=Some(protocol))
            {
                return Err(Error::Conflict);
            }
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(record);
        }
        let current = load_current(&mut tx, &expected.key)
            .await?
            .ok_or(Error::NotFound)?;
        if current.generation != expected.generation
            || current.state_version != expected.state_version
        {
            return Err(Error::Conflict);
        }
        if let McpInstanceTransition::Ready(protocol) = transition {
            let row=sqlx::query("SELECT d.version, json_extract(v.metadata_json, '$.server.enabled') AS enabled, json_extract(v.metadata_json, '$.protocol') AS protocol FROM mcp_definitions d JOIN mcp_definition_versions v USING (definition_id, version) WHERE d.definition_id = ?")
                .bind(current.key.definition_id().as_str())
            .fetch_optional(&mut *tx).await
            .map_err(|_|Error::Unavailable)?.ok_or(Error::DefinitionNotFound)?;
            let actual: i64 = row
                .try_get("version")
                .map_err(|_| Error::IntegrityViolation)?;
            if actual != positive(current.definition_version)? {
                return Err(Error::DefinitionChanged);
            }
            let enabled: bool = row
                .try_get("enabled")
                .map_err(|_| Error::IntegrityViolation)?;
            if !enabled {
                return Err(Error::Disabled);
            }
            let policy: String = row
                .try_get("protocol")
                .map_err(|_| Error::IntegrityViolation)?;
            if policy != McpProtocolPolicy::Auto.as_str() && policy != protocol.as_str() {
                return Err(Error::InvalidTransition);
            }
        }
        let next = current.transition(transition)?;
        write_state(&mut tx, &next).await?;
        append_event(&mut tx, &next, reason(transition)).await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(next)
    }

    async fn interrupt_mcp_instances_after_restart(
        &self,
        batch_size: std::num::NonZeroUsize,
    ) -> Result<usize, Error> {
        let limit = i64::try_from(batch_size.get()).map_err(|_| Error::InvalidRequest)?;
        let mut connection = self.connection.lock().await;
        let mut tx = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let rows=sqlx::query("SELECT g.generation_id, g.state_version, g.desired, g.negotiated_protocol FROM mcp_instance_generations g JOIN mcp_instances i USING (instance_key, generation_id) WHERE g.observed IN ('starting','ready','stopping') ORDER BY g.generation_id LIMIT ?")
            .bind(limit)
            .fetch_all(&mut *tx).await
            .map_err(|_|Error::Unavailable)?;
        for row in &rows {
            let generation: String = row
                .try_get("generation_id")
                .map_err(|_| Error::IntegrityViolation)?;
            McpGenerationId::parse(generation.clone()).map_err(|_| Error::IntegrityViolation)?;
            let version: i64 = row
                .try_get("state_version")
                .map_err(|_| Error::IntegrityViolation)?;
            let next = version
                .checked_add(1)
                .filter(|v| *v > 1)
                .ok_or(Error::IntegrityViolation)?;
            sqlx::query("UPDATE mcp_instance_generations SET observed = 'interrupted', state_version = ? WHERE generation_id = ?")
                .bind(next)
            .bind(&generation)
            .execute(&mut *tx).await
            .map_err(|_|Error::Unavailable)?;
            sqlx::query("INSERT INTO mcp_instance_events (generation_id, state_version, desired, observed, negotiated_protocol, reason) SELECT generation_id, state_version, desired, observed, negotiated_protocol, 'daemon_restart' FROM mcp_instance_generations WHERE generation_id = ?")
                .bind(&generation)
            .execute(&mut *tx).await
            .map_err(|_|Error::Unavailable)?;
        }
        let count = rows.len();
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(count)
    }
}

fn reason(transition: McpInstanceTransition) -> &'static str {
    match transition {
        McpInstanceTransition::Ready(_) => "ready",
        McpInstanceTransition::RequestStop => "stop_requested",
        McpInstanceTransition::Stopped => "stopped",
        McpInstanceTransition::ConnectionLost => "connection_lost",
        McpInstanceTransition::StartupFailed => "startup_failed",
    }
}
fn positive(value: u64) -> Result<i64, Error> {
    i64::try_from(value)
        .ok()
        .filter(|v| *v > 0)
        .ok_or(Error::InvalidRequest)
}
fn map_definition_error(error: kiln_core::McpDefinitionError) -> Error {
    match error {
        kiln_core::McpDefinitionError::LimitExceeded => Error::LimitExceeded,
        kiln_core::McpDefinitionError::Unavailable => Error::Unavailable,
        _ => Error::IntegrityViolation,
    }
}
async fn load_current(
    connection: &mut SqliteConnection,
    key: &McpInstanceKey,
) -> Result<Option<McpInstanceRecord>, Error> {
    let row=sqlx::query("SELECT g.generation_id, g.definition_id, g.definition_version, g.state_version, g.desired, g.observed, g.negotiated_protocol FROM mcp_instances i LEFT JOIN mcp_instance_generations g USING (instance_key, generation_id) WHERE i.instance_key = ?")
        .bind(key.canonical_json())
            .fetch_optional(connection).await
            .map_err(|_|Error::Unavailable)?;
    row.map(|row| parse_record(&row, key)).transpose()
}
fn parse_record(row: &SqliteRow, key: &McpInstanceKey) -> Result<McpInstanceRecord, Error> {
    let definition_id: String = row
        .try_get("definition_id")
        .map_err(|_| Error::IntegrityViolation)?;
    if definition_id != key.definition_id().as_str() {
        return Err(Error::IntegrityViolation);
    }
    let generation: String = row
        .try_get("generation_id")
        .map_err(|_| Error::IntegrityViolation)?;
    let definition_version: i64 = row
        .try_get("definition_version")
        .map_err(|_| Error::IntegrityViolation)?;
    let state_version: i64 = row
        .try_get("state_version")
        .map_err(|_| Error::IntegrityViolation)?;
    if definition_version <= 0 || state_version <= 0 {
        return Err(Error::IntegrityViolation);
    }
    let desired: String = row
        .try_get("desired")
        .map_err(|_| Error::IntegrityViolation)?;
    let observed: String = row
        .try_get("observed")
        .map_err(|_| Error::IntegrityViolation)?;
    let protocol: Option<String> = row
        .try_get("negotiated_protocol")
        .map_err(|_| Error::IntegrityViolation)?;
    Ok(McpInstanceRecord {
        key: key.clone(),
        generation: McpGenerationId::parse(generation).map_err(|_| Error::IntegrityViolation)?,
        definition_version: definition_version as u64,
        state_version: state_version as u64,
        desired: McpDesiredState::parse(&desired)?,
        observed: McpObservedState::parse(&observed)?,
        negotiated_protocol: protocol
            .map(|p| McpProtocolVersion::parse(&p).map_err(|_| Error::IntegrityViolation))
            .transpose()?,
    })
}
async fn write_state(
    connection: &mut SqliteConnection,
    record: &McpInstanceRecord,
) -> Result<(), Error> {
    let changed=sqlx::query("UPDATE mcp_instance_generations SET state_version = ?, desired = ?, observed = ?, negotiated_protocol = ? WHERE generation_id = ? AND state_version = ?")
        .bind(positive(record.state_version)?)
            .bind(record.desired.as_str())
            .bind(record.observed.as_str())
            .bind(record.negotiated_protocol.map(McpProtocolVersion::as_str))
        .bind(record.generation.as_str())
            .bind(positive(record.state_version-1)?)
            .execute(connection).await
            .map_err(|_|Error::Unavailable)?.rows_affected();
    if changed != 1 {
        return Err(Error::Conflict);
    }
    Ok(())
}
async fn append_event(
    connection: &mut SqliteConnection,
    record: &McpInstanceRecord,
    reason: &str,
) -> Result<(), Error> {
    sqlx::query("INSERT INTO mcp_instance_events (generation_id, state_version, desired, observed, negotiated_protocol, reason) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(record.generation.as_str())
            .bind(positive(record.state_version)?)
            .bind(record.desired.as_str())
            .bind(record.observed.as_str())
            .bind(record.negotiated_protocol.map(McpProtocolVersion::as_str))
            .bind(reason)
        .execute(connection).await
            .map_err(|_|Error::Unavailable)?;
    Ok(())
}
pub(super) async fn validate_owner(
    connection: &mut SqliteConnection,
    owner: &McpInstanceOwner,
) -> Result<(), Error> {
    let exists:bool=match owner {
        McpInstanceOwner::Core=>true,
        McpInstanceOwner::Workspace(id)=>sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workspaces WHERE workspace_id = ?)")
            .bind(id.as_str())
            .fetch_one(connection).await
            .map_err(|_|Error::Unavailable)?,
        McpInstanceOwner::Session(id)=>sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sessions WHERE session_id = ?)")
            .bind(id.as_str())
            .fetch_one(connection).await
            .map_err(|_|Error::Unavailable)?,
        McpInstanceOwner::WorkspaceCheckout(c)=>sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workspace_roots WHERE workspace_id = ? AND workspace_root_id = ? AND canonical_path = ? AND git_common_directory_path = ? AND filesystem_identity = ? AND state = 'available')")
            .bind(c.workspace_id().as_str())
            .bind(c.workspace_root_id().as_str())
            .bind(c.root_path())
            .bind(c.git_common_directory_path())
            .bind(c.filesystem_identity().as_str())
            .fetch_one(connection).await
            .map_err(|_|Error::Unavailable)?,
    };
    if exists {
        Ok(())
    } else {
        Err(Error::OwnerNotFound)
    }
}
