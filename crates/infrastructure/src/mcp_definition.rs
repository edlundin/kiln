use kiln_core::{
    McpDefinitionError as Error, McpDefinitionLimits, McpDefinitionRecord, McpDefinitionStore,
    McpServerDefinition, SharedConfigurationKey,
};
use sqlx::{Connection, Row, SqliteConnection};

use super::SqliteStore;

impl McpDefinitionStore for SqliteStore {
    async fn get_mcp_definition(
        &self,
        id: &SharedConfigurationKey,
        limits: McpDefinitionLimits,
    ) -> Result<Option<McpDefinitionRecord>, Error> {
        validate_key(id, limits)?;
        let mut connection = self.connection.lock().await;
        let version = current_version(&mut connection, id).await?;
        match version {
            Some(version) => load_version(&mut connection, id, version, limits)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    async fn register_mcp_definition(
        &self,
        definition: &McpServerDefinition,
        expected_version: u64,
        idempotency_key: &str,
        limits: McpDefinitionLimits,
    ) -> Result<McpDefinitionRecord, Error> {
        // Revalidate against this operation's budgets even when constructed by
        // an earlier caller using different limits.
        let definition =
            McpServerDefinition::from_metadata_json(definition.metadata_json().as_bytes(), limits)?;
        if idempotency_key.is_empty()
            || idempotency_key.len() > limits.max_key_bytes
            || !idempotency_key.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err(Error::InvalidRequest);
        }
        let expected = i64::try_from(expected_version).map_err(|_| Error::InvalidRequest)?;
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let receipt = sqlx::query("SELECT definition_id, expected_version, result_version FROM mcp_definition_commands WHERE idempotency_key = ?")
            .bind(idempotency_key).fetch_optional(&mut *transaction).await.map_err(|_| Error::Unavailable)?;
        if let Some(receipt) = receipt {
            let id: String = receipt
                .try_get("definition_id")
                .map_err(|_| Error::IntegrityViolation)?;
            let prior_expected: i64 = receipt
                .try_get("expected_version")
                .map_err(|_| Error::IntegrityViolation)?;
            if id != definition.id().as_str() || prior_expected != expected {
                return Err(Error::IdempotencyConflict);
            }
            let version: i64 = receipt
                .try_get("result_version")
                .map_err(|_| Error::IntegrityViolation)?;
            let original = load_version(&mut transaction, definition.id(), version, limits).await?;
            if original.definition.metadata_json() != definition.metadata_json() {
                return Err(Error::IdempotencyConflict);
            }
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(original);
        }
        let current = current_version(&mut transaction, definition.id())
            .await?
            .unwrap_or(0);
        if current != expected {
            return Err(Error::Conflict);
        }
        let next = current.checked_add(1).ok_or(Error::IntegrityViolation)?;
        sqlx::query("INSERT INTO mcp_definition_versions (definition_id, version, metadata_json) VALUES (?, ?, ?)")
            .bind(definition.id().as_str()).bind(next).bind(definition.metadata_json())
            .execute(&mut *transaction).await.map_err(|_| Error::Unavailable)?;
        sqlx::query("INSERT INTO mcp_definitions (definition_id, version) VALUES (?, ?) ON CONFLICT (definition_id) DO UPDATE SET version = excluded.version")
            .bind(definition.id().as_str()).bind(next).execute(&mut *transaction).await.map_err(|_| Error::Unavailable)?;
        sqlx::query("INSERT INTO mcp_definition_commands (idempotency_key, definition_id, expected_version, result_version) VALUES (?, ?, ?, ?)")
            .bind(idempotency_key).bind(definition.id().as_str()).bind(expected).bind(next)
            .execute(&mut *transaction).await.map_err(|_| Error::Unavailable)?;
        sqlx::query("INSERT INTO mcp_definition_events (definition_id, version, event_type) VALUES (?, ?, 'mcp_server.registered')")
            .bind(definition.id().as_str()).bind(next).execute(&mut *transaction).await.map_err(|_| Error::Unavailable)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(McpDefinitionRecord {
            definition,
            version: next as u64,
        })
    }
}

fn validate_key(id: &SharedConfigurationKey, limits: McpDefinitionLimits) -> Result<(), Error> {
    limits.validate().map_err(|_| Error::InvalidRequest)?;
    SharedConfigurationKey::parse(id.as_str(), limits.max_key_bytes)
        .map(|_| ())
        .map_err(|_| Error::InvalidRequest)
}

async fn current_version(
    connection: &mut SqliteConnection,
    id: &SharedConfigurationKey,
) -> Result<Option<i64>, Error> {
    let version: Option<i64> =
        sqlx::query_scalar("SELECT version FROM mcp_definitions WHERE definition_id = ?")
            .bind(id.as_str())
            .fetch_optional(connection)
            .await
            .map_err(|_| Error::Unavailable)?;
    if version.is_some_and(|v| v <= 0) {
        return Err(Error::IntegrityViolation);
    }
    Ok(version)
}

pub(super) async fn load_version(
    connection: &mut SqliteConnection,
    id: &SharedConfigurationKey,
    version: i64,
    limits: McpDefinitionLimits,
) -> Result<McpDefinitionRecord, Error> {
    if version <= 0 {
        return Err(Error::IntegrityViolation);
    }
    let limit = i64::try_from(limits.max_metadata_bytes).map_err(|_| Error::InvalidRequest)?;
    // CASE bounds the text returned by SQLite before allocating it in Rust.
    let row = sqlx::query("SELECT CASE WHEN length(CAST(metadata_json AS BLOB)) <= ? THEN metadata_json ELSE NULL END AS metadata_json FROM mcp_definition_versions WHERE definition_id = ? AND version = ?")
        .bind(limit).bind(id.as_str()).bind(version).fetch_optional(connection).await
        .map_err(|_| Error::Unavailable)?.ok_or(Error::IntegrityViolation)?;
    let metadata: Option<String> = row
        .try_get("metadata_json")
        .map_err(|_| Error::IntegrityViolation)?;
    let metadata = metadata.ok_or(Error::LimitExceeded)?;
    let definition =
        McpServerDefinition::from_metadata_json(metadata.as_bytes(), limits).map_err(|error| {
            match error {
                Error::LimitExceeded => error,
                _ => Error::IntegrityViolation,
            }
        })?;
    if definition.id() != id {
        return Err(Error::IntegrityViolation);
    }
    Ok(McpDefinitionRecord {
        definition,
        version: version as u64,
    })
}
