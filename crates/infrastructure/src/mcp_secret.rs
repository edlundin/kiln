use super::SqliteStore;
use kiln_core::{
    KilnInstanceId, McpDefinitionLimits, McpInstanceKey, McpSecretBinding, McpSecretJournal,
    McpSecretJournalError as Error, McpSecretPurpose, McpSecretReservation,
    McpSecretReservationState as State, SecretRef, SharedConfigurationKey, SharedMcpArgument,
    SharedMcpTransport,
};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteRow};

impl McpSecretJournal for SqliteStore {
    async fn reserve_mcp_secret(
        &self,
        binding: &McpSecretBinding,
        expected_definition_version: u64,
        limits: McpDefinitionLimits,
    ) -> Result<McpSecretReservation, Error> {
        limits.validate().map_err(|_| Error::InvalidRequest)?;
        let version = i64::try_from(expected_definition_version)
            .ok()
            .filter(|v| *v > 0)
            .ok_or(Error::InvalidRequest)?;
        if binding.key().canonical_json().len() > limits.max_metadata_bytes
            || binding.name().as_str().len() > limits.max_key_bytes
        {
            return Err(Error::LimitExceeded);
        }
        let mut connection = self.connection.lock().await;
        let mut tx = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        // Resolve old receipts before consulting current definitions. A retry
        // after reconfiguration must not become a fresh write opportunity.
        if let Some(row) = load(&mut tx, binding.secret_ref()).await? {
            check_identity(&row, binding)?;
            let stored: i64 = row
                .try_get("definition_version")
                .map_err(|_| Error::IntegrityViolation)?;
            if stored != version {
                return Err(Error::Conflict);
            }
            return Ok(McpSecretReservation::Existing(state(&row)?));
        }
        let current: Option<i64> =
            sqlx::query_scalar("SELECT version FROM mcp_definitions WHERE definition_id = ?")
                .bind(binding.key().definition_id().as_str())
                .fetch_optional(&mut *tx)
                .await
                .map_err(|_| Error::Unavailable)?;
        if current != Some(version) {
            return Err(Error::DefinitionChanged);
        }
        let definition = super::mcp_definition::load_version(
            &mut tx,
            binding.key().definition_id(),
            version,
            limits,
        )
        .await
        .map_err(|error| match error {
            kiln_core::McpDefinitionError::LimitExceeded => Error::LimitExceeded,
            kiln_core::McpDefinitionError::Unavailable => Error::Unavailable,
            _ => Error::IntegrityViolation,
        })?
        .definition;
        let canonical = McpInstanceKey::new(
            &definition,
            binding.key().owner().clone(),
            limits.max_metadata_bytes,
        )
        .map_err(|_| Error::InvalidBinding)?;
        if !definition.server().enabled
            || canonical.canonical_json() != binding.key().canonical_json()
        {
            return Err(Error::InvalidBinding);
        }
        super::mcp_instance::validate_owner(&mut tx, binding.key().owner())
            .await
            .map_err(|e| match e {
                kiln_core::McpInstanceError::Unavailable => Error::Unavailable,
                _ => Error::InvalidBinding,
            })?;
        let SharedMcpTransport::Stdio {
            arguments,
            environment,
            ..
        } = &definition.server().transport
        else {
            return Err(Error::InvalidBinding);
        };
        let used = match binding.purpose() {
            McpSecretPurpose::Argument => arguments.iter().any(
                |arg| matches!(arg, SharedMcpArgument::HostBinding(name) if name == binding.name()),
            ),
            McpSecretPurpose::Environment => {
                environment.values().any(|name| name == binding.name())
            }
        };
        if !used {
            return Err(Error::InvalidBinding);
        }
        sqlx::query("INSERT INTO mcp_secret_reservations (secret_ref, kiln_instance_id, instance_key, binding_name, purpose, definition_version, state) VALUES (?, ?, ?, ?, ?, ?, 'reserved')")
            .bind(binding.secret_ref().as_str()).bind(binding.instance_id().as_str())
            .bind(binding.key().canonical_json()).bind(binding.name().as_str())
            .bind(binding.purpose().as_str()).bind(version)
            .execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(McpSecretReservation::Fresh)
    }

    async fn retire_mcp_secret_reservation(
        &self,
        binding: &McpSecretBinding,
    ) -> Result<State, Error> {
        let mut connection = self.connection.lock().await;
        let mut tx = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let row = load(&mut tx, binding.secret_ref())
            .await?
            .ok_or(Error::NotFound)?;
        check_identity(&row, binding)?;
        let current = state(&row)?;
        if current == State::Reserved {
            sqlx::query(
                "UPDATE mcp_secret_reservations SET state = 'retired' WHERE secret_ref = ?",
            )
            .bind(binding.secret_ref().as_str())
            .execute(&mut *tx)
            .await
            .map_err(|_| Error::Unavailable)?;
        }
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(if current == State::Deleted {
            State::Deleted
        } else {
            State::Retired
        })
    }

    async fn finish_mcp_secret_deletion(&self, binding: &McpSecretBinding) -> Result<(), Error> {
        let mut connection = self.connection.lock().await;
        let mut tx = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let row = load(&mut tx, binding.secret_ref())
            .await?
            .ok_or(Error::NotFound)?;
        check_identity(&row, binding)?;
        match state(&row)? {
            State::Reserved => return Err(Error::Conflict),
            State::Retired => {
                sqlx::query(
                    "UPDATE mcp_secret_reservations SET state = 'deleted' WHERE secret_ref = ?",
                )
                .bind(binding.secret_ref().as_str())
                .execute(&mut *tx)
                .await
                .map_err(|_| Error::Unavailable)?;
            }
            State::Deleted => {}
        }
        tx.commit().await.map_err(|_| Error::Unavailable)
    }

    async fn pending_mcp_secret_reservations(
        &self,
        instance_id: &KilnInstanceId,
        key: &McpInstanceKey,
        batch_size: std::num::NonZeroUsize,
    ) -> Result<Vec<(McpSecretBinding, State)>, Error> {
        let limit = i64::try_from(batch_size.get()).map_err(|_| Error::InvalidRequest)?;
        let mut connection = self.connection.lock().await;
        let rows = sqlx::query("SELECT secret_ref, binding_name, purpose, state FROM mcp_secret_reservations WHERE kiln_instance_id = ? AND instance_key = ? AND state != 'deleted' ORDER BY secret_ref LIMIT ?")
            .bind(instance_id.as_str()).bind(key.canonical_json()).bind(limit)
            .fetch_all(&mut *connection).await.map_err(|_| Error::Unavailable)?;
        rows.into_iter()
            .map(|row| {
                let name: String = row
                    .try_get("binding_name")
                    .map_err(|_| Error::IntegrityViolation)?;
                let reference: String = row
                    .try_get("secret_ref")
                    .map_err(|_| Error::IntegrityViolation)?;
                let purpose: String = row
                    .try_get("purpose")
                    .map_err(|_| Error::IntegrityViolation)?;
                Ok((
                    McpSecretBinding::new(
                        instance_id.clone(),
                        key.clone(),
                        SharedConfigurationKey::parse(&name, name.len())
                            .map_err(|_| Error::IntegrityViolation)?,
                        match purpose.as_str() {
                            "argument" => McpSecretPurpose::Argument,
                            "environment" => McpSecretPurpose::Environment,
                            _ => return Err(Error::IntegrityViolation),
                        },
                        SecretRef::parse(reference).map_err(|_| Error::IntegrityViolation)?,
                    ),
                    state(&row)?,
                ))
            })
            .collect()
    }
}

async fn load(
    connection: &mut SqliteConnection,
    reference: &SecretRef,
) -> Result<Option<SqliteRow>, Error> {
    sqlx::query("SELECT kiln_instance_id, instance_key, binding_name, purpose, definition_version, state FROM mcp_secret_reservations WHERE secret_ref = ?")
        .bind(reference.as_str()).fetch_optional(connection).await.map_err(|_| Error::Unavailable)
}

fn check_identity(row: &SqliteRow, binding: &McpSecretBinding) -> Result<(), Error> {
    for (column, expected) in [
        ("kiln_instance_id", binding.instance_id().as_str()),
        ("instance_key", binding.key().canonical_json()),
        ("binding_name", binding.name().as_str()),
        ("purpose", binding.purpose().as_str()),
    ] {
        let actual: String = row.try_get(column).map_err(|_| Error::IntegrityViolation)?;
        if actual != expected {
            return Err(Error::Conflict);
        }
    }
    Ok(())
}

fn state(row: &SqliteRow) -> Result<State, Error> {
    let value: String = row
        .try_get("state")
        .map_err(|_| Error::IntegrityViolation)?;
    match value.as_str() {
        "reserved" => Ok(State::Reserved),
        "retired" => Ok(State::Retired),
        "deleted" => Ok(State::Deleted),
        _ => Err(Error::IntegrityViolation),
    }
}
