use super::SqliteStore;
use kiln_core::{
    McpDefinitionLimits, McpHostBindingError as Error, McpHostBindingRecord, McpHostBindingStore,
    McpHostBindings, McpInstanceKey, McpSecretPurpose, SharedMcpArgument, SharedMcpTransport,
};
use sqlx::{Connection, Row, SqliteConnection};
use std::num::NonZeroU64;

impl McpHostBindingStore for SqliteStore {
    async fn publish_mcp_host_bindings(
        &self,
        bindings: &McpHostBindings,
        expected_revision: u64,
        limits: McpDefinitionLimits,
    ) -> Result<McpHostBindingRecord, Error> {
        let next = expected_revision
            .checked_add(1)
            .and_then(|v| i64::try_from(v).ok())
            .ok_or(Error::InvalidRequest)?;
        let validated = McpHostBindings::from_metadata_json(
            bindings.key().clone(),
            bindings.metadata_json().as_bytes(),
            limits,
        )?;
        let mut connection = self.connection.lock().await;
        let mut tx = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        if let Some(record) = load_version(&mut tx, bindings.key(), next, limits).await? {
            if record.bindings.metadata_json() != bindings.metadata_json() {
                return Err(Error::Conflict);
            }
            return Ok(record);
        }
        let current: Option<i64> =
            sqlx::query_scalar("SELECT revision FROM mcp_host_bindings WHERE instance_key = ?")
                .bind(bindings.key().canonical_json())
                .fetch_optional(&mut *tx)
                .await
                .map_err(|_| Error::Unavailable)?;
        if current.unwrap_or(0) != next - 1 {
            return Err(Error::Conflict);
        }
        // The same IMMEDIATE transaction as generation claims closes the gap
        // between resolving a snapshot and reserving its process generation.
        let active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM mcp_instances i JOIN mcp_instance_generations g USING(instance_key, generation_id) WHERE i.instance_key = ? AND g.observed IN ('starting','ready','stopping','interrupted'))")
            .bind(bindings.key().canonical_json()).fetch_one(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        if active {
            return Err(Error::ActiveGeneration);
        }
        let version: Option<i64> =
            sqlx::query_scalar("SELECT version FROM mcp_definitions WHERE definition_id = ?")
                .bind(bindings.key().definition_id().as_str())
                .fetch_optional(&mut *tx)
                .await
                .map_err(|_| Error::Unavailable)?;
        if version != Some(bindings.definition_version() as i64) {
            return Err(Error::DefinitionChanged);
        }
        let definition = super::mcp_definition::load_version(
            &mut tx,
            bindings.key().definition_id(),
            bindings.definition_version() as i64,
            limits,
        )
        .await
        .map_err(|e| match e {
            kiln_core::McpDefinitionError::LimitExceeded => Error::LimitExceeded,
            kiln_core::McpDefinitionError::Unavailable => Error::Unavailable,
            _ => Error::IntegrityViolation,
        })?
        .definition;
        if !definition.server().enabled
            || McpInstanceKey::new(
                &definition,
                bindings.key().owner().clone(),
                limits.max_metadata_bytes,
            )
            .map_err(|_| Error::InvalidBinding)?
            .canonical_json()
                != bindings.key().canonical_json()
        {
            return Err(Error::InvalidBinding);
        }
        super::mcp_instance::validate_owner(&mut tx, bindings.key().owner())
            .await
            .map_err(|e| match e {
                kiln_core::McpInstanceError::Unavailable => Error::Unavailable,
                _ => Error::InvalidBinding,
            })?;
        let SharedMcpTransport::Stdio {
            runtime_binding,
            arguments,
            environment,
        } = &definition.server().transport
        else {
            return Err(Error::InvalidBinding);
        };
        let argument_names: std::collections::BTreeSet<_> = arguments
            .iter()
            .filter_map(|arg| match arg {
                SharedMcpArgument::HostBinding(name) => Some(name),
                _ => None,
            })
            .collect();
        let environment_names: std::collections::BTreeSet<_> = environment.values().collect();
        if runtime_binding != bindings.runtime_binding()
            || argument_names != bindings.arguments().keys().collect()
            || environment_names != bindings.environment().keys().collect()
        {
            return Err(Error::InvalidBinding);
        }
        for (purpose, name, reference) in references(bindings) {
            let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM mcp_secret_reservations WHERE secret_ref = ? AND kiln_instance_id = ? AND instance_key = ? AND binding_name = ? AND purpose = ? AND state = 'reserved')")
                .bind(reference.as_str()).bind(bindings.instance_id().as_str()).bind(bindings.key().canonical_json())
                .bind(name.as_str()).bind(purpose.as_str()).fetch_one(&mut *tx).await.map_err(|_| Error::Unavailable)?;
            if !valid {
                return Err(Error::InvalidBinding);
            }
        }
        let old = match current {
            Some(revision) => Some(
                load_version(&mut tx, bindings.key(), revision, limits)
                    .await?
                    .ok_or(Error::IntegrityViolation)?,
            ),
            None => None,
        };
        sqlx::query("INSERT INTO mcp_host_binding_versions(instance_key, revision, metadata_json) VALUES (?, ?, ?)")
            .bind(bindings.key().canonical_json()).bind(next).bind(bindings.metadata_json()).execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        sqlx::query("INSERT INTO mcp_host_bindings(instance_key, revision) VALUES (?, ?) ON CONFLICT(instance_key) DO UPDATE SET revision = excluded.revision")
            .bind(bindings.key().canonical_json()).bind(next).execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        sqlx::query("DELETE FROM mcp_host_binding_refs WHERE instance_key = ?")
            .bind(bindings.key().canonical_json())
            .execute(&mut *tx)
            .await
            .map_err(|_| Error::Unavailable)?;
        for (purpose, name, reference) in references(bindings) {
            sqlx::query("INSERT INTO mcp_host_binding_refs(instance_key, purpose, binding_name, secret_ref) VALUES (?, ?, ?, ?)")
                .bind(bindings.key().canonical_json()).bind(purpose.as_str()).bind(name.as_str()).bind(reference.as_str())
                .execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        }
        if let Some(old) = old {
            for (_, _, reference) in references(&old.bindings) {
                sqlx::query("UPDATE mcp_secret_reservations SET state = 'retired' WHERE secret_ref = ? AND state = 'reserved' AND NOT EXISTS(SELECT 1 FROM mcp_host_binding_refs WHERE secret_ref = ?)")
                    .bind(reference.as_str()).bind(reference.as_str()).execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
            }
        }
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(McpHostBindingRecord {
            bindings: validated,
            revision: NonZeroU64::new(next as u64).expect("positive revision"),
        })
    }

    async fn get_mcp_host_bindings(
        &self,
        key: &McpInstanceKey,
        limits: McpDefinitionLimits,
    ) -> Result<Option<McpHostBindingRecord>, Error> {
        let mut connection = self.connection.lock().await;
        let revision: Option<i64> =
            sqlx::query_scalar("SELECT revision FROM mcp_host_bindings WHERE instance_key = ?")
                .bind(key.canonical_json())
                .fetch_optional(&mut *connection)
                .await
                .map_err(|_| Error::Unavailable)?;
        match revision {
            Some(revision) => load_version(&mut connection, key, revision, limits)
                .await?
                .map(Some)
                .ok_or(Error::IntegrityViolation),
            None => Ok(None),
        }
    }
}

fn references(
    bindings: &McpHostBindings,
) -> impl Iterator<
    Item = (
        McpSecretPurpose,
        &kiln_core::SharedConfigurationKey,
        &kiln_core::SecretRef,
    ),
> {
    bindings
        .arguments()
        .iter()
        .map(|(name, reference)| (McpSecretPurpose::Argument, name, reference))
        .chain(
            bindings
                .environment()
                .iter()
                .map(|(name, reference)| (McpSecretPurpose::Environment, name, reference)),
        )
}

async fn load_version(
    connection: &mut SqliteConnection,
    key: &McpInstanceKey,
    revision: i64,
    limits: McpDefinitionLimits,
) -> Result<Option<McpHostBindingRecord>, Error> {
    let budget = limits
        .max_metadata_bytes
        .checked_sub(key.canonical_json().len())
        .ok_or(Error::LimitExceeded)?;
    // Check the stored byte count before materializing private snapshot metadata.
    let row = sqlx::query("SELECT length(CAST(metadata_json AS BLOB)) AS bytes, CASE WHEN length(CAST(metadata_json AS BLOB)) <= ? THEN metadata_json END AS metadata FROM mcp_host_binding_versions WHERE instance_key = ? AND revision = ?")
        .bind(i64::try_from(budget).map_err(|_| Error::InvalidRequest)?).bind(key.canonical_json()).bind(revision)
        .fetch_optional(connection).await.map_err(|_| Error::Unavailable)?;
    row.map(|row| {
        let metadata: Option<String> = row
            .try_get("metadata")
            .map_err(|_| Error::IntegrityViolation)?;
        let metadata = metadata.ok_or(Error::LimitExceeded)?;
        let bindings =
            McpHostBindings::from_metadata_json(key.clone(), metadata.as_bytes(), limits).map_err(
                |e| match e {
                    Error::LimitExceeded => e,
                    _ => Error::IntegrityViolation,
                },
            )?;
        let revision = u64::try_from(revision)
            .ok()
            .and_then(NonZeroU64::new)
            .ok_or(Error::IntegrityViolation)?;
        Ok(McpHostBindingRecord { bindings, revision })
    })
    .transpose()
}
