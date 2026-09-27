use super::SqliteStore;
use kiln_core::{
    McpDefinitionLimits, McpHostBindingError as Error, McpHostBindingRecord, McpHostBindingStore,
    McpHostBindings, McpHostTransportBindings, McpInstanceKey, SharedMcpArgument,
    SharedMcpTransport,
};
use sqlx::{Connection, Row, SqliteConnection};
use std::num::NonZeroU64;

impl McpHostBindingStore for SqliteStore {
    async fn inspect_mcp_host_binding_publication(
        &self,
        bindings: &McpHostBindings,
        expected_revision: u64,
        limits: McpDefinitionLimits,
    ) -> Result<Option<McpHostBindingRecord>, Error> {
        let next = expected_revision
            .checked_add(1)
            .and_then(|v| i64::try_from(v).ok())
            .ok_or(Error::InvalidRequest)?;
        McpHostBindings::from_metadata_json(
            bindings.key().clone(),
            bindings.metadata_json().as_bytes(),
            limits,
        )?;
        let mut connection = self.connection.lock().await;
        let mut tx = connection.begin().await.map_err(|_| Error::Unavailable)?;
        validate_publication(&mut tx, bindings, next, limits).await
    }
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
        if let Some(receipt) = validate_publication(&mut tx, bindings, next, limits).await? {
            return Ok(receipt);
        }
        let current = (expected_revision > 0).then_some(next - 1);
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
        for (purpose, name, reference) in bindings.references() {
            sqlx::query("INSERT INTO mcp_host_binding_refs(instance_key, purpose, binding_name, secret_ref) VALUES (?, ?, ?, ?)")
                .bind(bindings.key().canonical_json()).bind(purpose.as_str()).bind(name.as_str()).bind(reference.as_str())
                .execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        }
        if let Some(old) = old {
            retire_unpublished_references(&mut tx, &old.bindings).await?;
        }
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(McpHostBindingRecord {
            bindings: validated,
            revision: NonZeroU64::new(next as u64).expect("positive revision"),
            retired: false,
        })
    }

    async fn retire_mcp_host_bindings(
        &self,
        key: &McpInstanceKey,
        expected_revision: NonZeroU64,
        limits: McpDefinitionLimits,
    ) -> Result<McpHostBindingRecord, Error> {
        limits.validate().map_err(|_| Error::InvalidRequest)?;
        let next = expected_revision
            .get()
            .checked_add(1)
            .and_then(|v| i64::try_from(v).ok())
            .ok_or(Error::InvalidRequest)?;
        let mut connection = self.connection.lock().await;
        let mut tx = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        if let Some(receipt) = load_version(&mut tx, key, next, limits).await? {
            return if receipt.retired {
                Ok(receipt)
            } else {
                Err(Error::Conflict)
            };
        }
        let current: Option<i64> =
            sqlx::query_scalar("SELECT revision FROM mcp_host_bindings WHERE instance_key = ?")
                .bind(key.canonical_json())
                .fetch_optional(&mut *tx)
                .await
                .map_err(|_| Error::Unavailable)?;
        if current.ok_or(Error::NotFound)? != next - 1 {
            return Err(Error::Conflict);
        }
        let old = load_version(&mut tx, key, next - 1, limits)
            .await?
            .ok_or(Error::IntegrityViolation)?;
        if old.retired {
            return Err(Error::Conflict);
        }
        ensure_inactive(&mut tx, key).await?;
        sqlx::query("INSERT INTO mcp_host_binding_versions(instance_key, revision, metadata_json, retired) VALUES (?, ?, ?, 1)")
            .bind(key.canonical_json()).bind(next).bind(old.bindings.metadata_json())
            .execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        sqlx::query("UPDATE mcp_host_bindings SET revision = ? WHERE instance_key = ?")
            .bind(next)
            .bind(key.canonical_json())
            .execute(&mut *tx)
            .await
            .map_err(|_| Error::Unavailable)?;
        sqlx::query("DELETE FROM mcp_host_binding_refs WHERE instance_key = ?")
            .bind(key.canonical_json())
            .execute(&mut *tx)
            .await
            .map_err(|_| Error::Unavailable)?;
        retire_unpublished_references(&mut tx, &old.bindings).await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(McpHostBindingRecord {
            bindings: old.bindings,
            revision: NonZeroU64::new(next as u64).expect("positive revision"),
            retired: true,
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

/// The caller holds the same IMMEDIATE transaction used by generation claims;
/// checking outside it would race resolved credentials against reconfiguration.
async fn ensure_inactive(
    connection: &mut SqliteConnection,
    key: &McpInstanceKey,
) -> Result<(), Error> {
    let active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM mcp_instances i JOIN mcp_instance_generations g USING(instance_key, generation_id) WHERE i.instance_key = ? AND g.observed IN ('starting','ready','stopping','interrupted'))")
        .bind(key.canonical_json()).fetch_one(connection).await.map_err(|_| Error::Unavailable)?;
    if active {
        Err(Error::ActiveGeneration)
    } else {
        Ok(())
    }
}

async fn retire_unpublished_references(
    connection: &mut SqliteConnection,
    bindings: &McpHostBindings,
) -> Result<(), Error> {
    for (_, _, reference) in bindings.references() {
        sqlx::query("UPDATE mcp_secret_reservations SET state = 'retired' WHERE secret_ref = ? AND state = 'reserved' AND NOT EXISTS(SELECT 1 FROM mcp_host_binding_refs WHERE secret_ref = ?)")
            .bind(reference.as_str()).bind(reference.as_str()).execute(&mut *connection).await.map_err(|_| Error::Unavailable)?;
    }
    Ok(())
}

pub(super) async fn load_version(
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
    let row = sqlx::query("SELECT retired, CASE WHEN length(CAST(metadata_json AS BLOB)) <= ? THEN metadata_json END AS metadata FROM mcp_host_binding_versions WHERE instance_key = ? AND revision = ?")
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
        let retired: bool = row
            .try_get("retired")
            .map_err(|_| Error::IntegrityViolation)?;
        Ok(McpHostBindingRecord {
            bindings,
            revision,
            retired,
        })
    })
    .transpose()
}

async fn validate_publication(
    connection: &mut SqliteConnection,
    bindings: &McpHostBindings,
    next: i64,
    limits: McpDefinitionLimits,
) -> Result<Option<McpHostBindingRecord>, Error> {
    if let Some(record) = load_version(&mut *connection, bindings.key(), next, limits).await? {
        if record.retired || record.bindings.metadata_json() != bindings.metadata_json() {
            return Err(Error::Conflict);
        }
        return Ok(Some(record));
    }
    let current: Option<i64> =
        sqlx::query_scalar("SELECT revision FROM mcp_host_bindings WHERE instance_key = ?")
            .bind(bindings.key().canonical_json())
            .fetch_optional(&mut *connection)
            .await
            .map_err(|_| Error::Unavailable)?;
    if current.unwrap_or(0) != next - 1 {
        return Err(Error::Conflict);
    }
    ensure_inactive(&mut *connection, bindings.key()).await?;
    let version: Option<i64> =
        sqlx::query_scalar("SELECT version FROM mcp_definitions WHERE definition_id = ?")
            .bind(bindings.key().definition_id().as_str())
            .fetch_optional(&mut *connection)
            .await
            .map_err(|_| Error::Unavailable)?;
    if version != Some(bindings.definition_version() as i64) {
        return Err(Error::DefinitionChanged);
    }
    let definition = super::mcp_definition::load_version(
        &mut *connection,
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
    super::mcp_instance::validate_owner(&mut *connection, bindings.key().owner())
        .await
        .map_err(|e| match e {
            kiln_core::McpInstanceError::Unavailable => Error::Unavailable,
            _ => Error::InvalidBinding,
        })?;
    validate_directory(&mut *connection, bindings).await?;
    let valid_transport = match (&definition.server().transport, bindings.transport()) {
        (
            SharedMcpTransport::Stdio {
                runtime_binding,
                arguments,
                environment,
            },
            McpHostTransportBindings::Stdio {
                runtime_binding: host_runtime,
                arguments: host_arguments,
                environment: host_environment,
                ..
            },
        ) => {
            let argument_names: std::collections::BTreeSet<_> = arguments
                .iter()
                .filter_map(|arg| match arg {
                    SharedMcpArgument::HostBinding(name) => Some(name),
                    _ => None,
                })
                .collect();
            let environment_names: std::collections::BTreeSet<_> = environment.values().collect();
            runtime_binding == host_runtime
                && argument_names == host_arguments.keys().collect()
                && environment_names == host_environment.keys().collect()
        }
        (
            SharedMcpTransport::Https {
                endpoint,
                credential_binding,
            },
            McpHostTransportBindings::Http {
                endpoint: host_endpoint,
                endpoint_binding,
                credential,
            },
        ) => {
            endpoint == host_endpoint
                && endpoint_binding.is_none()
                && credential_binding.as_ref() == credential.as_ref().map(|(name, _)| name)
        }
        (
            SharedMcpTransport::HostEndpoint { endpoint_binding },
            McpHostTransportBindings::Http {
                endpoint_binding: host_binding,
                credential,
                ..
            },
        ) => host_binding.as_ref() == Some(endpoint_binding) && credential.is_none(),
        _ => false,
    };
    if !valid_transport {
        return Err(Error::InvalidBinding);
    }
    for (purpose, name, reference) in bindings.references() {
        let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM mcp_secret_reservations WHERE secret_ref = ? AND kiln_instance_id = ? AND instance_key = ? AND binding_name = ? AND purpose = ? AND state = 'reserved')")
                .bind(reference.as_str()).bind(bindings.instance_id().as_str()).bind(bindings.key().canonical_json())
                .bind(name.as_str()).bind(purpose.as_str()).fetch_one(&mut *connection).await.map_err(|_| Error::Unavailable)?;
        if !valid {
            return Err(Error::InvalidBinding);
        }
    }
    Ok(None)
}

/// Rechecked transactionally at publication and generation admission.
pub(super) async fn validate_directory(
    connection: &mut SqliteConnection,
    bindings: &McpHostBindings,
) -> Result<(), Error> {
    if let Some(directory) = bindings.working_directory() {
        super::mcp_instance::validate_owner(
            &mut *connection,
            &kiln_core::McpInstanceOwner::WorkspaceCheckout(directory.clone()),
        )
        .await
        .map_err(|e| match e {
            kiln_core::McpInstanceError::Unavailable => Error::Unavailable,
            _ => Error::InvalidBinding,
        })?;
        if let kiln_core::McpInstanceOwner::Session(id) = bindings.key().owner() {
            let matches: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM sessions WHERE session_id = ? AND workspace_id = ?)",
            )
            .bind(id.as_str())
            .bind(directory.workspace_id().as_str())
            .fetch_one(&mut *connection)
            .await
            .map_err(|_| Error::Unavailable)?;
            if !matches {
                return Err(Error::InvalidBinding);
            }
        }
    }
    Ok(())
}
