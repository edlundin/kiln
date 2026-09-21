use kiln_core::{
    ModelInvocation, ModelInvocationId, ModelInvocationState, ModelToolCatalog,
    ModelToolCatalogError, ModelToolCatalogLimits, ModelToolCatalogStore, ModelToolDefinitionInput,
};
use serde_json::{Map, Value};
use sqlx::{Connection, Row, Sqlite, Transaction};

use super::{SqliteStore, hash_bytes, load_model_invocation};

impl ModelToolCatalogStore for SqliteStore {
    async fn attach_model_tool_catalog(
        &self,
        invocation_id: &ModelInvocationId,
        catalog: &ModelToolCatalog,
    ) -> Result<ModelToolCatalog, ModelToolCatalogError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| ModelToolCatalogError::Unavailable)?;
        let invocation = load_model_invocation(&mut transaction, invocation_id)
            .await
            .map_err(ModelToolCatalogError::Invocation)?
            .ok_or(ModelToolCatalogError::InvalidInvocation)?;
        catalog.validate_for(&invocation)?;
        validate_retry(&mut transaction, &invocation, catalog).await?;
        if let Some(previous) = load_catalog(&mut transaction, &invocation).await? {
            if previous != *catalog {
                return Err(ModelToolCatalogError::IdempotencyConflict);
            }
        } else {
            if invocation.state() != ModelInvocationState::Pending {
                return Err(ModelToolCatalogError::InvalidInvocation);
            }
            insert_catalog(&mut transaction, invocation_id, catalog).await?;
        }
        transaction
            .commit()
            .await
            .map_err(|_| ModelToolCatalogError::Unavailable)?;
        Ok(catalog.clone())
    }

    async fn get_model_tool_catalog(
        &self,
        invocation_id: &ModelInvocationId,
    ) -> Result<Option<ModelToolCatalog>, ModelToolCatalogError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| ModelToolCatalogError::Unavailable)?;
        let invocation = load_model_invocation(&mut transaction, invocation_id)
            .await
            .map_err(ModelToolCatalogError::Invocation)?
            .ok_or(ModelToolCatalogError::InvalidInvocation)?;
        let catalog = load_catalog(&mut transaction, &invocation).await?;
        if let Some(catalog) = &catalog {
            validate_retry(&mut transaction, &invocation, catalog).await?;
        }
        transaction
            .commit()
            .await
            .map_err(|_| ModelToolCatalogError::Unavailable)?;
        Ok(catalog)
    }
}

/// Called in the claim transaction before moving the invocation to in_flight.
/// No catalog can subsequently be attached or replaced through the store API.
pub(super) async fn freeze_catalog(
    transaction: &mut Transaction<'_, Sqlite>,
    invocation: &ModelInvocation,
) -> Result<(), ModelToolCatalogError> {
    if let Some(catalog) = load_catalog(transaction, invocation).await? {
        validate_retry(transaction, invocation, &catalog).await?;
    } else {
        let catalog = retry_catalog(transaction, invocation)
            .await?
            .unwrap_or_else(ModelToolCatalog::empty);
        catalog.validate_for(invocation)?;
        insert_catalog(transaction, invocation.invocation_id(), &catalog).await?;
    }
    Ok(())
}

async fn retry_catalog(
    transaction: &mut Transaction<'_, Sqlite>,
    invocation: &ModelInvocation,
) -> Result<Option<ModelToolCatalog>, ModelToolCatalogError> {
    let Some(previous_id) = invocation.retry_of() else {
        return Ok(None);
    };
    let previous = load_model_invocation(transaction, previous_id)
        .await
        .map_err(ModelToolCatalogError::Invocation)?
        .ok_or(ModelToolCatalogError::IntegrityViolation)?;
    // Historical attempts without descriptions have no tool authority. A retry
    // must retain that empty catalog rather than use a newly configured registry.
    Ok(Some(
        load_catalog(transaction, &previous)
            .await?
            .unwrap_or_else(ModelToolCatalog::empty),
    ))
}

async fn validate_retry(
    transaction: &mut Transaction<'_, Sqlite>,
    invocation: &ModelInvocation,
    catalog: &ModelToolCatalog,
) -> Result<(), ModelToolCatalogError> {
    if retry_catalog(transaction, invocation)
        .await?
        .is_some_and(|previous| previous != *catalog)
    {
        return Err(ModelToolCatalogError::RetryMismatch);
    }
    Ok(())
}

async fn insert_catalog(
    transaction: &mut Transaction<'_, Sqlite>,
    invocation_id: &ModelInvocationId,
    catalog: &ModelToolCatalog,
) -> Result<(), ModelToolCatalogError> {
    sqlx::query(
        "INSERT INTO model_tool_catalogs
        (model_invocation_id, content_hash, tool_count) VALUES (?, ?, ?)",
    )
    .bind(invocation_id.as_str())
    .bind(hash_bytes(&catalog.canonical_bytes()).as_str())
    .bind(
        i64::try_from(catalog.definitions().len())
            .map_err(|_| ModelToolCatalogError::LimitExceeded)?,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|_| ModelToolCatalogError::Unavailable)?;
    for (position, definition) in catalog.definitions().iter().enumerate() {
        sqlx::query(
            "INSERT INTO model_tool_definitions
            (model_invocation_id, position, definition_json) VALUES (?, ?, ?)",
        )
        .bind(invocation_id.as_str())
        .bind(i64::try_from(position).map_err(|_| ModelToolCatalogError::LimitExceeded)?)
        .bind(definition.definition_json())
        .execute(&mut **transaction)
        .await
        .map_err(|_| ModelToolCatalogError::Unavailable)?;
    }
    Ok(())
}

pub(super) async fn load_catalog(
    transaction: &mut Transaction<'_, Sqlite>,
    invocation: &ModelInvocation,
) -> Result<Option<ModelToolCatalog>, ModelToolCatalogError> {
    let header = sqlx::query(
        "SELECT content_hash, tool_count FROM model_tool_catalogs
        WHERE model_invocation_id = ?",
    )
    .bind(invocation.invocation_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| ModelToolCatalogError::Unavailable)?;
    let Some(header) = header else {
        return Ok(None);
    };
    let stored_hash: String = header
        .try_get("content_hash")
        .map_err(|_| ModelToolCatalogError::IntegrityViolation)?;
    let count = usize::try_from(
        header
            .try_get::<i64, _>("tool_count")
            .map_err(|_| ModelToolCatalogError::IntegrityViolation)?,
    )
    .map_err(|_| ModelToolCatalogError::IntegrityViolation)?;
    let rows = sqlx::query(
        "SELECT position, definition_json FROM model_tool_definitions
        WHERE model_invocation_id = ? ORDER BY position ASC",
    )
    .bind(invocation.invocation_id().as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(|_| ModelToolCatalogError::Unavailable)?;
    if rows.len() != count {
        return Err(ModelToolCatalogError::IntegrityViolation);
    }
    let mut limits = ModelToolCatalogLimits {
        max_tools: count.max(1),
        max_definition_bytes: 1,
        max_total_definition_bytes: 1,
    };
    let mut total_bytes = 0_usize;
    let mut inputs = Vec::with_capacity(count);
    for (position, row) in rows.into_iter().enumerate() {
        if usize::try_from(
            row.try_get::<i64, _>("position")
                .map_err(|_| ModelToolCatalogError::IntegrityViolation)?,
        )
        .ok()
            != Some(position)
        {
            return Err(ModelToolCatalogError::IntegrityViolation);
        }
        let json: String = row
            .try_get("definition_json")
            .map_err(|_| ModelToolCatalogError::IntegrityViolation)?;
        total_bytes = total_bytes
            .checked_add(json.len())
            .ok_or(ModelToolCatalogError::IntegrityViolation)?;
        limits.max_definition_bytes = limits.max_definition_bytes.max(json.len());
        let mut object: Map<String, Value> =
            serde_json::from_str(&json).map_err(|_| ModelToolCatalogError::IntegrityViolation)?;
        let input = ModelToolDefinitionInput {
            name: take_string(&mut object, "name")?,
            description: take_string(&mut object, "description")?,
            capability: take_string(&mut object, "capability")?,
            revision: take_string(&mut object, "revision")?,
            input_schema: match object.remove("input_schema") {
                Some(Value::Object(schema)) => schema,
                _ => return Err(ModelToolCatalogError::IntegrityViolation),
            },
        };
        if !object.is_empty() {
            return Err(ModelToolCatalogError::IntegrityViolation);
        }
        inputs.push(input);
    }
    limits.max_total_definition_bytes = total_bytes.max(1);
    let catalog = ModelToolCatalog::new(inputs, limits)
        .map_err(|_| ModelToolCatalogError::IntegrityViolation)?;
    catalog
        .validate_for(invocation)
        .map_err(|_| ModelToolCatalogError::IntegrityViolation)?;
    if hash_bytes(&catalog.canonical_bytes()).as_str() != stored_hash {
        return Err(ModelToolCatalogError::IntegrityViolation);
    }
    Ok(Some(catalog))
}

fn take_string(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<String, ModelToolCatalogError> {
    match object.remove(key) {
        Some(Value::String(value)) => Ok(value),
        _ => Err(ModelToolCatalogError::IntegrityViolation),
    }
}
