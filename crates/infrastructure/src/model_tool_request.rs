use kiln_core::{
    CapabilitySupport, ModelInvocation, ModelInvocationCompletionIds,
    ModelInvocationCompletionKind, ModelInvocationCompletionMutation, ModelInvocationId,
    ModelInvocationMutationDisposition, ModelInvocationOutcome, ModelInvocationPurpose,
    ModelInvocationState, ModelToolRequestBatch, ModelToolRequestCompletion, ModelToolRequestError,
    ModelToolRequestInput, ModelToolRequestLimits, ModelToolRequestStore, ProviderUsageUpdate,
};
use sqlx::{Connection, Row};

use super::{
    SqliteStore, finish_model_invocation_in_transaction, hash_bytes, load_model_invocation, usage,
};

impl ModelToolRequestStore for SqliteStore {
    async fn finish_model_invocation_with_tool_requests(
        &self,
        invocation: &ModelInvocation,
        requests: &ModelToolRequestBatch,
        update: &ProviderUsageUpdate,
        ids: ModelInvocationCompletionIds,
    ) -> Result<ModelToolRequestCompletion, ModelToolRequestError> {
        requests.validate_completion(invocation, update)?;
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| ModelToolRequestError::Unavailable)?;
        let current = load_model_invocation(&mut transaction, invocation.invocation_id())
            .await
            .map_err(ModelToolRequestError::Invocation)?
            .ok_or(ModelToolRequestError::InvalidInvocation)?;
        requests.validate_completion(&current, update)?;
        let previous = load_requests(&mut transaction, &current).await?;
        if let Some(previous) = previous {
            if previous != *requests {
                return Err(ModelToolRequestError::IdempotencyConflict);
            }
        } else {
            // Never graft proposals onto a completed/cancelled invocation. The
            // initial batch, final usage, and completion must share one commit.
            if current.state() != ModelInvocationState::InFlight {
                return Err(ModelToolRequestError::InvalidInvocation);
            }
            let count = i64::try_from(requests.requests().len())
                .map_err(|_| ModelToolRequestError::RequestLimitExceeded)?;
            sqlx::query(
                "INSERT INTO model_tool_request_batches
                    (model_invocation_id, content_hash, request_count) VALUES (?, ?, ?)",
            )
            .bind(invocation.invocation_id().as_str())
            .bind(hash_bytes(&requests.canonical_bytes()).as_str())
            .bind(count)
            .execute(&mut *transaction)
            .await
            .map_err(|_| ModelToolRequestError::Unavailable)?;
            for (position, request) in requests.requests().iter().enumerate() {
                sqlx::query(
                    "INSERT INTO model_tool_requests
                        (model_invocation_id, position, provider_call_id, name, arguments_json)
                     VALUES (?, ?, ?, ?, ?)",
                )
                .bind(invocation.invocation_id().as_str())
                .bind(
                    i64::try_from(position)
                        .map_err(|_| ModelToolRequestError::RequestLimitExceeded)?,
                )
                .bind(request.provider_call_id())
                .bind(request.name())
                .bind(request.arguments_json())
                .execute(&mut *transaction)
                .await
                .map_err(|_| ModelToolRequestError::Unavailable)?;
            }
        }
        let usage = usage::record_usage_in_transaction(
            &mut transaction,
            update,
            ids.usage_observation_id,
            ids.usage_event_id,
        )
        .await
        .map_err(ModelToolRequestError::Usage)?;
        let completed = finish_model_invocation_in_transaction(
            &mut transaction,
            invocation,
            ModelInvocationOutcome::completed(ModelInvocationCompletionKind::ToolRequests),
            ids.invocation_event_id,
        )
        .await
        .map_err(ModelToolRequestError::Invocation)?;
        let mut events = usage.events;
        events.extend(completed.events);
        let disposition = if events.is_empty() {
            ModelInvocationMutationDisposition::Duplicate
        } else {
            ModelInvocationMutationDisposition::Applied
        };
        transaction
            .commit()
            .await
            .map_err(|_| ModelToolRequestError::Unavailable)?;
        Ok(ModelToolRequestCompletion {
            completion: ModelInvocationCompletionMutation {
                invocation: completed.value,
                usage: usage.value,
                events,
                disposition,
            },
            requests: requests.clone(),
        })
    }

    async fn get_model_tool_requests(
        &self,
        invocation_id: &ModelInvocationId,
    ) -> Result<Option<ModelToolRequestBatch>, ModelToolRequestError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| ModelToolRequestError::Unavailable)?;
        let invocation = load_model_invocation(&mut transaction, invocation_id)
            .await
            .map_err(ModelToolRequestError::Invocation)?
            .ok_or(ModelToolRequestError::InvalidInvocation)?;
        let requests = load_requests(&mut transaction, &invocation).await?;
        transaction
            .commit()
            .await
            .map_err(|_| ModelToolRequestError::Unavailable)?;
        Ok(requests)
    }
}

pub(super) async fn load_requests(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    invocation: &ModelInvocation,
) -> Result<Option<ModelToolRequestBatch>, ModelToolRequestError> {
    let header = sqlx::query(
        "SELECT content_hash, request_count FROM model_tool_request_batches
         WHERE model_invocation_id = ?",
    )
    .bind(invocation.invocation_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| ModelToolRequestError::Unavailable)?;
    let Some(header) = header else {
        return Ok(None);
    };
    if invocation.outcome()
        != Some(ModelInvocationOutcome::completed(
            ModelInvocationCompletionKind::ToolRequests,
        ))
        || invocation.purpose() != ModelInvocationPurpose::Generation
        || invocation.capabilities().tool_calls() != CapabilitySupport::Supported
    {
        return Err(ModelToolRequestError::IntegrityViolation);
    }
    let stored_hash: String = header
        .try_get("content_hash")
        .map_err(|_| ModelToolRequestError::IntegrityViolation)?;
    let count = usize::try_from(
        header
            .try_get::<i64, _>("request_count")
            .map_err(|_| ModelToolRequestError::IntegrityViolation)?,
    )
    .map_err(|_| ModelToolRequestError::IntegrityViolation)?;
    let rows = sqlx::query(
        "SELECT position, provider_call_id, name, arguments_json FROM model_tool_requests
         WHERE model_invocation_id = ? ORDER BY position ASC",
    )
    .bind(invocation.invocation_id().as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(|_| ModelToolRequestError::Unavailable)?;
    if rows.len() != count || count == 0 {
        return Err(ModelToolRequestError::IntegrityViolation);
    }
    // Reconstruction uses exactly the stored byte/count bounds. It does not
    // invent provider limits, and canonical hashing detects altered proposals.
    let mut limits = ModelToolRequestLimits {
        max_requests: count,
        max_provider_call_id_bytes: 1,
        max_name_bytes: 1,
        max_arguments_bytes: 1,
        max_total_arguments_bytes: 0,
    };
    let mut inputs = Vec::with_capacity(count);
    for (position, row) in rows.into_iter().enumerate() {
        if usize::try_from(
            row.try_get::<i64, _>("position")
                .map_err(|_| ModelToolRequestError::IntegrityViolation)?,
        )
        .ok()
            != Some(position)
        {
            return Err(ModelToolRequestError::IntegrityViolation);
        }
        let provider_call_id: String = row
            .try_get("provider_call_id")
            .map_err(|_| ModelToolRequestError::IntegrityViolation)?;
        let name: String = row
            .try_get("name")
            .map_err(|_| ModelToolRequestError::IntegrityViolation)?;
        let json: String = row
            .try_get("arguments_json")
            .map_err(|_| ModelToolRequestError::IntegrityViolation)?;
        limits.max_provider_call_id_bytes = limits
            .max_provider_call_id_bytes
            .max(provider_call_id.len());
        limits.max_name_bytes = limits.max_name_bytes.max(name.len());
        limits.max_arguments_bytes = limits.max_arguments_bytes.max(json.len());
        limits.max_total_arguments_bytes = limits
            .max_total_arguments_bytes
            .checked_add(json.len())
            .ok_or(ModelToolRequestError::IntegrityViolation)?;
        let arguments = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&json)
            .map_err(|_| ModelToolRequestError::IntegrityViolation)?;
        inputs.push(ModelToolRequestInput {
            provider_call_id,
            name,
            arguments,
        });
    }
    let requests = ModelToolRequestBatch::new(invocation.invocation_id().clone(), inputs, limits)
        .map_err(|_| ModelToolRequestError::IntegrityViolation)?;
    if hash_bytes(&requests.canonical_bytes()).as_str() != stored_hash {
        return Err(ModelToolRequestError::IntegrityViolation);
    }
    Ok(Some(requests))
}
