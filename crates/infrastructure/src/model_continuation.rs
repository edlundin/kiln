use kiln_core::{
    FinishModelInvocationWithContinuation, ModelContinuationError as Error,
    ModelContinuationLimits, ModelContinuationStore, ModelInvocation, ModelInvocationCompletionIds,
    ModelInvocationCompletionKind, ModelInvocationCompletionMutation, ModelInvocationContinuation,
    ModelInvocationId, ModelInvocationState,
};
use sqlx::{Connection, Row, Sqlite, Transaction};

use super::{SqliteStore, load_model_invocation, model_invocation_matches};

impl ModelContinuationStore for SqliteStore {
    async fn finish_model_invocation_with_continuation(
        &self,
        command: &FinishModelInvocationWithContinuation,
        ids: ModelInvocationCompletionIds,
    ) -> Result<ModelInvocationCompletionMutation, Error> {
        command.validate()?;
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let invocation = &command.completion.invocation;
        let current = load_model_invocation(&mut transaction, invocation.invocation_id())
            .await
            .map_err(Error::Invocation)?
            .ok_or(Error::InvalidBinding)?;
        if !model_invocation_matches(&current, invocation) {
            return Err(Error::InvalidBinding);
        }
        command.continuation.validate_for(&current)?;
        let limits = ModelContinuationLimits {
            max_format_bytes: command.continuation.format().len(),
            max_payload_bytes: command.continuation.payload().len(),
        };
        let previous = load_continuation(&mut transaction, &current, limits)
            .await
            .map_err(|error| {
                if error == Error::LimitExceeded {
                    Error::IdempotencyConflict
                } else {
                    error
                }
            })?;
        match previous {
            Some(previous) if previous != command.continuation => {
                return Err(Error::IdempotencyConflict);
            }
            Some(_) => {}
            None => {
                // A completed legacy invocation cannot acquire continuation
                // state retroactively. Retries must match the original commit.
                if current.state() != ModelInvocationState::InFlight {
                    return Err(Error::IdempotencyConflict);
                }
                sqlx::query(
                    "INSERT INTO model_invocation_continuations
                    (model_invocation_id, run_id, provider_account_id, provider, model,
                     format, payload, payload_size, content_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(current.invocation_id().as_str())
                .bind(current.run_id().as_str())
                .bind(current.provider_account_id().as_str())
                .bind(current.settings().provider().as_str())
                .bind(current.settings().model().as_str())
                .bind(command.continuation.format())
                .bind(command.continuation.payload())
                .bind(
                    i64::try_from(command.continuation.payload().len())
                        .map_err(|_| Error::LimitExceeded)?,
                )
                .bind(command.continuation.content_hash().as_str())
                .execute(&mut *transaction)
                .await
                .map_err(|_| Error::Unavailable)?;
            }
        }
        let completion = match &command.requests {
            Some(requests) => {
                super::model_tool_request::finish_with_requests(
                    &mut transaction,
                    invocation,
                    requests,
                    &command.completion.usage,
                    ids,
                )
                .await
                .map_err(Error::Requests)?
                .completion
            }
            None => super::usage::finish_with_usage(&mut transaction, &command.completion, ids)
                .await
                .map_err(Error::Completion)?,
        };
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(completion)
    }

    async fn get_model_continuation(
        &self,
        invocation_id: &ModelInvocationId,
        limits: ModelContinuationLimits,
    ) -> Result<Option<ModelInvocationContinuation>, Error> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection.begin().await.map_err(|_| Error::Unavailable)?;
        let invocation = load_model_invocation(&mut transaction, invocation_id)
            .await
            .map_err(Error::Invocation)?
            .ok_or(Error::InvalidBinding)?;
        let continuation = load_continuation(&mut transaction, &invocation, limits).await?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(continuation)
    }
}

async fn load_continuation(
    transaction: &mut Transaction<'_, Sqlite>,
    invocation: &ModelInvocation,
    limits: ModelContinuationLimits,
) -> Result<Option<ModelInvocationContinuation>, Error> {
    if limits.max_format_bytes == 0 || limits.max_payload_bytes == 0 {
        return Err(Error::InvalidLimits);
    }
    let sizes = sqlx::query(
        "SELECT payload_size, length(payload) AS actual_size,
        length(CAST(format AS BLOB)) AS format_size FROM model_invocation_continuations
        WHERE model_invocation_id = ?",
    )
    .bind(invocation.invocation_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| Error::Unavailable)?;
    let Some(sizes) = sizes else {
        return Ok(None);
    };
    let size: i64 = sizes
        .try_get("payload_size")
        .map_err(|_| Error::IntegrityViolation)?;
    let actual: i64 = sizes
        .try_get("actual_size")
        .map_err(|_| Error::IntegrityViolation)?;
    let format_size: i64 = sizes
        .try_get("format_size")
        .map_err(|_| Error::IntegrityViolation)?;
    if size <= 0 || size != actual || format_size <= 0 {
        return Err(Error::IntegrityViolation);
    }
    if usize::try_from(size).map_err(|_| Error::LimitExceeded)? > limits.max_payload_bytes
        || usize::try_from(format_size).map_err(|_| Error::LimitExceeded)? > limits.max_format_bytes
    {
        return Err(Error::LimitExceeded);
    }
    if invocation.state() != ModelInvocationState::Completed
        || !matches!(
            invocation
                .outcome()
                .and_then(|outcome| outcome.completion_kind()),
            Some(
                ModelInvocationCompletionKind::AssistantOutput
                    | ModelInvocationCompletionKind::ToolRequests
            )
        )
        || !super::usage::has_final_usage(transaction, invocation)
            .await
            .map_err(|error| match error {
                kiln_core::UsageStoreError::Unavailable => Error::Unavailable,
                _ => Error::IntegrityViolation,
            })?
    {
        return Err(Error::IntegrityViolation);
    }
    if invocation
        .outcome()
        .and_then(|outcome| outcome.completion_kind())
        == Some(ModelInvocationCompletionKind::ToolRequests)
        && super::model_tool_request::load_requests(transaction, invocation)
            .await
            .map_err(Error::Requests)?
            .is_none()
    {
        return Err(Error::IntegrityViolation);
    }
    // Both reads share one transaction. Preflight lengths before fetching the
    // opaque BLOB or format into application memory.
    let row = sqlx::query("SELECT run_id, provider_account_id, provider, model,
        format, payload, content_hash FROM model_invocation_continuations WHERE model_invocation_id = ?")
        .bind(invocation.invocation_id().as_str()).fetch_one(&mut **transaction).await
        .map_err(|_| Error::Unavailable)?;
    for (column, expected) in [
        ("run_id", invocation.run_id().as_str()),
        (
            "provider_account_id",
            invocation.provider_account_id().as_str(),
        ),
        ("provider", invocation.settings().provider().as_str()),
        ("model", invocation.settings().model().as_str()),
    ] {
        if row
            .try_get::<String, _>(column)
            .map_err(|_| Error::IntegrityViolation)?
            != expected
        {
            return Err(Error::IntegrityViolation);
        }
    }
    let payload: Vec<u8> = row
        .try_get("payload")
        .map_err(|_| Error::IntegrityViolation)?;
    if payload.len() != size as usize {
        return Err(Error::IntegrityViolation);
    }
    let format = row
        .try_get("format")
        .map_err(|_| Error::IntegrityViolation)?;
    let continuation = ModelInvocationContinuation::new(invocation, format, payload, limits)
        .map_err(|_| Error::IntegrityViolation)?;
    if continuation.content_hash().as_str()
        != row
            .try_get::<String, _>("content_hash")
            .map_err(|_| Error::IntegrityViolation)?
    {
        return Err(Error::IntegrityViolation);
    }
    Ok(Some(continuation))
}

pub(super) async fn load_reference(
    transaction: &mut Transaction<'_, Sqlite>,
    run: &kiln_core::Run,
    invocation_id: &ModelInvocationId,
    destination_sequence: Option<i64>,
) -> Result<kiln_core::ModelContinuationReference, Error> {
    let (sequence, invocation) = super::load_model_invocation_unchecked(transaction, invocation_id)
        .await
        .map_err(Error::Invocation)?
        .ok_or(Error::InvalidBinding)?;
    super::model_tool_exchange::validate_source_metadata(
        transaction,
        run,
        sequence,
        &invocation,
        destination_sequence,
    )
    .await
    .map_err(|error| match error {
        kiln_core::ModelToolExchangeError::Unavailable => Error::Unavailable,
        _ => Error::IntegrityViolation,
    })?;
    if !super::usage::has_final_usage(transaction, &invocation)
        .await
        .map_err(|_| Error::Unavailable)?
    {
        return Err(Error::IntegrityViolation);
    }
    if invocation
        .outcome()
        .and_then(|outcome| outcome.completion_kind())
        == Some(ModelInvocationCompletionKind::ToolRequests)
        && super::model_tool_request::load_requests(transaction, &invocation)
            .await
            .map_err(Error::Requests)?
            .is_none()
    {
        return Err(Error::IntegrityViolation);
    }
    let row = sqlx::query(
        "SELECT run_id, provider_account_id, provider, model, format,
        payload_size, length(payload) AS actual_size, content_hash
        FROM model_invocation_continuations WHERE model_invocation_id = ?",
    )
    .bind(invocation_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| Error::Unavailable)?
    .ok_or(Error::InvalidBinding)?;
    for (column, expected) in [
        ("run_id", invocation.run_id().as_str()),
        (
            "provider_account_id",
            invocation.provider_account_id().as_str(),
        ),
        ("provider", invocation.settings().provider().as_str()),
        ("model", invocation.settings().model().as_str()),
    ] {
        if row
            .try_get::<String, _>(column)
            .map_err(|_| Error::IntegrityViolation)?
            != expected
        {
            return Err(Error::IntegrityViolation);
        }
    }
    let size: i64 = row
        .try_get("payload_size")
        .map_err(|_| Error::IntegrityViolation)?;
    if size <= 0
        || row
            .try_get::<i64, _>("actual_size")
            .map_err(|_| Error::IntegrityViolation)?
            != size
    {
        return Err(Error::IntegrityViolation);
    }
    let format = row
        .try_get("format")
        .map_err(|_| Error::IntegrityViolation)?;
    let hash = kiln_core::ContentHash::parse(
        row.try_get::<String, _>("content_hash")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    kiln_core::ModelContinuationReference::new(run, &invocation, format, size as u64, hash)
}
