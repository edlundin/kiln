use kiln_core::{
    FinishModelInvocationWithUsage, ModelInvocationCompletionError, ModelInvocationCompletionIds,
    ModelInvocationCompletionMutation, ModelInvocationCompletionStore, ProviderUsageMetadata,
    ProviderUsageUpdate, QuantityRelation, UsageAccounting, UsageCompleteness, UsageFinality,
    UsageIdGenerator, UsageMutation, UsageMutationDisposition, UsageObservation,
    UsageObservationId, UsageObservationPage, UsageQuantity, UsageSource, UsageStore,
    UsageStoreError, apply_usage_update,
};
use serde_json::{Value, json};

use super::*;

impl UsageIdGenerator for UlidIdGenerator {
    fn usage_observation_id(&self) -> UsageObservationId {
        UsageObservationId::from_ulid(Ulid::generate())
    }

    fn event_id(&self) -> EventId {
        EventId::from_ulid(Ulid::generate())
    }
}

fn quantity_json(quantity: &UsageQuantity) -> Value {
    let (relation, subset_of) = match quantity.relation() {
        QuantityRelation::Additive => ("additive", None),
        QuantityRelation::Subset { of } => ("subset", Some(of.as_str())),
        QuantityRelation::Informational => ("informational", None),
    };
    json!({
        "dimension": quantity.dimension(), "unit": quantity.unit(),
        "amount": quantity.amount(), "relation": relation, "subset_of": subset_of,
    })
}

fn quantities_json(quantities: &[UsageQuantity]) -> Value {
    Value::Array(quantities.iter().map(quantity_json).collect())
}

fn update_json(update: &ProviderUsageUpdate) -> Value {
    let metadata = update.metadata();
    json!({
        "update_id": metadata.update_id,
        "provider_account_id": metadata.provider_account_id.as_str(),
        "work_id": metadata.work_id.as_str(),
        "model_invocation_id": metadata.model_invocation_id.as_str(),
        "accounting": metadata.accounting.as_str(),
        "finality": metadata.finality.as_str(),
        "completeness": metadata.completeness.as_str(),
        "observed_at_unix_ms": metadata.observed_at_unix_ms,
        "request_id": metadata.request_id,
        "resolved_model": metadata.resolved_model,
        "service_tier": metadata.service_tier,
        "source": metadata.source.as_str(),
        "quantities": quantities_json(update.quantities()),
    })
}

fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str, UsageStoreError> {
    value[key]
        .as_str()
        .ok_or(UsageStoreError::IntegrityViolation)
}

fn optional_string(value: &Value, key: &str) -> Result<Option<String>, UsageStoreError> {
    match value.get(key) {
        Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        _ => Err(UsageStoreError::IntegrityViolation),
    }
}

fn parse_quantities(value: &Value) -> Result<Vec<UsageQuantity>, UsageStoreError> {
    value
        .as_array()
        .ok_or(UsageStoreError::IntegrityViolation)?
        .iter()
        .map(|value| {
            let subset_of = optional_string(value, "subset_of")?;
            let relation = match (string(value, "relation")?, subset_of) {
                ("additive", None) => QuantityRelation::Additive,
                ("informational", None) => QuantityRelation::Informational,
                ("subset", Some(of)) => QuantityRelation::Subset { of },
                _ => return Err(UsageStoreError::IntegrityViolation),
            };
            let quantity = UsageQuantity::new(
                string(value, "dimension")?,
                string(value, "unit")?,
                value["amount"]
                    .as_u64()
                    .ok_or(UsageStoreError::IntegrityViolation)?,
                relation,
            )
            .map_err(|_| UsageStoreError::IntegrityViolation)?;
            if quantity_json(&quantity) != *value {
                return Err(UsageStoreError::IntegrityViolation);
            }
            Ok(quantity)
        })
        .collect()
}

fn parse_update(value: &Value) -> Result<ProviderUsageUpdate, UsageStoreError> {
    let metadata = ProviderUsageMetadata {
        update_id: string(value, "update_id")?.to_owned(),
        provider_account_id: ProviderAccountId::parse(string(value, "provider_account_id")?)
            .map_err(|_| UsageStoreError::IntegrityViolation)?,
        work_id: ModelWorkId::parse(string(value, "work_id")?)
            .map_err(|_| UsageStoreError::IntegrityViolation)?,
        model_invocation_id: ModelInvocationId::parse(string(value, "model_invocation_id")?)
            .map_err(|_| UsageStoreError::IntegrityViolation)?,
        accounting: UsageAccounting::parse(string(value, "accounting")?)
            .map_err(|_| UsageStoreError::IntegrityViolation)?,
        finality: UsageFinality::parse(string(value, "finality")?)
            .map_err(|_| UsageStoreError::IntegrityViolation)?,
        completeness: UsageCompleteness::parse(string(value, "completeness")?)
            .map_err(|_| UsageStoreError::IntegrityViolation)?,
        observed_at_unix_ms: value["observed_at_unix_ms"]
            .as_u64()
            .ok_or(UsageStoreError::IntegrityViolation)?,
        request_id: optional_string(value, "request_id")?,
        resolved_model: optional_string(value, "resolved_model")?,
        service_tier: optional_string(value, "service_tier")?,
        source: UsageSource::parse(string(value, "source")?)
            .map_err(|_| UsageStoreError::IntegrityViolation)?,
    };
    let update = ProviderUsageUpdate::new(metadata, parse_quantities(&value["quantities"])?)
        .map_err(|_| UsageStoreError::IntegrityViolation)?;
    if update_json(&update) != *value {
        return Err(UsageStoreError::IntegrityViolation);
    }
    Ok(update)
}

fn invocation_error(error: ModelInvocationStoreError) -> UsageStoreError {
    match error {
        ModelInvocationStoreError::Unavailable => UsageStoreError::Unavailable,
        _ => UsageStoreError::IntegrityViolation,
    }
}

async fn dispatched_invocation(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &ModelInvocationId,
) -> Result<ModelInvocation, UsageStoreError> {
    let invocation = load_model_invocation(transaction, id)
        .await
        .map_err(invocation_error)?
        .ok_or(UsageStoreError::ModelInvocationNotFound)?;
    ensure_dispatched(transaction, &invocation).await?;
    Ok(invocation)
}

async fn ensure_dispatched(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    invocation: &ModelInvocation,
) -> Result<(), UsageStoreError> {
    let dispatched: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM session_events
         WHERE model_invocation_id = ? AND event_type = 'model_invocation.state_changed'
           AND model_invocation_state = 'in_flight')",
    )
    .bind(invocation.invocation_id().as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(|_| UsageStoreError::Unavailable)?;
    if invocation.state() == ModelInvocationState::Pending || !dispatched {
        return Err(UsageStoreError::InvocationNotDispatched);
    }
    Ok(())
}

async fn attribution(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    invocation: &ModelInvocation,
) -> Result<(SessionId, WorkspaceId), UsageStoreError> {
    let row = sqlx::query(
        "SELECT r.session_id, s.workspace_id FROM runs r
         JOIN sessions s ON s.session_id = r.session_id WHERE r.run_id = ?",
    )
    .bind(invocation.run_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| UsageStoreError::Unavailable)?
    .ok_or(UsageStoreError::IntegrityViolation)?;
    Ok((
        SessionId::parse(
            row.try_get::<String, _>("session_id")
                .map_err(|_| UsageStoreError::IntegrityViolation)?,
        )
        .map_err(|_| UsageStoreError::IntegrityViolation)?,
        WorkspaceId::parse(
            row.try_get::<String, _>("workspace_id")
                .map_err(|_| UsageStoreError::IntegrityViolation)?,
        )
        .map_err(|_| UsageStoreError::IntegrityViolation)?,
    ))
}

async fn load_observations(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    invocation: &ModelInvocation,
) -> Result<Vec<UsageObservation>, UsageStoreError> {
    // ponytail: replaying every revision on each write repeats O(n²) work over
    // an attempt; use a validated head if measured histories require it.
    let (session_id, workspace_id) = attribution(transaction, invocation).await?;
    let rows = sqlx::query(
        "SELECT observation_id, model_invocation_id, work_id, provider_account_id,
                run_id, session_id, workspace_id, requested_model, update_id, update_json,
                revision, supersedes, quantities_json, completeness, is_terminal,
                observed_at_unix_ms
         FROM usage_observations WHERE model_invocation_id = ? ORDER BY revision",
    )
    .bind(invocation.invocation_id().as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(|_| UsageStoreError::Unavailable)?;
    let mut observations: Vec<UsageObservation> = Vec::with_capacity(rows.len());
    for row in rows {
        let field = |name| {
            row.try_get::<String, _>(name)
                .map_err(|_| UsageStoreError::IntegrityViolation)
        };
        let update_value: Value = serde_json::from_str(&field("update_json")?)
            .map_err(|_| UsageStoreError::IntegrityViolation)?;
        let update = parse_update(&update_value)?;
        let metadata = update.metadata();
        let revision = u64::try_from(
            row.try_get::<i64, _>("revision")
                .map_err(|_| UsageStoreError::IntegrityViolation)?,
        )
        .map_err(|_| UsageStoreError::IntegrityViolation)?;
        let supersedes = row
            .try_get::<Option<String>, _>("supersedes")
            .map_err(|_| UsageStoreError::IntegrityViolation)?
            .map(UsageObservationId::parse)
            .transpose()
            .map_err(|_| UsageStoreError::IntegrityViolation)?;
        let previous = observations.last();
        let expected_revision = previous
            .map_or(Some(1), |previous| previous.revision.checked_add(1))
            .ok_or(UsageStoreError::IntegrityViolation)?;
        let observed_at = u64::try_from(
            row.try_get::<i64, _>("observed_at_unix_ms")
                .map_err(|_| UsageStoreError::IntegrityViolation)?,
        )
        .map_err(|_| UsageStoreError::IntegrityViolation)?;
        if field("model_invocation_id")? != invocation.invocation_id().as_str()
            || field("work_id")? != invocation.work_id().as_str()
            || field("provider_account_id")? != invocation.provider_account_id().as_str()
            || field("run_id")? != invocation.run_id().as_str()
            || field("session_id")? != session_id.as_str()
            || field("workspace_id")? != workspace_id.as_str()
            || field("requested_model")? != invocation.settings().model().as_str()
            || metadata.model_invocation_id != *invocation.invocation_id()
            || metadata.work_id != *invocation.work_id()
            || metadata.provider_account_id != *invocation.provider_account_id()
            || metadata.update_id != field("update_id")?
            || metadata.observed_at_unix_ms != observed_at
            || revision != expected_revision
            || supersedes.as_ref() != previous.map(|previous| &previous.observation_id)
        {
            return Err(UsageStoreError::IntegrityViolation);
        }
        let effective = apply_usage_update(
            previous.map_or(&[], |previous| previous.quantities.as_slice()),
            previous.is_some_and(|previous| previous.is_terminal),
            &update,
        )
        .map_err(|_| UsageStoreError::IntegrityViolation)?;
        let quantity_value: Value = serde_json::from_str(&field("quantities_json")?)
            .map_err(|_| UsageStoreError::IntegrityViolation)?;
        let quantities = parse_quantities(&quantity_value)?;
        let completeness = UsageCompleteness::parse(&field("completeness")?)
            .map_err(|_| UsageStoreError::IntegrityViolation)?;
        let terminal = row
            .try_get::<i64, _>("is_terminal")
            .map_err(|_| UsageStoreError::IntegrityViolation)?;
        if quantities != effective.quantities()
            || completeness != metadata.completeness
            || terminal != i64::from(effective.is_final())
        {
            return Err(UsageStoreError::IntegrityViolation);
        }
        observations.push(UsageObservation {
            observation_id: UsageObservationId::parse(field("observation_id")?)
                .map_err(|_| UsageStoreError::IntegrityViolation)?,
            model_invocation_id: invocation.invocation_id().clone(),
            work_id: invocation.work_id().clone(),
            provider_account_id: invocation.provider_account_id().clone(),
            run_id: invocation.run_id().clone(),
            session_id: session_id.clone(),
            workspace_id: workspace_id.clone(),
            requested_model: invocation.settings().model().clone(),
            update,
            revision,
            supersedes,
            quantities,
            completeness,
            is_terminal: effective.is_final(),
        });
    }
    Ok(observations)
}

pub(super) async fn has_final_usage(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    invocation: &ModelInvocation,
) -> Result<bool, UsageStoreError> {
    Ok(load_observations(transaction, invocation)
        .await?
        .last()
        .is_some_and(|observation| observation.is_terminal))
}

pub(super) async fn load_event_observation(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    observation_id: &str,
    invocation_id: &str,
    cache: &mut std::collections::HashMap<
        ModelInvocationId,
        std::collections::HashMap<String, UsageObservation>,
    >,
) -> Result<UsageObservation, StoreError> {
    let invocation_id =
        ModelInvocationId::parse(invocation_id).map_err(|_| StoreError::Unavailable)?;
    if !cache.contains_key(&invocation_id) {
        let invocation = dispatched_invocation(transaction, &invocation_id)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let observations = load_observations(transaction, &invocation)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        cache.insert(
            invocation_id.clone(),
            observations
                .into_iter()
                .map(|observation| (observation.observation_id.as_str().to_owned(), observation))
                .collect(),
        );
    }
    cache
        .get(&invocation_id)
        .and_then(|observations| observations.get(observation_id))
        .cloned()
        .ok_or(StoreError::Unavailable)
}

impl UsageStore for SqliteStore {
    async fn record_usage(
        &self,
        update: &ProviderUsageUpdate,
        observation_id: UsageObservationId,
        event_id: EventId,
    ) -> Result<UsageMutation, UsageStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| UsageStoreError::Unavailable)?;
        let mutation =
            record_usage_in_transaction(&mut transaction, update, observation_id, event_id).await?;
        transaction
            .commit()
            .await
            .map_err(|_| UsageStoreError::Unavailable)?;
        Ok(mutation)
    }

    async fn list_usage_observations(
        &self,
        model_invocation_id: &ModelInvocationId,
    ) -> Result<Vec<UsageObservation>, UsageStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| UsageStoreError::Unavailable)?;
        let invocation = load_model_invocation(&mut transaction, model_invocation_id)
            .await
            .map_err(invocation_error)?
            .ok_or(UsageStoreError::ModelInvocationNotFound)?;
        let observations = load_observations(&mut transaction, &invocation).await?;
        if !observations.is_empty() {
            dispatched_invocation(&mut transaction, model_invocation_id).await?;
        }
        transaction
            .commit()
            .await
            .map_err(|_| UsageStoreError::Unavailable)?;
        Ok(observations)
    }

    async fn list_latest_usage_observations(
        &self,
        after: Option<&ModelInvocationId>,
        limit: u64,
    ) -> Result<UsageObservationPage, UsageStoreError> {
        if limit == 0 || limit > kiln_core::MAX_USAGE_PAGE_LIMIT {
            return Err(UsageStoreError::InvalidLimit);
        }
        let fetch_limit = i64::try_from(limit)
            .ok()
            .and_then(|limit| limit.checked_add(1))
            .ok_or(UsageStoreError::InvalidLimit)?;
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| UsageStoreError::Unavailable)?;
        let query = if let Some(after) = after {
            sqlx::query(
                "SELECT model_invocation_id FROM usage_observations WHERE model_invocation_id > ? GROUP BY model_invocation_id ORDER BY model_invocation_id LIMIT ?",
            )
            .bind(after.as_str())
            .bind(fetch_limit)
        } else {
            sqlx::query(
                "SELECT model_invocation_id FROM usage_observations GROUP BY model_invocation_id ORDER BY model_invocation_id LIMIT ?",
            )
            .bind(fetch_limit)
        };
        let rows = query
            .fetch_all(&mut *transaction)
            .await
            .map_err(|_| UsageStoreError::Unavailable)?;
        let mut observations = Vec::with_capacity(rows.len());
        for row in rows {
            let invocation_id = ModelInvocationId::parse(
                row.try_get::<String, _>("model_invocation_id")
                    .map_err(|_| UsageStoreError::IntegrityViolation)?,
            )
            .map_err(|_| UsageStoreError::IntegrityViolation)?;
            let invocation = load_model_invocation(&mut transaction, &invocation_id)
                .await
                .map_err(invocation_error)?
                .ok_or(UsageStoreError::IntegrityViolation)?;
            let history = load_observations(&mut transaction, &invocation).await?;
            let latest = history
                .last()
                .cloned()
                .ok_or(UsageStoreError::IntegrityViolation)?;
            dispatched_invocation(&mut transaction, &invocation_id).await?;
            observations.push(latest);
        }
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        let has_more = observations.len() > limit;
        observations.truncate(limit);
        transaction
            .commit()
            .await
            .map_err(|_| UsageStoreError::Unavailable)?;
        Ok(UsageObservationPage {
            observations,
            has_more,
        })
    }
}

impl ModelInvocationCompletionStore for SqliteStore {
    async fn finish_model_invocation_with_usage(
        &self,
        command: &FinishModelInvocationWithUsage,
        ids: ModelInvocationCompletionIds,
    ) -> Result<ModelInvocationCompletionMutation, ModelInvocationCompletionError> {
        if command.usage.metadata().model_invocation_id != *command.invocation.invocation_id() {
            return Err(ModelInvocationCompletionError::Usage(
                UsageStoreError::AttributionMismatch,
            ));
        }
        if command.usage.metadata().finality != UsageFinality::Final {
            return Err(ModelInvocationCompletionError::Usage(
                UsageStoreError::InvalidUpdate,
            ));
        }
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| {
                ModelInvocationCompletionError::Invocation(ModelInvocationStoreError::Unavailable)
            })?;
        let completion = finish_with_usage(&mut transaction, command, ids).await?;
        transaction.commit().await.map_err(|_| {
            ModelInvocationCompletionError::Invocation(ModelInvocationStoreError::Unavailable)
        })?;
        Ok(completion)
    }
}

pub(super) async fn record_usage_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    update: &ProviderUsageUpdate,
    observation_id: UsageObservationId,
    event_id: EventId,
) -> Result<UsageMutation, UsageStoreError> {
    let metadata = update.metadata();
    let observed_at =
        i64::try_from(metadata.observed_at_unix_ms).map_err(|_| UsageStoreError::InvalidUpdate)?;
    let invocation = dispatched_invocation(transaction, &metadata.model_invocation_id).await?;
    let previous = load_observations(transaction, &invocation).await?;
    if let Some(stored) = previous
        .iter()
        .find(|stored| stored.update.metadata().update_id == metadata.update_id)
    {
        if stored.update != *update {
            return Err(UsageStoreError::IdempotencyConflict);
        }
        let value = stored.clone();
        return Ok(UsageMutation {
            value,
            events: Vec::new(),
            disposition: UsageMutationDisposition::Duplicate,
        });
    }
    if metadata.provider_account_id != *invocation.provider_account_id()
        || metadata.work_id != *invocation.work_id()
    {
        return Err(UsageStoreError::AttributionMismatch);
    }
    let latest = previous.last();
    let effective = apply_usage_update(
        latest.map_or(&[], |latest| latest.quantities.as_slice()),
        latest.is_some_and(|latest| latest.is_terminal),
        update,
    )
    .map_err(|_| UsageStoreError::InvalidUpdate)?;
    let revision = latest
        .map_or(Some(1), |latest| latest.revision.checked_add(1))
        .ok_or(UsageStoreError::InvalidUpdate)?;
    let stored_revision = i64::try_from(revision).map_err(|_| UsageStoreError::InvalidUpdate)?;
    let (session_id, workspace_id) = attribution(transaction, &invocation).await?;
    let observation = UsageObservation {
        observation_id,
        model_invocation_id: invocation.invocation_id().clone(),
        work_id: invocation.work_id().clone(),
        provider_account_id: invocation.provider_account_id().clone(),
        run_id: invocation.run_id().clone(),
        session_id,
        workspace_id,
        requested_model: invocation.settings().model().clone(),
        update: update.clone(),
        revision,
        supersedes: latest.map(|latest| latest.observation_id.clone()),
        quantities: effective.quantities().to_vec(),
        completeness: metadata.completeness,
        is_terminal: effective.is_final(),
    };
    sqlx::query(
        "INSERT INTO usage_observations (
            observation_id, model_invocation_id, work_id, provider_account_id,
            run_id, session_id, workspace_id, requested_model, update_id, update_json,
            revision, supersedes, quantities_json, completeness, is_terminal,
            observed_at_unix_ms
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(observation.observation_id.as_str())
    .bind(observation.model_invocation_id.as_str())
    .bind(observation.work_id.as_str())
    .bind(observation.provider_account_id.as_str())
    .bind(observation.run_id.as_str())
    .bind(observation.session_id.as_str())
    .bind(observation.workspace_id.as_str())
    .bind(observation.requested_model.as_str())
    .bind(&metadata.update_id)
    .bind(update_json(update).to_string())
    .bind(stored_revision)
    .bind(
        observation
            .supersedes
            .as_ref()
            .map(UsageObservationId::as_str),
    )
    .bind(quantities_json(&observation.quantities).to_string())
    .bind(observation.completeness.as_str())
    .bind(observation.is_terminal)
    .bind(observed_at)
    .execute(&mut **transaction)
    .await
    .map_err(|_| UsageStoreError::Unavailable)?;
    let result = sqlx::query(
        "INSERT INTO session_events (
            event_id, session_id, event_type, run_id, model_invocation_id, usage_observation_id
         ) VALUES (?, ?, 'usage.observed', ?, ?, ?)",
    )
    .bind(event_id.as_str())
    .bind(observation.session_id.as_str())
    .bind(observation.run_id.as_str())
    .bind(observation.model_invocation_id.as_str())
    .bind(observation.observation_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(|_| UsageStoreError::Unavailable)?;
    let cursor = committed_cursor(result.last_insert_rowid())
        .map_err(|_| UsageStoreError::IntegrityViolation)?;
    let event = SessionEvent::usage_observed(event_id, observation.clone());
    let stored = StoredSessionEvent::from_parts(
        event.event_id().clone(),
        event.session_id().clone(),
        cursor,
        event.payload().clone(),
    )
    .map_err(|_| UsageStoreError::IntegrityViolation)?;
    Ok(UsageMutation {
        value: observation,
        events: vec![stored],
        disposition: UsageMutationDisposition::Applied,
    })
}

pub(super) async fn finish_with_usage(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    command: &FinishModelInvocationWithUsage,
    ids: ModelInvocationCompletionIds,
) -> Result<ModelInvocationCompletionMutation, ModelInvocationCompletionError> {
    if command.usage.metadata().model_invocation_id != *command.invocation.invocation_id() {
        return Err(ModelInvocationCompletionError::Usage(
            UsageStoreError::AttributionMismatch,
        ));
    }
    if command.usage.metadata().finality != UsageFinality::Final {
        return Err(ModelInvocationCompletionError::Usage(
            UsageStoreError::InvalidUpdate,
        ));
    }
    let usage = record_usage_in_transaction(
        transaction,
        &command.usage,
        ids.usage_observation_id,
        ids.usage_event_id,
    )
    .await
    .map_err(ModelInvocationCompletionError::Usage)?;
    let invocation = finish_model_invocation_in_transaction(
        transaction,
        &command.invocation,
        command.outcome,
        ids.invocation_event_id,
    )
    .await
    .map_err(ModelInvocationCompletionError::Invocation)?;
    let mut events = usage.events;
    events.extend(invocation.events);
    let disposition = if events.is_empty() {
        ModelInvocationMutationDisposition::Duplicate
    } else {
        ModelInvocationMutationDisposition::Applied
    };
    Ok(ModelInvocationCompletionMutation {
        invocation: invocation.value,
        usage: usage.value,
        events,
        disposition,
    })
}
