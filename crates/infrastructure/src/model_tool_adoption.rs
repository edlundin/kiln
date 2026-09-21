use kiln_core::{
    AdoptModelToolRequest, ApprovalState, ModelInvocationStoreError, ModelToolAdoptionDisposition,
    ModelToolAdoptionError as Error, ModelToolAdoptionIds, ModelToolAdoptionMutation,
    ModelToolAdoptionStore, RunId, ToolCallId, WorkspacePathScope,
};
use sqlx::{Connection, Sqlite, Transaction};

use super::{
    SqliteStore, insert_events, load_model_invocation, load_snapshot,
    model_tool_catalog::load_catalog, model_tool_request::load_requests,
};

impl ModelToolAdoptionStore for SqliteStore {
    async fn adopt_model_tool_request(
        &self,
        command: &AdoptModelToolRequest,
        ids: ModelToolAdoptionIds,
    ) -> Result<ModelToolAdoptionMutation, Error> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let snapshot = validate_source(&mut transaction, command).await?;
        let invocation = command.invocation();
        let definition = command
            .catalog()
            .find(command.request().name())
            .ok_or(Error::InvalidRequest)?;
        let prior: Option<String> = sqlx::query_scalar(
            "SELECT tool_call_id FROM model_tool_adoptions
             WHERE model_invocation_id = ? AND provider_call_id = ?",
        )
        .bind(invocation.invocation_id().as_str())
        .bind(command.request().provider_call_id())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| Error::Unavailable)?;
        if let Some(id) = prior {
            let id = ToolCallId::parse(id).map_err(|_| Error::IntegrityViolation)?;
            let tool = snapshot.tool_call(&id).ok_or(Error::IntegrityViolation)?;
            if tool.capability() != definition.capability() || tool.run_id() != invocation.run_id()
            {
                return Err(Error::IntegrityViolation);
            }
            if tool.requested_scope() != Some(command.requested_scope()) {
                return Err(Error::IdempotencyConflict);
            }
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(ModelToolAdoptionMutation {
                value: snapshot,
                tool_call_id: id,
                events: Vec::new(),
                disposition: ModelToolAdoptionDisposition::Duplicate,
            });
        }
        if snapshot.has_active_invocation()
            || snapshot
                .tool_calls()
                .iter()
                .any(|tool| !tool.state().is_terminal())
            || snapshot
                .approvals()
                .iter()
                .any(|approval| approval.state() == ApprovalState::Pending)
        {
            return Err(Error::ActiveWork);
        }
        // A stale batch may not create work after a newer model turn has started.
        if snapshot
            .model_invocations()
            .last()
            .map(|value| value.invocation_id())
            != Some(invocation.invocation_id())
        {
            return Err(Error::InvalidRequest);
        }
        let earlier_pending: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM model_tool_requests AS request
                LEFT JOIN model_tool_adoptions AS adoption
                  ON adoption.model_invocation_id = request.model_invocation_id
                 AND adoption.provider_call_id = request.provider_call_id
                LEFT JOIN tool_calls AS tool ON tool.tool_call_id = adoption.tool_call_id
                WHERE request.model_invocation_id = ? AND request.position < ?
                  AND (tool.tool_call_id IS NULL OR tool.state NOT IN
                    ('completed', 'failed', 'cancelled', 'denied'))
            )",
        )
        .bind(invocation.invocation_id().as_str())
        .bind(i64::try_from(command.position()).map_err(|_| Error::InvalidRequest)?)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| Error::Unavailable)?;
        if earlier_pending {
            return Err(Error::EarlierRequestPending);
        }
        let plan = command.authorize(snapshot.run(), ids)?;
        let tool = &plan.tool_call;
        let updated =
            sqlx::query("UPDATE runs SET state = ? WHERE run_id = ? AND state = 'running'")
                .bind(plan.run.state().as_str())
                .bind(plan.run.run_id().as_str())
                .execute(&mut *transaction)
                .await
                .map_err(|_| Error::Unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(Error::InvalidRun);
        }
        sqlx::query(
            "INSERT INTO tool_calls (tool_call_id, run_id, capability, state,
            requested_workspace_root_id, requested_relative_directory,
            effective_workspace_root_id, effective_relative_directory)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(tool.tool_call_id().as_str())
        .bind(tool.run_id().as_str())
        .bind(tool.capability())
        .bind(tool.state().as_str())
        .bind(command.requested_scope().workspace_root_id().as_str())
        .bind(command.requested_scope().relative_directory())
        .bind(
            tool.effective_scope()
                .map(|scope| scope.workspace_root_id().as_str()),
        )
        .bind(
            tool.effective_scope()
                .map(WorkspacePathScope::relative_directory),
        )
        .execute(&mut *transaction)
        .await
        .map_err(|_| Error::Unavailable)?;
        if let Some(approval) = &plan.approval {
            sqlx::query(
                "INSERT INTO approvals
                (approval_id, run_id, tool_call_id, workspace_root_id, relative_directory, state)
                VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(approval.approval_id().as_str())
            .bind(approval.run_id().as_str())
            .bind(approval.tool_call_id().as_str())
            .bind(approval.scope().workspace_root_id().as_str())
            .bind(approval.scope().relative_directory())
            .bind(approval.state().as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|_| Error::Unavailable)?;
        }
        sqlx::query(
            "INSERT INTO model_tool_adoptions
            (model_invocation_id, provider_call_id, tool_call_id) VALUES (?, ?, ?)",
        )
        .bind(invocation.invocation_id().as_str())
        .bind(command.request().provider_call_id())
        .bind(tool.tool_call_id().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| Error::Unavailable)?;
        let events = insert_events(&mut transaction, &plan.events)
            .await
            .map_err(Error::Store)?;
        let value = load_snapshot(&mut transaction, invocation.run_id())
            .await
            .map_err(Error::Store)?
            .ok_or(Error::IntegrityViolation)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(ModelToolAdoptionMutation {
            value,
            tool_call_id: tool.tool_call_id().clone(),
            events,
            disposition: ModelToolAdoptionDisposition::Applied,
        })
    }
}

/// Checked by both creation and claim so a pending next invocation cannot block
/// adoption of the previous invocation's still-unhandled proposals.
pub(super) async fn has_unfinished_requests(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
) -> Result<bool, ModelInvocationStoreError> {
    sqlx::query_scalar(
        "SELECT EXISTS(
        SELECT 1 FROM model_tool_requests AS request
        JOIN model_invocations AS invocation
          ON invocation.model_invocation_id = request.model_invocation_id
        LEFT JOIN model_tool_adoptions AS adoption
          ON adoption.model_invocation_id = request.model_invocation_id
         AND adoption.provider_call_id = request.provider_call_id
        LEFT JOIN tool_calls AS tool ON tool.tool_call_id = adoption.tool_call_id
        WHERE invocation.run_id = ? AND (tool.tool_call_id IS NULL OR tool.state NOT IN
            ('completed', 'failed', 'cancelled', 'denied'))
    )",
    )
    .bind(run_id.as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(|_| ModelInvocationStoreError::Unavailable)
}

pub(super) async fn validate_source(
    transaction: &mut Transaction<'_, Sqlite>,
    command: &AdoptModelToolRequest,
) -> Result<kiln_core::RunSnapshot, Error> {
    let invocation = load_model_invocation(transaction, command.invocation().invocation_id())
        .await
        .map_err(Error::Invocation)?
        .ok_or(Error::InvalidRequest)?;
    if invocation != *command.invocation() {
        return Err(Error::IntegrityViolation);
    }
    let requests = load_requests(transaction, &invocation)
        .await
        .map_err(Error::Requests)?
        .ok_or(Error::InvalidRequest)?;
    if requests.requests().get(command.position()) != Some(command.request()) {
        return Err(Error::IntegrityViolation);
    }
    let catalog = load_catalog(transaction, &invocation)
        .await
        .map_err(Error::Catalog)?
        .ok_or(Error::InvalidRequest)?;
    if catalog != *command.catalog() {
        return Err(Error::IntegrityViolation);
    }
    let snapshot = load_snapshot(transaction, invocation.run_id())
        .await
        .map_err(Error::Store)?
        .ok_or(Error::InvalidRun)?;
    Ok(snapshot)
}
