use kiln_core::{
    ModelInvocationId, ModelToolCompletionDisposition, ModelToolCompletionError as Error,
    ModelToolCompletionIds, ModelToolCompletionMutation, ModelToolCompletionStore, RunState,
    ToolCallId, ToolCallResult, ToolCallState, model_tool_completion_events,
};
use sqlx::{Connection, Row};

use super::{
    SqliteStore, insert_events, insert_tool_call_artifacts, load_model_invocation, load_snapshot,
    model_tool_catalog::load_catalog, model_tool_request::load_requests,
};

impl ModelToolCompletionStore for SqliteStore {
    async fn finish_model_tool_call(
        &self,
        tool_call_id: &ToolCallId,
        result: &ToolCallResult,
        ids: ModelToolCompletionIds,
    ) -> Result<ModelToolCompletionMutation, Error> {
        result.validate_native_output()?;
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let source = sqlx::query(
            "SELECT model_invocation_id, provider_call_id
            FROM model_tool_adoptions WHERE tool_call_id = ?",
        )
        .bind(tool_call_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| Error::Unavailable)?
        .ok_or(Error::ToolNotAdopted)?;
        let invocation_id = ModelInvocationId::parse(
            source
                .try_get::<String, _>("model_invocation_id")
                .map_err(|_| Error::IntegrityViolation)?,
        )
        .map_err(|_| Error::IntegrityViolation)?;
        let provider_call_id: String = source
            .try_get("provider_call_id")
            .map_err(|_| Error::IntegrityViolation)?;
        let invocation = load_model_invocation(&mut transaction, &invocation_id)
            .await
            .map_err(Error::Invocation)?
            .ok_or(Error::IntegrityViolation)?;
        let requests = load_requests(&mut transaction, &invocation)
            .await
            .map_err(Error::Requests)?
            .ok_or(Error::IntegrityViolation)?;
        let request = requests
            .requests()
            .iter()
            .find(|request| request.provider_call_id() == provider_call_id)
            .ok_or(Error::IntegrityViolation)?;
        let catalog = load_catalog(&mut transaction, &invocation)
            .await
            .map_err(Error::Catalog)?
            .ok_or(Error::IntegrityViolation)?;
        let definition = catalog
            .find(request.name())
            .ok_or(Error::IntegrityViolation)?;
        let snapshot = load_snapshot(&mut transaction, invocation.run_id())
            .await
            .map_err(Error::Store)?
            .ok_or(Error::IntegrityViolation)?;
        let tool = snapshot
            .tool_call(tool_call_id)
            .ok_or(Error::IntegrityViolation)?;
        if tool.capability() != definition.capability() || tool.run_id() != invocation.run_id() {
            return Err(Error::IntegrityViolation);
        }
        if tool.state().is_terminal() {
            if !result.matches_tool_call(tool) {
                return Err(Error::IdempotencyConflict);
            }
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(ModelToolCompletionMutation {
                value: snapshot,
                events: Vec::new(),
                disposition: ModelToolCompletionDisposition::Duplicate,
            });
        }
        if !matches!(
            snapshot.run().state(),
            RunState::Running | RunState::Cancelling
        ) || snapshot.has_active_invocation()
            || snapshot
                .tool_calls()
                .iter()
                .any(|other| other.tool_call_id() != tool_call_id && !other.state().is_terminal())
        {
            return Err(Error::InvalidTransition);
        }
        if result.state() == ToolCallState::Cancelled {
            if snapshot.run().state() != RunState::Cancelling
                || !matches!(tool.state(), ToolCallState::Ready | ToolCallState::Running)
            {
                return Err(Error::InvalidTransition);
            }
        } else if tool.state() != ToolCallState::Running {
            return Err(Error::InvalidTransition);
        }
        let terminal = tool.with_result(result).map_err(Error::Run)?;
        insert_tool_call_artifacts(&mut transaction, &terminal)
            .await
            .map_err(Error::Store)?;
        let updated = sqlx::query(
            "UPDATE tool_calls SET state = ?, stdout = ?, stderr = ?,
            stdout_artifact_hash = ?, stderr_artifact_hash = ?, exit_code = ?
            WHERE tool_call_id = ? AND state = ?",
        )
        .bind(terminal.state().as_str())
        .bind(terminal.stdout())
        .bind(terminal.stderr())
        .bind(
            terminal
                .stdout_artifact()
                .map(|artifact| artifact.content_hash().as_str()),
        )
        .bind(
            terminal
                .stderr_artifact()
                .map(|artifact| artifact.content_hash().as_str()),
        )
        .bind(terminal.exit_code())
        .bind(tool_call_id.as_str())
        .bind(tool.state().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| Error::Unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(Error::InvalidTransition);
        }
        let events = model_tool_completion_events(snapshot.run().session_id(), &terminal, ids);
        let events = insert_events(&mut transaction, &events)
            .await
            .map_err(Error::Store)?;
        let value = load_snapshot(&mut transaction, invocation.run_id())
            .await
            .map_err(Error::Store)?
            .ok_or(Error::IntegrityViolation)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(ModelToolCompletionMutation {
            value,
            events,
            disposition: ModelToolCompletionDisposition::Applied,
        })
    }
}
