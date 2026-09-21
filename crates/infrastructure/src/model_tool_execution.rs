use kiln_core::{
    AdoptModelToolRequest, ApprovalState, EventId, ModelToolAdoptionError as Error,
    ModelToolExecutionDisposition, ModelToolExecutionMutation, ModelToolExecutionStore,
    SessionEvent, ToolCallId, ToolCallState,
};
use sqlx::Connection;

use super::{SqliteStore, insert_events, model_tool_adoption::validate_source};

impl ModelToolExecutionStore for SqliteStore {
    async fn claim_model_tool_call(
        &self,
        source: &AdoptModelToolRequest,
        tool_call_id: &ToolCallId,
        event_id: EventId,
    ) -> Result<ModelToolExecutionMutation, Error> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let snapshot = validate_source(&mut transaction, source).await?;
        let linked: bool = sqlx::query_scalar(
            "SELECT EXISTS(
            SELECT 1 FROM model_tool_adoptions
            WHERE model_invocation_id = ? AND provider_call_id = ? AND tool_call_id = ?)",
        )
        .bind(source.invocation().invocation_id().as_str())
        .bind(source.request().provider_call_id())
        .bind(tool_call_id.as_str())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| Error::Unavailable)?;
        if !linked {
            return Err(Error::InvalidRequest);
        }
        let tool = snapshot
            .tool_call(tool_call_id)
            .ok_or(Error::IntegrityViolation)?;
        let definition = source
            .catalog()
            .find(source.request().name())
            .ok_or(Error::InvalidRequest)?;
        if tool.run_id() != source.invocation().run_id()
            || tool.capability() != definition.capability()
            || tool.requested_scope() != Some(source.requested_scope())
        {
            return Err(Error::IntegrityViolation);
        }
        if tool.state() == ToolCallState::Running || tool.state().is_terminal() {
            let tool_call = tool.clone();
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(ModelToolExecutionMutation {
                tool_call,
                events: Vec::new(),
                disposition: ModelToolExecutionDisposition::Duplicate,
            });
        }
        source.validate_run_scope(snapshot.run())?;
        if tool.state() != ToolCallState::Ready
            || snapshot.has_active_invocation()
            || snapshot
                .approvals()
                .iter()
                .any(|approval| approval.state() == ApprovalState::Pending)
            || snapshot
                .tool_calls()
                .iter()
                .any(|other| other.tool_call_id() != tool_call_id && !other.state().is_terminal())
        {
            return Err(Error::ActiveWork);
        }
        if snapshot
            .model_invocations()
            .last()
            .map(|invocation| invocation.invocation_id())
            != Some(source.invocation().invocation_id())
        {
            return Err(Error::InvalidRequest);
        }
        let running = tool
            .transition(ToolCallState::Running)
            .map_err(Error::Run)?;
        let updated = sqlx::query(
            "UPDATE tool_calls SET state = 'running'
            WHERE tool_call_id = ? AND state = 'ready'",
        )
        .bind(tool_call_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| Error::Unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(Error::ActiveWork);
        }
        let event = SessionEvent::tool_call_state_changed(
            event_id,
            snapshot.run().session_id().clone(),
            running.clone(),
        );
        let events = insert_events(&mut transaction, &[event])
            .await
            .map_err(Error::Store)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(ModelToolExecutionMutation {
            tool_call: running,
            events,
            disposition: ModelToolExecutionDisposition::Applied,
        })
    }
}
