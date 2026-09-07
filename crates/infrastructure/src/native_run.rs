use kiln_core::NativeRunStore;

use super::*;

impl NativeRunStore for SqliteStore {
    async fn list_recoverable_native_runs(
        &self,
        provider: &ProviderType,
    ) -> Result<Vec<RunSnapshot>, RunStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let run_ids = sqlx::query_scalar::<_, String>(
            "SELECT r.run_id FROM runs r
             WHERE (r.state NOT IN ('completed', 'failed', 'cancelled')
                OR (r.state IN ('failed', 'cancelled') AND EXISTS (
                    SELECT 1 FROM model_output_chunks o
                    WHERE o.run_id = r.run_id AND o.stream = 'assistant_text'
                      AND NOT EXISTS (SELECT 1 FROM messages m WHERE m.model_invocation_id = o.model_invocation_id)
                )))
               AND EXISTS (SELECT 1 FROM model_invocations i WHERE i.run_id = r.run_id AND i.provider = ?)
               AND NOT EXISTS (SELECT 1 FROM tool_calls t WHERE t.run_id = r.run_id)
             ORDER BY r.run_id",
        )
        .bind(provider.as_str())
        .fetch_all(&mut *transaction).await.map_err(|_| RunStoreError::Unavailable)?;
        let mut snapshots = Vec::with_capacity(run_ids.len());
        for run_id in run_ids {
            let run_id = RunId::parse(run_id).map_err(|_| RunStoreError::Unavailable)?;
            let snapshot = load_snapshot(&mut transaction, &run_id)
                .await?
                .ok_or(RunStoreError::Unavailable)?;
            if snapshot
                .model_invocations()
                .iter()
                .all(|invocation| invocation.settings().provider() == provider)
            {
                snapshots.push(snapshot);
            }
        }
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(snapshots)
    }

    async fn fail_native_run(
        &self,
        run_id: &RunId,
        event_id: EventId,
    ) -> Result<RunMutation<RunSnapshot>, RunStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let current = load_snapshot(&mut transaction, run_id)
            .await?
            .ok_or(RunStoreError::RunNotFound)?;
        if current.run().state() == RunState::Failed {
            transaction
                .commit()
                .await
                .map_err(|_| RunStoreError::Unavailable)?;
            return Ok(RunMutation {
                value: current,
                events: Vec::new(),
            });
        }
        if current.run().state() != RunState::Running
            || !current.tool_calls().is_empty()
            || current
                .model_invocations()
                .iter()
                .any(|invocation| !invocation.state().is_terminal())
            || !current.model_invocations().iter().any(|invocation| {
                invocation.purpose() == ModelInvocationPurpose::Generation
                    && invocation.state().is_terminal()
            })
            || has_non_terminal_descendants(&mut transaction, run_id).await?
        {
            return Err(RunStoreError::InvalidTransition);
        }
        let failed = current
            .run()
            .transition(RunState::Failed)
            .map_err(|_| RunStoreError::InvalidTransition)?;
        let updated =
            sqlx::query("UPDATE runs SET state = 'failed' WHERE run_id = ? AND state = 'running'")
                .bind(run_id.as_str())
                .execute(&mut *transaction)
                .await
                .map_err(|_| RunStoreError::Unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(RunStoreError::InvalidTransition);
        }
        let event = insert_run_event(
            &mut transaction,
            &SessionEvent::run_state_changed(event_id, &failed),
        )
        .await?;
        let value = RunSnapshot::with_model_invocations(
            failed,
            current.tool_calls().to_vec(),
            current.approvals().to_vec(),
            current.model_invocations().to_vec(),
        );
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(RunMutation {
            value,
            events: vec![event],
        })
    }
}
