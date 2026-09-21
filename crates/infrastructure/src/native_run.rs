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
               AND NOT EXISTS (
                   SELECT 1 FROM tool_calls t WHERE t.run_id = r.run_id
                   AND NOT EXISTS (
                       SELECT 1 FROM model_tool_adoptions a
                       JOIN model_invocations source ON source.model_invocation_id = a.model_invocation_id
                       WHERE a.tool_call_id = t.tool_call_id AND source.run_id = r.run_id
                   )
               )
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
                for tool in snapshot
                    .tool_calls()
                    .iter()
                    .filter(|tool| tool.state().is_terminal())
                {
                    model_tool_exchange::load_exchange(
                        &mut transaction,
                        &run_id,
                        tool.tool_call_id(),
                        model_tool_exchange::ExchangeContext::NewManifest,
                    )
                    .await
                    .map_err(|_| RunStoreError::Unavailable)?;
                }
                snapshots.push(snapshot);
            }
        }
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(snapshots)
    }

    async fn reject_native_invocation(
        &self,
        invocation_id: &ModelInvocationId,
        invocation_event_id: EventId,
        run_event_id: EventId,
    ) -> Result<RunMutation<RunSnapshot>, RunStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let invocation = load_model_invocation(&mut transaction, invocation_id)
            .await
            .map_err(|_| RunStoreError::Unavailable)?
            .ok_or(RunStoreError::InvalidTransition)?;
        let current = load_snapshot(&mut transaction, invocation.run_id())
            .await?
            .ok_or(RunStoreError::RunNotFound)?;
        let outcome =
            ModelInvocationOutcome::failed(kiln_core::ModelInvocationFailureReason::InvalidRequest);
        if current.run().state() == RunState::Failed
            && invocation.state() == ModelInvocationState::Failed
            && invocation.outcome() == Some(outcome)
        {
            transaction
                .commit()
                .await
                .map_err(|_| RunStoreError::Unavailable)?;
            return Ok(RunMutation {
                value: current,
                events: Vec::new(),
            });
        }
        if invocation.state() != ModelInvocationState::Pending
            || invocation.purpose() != ModelInvocationPurpose::Generation
        {
            return Err(RunStoreError::InvalidTransition);
        }
        // The coordinator has already rejected this claim. Even if the account
        // is reconnected meanwhile, this attempt stays undispatched; a new Run
        // may use the replacement. Never revalidate into an implicit retry.
        let rejected = invocation
            .transition(ModelInvocationState::Failed, Some(outcome))
            .map_err(|_| RunStoreError::InvalidTransition)?;
        let updated = sqlx::query("UPDATE model_invocations SET state = 'failed', terminal_reason = 'invalid_request' WHERE model_invocation_id = ? AND state = 'pending'")
            .bind(invocation_id.as_str()).execute(&mut *transaction).await
            .map_err(|_| RunStoreError::Unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(RunStoreError::InvalidTransition);
        }
        let event = insert_model_invocation_event(
            &mut transaction,
            &SessionEvent::model_invocation_state_changed(
                invocation_event_id,
                current.run().session_id().clone(),
                rejected.clone(),
            ),
        )
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        let invocations = current
            .model_invocations()
            .iter()
            .map(|value| {
                if value.invocation_id() == invocation_id {
                    rejected.clone()
                } else {
                    value.clone()
                }
            })
            .collect();
        let current = RunSnapshot::with_model_invocations(
            current.run().clone(),
            current.tool_calls().to_vec(),
            current.approvals().to_vec(),
            invocations,
        );
        let mut mutation =
            fail_native_run_in_transaction(&mut transaction, current, run_event_id, true).await?;
        mutation.events.insert(0, event);
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(mutation)
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
        let mutation =
            fail_native_run_in_transaction(&mut transaction, current, event_id, false).await?;
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(mutation)
    }
}

// All work must be terminal before either failure path can finish the Run.
async fn fail_native_run_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    current: RunSnapshot,
    event_id: EventId,
    allow_queued: bool,
) -> Result<RunMutation<RunSnapshot>, RunStoreError> {
    let run_id = current.run().run_id();
    if current.run().state() == RunState::Failed {
        return Ok(RunMutation {
            value: current,
            events: Vec::new(),
        });
    }
    if !(current.run().state() == RunState::Running
        || (allow_queued && current.run().state() == RunState::Queued))
        || current
            .tool_calls()
            .iter()
            .any(|tool| !tool.state().is_terminal())
        || current
            .approvals()
            .iter()
            .any(|approval| approval.state() == ApprovalState::Pending)
        || current
            .model_invocations()
            .iter()
            .any(|invocation| !invocation.state().is_terminal())
        || !current.model_invocations().iter().any(|invocation| {
            invocation.purpose() == ModelInvocationPurpose::Generation
                && invocation.state().is_terminal()
        })
        || has_non_terminal_descendants(transaction, run_id).await?
    {
        return Err(RunStoreError::InvalidTransition);
    }
    // Terminal native calls are retained as evidence. A fixture or foreign
    // tool cannot acquire native failure semantics merely by being terminal.
    for tool in current.tool_calls() {
        model_tool_exchange::load_exchange(
            transaction,
            run_id,
            tool.tool_call_id(),
            model_tool_exchange::ExchangeContext::NewManifest,
        )
        .await
        .map_err(|error| match error {
            kiln_core::ModelToolExchangeError::Unavailable => RunStoreError::Unavailable,
            _ => RunStoreError::InvalidTransition,
        })?;
    }
    let failed = current
        .run()
        .transition(RunState::Failed)
        .map_err(|_| RunStoreError::InvalidTransition)?;
    let updated = sqlx::query("UPDATE runs SET state = 'failed' WHERE run_id = ? AND state = ?")
        .bind(run_id.as_str())
        .bind(current.run().state().as_str())
        .execute(&mut **transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
    if updated.rows_affected() != 1 {
        return Err(RunStoreError::InvalidTransition);
    }
    let event = insert_run_event(
        transaction,
        &SessionEvent::run_state_changed(event_id, &failed),
    )
    .await?;
    let value = RunSnapshot::with_model_invocations(
        failed,
        current.tool_calls().to_vec(),
        current.approvals().to_vec(),
        current.model_invocations().to_vec(),
    );
    Ok(RunMutation {
        value,
        events: vec![event],
    })
}
