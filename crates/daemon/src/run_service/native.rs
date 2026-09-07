use std::time::{SystemTime, UNIX_EPOCH};

use kiln_core::{
    AssistantMessageApplication, AssistantMessageStoreError, CapabilitySupport,
    ContextInstructionProvenance, ContextManifestApplication, ContextManifestEntryInput,
    CreateContextManifest, CreateModelInvocation, GenerationSettings, MessageDeliveryMode,
    MessageDeliveryState, ModelCapabilitySnapshot, ModelId, ModelInvocation,
    ModelInvocationCompletionKind, ModelInvocationFailureReason, ModelInvocationId,
    ModelInvocationPurpose, ModelInvocationSettings, ModelProvider, ModelProviderOperation,
    NativeRunApplication, ProviderAccountId, ProviderApplication, ProviderClaim, ProviderError,
    ProviderType, ProviderUpdate, ProviderUpdateMutation, ProviderUsageMetadata,
    ProviderUsageUpdate, ReasoningSettings, RecordRunInputDelivery, UsageAccounting,
    UsageCompleteness, UsageFinality, UsageSource,
};
use kiln_providers::{
    DETERMINISTIC_MODEL_ID, DETERMINISTIC_PROVIDER_TYPE, DeterministicModelProvider,
    DeterministicModelResponse,
};

use super::*;

impl RunService {
    pub(super) async fn execute_native(
        &self,
        run_id: RunId,
        mut cancellation: oneshot::Receiver<()>,
    ) -> Result<RunSnapshot, RunError> {
        let mut entries = vec![ContextManifestEntryInput::Instruction {
            provenance: ContextInstructionProvenance::Runtime,
            content: "Execute the deterministic native model fixture.".to_owned(),
        }];
        loop {
            let claim = {
                let _sequence = self.commit_sequence.lock().await;
                let snapshot = self.runs.get_run(run_id.clone()).await?;
                if matches!(
                    snapshot.run().state(),
                    RunState::Cancelling | RunState::Cancelled
                ) {
                    None
                } else {
                    while let Some(delivery) =
                        self.runs.next_queued_run_input(run_id.clone()).await?
                    {
                        let message_id = delivery.message().id().clone();
                        let mutation = self
                            .runs
                            .record_run_input_delivery(RecordRunInputDelivery {
                                message_id: message_id.clone(),
                                state: MessageDeliveryState::Delivered,
                            })
                            .await?;
                        self.events.publish(mutation.events);
                        entries.push(ContextManifestEntryInput::Message { message_id });
                    }
                    let turn_key = format!("native:turn:{}", snapshot.model_invocations().len());
                    let manifest =
                        ContextManifestApplication::new(self.store.clone(), UlidIdGenerator)
                            .create_context_manifest(CreateContextManifest {
                                run_id: run_id.clone(),
                                entries: entries.clone(),
                                idempotency_key: turn_key.clone(),
                            })
                            .await
                            .map_err(|_| RunError::RunStoreUnavailable)?;
                    self.events.publish(manifest.events);
                    let invocation =
                        ModelInvocationApplication::new(self.store.clone(), UlidIdGenerator)
                            .create_model_invocation(CreateModelInvocation {
                                run_id: run_id.clone(),
                                context_manifest_id: manifest.value.context_manifest_id().clone(),
                                context_manifest_hash: manifest.value.content_hash().clone(),
                                provider_account_id: ProviderAccountId::parse(
                                    "pac_00000000000000000000000000",
                                )
                                .map_err(|_| RunError::InvalidTransition)?,
                                settings: deterministic_settings()?,
                                capabilities: ModelCapabilitySnapshot::new(
                                    "deterministic-v1",
                                    CapabilitySupport::Unsupported,
                                    CapabilitySupport::Unsupported,
                                    CapabilitySupport::Unsupported,
                                )
                                .map_err(|_| RunError::InvalidTransition)?,
                                purpose: ModelInvocationPurpose::Generation,
                                retry_of: None,
                                idempotency_key: turn_key,
                            })
                            .await
                            .map_err(|_| RunError::RunStoreUnavailable)?;
                    self.events.publish(invocation.events);
                    let claim = ProviderApplication::new(self.store.clone(), UlidIdGenerator)
                        .claim(invocation.value.invocation_id().clone())
                        .await
                        .map_err(|_| RunError::RunStoreUnavailable)?;
                    if let ProviderClaim::Applied { events, .. } = &claim {
                        self.events.publish(events.clone());
                    }
                    Some(claim)
                }
            };
            let Some(claim) = claim else {
                return self.finish_native_cancellation(run_id).await;
            };
            let request = match claim {
                ProviderClaim::Applied { request, .. } => request,
                ProviderClaim::Duplicate { .. } => return Err(RunError::InvalidTransition),
            };
            let invocation = request.invocation().clone();
            let provider = DeterministicModelProvider::new(DeterministicModelResponse {
                text: vec!["Kiln deterministic native assistant response.".to_owned()],
                quantities: Vec::new(),
                completeness: UsageCompleteness::Unknown,
                outcome: ModelInvocationOutcome::completed(
                    ModelInvocationCompletionKind::AssistantOutput,
                ),
                observed_at_unix_ms: observed_at()?,
            });
            let (outcome, steered) = match provider.start(request).await {
                Ok(mut operation) => {
                    self.drive_native_operation(&invocation, &mut operation, &mut cancellation)
                        .await?
                }
                Err(error) => {
                    let outcome = failure_outcome(error);
                    self.persist_native_update(
                        invocation.invocation_id(),
                        unknown_terminal(&invocation, outcome)?,
                    )
                    .await?;
                    (outcome, false)
                }
            };
            self.active.changed.notify_waiters();
            let snapshot = self.runs.get_run(run_id.clone()).await?;
            if matches!(
                snapshot.run().state(),
                RunState::Cancelling | RunState::Cancelled
            ) {
                return self.finish_native_cancellation(run_id).await;
            }
            if steered {
                continue;
            }
            if outcome.completion_kind() != Some(ModelInvocationCompletionKind::AssistantOutput) {
                return self.finish_native_failure(run_id).await;
            }
            loop {
                let changed = self.active.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let result = {
                    let _sequence = self.commit_sequence.lock().await;
                    let snapshot = self.runs.get_run(run_id.clone()).await?;
                    if matches!(
                        snapshot.run().state(),
                        RunState::Cancelling | RunState::Cancelled
                    ) {
                        None
                    } else {
                        let result =
                            AssistantMessageApplication::new(self.store.clone(), UlidIdGenerator)
                                .finalize_assistant_message(invocation.invocation_id().clone())
                                .await;
                        if let Ok(mutation) = &result {
                            self.events.publish(mutation.events.clone());
                        }
                        Some(result)
                    }
                };
                match result {
                    None => return self.finish_native_cancellation(run_id).await,
                    Some(Ok(_)) => {
                        return self.runs.get_run(run_id).await;
                    }
                    Some(Err(AssistantMessageStoreError::QueuedInputPending)) => break,
                    Some(Err(AssistantMessageStoreError::ActiveDescendants)) => changed.await,
                    Some(Err(AssistantMessageStoreError::NoAssistantText)) => {
                        return self.finish_native_failure(run_id).await;
                    }
                    Some(Err(_)) => return Err(RunError::RunStoreUnavailable),
                }
            }
        }
    }

    async fn drive_native_operation<O: ModelProviderOperation>(
        &self,
        invocation: &ModelInvocation,
        operation: &mut O,
        cancellation: &mut oneshot::Receiver<()>,
    ) -> Result<(ModelInvocationOutcome, bool), RunError> {
        let mut stopped = false;
        let mut steered = false;
        loop {
            let changed = self.active.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !stopped {
                let _sequence = self.commit_sequence.lock().await;
                let snapshot = self.runs.get_run(invocation.run_id().clone()).await?;
                let inputs = self
                    .store
                    .list_queued_run_inputs(invocation.run_id())
                    .await
                    .map_err(|_| RunError::RunStoreUnavailable)?;
                steered = inputs
                    .iter()
                    .any(|input| input.mode() == MessageDeliveryMode::Interrupt);
                let should_stop = snapshot.run().state() == RunState::Cancelling || steered;
                drop(_sequence);
                if should_stop {
                    operation
                        .cancel()
                        .await
                        .map_err(|_| RunError::CancellationFailed)?;
                    stopped = true;
                }
            }
            let next = tokio::select! {
                _ = &mut *cancellation, if !stopped => {
                    operation.cancel().await.map_err(|_| RunError::CancellationFailed)?;
                    stopped = true;
                    continue;
                }
                _ = &mut changed, if !stopped => continue,
                update = operation.next_update() => update,
            };
            match next {
                Ok(Some(update)) => {
                    let outcome = match &update {
                        ProviderUpdate::Finished { outcome, .. } => Some(*outcome),
                        _ => None,
                    };
                    if let Err(error) = update.validate_for(invocation) {
                        operation
                            .cancel()
                            .await
                            .map_err(|_| RunError::CancellationFailed)?;
                        let outcome = failure_outcome(error);
                        self.persist_native_update(
                            invocation.invocation_id(),
                            unknown_terminal(invocation, outcome)?,
                        )
                        .await?;
                        return Ok((outcome, false));
                    }
                    if let Err(error) = self
                        .persist_native_update(invocation.invocation_id(), update)
                        .await
                    {
                        operation
                            .cancel()
                            .await
                            .map_err(|_| RunError::CancellationFailed)?;
                        return Err(error);
                    }
                    if let Some(outcome) = outcome {
                        return Ok((outcome, steered));
                    }
                }
                other => {
                    operation
                        .cancel()
                        .await
                        .map_err(|_| RunError::CancellationFailed)?;
                    let outcome = failure_outcome(
                        other
                            .err()
                            .unwrap_or(ProviderError::ProviderStreamInterrupted),
                    );
                    self.persist_native_update(
                        invocation.invocation_id(),
                        unknown_terminal(invocation, outcome)?,
                    )
                    .await?;
                    return Ok((outcome, false));
                }
            }
        }
    }

    async fn persist_native_update(
        &self,
        invocation_id: &ModelInvocationId,
        update: ProviderUpdate,
    ) -> Result<(), RunError> {
        let _sequence = self.commit_sequence.lock().await;
        let mutation = ProviderApplication::new(self.store.clone(), UlidIdGenerator)
            .record_update(invocation_id.clone(), update)
            .await
            .map_err(|_| RunError::RunStoreUnavailable)?;
        let events = match mutation {
            ProviderUpdateMutation::Output(mutation) => mutation.events,
            ProviderUpdateMutation::Usage(mutation) => mutation.events,
            ProviderUpdateMutation::Finished(mutation) => mutation.events,
        };
        self.events.publish(events);
        Ok(())
    }

    async fn finish_native_failure(&self, run_id: RunId) -> Result<RunSnapshot, RunError> {
        self.wait_for_descendants_terminal(&run_id).await?;
        let _sequence = self.commit_sequence.lock().await;
        if matches!(
            self.runs.get_run(run_id.clone()).await?.run().state(),
            RunState::Cancelling | RunState::Cancelled
        ) {
            drop(_sequence);
            return self.finish_native_cancellation(run_id).await;
        }
        let mutation = NativeRunApplication::new(self.store.clone(), UlidIdGenerator)
            .fail_native_run(run_id.clone())
            .await?;
        self.events.publish(mutation.events);
        self.retain_incomplete_native_messages(&mutation.value)
            .await?;
        Ok(mutation.value)
    }

    async fn finish_native_cancellation(&self, run_id: RunId) -> Result<RunSnapshot, RunError> {
        self.wait_for_descendants_terminal(&run_id).await?;
        let _sequence = self.commit_sequence.lock().await;
        let mutation = self.runs.request_cancellation(run_id.clone()).await?;
        self.events.publish(mutation.events);
        let mutation = self.runs.request_cancellation(run_id).await?;
        self.events.publish(mutation.events);
        if mutation.value.run().state() != RunState::Cancelled {
            return Err(RunError::CancellationFailed);
        }
        self.retain_incomplete_native_messages(&mutation.value)
            .await?;
        self.active.changed.notify_waiters();
        Ok(mutation.value)
    }

    async fn retain_incomplete_native_messages(
        &self,
        snapshot: &RunSnapshot,
    ) -> Result<(), RunError> {
        for invocation in snapshot.model_invocations() {
            if invocation.purpose() != ModelInvocationPurpose::Generation {
                continue;
            }
            match AssistantMessageApplication::new(self.store.clone(), UlidIdGenerator)
                .finalize_assistant_message(invocation.invocation_id().clone())
                .await
            {
                Ok(mutation) => self.events.publish(mutation.events),
                Err(
                    AssistantMessageStoreError::NoAssistantText
                    | AssistantMessageStoreError::InvocationNotComplete,
                ) => {}
                Err(_) => return Err(RunError::RunStoreUnavailable),
            }
        }
        Ok(())
    }
}

fn deterministic_settings() -> Result<ModelInvocationSettings, RunError> {
    Ok(ModelInvocationSettings::new(
        ProviderType::parse(DETERMINISTIC_PROVIDER_TYPE)
            .map_err(|_| RunError::InvalidTransition)?,
        ModelId::parse(DETERMINISTIC_MODEL_ID).map_err(|_| RunError::InvalidTransition)?,
        GenerationSettings::new(None).map_err(|_| RunError::InvalidTransition)?,
        ReasoningSettings::new(None).map_err(|_| RunError::InvalidTransition)?,
    ))
}

fn observed_at() -> Result<u64, RunError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .ok_or(RunError::RunStoreUnavailable)
}

fn failure_outcome(error: ProviderError) -> ModelInvocationOutcome {
    if error == ProviderError::ProviderStreamInterrupted {
        ModelInvocationOutcome::interrupted()
    } else {
        ModelInvocationOutcome::failed(ModelInvocationFailureReason::ProviderError)
    }
}

fn unknown_terminal(
    invocation: &ModelInvocation,
    outcome: ModelInvocationOutcome,
) -> Result<ProviderUpdate, RunError> {
    let usage = ProviderUsageUpdate::new(
        ProviderUsageMetadata {
            update_id: format!("native:terminal:{}", invocation.invocation_id().as_str()),
            provider_account_id: invocation.provider_account_id().clone(),
            work_id: invocation.work_id().clone(),
            model_invocation_id: invocation.invocation_id().clone(),
            accounting: UsageAccounting::Delta,
            finality: UsageFinality::Final,
            completeness: UsageCompleteness::Unknown,
            observed_at_unix_ms: observed_at()?,
            request_id: None,
            resolved_model: None,
            service_tier: None,
            source: UsageSource::NativeProvider,
        },
        Vec::new(),
    )
    .map_err(|_| RunError::RunStoreUnavailable)?;
    Ok(ProviderUpdate::Finished { outcome, usage })
}
