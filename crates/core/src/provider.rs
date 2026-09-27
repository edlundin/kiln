use std::future::Future;

use crate::{
    ContextManifest, ContextManifestStore, ContextManifestStoreError,
    FinishModelInvocationWithUsage, ModelInvocation, ModelInvocationCompletionError,
    ModelInvocationCompletionIds, ModelInvocationCompletionKind, ModelInvocationCompletionMutation,
    ModelInvocationCompletionStore, ModelInvocationId, ModelInvocationIdGenerator,
    ModelInvocationMutationDisposition, ModelInvocationOutcome, ModelInvocationState,
    ModelInvocationStore, ModelInvocationStoreError, ModelOutputIdGenerator, ModelOutputMutation,
    ModelOutputStore, ModelOutputStoreError, ModelToolCatalog, ModelToolCatalogError,
    ModelToolCatalogStore, ModelToolRequestBatch, ModelToolRequestCompletion,
    ModelToolRequestError, ModelToolRequestStore, ProviderUsageUpdate, RecordModelOutput,
    StoredSessionEvent, UsageFinality, UsageIdGenerator, UsageMutation, UsageStore,
    UsageStoreError,
};

pub struct ProviderRequest {
    invocation: ModelInvocation,
    manifest: ContextManifest,
    tool_catalog: ModelToolCatalog,
    credential: Option<crate::ModelInvocationCredential>,
}

impl ProviderRequest {
    pub fn invocation(&self) -> &ModelInvocation {
        &self.invocation
    }

    pub fn manifest(&self) -> &ContextManifest {
        &self.manifest
    }

    pub fn tool_catalog(&self) -> &ModelToolCatalog {
        &self.tool_catalog
    }
    pub fn credential(&self) -> Option<&crate::ModelInvocationCredential> {
        self.credential.as_ref()
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "Claims carry the owned request or durable duplicate receipt as an existing public by-value contract."
)]
pub enum ProviderClaim {
    Applied {
        request: ProviderRequest,
        events: Vec<StoredSessionEvent>,
    },
    /// No dispatch request is issued. Includes pending attempts cancelled before dispatch.
    Duplicate { invocation: ModelInvocation },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderClaimError {
    Invocation(ModelInvocationStoreError),
    Context(ContextManifestStoreError),
    ToolCatalog(ModelToolCatalogError),
    ContextNotFound,
    ContextMismatch,
    IntegrityViolation,
}

pub struct ProviderApplication<S, I> {
    store: S,
    ids: I,
}

impl<S, I> ProviderApplication<S, I> {
    pub fn new(store: S, ids: I) -> Self {
        Self { store, ids }
    }
}

impl<
    S: ModelInvocationStore + ContextManifestStore + ModelToolCatalogStore,
    I: ModelInvocationIdGenerator,
> ProviderApplication<S, I>
{
    pub async fn claim(
        &self,
        invocation_id: ModelInvocationId,
    ) -> Result<ProviderClaim, ProviderClaimError> {
        let invocation = self
            .store
            .get_model_invocation(&invocation_id)
            .await
            .map_err(ProviderClaimError::Invocation)?
            .ok_or(ProviderClaimError::Invocation(
                ModelInvocationStoreError::ModelInvocationNotFound,
            ))?;
        if invocation.invocation_id() != &invocation_id {
            return Err(ProviderClaimError::IntegrityViolation);
        }
        if invocation.state() != ModelInvocationState::Pending {
            return Ok(ProviderClaim::Duplicate { invocation });
        }
        let manifest = self
            .store
            .get_context_manifest(invocation.context_manifest_id())
            .await
            .map_err(ProviderClaimError::Context)?
            .ok_or(ProviderClaimError::ContextNotFound)?;
        if manifest.context_manifest_id() != invocation.context_manifest_id()
            || manifest.run_id() != invocation.run_id()
            || manifest.content_hash() != invocation.context_manifest_hash()
        {
            return Err(ProviderClaimError::ContextMismatch);
        }
        for entry in manifest.entries() {
            if let crate::ContextManifestEntry::ContinuationReference { reference } = entry {
                reference
                    .validate_destination(&invocation)
                    .map_err(|_| ProviderClaimError::ContextMismatch)?;
            }
        }
        let expected = invocation
            .transition(ModelInvocationState::InFlight, None)
            .map_err(|_| ProviderClaimError::IntegrityViolation)?;
        let mutation = self
            .store
            .begin_model_invocation(&invocation, self.ids.event_id(), self.ids.event_id())
            .await
            .map_err(ProviderClaimError::Invocation)?;
        match mutation.disposition {
            ModelInvocationMutationDisposition::Applied => {
                if mutation.value != expected || mutation.events.is_empty() {
                    return Err(ProviderClaimError::IntegrityViolation);
                }
                // Read after the claim transaction has frozen the catalog. This
                // avoids racing a pending invocation's first catalog attachment.
                let tool_catalog = self
                    .store
                    .get_model_tool_catalog(&invocation_id)
                    .await
                    .map_err(ProviderClaimError::ToolCatalog)?
                    .ok_or(ProviderClaimError::IntegrityViolation)?;
                tool_catalog
                    .validate_for(&mutation.value)
                    .map_err(ProviderClaimError::ToolCatalog)?;
                let credential = self
                    .store
                    .get_model_invocation_credential(&invocation_id)
                    .await
                    .map_err(ProviderClaimError::Invocation)?;
                if credential
                    .as_ref()
                    .is_some_and(|credential| credential.invocation_id() != &invocation_id)
                {
                    return Err(ProviderClaimError::IntegrityViolation);
                }
                Ok(ProviderClaim::Applied {
                    request: ProviderRequest {
                        invocation: mutation.value,
                        manifest,
                        tool_catalog,
                        credential,
                    },
                    events: mutation.events,
                })
            }
            ModelInvocationMutationDisposition::Duplicate => {
                if !mutation.events.is_empty() {
                    return Err(ProviderClaimError::IntegrityViolation);
                }
                Ok(ProviderClaim::Duplicate {
                    invocation: mutation.value,
                })
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderError {
    AuthenticationRequired,
    AuthenticationRefreshFailed,
    SubscriptionEntitlementMissing,
    ProviderAccountMismatch,
    ModelUnavailable,
    QuotaExhausted,
    RateLimited,
    ProviderUnavailable,
    ProviderProtocolChanged,
    ProviderStreamInterrupted,
    ProviderResponseInvalid,
}

impl<S: crate::ModelContinuationStore, I: UsageIdGenerator> ProviderApplication<S, I> {
    pub async fn finish_with_continuation(
        &self,
        command: crate::FinishModelInvocationWithContinuation,
    ) -> Result<ModelInvocationCompletionMutation, crate::ModelContinuationError> {
        command.validate()?;
        self.store
            .finish_model_invocation_with_continuation(
                &command,
                ModelInvocationCompletionIds {
                    usage_observation_id: self.ids.usage_observation_id(),
                    usage_event_id: self.ids.event_id(),
                    invocation_event_id: self.ids.event_id(),
                },
            )
            .await
    }
}

pub enum ProviderUpdate {
    Output(RecordModelOutput),
    Usage(ProviderUsageUpdate),
    Finished {
        outcome: ModelInvocationOutcome,
        usage: ProviderUsageUpdate,
    },
    /// Terminal generation output. Proposals remain inert until Kiln adopts them.
    ToolRequests {
        requests: ModelToolRequestBatch,
        usage: ProviderUsageUpdate,
    },
    /// Successful terminal output with provider-private replay data. The
    /// continuation, proposals, usage and completion share one transaction.
    CompletedWithContinuation {
        outcome: ModelInvocationOutcome,
        usage: ProviderUsageUpdate,
        requests: Option<ModelToolRequestBatch>,
        continuation: crate::ModelInvocationContinuation,
    },
}

impl ProviderUpdate {
    pub fn validate_for(&self, invocation: &ModelInvocation) -> Result<(), ProviderError> {
        let (usage, finality) = match self {
            Self::Output(output) => {
                return if output.model_invocation_id() == invocation.invocation_id() {
                    Ok(())
                } else {
                    Err(ProviderError::ProviderResponseInvalid)
                };
            }
            Self::Usage(usage) => (usage, UsageFinality::Partial),
            Self::CompletedWithContinuation {
                outcome,
                usage,
                requests,
                continuation,
            } => {
                continuation
                    .validate_completion(invocation, *outcome, usage, requests.as_ref())
                    .map_err(|_| ProviderError::ProviderResponseInvalid)?;
                (usage, UsageFinality::Final)
            }
            Self::ToolRequests { requests, usage } => {
                requests
                    .validate_completion(invocation, usage)
                    .map_err(|_| ProviderError::ProviderResponseInvalid)?;
                (usage, UsageFinality::Final)
            }
            Self::Finished { outcome, usage } => {
                if outcome.completion_kind() == Some(ModelInvocationCompletionKind::ToolRequests) {
                    return Err(ProviderError::ProviderResponseInvalid);
                }
                (usage, UsageFinality::Final)
            }
        };
        let metadata = usage.metadata();
        if metadata.model_invocation_id != *invocation.invocation_id()
            || metadata.work_id != *invocation.work_id()
            || metadata.provider_account_id != *invocation.provider_account_id()
            || metadata.finality != finality
        {
            return Err(ProviderError::ProviderResponseInvalid);
        }
        Ok(())
    }
}

pub enum ProviderUpdateMutation {
    Output(ModelOutputMutation),
    Usage(UsageMutation),
    Finished(ModelInvocationCompletionMutation),
    ToolRequests(ModelToolRequestCompletion),
    Continuation(ModelInvocationCompletionMutation),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderUpdateError {
    Invocation(ModelInvocationStoreError),
    Provider(ProviderError),
    Output(ModelOutputStoreError),
    Usage(UsageStoreError),
    Completion(ModelInvocationCompletionError),
    ToolRequests(ModelToolRequestError),
    Continuation(crate::ModelContinuationError),
    IntegrityViolation,
}

impl<S, I> ProviderApplication<S, I>
where
    S: ModelInvocationStore
        + ModelOutputStore
        + UsageStore
        + ModelInvocationCompletionStore
        + ModelToolRequestStore
        + crate::ModelContinuationStore,
    I: ModelOutputIdGenerator + UsageIdGenerator,
{
    pub async fn record_update(
        &self,
        invocation_id: ModelInvocationId,
        update: ProviderUpdate,
    ) -> Result<ProviderUpdateMutation, ProviderUpdateError> {
        let invocation = self
            .store
            .get_model_invocation(&invocation_id)
            .await
            .map_err(ProviderUpdateError::Invocation)?
            .ok_or(ProviderUpdateError::Invocation(
                ModelInvocationStoreError::ModelInvocationNotFound,
            ))?;
        if invocation.invocation_id() != &invocation_id {
            return Err(ProviderUpdateError::IntegrityViolation);
        }
        update
            .validate_for(&invocation)
            .map_err(ProviderUpdateError::Provider)?;
        match update {
            ProviderUpdate::CompletedWithContinuation {
                outcome,
                usage,
                requests,
                continuation,
            } => self
                .finish_with_continuation(crate::FinishModelInvocationWithContinuation {
                    completion: FinishModelInvocationWithUsage {
                        invocation,
                        outcome,
                        usage,
                    },
                    requests,
                    continuation,
                })
                .await
                .map(ProviderUpdateMutation::Continuation)
                .map_err(ProviderUpdateError::Continuation),
            ProviderUpdate::ToolRequests { requests, usage } => self
                .record_tool_requests(&invocation, &requests, &usage)
                .await
                .map(ProviderUpdateMutation::ToolRequests)
                .map_err(ProviderUpdateError::ToolRequests),
            ProviderUpdate::Output(command) => self
                .store
                .record_model_output(
                    &command,
                    self.ids.output_chunk_id(),
                    ModelOutputIdGenerator::event_id(&self.ids),
                )
                .await
                .map(ProviderUpdateMutation::Output)
                .map_err(ProviderUpdateError::Output),
            ProviderUpdate::Usage(usage) => self
                .store
                .record_usage(
                    &usage,
                    self.ids.usage_observation_id(),
                    UsageIdGenerator::event_id(&self.ids),
                )
                .await
                .map(ProviderUpdateMutation::Usage)
                .map_err(ProviderUpdateError::Usage),
            ProviderUpdate::Finished { outcome, usage } => self
                .store
                .finish_model_invocation_with_usage(
                    &FinishModelInvocationWithUsage {
                        invocation,
                        outcome,
                        usage,
                    },
                    ModelInvocationCompletionIds {
                        usage_observation_id: self.ids.usage_observation_id(),
                        usage_event_id: UsageIdGenerator::event_id(&self.ids),
                        invocation_event_id: UsageIdGenerator::event_id(&self.ids),
                    },
                )
                .await
                .map(ProviderUpdateMutation::Finished)
                .map_err(ProviderUpdateError::Completion),
        }
    }
}

impl<S: ModelToolRequestStore, I: UsageIdGenerator> ProviderApplication<S, I> {
    /// Record finalized proposals and terminal usage atomically. This operation
    /// does not create executable ToolCalls or grant any tool authority.
    pub async fn record_tool_requests(
        &self,
        invocation: &ModelInvocation,
        requests: &ModelToolRequestBatch,
        usage: &ProviderUsageUpdate,
    ) -> Result<ModelToolRequestCompletion, ModelToolRequestError> {
        requests.validate_completion(invocation, usage)?;
        self.store
            .finish_model_invocation_with_tool_requests(
                invocation,
                requests,
                usage,
                ModelInvocationCompletionIds {
                    usage_observation_id: self.ids.usage_observation_id(),
                    usage_event_id: self.ids.event_id(),
                    invocation_event_id: self.ids.event_id(),
                },
            )
            .await
    }
}

impl<S: ModelInvocationStore + ModelToolRequestStore + ModelToolCatalogStore, I>
    ProviderApplication<S, I>
{
    /// Load immutable durable proposals and definitions, then normalize through
    /// Kiln's local registry. No policy decision or executable ToolCall is made.
    pub async fn resolve_tool_requests<R: crate::ModelToolArgumentResolver>(
        &self,
        invocation_id: ModelInvocationId,
        resolver: &R,
    ) -> Result<crate::ResolvedModelToolBatch<R::Command>, crate::ModelToolResolutionError> {
        use crate::ModelToolResolutionError as Error;

        let invocation = self
            .store
            .get_model_invocation(&invocation_id)
            .await
            .map_err(Error::Invocation)?
            .ok_or(Error::InvocationNotComplete)?;
        if invocation.invocation_id() != &invocation_id {
            return Err(Error::IntegrityViolation);
        }
        let requests = self
            .store
            .get_model_tool_requests(&invocation_id)
            .await
            .map_err(Error::Requests)?
            .ok_or(Error::RequestsMissing)?;
        let catalog = self
            .store
            .get_model_tool_catalog(&invocation_id)
            .await
            .map_err(Error::Catalog)?
            .ok_or(Error::CatalogMissing)?;
        crate::model_tool_resolution::resolve_model_tool_requests(
            invocation, requests, catalog, resolver,
        )
    }
}

impl<S: crate::ModelToolAdoptionStore, I: crate::RunIdGenerator> ProviderApplication<S, I> {
    pub async fn adopt_tool_request(
        &self,
        command: &crate::AdoptModelToolRequest,
    ) -> Result<crate::ModelToolAdoptionMutation, crate::ModelToolAdoptionError> {
        self.store
            .adopt_model_tool_request(
                command,
                crate::ModelToolAdoptionIds {
                    tool_call_id: self.ids.tool_call_id(),
                    approval_id: self.ids.approval_id(),
                    request_event_id: self.ids.event_id(),
                    tool_event_id: self.ids.event_id(),
                    approval_event_id: self.ids.event_id(),
                    run_event_id: self.ids.event_id(),
                },
            )
            .await
    }
}

impl<S: crate::ModelToolCompletionStore, I: crate::RunIdGenerator> ProviderApplication<S, I> {
    pub async fn finish_tool_call(
        &self,
        tool_call_id: &crate::ToolCallId,
        result: &crate::ToolCallResult,
    ) -> Result<crate::ModelToolCompletionMutation, crate::ModelToolCompletionError> {
        result.validate_native_output()?;
        self.store
            .finish_model_tool_call(
                tool_call_id,
                result,
                crate::ModelToolCompletionIds {
                    stdout_event_id: self.ids.event_id(),
                    stderr_event_id: self.ids.event_id(),
                    state_event_id: self.ids.event_id(),
                },
            )
            .await
    }
}

impl<S: crate::ModelToolExecutionStore + crate::RunStore, I: crate::RunIdGenerator>
    ProviderApplication<S, I>
{
    /// Consume locally resolved data and issue execution input only after a
    /// fresh durable ready-to-running claim. Duplicate claims cannot dispatch.
    pub async fn claim_tool_call<C>(
        &self,
        batch: crate::ResolvedModelToolBatch<C>,
        position: usize,
        tool_call_id: crate::ToolCallId,
    ) -> Result<crate::ModelToolExecutionClaim<C>, crate::ModelToolAdoptionError> {
        use crate::{
            ModelToolAdoptionError as Error, ModelToolExecutionDisposition as Disposition,
        };
        let (_, tool) = self
            .store
            .get_tool_call(&tool_call_id)
            .await
            .map_err(Error::Store)?
            .ok_or(Error::InvalidRequest)?;
        let scope = tool
            .requested_scope()
            .cloned()
            .ok_or(Error::IntegrityViolation)?;
        let source = batch.prepare_adoption(position, scope)?;
        let resolved = batch
            .into_request_at(position)
            .ok_or(Error::InvalidRequest)?;
        let mutation = self
            .store
            .claim_model_tool_call(&source, &tool_call_id, self.ids.event_id())
            .await?;
        if mutation.tool_call.tool_call_id() != &tool_call_id {
            return Err(Error::IntegrityViolation);
        }
        match mutation.disposition {
            Disposition::Applied => {
                if mutation.events.len() != 1 {
                    return Err(Error::IntegrityViolation);
                }
                let request = crate::ModelToolExecutionRequest::new(
                    &source,
                    mutation.tool_call,
                    resolved.into_command(),
                )?;
                Ok(crate::ModelToolExecutionClaim::Applied {
                    request,
                    events: mutation.events,
                })
            }
            Disposition::Duplicate => {
                if !mutation.events.is_empty()
                    || !(mutation.tool_call.state().is_terminal()
                        || mutation.tool_call.state() == crate::ToolCallState::Running)
                {
                    return Err(Error::IntegrityViolation);
                }
                Ok(crate::ModelToolExecutionClaim::Duplicate {
                    tool_call: mutation.tool_call,
                })
            }
        }
    }
}

pub trait ModelProvider: Send + Sync {
    type Operation: ModelProviderOperation;

    fn start(
        &self,
        request: ProviderRequest,
    ) -> impl Future<Output = Result<Self::Operation, ProviderError>> + Send;
}

pub trait ModelProviderOperation: Send {
    /// Dropping this future must not discard an update. Return `None` only after a terminal update.
    fn next_update(
        &mut self,
    ) -> impl Future<Output = Result<Option<ProviderUpdate>, ProviderError>> + Send;

    /// Stop locally owned provider I/O before returning success; the caller must
    /// still consume terminal usage. This does not establish that remote inference
    /// or billing stopped unless the provider explicitly confirms it. No further
    /// output or tool proposals may escape the operation after this local stop.
    fn cancel(&mut self) -> impl Future<Output = Result<(), ProviderError>> + Send;
}
