use std::future::Future;

use crate::{
    ContextManifest, ContextManifestStore, ContextManifestStoreError, ModelInvocation,
    ModelInvocationCompletionKind, ModelInvocationId, ModelInvocationIdGenerator,
    ModelInvocationMutationDisposition, ModelInvocationOutcome, ModelInvocationState,
    ModelInvocationStore, ModelInvocationStoreError, ProviderUsageUpdate, RecordModelOutput,
    StoredSessionEvent, UsageFinality,
};

pub struct ProviderRequest {
    invocation: ModelInvocation,
    manifest: ContextManifest,
}

impl ProviderRequest {
    pub fn invocation(&self) -> &ModelInvocation {
        &self.invocation
    }

    pub fn manifest(&self) -> &ContextManifest {
        &self.manifest
    }
}

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

impl<S: ModelInvocationStore + ContextManifestStore, I: ModelInvocationIdGenerator>
    ProviderApplication<S, I>
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
                Ok(ProviderClaim::Applied {
                    request: ProviderRequest {
                        invocation: mutation.value,
                        manifest,
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

pub enum ProviderUpdate {
    Output(RecordModelOutput),
    Usage(ProviderUsageUpdate),
    Finished {
        outcome: ModelInvocationOutcome,
        usage: ProviderUsageUpdate,
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

    /// Stop external work before returning success; the caller must still consume terminal usage.
    fn cancel(&mut self) -> impl Future<Output = Result<(), ProviderError>> + Send;
}
