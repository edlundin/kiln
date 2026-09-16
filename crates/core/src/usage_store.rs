use std::future::Future;

use ulid::Ulid;

use crate::{
    EventId, InvalidKilnId, ModelInvocation, ModelInvocationId, ModelInvocationMutationDisposition,
    ModelInvocationOutcome, ModelInvocationStoreError, ModelWorkId, ProviderAccountId,
    ProviderUsageUpdate, RunId, SessionId, StoredSessionEvent, UsageCompleteness, UsageQuantity,
    WorkspaceId,
};

pub const DEFAULT_USAGE_PAGE_LIMIT: u64 = 100;
pub const MAX_USAGE_PAGE_LIMIT: u64 = 1_000;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UsageObservationId(String);

impl UsageObservationId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("uso_{value}"))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        crate::parse_id(value.into(), "uso_").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageObservation {
    pub observation_id: UsageObservationId,
    pub model_invocation_id: ModelInvocationId,
    pub work_id: ModelWorkId,
    pub provider_account_id: ProviderAccountId,
    pub run_id: RunId,
    pub session_id: SessionId,
    pub workspace_id: WorkspaceId,
    pub requested_model: crate::ModelId,
    pub update: ProviderUsageUpdate,
    pub revision: u64,
    pub supersedes: Option<UsageObservationId>,
    pub quantities: Vec<UsageQuantity>,
    pub completeness: UsageCompleteness,
    pub is_terminal: bool,
}

/// A bounded store page containing the validated latest revision for each
/// physical model invocation selected by the query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageObservationPage {
    pub observations: Vec<UsageObservation>,
    pub has_more: bool,
}

/// The read-only Usage ledger projection. `next_cursor` is the last physical
/// invocation ID in this page and is passed back as the exclusive `after`
/// cursor for the next page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageLedgerPage {
    pub observations: Vec<UsageObservation>,
    pub next_cursor: Option<ModelInvocationId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageQueryError {
    InvalidLimit,
    IntegrityViolation,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageStoreError {
    ModelInvocationNotFound,
    InvocationNotDispatched,
    AttributionMismatch,
    IdempotencyConflict,
    InvalidUpdate,
    InvalidLimit,
    IntegrityViolation,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageMutationDisposition {
    Applied,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageMutation {
    pub value: UsageObservation,
    pub events: Vec<StoredSessionEvent>,
    pub disposition: UsageMutationDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinishModelInvocationWithUsage {
    pub invocation: ModelInvocation,
    pub outcome: ModelInvocationOutcome,
    pub usage: ProviderUsageUpdate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelInvocationCompletionIds {
    pub usage_observation_id: UsageObservationId,
    pub usage_event_id: EventId,
    pub invocation_event_id: EventId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelInvocationCompletionError {
    Invocation(ModelInvocationStoreError),
    Usage(UsageStoreError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelInvocationCompletionMutation {
    pub invocation: ModelInvocation,
    pub usage: UsageObservation,
    pub events: Vec<StoredSessionEvent>,
    pub disposition: ModelInvocationMutationDisposition,
}

pub trait ModelInvocationCompletionStore: Send + Sync {
    fn finish_model_invocation_with_usage(
        &self,
        command: &FinishModelInvocationWithUsage,
        ids: ModelInvocationCompletionIds,
    ) -> impl Future<
        Output = Result<ModelInvocationCompletionMutation, ModelInvocationCompletionError>,
    > + Send;
}

pub trait UsageIdGenerator: Send + Sync {
    fn usage_observation_id(&self) -> UsageObservationId;
    fn event_id(&self) -> EventId;
}

pub trait UsageStore: Send + Sync {
    fn record_usage(
        &self,
        update: &ProviderUsageUpdate,
        observation_id: UsageObservationId,
        event_id: EventId,
    ) -> impl Future<Output = Result<UsageMutation, UsageStoreError>> + Send;

    fn list_usage_observations(
        &self,
        model_invocation_id: &ModelInvocationId,
    ) -> impl Future<Output = Result<Vec<UsageObservation>, UsageStoreError>> + Send;

    fn list_latest_usage_observations(
        &self,
        after: Option<&ModelInvocationId>,
        limit: u64,
    ) -> impl Future<Output = Result<UsageObservationPage, UsageStoreError>> + Send {
        let _ = (after, limit);
        async { Err(UsageStoreError::Unavailable) }
    }
}

pub struct UsageApplication<S, I> {
    store: S,
    ids: I,
}

impl<S, I> UsageApplication<S, I> {
    pub fn new(store: S, ids: I) -> Self {
        Self { store, ids }
    }
}

impl<S: UsageStore, I: UsageIdGenerator> UsageApplication<S, I> {
    pub async fn record_usage(
        &self,
        update: ProviderUsageUpdate,
    ) -> Result<UsageMutation, UsageStoreError> {
        self.store
            .record_usage(
                &update,
                self.ids.usage_observation_id(),
                self.ids.event_id(),
            )
            .await
    }

    pub async fn list_usage_observations(
        &self,
        model_invocation_id: ModelInvocationId,
    ) -> Result<Vec<UsageObservation>, UsageStoreError> {
        self.store
            .list_usage_observations(&model_invocation_id)
            .await
    }

    pub async fn list_usage_ledger(
        &self,
        after: Option<ModelInvocationId>,
        limit: u64,
    ) -> Result<UsageLedgerPage, UsageQueryError> {
        if limit == 0 || limit > MAX_USAGE_PAGE_LIMIT {
            return Err(UsageQueryError::InvalidLimit);
        }
        let page = self
            .store
            .list_latest_usage_observations(after.as_ref(), limit)
            .await
            .map_err(|error| match error {
                UsageStoreError::InvalidLimit => UsageQueryError::InvalidLimit,
                UsageStoreError::IntegrityViolation => UsageQueryError::IntegrityViolation,
                _ => UsageQueryError::Unavailable,
            })?;
        let next_cursor = page
            .has_more
            .then(|| {
                page.observations
                    .last()
                    .map(|observation| observation.model_invocation_id.clone())
            })
            .flatten();
        Ok(UsageLedgerPage {
            observations: page.observations,
            next_cursor,
        })
    }
}

impl<S: ModelInvocationCompletionStore, I: UsageIdGenerator> UsageApplication<S, I> {
    pub async fn finish_model_invocation_with_usage(
        &self,
        command: FinishModelInvocationWithUsage,
    ) -> Result<ModelInvocationCompletionMutation, ModelInvocationCompletionError> {
        self.store
            .finish_model_invocation_with_usage(
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
