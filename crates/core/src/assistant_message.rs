use std::future::Future;

use crate::{
    EventId, Message, MessageId, ModelInvocationId, ModelInvocationMutationDisposition, Run,
    StoredSessionEvent,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssistantMessageStoreError {
    ModelInvocationNotFound,
    RunNotFound,
    InvocationNotComplete,
    InvocationNotLatest,
    RunNotEligible,
    FinalUsageRequired,
    QueuedInputPending,
    ActiveToolCalls,
    ActiveInvocations,
    ActiveDescendants,
    NoAssistantText,
    IntegrityViolation,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssistantMessageMutation {
    pub message: Message,
    pub run: Run,
    pub events: Vec<StoredSessionEvent>,
    pub disposition: ModelInvocationMutationDisposition,
}

pub trait AssistantMessageIdGenerator: Send + Sync {
    fn message_id(&self) -> MessageId;
    fn event_id(&self) -> EventId;
}

pub trait AssistantMessageStore: Send + Sync {
    fn finalize_assistant_message(
        &self,
        invocation_id: &ModelInvocationId,
        message_id: MessageId,
        message_event_id: EventId,
        run_event_id: EventId,
    ) -> impl Future<Output = Result<AssistantMessageMutation, AssistantMessageStoreError>> + Send;
}

pub struct AssistantMessageApplication<S, I> {
    store: S,
    ids: I,
}

impl<S, I> AssistantMessageApplication<S, I> {
    pub fn new(store: S, ids: I) -> Self {
        Self { store, ids }
    }
}

impl<S: AssistantMessageStore, I: AssistantMessageIdGenerator> AssistantMessageApplication<S, I> {
    pub async fn finalize_assistant_message(
        &self,
        invocation_id: ModelInvocationId,
    ) -> Result<AssistantMessageMutation, AssistantMessageStoreError> {
        self.store
            .finalize_assistant_message(
                &invocation_id,
                self.ids.message_id(),
                self.ids.event_id(),
                self.ids.event_id(),
            )
            .await
    }
}
