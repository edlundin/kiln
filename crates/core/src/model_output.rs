use std::{fmt, future::Future};

use ulid::Ulid;

use crate::{
    EventId, InvalidKilnId, ModelInvocationId, ModelInvocationMutationDisposition, RunId,
    SessionId, StoredSessionEvent,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelOutputChunkId(String);

impl ModelOutputChunkId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("moc_{value}"))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        crate::parse_id(value.into(), "moc_").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidModelOutput {
    UpdateId,
    Position,
    Stream,
    Content,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelOutputStream {
    AssistantText,
    ReasoningSummary,
}

impl ModelOutputStream {
    pub fn parse(value: &str) -> Result<Self, InvalidModelOutput> {
        match value {
            "assistant_text" => Ok(Self::AssistantText),
            "reasoning_summary" => Ok(Self::ReasoningSummary),
            _ => Err(InvalidModelOutput::Stream),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::AssistantText => "assistant_text",
            Self::ReasoningSummary => "reasoning_summary",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RecordModelOutput {
    model_invocation_id: ModelInvocationId,
    update_id: String,
    position: u64,
    stream: ModelOutputStream,
    content: String,
}

impl RecordModelOutput {
    pub fn new(
        model_invocation_id: ModelInvocationId,
        update_id: String,
        position: u64,
        stream: ModelOutputStream,
        content: String,
    ) -> Result<Self, InvalidModelOutput> {
        if !crate::usage::valid_identifier(&update_id) {
            return Err(InvalidModelOutput::UpdateId);
        }
        if position == 0 {
            return Err(InvalidModelOutput::Position);
        }
        if content.is_empty() {
            return Err(InvalidModelOutput::Content);
        }
        Ok(Self {
            model_invocation_id,
            update_id,
            position,
            stream,
            content,
        })
    }

    pub fn model_invocation_id(&self) -> &ModelInvocationId {
        &self.model_invocation_id
    }

    pub fn update_id(&self) -> &str {
        &self.update_id
    }

    pub fn position(&self) -> u64 {
        self.position
    }

    pub fn stream(&self) -> ModelOutputStream {
        self.stream
    }

    pub fn content(&self) -> &str {
        &self.content
    }
}

impl fmt::Debug for RecordModelOutput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordModelOutput")
            .field("model_invocation_id", &self.model_invocation_id)
            .field("update_id", &self.update_id)
            .field("position", &self.position)
            .field("stream", &self.stream)
            .field("content_bytes", &self.content.len())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ModelOutputChunk {
    pub output_chunk_id: ModelOutputChunkId,
    pub model_invocation_id: ModelInvocationId,
    pub run_id: RunId,
    pub session_id: SessionId,
    pub update_id: String,
    pub position: u64,
    pub stream: ModelOutputStream,
    pub content: String,
}

impl fmt::Debug for ModelOutputChunk {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelOutputChunk")
            .field("output_chunk_id", &self.output_chunk_id)
            .field("model_invocation_id", &self.model_invocation_id)
            .field("run_id", &self.run_id)
            .field("session_id", &self.session_id)
            .field("update_id", &self.update_id)
            .field("position", &self.position)
            .field("stream", &self.stream)
            .field("content_bytes", &self.content.len())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelOutputStoreError {
    ModelInvocationNotFound,
    InvocationNotInFlight,
    IdempotencyConflict,
    OutOfOrder,
    InvalidChunk,
    IntegrityViolation,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelOutputMutation {
    pub value: ModelOutputChunk,
    pub events: Vec<StoredSessionEvent>,
    pub disposition: ModelInvocationMutationDisposition,
}

pub trait ModelOutputIdGenerator: Send + Sync {
    fn output_chunk_id(&self) -> ModelOutputChunkId;
    fn event_id(&self) -> EventId;
}

pub trait ModelOutputStore: Send + Sync {
    fn record_model_output(
        &self,
        command: &RecordModelOutput,
        output_chunk_id: ModelOutputChunkId,
        event_id: EventId,
    ) -> impl Future<Output = Result<ModelOutputMutation, ModelOutputStoreError>> + Send;

    fn list_model_output(
        &self,
        model_invocation_id: &ModelInvocationId,
    ) -> impl Future<Output = Result<Vec<ModelOutputChunk>, ModelOutputStoreError>> + Send;
}

pub struct ModelOutputApplication<S, I> {
    store: S,
    ids: I,
}

impl<S, I> ModelOutputApplication<S, I> {
    pub fn new(store: S, ids: I) -> Self {
        Self { store, ids }
    }
}

impl<S: ModelOutputStore, I: ModelOutputIdGenerator> ModelOutputApplication<S, I> {
    pub async fn record_model_output(
        &self,
        command: RecordModelOutput,
    ) -> Result<ModelOutputMutation, ModelOutputStoreError> {
        self.store
            .record_model_output(&command, self.ids.output_chunk_id(), self.ids.event_id())
            .await
    }

    pub async fn list_model_output(
        &self,
        model_invocation_id: ModelInvocationId,
    ) -> Result<Vec<ModelOutputChunk>, ModelOutputStoreError> {
        self.store.list_model_output(&model_invocation_id).await
    }
}
