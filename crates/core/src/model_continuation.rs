use sha2::{Digest, Sha256};
use std::{fmt, future::Future};

use crate::{
    FinishModelInvocationWithUsage, ModelId, ModelInvocation, ModelInvocationCompletionError,
    ModelInvocationCompletionIds, ModelInvocationCompletionKind, ModelInvocationCompletionMutation,
    ModelInvocationId, ModelInvocationPurpose, ModelInvocationStoreError, ModelToolRequestBatch,
    ModelToolRequestError, ProviderAccountId, ProviderType, RunId, UsageFinality,
};

#[derive(Debug, Clone, Copy)]
pub struct ModelContinuationLimits {
    pub max_format_bytes: usize,
    pub max_payload_bytes: usize,
}

/// Opaque provider-private continuation data. Never publish the payload in
/// Events, assistant output, diagnostics, or a general downloadable Artifact.
#[derive(Clone, PartialEq, Eq)]
pub struct ModelInvocationContinuation {
    invocation_id: ModelInvocationId,
    run_id: RunId,
    provider_account_id: ProviderAccountId,
    provider: ProviderType,
    model: ModelId,
    format: String,
    payload: Vec<u8>,
}

impl fmt::Debug for ModelInvocationContinuation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelInvocationContinuation")
            .field("invocation_id", &self.invocation_id)
            .field("bytes", &self.payload.len())
            .finish_non_exhaustive()
    }
}

impl ModelInvocationContinuation {
    pub fn new(
        invocation: &ModelInvocation,
        format: String,
        payload: Vec<u8>,
        limits: ModelContinuationLimits,
    ) -> Result<Self, ModelContinuationError> {
        if limits.max_format_bytes == 0 || limits.max_payload_bytes == 0 {
            return Err(ModelContinuationError::InvalidLimits);
        }
        if !crate::model_tool_request::valid_identifier(&format, limits.max_format_bytes)
            || payload.is_empty()
            || payload.len() > limits.max_payload_bytes
        {
            return Err(ModelContinuationError::InvalidPayload);
        }
        if invocation.purpose() != ModelInvocationPurpose::Generation {
            return Err(ModelContinuationError::InvalidBinding);
        }
        Ok(Self {
            invocation_id: invocation.invocation_id().clone(),
            run_id: invocation.run_id().clone(),
            provider_account_id: invocation.provider_account_id().clone(),
            provider: invocation.settings().provider().clone(),
            model: invocation.settings().model().clone(),
            format,
            payload,
        })
    }

    pub fn validate_for(&self, invocation: &ModelInvocation) -> Result<(), ModelContinuationError> {
        if self.invocation_id != *invocation.invocation_id()
            || self.run_id != *invocation.run_id()
            || self.provider_account_id != *invocation.provider_account_id()
            || self.provider != *invocation.settings().provider()
            || self.model != *invocation.settings().model()
            || invocation.purpose() != ModelInvocationPurpose::Generation
        {
            return Err(ModelContinuationError::InvalidBinding);
        }
        Ok(())
    }

    pub fn invocation_id(&self) -> &ModelInvocationId {
        &self.invocation_id
    }
    pub fn format(&self) -> &str {
        &self.format
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn content_hash(&self) -> crate::ContentHash {
        let mut hash = Sha256::new();
        for field in [
            b"kiln.model-continuation.v1".as_slice(),
            self.invocation_id.as_str().as_bytes(),
            self.run_id.as_str().as_bytes(),
            self.provider_account_id.as_str().as_bytes(),
            self.provider.as_str().as_bytes(),
            self.model.as_str().as_bytes(),
            self.format.as_bytes(),
            self.payload.as_slice(),
        ] {
            hash.update((field.len() as u64).to_be_bytes());
            hash.update(field);
        }
        let mut encoded = String::with_capacity(64);
        for byte in hash.finalize() {
            encoded.push(b"0123456789abcdef"[(byte >> 4) as usize] as char);
            encoded.push(b"0123456789abcdef"[(byte & 0x0f) as usize] as char);
        }
        crate::ContentHash::parse(encoded)
            .expect("SHA-256 is a lowercase 64-character hexadecimal value")
    }
}

pub struct FinishModelInvocationWithContinuation {
    pub completion: FinishModelInvocationWithUsage,
    pub requests: Option<ModelToolRequestBatch>,
    pub continuation: ModelInvocationContinuation,
}

impl FinishModelInvocationWithContinuation {
    pub fn validate(&self) -> Result<(), ModelContinuationError> {
        self.continuation.validate_completion(
            &self.completion.invocation,
            self.completion.outcome,
            &self.completion.usage,
            self.requests.as_ref(),
        )
    }
}

impl ModelInvocationContinuation {
    pub fn validate_completion(
        &self,
        invocation: &ModelInvocation,
        outcome: crate::ModelInvocationOutcome,
        usage: &crate::ProviderUsageUpdate,
        requests: Option<&ModelToolRequestBatch>,
    ) -> Result<(), ModelContinuationError> {
        self.validate_for(invocation)?;
        let expected = match requests {
            Some(requests) => {
                requests
                    .validate_completion(invocation, usage)
                    .map_err(ModelContinuationError::Requests)?;
                ModelInvocationCompletionKind::ToolRequests
            }
            None => ModelInvocationCompletionKind::AssistantOutput,
        };
        let metadata = usage.metadata();
        if outcome.completion_kind() != Some(expected)
            || metadata.finality != UsageFinality::Final
            || metadata.model_invocation_id != *invocation.invocation_id()
            || metadata.work_id != *invocation.work_id()
            || metadata.provider_account_id != *invocation.provider_account_id()
        {
            return Err(ModelContinuationError::InvalidBinding);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelContinuationError {
    InvalidLimits,
    InvalidPayload,
    InvalidBinding,
    LimitExceeded,
    IntegrityViolation,
    IdempotencyConflict,
    Unavailable,
    Invocation(ModelInvocationStoreError),
    Completion(ModelInvocationCompletionError),
    Requests(ModelToolRequestError),
}

pub trait ModelContinuationStore: Send + Sync {
    fn finish_model_invocation_with_continuation(
        &self,
        command: &FinishModelInvocationWithContinuation,
        ids: ModelInvocationCompletionIds,
    ) -> impl Future<Output = Result<ModelInvocationCompletionMutation, ModelContinuationError>> + Send;

    fn get_model_continuation(
        &self,
        invocation_id: &ModelInvocationId,
        limits: ModelContinuationLimits,
    ) -> impl Future<Output = Result<Option<ModelInvocationContinuation>, ModelContinuationError>> + Send;
}

/// A context snapshot of private replay metadata, never the replay payload.
#[derive(Clone, PartialEq, Eq)]
pub struct ModelContinuationReference {
    session_id: crate::SessionId,
    invocation_id: ModelInvocationId,
    run_id: RunId,
    provider_account_id: ProviderAccountId,
    provider: ProviderType,
    model: ModelId,
    format: String,
    payload_size: u64,
    content_hash: crate::ContentHash,
    content_json: String,
}

impl fmt::Debug for ModelContinuationReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelContinuationReference")
            .field("invocation_id", &self.invocation_id)
            .field("bytes", &self.payload_size)
            .finish_non_exhaustive()
    }
}

impl ModelContinuationReference {
    pub fn new(
        run: &crate::Run,
        invocation: &ModelInvocation,
        format: String,
        payload_size: u64,
        content_hash: crate::ContentHash,
    ) -> Result<Self, ModelContinuationError> {
        if invocation.run_id() != run.run_id()
            || invocation.purpose() != ModelInvocationPurpose::Generation
            || !matches!(
                invocation
                    .outcome()
                    .and_then(|outcome| outcome.completion_kind()),
                Some(
                    ModelInvocationCompletionKind::AssistantOutput
                        | ModelInvocationCompletionKind::ToolRequests
                )
            )
            || payload_size == 0
            || !crate::model_tool_request::valid_identifier(&format, format.len())
        {
            return Err(ModelContinuationError::InvalidBinding);
        }
        let mut content = serde_json::json!({
            "version": 1,
            "session_id": run.session_id().as_str(),
            "run_id": run.run_id().as_str(),
            "model_invocation_id": invocation.invocation_id().as_str(),
            "provider_account_id": invocation.provider_account_id().as_str(),
            "provider": invocation.settings().provider().as_str(),
            "model": invocation.settings().model().as_str(),
            "format": format,
            "payload_size": payload_size,
            "content_hash": content_hash.as_str(),
        });
        content.sort_all_objects();
        let content_json = content.to_string();
        Ok(Self {
            session_id: run.session_id().clone(),
            invocation_id: invocation.invocation_id().clone(),
            run_id: run.run_id().clone(),
            provider_account_id: invocation.provider_account_id().clone(),
            provider: invocation.settings().provider().clone(),
            model: invocation.settings().model().clone(),
            format,
            payload_size,
            content_hash,
            content_json,
        })
    }

    pub fn validate_destination(
        &self,
        destination: &ModelInvocation,
    ) -> Result<(), ModelContinuationError> {
        if destination.invocation_id() == &self.invocation_id
            || destination.run_id() != &self.run_id
            || destination.provider_account_id() != &self.provider_account_id
            || destination.settings().provider() != &self.provider
            || destination.settings().model() != &self.model
            || destination.purpose() != ModelInvocationPurpose::Generation
        {
            return Err(ModelContinuationError::InvalidBinding);
        }
        Ok(())
    }

    pub fn matches(&self, continuation: &ModelInvocationContinuation) -> bool {
        continuation.invocation_id == self.invocation_id
            && continuation.run_id == self.run_id
            && continuation.provider_account_id == self.provider_account_id
            && continuation.provider == self.provider
            && continuation.model == self.model
            && continuation.format == self.format
            && continuation.payload.len() as u64 == self.payload_size
            && continuation.content_hash() == self.content_hash
    }

    pub fn session_id(&self) -> &crate::SessionId {
        &self.session_id
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn invocation_id(&self) -> &ModelInvocationId {
        &self.invocation_id
    }
    pub fn format(&self) -> &str {
        &self.format
    }
    pub fn payload_size(&self) -> u64 {
        self.payload_size
    }
    pub fn content_hash(&self) -> &crate::ContentHash {
        &self.content_hash
    }
    pub fn content_json(&self) -> &str {
        &self.content_json
    }
}
