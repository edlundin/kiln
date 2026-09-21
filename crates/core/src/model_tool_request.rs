use std::{collections::HashSet, fmt, future::Future, io::Write};

use crate::{
    CapabilitySupport, ModelInvocation, ModelInvocationCompletionIds,
    ModelInvocationCompletionMutation, ModelInvocationId, ModelInvocationPurpose,
    ModelInvocationState, ModelInvocationStoreError, ProviderUsageUpdate, UsageFinality,
    UsageStoreError,
};

/// Complete provider output only. A transport must finish parsing its argument
/// object before submitting it; partial argument fragments are not tool requests.
pub struct ModelToolRequestInput {
    pub provider_call_id: String,
    pub name: String,
    pub arguments: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Copy)]
pub struct ModelToolRequestLimits {
    pub max_requests: usize,
    pub max_provider_call_id_bytes: usize,
    pub max_name_bytes: usize,
    pub max_arguments_bytes: usize,
    pub max_total_arguments_bytes: usize,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ModelToolRequest {
    provider_call_id: String,
    name: String,
    arguments_json: String,
}

impl ModelToolRequest {
    pub fn provider_call_id(&self) -> &str {
        &self.provider_call_id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn arguments_json(&self) -> &str {
        &self.arguments_json
    }
}

/// Requests are inert model proposals. Recording them grants no tool capability,
/// scope, approval, or permission to execute. Names are not capability identifiers.
#[derive(Clone, PartialEq, Eq)]
pub struct ModelToolRequestBatch {
    invocation_id: ModelInvocationId,
    requests: Vec<ModelToolRequest>,
}

impl fmt::Debug for ModelToolRequestBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelToolRequestBatch")
            .field("invocation_id", &self.invocation_id)
            .field("request_count", &self.requests.len())
            .finish_non_exhaustive()
    }
}

impl ModelToolRequestBatch {
    pub fn new(
        invocation_id: ModelInvocationId,
        inputs: Vec<ModelToolRequestInput>,
        limits: ModelToolRequestLimits,
    ) -> Result<Self, ModelToolRequestError> {
        if limits.max_requests == 0
            || limits.max_provider_call_id_bytes == 0
            || limits.max_name_bytes == 0
            || limits.max_arguments_bytes == 0
            || limits.max_total_arguments_bytes == 0
        {
            return Err(ModelToolRequestError::InvalidLimits);
        }
        if inputs.is_empty() || inputs.len() > limits.max_requests {
            return Err(ModelToolRequestError::RequestLimitExceeded);
        }
        let mut seen = HashSet::new();
        let mut requests = Vec::with_capacity(inputs.len());
        let mut total_bytes = 0_usize;
        for input in inputs {
            if !valid_identifier(&input.provider_call_id, limits.max_provider_call_id_bytes)
                || !valid_identifier(&input.name, limits.max_name_bytes)
            {
                return Err(ModelToolRequestError::InvalidIdentifier);
            }
            if !seen.insert(input.provider_call_id.clone()) {
                return Err(ModelToolRequestError::DuplicateCallId);
            }
            let remaining = limits
                .max_total_arguments_bytes
                .checked_sub(total_bytes)
                .ok_or(ModelToolRequestError::ArgumentsLimitExceeded)?;
            let mut buffer = LimitedBuffer {
                bytes: Vec::new(),
                limit: remaining.min(limits.max_arguments_bytes),
            };
            let mut arguments = serde_json::Value::Object(input.arguments);
            arguments.sort_all_objects();
            serde_json::to_writer(&mut buffer, &arguments)
                .map_err(|_| ModelToolRequestError::ArgumentsLimitExceeded)?;
            total_bytes += buffer.bytes.len();
            let arguments_json = String::from_utf8(buffer.bytes)
                .map_err(|_| ModelToolRequestError::InvalidArguments)?;
            // Persist only values that the storage reader can reconstruct,
            // including its JSON nesting limit and exact floating-point values.
            let restored: serde_json::Value = serde_json::from_str(&arguments_json)
                .map_err(|_| ModelToolRequestError::InvalidArguments)?;
            if restored != arguments {
                return Err(ModelToolRequestError::InvalidArguments);
            }
            requests.push(ModelToolRequest {
                provider_call_id: input.provider_call_id,
                name: input.name,
                arguments_json,
            });
        }
        Ok(Self {
            invocation_id,
            requests,
        })
    }

    pub fn invocation_id(&self) -> &ModelInvocationId {
        &self.invocation_id
    }
    pub fn requests(&self) -> &[ModelToolRequest] {
        &self.requests
    }

    pub fn validate_completion(
        &self,
        invocation: &ModelInvocation,
        usage: &ProviderUsageUpdate,
    ) -> Result<(), ModelToolRequestError> {
        if self.invocation_id != *invocation.invocation_id()
            || invocation.purpose() != ModelInvocationPurpose::Generation
            || invocation.capabilities().tool_calls() != CapabilitySupport::Supported
            || !matches!(
                invocation.state(),
                ModelInvocationState::InFlight | ModelInvocationState::Completed
            )
        {
            return Err(ModelToolRequestError::InvalidInvocation);
        }
        let metadata = usage.metadata();
        if metadata.model_invocation_id != self.invocation_id
            || metadata.work_id != *invocation.work_id()
            || metadata.provider_account_id != *invocation.provider_account_id()
            || metadata.finality != UsageFinality::Final
        {
            return Err(ModelToolRequestError::Usage(
                UsageStoreError::AttributionMismatch,
            ));
        }
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        crate::push_context_field(&mut bytes, b"kiln.model-tool-requests.v1");
        crate::push_context_field(&mut bytes, self.invocation_id.as_str().as_bytes());
        crate::push_context_field(&mut bytes, &(self.requests.len() as u64).to_be_bytes());
        for request in &self.requests {
            crate::push_context_field(&mut bytes, request.provider_call_id.as_bytes());
            crate::push_context_field(&mut bytes, request.name.as_bytes());
            crate::push_context_field(&mut bytes, request.arguments_json.as_bytes());
        }
        bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelToolRequestError {
    InvalidLimits,
    RequestLimitExceeded,
    ArgumentsLimitExceeded,
    InvalidIdentifier,
    InvalidArguments,
    DuplicateCallId,
    InvalidInvocation,
    IdempotencyConflict,
    IntegrityViolation,
    Unavailable,
    Invocation(ModelInvocationStoreError),
    Usage(UsageStoreError),
}

pub struct ModelToolRequestCompletion {
    pub completion: ModelInvocationCompletionMutation,
    pub requests: ModelToolRequestBatch,
}

pub trait ModelToolRequestStore: Send + Sync {
    fn finish_model_invocation_with_tool_requests(
        &self,
        invocation: &ModelInvocation,
        requests: &ModelToolRequestBatch,
        usage: &ProviderUsageUpdate,
        ids: ModelInvocationCompletionIds,
    ) -> impl Future<Output = Result<ModelToolRequestCompletion, ModelToolRequestError>> + Send;

    fn get_model_tool_requests(
        &self,
        invocation_id: &ModelInvocationId,
    ) -> impl Future<Output = Result<Option<ModelToolRequestBatch>, ModelToolRequestError>> + Send;
}

fn valid_identifier(value: &str, limit: usize) -> bool {
    !value.is_empty()
        && value.len() <= limit
        && value.is_ascii()
        && !value
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
}

struct LimitedBuffer {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for LimitedBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("argument byte limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
