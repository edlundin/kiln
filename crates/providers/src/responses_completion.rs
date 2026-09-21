use kiln_core::{
    ModelContinuationLimits, ModelInvocationCompletionKind, ModelInvocationOutcome,
    ModelOutputStream, ModelToolRequestBatch, ModelToolRequestInput, ModelToolRequestLimits,
    ProviderRequest, ProviderUpdate, ProviderUsageMetadata, ProviderUsageUpdate, QuantityRelation,
    RecordModelOutput, UsageAccounting, UsageCompleteness, UsageFinality, UsageQuantity,
    UsageSource,
};
use serde_json::Value;

use crate::{OPENAI_API_PROVIDER_TYPE, ResponsesReplay, ResponsesReplayLimits};

#[derive(Debug, Clone, Copy)]
pub struct ResponsesCompletionLimits {
    pub max_response_bytes: usize,
    pub max_identifier_bytes: usize,
    pub max_visible_output_bytes: usize,
    pub replay: ResponsesReplayLimits,
    pub requests: ModelToolRequestLimits,
    pub continuation: ModelContinuationLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponsesCompletionError {
    InvalidLimits,
    ResponseLimitExceeded,
    InvalidEnvelope,
    NotCompleted,
    InvalidReplay,
    InvalidToolRequests,
    InvalidUsage,
    InvalidOutput,
    InvalidContinuation,
}

/// One successful response within the supported Responses subset, not an SSE decoder. No updates are
/// returned until output, proposals, replay and final usage all validate.
/// Deliberately no Debug: the response contains model content and private replay.
pub struct ResponsesCompletion {
    output: Vec<RecordModelOutput>,
    terminal: ProviderUpdate,
}

impl ResponsesCompletion {
    pub fn from_response_json(
        request: &ProviderRequest,
        response: &[u8],
        observed_at_unix_ms: u64,
        limits: ResponsesCompletionLimits,
    ) -> Result<Self, ResponsesCompletionError> {
        use ResponsesCompletionError as Error;
        if limits.max_response_bytes == 0
            || limits.max_identifier_bytes == 0
            || limits.max_visible_output_bytes == 0
            || limits.replay.max_output_bytes == 0
            || limits.replay.max_item_bytes == 0
            || limits.replay.max_items == 0
            || limits.requests.max_requests == 0
            || limits.requests.max_provider_call_id_bytes == 0
            || limits.requests.max_name_bytes == 0
            || limits.requests.max_arguments_bytes == 0
            || limits.requests.max_total_arguments_bytes == 0
            || limits.continuation.max_format_bytes == 0
            || limits.continuation.max_payload_bytes == 0
        {
            return Err(Error::InvalidLimits);
        }
        if response.len() > limits.max_response_bytes {
            return Err(Error::ResponseLimitExceeded);
        }
        let invocation = request.invocation();
        if invocation.settings().provider().as_str() != OPENAI_API_PROVIDER_TYPE {
            return Err(Error::InvalidEnvelope);
        }
        let envelope: Value =
            serde_json::from_slice(response).map_err(|_| Error::InvalidEnvelope)?;
        if envelope["object"] != "response" {
            return Err(Error::InvalidEnvelope);
        }
        if envelope["status"] != "completed" {
            return Err(Error::NotCompleted);
        }
        if !envelope["error"].is_null() || !envelope["incomplete_details"].is_null() {
            return Err(Error::InvalidEnvelope);
        }
        let response_id = identifier(&envelope["id"], limits.max_identifier_bytes)?;
        let resolved_model = identifier(&envelope["model"], limits.max_identifier_bytes)?;
        let service_tier = if envelope["service_tier"].is_null() {
            None
        } else {
            Some(identifier(&envelope["service_tier"], limits.max_identifier_bytes)?.to_owned())
        };
        let output_json =
            serde_json::to_vec(&envelope["output"]).map_err(|_| Error::InvalidReplay)?;
        let replay = ResponsesReplay::from_output_json(&output_json, limits.replay)
            .map_err(|_| Error::InvalidReplay)?;
        let items = envelope["output"].as_array().ok_or(Error::InvalidReplay)?;
        let mut output = Vec::new();
        let mut requests = Vec::new();
        let mut visible_bytes = 0usize;
        let mut has_assistant_text = false;
        for item in items {
            match item["type"].as_str() {
                Some("function_call") => {
                    let name = item["name"].as_str().ok_or(Error::InvalidToolRequests)?;
                    if request.tool_catalog().find(name).is_none() {
                        return Err(Error::InvalidToolRequests);
                    }
                    requests.push(ModelToolRequestInput {
                        provider_call_id: item["call_id"]
                            .as_str()
                            .ok_or(Error::InvalidToolRequests)?
                            .to_owned(),
                        name: name.to_owned(),
                        arguments: serde_json::from_str(
                            item["arguments"]
                                .as_str()
                                .ok_or(Error::InvalidToolRequests)?,
                        )
                        .map_err(|_| Error::InvalidToolRequests)?,
                    });
                }
                Some("message") | Some("reasoning") => {
                    let is_message = item["type"] == "message";
                    let parts = item[if is_message { "content" } else { "summary" }]
                        .as_array()
                        .ok_or(Error::InvalidOutput)?;
                    for part in parts {
                        let text = part[if part["type"] == "refusal" {
                            "refusal"
                        } else {
                            "text"
                        }]
                        .as_str()
                        .ok_or(Error::InvalidOutput)?;
                        // Core output chunks must be nonempty; the complete replay still
                        // retains empty parts, phase, annotations and provider item IDs.
                        if text.is_empty() {
                            continue;
                        }
                        visible_bytes = visible_bytes
                            .checked_add(text.len())
                            .ok_or(Error::ResponseLimitExceeded)?;
                        if visible_bytes > limits.max_visible_output_bytes {
                            return Err(Error::ResponseLimitExceeded);
                        }
                        let position = u64::try_from(output.len())
                            .ok()
                            .and_then(|p| p.checked_add(1))
                            .ok_or(Error::InvalidOutput)?;
                        output.push(
                            RecordModelOutput::new(
                                invocation.invocation_id().clone(),
                                format!(
                                    "responses:{}:output:{position}",
                                    invocation.invocation_id().as_str()
                                ),
                                position,
                                if is_message {
                                    ModelOutputStream::AssistantText
                                } else {
                                    ModelOutputStream::ReasoningSummary
                                },
                                text.to_owned(),
                            )
                            .map_err(|_| Error::InvalidOutput)?,
                        );
                        has_assistant_text |= is_message;
                    }
                }
                _ => return Err(Error::InvalidReplay),
            }
        }
        if requests.is_empty() && !has_assistant_text {
            return Err(Error::InvalidOutput);
        }
        let requests = if requests.is_empty() {
            None
        } else {
            Some(
                ModelToolRequestBatch::new(
                    invocation.invocation_id().clone(),
                    requests,
                    limits.requests,
                )
                .map_err(|_| Error::InvalidToolRequests)?,
            )
        };
        let (completeness, quantities) = usage_quantities(&envelope["usage"])?;
        let usage = ProviderUsageUpdate::new(
            ProviderUsageMetadata {
                update_id: format!("responses:{}:terminal", invocation.invocation_id().as_str()),
                provider_account_id: invocation.provider_account_id().clone(),
                work_id: invocation.work_id().clone(),
                model_invocation_id: invocation.invocation_id().clone(),
                accounting: UsageAccounting::Cumulative,
                finality: UsageFinality::Final,
                completeness,
                observed_at_unix_ms,
                request_id: Some(response_id.to_owned()),
                resolved_model: Some(resolved_model.to_owned()),
                service_tier,
                source: UsageSource::NativeProvider,
            },
            quantities,
        )
        .map_err(|_| Error::InvalidUsage)?;
        let outcome = ModelInvocationOutcome::completed(if requests.is_some() {
            ModelInvocationCompletionKind::ToolRequests
        } else {
            ModelInvocationCompletionKind::AssistantOutput
        });
        let continuation = replay
            .into_continuation(invocation, limits.continuation)
            .map_err(|_| Error::InvalidContinuation)?;
        let terminal = ProviderUpdate::CompletedWithContinuation {
            outcome,
            usage,
            requests,
            continuation,
        };
        terminal
            .validate_for(invocation)
            .map_err(|_| Error::InvalidContinuation)?;
        Ok(Self { output, terminal })
    }

    /// A future streaming adapter must reconcile emitted deltas with these final
    /// chunks before persisting the terminal update; do not emit both copies.
    pub fn output(&self) -> &[RecordModelOutput] {
        &self.output
    }

    pub fn into_parts(self) -> (Vec<RecordModelOutput>, ProviderUpdate) {
        (self.output, self.terminal)
    }
}

fn identifier(value: &Value, limit: usize) -> Result<&str, ResponsesCompletionError> {
    let value = value
        .as_str()
        .ok_or(ResponsesCompletionError::InvalidEnvelope)?;
    if value.is_empty()
        || value.len() > limit
        || !value.is_ascii()
        || value
            .bytes()
            .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
    {
        return Err(ResponsesCompletionError::InvalidEnvelope);
    }
    Ok(value)
}

fn usage_quantities(
    value: &Value,
) -> Result<(UsageCompleteness, Vec<UsageQuantity>), ResponsesCompletionError> {
    use ResponsesCompletionError as Error;
    if value.is_null() {
        return Ok((UsageCompleteness::Unknown, Vec::new()));
    }
    let object = value.as_object().ok_or(Error::InvalidUsage)?;
    let input = value["input_tokens"].as_u64().ok_or(Error::InvalidUsage)?;
    let output = value["output_tokens"].as_u64().ok_or(Error::InvalidUsage)?;
    let total = value["total_tokens"].as_u64().ok_or(Error::InvalidUsage)?;
    if input.checked_add(output) != Some(total) {
        return Err(Error::InvalidUsage);
    }
    let mut quantities = vec![
        UsageQuantity::new("tokens.input", "token", input, QuantityRelation::Additive)
            .map_err(|_| Error::InvalidUsage)?,
        UsageQuantity::new("tokens.output", "token", output, QuantityRelation::Additive)
            .map_err(|_| Error::InvalidUsage)?,
    ];
    let mut complete = object.keys().all(|key| {
        matches!(
            key.as_str(),
            "input_tokens"
                | "output_tokens"
                | "total_tokens"
                | "input_tokens_details"
                | "output_tokens_details"
        )
    });
    for (field, parent, mappings) in [
        (
            "input_tokens_details",
            "tokens.input",
            &[
                ("cached_tokens", "tokens.input.cached"),
                ("cache_write_tokens", "tokens.input.cache_write"),
            ][..],
        ),
        (
            "output_tokens_details",
            "tokens.output",
            &[("reasoning_tokens", "tokens.output.reasoning")][..],
        ),
    ] {
        if value[field].is_null() {
            complete = false;
            continue;
        }
        let details = value[field].as_object().ok_or(Error::InvalidUsage)?;
        complete &= details
            .keys()
            .all(|key| mappings.iter().any(|(wire, _)| key == wire));
        for (wire, dimension) in mappings {
            match details.get(*wire) {
                None | Some(Value::Null) => {
                    complete = false;
                }
                Some(amount) => quantities.push(
                    UsageQuantity::new(
                        *dimension,
                        "token",
                        amount.as_u64().ok_or(Error::InvalidUsage)?,
                        QuantityRelation::Subset { of: parent.into() },
                    )
                    .map_err(|_| Error::InvalidUsage)?,
                ),
            }
        }
    }
    Ok((
        if complete {
            UsageCompleteness::Complete
        } else {
            UsageCompleteness::Partial
        },
        quantities,
    ))
}
