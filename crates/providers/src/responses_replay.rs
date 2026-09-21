use std::{collections::HashSet, fmt, io::Write};

use serde_json::Value;

/// Caller-selected wire budgets, not model token-window or memory estimates.
#[derive(Debug, Clone, Copy)]
pub struct ResponsesReplayLimits {
    pub max_output_bytes: usize,
    pub max_items: usize,
    pub max_item_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponsesReplayError {
    InvalidLimits,
    OutputLimitExceeded,
    InvalidJson,
    ItemLimitExceeded,
    UnsupportedItem { position: usize },
    IncompleteItem { position: usize },
    MissingEncryptedReasoning { position: usize },
    DuplicateCallId { position: usize },
}

/// Replayable Responses output, including encrypted reasoning and assistant
/// phase. This is provider-private input data, never a tool grant or UI stream.
/// No credentials, endpoint selection, storage, or network effects are owned here.
pub struct ResponsesReplay {
    json: Vec<u8>,
    item_count: usize,
}

impl fmt::Debug for ResponsesReplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponsesReplay")
            .field("item_count", &self.item_count)
            .field("bytes", &self.json.len())
            .finish_non_exhaustive()
    }
}

impl ResponsesReplay {
    /// Accept the complete terminal response's `output` array. Canonical JSON
    /// serialization preserves every accepted field and array order, including
    /// provider IDs, annotations, encrypted content, and assistant phase.
    pub fn from_output_json(
        output: &[u8],
        limits: ResponsesReplayLimits,
    ) -> Result<Self, ResponsesReplayError> {
        use ResponsesReplayError as Error;
        if limits.max_output_bytes == 0 || limits.max_items == 0 || limits.max_item_bytes == 0 {
            return Err(Error::InvalidLimits);
        }
        if output.len() > limits.max_output_bytes {
            return Err(Error::OutputLimitExceeded);
        }
        let items: Vec<Value> = serde_json::from_slice(output).map_err(|_| Error::InvalidJson)?;
        if items.len() > limits.max_items {
            return Err(Error::ItemLimitExceeded);
        }
        let mut calls = HashSet::new();
        let mut json = Vec::new();
        append(&mut json, b"[", limits.max_output_bytes)?;
        for (position, item) in items.iter().enumerate() {
            validate_item(item, position)?;
            if item["type"] == "function_call" {
                let call_id = item["call_id"]
                    .as_str()
                    .ok_or(Error::UnsupportedItem { position })?;
                if !calls.insert(call_id) {
                    return Err(Error::DuplicateCallId { position });
                }
            }
            if position != 0 {
                append(&mut json, b",", limits.max_output_bytes)?;
            }
            let available = limits.max_output_bytes - json.len();
            let mut writer = LimitedWriter {
                bytes: &mut json,
                remaining: available.min(limits.max_item_bytes),
            };
            serde_json::to_writer(&mut writer, item).map_err(|_| Error::OutputLimitExceeded)?;
        }
        append(&mut json, b"]", limits.max_output_bytes)?;
        Ok(Self {
            json,
            item_count: items.len(),
        })
    }

    pub fn as_json(&self) -> &[u8] {
        &self.json
    }

    pub fn item_count(&self) -> usize {
        self.item_count
    }

    pub fn into_continuation(
        self,
        invocation: &kiln_core::ModelInvocation,
        limits: kiln_core::ModelContinuationLimits,
    ) -> Result<kiln_core::ModelInvocationContinuation, kiln_core::ModelContinuationError> {
        if !matches!(
            invocation.settings().provider().as_str(),
            crate::OPENAI_CODEX_SUBSCRIPTION_PROVIDER_TYPE | crate::OPENAI_API_PROVIDER_TYPE
        ) {
            return Err(kiln_core::ModelContinuationError::InvalidBinding);
        }
        kiln_core::ModelInvocationContinuation::new(
            invocation,
            "openai.responses.output.v1".into(),
            self.json,
            limits,
        )
    }
}

fn validate_item(item: &Value, position: usize) -> Result<(), ResponsesReplayError> {
    use ResponsesReplayError as Error;
    let invalid = Error::UnsupportedItem { position };
    if !item.is_object() {
        return Err(invalid);
    }
    if item
        .get("status")
        .is_some_and(|status| !status.is_null() && status != "completed")
    {
        return Err(Error::IncompleteItem { position });
    }
    match item["type"].as_str() {
        Some("message") => {
            if item["role"] != "assistant"
                || item.get("phase").is_some_and(|phase| {
                    !phase.is_null() && phase != "commentary" && phase != "final_answer"
                })
                || !item["content"].as_array().is_some_and(|parts| {
                    parts.iter().all(|part| match part["type"].as_str() {
                        Some("output_text") => part["text"].is_string(),
                        Some("refusal") => part["refusal"].is_string(),
                        _ => false,
                    })
                })
            {
                return Err(invalid);
            }
        }
        Some("reasoning") => {
            if !nonempty_string(&item["encrypted_content"]) {
                return Err(Error::MissingEncryptedReasoning { position });
            }
            // Never accept a raw reasoning-content field as replay state.
            // Safe summaries and opaque ciphertext are distinct from that data.
            if item
                .get("content")
                .is_some_and(|content| !content.is_null())
                || !item["summary"].as_array().is_some_and(|parts| {
                    parts
                        .iter()
                        .all(|part| part["type"] == "summary_text" && part["text"].is_string())
                })
            {
                return Err(invalid);
            }
        }
        Some("function_call") => {
            if !nonempty_string(&item["call_id"])
                || !nonempty_string(&item["name"])
                // Current Kiln function catalogs have no namespace, asynchronous,
                // or programmatic execution semantics.
                || item.get("namespace").is_some_and(|v| !v.is_null())
                || item.get("async").is_some_and(|v| !v.is_null() && v != false)
                || item.get("caller").is_some_and(|v| !v.is_null() && v != &serde_json::json!({"type": "direct"}))
                || !item["arguments"].as_str().is_some_and(|arguments| {
                    serde_json::from_str::<serde_json::Map<String, Value>>(arguments).is_ok()
                })
            {
                return Err(invalid);
            }
        }
        _ => return Err(invalid),
    }
    Ok(())
}

fn nonempty_string(value: &Value) -> bool {
    value.as_str().is_some_and(|text| !text.is_empty())
}

fn append(bytes: &mut Vec<u8>, value: &[u8], limit: usize) -> Result<(), ResponsesReplayError> {
    if value.len() > limit.saturating_sub(bytes.len()) {
        return Err(ResponsesReplayError::OutputLimitExceeded);
    }
    bytes.extend_from_slice(value);
    Ok(())
}

struct LimitedWriter<'a> {
    bytes: &'a mut Vec<u8>,
    remaining: usize,
}

impl Write for LimitedWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(std::io::Error::other("response replay byte limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        self.remaining -= bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
