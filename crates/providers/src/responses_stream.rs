use std::collections::{BTreeMap, HashSet};

use kiln_core::ProviderRequest;
use serde_json::Value;

use crate::{
    ResponsesCompletion, ResponsesCompletionError, ResponsesCompletionLimits, ResponsesSseDecoder,
    ResponsesSseError, ResponsesSseEvent, ResponsesSseLimits,
};

#[derive(Debug, Clone, Copy)]
pub struct ResponsesStreamLimits {
    pub framing: ResponsesSseLimits,
    pub completion: ResponsesCompletionLimits,
    /// Serialized completed items/terminal response plus identifiers and delta text.
    /// This is a data budget, not a process-memory estimate.
    pub max_retained_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponsesStreamError {
    Framing(ResponsesSseError),
    Completion(ResponsesCompletionError),
    InvalidEvent,
    InvalidSequence,
    IdentityMismatch,
    OutputMismatch,
    LimitExceeded,
    MissingTerminal,
    Closed,
}

/// A bounded semantic collector for one HTTP Responses stream. It buffers output
/// until terminal validation, so partial arguments never become executable
/// proposals. Token-by-token UI projection is intentionally a separate concern.
/// No Debug: state contains provider-private content.
pub struct ResponsesStream {
    request: ProviderRequest,
    decoder: ResponsesSseDecoder,
    limits: ResponsesStreamLimits,
    response_id: Option<String>,
    model: Option<String>,
    sequence: Option<u64>,
    items: BTreeMap<u64, StreamItem>,
    item_ids: HashSet<String>,
    retained: usize,
    completion: Option<ResponsesCompletion>,
    terminal: bool,
    closed: bool,
}

struct StreamItem {
    id: String,
    kind: String,
    name: Option<String>,
    call_id: Option<String>,
    // (part index, part type); function arguments use (0, "arguments").
    parts: BTreeMap<(u64, String), String>,
    done_parts: HashSet<(u64, String)>,
    completed: Option<Value>,
}

impl ResponsesStream {
    pub fn new(
        request: ProviderRequest,
        limits: ResponsesStreamLimits,
    ) -> Result<Self, ResponsesStreamError> {
        if limits.max_retained_bytes == 0 {
            return Err(ResponsesStreamError::LimitExceeded);
        }
        limits
            .completion
            .validate()
            .map_err(ResponsesStreamError::Completion)?;
        if request.invocation().settings().provider().as_str() != crate::OPENAI_API_PROVIDER_TYPE
            || request.invocation().purpose() != kiln_core::ModelInvocationPurpose::Generation
        {
            return Err(ResponsesStreamError::InvalidEvent);
        }
        let decoder =
            ResponsesSseDecoder::new(limits.framing).map_err(ResponsesStreamError::Framing)?;
        Ok(Self {
            request,
            decoder,
            limits,
            response_id: None,
            model: None,
            sequence: None,
            items: BTreeMap::new(),
            item_ids: HashSet::new(),
            retained: 0,
            completion: None,
            terminal: false,
            closed: false,
        })
    }

    pub fn push(
        &mut self,
        bytes: &[u8],
        observed_at_unix_ms: u64,
    ) -> Result<(), ResponsesStreamError> {
        if self.closed {
            return Err(ResponsesStreamError::Closed);
        }
        let result = self.push_inner(bytes, observed_at_unix_ms);
        if result.is_err() {
            self.closed = true;
        }
        result
    }

    fn push_inner(
        &mut self,
        bytes: &[u8],
        observed_at_unix_ms: u64,
    ) -> Result<(), ResponsesStreamError> {
        let events = self
            .decoder
            .push(bytes)
            .map_err(ResponsesStreamError::Framing)?;
        for event in events {
            if let Some(value) = self.accept(event, observed_at_unix_ms)? {
                self.completion = Some(value);
            }
        }
        Ok(())
    }

    fn accept(
        &mut self,
        event: ResponsesSseEvent,
        observed_at_unix_ms: u64,
    ) -> Result<Option<ResponsesCompletion>, ResponsesStreamError> {
        use ResponsesStreamError as Error;
        let ResponsesSseEvent::Event { kind, data } = event else {
            return if self.terminal {
                Ok(None)
            } else {
                Err(Error::MissingTerminal)
            };
        };
        if self.terminal {
            return Err(Error::InvalidEvent);
        }
        let sequence = number(&data["sequence_number"])?;
        if self.sequence.is_some_and(|last| sequence <= last) {
            return Err(Error::InvalidSequence);
        }
        self.sequence = Some(sequence);
        if kind == "response.created" {
            if self.response_id.is_some()
                || data["response"]["object"] != "response"
                || data["response"]["status"] != "in_progress"
            {
                return Err(Error::InvalidEvent);
            }
            let id = self.identifier(&data["response"]["id"])?;
            let model = self.identifier(&data["response"]["model"])?;
            self.reserve(id.len() + model.len())?;
            self.response_id = Some(id);
            self.model = Some(model);
            return Ok(None);
        }
        if self.response_id.is_none() {
            return Err(Error::InvalidEvent);
        }
        match kind.as_str() {
            "response.in_progress" => {
                self.check_response(&data["response"])?;
                if data["response"]["status"] != "in_progress" {
                    return Err(Error::InvalidEvent);
                }
            }
            "response.output_item.added" => self.add_item(&data)?,
            "response.content_part.added" | "response.reasoning_summary_part.added" => {
                self.add_part(&data, kind.contains("reasoning_summary"))?
            }
            "response.output_text.delta"
            | "response.refusal.delta"
            | "response.reasoning_summary_text.delta"
            | "response.function_call_arguments.delta" => {
                self.delta(&data, &kind)?;
            }
            "response.output_text.done"
            | "response.refusal.done"
            | "response.reasoning_summary_text.done"
            | "response.function_call_arguments.done" => {
                let (index, key) = self.part_key(&data, &kind)?;
                let field = if key.1 == "arguments" {
                    "arguments"
                } else if key.1 == "refusal" {
                    "refusal"
                } else {
                    "text"
                };
                let expected = string(&data[field])?;
                let item = self.items.get_mut(&index).ok_or(Error::InvalidEvent)?;
                if item.parts.get(&key).map(String::as_str) != Some(expected)
                    || !item.done_parts.insert(key)
                {
                    return Err(Error::OutputMismatch);
                }
            }
            "response.content_part.done" | "response.reasoning_summary_part.done" => {
                let summary = kind.contains("reasoning_summary");
                let (index, key) = self.part_from_payload(&data, summary)?;
                let expected = part_text(&data["part"])?;
                let item = self.items.get(&index).ok_or(Error::InvalidEvent)?;
                if item.parts.get(&key).map(String::as_str) != Some(expected)
                    || !item.done_parts.contains(&key)
                {
                    return Err(Error::OutputMismatch);
                }
            }
            "response.output_text.annotation.added" => {
                self.item_index(&data)?;
            }
            "response.output_item.done" => self.complete_item(&data)?,
            "response.completed" | "response.failed" | "response.incomplete" => {
                let response = &data["response"];
                self.check_response(response)?;
                let status = kind.strip_prefix("response.").ok_or(Error::InvalidEvent)?;
                if response["status"] != status {
                    return Err(Error::InvalidEvent);
                }
                if status == "completed" {
                    self.check_output(response)?;
                }
                let bytes = serde_json::to_vec(response).map_err(|_| Error::InvalidEvent)?;
                self.reserve(bytes.len())?;
                let result = ResponsesCompletion::from_response_json(
                    &self.request,
                    &bytes,
                    observed_at_unix_ms,
                    self.limits.completion,
                )
                .map_err(Error::Completion)?;
                self.terminal = true;
                return Ok(Some(result));
            }
            // Raw reasoning, built-in tools, compaction, server steering and new
            // event semantics require their own explicit normalization contract.
            _ => return Err(Error::InvalidEvent),
        }
        Ok(None)
    }

    fn add_item(&mut self, data: &Value) -> Result<(), ResponsesStreamError> {
        use ResponsesStreamError as Error;
        let index = number(&data["output_index"])?;
        let item = &data["item"];
        let id = self.identifier(&item["id"])?;
        let kind = string(&item["type"])?;
        if self.items.len() >= self.limits.completion.replay.max_items
            || self.items.contains_key(&index)
            || !self.item_ids.insert(id.clone())
        {
            return Err(Error::InvalidEvent);
        }
        let (name, call_id) = match kind {
            "message"
                if item["role"] == "assistant"
                    && item["content"].as_array().is_some_and(Vec::is_empty) =>
            {
                (None, None)
            }
            "reasoning"
                if item["summary"].as_array().is_some_and(Vec::is_empty)
                    && item["content"].is_null() =>
            {
                (None, None)
            }
            "function_call" => (
                Some(self.identifier(&item["name"])?),
                Some(self.identifier(&item["call_id"])?),
            ),
            _ => return Err(Error::InvalidEvent),
        };
        let mut parts = BTreeMap::new();
        let arguments = if kind == "function_call" {
            string(&item["arguments"])?
        } else {
            ""
        };
        self.reserve(
            id.len()
                + kind.len()
                + name.as_ref().map_or(0, String::len)
                + call_id.as_ref().map_or(0, String::len)
                + arguments.len(),
        )?;
        if kind == "function_call" {
            parts.insert((0, "arguments".into()), arguments.into());
        }
        self.items.insert(
            index,
            StreamItem {
                id,
                kind: kind.into(),
                name,
                call_id,
                parts,
                done_parts: HashSet::new(),
                completed: None,
            },
        );
        Ok(())
    }

    fn add_part(&mut self, data: &Value, summary: bool) -> Result<(), ResponsesStreamError> {
        let (index, key) = self.part_from_payload(data, summary)?;
        let text = part_text(&data["part"])?;
        self.reserve(text.len() + key.1.len())?;
        let item = self
            .items
            .get_mut(&index)
            .ok_or(ResponsesStreamError::InvalidEvent)?;
        if item.parts.insert(key, text.into()).is_some() {
            return Err(ResponsesStreamError::InvalidEvent);
        }
        Ok(())
    }

    fn delta(&mut self, data: &Value, kind: &str) -> Result<(), ResponsesStreamError> {
        let (index, key) = self.part_key(data, kind)?;
        let delta = string(&data["delta"])?;
        self.reserve(delta.len())?;
        let item = self
            .items
            .get_mut(&index)
            .ok_or(ResponsesStreamError::InvalidEvent)?;
        if item.done_parts.contains(&key) {
            return Err(ResponsesStreamError::InvalidEvent);
        }
        item.parts
            .get_mut(&key)
            .ok_or(ResponsesStreamError::InvalidEvent)?
            .push_str(delta);
        Ok(())
    }

    fn complete_item(&mut self, data: &Value) -> Result<(), ResponsesStreamError> {
        use ResponsesStreamError as Error;
        let index = number(&data["output_index"])?;
        let value = &data["item"];
        let item = self.items.get(&index).ok_or(Error::InvalidEvent)?;
        if item.completed.is_some() || value["id"] != item.id || value["type"] != item.kind {
            return Err(Error::OutputMismatch);
        }
        // Incomplete items can legitimately end without all delta streams being
        // finalized. Keep them only for a failed terminal's usage accounting;
        // successful replay validation will reject their incomplete status.
        if value["status"] == "incomplete" {
            let size = serde_json::to_vec(value)
                .map_err(|_| Error::InvalidEvent)?
                .len();
            self.reserve(size)?;
            self.items
                .get_mut(&index)
                .ok_or(Error::InvalidEvent)?
                .completed = Some(value.clone());
            return Ok(());
        }
        if item.parts.len() != item.done_parts.len() {
            return Err(Error::OutputMismatch);
        }
        match item.kind.as_str() {
            "function_call" => {
                if value["name"].as_str() != item.name.as_deref()
                    || value["call_id"].as_str() != item.call_id.as_deref()
                    || value["arguments"].as_str()
                        != item.parts.get(&(0, "arguments".into())).map(String::as_str)
                {
                    return Err(Error::OutputMismatch);
                }
            }
            kind => {
                let parts = value[if kind == "message" {
                    "content"
                } else {
                    "summary"
                }]
                .as_array()
                .ok_or(Error::OutputMismatch)?;
                if parts.len() != item.parts.len() {
                    return Err(Error::OutputMismatch);
                }
                for (index, part) in parts.iter().enumerate() {
                    let key = (
                        u64::try_from(index).map_err(|_| Error::LimitExceeded)?,
                        string(&part["type"])?.to_owned(),
                    );
                    if item.parts.get(&key).map(String::as_str) != Some(part_text(part)?) {
                        return Err(Error::OutputMismatch);
                    }
                }
            }
        }
        let size = serde_json::to_vec(value)
            .map_err(|_| Error::InvalidEvent)?
            .len();
        self.reserve(size)?;
        self.items
            .get_mut(&index)
            .ok_or(Error::InvalidEvent)?
            .completed = Some(value.clone());
        Ok(())
    }

    fn check_output(&self, response: &Value) -> Result<(), ResponsesStreamError> {
        let output = response["output"]
            .as_array()
            .ok_or(ResponsesStreamError::OutputMismatch)?;
        if output.len() != self.items.len() {
            return Err(ResponsesStreamError::OutputMismatch);
        }
        for (index, item) in output.iter().enumerate() {
            let index = u64::try_from(index).map_err(|_| ResponsesStreamError::LimitExceeded)?;
            if self.items.get(&index).and_then(|i| i.completed.as_ref()) != Some(item) {
                return Err(ResponsesStreamError::OutputMismatch);
            }
        }
        Ok(())
    }

    fn item_index(&self, data: &Value) -> Result<u64, ResponsesStreamError> {
        let index = number(&data["output_index"])?;
        let item = self
            .items
            .get(&index)
            .ok_or(ResponsesStreamError::InvalidEvent)?;
        if data["item_id"] != item.id || item.completed.is_some() {
            return Err(ResponsesStreamError::IdentityMismatch);
        }
        Ok(index)
    }

    fn part_key(
        &self,
        data: &Value,
        kind: &str,
    ) -> Result<(u64, (u64, String)), ResponsesStreamError> {
        let index = self.item_index(data)?;
        let (part, expected_item, field) = if kind.contains("function_call_arguments") {
            ("arguments", "function_call", None)
        } else if kind.contains("reasoning_summary_text") {
            ("summary_text", "reasoning", Some("summary_index"))
        } else if kind.contains("refusal") {
            ("refusal", "message", Some("content_index"))
        } else {
            ("output_text", "message", Some("content_index"))
        };
        if self.items[&index].kind != expected_item {
            return Err(ResponsesStreamError::InvalidEvent);
        }
        Ok((
            index,
            (field.map_or(Ok(0), |f| number(&data[f]))?, part.into()),
        ))
    }

    fn part_from_payload(
        &self,
        data: &Value,
        summary: bool,
    ) -> Result<(u64, (u64, String)), ResponsesStreamError> {
        let kind = string(&data["part"]["type"])?;
        let event = match (summary, kind) {
            (true, "summary_text") => "reasoning_summary_text",
            (false, "output_text") => "output_text",
            (false, "refusal") => "refusal",
            _ => return Err(ResponsesStreamError::InvalidEvent),
        };
        self.part_key(data, event)
    }

    fn check_response(&self, response: &Value) -> Result<(), ResponsesStreamError> {
        if response["object"] != "response"
            || response["id"].as_str() != self.response_id.as_deref()
            || response["model"].as_str() != self.model.as_deref()
        {
            return Err(ResponsesStreamError::IdentityMismatch);
        }
        Ok(())
    }

    fn identifier(&self, value: &Value) -> Result<String, ResponsesStreamError> {
        let value = string(value)?;
        if value.is_empty()
            || value.len() > self.limits.completion.max_identifier_bytes
            || !value.is_ascii()
            || value
                .bytes()
                .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
        {
            return Err(ResponsesStreamError::InvalidEvent);
        }
        Ok(value.into())
    }

    fn reserve(&mut self, bytes: usize) -> Result<(), ResponsesStreamError> {
        self.retained = self
            .retained
            .checked_add(bytes)
            .ok_or(ResponsesStreamError::LimitExceeded)?;
        if self.retained > self.limits.max_retained_bytes {
            return Err(ResponsesStreamError::LimitExceeded);
        }
        Ok(())
    }

    /// Release the candidate completion only after clean transport EOF. A bad
    /// tail cannot arrive after the caller has persisted a successful terminal.
    pub fn finish(&mut self) -> Result<ResponsesCompletion, ResponsesStreamError> {
        if self.closed {
            return Err(ResponsesStreamError::Closed);
        }
        self.closed = true;
        self.decoder
            .finish()
            .map_err(ResponsesStreamError::Framing)?;
        if !self.terminal {
            return Err(ResponsesStreamError::MissingTerminal);
        }
        self.completion
            .take()
            .ok_or(ResponsesStreamError::MissingTerminal)
    }
}

fn number(value: &Value) -> Result<u64, ResponsesStreamError> {
    value.as_u64().ok_or(ResponsesStreamError::InvalidEvent)
}
fn string(value: &Value) -> Result<&str, ResponsesStreamError> {
    value.as_str().ok_or(ResponsesStreamError::InvalidEvent)
}
fn part_text(part: &Value) -> Result<&str, ResponsesStreamError> {
    string(
        &part[if part["type"] == "refusal" {
            "refusal"
        } else {
            "text"
        }],
    )
}
