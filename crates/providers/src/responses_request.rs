use std::{
    collections::{HashMap, HashSet},
    fmt,
    io::Write,
};

use base64::{Engine, engine::general_purpose::STANDARD};
use kiln_core::{
    CapabilitySupport, ContextInstructionProvenance, MessageRole, ModelInvocationId,
    ModelInvocationPurpose, ProviderContext, ProviderContextAttachment, ProviderContextEntry,
    ProviderRequest,
};
use serde_json::{Value, json};

use crate::{OPENAI_API_PROVIDER_TYPE, ResponsesReplay, ResponsesReplayLimits};

/// Wire budgets supplied by the adapter, not model token-window estimates.
#[derive(Debug, Clone, Copy)]
pub struct ResponsesRequestLimits {
    pub max_request_bytes: usize,
    pub max_input_items: usize,
    pub replay: ResponsesReplayLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponsesRequestError {
    InvalidLimits,
    UnsupportedProvider,
    UnsupportedPurpose,
    UnsupportedSettings,
    ContextMismatch,
    RequestLimitExceeded,
    ItemLimitExceeded,
    InvalidReplay,
    InvalidToolHistory,
    InvalidToolDefinition,
    UnsupportedAttachment,
    VisionUnsupported,
    InvalidText,
}

/// Private public-API request body. Contains model input and opaque replay;
/// never log it or expose it through Events. This type owns no transport or
/// credentials and does not establish subscription-endpoint compatibility.
pub struct ResponsesRequestBody {
    json: Vec<u8>,
}

impl fmt::Debug for ResponsesRequestBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponsesRequestBody")
            .field("bytes", &self.json.len())
            .finish_non_exhaustive()
    }
}

impl ResponsesRequestBody {
    pub fn from_context(
        request: &ProviderRequest,
        context: &ProviderContext,
        limits: ResponsesRequestLimits,
    ) -> Result<Self, ResponsesRequestError> {
        use ResponsesRequestError as Error;
        if limits.max_request_bytes == 0
            || limits.max_input_items == 0
            || limits.replay.max_output_bytes == 0
            || limits.replay.max_item_bytes == 0
            || limits.replay.max_items == 0
        {
            return Err(Error::InvalidLimits);
        }
        let invocation = request.invocation();
        if invocation.settings().provider().as_str() != OPENAI_API_PROVIDER_TYPE {
            return Err(Error::UnsupportedProvider);
        }
        if invocation.purpose() != ModelInvocationPurpose::Generation {
            return Err(Error::UnsupportedPurpose);
        }
        if context.manifest_id() != invocation.context_manifest_id()
            || context.manifest_hash() != invocation.context_manifest_hash()
            || context.run_id() != invocation.run_id()
            || context.session_id() != request.manifest().session_id()
        {
            return Err(Error::ContextMismatch);
        }
        request
            .tool_catalog()
            .validate_for(invocation)
            .map_err(|_| Error::InvalidToolDefinition)?;

        let mut writer = RequestWriter {
            bytes: Vec::new(),
            limit: limits.max_request_bytes,
            items: 0,
            max_items: limits.max_input_items,
        };
        let mut header = json!({
            "model": invocation.settings().model().as_str(),
            "store": false,
            "stream": true,
            "parallel_tool_calls": false,
            "truncation": "disabled",
            "include": ["reasoning.encrypted_content"],
        });
        if let Some(tokens) = invocation.settings().generation().max_output_tokens() {
            // Public Responses schema minimum; never silently raise a requested cap.
            if tokens < 16 {
                return Err(Error::UnsupportedSettings);
            }
            header["max_output_tokens"] = tokens.into();
        }
        if let Some(effort) = invocation.settings().reasoning().effort() {
            if !matches!(
                effort,
                "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
            ) {
                return Err(Error::UnsupportedSettings);
            }
            header["reasoning"] = json!({"effort": effort});
        }
        writer.json(&header)?;
        writer.bytes.pop(); // Append arrays without materializing the complete request tree.
        writer.raw(b",\"tools\":[")?;
        for (position, definition) in request.tool_catalog().definitions().iter().enumerate() {
            if position != 0 {
                writer.raw(b",")?;
            }
            // Responses function names have a narrower contract than Kiln identifiers.
            if definition.name().len() > 64
                || !definition
                    .name()
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            {
                return Err(Error::InvalidToolDefinition);
            }
            let schema: Value = serde_json::from_str(definition.input_schema_json())
                .map_err(|_| Error::InvalidToolDefinition)?;
            writer.json(&json!({"type": "function", "name": definition.name(),
                "description": definition.description(), "parameters": schema, "strict": false}))?;
        }
        writer.raw(b"],\"input\":[")?;
        let mut seen_calls = HashSet::new();
        let mut seen_sources = HashSet::new();
        let mut pending = HashMap::<String, ReplayedCall>::new();
        let vision = invocation.capabilities().vision() == CapabilitySupport::Supported;
        let mut file_bytes = 0usize;
        let mut image_count = 0usize;
        for entry in context.entries() {
            match entry {
                ProviderContextEntry::Continuation {
                    reference,
                    continuation,
                } => {
                    if !pending.is_empty()
                        || !seen_sources.insert(reference.invocation_id())
                        || reference.validate_destination(invocation).is_err()
                        || !reference.matches(continuation)
                        || reference.format() != "openai.responses.output.v1"
                    {
                        return Err(Error::InvalidReplay);
                    }
                    let replay =
                        ResponsesReplay::from_output_json(continuation.payload(), limits.replay)
                            .map_err(|_| Error::InvalidReplay)?;
                    let items: Vec<Value> = serde_json::from_slice(replay.as_json())
                        .map_err(|_| Error::InvalidReplay)?;
                    for item in items {
                        if item["type"] == "function_call" {
                            let call_id = item["call_id"].as_str().ok_or(Error::InvalidReplay)?;
                            if !seen_calls.insert(call_id.to_owned()) {
                                return Err(Error::InvalidToolHistory);
                            }
                            pending.insert(
                                call_id.to_owned(),
                                ReplayedCall {
                                    invocation_id: reference.invocation_id().clone(),
                                    name: item["name"]
                                        .as_str()
                                        .ok_or(Error::InvalidReplay)?
                                        .to_owned(),
                                    arguments: serde_json::from_str(
                                        item["arguments"].as_str().ok_or(Error::InvalidReplay)?,
                                    )
                                    .map_err(|_| Error::InvalidReplay)?,
                                },
                            );
                        }
                        writer.item(&item)?;
                    }
                }
                ProviderContextEntry::ToolExchange {
                    exchange,
                    stdout_artifact,
                    stderr_artifact,
                } => {
                    let proposal = exchange.request();
                    let call = pending
                        .remove(proposal.provider_call_id())
                        .ok_or(Error::InvalidToolHistory)?;
                    let arguments: Value = serde_json::from_str(proposal.arguments_json())
                        .map_err(|_| Error::InvalidToolHistory)?;
                    if call.invocation_id != *exchange.invocation_id()
                        || call.name != proposal.name()
                        || call.arguments != arguments
                        || exchange.run_id() != context.run_id()
                        || exchange.session_id() != context.session_id()
                    {
                        return Err(Error::InvalidToolHistory);
                    }
                    let mut output: Value = serde_json::from_str(exchange.content_json())
                        .map_err(|_| Error::InvalidToolHistory)?;
                    // Keep state, exit code, provenance, and artifact metadata. A denied
                    // result remains distinct from successful empty stdout/stderr.
                    for (key, attachment) in
                        [("stdout", stdout_artifact), ("stderr", stderr_artifact)]
                    {
                        if let Some(attachment) = attachment {
                            if attachment.artifact().media_type()
                                != kiln_core::TOOL_OUTPUT_MEDIA_TYPE
                            {
                                return Err(Error::UnsupportedAttachment);
                            }
                            output[key] = text(attachment.bytes())?.into();
                        }
                    }
                    let output =
                        serde_json::to_string(&output).map_err(|_| Error::InvalidToolHistory)?;
                    writer.item(&json!({"type": "function_call_output",
                        "call_id": proposal.provider_call_id(), "output": output}))?;
                }
                ProviderContextEntry::Instruction {
                    provenance,
                    content,
                } => {
                    let role = if *provenance == ContextInstructionProvenance::Runtime {
                        "developer"
                    } else {
                        "user"
                    };
                    let content = if role == "developer" {
                        content.clone()
                    } else {
                        json!({"kind": "kiln_instruction", "provenance": provenance.as_str(),
                            "workspace_root_id": provenance.workspace_root_id().map(|id| id.as_str()),
                            "run_id": provenance.run_id().map(|id| id.as_str()), "content": content}).to_string()
                    };
                    writer.item(&json!({"role": role, "content": content}))?;
                }
                ProviderContextEntry::Message {
                    role,
                    content,
                    attachments,
                    ..
                } => {
                    if *role == MessageRole::Assistant && !attachments.is_empty() {
                        return Err(Error::UnsupportedAttachment);
                    }
                    if attachments.is_empty() {
                        writer.item(&json!({"role": role.as_str(), "content": content}))?;
                    } else {
                        writer.begin_item()?;
                        writer.raw(b"{\"role\":\"user\",\"content\":[")?;
                        writer.json(&json!({"type": "input_text", "text": content}))?;
                        for attachment in attachments {
                            append_attachment(
                                &mut writer,
                                attachment,
                                vision,
                                &mut file_bytes,
                                &mut image_count,
                            )?;
                        }
                        writer.raw(b"]}")?;
                    }
                }
                ProviderContextEntry::ChildActivity {
                    reaction_message_id,
                    reference,
                    content,
                } => {
                    writer.item(&json!({"role": "user", "content": json!({
                        "kind": "kiln_child_activity", "reaction_message_id": reaction_message_id.as_str(),
                        "run_id": reference.run_id.as_str(), "event_id": reference.event_id.as_str(),
                        "content": content,
                    }).to_string()}))?;
                }
            }
        }
        if !pending.is_empty() {
            return Err(Error::InvalidToolHistory);
        }
        writer.raw(b"]}")?;
        // Documented public image-input payload ceiling, in decimal MB.
        if image_count != 0 && writer.bytes.len() > 512_000_000 {
            return Err(Error::RequestLimitExceeded);
        }
        Ok(Self { json: writer.bytes })
    }

    pub fn as_json(&self) -> &[u8] {
        &self.json
    }
}

struct ReplayedCall {
    invocation_id: ModelInvocationId,
    name: String,
    arguments: Value,
}

fn text(bytes: &[u8]) -> Result<&str, ResponsesRequestError> {
    std::str::from_utf8(bytes).map_err(|_| ResponsesRequestError::InvalidText)
}

fn append_attachment(
    writer: &mut RequestWriter,
    attachment: &ProviderContextAttachment,
    vision: bool,
    file_bytes: &mut usize,
    image_count: &mut usize,
) -> Result<(), ResponsesRequestError> {
    use ResponsesRequestError as Error;
    let artifact = attachment.artifact();
    let media = artifact.media_type();
    let bytes = attachment.bytes();
    if bytes.len() > writer.remaining() {
        return Err(Error::RequestLimitExceeded);
    }
    let mut metadata = json!({"kind": "kiln_attachment", "content_hash": artifact.content_hash().as_str(),
        "media_type": media, "size": artifact.size()});
    match media {
        "text/plain" | "text/plain; charset=utf-8" | "text/markdown" | "application/json" => {
            metadata["content"] = text(bytes)?.into();
            writer.raw(b",")?;
            writer.json(&json!({"type": "input_text", "text": metadata.to_string()}))?;
        }
        "image/png" | "image/jpeg" | "image/webp" | "application/pdf" => {
            if !vision {
                return Err(Error::VisionUnsupported);
            }
            // Preflight base64 expansion before allocating. The final writer also
            // checks JSON escaping and all surrounding fields against the budget.
            let encoded_len =
                base64::encoded_len(bytes.len(), true).ok_or(Error::RequestLimitExceeded)?;
            if encoded_len > writer.remaining() {
                return Err(Error::RequestLimitExceeded);
            }
            if media == "application/pdf" {
                // Public file-input contract: each file <50 MB, combined <=50 MB.
                const FILE_LIMIT: usize = 50_000_000;
                *file_bytes = file_bytes
                    .checked_add(bytes.len())
                    .ok_or(Error::RequestLimitExceeded)?;
                if bytes.len() >= FILE_LIMIT || *file_bytes > FILE_LIMIT {
                    return Err(Error::RequestLimitExceeded);
                }
            } else {
                // Public image-input contract; PDFs are governed by the file limit.
                if *image_count >= 1_500 {
                    return Err(Error::RequestLimitExceeded);
                }
                *image_count += 1;
            }
            let data = format!("data:{media};base64,{}", STANDARD.encode(bytes));
            writer.raw(b",")?;
            writer.json(&json!({"type": "input_text", "text": metadata.to_string()}))?;
            writer.raw(b",")?;
            writer.json(&if media == "application/pdf" {
                json!({"type": "input_file", "filename": format!("{}.pdf", artifact.content_hash().as_str()),
                    "file_data": data})
            } else { json!({"type": "input_image", "image_url": data, "detail": "auto"}) })?;
        }
        // GIF animation and other formats need an explicit supported contract.
        _ => return Err(Error::UnsupportedAttachment),
    }
    Ok(())
}

struct RequestWriter {
    bytes: Vec<u8>,
    limit: usize,
    items: usize,
    max_items: usize,
}

impl RequestWriter {
    fn remaining(&self) -> usize {
        self.limit - self.bytes.len()
    }
    fn raw(&mut self, bytes: &[u8]) -> Result<(), ResponsesRequestError> {
        self.write_all(bytes)
            .map_err(|_| ResponsesRequestError::RequestLimitExceeded)
    }
    fn json(&mut self, value: &Value) -> Result<(), ResponsesRequestError> {
        serde_json::to_writer(self, value).map_err(|_| ResponsesRequestError::RequestLimitExceeded)
    }
    fn item(&mut self, value: &Value) -> Result<(), ResponsesRequestError> {
        self.begin_item()?;
        self.json(value)
    }
    fn begin_item(&mut self) -> Result<(), ResponsesRequestError> {
        if self.items >= self.max_items {
            return Err(ResponsesRequestError::ItemLimitExceeded);
        }
        if self.items != 0 {
            self.raw(b",")?;
        }
        self.items += 1;
        Ok(())
    }
}

impl Write for RequestWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.remaining() {
            return Err(std::io::Error::other("request byte limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
