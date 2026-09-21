use std::{env, time::Duration};

use kiln_core::{
    CapabilitySupport, GenerationSettings, ModelCapabilitySnapshot, ModelContinuationLimits,
    ModelId, ModelInvocationSettings, ModelToolRequestLimits, ProviderAccountId,
    ProviderContextLimits, ProviderType, ReasoningSettings,
};
use kiln_providers::{
    DETERMINISTIC_MODEL_ID, DETERMINISTIC_PROVIDER_ACCOUNT_ID, DETERMINISTIC_PROVIDER_TYPE,
    OPENAI_API_PROVIDER_TYPE, OpenAiApiTransportLimits, ResponsesCompletionLimits,
    ResponsesReplayLimits, ResponsesRequestLimits, ResponsesSseLimits, ResponsesStreamLimits,
};
use serde_json::{Map, Value};

/// Immutable daemon selection, copied into every invocation in the Run. This
/// declares model capabilities; it does not grant authority to execute tools.
#[derive(Clone)]
pub(crate) struct NativeModelSelection {
    pub account_id: ProviderAccountId,
    pub settings: ModelInvocationSettings,
    pub capabilities: ModelCapabilitySnapshot,
    pub instruction: &'static str,
}

impl NativeModelSelection {
    pub fn deterministic() -> Result<Self, String> {
        Ok(Self {
            account_id: ProviderAccountId::parse(DETERMINISTIC_PROVIDER_ACCOUNT_ID)
                .map_err(|_| "invalid deterministic account")?,
            settings: ModelInvocationSettings::new(
                ProviderType::parse(DETERMINISTIC_PROVIDER_TYPE).map_err(|_| "invalid provider")?,
                ModelId::parse(DETERMINISTIC_MODEL_ID).map_err(|_| "invalid model")?,
                GenerationSettings::new(None).map_err(|_| "invalid generation settings")?,
                ReasoningSettings::new(None).map_err(|_| "invalid reasoning settings")?,
            ),
            capabilities: ModelCapabilitySnapshot::new(
                "deterministic-v1",
                CapabilitySupport::Unsupported,
                CapabilitySupport::Unsupported,
                CapabilitySupport::Unsupported,
            )
            .map_err(|_| "invalid capabilities")?,
            instruction: "Execute the deterministic native model fixture.",
        })
    }
}

pub(crate) struct OpenAiApiConfig {
    pub selection: NativeModelSelection,
    pub context: ProviderContextLimits,
    pub transport: OpenAiApiTransportLimits,
}

impl OpenAiApiConfig {
    /// Operator-owned configuration only. No credentials, endpoint overrides,
    /// implicit model/capability choices or implicit resource budgets.
    pub fn from_environment() -> Result<Self, String> {
        let path = env::var_os("KILN_OPENAI_API_CONFIG")
            .ok_or("openai-api requires KILN_OPENAI_API_CONFIG")?;
        let bytes = std::fs::read(path).map_err(|_| "cannot read KILN_OPENAI_API_CONFIG")?;
        let value = serde_json::from_slice(&bytes).map_err(|_| "invalid native model JSON")?;
        let mut root = Fields::new(value)?;
        let account_id = ProviderAccountId::parse(root.string("account_id")?)
            .map_err(|_| "invalid account_id")?;
        let model = ModelId::parse(root.string("model")?).map_err(|_| "invalid model")?;
        let max_output_tokens = u32::try_from(root.positive("max_output_tokens")?)
            .map_err(|_| "max_output_tokens overflows")?;
        if max_output_tokens < 16 {
            return Err("max_output_tokens must be at least 16".into());
        }
        let effort = match root.take("reasoning_effort")? {
            Value::Null => None,
            Value::String(value)
                if matches!(
                    value.as_str(),
                    "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
                ) =>
            {
                Some(value)
            }
            _ => return Err("invalid reasoning_effort".into()),
        };
        let mut capabilities = root.object("capabilities")?;
        let capabilities_value = ModelCapabilitySnapshot::new(
            capabilities.string("version")?,
            capabilities.support("tool_calls")?,
            capabilities.support("vision")?,
            capabilities.support("structured_output")?,
        )
        .map_err(|_| "invalid capabilities version")?;
        capabilities.finish()?;
        let selection = NativeModelSelection {
            account_id,
            settings: ModelInvocationSettings::new(
                ProviderType::parse(OPENAI_API_PROVIDER_TYPE).map_err(|_| "invalid provider")?,
                model,
                GenerationSettings::new(Some(max_output_tokens))
                    .map_err(|_| "invalid max_output_tokens")?,
                ReasoningSettings::new(effort).map_err(|_| "invalid reasoning_effort")?,
            ),
            capabilities: capabilities_value,
            instruction: "You are the assistant for this Kiln Run.",
        };
        let mut context = root.object("context")?;
        let context_value = ProviderContextLimits {
            max_text_bytes: context.positive("max_text_bytes")?,
            max_attachment_bytes: context.positive("max_attachment_bytes")?,
            max_total_attachment_bytes: context.positive("max_total_attachment_bytes")?,
            max_continuation_bytes: context.positive("max_continuation_bytes")?,
            max_total_continuation_bytes: context.positive("max_total_continuation_bytes")?,
        };
        context.finish()?;
        let mut transport = root.object("transport")?;
        let connect_timeout = Duration::from_millis(transport.positive("connect_timeout_ms")?);
        let request_timeout = Duration::from_millis(transport.positive("request_timeout_ms")?);
        let mut request = transport.object("request")?;
        let request_value = ResponsesRequestLimits {
            max_request_bytes: request.size("max_request_bytes")?,
            max_input_items: request.size("max_input_items")?,
            replay: replay(request.object("replay")?)?,
        };
        request.finish()?;
        let mut stream = transport.object("stream")?;
        let max_retained_bytes = stream.size("max_retained_bytes")?;
        let mut framing = stream.object("framing")?;
        let framing_value = ResponsesSseLimits {
            max_frame_bytes: framing.size("max_frame_bytes")?,
            max_stream_bytes: framing.size("max_stream_bytes")?,
            max_events: framing.size("max_events")?,
        };
        framing.finish()?;
        let mut completion = stream.object("completion")?;
        let max_response_bytes = completion.size("max_response_bytes")?;
        let max_identifier_bytes = completion.size("max_identifier_bytes")?;
        let max_visible_output_bytes = completion.size("max_visible_output_bytes")?;
        let completion_replay = replay(completion.object("replay")?)?;
        let mut requests = completion.object("requests")?;
        let requests_value = ModelToolRequestLimits {
            max_requests: requests.size("max_requests")?,
            max_provider_call_id_bytes: requests.size("max_provider_call_id_bytes")?,
            max_name_bytes: requests.size("max_name_bytes")?,
            max_arguments_bytes: requests.size("max_arguments_bytes")?,
            max_total_arguments_bytes: requests.size("max_total_arguments_bytes")?,
        };
        requests.finish()?;
        let mut continuation = completion.object("continuation")?;
        let continuation_value = ModelContinuationLimits {
            max_format_bytes: continuation.size("max_format_bytes")?,
            max_payload_bytes: continuation.size("max_payload_bytes")?,
        };
        continuation.finish()?;
        completion.finish()?;
        stream.finish()?;
        transport.finish()?;
        root.finish()?;
        Ok(Self {
            selection,
            context: context_value,
            transport: OpenAiApiTransportLimits {
                connect_timeout,
                request_timeout,
                request: request_value,
                stream: ResponsesStreamLimits {
                    max_retained_bytes,
                    framing: framing_value,
                    completion: ResponsesCompletionLimits {
                        max_response_bytes,
                        max_identifier_bytes,
                        max_visible_output_bytes,
                        replay: completion_replay,
                        requests: requests_value,
                        continuation: continuation_value,
                    },
                },
            },
        })
    }
}

fn replay(mut fields: Fields) -> Result<ResponsesReplayLimits, String> {
    let value = ResponsesReplayLimits {
        max_output_bytes: fields.size("max_output_bytes")?,
        max_items: fields.size("max_items")?,
        max_item_bytes: fields.size("max_item_bytes")?,
    };
    fields.finish()?;
    Ok(value)
}

// Errors include only maintained field names, never supplied values or payloads.
struct Fields(Map<String, Value>);
impl Fields {
    fn new(value: Value) -> Result<Self, String> {
        match value {
            Value::Object(fields) => Ok(Self(fields)),
            _ => Err("expected configuration object".into()),
        }
    }
    fn take(&mut self, name: &'static str) -> Result<Value, String> {
        self.0
            .remove(name)
            .ok_or_else(|| format!("missing configuration field: {name}"))
    }
    fn object(&mut self, name: &'static str) -> Result<Self, String> {
        Self::new(self.take(name)?)
    }
    fn string(&mut self, name: &'static str) -> Result<String, String> {
        match self.take(name)? {
            Value::String(value) => Ok(value),
            _ => Err(format!("expected string: {name}")),
        }
    }
    fn positive(&mut self, name: &'static str) -> Result<u64, String> {
        self.take(name)?
            .as_u64()
            .filter(|value| *value > 0)
            .ok_or_else(|| format!("expected positive integer: {name}"))
    }
    fn size(&mut self, name: &'static str) -> Result<usize, String> {
        usize::try_from(self.positive(name)?)
            .map_err(|_| format!("configuration field overflows: {name}"))
    }
    fn support(&mut self, name: &'static str) -> Result<CapabilitySupport, String> {
        CapabilitySupport::parse(&self.string(name)?)
            .map_err(|_| format!("invalid capability: {name}"))
    }
    fn finish(self) -> Result<(), String> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err("unknown configuration field".into())
        }
    }
}
