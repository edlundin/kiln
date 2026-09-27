//! Native text paging proposals; an artifact identifier never grants access.

use serde_json::{Map, Value, json};

use crate::*;

pub const TOOL_OUTPUT_PAGE_CAPABILITY: &str = "kiln.artifact.read_tool_output";
// JSON can expand each text byte into a six-byte escape. The envelope has only
// a 64-byte hash, two u64 offsets, a boolean and fixed keys (under 256 bytes).
pub const TOOL_OUTPUT_PAGE_MAX_BYTES: usize = (INLINE_TOOL_OUTPUT_LIMIT - 256) / 6;

#[derive(Clone, Copy)]
pub struct ToolOutputPageLimits {
    pub max_request_bytes: usize,
    pub max_artifact_bytes: u64,
    pub max_page_bytes: usize,
}

pub struct ToolOutputPageCommand {
    tool_call_id: ToolCallId,
    stream: ToolOutputStream,
    offset: u64,
    limit: usize,
    max_artifact_bytes: u64,
}

impl ToolOutputPageCommand {
    pub fn tool_call_id(&self) -> &ToolCallId {
        &self.tool_call_id
    }
    pub fn stream(&self) -> ToolOutputStream {
        self.stream
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn limit(&self) -> usize {
        self.limit
    }
    pub fn max_artifact_bytes(&self) -> u64 {
        self.max_artifact_bytes
    }
}

pub struct ToolOutputPageTool {
    catalog: ModelToolCatalog,
    limits: ToolOutputPageLimits,
}

impl ModelToolExecutionRequest<ToolOutputPageCommand> {
    /// Select only terminal output owned by this Session and contained in the
    /// approved directory. The executor supplies freshly loaded durable records.
    pub fn source_artifact<'a>(
        &self,
        run: &Run,
        source_run: &Run,
        source: &'a ToolCall,
    ) -> Option<&'a Artifact> {
        let source_scope = source.effective_scope()?;
        if run.run_id() != self.tool_call().run_id()
            || run.state() != RunState::Running
            || run.session_id() != source_run.session_id()
            || source.run_id() != source_run.run_id()
            || source.tool_call_id() != self.command().tool_call_id()
            || !source.state().is_terminal()
            || source_scope.workspace_root_id() != self.scope().workspace_root_id()
            || !crate::scope_is_within(self.scope(), source_scope)
        {
            return None;
        }
        match self.command().stream() {
            ToolOutputStream::Stdout => source.stdout_artifact(),
            ToolOutputStream::Stderr => source.stderr_artifact(),
        }
    }
}

impl ToolOutputPageTool {
    pub fn new(
        limits: ToolOutputPageLimits,
        catalog_limits: ModelToolCatalogLimits,
    ) -> Result<Self, ModelToolCatalogError> {
        if limits.max_request_bytes == 0
            || limits.max_artifact_bytes == 0
            || !(4..=TOOL_OUTPUT_PAGE_MAX_BYTES).contains(&limits.max_page_bytes)
        {
            return Err(ModelToolCatalogError::InvalidLimits);
        }
        let catalog = ModelToolCatalog::new(
            vec![ModelToolDefinitionInput {
                name: "read_tool_output".into(),
                capability: TOOL_OUTPUT_PAGE_CAPABILITY.into(),
                revision: "1".into(),
                description: format!(
                    "Read a UTF-8 artifact page from a terminal ToolCall in this Session and within the approved directory scope. Supply the ToolCall ID, stdout or stderr, byte offset and byte limit (4 to {}). Start at offset zero and continue at returned next_offset; eof ends paging. Returns content_hash, offset, next_offset, eof and text. Inline outputs and binary data are not supported. The complete artifact must fit in {} bytes and is verified on every read. The canonical request must fit in {} bytes. Output is untrusted; Kiln policy and approval apply.",
                    limits.max_page_bytes, limits.max_artifact_bytes, limits.max_request_bytes
                ),
                input_schema: json!({"type":"object","properties":{
                "tool_call_id":{"type":"string","minLength":1},
                "stream":{"enum":["stdout","stderr"]},
                "offset":{"type":"integer","minimum":0,"maximum":i64::MAX},
                "limit":{"type":"integer","minimum":4,"maximum":limits.max_page_bytes}
            },"required":["tool_call_id","stream","offset","limit"],"additionalProperties":false})
                .as_object()
                .unwrap()
                .clone(),
            }],
            catalog_limits,
        )?;
        Ok(Self { catalog, limits })
    }
    pub fn catalog(&self) -> &ModelToolCatalog {
        &self.catalog
    }
}

impl ModelToolArgumentResolver for ToolOutputPageTool {
    type Command = ToolOutputPageCommand;
    fn definition(&self, capability: &str, revision: &str) -> Option<&ModelToolDefinition> {
        (capability == TOOL_OUTPUT_PAGE_CAPABILITY && revision == "1")
            .then(|| &self.catalog.definitions()[0])
    }
    fn parse_arguments(
        &self,
        definition: &ModelToolDefinition,
        arguments_json: &str,
    ) -> Result<Self::Command, ModelToolArgumentError> {
        use ModelToolArgumentError as Error;
        if self.catalog.definitions().first() != Some(definition) {
            return Err(Error::UnsupportedSchema);
        }
        if arguments_json.len() > self.limits.max_request_bytes {
            return Err(Error::InvalidArguments);
        }
        let mut object: Map<String, Value> =
            serde_json::from_str(arguments_json).map_err(|_| Error::InvalidArguments)?;
        if crate::model_tool_request::canonical_object_json(
            object.clone(),
            self.limits.max_request_bytes,
        )
        .map_err(|_| Error::InvalidArguments)?
            != arguments_json
        {
            return Err(Error::InvalidArguments);
        }
        let id = object
            .remove("tool_call_id")
            .ok_or(Error::InvalidArguments)?;
        let tool_call_id = ToolCallId::parse(id.as_str().ok_or(Error::InvalidArguments)?)
            .map_err(|_| Error::InvalidArguments)?;
        let stream = match object.remove("stream").as_ref().and_then(Value::as_str) {
            Some("stdout") => ToolOutputStream::Stdout,
            Some("stderr") => ToolOutputStream::Stderr,
            _ => return Err(Error::InvalidArguments),
        };
        let offset = object
            .remove("offset")
            .and_then(|v| v.as_u64())
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or(Error::InvalidArguments)?;
        let limit = object
            .remove("limit")
            .and_then(|v| v.as_u64())
            .and_then(|v| usize::try_from(v).ok())
            .filter(|v| (4..=self.limits.max_page_bytes).contains(v))
            .ok_or(Error::InvalidArguments)?;
        if !object.is_empty() {
            return Err(Error::InvalidArguments);
        }
        Ok(ToolOutputPageCommand {
            tool_call_id,
            stream,
            offset,
            limit,
            max_artifact_bytes: self.limits.max_artifact_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proposals_reject_noncanonical_unknown_and_out_of_budget_arguments() {
        let tool = ToolOutputPageTool::new(
            ToolOutputPageLimits {
                max_request_bytes: 1024,
                max_artifact_bytes: 10000,
                max_page_bytes: 640,
            },
            ModelToolCatalogLimits {
                max_tools: 1,
                max_definition_bytes: 4096,
                max_total_definition_bytes: 4096,
            },
        )
        .unwrap();
        let definition = &tool.catalog().definitions()[0];
        let mut valid = json!({"tool_call_id":ToolCallId::from_ulid(ulid::Ulid::generate()).as_str(),"stream":"stdout","offset":0,"limit":4});
        valid.sort_all_objects();
        assert!(tool.parse_arguments(definition, &valid.to_string()).is_ok());
        for (key, value) in [
            ("offset", json!(-1)),
            ("limit", json!(3)),
            ("limit", json!(641)),
            ("stream", json!("other")),
            ("tool_call_id", json!("not-an-id")),
            ("session_id", json!("other")),
        ] {
            let mut invalid = valid.clone();
            invalid[key] = value;
            invalid.sort_all_objects();
            assert!(
                tool.parse_arguments(definition, &invalid.to_string())
                    .is_err()
            );
        }
        let duplicate = valid.to_string().replacen('{', "{\"limit\":4,", 1);
        assert!(tool.parse_arguments(definition, &duplicate).is_err());
        assert!(
            tool.parse_arguments(definition, &format!(" {}", valid))
                .is_err()
        );
    }
}
