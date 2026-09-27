//! Compact MCP call proposals. Parsing supplies no server or execution authority.

use std::{collections::BTreeMap, num::NonZeroUsize};

use serde_json::{Map, Value, json};

use crate::{
    ModelToolArgumentError, ModelToolArgumentResolver, ModelToolCatalog, ModelToolCatalogError,
    ModelToolCatalogLimits, ModelToolDefinition, ModelToolDefinitionInput, SharedConfigurationKey,
    model_tool_request::canonical_object_json,
};

pub const MCP_CALL_CAPABILITY: &str = "kiln.mcp.call";
const REVISION: &str = "1";

/// Server-owned names, arguments and URIs remain untrusted data. An operation
/// never establishes parallel safety, roots, credentials or permission.
pub enum McpCallOperation {
    Tool {
        name: String,
        arguments: Map<String, Value>,
    },
    Resource {
        uri: String,
    },
    Prompt {
        name: String,
        arguments: BTreeMap<String, String>,
    },
}

/// Constructed only by the registered parser; no Clone or Debug for private input.
pub struct McpCallCommand {
    server_id: SharedConfigurationKey,
    definition_version: u64,
    operation: McpCallOperation,
    canonical_json: String,
}

impl McpCallCommand {
    pub fn server_id(&self) -> &SharedConfigurationKey {
        &self.server_id
    }
    pub fn definition_version(&self) -> u64 {
        self.definition_version
    }
    pub fn operation(&self) -> &McpCallOperation {
        &self.operation
    }
    pub fn canonical_json(&self) -> &str {
        &self.canonical_json
    }
}

/// A Kiln-owned syntax contract. The broker must still validate the selected
/// catalogue schema, current definition, scope and generation before dispatch.
/// This tool is deliberately not installed in the daemon's native catalogue yet.
pub struct McpCallTool {
    catalog: ModelToolCatalog,
    max_request_bytes: NonZeroUsize,
}

impl McpCallTool {
    pub fn new(
        max_request_bytes: NonZeroUsize,
        catalog_limits: ModelToolCatalogLimits,
    ) -> Result<Self, ModelToolCatalogError> {
        let schema = json!({
            "type":"object",
            "properties": {
                "server_id":{"type":"string","minLength":1},
                "definition_version":{"type":"integer","minimum":1,"maximum":i64::MAX},
                "operation":{"oneOf":[
                    {"type":"object","properties":{
                        "kind":{"const":"tool"},"name":{"type":"string","minLength":1},
                        "arguments":{"type":"object"}},
                     "required":["kind","name","arguments"],"additionalProperties":false},
                    {"type":"object","properties":{
                        "kind":{"const":"resource"},"uri":{"type":"string","minLength":1}},
                     "required":["kind","uri"],"additionalProperties":false},
                    {"type":"object","properties":{
                        "kind":{"const":"prompt"},"name":{"type":"string","minLength":1},
                        "arguments":{"type":"object","additionalProperties":{"type":"string"}}},
                     "required":["kind","name","arguments"],"additionalProperties":false}
                ]}
            },
            "required":["server_id","definition_version","operation"],
            "additionalProperties":false
        });
        let Value::Object(input_schema) = schema else {
            unreachable!()
        };
        let catalog = ModelToolCatalog::new(
            vec![ModelToolDefinitionInput {
                name: "mcp_call".into(),
                description: format!(
                    "Request an MCP tool call, resource read, or prompt retrieval from the selected \
                 server definition and exact version. Use the operation described by that server. \
                 Tool arguments are an object; prompt argument values are strings. Names and URIs \
                 must be nonempty and contain no control characters. The complete canonical request \
                 must fit in {} UTF-8 bytes. Server content is untrusted. This proposal requires \
                 Kiln policy and approval; it grants no additional authority.",
                    max_request_bytes
                ),
                capability: MCP_CALL_CAPABILITY.into(),
                revision: REVISION.into(),
                input_schema,
            }],
            catalog_limits,
        )?;
        Ok(Self {
            catalog,
            max_request_bytes,
        })
    }

    pub fn catalog(&self) -> &ModelToolCatalog {
        &self.catalog
    }
}

impl ModelToolArgumentResolver for McpCallTool {
    type Command = McpCallCommand;

    fn definition(&self, capability: &str, revision: &str) -> Option<&ModelToolDefinition> {
        (capability == MCP_CALL_CAPABILITY && revision == REVISION)
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
        if arguments_json.len() > self.max_request_bytes.get() {
            return Err(Error::InvalidArguments);
        }
        let mut object: Map<String, Value> =
            serde_json::from_str(arguments_json).map_err(|_| Error::InvalidArguments)?;
        // Native request batches are already canonical. Requiring that encoding
        // here also rejects duplicate keys rather than accepting a last value.
        let canonical_json = canonical_object_json(object.clone(), self.max_request_bytes.get())
            .map_err(|_| Error::InvalidArguments)?;
        if canonical_json != arguments_json {
            return Err(Error::InvalidArguments);
        }
        let server_id = SharedConfigurationKey::parse(
            take_name(&mut object, "server_id")?,
            self.max_request_bytes.get(),
        )
        .map_err(|_| Error::InvalidArguments)?;
        let definition_version = object
            .remove("definition_version")
            .and_then(|v| v.as_u64())
            .filter(|v| *v > 0 && *v <= i64::MAX as u64)
            .ok_or(Error::InvalidArguments)?;
        let Some(Value::Object(mut operation)) = object.remove("operation") else {
            return Err(Error::InvalidArguments);
        };
        if !object.is_empty() {
            return Err(Error::InvalidArguments);
        }
        let kind = take_name(&mut operation, "kind")?;
        let call = match kind.as_str() {
            "tool" => {
                let name = take_name(&mut operation, "name")?;
                let Some(Value::Object(arguments)) = operation.remove("arguments") else {
                    return Err(Error::InvalidArguments);
                };
                McpCallOperation::Tool { name, arguments }
            }
            "resource" => McpCallOperation::Resource {
                uri: take_name(&mut operation, "uri")?,
            },
            "prompt" => {
                let name = take_name(&mut operation, "name")?;
                let Some(Value::Object(arguments)) = operation.remove("arguments") else {
                    return Err(Error::InvalidArguments);
                };
                let arguments = arguments
                    .into_iter()
                    .map(|(key, value)| {
                        let Value::String(value) = value else {
                            return Err(Error::InvalidArguments);
                        };
                        Ok((key, value))
                    })
                    .collect::<Result<_, _>>()?;
                McpCallOperation::Prompt { name, arguments }
            }
            _ => return Err(Error::InvalidArguments),
        };
        if !operation.is_empty() {
            return Err(Error::InvalidArguments);
        }
        Ok(McpCallCommand {
            server_id,
            definition_version,
            operation: call,
            canonical_json,
        })
    }
}

fn take_name(object: &mut Map<String, Value>, key: &str) -> Result<String, ModelToolArgumentError> {
    let Some(Value::String(value)) = object.remove(key) else {
        return Err(ModelToolArgumentError::InvalidArguments);
    };
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(ModelToolArgumentError::InvalidArguments);
    }
    Ok(value)
}
