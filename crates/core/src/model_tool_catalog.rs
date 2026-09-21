use std::{collections::HashSet, fmt, future::Future};

use serde_json::{Map, Value};

use crate::{
    CapabilitySupport, ModelInvocation, ModelInvocationId, ModelInvocationPurpose,
    ModelInvocationStoreError, ModelToolRequestError,
    model_tool_request::{canonical_object_json, valid_identifier},
};

/// Supplied by Kiln's tool registry, never by a provider response. The revision
/// identifies the local implementation and argument-validation contract.
pub struct ModelToolDefinitionInput {
    pub name: String,
    pub description: String,
    pub capability: String,
    pub revision: String,
    pub input_schema: Map<String, Value>,
}

#[derive(Debug, Clone, Copy)]
pub struct ModelToolCatalogLimits {
    pub max_tools: usize,
    pub max_definition_bytes: usize,
    pub max_total_definition_bytes: usize,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ModelToolDefinition {
    name: String,
    description: String,
    capability: String,
    revision: String,
    input_schema_json: String,
    definition_json: String,
}

impl ModelToolDefinition {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn description(&self) -> &str {
        &self.description
    }
    pub fn capability(&self) -> &str {
        &self.capability
    }
    pub fn revision(&self) -> &str {
        &self.revision
    }
    pub fn input_schema_json(&self) -> &str {
        &self.input_schema_json
    }
    pub fn definition_json(&self) -> &str {
        &self.definition_json
    }
}

/// Ordered immutable descriptions, not executable capabilities or grants.
/// Schema documents are retained verbatim in canonical JSON; the registered
/// implementation must validate schema semantics and arguments before adoption.
#[derive(Clone, PartialEq, Eq)]
pub struct ModelToolCatalog {
    definitions: Vec<ModelToolDefinition>,
}

impl fmt::Debug for ModelToolCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelToolCatalog")
            .field("tool_count", &self.definitions.len())
            .finish_non_exhaustive()
    }
}

impl ModelToolCatalog {
    pub fn empty() -> Self {
        Self {
            definitions: Vec::new(),
        }
    }

    pub fn new(
        inputs: Vec<ModelToolDefinitionInput>,
        limits: ModelToolCatalogLimits,
    ) -> Result<Self, ModelToolCatalogError> {
        if limits.max_tools == 0
            || limits.max_definition_bytes == 0
            || limits.max_total_definition_bytes == 0
        {
            return Err(ModelToolCatalogError::InvalidLimits);
        }
        if inputs.len() > limits.max_tools {
            return Err(ModelToolCatalogError::LimitExceeded);
        }
        let mut names = HashSet::new();
        let mut definitions = Vec::with_capacity(inputs.len());
        let mut remaining = limits.max_total_definition_bytes;
        for input in inputs {
            if input.description.len() > remaining.min(limits.max_definition_bytes) {
                return Err(ModelToolCatalogError::LimitExceeded);
            }
            if !valid_identifier(&input.name, limits.max_definition_bytes)
                || !valid_identifier(&input.capability, limits.max_definition_bytes)
                || !valid_identifier(&input.revision, limits.max_definition_bytes)
                || input.description.trim().is_empty()
                || input.input_schema.get("type").and_then(Value::as_str) != Some("object")
            {
                return Err(ModelToolCatalogError::InvalidDefinition);
            }
            if !names.insert(input.name.clone()) {
                return Err(ModelToolCatalogError::DuplicateName);
            }
            let limit = remaining.min(limits.max_definition_bytes);
            let input_schema_json =
                canonical_object_json(input.input_schema, limit).map_err(map_json_error)?;
            let mut object = Map::new();
            object.insert("name".into(), Value::String(input.name.clone()));
            object.insert(
                "description".into(),
                Value::String(input.description.clone()),
            );
            object.insert("capability".into(), Value::String(input.capability.clone()));
            object.insert("revision".into(), Value::String(input.revision.clone()));
            object.insert(
                "input_schema".into(),
                serde_json::from_str(&input_schema_json)
                    .map_err(|_| ModelToolCatalogError::InvalidDefinition)?,
            );
            let definition_json = canonical_object_json(object, limit).map_err(map_json_error)?;
            remaining -= definition_json.len();
            definitions.push(ModelToolDefinition {
                name: input.name,
                description: input.description,
                capability: input.capability,
                revision: input.revision,
                input_schema_json,
                definition_json,
            });
        }
        Ok(Self { definitions })
    }

    pub fn definitions(&self) -> &[ModelToolDefinition] {
        &self.definitions
    }

    pub fn find(&self, name: &str) -> Option<&ModelToolDefinition> {
        self.definitions
            .iter()
            .find(|definition| definition.name == name)
    }

    pub fn validate_for(&self, invocation: &ModelInvocation) -> Result<(), ModelToolCatalogError> {
        if !self.definitions.is_empty()
            && (invocation.purpose() != ModelInvocationPurpose::Generation
                || invocation.capabilities().tool_calls() != CapabilitySupport::Supported)
        {
            return Err(ModelToolCatalogError::InvalidInvocation);
        }
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        crate::push_context_field(&mut bytes, b"kiln.model-tool-catalog.v1");
        crate::push_context_field(&mut bytes, &(self.definitions.len() as u64).to_be_bytes());
        for definition in &self.definitions {
            crate::push_context_field(&mut bytes, definition.definition_json.as_bytes());
        }
        bytes
    }
}

fn map_json_error(error: ModelToolRequestError) -> ModelToolCatalogError {
    match error {
        ModelToolRequestError::ArgumentsLimitExceeded => ModelToolCatalogError::LimitExceeded,
        _ => ModelToolCatalogError::InvalidDefinition,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelToolCatalogError {
    InvalidLimits,
    LimitExceeded,
    InvalidDefinition,
    DuplicateName,
    InvalidInvocation,
    IdempotencyConflict,
    RetryMismatch,
    IntegrityViolation,
    Unavailable,
    Invocation(ModelInvocationStoreError),
}

pub trait ModelToolCatalogStore: Send + Sync {
    /// Attach once while pending. Matching retries return the immutable snapshot.
    fn attach_model_tool_catalog(
        &self,
        invocation_id: &ModelInvocationId,
        catalog: &ModelToolCatalog,
    ) -> impl Future<Output = Result<ModelToolCatalog, ModelToolCatalogError>> + Send;

    fn get_model_tool_catalog(
        &self,
        invocation_id: &ModelInvocationId,
    ) -> impl Future<Output = Result<Option<ModelToolCatalog>, ModelToolCatalogError>> + Send;
}
