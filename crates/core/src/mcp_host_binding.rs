//! Host-local launch snapshots. Neither metadata nor references grant execution.

use crate::{
    KilnInstanceId, McpDefinitionLimits, McpInstanceKey, SecretRef, SharedConfigurationKey,
};
use std::collections::BTreeMap;

/// Persisted executable paths are UTF-8 absolute Unix paths. Native callers with
/// other path encodings must use an explicitly authorized materialized launch.
/// No Debug: paths and binding identities can be private host information.
pub struct McpHostBindingInput {
    pub instance_id: KilnInstanceId,
    pub definition_version: u64,
    pub runtime_binding: SharedConfigurationKey,
    pub executable: String,
    pub arguments: BTreeMap<SharedConfigurationKey, SecretRef>,
    pub environment: BTreeMap<SharedConfigurationKey, SecretRef>,
}

#[derive(Clone)]
pub struct McpHostBindings {
    key: McpInstanceKey,
    instance_id: KilnInstanceId,
    definition_version: u64,
    runtime_binding: SharedConfigurationKey,
    executable: String,
    arguments: BTreeMap<SharedConfigurationKey, SecretRef>,
    environment: BTreeMap<SharedConfigurationKey, SecretRef>,
    metadata_json: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    instance_id: String,
    definition_version: u64,
    runtime_binding: String,
    executable: String,
    arguments: BTreeMap<String, String>,
    environment: BTreeMap<String, String>,
}

impl McpHostBindings {
    pub fn new(
        key: McpInstanceKey,
        input: McpHostBindingInput,
        limits: McpDefinitionLimits,
    ) -> Result<Self, McpHostBindingError> {
        use McpHostBindingError as Error;
        limits.validate().map_err(|_| Error::InvalidRequest)?;
        if input.definition_version == 0
            || input.definition_version > i64::MAX as u64
            || !input.executable.starts_with('/')
            || input.executable.contains('\0')
        {
            return Err(Error::InvalidRequest);
        }
        if input.arguments.len() > limits.max_arguments
            || input.environment.len() > limits.max_environment
        {
            return Err(Error::LimitExceeded);
        }
        for name in std::iter::once(&input.runtime_binding)
            .chain(input.arguments.keys())
            .chain(input.environment.keys())
        {
            SharedConfigurationKey::parse(name.as_str(), limits.max_key_bytes)
                .map_err(|_| Error::LimitExceeded)?;
        }
        let refs = |values: &BTreeMap<SharedConfigurationKey, SecretRef>| {
            values
                .iter()
                .map(|(key, value)| (key.as_str().to_owned(), value.as_str().to_owned()))
                .collect()
        };
        let metadata = Metadata {
            instance_id: input.instance_id.as_str().into(),
            definition_version: input.definition_version,
            runtime_binding: input.runtime_binding.as_str().into(),
            executable: input.executable.clone(),
            arguments: refs(&input.arguments),
            environment: refs(&input.environment),
        };
        let serde_json::Value::Object(object) =
            serde_json::to_value(metadata).map_err(|_| Error::InvalidRequest)?
        else {
            unreachable!()
        };
        let budget = limits
            .max_metadata_bytes
            .checked_sub(key.canonical_json().len())
            .ok_or(Error::LimitExceeded)?;
        let metadata_json = crate::model_tool_request::canonical_object_json(object, budget)
            .map_err(|_| Error::LimitExceeded)?;
        Ok(Self {
            key,
            instance_id: input.instance_id,
            definition_version: input.definition_version,
            runtime_binding: input.runtime_binding,
            executable: input.executable,
            arguments: input.arguments,
            environment: input.environment,
            metadata_json,
        })
    }
    pub fn from_metadata_json(
        key: McpInstanceKey,
        bytes: &[u8],
        limits: McpDefinitionLimits,
    ) -> Result<Self, McpHostBindingError> {
        use McpHostBindingError as Error;
        if bytes
            .len()
            .checked_add(key.canonical_json().len())
            .is_none_or(|size| size > limits.max_metadata_bytes)
        {
            return Err(Error::LimitExceeded);
        }
        let raw: Metadata = serde_json::from_slice(bytes).map_err(|_| Error::InvalidRequest)?;
        let refs = |values: BTreeMap<String, String>| {
            values
                .into_iter()
                .map(|(key, value)| {
                    Ok((
                        SharedConfigurationKey::parse(key, limits.max_key_bytes)
                            .map_err(|_| Error::InvalidRequest)?,
                        SecretRef::parse(value).map_err(|_| Error::InvalidRequest)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, Error>>()
        };
        let value = Self::new(
            key,
            McpHostBindingInput {
                instance_id: KilnInstanceId::parse(raw.instance_id)
                    .map_err(|_| Error::InvalidRequest)?,
                definition_version: raw.definition_version,
                runtime_binding: SharedConfigurationKey::parse(
                    raw.runtime_binding,
                    limits.max_key_bytes,
                )
                .map_err(|_| Error::InvalidRequest)?,
                executable: raw.executable,
                arguments: refs(raw.arguments)?,
                environment: refs(raw.environment)?,
            },
            limits,
        )?;
        if value.metadata_json.as_bytes() != bytes {
            return Err(Error::InvalidRequest);
        }
        Ok(value)
    }
    pub fn key(&self) -> &McpInstanceKey {
        &self.key
    }
    pub fn instance_id(&self) -> &KilnInstanceId {
        &self.instance_id
    }
    pub fn definition_version(&self) -> u64 {
        self.definition_version
    }
    pub fn runtime_binding(&self) -> &SharedConfigurationKey {
        &self.runtime_binding
    }
    pub fn executable(&self) -> &str {
        &self.executable
    }
    pub fn arguments(&self) -> &BTreeMap<SharedConfigurationKey, SecretRef> {
        &self.arguments
    }
    pub fn environment(&self) -> &BTreeMap<SharedConfigurationKey, SecretRef> {
        &self.environment
    }
    pub fn metadata_json(&self) -> &str {
        &self.metadata_json
    }
}

#[derive(Clone)]
pub struct McpHostBindingRecord {
    pub bindings: McpHostBindings,
    pub revision: std::num::NonZeroU64,
}

#[derive(Clone, PartialEq, Eq)]
pub struct McpHostBindingVersion {
    pub instance_id: KilnInstanceId,
    pub revision: std::num::NonZeroU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpHostBindingError {
    InvalidRequest,
    LimitExceeded,
    DefinitionChanged,
    InvalidBinding,
    ActiveGeneration,
    Conflict,
    IntegrityViolation,
    Unavailable,
}

pub trait McpHostBindingStore: Send + Sync {
    /// Caller has successfully written each new reserved reference to the MCP
    /// vault and serializes publication with its own writes/cleanup. Publication
    /// retires replaced references atomically. Active or uncertain generations
    /// must be stopped and reaped first. Exact retries return original receipts.
    fn publish_mcp_host_bindings(
        &self,
        bindings: &McpHostBindings,
        expected_revision: u64,
        limits: McpDefinitionLimits,
    ) -> impl Future<Output = Result<McpHostBindingRecord, McpHostBindingError>> + Send;
    fn get_mcp_host_bindings(
        &self,
        key: &McpInstanceKey,
        limits: McpDefinitionLimits,
    ) -> impl Future<Output = Result<Option<McpHostBindingRecord>, McpHostBindingError>> + Send;
}
