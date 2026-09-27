//! Host-local launch snapshots. Neither metadata nor references grant execution.

use crate::{
    KilnInstanceId, McpDefinitionLimits, McpInstanceKey, SecretRef, SharedConfigurationKey,
};
use std::collections::BTreeMap;

/// Host-local registered checkout selection. Decoding this metadata grants no
/// authority; publication checks registration and launch must recheck approval
/// and pin the directory against its recorded filesystem identity.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpHostWorkingDirectory {
    pub workspace_id: String,
    pub workspace_root_id: String,
    pub relative_directory: String,
    pub root_path: String,
    pub git_common_directory_path: String,
    pub filesystem_identity: String,
}

impl From<&crate::WorkspaceCheckout> for McpHostWorkingDirectory {
    fn from(checkout: &crate::WorkspaceCheckout) -> Self {
        Self {
            workspace_id: checkout.workspace_id().as_str().into(),
            workspace_root_id: checkout.workspace_root_id().as_str().into(),
            relative_directory: checkout.relative_directory().into(),
            root_path: checkout.root_path().into(),
            git_common_directory_path: checkout.git_common_directory_path().into(),
            filesystem_identity: checkout.filesystem_identity().as_str().into(),
        }
    }
}

impl McpHostWorkingDirectory {
    fn into_checkout(self) -> Result<crate::WorkspaceCheckout, McpHostBindingError> {
        let invalid = McpHostBindingError::InvalidRequest;
        if !self.root_path.starts_with('/')
            || !self.git_common_directory_path.starts_with('/')
            || self.root_path.contains('\0')
            || self.git_common_directory_path.contains('\0')
        {
            return Err(invalid);
        }
        crate::WorkspaceCheckout::from_resolved_paths(
            crate::WorkspaceId::parse(self.workspace_id).map_err(|_| invalid)?,
            crate::WorkspaceRootId::parse(self.workspace_root_id).map_err(|_| invalid)?,
            self.relative_directory,
            self.root_path,
            self.git_common_directory_path,
            crate::FilesystemIdentity::new(self.filesystem_identity).ok_or(invalid)?,
        )
        .map_err(|_| invalid)
    }
}

/// Persisted executable paths are UTF-8 absolute Unix paths. Native callers with
/// other path encodings must use an explicitly authorized materialized launch.
/// No Debug: paths and binding identities can be private host information.
pub struct McpHostBindingInput {
    pub instance_id: KilnInstanceId,
    pub definition_version: u64,
    pub runtime_binding: SharedConfigurationKey,
    pub executable: String,
    pub working_directory: Option<McpHostWorkingDirectory>,
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
    working_directory: Option<crate::WorkspaceCheckout>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    working_directory: Option<McpHostWorkingDirectory>,
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
        let working_directory = input
            .working_directory
            .map(McpHostWorkingDirectory::into_checkout)
            .transpose()?;
        if let Some(directory) = &working_directory {
            let matches_owner = match key.owner() {
                crate::McpInstanceOwner::WorkspaceCheckout(owner) => owner == directory,
                crate::McpInstanceOwner::Workspace(id) => id == directory.workspace_id(),
                // Session membership is checked against the store at publication.
                crate::McpInstanceOwner::Session(_) | crate::McpInstanceOwner::Core => true,
            };
            if !matches_owner {
                return Err(Error::InvalidBinding);
            }
        }
        let metadata = Metadata {
            instance_id: input.instance_id.as_str().into(),
            definition_version: input.definition_version,
            runtime_binding: input.runtime_binding.as_str().into(),
            executable: input.executable.clone(),
            working_directory: working_directory.as_ref().map(Into::into),
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
            working_directory,
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
                working_directory: raw.working_directory,
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
    pub fn working_directory(&self) -> Option<&crate::WorkspaceCheckout> {
        self.working_directory.as_ref()
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
    /// Retained removal receipt, never a usable launch snapshot.
    pub retired: bool,
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
    NotFound,
    IntegrityViolation,
    Unavailable,
}

pub trait McpHostBindingStore: Send + Sync {
    /// Read-only publication preflight before vault access. Some is an exact
    /// existing receipt; None means the metadata is currently admissible, not a
    /// permission or lease. Publication must revalidate after external work.
    fn inspect_mcp_host_binding_publication(
        &self,
        bindings: &McpHostBindings,
        expected_revision: u64,
        limits: McpDefinitionLimits,
    ) -> impl Future<Output = Result<Option<McpHostBindingRecord>, McpHostBindingError>> + Send;
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
    /// Retains a new revision tombstone and retires all published references
    /// atomically. Active/uncertain generations must be cleaned up first. This
    /// remains possible after definition disablement or owner removal. An exact
    /// retry returns the original receipt, even after explicit republication.
    fn retire_mcp_host_bindings(
        &self,
        key: &McpInstanceKey,
        expected_revision: std::num::NonZeroU64,
        limits: McpDefinitionLimits,
    ) -> impl Future<Output = Result<McpHostBindingRecord, McpHostBindingError>> + Send;
}
