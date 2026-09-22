//! Coherent portable configuration metadata. Validation neither resolves local
//! bindings nor grants execution/network authority to synchronized definitions.

use crate::{
    ConfigurationRevision, ContentHash, GlobalSkillId, ModelCapabilitySnapshot,
    ModelInvocationSettings, SharedSkillPackage, model_tool_request::canonical_object_json,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub const SHARED_CONFIGURATION_SCHEMA_VERSION: u32 = 1;

/// Stable portable key for definitions and host bindings. Never a host path,
/// account ID or SecretRef; resolution is local and separate from synchronization.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SharedConfigurationKey(String);
impl SharedConfigurationKey {
    pub fn parse(
        value: impl Into<String>,
        max_bytes: usize,
    ) -> Result<Self, SharedConfigurationError> {
        let value = value.into();
        if max_bytes == 0
            || value.is_empty()
            || value.len() > max_bytes
            || !value.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
            })
        {
            return Err(SharedConfigurationError::InvalidKey);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone)]
pub struct SharedModelDefaults {
    pub account_binding: SharedConfigurationKey,
    pub settings: ModelInvocationSettings,
    pub capabilities: ModelCapabilitySnapshot,
}

/// The initial allowlist contains native model preferences. Additional global
/// setting categories require an explicit schema change and scope decision.
#[derive(Clone, Default)]
pub struct SharedSettings {
    pub model_defaults: Option<SharedModelDefaults>,
}

#[derive(Clone)]
pub enum SharedMcpArgument {
    Literal(String),
    HostBinding(SharedConfigurationKey),
}
#[derive(Clone)]
pub enum SharedMcpTransport {
    Stdio {
        runtime_binding: SharedConfigurationKey,
        arguments: Vec<SharedMcpArgument>,
        environment: BTreeMap<String, SharedConfigurationKey>,
    },
    Https {
        endpoint: String,
        credential_binding: Option<SharedConfigurationKey>,
    },
    /// A host-specific endpoint, e.g. a local process service. Its transport and
    /// credential policy must be validated when that host resolves the binding.
    HostEndpoint {
        endpoint_binding: SharedConfigurationKey,
    },
}
#[derive(Clone)]
pub struct SharedMcpServerInput {
    pub id: SharedConfigurationKey,
    pub enabled: bool,
    pub transport: SharedMcpTransport,
}

#[derive(Debug, Clone, Copy)]
pub struct SharedConfigurationLimits {
    pub max_key_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_mcp_servers: usize,
    pub max_mcp_arguments: usize,
    pub max_mcp_argument_bytes: usize,
    pub max_mcp_environment: usize,
    pub max_endpoint_bytes: usize,
    pub max_skills: usize,
    pub max_total_skill_files: usize,
    pub max_total_skill_bytes: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedConfigurationError {
    InvalidLimits,
    InvalidMetadata,
    InvalidKey,
    InvalidEnvironmentName,
    InvalidArgument,
    InvalidEndpoint,
    DuplicateDefinition,
    MissingSkillDependency,
    DisabledSkillDependency,
    SkillDependencyCycle,
    LimitExceeded,
    RevisionMismatch,
}

pub struct SharedConfigurationInput {
    pub settings: SharedSettings,
    pub mcp_servers: Vec<SharedMcpServerInput>,
    pub skills: Vec<SharedSkillPackage>,
}

/// Immutable, complete validated content. The canonical metadata commits to
/// the full skill package hashes; the matching bytes stay in the package values.
/// This is not evidence of authentication or an applied local revision.
pub struct SharedConfigurationSnapshot {
    settings: SharedSettings,
    mcp_servers: Vec<SharedMcpServerInput>,
    skills: Vec<SharedSkillPackage>,
    metadata_json: String,
    content_hash: ContentHash,
}
impl std::fmt::Debug for SharedConfigurationSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedConfigurationSnapshot")
            .field("mcp_servers", &self.mcp_servers.len())
            .field("skills", &self.skills.len())
            .field("metadata_bytes", &self.metadata_json.len())
            .finish_non_exhaustive()
    }
}
impl SharedConfigurationSnapshot {
    pub fn validate(
        input: SharedConfigurationInput,
        limits: SharedConfigurationLimits,
    ) -> Result<Self, SharedConfigurationError> {
        use SharedConfigurationError as Error;
        limits.validate()?;
        if input.mcp_servers.len() > limits.max_mcp_servers
            || input.skills.len() > limits.max_skills
        {
            return Err(Error::LimitExceeded);
        }
        let settings = settings_json(&input.settings, limits)?;
        let mut servers = BTreeMap::new();
        for mut server in input.mcp_servers {
            validate_key(&server.id, limits)?;
            let metadata = mcp_json(&mut server, limits)?;
            if servers
                .insert(server.id.clone(), (server, metadata))
                .is_some()
            {
                return Err(Error::DuplicateDefinition);
            }
        }
        let mut skills = BTreeMap::new();
        let mut total_files = 0usize;
        let mut total_bytes = 0usize;
        for skill in input.skills {
            if skill.id().as_str().len() > limits.max_key_bytes {
                return Err(Error::LimitExceeded);
            }
            total_files = total_files
                .checked_add(skill.files().len())
                .ok_or(Error::LimitExceeded)?;
            total_bytes = total_bytes
                .checked_add(skill.total_file_bytes())
                .ok_or(Error::LimitExceeded)?;
            if total_files > limits.max_total_skill_files
                || total_bytes > limits.max_total_skill_bytes
            {
                return Err(Error::LimitExceeded);
            }
            if skills.insert(skill.id().clone(), skill).is_some() {
                return Err(Error::DuplicateDefinition);
            }
        }
        validate_skill_dependencies(&skills)?;
        let metadata = json!({
            "schema_version": SHARED_CONFIGURATION_SCHEMA_VERSION,
            "settings": settings,
            "mcp_servers": servers.values().map(|(_, metadata)| metadata.clone()).collect::<Vec<_>>(),
            "skills": skills.values().map(|skill| json!({"id":skill.id().as_str(), "package_hash":skill.content_hash().as_str()})).collect::<Vec<_>>(),
        });
        let Value::Object(metadata) = metadata else {
            unreachable!("metadata is an object")
        };
        let metadata_json = canonical_object_json(metadata, limits.max_metadata_bytes)
            .map_err(|_| Error::LimitExceeded)?;
        let mut digest = Sha256::new();
        digest.update(b"kiln:shared-configuration:v1\0");
        digest.update((metadata_json.len() as u64).to_be_bytes());
        digest.update(metadata_json.as_bytes());
        let mut encoded = String::with_capacity(64);
        for byte in digest.finalize() {
            encoded.push(b"0123456789abcdef"[(byte >> 4) as usize] as char);
            encoded.push(b"0123456789abcdef"[(byte & 0x0f) as usize] as char);
        }
        Ok(Self {
            settings: input.settings,
            mcp_servers: servers.into_values().map(|(server, _)| server).collect(),
            skills: skills.into_values().collect(),
            metadata_json,
            content_hash: ContentHash::parse(encoded).expect("SHA-256 is a valid content hash"),
        })
    }
    pub fn settings(&self) -> &SharedSettings {
        &self.settings
    }
    pub fn mcp_servers(&self) -> &[SharedMcpServerInput] {
        &self.mcp_servers
    }
    pub fn skills(&self) -> &[SharedSkillPackage] {
        &self.skills
    }
    pub fn metadata_json(&self) -> &str {
        &self.metadata_json
    }
    pub fn content_hash(&self) -> &ContentHash {
        &self.content_hash
    }
    pub fn verify_revision(
        &self,
        revision: &ConfigurationRevision,
    ) -> Result<(), SharedConfigurationError> {
        if revision.schema_version() != SHARED_CONFIGURATION_SCHEMA_VERSION
            || revision.content_hash() != &self.content_hash
        {
            return Err(SharedConfigurationError::RevisionMismatch);
        }
        Ok(())
    }
}

fn validate_key(
    key: &SharedConfigurationKey,
    limits: SharedConfigurationLimits,
) -> Result<(), SharedConfigurationError> {
    SharedConfigurationKey::parse(key.as_str(), limits.max_key_bytes).map(|_| ())
}
fn settings_json(
    settings: &SharedSettings,
    limits: SharedConfigurationLimits,
) -> Result<Value, SharedConfigurationError> {
    let model = if let Some(model) = &settings.model_defaults {
        validate_key(&model.account_binding, limits)?;
        for value in [
            model.settings.provider().as_str(),
            model.settings.model().as_str(),
            model.settings.reasoning().effort().unwrap_or_default(),
            model.capabilities.version(),
        ] {
            if value.len() > limits.max_metadata_bytes {
                return Err(SharedConfigurationError::LimitExceeded);
            }
        }
        json!({
            "account_binding":model.account_binding.as_str(),
            "provider":model.settings.provider().as_str(), "model":model.settings.model().as_str(),
            "max_output_tokens":model.settings.generation().max_output_tokens(),
            "reasoning_effort":model.settings.reasoning().effort(),
            "capabilities": {"version":model.capabilities.version(),
                "tool_calls":model.capabilities.tool_calls().as_str(), "vision":model.capabilities.vision().as_str(),
                "structured_output":model.capabilities.structured_output().as_str()},
        })
    } else {
        Value::Null
    };
    Ok(json!({"model_defaults":model}))
}
fn mcp_json(
    server: &mut SharedMcpServerInput,
    limits: SharedConfigurationLimits,
) -> Result<Value, SharedConfigurationError> {
    use SharedConfigurationError as Error;
    let transport = match &mut server.transport {
        SharedMcpTransport::Stdio {
            runtime_binding,
            arguments,
            environment,
        } => {
            validate_key(runtime_binding, limits)?;
            if arguments.len() > limits.max_mcp_arguments
                || environment.len() > limits.max_mcp_environment
            {
                return Err(Error::LimitExceeded);
            }
            let mut args = Vec::with_capacity(arguments.len());
            for argument in arguments {
                args.push(match argument {
                    SharedMcpArgument::Literal(value) => {
                        if value.len() > limits.max_mcp_argument_bytes {
                            return Err(Error::LimitExceeded);
                        }
                        if value.contains('\0') {
                            return Err(Error::InvalidArgument);
                        }
                        json!({"literal":value})
                    }
                    SharedMcpArgument::HostBinding(key) => {
                        validate_key(key, limits)?;
                        json!({"host_binding":key.as_str()})
                    }
                });
            }
            let mut env = BTreeMap::new();
            let mut env_names = BTreeSet::new();
            for (name, binding) in environment {
                if name.is_empty()
                    || name.len() > limits.max_key_bytes
                    || !name.bytes().enumerate().all(|(index, byte)| {
                        byte == b'_'
                            || byte.is_ascii_alphabetic()
                            || (index > 0 && byte.is_ascii_digit())
                    })
                {
                    return Err(Error::InvalidEnvironmentName);
                }
                if !env_names.insert(name.to_ascii_uppercase()) {
                    return Err(Error::InvalidEnvironmentName);
                }
                validate_key(binding, limits)?;
                env.insert(name, binding.as_str());
            }
            json!({"kind":"stdio", "runtime_binding":runtime_binding.as_str(), "arguments":args, "environment":env})
        }
        SharedMcpTransport::Https {
            endpoint,
            credential_binding,
        } => {
            if endpoint.len() > limits.max_endpoint_bytes {
                return Err(Error::LimitExceeded);
            }
            if endpoint
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
            {
                return Err(Error::InvalidEndpoint);
            }
            let url = url::Url::parse(endpoint).map_err(|_| Error::InvalidEndpoint)?;
            if url.scheme() != "https"
                || !url.has_host()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(Error::InvalidEndpoint);
            }
            if url.as_str().len() > limits.max_endpoint_bytes {
                return Err(Error::LimitExceeded);
            }
            *endpoint = url.to_string();
            if let Some(binding) = credential_binding {
                validate_key(binding, limits)?;
            }
            json!({"kind":"https", "endpoint":url.as_str(), "credential_binding":credential_binding.as_ref().map(SharedConfigurationKey::as_str)})
        }
        SharedMcpTransport::HostEndpoint { endpoint_binding } => {
            validate_key(endpoint_binding, limits)?;
            json!({"kind":"host_endpoint", "endpoint_binding":endpoint_binding.as_str()})
        }
    };
    Ok(json!({"id":server.id.as_str(), "enabled":server.enabled, "transport":transport}))
}

fn validate_skill_dependencies(
    skills: &BTreeMap<GlobalSkillId, SharedSkillPackage>,
) -> Result<(), SharedConfigurationError> {
    use SharedConfigurationError as Error;
    let mut pending = BTreeMap::new();
    let mut dependents: BTreeMap<&GlobalSkillId, Vec<&GlobalSkillId>> = BTreeMap::new();
    let mut ready = VecDeque::new();
    for (id, skill) in skills {
        pending.insert(id, skill.dependencies().len());
        if skill.dependencies().is_empty() {
            ready.push_back(id);
        }
        for dependency in skill.dependencies() {
            let target = skills
                .get(dependency)
                .ok_or(Error::MissingSkillDependency)?;
            if skill.enabled() && !target.enabled() {
                return Err(Error::DisabledSkillDependency);
            }
            dependents.entry(dependency).or_default().push(id);
        }
    }
    let mut visited = BTreeSet::new();
    while let Some(id) = ready.pop_front() {
        visited.insert(id);
        if let Some(children) = dependents.get(id) {
            for child in children {
                let count = pending
                    .get_mut(child)
                    .expect("all dependencies were indexed");
                *count -= 1;
                if *count == 0 {
                    ready.push_back(child);
                }
            }
        }
    }
    if visited.len() != skills.len() {
        return Err(Error::SkillDependencyCycle);
    }
    Ok(())
}

impl SharedConfigurationLimits {
    pub fn validate(self) -> Result<(), SharedConfigurationError> {
        if self.max_key_bytes == 0
            || self.max_metadata_bytes == 0
            || self.max_mcp_servers == 0
            || self.max_mcp_arguments == 0
            || self.max_mcp_argument_bytes == 0
            || self.max_mcp_environment == 0
            || self.max_endpoint_bytes == 0
            || self.max_skills == 0
            || self.max_total_skill_files == 0
            || self.max_total_skill_bytes == 0
        {
            return Err(SharedConfigurationError::InvalidLimits);
        }
        Ok(())
    }
}
