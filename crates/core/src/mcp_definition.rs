//! Validated local MCP registration metadata. Registration grants no authority.

use crate::{SharedConfigurationKey, SharedConfigurationLimits, SharedMcpServerInput};
use serde_json::{Value, json};

/// Caller resource budgets for one definition; no product-wide defaults.
#[derive(Debug, Clone, Copy)]
pub struct McpDefinitionLimits {
    pub max_key_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_arguments: usize,
    pub max_argument_bytes: usize,
    pub max_environment: usize,
    pub max_endpoint_bytes: usize,
}
impl McpDefinitionLimits {
    fn shared(self) -> SharedConfigurationLimits {
        SharedConfigurationLimits {
            max_key_bytes: self.max_key_bytes,
            max_metadata_bytes: self.max_metadata_bytes,
            max_mcp_servers: 1,
            max_mcp_arguments: self.max_arguments,
            max_mcp_argument_bytes: self.max_argument_bytes,
            max_mcp_environment: self.max_environment,
            max_endpoint_bytes: self.max_endpoint_bytes,
            // No skills are passed to the shared MCP-field validator.
            max_skills: 1,
            max_total_skill_files: 1,
            max_total_skill_bytes: 1,
        }
    }
    pub fn validate(self) -> Result<(), McpDefinitionError> {
        self.shared()
            .validate()
            .map_err(|_| McpDefinitionError::InvalidRequest)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpProtocolVersion {
    V20241105,
    V20250326,
    V20250618,
    V20251125,
    V20260728,
}

impl McpProtocolVersion {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V20241105 => "2024-11-05",
            Self::V20250326 => "2025-03-26",
            Self::V20250618 => "2025-06-18",
            Self::V20251125 => "2025-11-25",
            Self::V20260728 => "2026-07-28",
        }
    }
    pub fn parse(value: &str) -> Result<Self, McpDefinitionError> {
        match value {
            "2024-11-05" => Ok(Self::V20241105),
            "2025-03-26" => Ok(Self::V20250326),
            "2025-06-18" => Ok(Self::V20250618),
            "2025-11-25" => Ok(Self::V20251125),
            "2026-07-28" => Ok(Self::V20260728),
            _ => Err(McpDefinitionError::InvalidRequest),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpProtocolPolicy {
    Auto,
    Pinned(McpProtocolVersion),
}
impl McpProtocolPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Pinned(version) => version.as_str(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum McpLifecycleScope {
    #[default]
    WorkspaceCheckout,
    Workspace,
    Session,
    Core,
}
impl McpLifecycleScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WorkspaceCheckout => "workspace_checkout",
            Self::Workspace => "workspace",
            Self::Session => "session",
            Self::Core => "core",
        }
    }
    fn parse(value: &str) -> Result<Self, McpDefinitionError> {
        match value {
            "workspace_checkout" => Ok(Self::WorkspaceCheckout),
            "workspace" => Ok(Self::Workspace),
            "session" => Ok(Self::Session),
            "core" => Ok(Self::Core),
            _ => Err(McpDefinitionError::InvalidRequest),
        }
    }
}

/// Local registration uses portable host-binding references rather than resolved
/// paths or credentials. Shared snapshot consumption must validate its own
/// authority/source and is not performed by this constructor.
#[derive(Clone)]
pub struct McpServerDefinition {
    server: SharedMcpServerInput,
    protocol: McpProtocolPolicy,
    scope: McpLifecycleScope,
    auth_profile: Option<SharedConfigurationKey>,
    metadata_json: String,
}

impl McpServerDefinition {
    pub fn new(
        mut server: SharedMcpServerInput,
        protocol: McpProtocolPolicy,
        scope: McpLifecycleScope,
        auth_profile: Option<SharedConfigurationKey>,
        limits: McpDefinitionLimits,
    ) -> Result<Self, McpDefinitionError> {
        limits
            .validate()
            .map_err(|_| McpDefinitionError::InvalidRequest)?;
        SharedConfigurationKey::parse(server.id.as_str(), limits.max_key_bytes)
            .map_err(|_| McpDefinitionError::InvalidRequest)?;
        if let Some(key) = &auth_profile {
            SharedConfigurationKey::parse(key.as_str(), limits.max_key_bytes)
                .map_err(|_| McpDefinitionError::InvalidRequest)?;
        }
        let transport = crate::shared_configuration::mcp_json(&mut server, limits.shared())
            .map_err(|_| McpDefinitionError::InvalidRequest)?;
        let Value::Object(metadata) = json!({
            "schema_version": 1, "source": "local", "server": transport,
            "trust_policy": "kiln_mediated_serial",
            "protocol": protocol.as_str(), "scope": scope.as_str(),
            "auth_profile": auth_profile.as_ref().map(SharedConfigurationKey::as_str),
        }) else {
            unreachable!("object literal")
        };
        let metadata_json =
            crate::model_tool_request::canonical_object_json(metadata, limits.max_metadata_bytes)
                .map_err(|_| McpDefinitionError::LimitExceeded)?;
        Ok(Self {
            server,
            protocol,
            scope,
            auth_profile,
            metadata_json,
        })
    }

    pub fn from_metadata_json(
        bytes: &[u8],
        limits: McpDefinitionLimits,
    ) -> Result<Self, McpDefinitionError> {
        if bytes.len() > limits.max_metadata_bytes {
            return Err(McpDefinitionError::LimitExceeded);
        }
        let value: Value =
            serde_json::from_slice(bytes).map_err(|_| McpDefinitionError::InvalidRequest)?;
        if value["schema_version"].as_u64() != Some(1) || value["source"].as_str() != Some("local")
        {
            return Err(McpDefinitionError::InvalidRequest);
        }
        let server =
            crate::shared_configuration_decode::decode_mcp(&value["server"], limits.shared())
                .map_err(|_| McpDefinitionError::InvalidRequest)?;
        let policy = value["protocol"]
            .as_str()
            .ok_or(McpDefinitionError::InvalidRequest)?;
        let protocol = if policy == "auto" {
            McpProtocolPolicy::Auto
        } else {
            McpProtocolPolicy::Pinned(McpProtocolVersion::parse(policy)?)
        };
        let scope = McpLifecycleScope::parse(
            value["scope"]
                .as_str()
                .ok_or(McpDefinitionError::InvalidRequest)?,
        )?;
        let auth_profile = if value["auth_profile"].is_null() {
            None
        } else {
            Some(
                SharedConfigurationKey::parse(
                    value["auth_profile"]
                        .as_str()
                        .ok_or(McpDefinitionError::InvalidRequest)?,
                    limits.max_key_bytes,
                )
                .map_err(|_| McpDefinitionError::InvalidRequest)?,
            )
        };
        let definition = Self::new(server, protocol, scope, auth_profile, limits)?;
        // Exact regeneration rejects missing/extra/duplicate fields and noncanonical metadata.
        if definition.metadata_json.as_bytes() != bytes {
            return Err(McpDefinitionError::InvalidRequest);
        }
        Ok(definition)
    }

    pub fn id(&self) -> &SharedConfigurationKey {
        &self.server.id
    }
    pub fn server(&self) -> &SharedMcpServerInput {
        &self.server
    }
    pub fn protocol(&self) -> McpProtocolPolicy {
        self.protocol
    }
    pub fn scope(&self) -> McpLifecycleScope {
        self.scope
    }
    pub fn auth_profile(&self) -> Option<&SharedConfigurationKey> {
        self.auth_profile.as_ref()
    }
    pub fn metadata_json(&self) -> &str {
        &self.metadata_json
    }
}

pub struct McpDefinitionRecord {
    pub definition: McpServerDefinition,
    pub version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpDefinitionError {
    InvalidRequest,
    LimitExceeded,
    Conflict,
    IdempotencyConflict,
    IntegrityViolation,
    Unavailable,
}

pub trait McpDefinitionStore: Send + Sync {
    fn get_mcp_definition(
        &self,
        id: &SharedConfigurationKey,
        limits: McpDefinitionLimits,
    ) -> impl Future<Output = Result<Option<McpDefinitionRecord>, McpDefinitionError>> + Send;
    /// Exact retries return the original version, never republish an old definition.
    fn register_mcp_definition(
        &self,
        definition: &McpServerDefinition,
        expected_version: u64,
        idempotency_key: &str,
        limits: McpDefinitionLimits,
    ) -> impl Future<Output = Result<McpDefinitionRecord, McpDefinitionError>> + Send;
}
