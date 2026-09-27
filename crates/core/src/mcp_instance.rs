//! Scoped generation claims, distinct from tool permission or process handles.

use crate::{
    McpDefinitionLimits, McpLifecycleScope, McpProtocolVersion, McpServerDefinition, SessionId,
    SharedConfigurationKey, WorkspaceCheckout, WorkspaceId,
};
use serde_json::json;
use ulid::Ulid;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct McpGenerationId(String);
impl McpGenerationId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("mcg_{value}"))
    }
    pub fn parse(value: impl Into<String>) -> Result<Self, crate::InvalidKilnId> {
        crate::parse_id(value.into(), "mcg_").map(Self)
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum McpInstanceOwner {
    WorkspaceCheckout(WorkspaceCheckout),
    Workspace(WorkspaceId),
    Session(SessionId),
    Core,
}
impl McpInstanceOwner {
    pub fn scope(&self) -> McpLifecycleScope {
        match self {
            Self::WorkspaceCheckout(_) => McpLifecycleScope::WorkspaceCheckout,
            Self::Workspace(_) => McpLifecycleScope::Workspace,
            Self::Session(_) => McpLifecycleScope::Session,
            Self::Core => McpLifecycleScope::Core,
        }
    }
}

/// Definition + concrete owner + auth profile. Definition version is deliberately
/// not part of the key: reconfiguration must stop an active old generation first.
/// Checkout identity includes its pinned filesystem identity and resolved paths.
#[derive(Clone)]
pub struct McpInstanceKey {
    definition_id: SharedConfigurationKey,
    owner: McpInstanceOwner,
    auth_profile: Option<SharedConfigurationKey>,
    canonical_json: String,
}
impl McpInstanceKey {
    pub fn new(
        definition: &McpServerDefinition,
        owner: McpInstanceOwner,
        max_bytes: usize,
    ) -> Result<Self, McpInstanceError> {
        if definition.scope() != owner.scope() {
            return Err(McpInstanceError::OwnerMismatch);
        }
        let owner_json = match &owner {
            McpInstanceOwner::WorkspaceCheckout(checkout) => json!({
                "kind":"workspace_checkout", "workspace_id":checkout.workspace_id().as_str(),
                "workspace_root_id":checkout.workspace_root_id().as_str(), "relative_directory":checkout.relative_directory(),
                "root_path":checkout.root_path(), "git_common_directory_path":checkout.git_common_directory_path(),
                "filesystem_identity":checkout.filesystem_identity().as_str(),
            }),
            McpInstanceOwner::Workspace(id) => {
                json!({"kind":"workspace", "workspace_id":id.as_str()})
            }
            McpInstanceOwner::Session(id) => json!({"kind":"session", "session_id":id.as_str()}),
            McpInstanceOwner::Core => json!({"kind":"core"}),
        };
        let serde_json::Value::Object(value) = json!({"definition_id":definition.id().as_str(),"owner":owner_json,
            "auth_profile":definition.auth_profile().map(SharedConfigurationKey::as_str)})
        else {
            unreachable!()
        };
        let canonical_json = crate::model_tool_request::canonical_object_json(value, max_bytes)
            .map_err(|_| McpInstanceError::LimitExceeded)?;
        Ok(Self {
            definition_id: definition.id().clone(),
            owner,
            auth_profile: definition.auth_profile().cloned(),
            canonical_json,
        })
    }
    pub fn definition_id(&self) -> &SharedConfigurationKey {
        &self.definition_id
    }
    pub fn owner(&self) -> &McpInstanceOwner {
        &self.owner
    }
    pub fn auth_profile(&self) -> Option<&SharedConfigurationKey> {
        self.auth_profile.as_ref()
    }
    pub fn canonical_json(&self) -> &str {
        &self.canonical_json
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpDesiredState {
    Running,
    Stopped,
}
impl McpDesiredState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Stopped => "stopped",
        }
    }
    pub fn parse(value: &str) -> Result<Self, McpInstanceError> {
        match value {
            "running" => Ok(Self::Running),
            "stopped" => Ok(Self::Stopped),
            _ => Err(McpInstanceError::IntegrityViolation),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpObservedState {
    Starting,
    Ready,
    Stopping,
    Stopped,
    Interrupted,
    Failed,
}
impl McpObservedState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Interrupted => "interrupted",
            Self::Failed => "failed",
        }
    }
    pub fn parse(value: &str) -> Result<Self, McpInstanceError> {
        match value {
            "starting" => Ok(Self::Starting),
            "ready" => Ok(Self::Ready),
            "stopping" => Ok(Self::Stopping),
            "stopped" => Ok(Self::Stopped),
            "interrupted" => Ok(Self::Interrupted),
            "failed" => Ok(Self::Failed),
            _ => Err(McpInstanceError::IntegrityViolation),
        }
    }
    pub fn is_active(self) -> bool {
        matches!(self, Self::Starting | Self::Ready | Self::Stopping)
    }
}

#[derive(Clone)]
pub struct McpInstanceRecord {
    pub key: McpInstanceKey,
    pub generation: McpGenerationId,
    pub definition_version: u64,
    pub state_version: u64,
    pub desired: McpDesiredState,
    pub observed: McpObservedState,
    pub negotiated_protocol: Option<McpProtocolVersion>,
}

pub enum McpInstanceClaim {
    /// Newly committed generation. Still requires separate launch authorization.
    Acquired(McpInstanceRecord),
    /// Must reuse/inspect the existing owner, never spawn from this result.
    Existing(McpInstanceRecord),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpInstanceTransition {
    // Stopped and StartupFailed are observations from the trusted lifecycle
    // owner after process cleanup, never acknowledgements of a user intention.
    Ready(McpProtocolVersion),
    RequestStop,
    Stopped,
    ConnectionLost,
    StartupFailed,
}
impl McpInstanceRecord {
    pub fn transition(&self, transition: McpInstanceTransition) -> Result<Self, McpInstanceError> {
        let mut next = self.clone();
        match transition {
            McpInstanceTransition::Ready(protocol)
                if self.observed == McpObservedState::Starting
                    && self.desired == McpDesiredState::Running =>
            {
                next.observed = McpObservedState::Ready;
                next.negotiated_protocol = Some(protocol);
            }
            McpInstanceTransition::RequestStop => {
                next.desired = McpDesiredState::Stopped;
                next.observed = if self.observed.is_active()
                    || self.observed == McpObservedState::Interrupted
                {
                    McpObservedState::Stopping
                } else {
                    McpObservedState::Stopped
                };
            }
            McpInstanceTransition::Stopped
                if (self.observed == McpObservedState::Stopping
                    && self.desired == McpDesiredState::Stopped)
                    || self.observed == McpObservedState::Interrupted =>
            {
                next.observed = McpObservedState::Stopped
            }
            McpInstanceTransition::ConnectionLost if self.observed.is_active() => {
                next.observed = McpObservedState::Interrupted
            }
            McpInstanceTransition::StartupFailed if self.observed == McpObservedState::Starting => {
                next.observed = McpObservedState::Failed
            }
            _ => return Err(McpInstanceError::InvalidTransition),
        }
        next.state_version = self
            .state_version
            .checked_add(1)
            .ok_or(McpInstanceError::IntegrityViolation)?;
        Ok(next)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpInstanceError {
    InvalidRequest,
    LimitExceeded,
    DefinitionNotFound,
    DefinitionChanged,
    Disabled,
    OwnerMismatch,
    OwnerNotFound,
    GenerationReused,
    Conflict,
    NotFound,
    InvalidTransition,
    IntegrityViolation,
    Unavailable,
}

pub trait McpInstanceStore: Send + Sync {
    fn claim_mcp_instance(
        &self,
        key: &McpInstanceKey,
        expected_definition_version: u64,
        generation: &McpGenerationId,
        limits: McpDefinitionLimits,
    ) -> impl Future<Output = Result<McpInstanceClaim, McpInstanceError>> + Send;
    fn get_mcp_instance(
        &self,
        key: &McpInstanceKey,
    ) -> impl Future<Output = Result<Option<McpInstanceRecord>, McpInstanceError>> + Send;
    fn transition_mcp_instance(
        &self,
        expected: &McpInstanceRecord,
        transition: McpInstanceTransition,
    ) -> impl Future<Output = Result<McpInstanceRecord, McpInstanceError>> + Send;
    /// Caller must hold exclusive daemon store ownership and invoke before any
    /// new dispatch. This records uncertainty; it neither kills orphan processes
    /// nor proves external side effects absent. No invocation is replayed.
    fn interrupt_mcp_instances_after_restart(
        &self,
        batch_size: std::num::NonZeroUsize,
    ) -> impl Future<Output = Result<usize, McpInstanceError>> + Send;
}
