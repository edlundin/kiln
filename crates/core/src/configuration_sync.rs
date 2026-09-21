//! Single-master identity and revision checks. These values carry no transport
//! authentication proof and cannot validate or activate snapshot content.

use crate::{ContentHash, InvalidKilnId, parse_id};
use ulid::Ulid;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KilnInstanceId(String);
impl KilnInstanceId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("ins_{value}"))
    }
    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        parse_id(value.into(), "ins_").map(Self)
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConfigurationGroupId(String);
impl ConfigurationGroupId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("cfg_{value}"))
    }
    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        parse_id(value.into(), "cfg_").map(Self)
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Immutable enrollment choice. Changing master requires a new group and an
/// explicit reenrollment; snapshots never alter this authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationAuthority {
    group_id: ConfigurationGroupId,
    master_id: KilnInstanceId,
}
impl ConfigurationAuthority {
    pub fn new(group_id: ConfigurationGroupId, master_id: KilnInstanceId) -> Self {
        Self {
            group_id,
            master_id,
        }
    }
    pub fn group_id(&self) -> &ConfigurationGroupId {
        &self.group_id
    }
    pub fn master_id(&self) -> &KilnInstanceId {
        &self.master_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationRevision {
    authority: ConfigurationAuthority,
    number: u64,
    schema_version: u32,
    content_hash: ContentHash,
}
impl ConfigurationRevision {
    pub fn new(
        authority: ConfigurationAuthority,
        number: u64,
        schema_version: u32,
        content_hash: ContentHash,
    ) -> Result<Self, ConfigurationSyncError> {
        if number == 0 || schema_version == 0 {
            return Err(ConfigurationSyncError::InvalidRevision);
        }
        Ok(Self {
            authority,
            number,
            schema_version,
            content_hash,
        })
    }
    pub fn authority(&self) -> &ConfigurationAuthority {
        &self.authority
    }
    pub fn number(&self) -> u64 {
        self.number
    }
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }
    pub fn content_hash(&self) -> &ContentHash {
        &self.content_hash
    }

    fn require_at_least(&self, previous: &Self) -> Result<(), ConfigurationSyncError> {
        if self.authority != previous.authority {
            return Err(ConfigurationSyncError::AuthorityMismatch);
        }
        if self.number < previous.number {
            return Err(ConfigurationSyncError::StaleRevision);
        }
        if self.number == previous.number && self != previous {
            return Err(ConfigurationSyncError::RevisionConflict);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationSyncError {
    InvalidRevision,
    SelfEnrollment,
    AuthorityMismatch,
    StaleRevision,
    RevisionConflict,
    UnsupportedSchema,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationCandidateDisposition {
    /// Metadata is eligible. Content must still be verified and atomically applied.
    NewRevision,
    AlreadyApplied,
}

/// Durable revision watermarks for one explicitly enrolled follower. An observed
/// revision survives failed downloads/disconnects, preventing stale replacement.
/// Connectivity and authentication are separate from these metadata checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationFollowerCursor {
    instance_id: KilnInstanceId,
    authority: ConfigurationAuthority,
    applied: Option<ConfigurationRevision>,
    observed: Option<ConfigurationRevision>,
}
impl ConfigurationFollowerCursor {
    /// Reconstruct only after checking stored snapshot content separately. The
    /// metadata constructor does not attest that any files/settings are present.
    pub fn from_persisted(
        instance_id: KilnInstanceId,
        authority: ConfigurationAuthority,
        applied: Option<ConfigurationRevision>,
        observed: Option<ConfigurationRevision>,
    ) -> Result<Self, ConfigurationSyncError> {
        if instance_id == authority.master_id {
            return Err(ConfigurationSyncError::SelfEnrollment);
        }
        for revision in applied.iter().chain(observed.iter()) {
            if revision.authority != authority {
                return Err(ConfigurationSyncError::AuthorityMismatch);
            }
        }
        if let (Some(applied), Some(observed)) = (&applied, &observed) {
            observed.require_at_least(applied)?;
        }
        Ok(Self {
            instance_id,
            authority,
            applied,
            observed,
        })
    }
    pub fn instance_id(&self) -> &KilnInstanceId {
        &self.instance_id
    }
    pub fn authority(&self) -> &ConfigurationAuthority {
        &self.authority
    }
    pub fn applied(&self) -> Option<&ConfigurationRevision> {
        self.applied.as_ref()
    }
    pub fn observed(&self) -> Option<&ConfigurationRevision> {
        self.observed.as_ref()
    }

    /// Call only after authenticating the enrolled master. This checks the
    /// monotonic watermark but cannot establish network identity or liveness.
    pub fn observe(&self, revision: ConfigurationRevision) -> Result<Self, ConfigurationSyncError> {
        self.require_not_stale(&revision)?;
        Ok(Self {
            observed: Some(revision),
            ..self.clone()
        })
    }

    pub fn assess_candidate(
        &self,
        candidate: &ConfigurationRevision,
        supported_schema: u32,
    ) -> Result<ConfigurationCandidateDisposition, ConfigurationSyncError> {
        self.require_not_stale(candidate)?;
        if candidate.schema_version != supported_schema {
            return Err(ConfigurationSyncError::UnsupportedSchema);
        }
        Ok(if self.applied.as_ref() == Some(candidate) {
            ConfigurationCandidateDisposition::AlreadyApplied
        } else {
            ConfigurationCandidateDisposition::NewRevision
        })
    }

    /// True is pending work. False alone never means the follower is current:
    /// an authenticated, still-valid master observation is also required.
    pub fn awaiting_snapshot(&self) -> bool {
        self.applied.is_none()
            || self.observed.as_ref().is_some_and(|value| {
                self.applied
                    .as_ref()
                    .is_none_or(|applied| value.number > applied.number)
            })
    }

    fn require_not_stale(
        &self,
        revision: &ConfigurationRevision,
    ) -> Result<(), ConfigurationSyncError> {
        if revision.authority != self.authority {
            return Err(ConfigurationSyncError::AuthorityMismatch);
        }
        for previous in self.applied.iter().chain(self.observed.iter()) {
            revision.require_at_least(previous)?;
        }
        Ok(())
    }
}

/// Role changes are local administrative decisions, never inferred from a
/// snapshot or connectivity failure. Transport/enrollment authorization belongs
/// to the caller of the internal storage boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigurationRole {
    Unassigned,
    Master(ConfigurationAuthority),
    Follower(ConfigurationAuthority),
}
impl ConfigurationRole {
    pub fn authority(&self) -> Option<&ConfigurationAuthority> {
        match self {
            Self::Unassigned => None,
            Self::Master(value) | Self::Follower(value) => Some(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationInstanceState {
    instance_id: KilnInstanceId,
    version: u64,
    role: ConfigurationRole,
    observed: Option<ConfigurationRevision>,
}
impl ConfigurationInstanceState {
    pub fn from_persisted(
        instance_id: KilnInstanceId,
        version: u64,
        role: ConfigurationRole,
        observed: Option<ConfigurationRevision>,
    ) -> Result<Self, ConfigurationStateError> {
        if version == 0 || version > i64::MAX as u64 {
            return Err(ConfigurationStateError::IntegrityViolation);
        }
        match &role {
            ConfigurationRole::Unassigned if observed.is_none() => {}
            ConfigurationRole::Master(authority)
                if authority.master_id() == &instance_id && observed.is_none() => {}
            ConfigurationRole::Follower(authority) => {
                ConfigurationFollowerCursor::from_persisted(
                    instance_id.clone(),
                    authority.clone(),
                    None,
                    observed.clone(),
                )
                .map_err(ConfigurationStateError::Revision)?;
            }
            _ => return Err(ConfigurationStateError::InvalidRole),
        }
        Ok(Self {
            instance_id,
            version,
            role,
            observed,
        })
    }
    pub fn instance_id(&self) -> &KilnInstanceId {
        &self.instance_id
    }
    /// Compare-and-swap version, also fencing asynchronous work after reenrollment.
    pub fn version(&self) -> u64 {
        self.version
    }
    pub fn role(&self) -> &ConfigurationRole {
        &self.role
    }
    pub fn observed(&self) -> Option<&ConfigurationRevision> {
        self.observed.as_ref()
    }

    pub fn change_role(&self, role: ConfigurationRole) -> Result<Self, ConfigurationStateError> {
        if role == self.role {
            return Ok(self.clone());
        }
        Self::from_persisted(self.instance_id.clone(), self.next_version()?, role, None)
    }
    pub fn observe(
        &self,
        revision: ConfigurationRevision,
    ) -> Result<Self, ConfigurationStateError> {
        let ConfigurationRole::Follower(authority) = &self.role else {
            return Err(ConfigurationStateError::InvalidRole);
        };
        let cursor = ConfigurationFollowerCursor::from_persisted(
            self.instance_id.clone(),
            authority.clone(),
            None,
            self.observed.clone(),
        )
        .map_err(ConfigurationStateError::Revision)?;
        cursor
            .observe(revision.clone())
            .map_err(ConfigurationStateError::Revision)?;
        if self.observed.as_ref() == Some(&revision) {
            return Ok(self.clone());
        }
        Self::from_persisted(
            self.instance_id.clone(),
            self.next_version()?,
            self.role.clone(),
            Some(revision),
        )
    }
    fn next_version(&self) -> Result<u64, ConfigurationStateError> {
        self.version
            .checked_add(1)
            .filter(|value| *value <= i64::MAX as u64)
            .ok_or(ConfigurationStateError::VersionExhausted)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationStateError {
    Uninitialized,
    InvalidRole,
    Conflict,
    AuthorityConflict,
    VersionExhausted,
    IntegrityViolation,
    Revision(ConfigurationSyncError),
    Unavailable,
}

/// Internal administrative storage, not an enrollment or public network API.
/// No method here proves peer identity or grants access to snapshot data.
pub trait ConfigurationStateStore: Send + Sync {
    fn initialize_configuration_instance(
        &self,
        proposed_id: KilnInstanceId,
    ) -> impl std::future::Future<
        Output = Result<ConfigurationInstanceState, ConfigurationStateError>,
    > + Send;
    fn get_configuration_instance(
        &self,
    ) -> impl std::future::Future<
        Output = Result<Option<ConfigurationInstanceState>, ConfigurationStateError>,
    > + Send;
    fn change_configuration_role(
        &self,
        expected: &ConfigurationInstanceState,
        role: ConfigurationRole,
    ) -> impl std::future::Future<
        Output = Result<ConfigurationInstanceState, ConfigurationStateError>,
    > + Send;
    fn observe_configuration_revision(
        &self,
        expected: &ConfigurationInstanceState,
        revision: ConfigurationRevision,
    ) -> impl std::future::Future<
        Output = Result<ConfigurationInstanceState, ConfigurationStateError>,
    > + Send;
}
