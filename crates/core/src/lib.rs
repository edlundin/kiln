//! Process-independent Kiln domain and application operations.

use std::{collections::HashSet, fmt, future::Future, path::Path};

use ulid::Ulid;

mod assistant_message;
mod child_activity;
mod model_output;
mod native_run;
mod provider;
mod usage;
mod usage_store;
pub use assistant_message::*;
pub use child_activity::*;
pub use model_output::*;
pub use native_run::*;
pub use provider::*;
pub use usage::*;
pub use usage_store::*;

pub const INLINE_TOOL_OUTPUT_LIMIT: usize = 4_096;
pub const TOOL_OUTPUT_MEDIA_TYPE: &str = "text/plain; charset=utf-8";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreMetadata {
    pub id: String,
    pub name: String,
}

impl Default for StoreMetadata {
    fn default() -> Self {
        Self {
            id: "local".to_owned(),
            name: "Kiln local store".to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkspaceId(String);

impl WorkspaceId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("wsp_{value}"))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        parse_id(value.into(), "wsp_").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkspaceRootId(String);

impl WorkspaceRootId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("wrt_{value}"))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        parse_id(value.into(), "wrt_").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidKilnId;

fn parse_id(value: String, prefix: &str) -> Result<String, InvalidKilnId> {
    let suffix = value.strip_prefix(prefix).ok_or(InvalidKilnId)?;
    let ulid = suffix.parse::<Ulid>().map_err(|_| InvalidKilnId)?;
    if ulid.to_string() != suffix {
        return Err(InvalidKilnId);
    }
    Ok(value)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId(String);

impl SessionId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("ses_{value}"))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        parse_id(value.into(), "ses_").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskId(String);

impl TaskId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("tsk_{value}"))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        parse_id(value.into(), "tsk_").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MessageId(String);

impl MessageId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("msg_{value}"))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        parse_id(value.into(), "msg_").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContextManifestId(String);

impl ContextManifestId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("cmf_{value}"))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        parse_id(value.into(), "cmf_").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelInvocationId(String);

impl ModelInvocationId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("miv_{value}"))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        parse_id(value.into(), "miv_").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelWorkId(String);

impl ModelWorkId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("wrk_{value}"))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        parse_id(value.into(), "wrk_").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProviderAccountId(String);

impl ProviderAccountId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("pac_{value}"))
    }

    pub fn new(value: impl Into<String>) -> Result<Self, InvalidProviderAccountId> {
        parse_id(value.into(), "pac_")
            .map(Self)
            .map_err(|_| InvalidProviderAccountId)
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidProviderAccountId> {
        Self::new(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidProviderAccountId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventId(String);

impl EventId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("evt_{value}"))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        parse_id(value.into(), "evt_").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RunId(String);

impl RunId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("run_{value}"))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        parse_id(value.into(), "run_").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ToolCallId(String);

impl ToolCallId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("tcl_{value}"))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        parse_id(value.into(), "tcl_").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ApprovalId(String);

impl ApprovalId {
    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("apr_{value}"))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidKilnId> {
        parse_id(value.into(), "apr_").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContentHash(String);

impl ContentHash {
    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidContentHash> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(InvalidContentHash);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidContentHash;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Artifact {
    content_hash: ContentHash,
    media_type: String,
    size: u64,
}

impl Artifact {
    pub fn new(
        content_hash: ContentHash,
        media_type: impl Into<String>,
        size: u64,
    ) -> Result<Self, InvalidArtifact> {
        let media_type = media_type.into();
        if media_type.is_empty()
            || !media_type.is_ascii()
            || media_type.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(InvalidArtifact);
        }
        Ok(Self {
            content_hash,
            media_type,
            size,
        })
    }

    pub fn content_hash(&self) -> &ContentHash {
        &self.content_hash
    }

    pub fn media_type(&self) -> &str {
        &self.media_type
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidArtifact;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalPolicy {
    Ask,
    ReadOnly,
    FullAccess,
}

impl ApprovalPolicy {
    pub fn parse(value: &str) -> Result<Self, InvalidApprovalPolicy> {
        match value {
            "ask" => Ok(Self::Ask),
            "read_only" => Ok(Self::ReadOnly),
            "full_access" => Ok(Self::FullAccess),
            _ => Err(InvalidApprovalPolicy),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ask => "ask",
            Self::ReadOnly => "read_only",
            Self::FullAccess => "full_access",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidApprovalPolicy;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspacePathScopeError {
    Absolute,
    ParentTraversal,
    Prefix,
    InvalidComponent,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkspacePathScope {
    workspace_root_id: WorkspaceRootId,
    relative_directory: String,
}

/// The immutable workspace checkout selected for a Session.
///
/// The resolved root path is captured when the Session is created. Read-only
/// operations use this path so a later Workspace snapshot cannot silently
/// redirect an existing Session to a different checkout.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkspaceCheckout {
    workspace_id: WorkspaceId,
    workspace_root_id: WorkspaceRootId,
    relative_directory: String,
    root_path: String,
    git_common_directory_path: String,
    filesystem_identity: FilesystemIdentity,
}

impl WorkspaceCheckout {
    pub fn new(
        workspace_id: WorkspaceId,
        root: &WorkspaceRoot,
        scope: WorkspacePathScope,
    ) -> Result<Self, WorkspaceError> {
        if root.id() != scope.workspace_root_id() {
            return Err(WorkspaceError::WorkspaceRootNotFound);
        }
        Ok(Self {
            workspace_id,
            workspace_root_id: root.id().clone(),
            relative_directory: scope.relative_directory().to_owned(),
            root_path: root.canonical_path().to_owned(),
            git_common_directory_path: root.git_common_directory_path().to_owned(),
            filesystem_identity: root.filesystem_identity().clone(),
        })
    }

    pub fn from_resolved_paths(
        workspace_id: WorkspaceId,
        workspace_root_id: WorkspaceRootId,
        relative_directory: impl AsRef<Path>,
        root_path: impl Into<String>,
        git_common_directory_path: impl Into<String>,
        filesystem_identity: FilesystemIdentity,
    ) -> Result<Self, WorkspaceError> {
        let root_path = root_path.into();
        let git_common_directory_path = git_common_directory_path.into();
        if root_path.is_empty() || git_common_directory_path.is_empty() {
            return Err(WorkspaceError::WorkspaceRootMissing);
        }
        let scope = WorkspacePathScope::new(workspace_root_id.clone(), relative_directory)
            .map_err(|_| WorkspaceError::PathOutsideWorkspaceRoot)?;
        Ok(Self {
            workspace_id,
            workspace_root_id,
            relative_directory: scope.relative_directory,
            root_path,
            git_common_directory_path,
            filesystem_identity,
        })
    }

    pub fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    pub fn workspace_root_id(&self) -> &WorkspaceRootId {
        &self.workspace_root_id
    }

    pub fn relative_directory(&self) -> &str {
        &self.relative_directory
    }

    pub fn root_path(&self) -> &str {
        &self.root_path
    }

    pub fn git_common_directory_path(&self) -> &str {
        &self.git_common_directory_path
    }

    pub fn filesystem_identity(&self) -> &FilesystemIdentity {
        &self.filesystem_identity
    }

    pub fn scope(&self) -> WorkspacePathScope {
        WorkspacePathScope {
            workspace_root_id: self.workspace_root_id.clone(),
            relative_directory: self.relative_directory.clone(),
        }
    }
}

impl WorkspacePathScope {
    pub fn new(
        workspace_root_id: WorkspaceRootId,
        relative_directory: impl AsRef<Path>,
    ) -> Result<Self, WorkspacePathScopeError> {
        let input = relative_directory.as_ref();
        if input.is_absolute() {
            return Err(WorkspacePathScopeError::Absolute);
        }
        let text = input
            .to_str()
            .ok_or(WorkspacePathScopeError::InvalidComponent)?;
        if text.starts_with("\\\\") || text.as_bytes().get(1).is_some_and(|byte| byte == &b':') {
            return Err(WorkspacePathScopeError::Prefix);
        }

        let mut components = Vec::new();
        for component in input.components() {
            match component {
                std::path::Component::Normal(value) => components.push(
                    value
                        .to_str()
                        .ok_or(WorkspacePathScopeError::InvalidComponent)?
                        .to_owned(),
                ),
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => {
                    return Err(WorkspacePathScopeError::ParentTraversal);
                }
                std::path::Component::RootDir | std::path::Component::Prefix(_) => {
                    return Err(WorkspacePathScopeError::Prefix);
                }
            }
        }
        let relative_directory = if components.is_empty() {
            ".".to_owned()
        } else {
            components.join("/")
        };
        Ok(Self {
            workspace_root_id,
            relative_directory,
        })
    }

    pub fn workspace_root_id(&self) -> &WorkspaceRootId {
        &self.workspace_root_id
    }

    pub fn relative_directory(&self) -> &str {
        &self.relative_directory
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceChangeKind {
    Added,
    Modified,
    Deleted,
    TypeChanged,
    Conflicted,
    Renamed,
    Untracked,
    Binary,
}

impl WorkspaceChangeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Modified => "modified",
            Self::Deleted => "deleted",
            Self::TypeChanged => "type_changed",
            Self::Conflicted => "conflicted",
            Self::Renamed => "renamed",
            Self::Untracked => "untracked",
            Self::Binary => "binary",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkspaceChangePath(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceChangePathError {
    Empty,
    Absolute,
    ParentTraversal,
    Prefix,
    InvalidComponent,
}

impl WorkspaceChangePath {
    pub fn parse(value: impl Into<String>) -> Result<Self, WorkspaceChangePathError> {
        let value = value.into();
        if value.is_empty() {
            return Err(WorkspaceChangePathError::Empty);
        }
        if value
            .bytes()
            .any(|byte| byte == 0 || byte.is_ascii_control())
        {
            return Err(WorkspaceChangePathError::InvalidComponent);
        }
        if value.contains('\\') {
            return Err(WorkspaceChangePathError::Prefix);
        }

        let input = Path::new(&value);
        if input.is_absolute() {
            return Err(WorkspaceChangePathError::Absolute);
        }
        if value.starts_with("\\\\") || value.as_bytes().get(1) == Some(&b':') {
            return Err(WorkspaceChangePathError::Prefix);
        }

        let mut components = Vec::new();
        for component in input.components() {
            match component {
                std::path::Component::Normal(value) => components.push(
                    value
                        .to_str()
                        .ok_or(WorkspaceChangePathError::InvalidComponent)?
                        .to_owned(),
                ),
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => {
                    return Err(WorkspaceChangePathError::ParentTraversal);
                }
                std::path::Component::RootDir | std::path::Component::Prefix(_) => {
                    return Err(WorkspaceChangePathError::Prefix);
                }
            }
        }
        if components.is_empty() {
            return Err(WorkspaceChangePathError::Empty);
        }

        Ok(Self(components.join("/")))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_within(&self, checkout: &WorkspaceCheckout) -> bool {
        let scope = checkout.relative_directory();
        scope == "."
            || self
                .0
                .strip_prefix(scope)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceChangeDiffUnavailableReason {
    Untracked,
    Binary,
    Conflicted,
    Renamed,
    UnsupportedFileType,
    UnsupportedEncoding,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkspaceChangeDiffContent {
    Text { patch: String, truncated: bool },
    Unavailable(WorkspaceChangeDiffUnavailableReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceChangeDiff {
    checkout: WorkspaceCheckout,
    file: WorkspaceChangedFile,
    content: WorkspaceChangeDiffContent,
}

impl WorkspaceChangeDiff {
    pub fn new(
        checkout: WorkspaceCheckout,
        file: WorkspaceChangedFile,
        content: WorkspaceChangeDiffContent,
    ) -> Self {
        Self {
            checkout,
            file,
            content,
        }
    }

    pub fn checkout(&self) -> &WorkspaceCheckout {
        &self.checkout
    }

    pub fn file(&self) -> &WorkspaceChangedFile {
        &self.file
    }

    pub fn content(&self) -> &WorkspaceChangeDiffContent {
        &self.content
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceChangedFile {
    path: String,
    kind: WorkspaceChangeKind,
    additions: Option<u64>,
    deletions: Option<u64>,
}

impl WorkspaceChangedFile {
    pub fn new(
        path: String,
        kind: WorkspaceChangeKind,
        additions: Option<u64>,
        deletions: Option<u64>,
    ) -> Self {
        Self {
            path,
            kind,
            additions,
            deletions,
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn kind(&self) -> WorkspaceChangeKind {
        self.kind
    }

    /// `None` means the count is not meaningful, such as an untracked or
    /// binary file. It is never rendered as a misleading zero.
    pub fn additions(&self) -> Option<u64> {
        self.additions
    }

    pub fn deletions(&self) -> Option<u64> {
        self.deletions
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceChangeSummary {
    checkout: WorkspaceCheckout,
    files: Vec<WorkspaceChangedFile>,
}

impl WorkspaceChangeSummary {
    pub fn new(checkout: WorkspaceCheckout, mut files: Vec<WorkspaceChangedFile>) -> Self {
        files.sort_by(|left, right| left.path.cmp(&right.path));
        Self { checkout, files }
    }

    pub fn checkout(&self) -> &WorkspaceCheckout {
        &self.checkout
    }

    pub fn files(&self) -> &[WorkspaceChangedFile] {
        &self.files
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidEventCursor;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventCursor(u64);

impl EventCursor {
    pub fn parse(value: &str) -> Result<Self, InvalidEventCursor> {
        if value.is_empty() || (value.len() > 1 && value.starts_with('0')) {
            return Err(InvalidEventCursor);
        }
        if !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(InvalidEventCursor);
        }
        value.parse().map(Self).map_err(|_| InvalidEventCursor)
    }

    pub fn from_value(value: u64) -> Self {
        Self(value)
    }

    pub fn zero() -> Self {
        Self(0)
    }

    pub fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for EventCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidMessageRole;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageRole {
    User,
    Assistant,
}

impl MessageRole {
    pub fn parse(value: &str) -> Result<Self, InvalidMessageRole> {
        match value {
            "user" => Ok(Self::User),
            "assistant" => Ok(Self::Assistant),
            _ => Err(InvalidMessageRole),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidMessageStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageStatus {
    Complete,
    Incomplete,
}

impl MessageStatus {
    pub fn parse(value: &str) -> Result<Self, InvalidMessageStatus> {
        match value {
            "complete" => Ok(Self::Complete),
            "incomplete" => Ok(Self::Incomplete),
            _ => Err(InvalidMessageStatus),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Incomplete => "incomplete",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssistantMessageOrigin {
    pub run_id: RunId,
    pub model_invocation_id: ModelInvocationId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    id: SessionId,
    workspace_id: WorkspaceId,
    checkout: Option<WorkspaceCheckout>,
}

impl Session {
    pub fn new(id: SessionId, workspace_id: WorkspaceId) -> Self {
        Self {
            id,
            workspace_id,
            checkout: None,
        }
    }

    pub fn with_checkout(id: SessionId, checkout: WorkspaceCheckout) -> Self {
        Self {
            id,
            workspace_id: checkout.workspace_id.clone(),
            checkout: Some(checkout),
        }
    }

    pub fn id(&self) -> &SessionId {
        &self.id
    }

    pub fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    pub fn checkout(&self) -> Option<&WorkspaceCheckout> {
        self.checkout.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildActivityReference {
    pub run_id: RunId,
    pub event_id: EventId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    id: MessageId,
    session_id: SessionId,
    role: MessageRole,
    content: String,
    target_run_id: Option<RunId>,
    status: MessageStatus,
    origin: Option<AssistantMessageOrigin>,
    child_activity: Option<ChildActivityReference>,
    attachments: Vec<Artifact>,
}

pub struct PersistedMessage {
    pub id: MessageId,
    pub session_id: SessionId,
    pub role: MessageRole,
    pub content: String,
    pub target_run_id: Option<RunId>,
    pub status: MessageStatus,
    pub origin: Option<AssistantMessageOrigin>,
    pub attachments: Vec<Artifact>,
}

impl Message {
    pub fn new(
        id: MessageId,
        session_id: SessionId,
        role: MessageRole,
        content: String,
    ) -> Result<Self, SessionError> {
        Self::new_with_target_and_attachments(id, session_id, role, content, None, Vec::new())
    }

    pub fn new_with_attachments(
        id: MessageId,
        session_id: SessionId,
        role: MessageRole,
        content: String,
        attachments: Vec<Artifact>,
    ) -> Result<Self, SessionError> {
        Self::new_with_target_and_attachments(id, session_id, role, content, None, attachments)
    }

    pub fn new_targeted(
        id: MessageId,
        session_id: SessionId,
        role: MessageRole,
        content: String,
        target_run_id: RunId,
    ) -> Result<Self, SessionError> {
        Self::new_with_target_and_attachments(
            id,
            session_id,
            role,
            content,
            Some(target_run_id),
            Vec::new(),
        )
    }

    pub fn new_targeted_with_attachments(
        id: MessageId,
        session_id: SessionId,
        role: MessageRole,
        content: String,
        target_run_id: RunId,
        attachments: Vec<Artifact>,
    ) -> Result<Self, SessionError> {
        Self::new_with_target_and_attachments(
            id,
            session_id,
            role,
            content,
            Some(target_run_id),
            attachments,
        )
    }

    fn new_with_target_and_attachments(
        id: MessageId,
        session_id: SessionId,
        role: MessageRole,
        content: String,
        target_run_id: Option<RunId>,
        attachments: Vec<Artifact>,
    ) -> Result<Self, SessionError> {
        if role != MessageRole::User {
            return Err(SessionError::InvalidMessageOrigin);
        }
        if content.trim().is_empty() && attachments.is_empty() {
            return Err(SessionError::MessageContentRequired);
        }
        Ok(Self {
            id,
            session_id,
            role,
            content,
            target_run_id,
            status: MessageStatus::Complete,
            origin: None,
            child_activity: None,
            attachments,
        })
    }

    pub fn new_assistant(
        id: MessageId,
        session_id: SessionId,
        origin: AssistantMessageOrigin,
        status: MessageStatus,
        content: String,
    ) -> Result<Self, SessionError> {
        if content.is_empty() {
            return Err(SessionError::MessageContentRequired);
        }
        Ok(Self {
            id,
            session_id,
            role: MessageRole::Assistant,
            content,
            target_run_id: None,
            status,
            origin: Some(origin),
            child_activity: None,
            attachments: Vec::new(),
        })
    }

    pub fn from_persisted(value: PersistedMessage) -> Result<Self, SessionError> {
        match (value.role, value.status, value.origin, value.target_run_id) {
            (MessageRole::User, MessageStatus::Complete, None, target_run_id) => {
                Self::new_with_target_and_attachments(
                    value.id,
                    value.session_id,
                    value.role,
                    value.content,
                    target_run_id,
                    value.attachments,
                )
            }
            (MessageRole::Assistant, status, Some(origin), None) => {
                if !value.attachments.is_empty() {
                    return Err(SessionError::InvalidMessageOrigin);
                }
                Self::new_assistant(value.id, value.session_id, origin, status, value.content)
            }
            _ => Err(SessionError::InvalidMessageOrigin),
        }
    }

    pub fn id(&self) -> &MessageId {
        &self.id
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn role(&self) -> MessageRole {
        self.role
    }

    pub fn content(&self) -> &str {
        &self.content
    }

    pub fn target_run_id(&self) -> Option<&RunId> {
        self.target_run_id.as_ref()
    }

    pub fn status(&self) -> MessageStatus {
        self.status
    }

    pub fn origin(&self) -> Option<&AssistantMessageOrigin> {
        self.origin.as_ref()
    }

    pub fn child_activity(&self) -> Option<&ChildActivityReference> {
        self.child_activity.as_ref()
    }

    pub fn attachments(&self) -> &[Artifact] {
        &self.attachments
    }

    pub fn with_attachments(mut self, attachments: Vec<Artifact>) -> Result<Self, SessionError> {
        if self.role != MessageRole::User && !attachments.is_empty() {
            return Err(SessionError::InvalidMessageOrigin);
        }
        if self.content.trim().is_empty() && attachments.is_empty() {
            return Err(SessionError::MessageContentRequired);
        }
        self.attachments = attachments;
        Ok(self)
    }

    pub fn with_child_activity(
        mut self,
        reference: ChildActivityReference,
    ) -> Result<Self, SessionError> {
        if self.role != MessageRole::User || self.target_run_id.is_none() {
            return Err(SessionError::InvalidMessageOrigin);
        }
        self.child_activity = Some(reference);
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContextInstructionProvenance {
    Runtime,
    User,
    Workspace { workspace_root_id: WorkspaceRootId },
    Run { run_id: RunId },
}

impl ContextInstructionProvenance {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Runtime => "runtime",
            Self::User => "user",
            Self::Workspace { .. } => "workspace",
            Self::Run { .. } => "run",
        }
    }

    pub fn workspace_root_id(&self) -> Option<&WorkspaceRootId> {
        match self {
            Self::Workspace { workspace_root_id } => Some(workspace_root_id),
            _ => None,
        }
    }

    pub fn run_id(&self) -> Option<&RunId> {
        match self {
            Self::Run { run_id } => Some(run_id),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContextManifestEntryInput {
    Instruction {
        provenance: ContextInstructionProvenance,
        content: String,
    },
    Message {
        message_id: MessageId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContextManifestEntry {
    Instruction {
        provenance: ContextInstructionProvenance,
        content: String,
    },
    MessageSnapshot {
        message_id: MessageId,
        role: MessageRole,
        content: String,
    },
    ChildActivitySnapshot {
        reaction_message_id: MessageId,
        reference: ChildActivityReference,
        content: String,
    },
}

impl ContextManifestEntry {
    pub fn instruction(
        provenance: ContextInstructionProvenance,
        content: String,
    ) -> Result<Self, InvalidContextManifest> {
        if content.trim().is_empty() {
            return Err(InvalidContextManifest::InstructionContentRequired);
        }
        Ok(Self::Instruction {
            provenance,
            content,
        })
    }

    pub fn message_snapshot(
        message_id: MessageId,
        role: MessageRole,
        content: String,
    ) -> Result<Self, InvalidContextManifest> {
        if content.is_empty() || role == MessageRole::User && content.trim().is_empty() {
            return Err(InvalidContextManifest::MessageContentRequired);
        }
        Ok(Self::MessageSnapshot {
            message_id,
            role,
            content,
        })
    }

    pub fn content(&self) -> &str {
        match self {
            Self::Instruction { content, .. }
            | Self::MessageSnapshot { content, .. }
            | Self::ChildActivitySnapshot { content, .. } => content,
        }
    }

    pub fn child_activity_snapshot(
        reaction_message_id: MessageId,
        reference: ChildActivityReference,
        content: String,
    ) -> Result<Self, InvalidContextManifest> {
        if content.trim().is_empty() {
            return Err(InvalidContextManifest::InvalidChildActivity);
        }
        Ok(Self::ChildActivitySnapshot {
            reaction_message_id,
            reference,
            content,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextManifest {
    context_manifest_id: ContextManifestId,
    session_id: SessionId,
    run_id: RunId,
    content_hash: ContentHash,
    entries: Vec<ContextManifestEntry>,
}

impl ContextManifest {
    pub fn new(
        context_manifest_id: ContextManifestId,
        session_id: SessionId,
        run_id: RunId,
        content_hash: ContentHash,
        entries: Vec<ContextManifestEntry>,
    ) -> Result<Self, InvalidContextManifest> {
        let mut message_ids = HashSet::new();
        let mut user_message_ids = HashSet::new();
        let mut reaction_ids = HashSet::new();
        for entry in &entries {
            if entry.content().is_empty()
                || !matches!(
                    entry,
                    ContextManifestEntry::MessageSnapshot {
                        role: MessageRole::Assistant,
                        ..
                    }
                ) && entry.content().trim().is_empty()
            {
                return Err(match entry {
                    ContextManifestEntry::Instruction { .. } => {
                        InvalidContextManifest::InstructionContentRequired
                    }
                    ContextManifestEntry::MessageSnapshot { .. } => {
                        InvalidContextManifest::MessageContentRequired
                    }
                    ContextManifestEntry::ChildActivitySnapshot { .. } => {
                        InvalidContextManifest::InvalidChildActivity
                    }
                });
            }
            match entry {
                ContextManifestEntry::Instruction {
                    provenance:
                        ContextInstructionProvenance::Run {
                            run_id: source_run_id,
                        },
                    ..
                } if source_run_id != &run_id => {
                    return Err(InvalidContextManifest::InvalidRunProvenance);
                }
                ContextManifestEntry::MessageSnapshot { message_id, .. }
                    if !message_ids.insert(message_id) =>
                {
                    return Err(InvalidContextManifest::DuplicateMessage);
                }
                ContextManifestEntry::MessageSnapshot {
                    message_id,
                    role: MessageRole::User,
                    ..
                } => {
                    user_message_ids.insert(message_id);
                }
                ContextManifestEntry::ChildActivitySnapshot {
                    reaction_message_id,
                    reference,
                    ..
                } if reference.run_id == run_id
                    || !user_message_ids.contains(reaction_message_id)
                    || !reaction_ids.insert(reaction_message_id) =>
                {
                    return Err(InvalidContextManifest::InvalidChildActivity);
                }
                _ => {}
            }
        }
        Ok(Self {
            context_manifest_id,
            session_id,
            run_id,
            content_hash,
            entries,
        })
    }

    pub fn context_manifest_id(&self) -> &ContextManifestId {
        &self.context_manifest_id
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn content_hash(&self) -> &ContentHash {
        &self.content_hash
    }

    pub fn entries(&self) -> &[ContextManifestEntry] {
        &self.entries
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidContextManifest {
    InvalidChildActivity,
    InstructionContentRequired,
    MessageContentRequired,
    DuplicateMessage,
    InvalidRunProvenance,
}

pub const CONTEXT_MANIFEST_ENCODING_VERSION: u8 = 1;

pub fn canonical_context_manifest_bytes(
    session_id: &SessionId,
    run_id: &RunId,
    entries: &[ContextManifestEntry],
) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_context_field(&mut encoded, b"kiln.context-manifest");
    push_context_field(&mut encoded, &[CONTEXT_MANIFEST_ENCODING_VERSION]);
    push_context_field(&mut encoded, session_id.as_str().as_bytes());
    push_context_field(&mut encoded, run_id.as_str().as_bytes());
    push_context_field(&mut encoded, &usize_context_bytes(entries.len()));
    for (position, entry) in entries.iter().enumerate() {
        push_context_field(&mut encoded, &usize_context_bytes(position));
        match entry {
            ContextManifestEntry::Instruction {
                provenance,
                content,
            } => {
                push_context_field(&mut encoded, b"instruction");
                push_context_field(&mut encoded, provenance.as_str().as_bytes());
                let source_id = provenance
                    .workspace_root_id()
                    .map(WorkspaceRootId::as_str)
                    .or_else(|| provenance.run_id().map(RunId::as_str))
                    .unwrap_or("");
                push_context_field(&mut encoded, source_id.as_bytes());
                push_context_field(&mut encoded, b"");
                push_context_field(&mut encoded, content.as_bytes());
            }
            ContextManifestEntry::MessageSnapshot {
                message_id,
                role,
                content,
            } => {
                push_context_field(&mut encoded, b"message");
                push_context_field(&mut encoded, b"session_message");
                push_context_field(&mut encoded, message_id.as_str().as_bytes());
                push_context_field(&mut encoded, role.as_str().as_bytes());
                push_context_field(&mut encoded, content.as_bytes());
            }
            ContextManifestEntry::ChildActivitySnapshot {
                reaction_message_id,
                reference,
                content,
            } => {
                push_context_field(&mut encoded, b"child_activity");
                push_context_field(&mut encoded, reaction_message_id.as_str().as_bytes());
                push_context_field(&mut encoded, reference.run_id.as_str().as_bytes());
                push_context_field(&mut encoded, reference.event_id.as_str().as_bytes());
                push_context_field(&mut encoded, content.as_bytes());
            }
        }
    }
    encoded
}

pub fn canonical_context_manifest_request_bytes(command: &CreateContextManifest) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_context_field(&mut encoded, b"kiln.context-manifest.request");
    push_context_field(&mut encoded, &[CONTEXT_MANIFEST_ENCODING_VERSION]);
    push_context_field(&mut encoded, command.run_id.as_str().as_bytes());
    push_context_field(&mut encoded, &usize_context_bytes(command.entries.len()));
    for (position, entry) in command.entries.iter().enumerate() {
        push_context_field(&mut encoded, &usize_context_bytes(position));
        match entry {
            ContextManifestEntryInput::Instruction {
                provenance,
                content,
            } => {
                push_context_field(&mut encoded, b"instruction");
                push_context_field(&mut encoded, provenance.as_str().as_bytes());
                let source_id = provenance
                    .workspace_root_id()
                    .map(WorkspaceRootId::as_str)
                    .or_else(|| provenance.run_id().map(RunId::as_str))
                    .unwrap_or("");
                push_context_field(&mut encoded, source_id.as_bytes());
                push_context_field(&mut encoded, content.as_bytes());
            }
            ContextManifestEntryInput::Message { message_id } => {
                push_context_field(&mut encoded, b"message");
                push_context_field(&mut encoded, b"session_message");
                push_context_field(&mut encoded, message_id.as_str().as_bytes());
                push_context_field(&mut encoded, b"");
            }
        }
    }
    encoded
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProviderType(String);

impl ProviderType {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidModelReference> {
        parse_model_reference(value.into()).map(Self)
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidModelReference> {
        Self::new(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelId(String);

impl ModelId {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidModelReference> {
        parse_model_reference(value.into()).map(Self)
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidModelReference> {
        Self::new(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn parse_model_reference(value: String) -> Result<String, InvalidModelReference> {
    if value.is_empty()
        || value.trim() != value
        || !value.is_ascii()
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(InvalidModelReference);
    }
    Ok(value)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidModelReference;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationSettings {
    max_output_tokens: Option<u32>,
}

impl GenerationSettings {
    pub fn new(max_output_tokens: Option<u32>) -> Result<Self, InvalidGenerationSettings> {
        if max_output_tokens == Some(0) {
            return Err(InvalidGenerationSettings);
        }
        Ok(Self { max_output_tokens })
    }

    pub fn max_output_tokens(&self) -> Option<u32> {
        self.max_output_tokens
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidGenerationSettings;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReasoningSettings {
    effort: Option<String>,
}

impl ReasoningSettings {
    pub fn new(effort: Option<String>) -> Result<Self, InvalidReasoningSettings> {
        if effort.as_deref().is_some_and(|effort| {
            effort.is_empty()
                || effort.trim() != effort
                || !effort.is_ascii()
                || effort
                    .bytes()
                    .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        }) {
            return Err(InvalidReasoningSettings);
        }
        Ok(Self { effort })
    }

    pub fn effort(&self) -> Option<&str> {
        self.effort.as_deref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidReasoningSettings;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilitySupport {
    Supported,
    Unsupported,
    Unknown,
}

impl CapabilitySupport {
    pub fn parse(value: &str) -> Result<Self, InvalidCapabilitySupport> {
        match value {
            "supported" => Ok(Self::Supported),
            "unsupported" => Ok(Self::Unsupported),
            "unknown" => Ok(Self::Unknown),
            _ => Err(InvalidCapabilitySupport),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Unsupported => "unsupported",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidCapabilitySupport;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelCapabilitySnapshot {
    version: String,
    tool_calls: CapabilitySupport,
    vision: CapabilitySupport,
    structured_output: CapabilitySupport,
}

impl ModelCapabilitySnapshot {
    pub fn new(
        version: impl Into<String>,
        tool_calls: CapabilitySupport,
        vision: CapabilitySupport,
        structured_output: CapabilitySupport,
    ) -> Result<Self, InvalidCapabilitySnapshot> {
        let version = version.into();
        if version.is_empty()
            || version.trim() != version
            || !version.is_ascii()
            || version
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        {
            return Err(InvalidCapabilitySnapshot);
        }
        Ok(Self {
            version,
            tool_calls,
            vision,
            structured_output,
        })
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn tool_calls(&self) -> CapabilitySupport {
        self.tool_calls
    }

    pub fn vision(&self) -> CapabilitySupport {
        self.vision
    }

    pub fn structured_output(&self) -> CapabilitySupport {
        self.structured_output
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidCapabilitySnapshot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelInvocationSettings {
    provider: ProviderType,
    model: ModelId,
    generation: GenerationSettings,
    reasoning: ReasoningSettings,
}

impl ModelInvocationSettings {
    pub fn new(
        provider: ProviderType,
        model: ModelId,
        generation: GenerationSettings,
        reasoning: ReasoningSettings,
    ) -> Self {
        Self {
            provider,
            model,
            generation,
            reasoning,
        }
    }

    pub fn provider(&self) -> &ProviderType {
        &self.provider
    }

    pub fn model(&self) -> &ModelId {
        &self.model
    }

    pub fn generation(&self) -> &GenerationSettings {
        &self.generation
    }

    pub fn reasoning(&self) -> &ReasoningSettings {
        &self.reasoning
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelInvocationPurpose {
    Generation,
    Compaction,
}

impl ModelInvocationPurpose {
    pub fn parse(value: &str) -> Result<Self, InvalidModelInvocationPurpose> {
        match value {
            "generation" => Ok(Self::Generation),
            "compaction" => Ok(Self::Compaction),
            _ => Err(InvalidModelInvocationPurpose),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Generation => "generation",
            Self::Compaction => "compaction",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidModelInvocationPurpose;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelInvocationState {
    Pending,
    InFlight,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

impl ModelInvocationState {
    pub fn parse(value: &str) -> Result<Self, InvalidModelInvocationState> {
        match value {
            "pending" => Ok(Self::Pending),
            "in_flight" => Ok(Self::InFlight),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "interrupted" => Ok(Self::Interrupted),
            _ => Err(InvalidModelInvocationState),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InFlight => "in_flight",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
        }
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Pending, Self::InFlight | Self::Cancelled)
                | (
                    Self::InFlight,
                    Self::Completed | Self::Failed | Self::Cancelled | Self::Interrupted
                )
        )
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidModelInvocationState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelInvocationCompletionKind {
    AssistantOutput,
    ToolRequests,
}

impl ModelInvocationCompletionKind {
    pub fn parse(value: &str) -> Result<Self, InvalidModelInvocationOutcome> {
        match value {
            "assistant_output" => Ok(Self::AssistantOutput),
            "tool_requests" => Ok(Self::ToolRequests),
            _ => Err(InvalidModelInvocationOutcome),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::AssistantOutput => "assistant_output",
            Self::ToolRequests => "tool_requests",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelInvocationTerminalReason {
    Completed,
    ProviderError,
    InvalidRequest,
    Cancelled,
    Interrupted,
    Unknown,
}

impl ModelInvocationTerminalReason {
    pub fn parse(value: &str) -> Result<Self, InvalidModelInvocationOutcome> {
        match value {
            "completed" => Ok(Self::Completed),
            "provider_error" => Ok(Self::ProviderError),
            "invalid_request" => Ok(Self::InvalidRequest),
            "cancelled" => Ok(Self::Cancelled),
            "interrupted" => Ok(Self::Interrupted),
            "unknown" => Ok(Self::Unknown),
            _ => Err(InvalidModelInvocationOutcome),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::ProviderError => "provider_error",
            Self::InvalidRequest => "invalid_request",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelInvocationFailureReason {
    ProviderError,
    InvalidRequest,
    Unknown,
}

impl ModelInvocationFailureReason {
    pub fn parse(value: &str) -> Result<Self, InvalidModelInvocationOutcome> {
        match value {
            "provider_error" => Ok(Self::ProviderError),
            "invalid_request" => Ok(Self::InvalidRequest),
            "unknown" => Ok(Self::Unknown),
            _ => Err(InvalidModelInvocationOutcome),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProviderError => "provider_error",
            Self::InvalidRequest => "invalid_request",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelInvocationOutcome {
    Completed {
        completion_kind: ModelInvocationCompletionKind,
    },
    Failed {
        reason: ModelInvocationFailureReason,
    },
    Cancelled,
    Interrupted,
}

impl ModelInvocationOutcome {
    pub fn completed(completion_kind: ModelInvocationCompletionKind) -> Self {
        Self::Completed { completion_kind }
    }

    pub fn failed(reason: ModelInvocationFailureReason) -> Self {
        Self::Failed { reason }
    }

    pub fn cancelled() -> Self {
        Self::Cancelled
    }

    pub fn interrupted() -> Self {
        Self::Interrupted
    }

    pub fn state(self) -> ModelInvocationState {
        match self {
            Self::Completed { .. } => ModelInvocationState::Completed,
            Self::Failed { .. } => ModelInvocationState::Failed,
            Self::Cancelled => ModelInvocationState::Cancelled,
            Self::Interrupted => ModelInvocationState::Interrupted,
        }
    }

    pub fn completion_kind(self) -> Option<ModelInvocationCompletionKind> {
        match self {
            Self::Completed { completion_kind } => Some(completion_kind),
            Self::Failed { .. } | Self::Cancelled | Self::Interrupted => None,
        }
    }

    pub fn terminal_reason(self) -> ModelInvocationTerminalReason {
        match self {
            Self::Completed { .. } => ModelInvocationTerminalReason::Completed,
            Self::Failed { reason } => match reason {
                ModelInvocationFailureReason::ProviderError => {
                    ModelInvocationTerminalReason::ProviderError
                }
                ModelInvocationFailureReason::InvalidRequest => {
                    ModelInvocationTerminalReason::InvalidRequest
                }
                ModelInvocationFailureReason::Unknown => ModelInvocationTerminalReason::Unknown,
            },
            Self::Cancelled => ModelInvocationTerminalReason::Cancelled,
            Self::Interrupted => ModelInvocationTerminalReason::Interrupted,
        }
    }

    pub fn from_persisted(
        state: ModelInvocationState,
        completion_kind: Option<&str>,
        terminal_reason: Option<&str>,
    ) -> Result<Option<Self>, InvalidModelInvocationOutcome> {
        let Some(terminal_reason) = terminal_reason else {
            return if completion_kind.is_none() && !state.is_terminal() {
                Ok(None)
            } else {
                Err(InvalidModelInvocationOutcome)
            };
        };
        let reason = ModelInvocationTerminalReason::parse(terminal_reason)?;
        let outcome = match state {
            ModelInvocationState::Completed => {
                if reason != ModelInvocationTerminalReason::Completed {
                    return Err(InvalidModelInvocationOutcome);
                }
                let kind = ModelInvocationCompletionKind::parse(
                    completion_kind.ok_or(InvalidModelInvocationOutcome)?,
                )?;
                Self::Completed {
                    completion_kind: kind,
                }
            }
            ModelInvocationState::Failed => {
                if completion_kind.is_some() {
                    return Err(InvalidModelInvocationOutcome);
                }
                Self::Failed {
                    reason: ModelInvocationFailureReason::parse(terminal_reason)?,
                }
            }
            ModelInvocationState::Cancelled => {
                if completion_kind.is_some() || reason != ModelInvocationTerminalReason::Cancelled {
                    return Err(InvalidModelInvocationOutcome);
                }
                Self::Cancelled
            }
            ModelInvocationState::Interrupted => {
                if completion_kind.is_some() || reason != ModelInvocationTerminalReason::Interrupted
                {
                    return Err(InvalidModelInvocationOutcome);
                }
                Self::Interrupted
            }
            ModelInvocationState::Pending | ModelInvocationState::InFlight => {
                return Err(InvalidModelInvocationOutcome);
            }
        };
        Ok(Some(outcome))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidModelInvocationOutcome;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelInvocationRequest {
    pub invocation_id: ModelInvocationId,
    pub work_id: ModelWorkId,
    pub run_id: RunId,
    pub context_manifest_id: ContextManifestId,
    pub context_manifest_hash: ContentHash,
    pub provider_account_id: ProviderAccountId,
    pub settings: ModelInvocationSettings,
    pub capabilities: ModelCapabilitySnapshot,
    pub purpose: ModelInvocationPurpose,
    pub retry_of: Option<ModelInvocationId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedModelInvocation {
    pub request: ModelInvocationRequest,
    pub state: ModelInvocationState,
    pub outcome: Option<ModelInvocationOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelInvocation {
    invocation_id: ModelInvocationId,
    work_id: ModelWorkId,
    run_id: RunId,
    context_manifest_id: ContextManifestId,
    context_manifest_hash: ContentHash,
    provider_account_id: ProviderAccountId,
    settings: ModelInvocationSettings,
    capabilities: ModelCapabilitySnapshot,
    purpose: ModelInvocationPurpose,
    retry_of: Option<ModelInvocationId>,
    state: ModelInvocationState,
    outcome: Option<ModelInvocationOutcome>,
}

impl ModelInvocation {
    pub fn new(request: ModelInvocationRequest) -> Result<Self, InvalidModelInvocation> {
        if request.retry_of.as_ref() == Some(&request.invocation_id) {
            return Err(InvalidModelInvocation);
        }
        Ok(Self {
            invocation_id: request.invocation_id,
            work_id: request.work_id,
            run_id: request.run_id,
            context_manifest_id: request.context_manifest_id,
            context_manifest_hash: request.context_manifest_hash,
            provider_account_id: request.provider_account_id,
            settings: request.settings,
            capabilities: request.capabilities,
            purpose: request.purpose,
            retry_of: request.retry_of,
            state: ModelInvocationState::Pending,
            outcome: None,
        })
    }

    pub fn from_persisted(
        persisted: PersistedModelInvocation,
    ) -> Result<Self, InvalidModelInvocation> {
        let PersistedModelInvocation {
            request,
            state,
            outcome,
        } = persisted;
        if outcome.is_some() != state.is_terminal()
            || outcome
                .as_ref()
                .is_some_and(|outcome| outcome.state() != state)
        {
            return Err(InvalidModelInvocation);
        }
        let mut invocation = Self::new(request)?;
        invocation.state = state;
        invocation.outcome = outcome;
        Ok(invocation)
    }

    pub fn invocation_id(&self) -> &ModelInvocationId {
        &self.invocation_id
    }

    pub fn id(&self) -> &ModelInvocationId {
        self.invocation_id()
    }

    pub fn work_id(&self) -> &ModelWorkId {
        &self.work_id
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn context_manifest_id(&self) -> &ContextManifestId {
        &self.context_manifest_id
    }

    pub fn context_manifest_hash(&self) -> &ContentHash {
        &self.context_manifest_hash
    }

    pub fn provider_account_id(&self) -> &ProviderAccountId {
        &self.provider_account_id
    }

    pub fn settings(&self) -> &ModelInvocationSettings {
        &self.settings
    }

    pub fn capabilities(&self) -> &ModelCapabilitySnapshot {
        &self.capabilities
    }

    pub fn purpose(&self) -> ModelInvocationPurpose {
        self.purpose
    }

    pub fn retry_of(&self) -> Option<&ModelInvocationId> {
        self.retry_of.as_ref()
    }

    pub fn state(&self) -> ModelInvocationState {
        self.state
    }

    pub fn outcome(&self) -> Option<ModelInvocationOutcome> {
        self.outcome
    }

    pub fn transition(
        &self,
        state: ModelInvocationState,
        outcome: Option<ModelInvocationOutcome>,
    ) -> Result<Self, ModelInvocationError> {
        if !self.state.can_transition_to(state) {
            return Err(ModelInvocationError::InvalidTransition);
        }
        if outcome
            .as_ref()
            .is_some_and(|outcome| outcome.state() != state)
            || outcome.is_some() != state.is_terminal()
        {
            return Err(ModelInvocationError::InvalidTransition);
        }
        Ok(Self {
            state,
            outcome,
            ..self.clone()
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidModelInvocation;

fn usize_context_bytes(value: usize) -> [u8; 8] {
    u64::try_from(value)
        .expect("usize fits in the context manifest u64 encoding")
        .to_be_bytes()
}

fn push_context_field(encoded: &mut Vec<u8>, value: &[u8]) {
    encoded.extend_from_slice(&usize_context_bytes(value.len()));
    encoded.extend_from_slice(value);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MessageDeliveryMode {
    #[default]
    Queued,
    Interrupt,
}

impl MessageDeliveryMode {
    pub fn parse(value: &str) -> Result<Self, InvalidMessageDelivery> {
        match value {
            "queued" => Ok(Self::Queued),
            "interrupt" => Ok(Self::Interrupt),
            _ => Err(InvalidMessageDelivery),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Interrupt => "interrupt",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageDeliveryState {
    Queued,
    Delivered,
    Failed,
    Cancelled,
}

impl MessageDeliveryState {
    pub fn parse(value: &str) -> Result<Self, InvalidMessageDelivery> {
        match value {
            "queued" => Ok(Self::Queued),
            "delivered" => Ok(Self::Delivered),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(InvalidMessageDelivery),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Delivered => "delivered",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Delivered | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidMessageDelivery;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageDelivery {
    message: Message,
    mode: MessageDeliveryMode,
    state: MessageDeliveryState,
}

impl MessageDelivery {
    pub fn queued(
        message: Message,
        mode: MessageDeliveryMode,
    ) -> Result<Self, InvalidMessageDelivery> {
        if message.target_run_id().is_none() {
            return Err(InvalidMessageDelivery);
        }
        Ok(Self {
            message,
            mode,
            state: MessageDeliveryState::Queued,
        })
    }

    pub fn from_persisted(
        message: Message,
        mode: MessageDeliveryMode,
        state: MessageDeliveryState,
    ) -> Result<Self, InvalidMessageDelivery> {
        if message.target_run_id().is_none() {
            return Err(InvalidMessageDelivery);
        }
        Ok(Self {
            message,
            mode,
            state,
        })
    }

    pub fn with_state(&self, state: MessageDeliveryState) -> Result<Self, InvalidMessageDelivery> {
        if self.state != MessageDeliveryState::Queued || !state.is_terminal() {
            return Err(InvalidMessageDelivery);
        }
        Ok(Self {
            message: self.message.clone(),
            mode: self.mode,
            state,
        })
    }

    pub fn message(&self) -> &Message {
        &self.message
    }

    pub fn mode(&self) -> MessageDeliveryMode {
        self.mode
    }

    pub fn state(&self) -> MessageDeliveryState {
        self.state
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Ready,
    Running,
    Blocked,
    Completed,
    Failed,
    Cancelled,
}

impl TaskState {
    pub fn parse(value: &str) -> Result<Self, InvalidTaskState> {
        match value {
            "pending" => Ok(Self::Pending),
            "ready" => Ok(Self::Ready),
            "running" => Ok(Self::Running),
            "blocked" => Ok(Self::Blocked),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(InvalidTaskState),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Blocked => "blocked",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn can_transition_to(self, state: Self) -> bool {
        matches!(
            (self, state),
            (Self::Pending, Self::Ready | Self::Blocked | Self::Cancelled)
                | (Self::Blocked, Self::Ready | Self::Cancelled)
                | (Self::Ready, Self::Running | Self::Cancelled)
                | (
                    Self::Running,
                    Self::Completed | Self::Failed | Self::Cancelled
                )
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidTaskState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    task_id: TaskId,
    session_id: SessionId,
    objective: String,
    state: TaskState,
    parent_task_id: Option<TaskId>,
    dependency_task_ids: Vec<TaskId>,
    assigned_run_id: Option<RunId>,
}

impl Task {
    pub fn new(
        task_id: TaskId,
        session_id: SessionId,
        objective: String,
        parent_task_id: Option<TaskId>,
        mut dependency_task_ids: Vec<TaskId>,
    ) -> Result<Self, TaskError> {
        validate_task_input(
            &task_id,
            &objective,
            parent_task_id.as_ref(),
            &mut dependency_task_ids,
        )?;
        Ok(Self {
            task_id,
            session_id,
            objective,
            state: TaskState::Pending,
            parent_task_id,
            dependency_task_ids,
            assigned_run_id: None,
        })
    }

    pub fn from_persisted(
        task_id: TaskId,
        session_id: SessionId,
        objective: String,
        state: TaskState,
        parent_task_id: Option<TaskId>,
        dependency_task_ids: Vec<TaskId>,
        assigned_run_id: Option<RunId>,
    ) -> Result<Self, TaskError> {
        let mut task = Self::new(
            task_id,
            session_id,
            objective,
            parent_task_id,
            dependency_task_ids,
        )?;
        task.state = state;
        task.assigned_run_id = assigned_run_id;
        Ok(task)
    }

    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn objective(&self) -> &str {
        &self.objective
    }

    pub fn state(&self) -> TaskState {
        self.state
    }

    pub fn parent_task_id(&self) -> Option<&TaskId> {
        self.parent_task_id.as_ref()
    }

    pub fn dependency_task_ids(&self) -> &[TaskId] {
        &self.dependency_task_ids
    }

    pub fn assigned_run_id(&self) -> Option<&RunId> {
        self.assigned_run_id.as_ref()
    }

    pub fn update(
        &self,
        objective: String,
        dependency_task_ids: Vec<TaskId>,
    ) -> Result<Self, TaskError> {
        if !matches!(self.state, TaskState::Pending | TaskState::Blocked) {
            return Err(TaskError::InvalidTransition);
        }
        let mut task = Self::new(
            self.task_id.clone(),
            self.session_id.clone(),
            objective,
            self.parent_task_id.clone(),
            dependency_task_ids,
        )?;
        task.state = self.state;
        task.assigned_run_id = self.assigned_run_id.clone();
        Ok(task)
    }

    pub fn unblock(&self) -> Result<Self, TaskError> {
        if self.state != TaskState::Blocked {
            return Err(TaskError::InvalidTransition);
        }
        let mut task = self.clone();
        task.state = TaskState::Pending;
        Ok(task)
    }

    pub fn transition(&self, state: TaskState) -> Result<Self, TaskError> {
        if !self.state.can_transition_to(state) {
            return Err(TaskError::InvalidTransition);
        }
        let mut task = self.clone();
        task.state = state;
        Ok(task)
    }

    pub fn assign(&self, run_id: RunId) -> Result<Self, TaskError> {
        if !matches!(
            self.state,
            TaskState::Pending | TaskState::Ready | TaskState::Blocked
        ) {
            return Err(TaskError::InvalidAssignment);
        }
        let mut task = self.clone();
        task.assigned_run_id = Some(run_id);
        Ok(task)
    }
}

fn validate_task_input(
    task_id: &TaskId,
    objective: &str,
    parent_task_id: Option<&TaskId>,
    dependency_task_ids: &mut [TaskId],
) -> Result<(), TaskError> {
    if objective.trim().is_empty() {
        return Err(TaskError::ObjectiveRequired);
    }
    dependency_task_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    if dependency_task_ids.windows(2).any(|ids| ids[0] == ids[1]) {
        return Err(TaskError::DuplicateDependency);
    }
    if parent_task_id == Some(task_id) || dependency_task_ids.iter().any(|id| id == task_id) {
        return Err(TaskError::Cycle);
    }
    Ok(())
}

pub const DETERMINISTIC_SUBPROCESS_CAPABILITY: &str = "kiln.deterministic.subprocess";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Queued,
    Running,
    WaitingForApproval,
    Cancelling,
    Completed,
    Failed,
    Cancelled,
}

impl RunState {
    pub fn parse(value: &str) -> Result<Self, InvalidRunState> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "waiting_for_approval" => Ok(Self::WaitingForApproval),
            "cancelling" => Ok(Self::Cancelling),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(InvalidRunState),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::WaitingForApproval => "waiting_for_approval",
            Self::Cancelling => "cancelling",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Queued, Self::Running | Self::Cancelled)
                | (Self::Queued, Self::Cancelling)
                | (
                    Self::Running,
                    Self::WaitingForApproval | Self::Completed | Self::Failed | Self::Cancelling
                )
                | (Self::WaitingForApproval, Self::Running | Self::Cancelling)
                | (Self::Cancelling, Self::Cancelled)
        )
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    pub fn accepts_input(self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Running | Self::WaitingForApproval
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidRunState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCallState {
    Requested,
    AwaitingApproval,
    Ready,
    Running,
    Completed,
    Failed,
    Cancelled,
    Denied,
}

impl ToolCallState {
    pub fn parse(value: &str) -> Result<Self, InvalidToolCallState> {
        match value {
            "requested" => Ok(Self::Requested),
            "awaiting_approval" => Ok(Self::AwaitingApproval),
            "ready" => Ok(Self::Ready),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "denied" => Ok(Self::Denied),
            _ => Err(InvalidToolCallState),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::AwaitingApproval => "awaiting_approval",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Denied => "denied",
        }
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Requested,
                Self::AwaitingApproval | Self::Ready | Self::Denied
            ) | (Self::AwaitingApproval, Self::Ready | Self::Denied)
                | (Self::Ready, Self::Running | Self::Cancelled)
                | (
                    Self::Running,
                    Self::Completed | Self::Failed | Self::Cancelled
                )
        )
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Denied
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidToolCallState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolOutputStream {
    Stdout,
    Stderr,
}

impl ToolOutputStream {
    pub fn parse(value: &str) -> Result<Self, InvalidToolOutputStream> {
        match value {
            "stdout" => Ok(Self::Stdout),
            "stderr" => Ok(Self::Stderr),
            _ => Err(InvalidToolOutputStream),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidToolOutputStream;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunInputMode {
    Interactive,
    ReadOnly,
}

impl RunInputMode {
    pub fn parse(value: &str) -> Result<Self, InvalidRunInputMode> {
        match value {
            "interactive" => Ok(Self::Interactive),
            "read_only" => Ok(Self::ReadOnly),
            _ => Err(InvalidRunInputMode),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::ReadOnly => "read_only",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidRunInputMode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    run_id: RunId,
    session_id: SessionId,
    parent_run_id: Option<RunId>,
    task_id: Option<TaskId>,
    user_input_mode: RunInputMode,
    approval_policy: Option<ApprovalPolicy>,
    requested_scope: Option<WorkspacePathScope>,
    state: RunState,
}

impl Run {
    pub fn new(
        run_id: RunId,
        session_id: SessionId,
        approval_policy: ApprovalPolicy,
        requested_scope: WorkspacePathScope,
    ) -> Self {
        Self {
            run_id,
            session_id,
            parent_run_id: None,
            task_id: None,
            user_input_mode: RunInputMode::Interactive,
            approval_policy: Some(approval_policy),
            requested_scope: Some(requested_scope),
            state: RunState::Queued,
        }
    }

    pub fn from_persisted(
        run_id: RunId,
        session_id: SessionId,
        state: RunState,
        approval_policy: Option<ApprovalPolicy>,
        requested_scope: Option<WorkspacePathScope>,
    ) -> Result<Self, InvalidPersistedRun> {
        Self::from_persisted_hierarchy(
            run_id,
            session_id,
            state,
            None,
            None,
            RunInputMode::Interactive,
            approval_policy,
            requested_scope,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_persisted_hierarchy(
        run_id: RunId,
        session_id: SessionId,
        state: RunState,
        parent_run_id: Option<RunId>,
        task_id: Option<TaskId>,
        user_input_mode: RunInputMode,
        approval_policy: Option<ApprovalPolicy>,
        requested_scope: Option<WorkspacePathScope>,
    ) -> Result<Self, InvalidPersistedRun> {
        let scoped = approval_policy.is_some() && requested_scope.is_some();
        let legacy_terminal = approval_policy.is_none()
            && requested_scope.is_none()
            && parent_run_id.is_none()
            && task_id.is_none()
            && user_input_mode == RunInputMode::Interactive
            && matches!(
                state,
                RunState::Completed | RunState::Failed | RunState::Cancelled
            );
        if !scoped && !legacy_terminal {
            return Err(InvalidPersistedRun);
        }
        if parent_run_id.is_none() && user_input_mode != RunInputMode::Interactive {
            return Err(InvalidPersistedRun);
        }
        Ok(Self {
            run_id,
            session_id,
            parent_run_id,
            task_id,
            user_input_mode,
            approval_policy,
            requested_scope,
            state,
        })
    }

    pub fn new_child(
        run_id: RunId,
        session_id: SessionId,
        parent_run_id: RunId,
        task_id: Option<TaskId>,
        user_input_mode: RunInputMode,
        approval_policy: ApprovalPolicy,
        requested_scope: WorkspacePathScope,
    ) -> Self {
        Self {
            run_id,
            session_id,
            parent_run_id: Some(parent_run_id),
            task_id,
            user_input_mode,
            approval_policy: Some(approval_policy),
            requested_scope: Some(requested_scope),
            state: RunState::Queued,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn id(&self) -> &RunId {
        self.run_id()
    }
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }
    pub fn parent_run_id(&self) -> Option<&RunId> {
        self.parent_run_id.as_ref()
    }
    pub fn task_id(&self) -> Option<&TaskId> {
        self.task_id.as_ref()
    }
    pub fn user_input_mode(&self) -> RunInputMode {
        self.user_input_mode
    }
    pub fn approval_policy(&self) -> Option<ApprovalPolicy> {
        self.approval_policy
    }
    pub fn requested_scope(&self) -> Option<&WorkspacePathScope> {
        self.requested_scope.as_ref()
    }
    pub fn state(&self) -> RunState {
        self.state
    }

    pub fn transition(&self, state: RunState) -> Result<Self, RunError> {
        if !self.state.can_transition_to(state) {
            return Err(RunError::InvalidTransition);
        }
        Ok(Self {
            run_id: self.run_id.clone(),
            session_id: self.session_id.clone(),
            parent_run_id: self.parent_run_id.clone(),
            task_id: self.task_id.clone(),
            user_input_mode: self.user_input_mode,
            approval_policy: self.approval_policy,
            requested_scope: self.requested_scope.clone(),
            state,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidPersistedRun;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalState {
    Pending,
    Approved,
    Rejected,
}

impl ApprovalState {
    pub fn parse(value: &str) -> Result<Self, InvalidApprovalState> {
        match value {
            "pending" => Ok(Self::Pending),
            "approved" => Ok(Self::Approved),
            "rejected" => Ok(Self::Rejected),
            _ => Err(InvalidApprovalState),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidApprovalState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Approval {
    approval_id: ApprovalId,
    run_id: RunId,
    tool_call_id: ToolCallId,
    scope: WorkspacePathScope,
    state: ApprovalState,
}

impl Approval {
    pub fn new(
        approval_id: ApprovalId,
        run_id: RunId,
        tool_call_id: ToolCallId,
        scope: WorkspacePathScope,
    ) -> Self {
        Self {
            approval_id,
            run_id,
            tool_call_id,
            scope,
            state: ApprovalState::Pending,
        }
    }

    pub fn from_persisted(
        approval_id: ApprovalId,
        run_id: RunId,
        tool_call_id: ToolCallId,
        scope: WorkspacePathScope,
        state: ApprovalState,
    ) -> Result<Self, InvalidPersistedApproval> {
        Ok(Self {
            approval_id,
            run_id,
            tool_call_id,
            scope,
            state,
        })
    }

    pub fn approval_id(&self) -> &ApprovalId {
        &self.approval_id
    }
    pub fn id(&self) -> &ApprovalId {
        self.approval_id()
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn tool_call_id(&self) -> &ToolCallId {
        &self.tool_call_id
    }
    pub fn scope(&self) -> &WorkspacePathScope {
        &self.scope
    }
    pub fn state(&self) -> ApprovalState {
        self.state
    }

    pub fn decide(&self, state: ApprovalState) -> Result<Self, RunError> {
        if self.state != ApprovalState::Pending
            || !matches!(state, ApprovalState::Approved | ApprovalState::Rejected)
        {
            return Err(RunError::InvalidTransition);
        }
        Ok(Self {
            approval_id: self.approval_id.clone(),
            run_id: self.run_id.clone(),
            tool_call_id: self.tool_call_id.clone(),
            scope: self.scope.clone(),
            state,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidPersistedApproval;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedToolCall {
    pub tool_call_id: ToolCallId,
    pub run_id: RunId,
    pub capability: String,
    pub requested_scope: Option<WorkspacePathScope>,
    pub effective_scope: Option<WorkspacePathScope>,
    pub state: ToolCallState,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub stdout_artifact: Option<Artifact>,
    pub stderr_artifact: Option<Artifact>,
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCall {
    tool_call_id: ToolCallId,
    run_id: RunId,
    capability: String,
    requested_scope: Option<WorkspacePathScope>,
    effective_scope: Option<WorkspacePathScope>,
    state: ToolCallState,
    stdout: Option<String>,
    stderr: Option<String>,
    stdout_artifact: Option<Artifact>,
    stderr_artifact: Option<Artifact>,
    exit_code: Option<i32>,
}

impl ToolCall {
    pub fn new(
        tool_call_id: ToolCallId,
        run_id: RunId,
        capability: String,
        requested_scope: WorkspacePathScope,
    ) -> Self {
        Self {
            tool_call_id,
            run_id,
            capability,
            requested_scope: Some(requested_scope),
            effective_scope: None,
            state: ToolCallState::Requested,
            stdout: None,
            stderr: None,
            stdout_artifact: None,
            stderr_artifact: None,
            exit_code: None,
        }
    }

    pub fn from_persisted(persisted: PersistedToolCall) -> Result<Self, InvalidPersistedToolCall> {
        Self::from_persisted_inner(persisted, false)
    }

    pub fn from_persisted_event(
        persisted: PersistedToolCall,
    ) -> Result<Self, InvalidPersistedToolCall> {
        Self::from_persisted_inner(persisted, true)
    }

    fn from_persisted_inner(
        persisted: PersistedToolCall,
        allow_legacy_in_progress: bool,
    ) -> Result<Self, InvalidPersistedToolCall> {
        let PersistedToolCall {
            tool_call_id,
            run_id,
            capability,
            requested_scope,
            effective_scope,
            state,
            stdout,
            stderr,
            stdout_artifact,
            stderr_artifact,
            exit_code,
        } = persisted;
        let scoped = requested_scope.is_some();
        let legacy_unscoped = !scoped && effective_scope.is_none();
        let scope_relation_valid = effective_scope.as_ref().is_none_or(|effective| {
            requested_scope.as_ref().is_some_and(|requested| {
                effective.workspace_root_id() == requested.workspace_root_id()
                    && scope_is_within(requested, effective)
            })
        });
        let valid_result = match state {
            ToolCallState::Requested | ToolCallState::AwaitingApproval => {
                (scoped
                    || (allow_legacy_in_progress
                        && legacy_unscoped
                        && state == ToolCallState::Requested))
                    && effective_scope.is_none()
                    && stdout.is_none()
                    && stderr.is_none()
                    && stdout_artifact.is_none()
                    && stderr_artifact.is_none()
                    && exit_code.is_none()
            }
            ToolCallState::Ready | ToolCallState::Running => {
                ((scoped && effective_scope.is_some())
                    || (allow_legacy_in_progress
                        && legacy_unscoped
                        && state == ToolCallState::Running))
                    && stdout.is_none()
                    && stderr.is_none()
                    && stdout_artifact.is_none()
                    && stderr_artifact.is_none()
                    && exit_code.is_none()
            }
            ToolCallState::Completed | ToolCallState::Failed => {
                ((scoped && effective_scope.is_some()) || legacy_unscoped)
                    && (stdout.is_some() ^ stdout_artifact.is_some())
                    && (stderr.is_some() ^ stderr_artifact.is_some())
                    && terminal_exit_matches(state, exit_code)
            }
            ToolCallState::Cancelled => {
                ((scoped && effective_scope.is_some()) || legacy_unscoped)
                    && (stdout.is_some() ^ stdout_artifact.is_some())
                        == (stderr.is_some() ^ stderr_artifact.is_some())
            }
            ToolCallState::Denied => {
                scoped
                    && effective_scope.is_none()
                    && stdout.is_none()
                    && stderr.is_none()
                    && stdout_artifact.is_none()
                    && stderr_artifact.is_none()
                    && exit_code.is_none()
            }
        };
        if !valid_result || !scope_relation_valid {
            return Err(InvalidPersistedToolCall);
        }
        Ok(Self {
            tool_call_id,
            run_id,
            capability,
            requested_scope,
            effective_scope,
            state,
            stdout,
            stderr,
            stdout_artifact,
            stderr_artifact,
            exit_code,
        })
    }

    pub fn tool_call_id(&self) -> &ToolCallId {
        &self.tool_call_id
    }

    pub fn id(&self) -> &ToolCallId {
        self.tool_call_id()
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn capability(&self) -> &str {
        &self.capability
    }
    pub fn requested_scope(&self) -> Option<&WorkspacePathScope> {
        self.requested_scope.as_ref()
    }
    pub fn effective_scope(&self) -> Option<&WorkspacePathScope> {
        self.effective_scope.as_ref()
    }
    pub fn state(&self) -> ToolCallState {
        self.state
    }
    pub fn stdout(&self) -> Option<&str> {
        self.stdout.as_deref()
    }
    pub fn stderr(&self) -> Option<&str> {
        self.stderr.as_deref()
    }
    pub fn stdout_artifact(&self) -> Option<&Artifact> {
        self.stdout_artifact.as_ref()
    }
    pub fn stderr_artifact(&self) -> Option<&Artifact> {
        self.stderr_artifact.as_ref()
    }
    pub fn exit_code(&self) -> Option<i32> {
        self.exit_code
    }

    pub fn transition(&self, state: ToolCallState) -> Result<Self, RunError> {
        if !self.state.can_transition_to(state) {
            return Err(RunError::InvalidTransition);
        }
        let effective_scope = match state {
            ToolCallState::Ready | ToolCallState::Running => Some(
                self.effective_scope
                    .clone()
                    .ok_or(RunError::InvalidTransition)?,
            ),
            ToolCallState::AwaitingApproval | ToolCallState::Denied => None,
            _ => self.effective_scope.clone(),
        };
        Ok(Self {
            effective_scope,
            state,
            ..self.clone()
        })
    }

    pub fn with_effective_scope(
        &self,
        effective_scope: WorkspacePathScope,
    ) -> Result<Self, RunError> {
        if !matches!(
            self.state,
            ToolCallState::Requested | ToolCallState::AwaitingApproval
        ) || self.requested_scope.as_ref().is_none_or(|requested_scope| {
            effective_scope.workspace_root_id() != requested_scope.workspace_root_id()
                || !scope_is_within(requested_scope, &effective_scope)
        }) {
            return Err(RunError::InvalidTransition);
        }
        Ok(Self {
            effective_scope: Some(effective_scope),
            state: ToolCallState::Ready,
            stdout: None,
            stderr: None,
            stdout_artifact: None,
            stderr_artifact: None,
            exit_code: None,
            ..self.clone()
        })
    }

    pub fn with_result(&self, result: &ToolCallResult) -> Result<Self, RunError> {
        let state = result.state();
        if !self.state.can_transition_to(state) {
            return Err(RunError::InvalidTransition);
        }
        Ok(Self {
            state,
            stdout: result.stdout.clone(),
            stderr: result.stderr.clone(),
            stdout_artifact: result.stdout_artifact.clone(),
            stderr_artifact: result.stderr_artifact.clone(),
            exit_code: result.exit_code,
            ..self.clone()
        })
    }
}

fn scope_is_within(requested: &WorkspacePathScope, effective: &WorkspacePathScope) -> bool {
    effective.relative_directory() == requested.relative_directory()
        || (requested.relative_directory() == "." && effective.relative_directory() != "")
        || effective
            .relative_directory()
            .strip_prefix(requested.relative_directory())
            .is_some_and(|suffix| suffix.starts_with('/'))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidPersistedToolCall;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallResult {
    state: ToolCallState,
    stdout: Option<String>,
    stderr: Option<String>,
    stdout_artifact: Option<Artifact>,
    stderr_artifact: Option<Artifact>,
    exit_code: Option<i32>,
}

impl ToolCallResult {
    pub fn new(
        state: ToolCallState,
        stdout: String,
        stderr: String,
        exit_code: Option<i32>,
    ) -> Result<Self, RunError> {
        if !matches!(state, ToolCallState::Completed | ToolCallState::Failed)
            || !terminal_exit_matches(state, exit_code)
        {
            return Err(RunError::InvalidTransition);
        }
        Ok(Self {
            state,
            stdout: Some(stdout),
            stderr: Some(stderr),
            stdout_artifact: None,
            stderr_artifact: None,
            exit_code,
        })
    }

    pub fn from_subprocess(
        state: ToolCallState,
        output: SubprocessOutput,
    ) -> Result<Self, RunError> {
        if !matches!(state, ToolCallState::Completed | ToolCallState::Failed)
            || !terminal_exit_matches(state, output.exit_code)
            || (output.stdout_artifact.is_some() && !output.stdout.is_empty())
            || (output.stderr_artifact.is_some() && !output.stderr.is_empty())
        {
            return Err(RunError::InvalidTransition);
        }
        Ok(Self {
            state,
            stdout: output.stdout_artifact.is_none().then_some(output.stdout),
            stderr: output.stderr_artifact.is_none().then_some(output.stderr),
            stdout_artifact: output.stdout_artifact,
            stderr_artifact: output.stderr_artifact,
            exit_code: output.exit_code,
        })
    }
    pub fn state(&self) -> ToolCallState {
        self.state
    }
    pub fn stdout(&self) -> Option<&str> {
        self.stdout.as_deref()
    }
    pub fn stderr(&self) -> Option<&str> {
        self.stderr.as_deref()
    }
    pub fn stdout_artifact(&self) -> Option<&Artifact> {
        self.stdout_artifact.as_ref()
    }
    pub fn stderr_artifact(&self) -> Option<&Artifact> {
        self.stderr_artifact.as_ref()
    }
    pub fn exit_code(&self) -> Option<i32> {
        self.exit_code
    }
}

fn terminal_exit_matches(state: ToolCallState, exit_code: Option<i32>) -> bool {
    match state {
        ToolCallState::Completed => exit_code == Some(0),
        ToolCallState::Failed => exit_code != Some(0),
        ToolCallState::Requested
        | ToolCallState::AwaitingApproval
        | ToolCallState::Ready
        | ToolCallState::Running
        | ToolCallState::Cancelled
        | ToolCallState::Denied => false,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSnapshot {
    run: Run,
    tool_calls: Vec<ToolCall>,
    approvals: Vec<Approval>,
    model_invocations: Vec<ModelInvocation>,
}

impl RunSnapshot {
    pub fn new(run: Run, tool_calls: Vec<ToolCall>) -> Self {
        Self {
            run,
            tool_calls,
            approvals: Vec::new(),
            model_invocations: Vec::new(),
        }
    }

    pub fn with_approvals(run: Run, tool_calls: Vec<ToolCall>, approvals: Vec<Approval>) -> Self {
        Self {
            run,
            tool_calls,
            approvals,
            model_invocations: Vec::new(),
        }
    }

    pub fn with_model_invocations(
        run: Run,
        tool_calls: Vec<ToolCall>,
        approvals: Vec<Approval>,
        model_invocations: Vec<ModelInvocation>,
    ) -> Self {
        Self {
            run,
            tool_calls,
            approvals,
            model_invocations,
        }
    }
    pub fn run(&self) -> &Run {
        &self.run
    }
    pub fn tool_calls(&self) -> &[ToolCall] {
        &self.tool_calls
    }
    pub fn tool_call(&self, id: &ToolCallId) -> Option<&ToolCall> {
        self.tool_calls
            .iter()
            .find(|tool_call| tool_call.tool_call_id() == id)
    }
    pub fn approvals(&self) -> &[Approval] {
        &self.approvals
    }
    pub fn approval(&self, id: &ApprovalId) -> Option<&Approval> {
        self.approvals
            .iter()
            .find(|approval| approval.approval_id() == id)
    }
    pub fn approval_for_tool_call(&self, id: &ToolCallId) -> Option<&Approval> {
        self.approvals
            .iter()
            .find(|approval| approval.tool_call_id() == id)
    }

    pub fn model_invocations(&self) -> &[ModelInvocation] {
        &self.model_invocations
    }

    pub fn model_invocation(&self, id: &ModelInvocationId) -> Option<&ModelInvocation> {
        self.model_invocations
            .iter()
            .find(|invocation| invocation.invocation_id() == id)
    }

    pub fn active_model_invocation(&self) -> Option<&ModelInvocation> {
        self.model_invocations
            .iter()
            .find(|invocation| !invocation.state().is_terminal())
    }

    pub fn has_active_invocation(&self) -> bool {
        self.active_model_invocation().is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubprocessOutput {
    pub stdout: String,
    pub stderr: String,
    pub stdout_artifact: Option<Artifact>,
    pub stderr_artifact: Option<Artifact>,
    pub exit_code: Option<i32>,
    pub spawn_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubprocessRequest {
    workspace_root_path: String,
    workspace_root_filesystem_identity: FilesystemIdentity,
    scope: WorkspacePathScope,
}

impl SubprocessRequest {
    pub fn new(
        workspace_root_path: String,
        workspace_root_filesystem_identity: FilesystemIdentity,
        scope: WorkspacePathScope,
    ) -> Result<Self, RunError> {
        if workspace_root_path.is_empty() {
            return Err(RunError::PathOutsideWorkspaceRoot);
        }
        Ok(Self {
            workspace_root_path,
            workspace_root_filesystem_identity,
            scope,
        })
    }

    pub fn workspace_root_path(&self) -> &str {
        &self.workspace_root_path
    }

    pub fn workspace_root_filesystem_identity(&self) -> &FilesystemIdentity {
        &self.workspace_root_filesystem_identity
    }

    pub fn scope(&self) -> &WorkspacePathScope {
        &self.scope
    }
}

impl SubprocessOutput {
    pub fn success(stdout: impl Into<String>, stderr: impl Into<String>, exit_code: i32) -> Self {
        Self {
            stdout: stdout.into(),
            stderr: stderr.into(),
            stdout_artifact: None,
            stderr_artifact: None,
            exit_code: Some(exit_code),
            spawn_error: None,
        }
    }

    pub fn failure(
        stdout: impl Into<String>,
        stderr: impl Into<String>,
        exit_code: Option<i32>,
    ) -> Self {
        Self {
            stdout: stdout.into(),
            stderr: stderr.into(),
            stdout_artifact: None,
            stderr_artifact: None,
            exit_code,
            spawn_error: None,
        }
    }

    pub fn spawn_failure(message: impl Into<String>) -> Self {
        Self {
            stdout: String::new(),
            stderr: message.into(),
            stdout_artifact: None,
            stderr_artifact: None,
            exit_code: None,
            spawn_error: Some("subprocess failed to start".to_owned()),
        }
    }

    pub fn succeeded(&self) -> bool {
        self.spawn_error.is_none() && self.exit_code == Some(0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubprocessExecution {
    Finished(SubprocessOutput),
    Cancelled(SubprocessOutput),
    CancellationFailed,
}

pub trait SubprocessExecutor: Send + Sync {
    fn execute<C>(
        &self,
        request: SubprocessRequest,
        cancellation: C,
    ) -> impl Future<Output = SubprocessExecution> + Send
    where
        C: Future<Output = ()> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionEventPayload {
    ModelOutputRecorded {
        chunk: ModelOutputChunk,
    },
    UsageObserved {
        observation: UsageObservation,
    },
    SessionCreated {
        workspace_id: WorkspaceId,
    },
    MessageAppended {
        message: Message,
    },
    ContextManifestCreated {
        context_manifest_id: ContextManifestId,
        run_id: RunId,
        content_hash: ContentHash,
        entry_count: u64,
    },
    ModelInvocationCreated {
        invocation: ModelInvocation,
    },
    ModelInvocationStateChanged {
        invocation: ModelInvocation,
    },
    TaskCreated {
        task: Task,
    },
    TaskUpdated {
        task: Task,
    },
    TaskStateChanged {
        task: Task,
    },
    TaskAssigned {
        task: Task,
    },
    RunCreated {
        run_id: RunId,
        state: RunState,
        parent_run_id: Option<RunId>,
        task_id: Option<TaskId>,
        user_input_mode: RunInputMode,
        approval_policy: Option<ApprovalPolicy>,
        requested_scope: Option<WorkspacePathScope>,
    },
    RunQueued {
        run_id: RunId,
    },
    RunChildAdded {
        parent_run_id: RunId,
        child_run_id: RunId,
    },
    RunStateChanged {
        run_id: RunId,
        state: RunState,
    },
    RunCancellationRequested {
        run_id: RunId,
    },
    RunInputQueued {
        run_id: RunId,
        message_id: MessageId,
    },
    RunInterruptRequested {
        run_id: RunId,
        message_id: MessageId,
    },
    RunInputDelivered {
        run_id: RunId,
        message_id: MessageId,
    },
    RunInputFailed {
        run_id: RunId,
        message_id: MessageId,
    },
    RunInputCancelled {
        run_id: RunId,
        message_id: MessageId,
    },
    ToolCallRequested {
        tool_call: ToolCall,
    },
    ApprovalRequested {
        approval: Approval,
    },
    ApprovalDecided {
        approval: Approval,
    },
    ToolCallDenied {
        tool_call: ToolCall,
    },
    ToolCallStateChanged {
        tool_call: ToolCall,
    },
    ToolCallOutput {
        run_id: RunId,
        tool_call_id: ToolCallId,
        stream: ToolOutputStream,
        content: String,
    },
    ArtifactRegistered {
        run_id: RunId,
        tool_call_id: ToolCallId,
        stream: ToolOutputStream,
        artifact: Artifact,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionEvent {
    event_id: EventId,
    session_id: SessionId,
    payload: SessionEventPayload,
}

impl SessionEvent {
    pub fn session_created(
        event_id: EventId,
        session_id: SessionId,
        workspace_id: WorkspaceId,
    ) -> Self {
        Self {
            event_id,
            session_id,
            payload: SessionEventPayload::SessionCreated { workspace_id },
        }
    }

    pub fn message_appended(event_id: EventId, message: Message) -> Self {
        Self {
            event_id,
            session_id: message.session_id.clone(),
            payload: SessionEventPayload::MessageAppended { message },
        }
    }

    pub fn task_created(event_id: EventId, task: Task) -> Self {
        Self {
            event_id,
            session_id: task.session_id.clone(),
            payload: SessionEventPayload::TaskCreated { task },
        }
    }

    pub fn task_updated(event_id: EventId, task: Task) -> Self {
        Self {
            event_id,
            session_id: task.session_id.clone(),
            payload: SessionEventPayload::TaskUpdated { task },
        }
    }

    pub fn task_state_changed(event_id: EventId, task: Task) -> Self {
        Self {
            event_id,
            session_id: task.session_id.clone(),
            payload: SessionEventPayload::TaskStateChanged { task },
        }
    }

    pub fn task_assigned(event_id: EventId, task: Task) -> Self {
        Self {
            event_id,
            session_id: task.session_id.clone(),
            payload: SessionEventPayload::TaskAssigned { task },
        }
    }

    pub fn run_created(event_id: EventId, run: &Run) -> Self {
        Self {
            event_id,
            session_id: run.session_id.clone(),
            payload: SessionEventPayload::RunCreated {
                run_id: run.run_id.clone(),
                state: run.state,
                parent_run_id: run.parent_run_id.clone(),
                task_id: run.task_id.clone(),
                user_input_mode: run.user_input_mode,
                approval_policy: run.approval_policy,
                requested_scope: run.requested_scope.clone(),
            },
        }
    }

    pub fn run_queued(event_id: EventId, run: &Run) -> Self {
        Self {
            event_id,
            session_id: run.session_id.clone(),
            payload: SessionEventPayload::RunQueued {
                run_id: run.run_id.clone(),
            },
        }
    }

    pub fn run_child_added(event_id: EventId, parent: &Run, child: &Run) -> Self {
        Self {
            event_id,
            session_id: parent.session_id.clone(),
            payload: SessionEventPayload::RunChildAdded {
                parent_run_id: parent.run_id.clone(),
                child_run_id: child.run_id.clone(),
            },
        }
    }

    pub fn run_state_changed(event_id: EventId, run: &Run) -> Self {
        Self {
            event_id,
            session_id: run.session_id.clone(),
            payload: SessionEventPayload::RunStateChanged {
                run_id: run.run_id.clone(),
                state: run.state,
            },
        }
    }

    pub fn context_manifest_created(event_id: EventId, manifest: &ContextManifest) -> Self {
        Self {
            event_id,
            session_id: manifest.session_id.clone(),
            payload: SessionEventPayload::ContextManifestCreated {
                context_manifest_id: manifest.context_manifest_id.clone(),
                run_id: manifest.run_id.clone(),
                content_hash: manifest.content_hash.clone(),
                entry_count: u64::try_from(manifest.entries.len())
                    .expect("context manifest entry count fits in u64"),
            },
        }
    }

    pub fn usage_observed(event_id: EventId, observation: UsageObservation) -> Self {
        Self {
            event_id,
            session_id: observation.session_id.clone(),
            payload: SessionEventPayload::UsageObserved { observation },
        }
    }

    pub fn model_output_recorded(event_id: EventId, chunk: ModelOutputChunk) -> Self {
        Self {
            event_id,
            session_id: chunk.session_id.clone(),
            payload: SessionEventPayload::ModelOutputRecorded { chunk },
        }
    }

    pub fn model_invocation_created(
        event_id: EventId,
        session_id: SessionId,
        invocation: ModelInvocation,
    ) -> Self {
        Self {
            event_id,
            session_id,
            payload: SessionEventPayload::ModelInvocationCreated { invocation },
        }
    }

    pub fn model_invocation_state_changed(
        event_id: EventId,
        session_id: SessionId,
        invocation: ModelInvocation,
    ) -> Self {
        Self {
            event_id,
            session_id,
            payload: SessionEventPayload::ModelInvocationStateChanged { invocation },
        }
    }

    pub fn run_cancellation_requested(event_id: EventId, run: &Run) -> Self {
        Self {
            event_id,
            session_id: run.session_id.clone(),
            payload: SessionEventPayload::RunCancellationRequested {
                run_id: run.run_id.clone(),
            },
        }
    }

    pub fn run_input_queued(event_id: EventId, delivery: &MessageDelivery) -> Self {
        Self::run_input_event(event_id, delivery, MessageDeliveryState::Queued)
    }

    pub fn run_input_delivered(event_id: EventId, delivery: &MessageDelivery) -> Self {
        Self::run_input_event(event_id, delivery, MessageDeliveryState::Delivered)
    }

    pub fn run_input_failed(event_id: EventId, delivery: &MessageDelivery) -> Self {
        Self::run_input_event(event_id, delivery, MessageDeliveryState::Failed)
    }

    pub fn run_input_cancelled(event_id: EventId, delivery: &MessageDelivery) -> Self {
        Self::run_input_event(event_id, delivery, MessageDeliveryState::Cancelled)
    }

    fn run_input_event(
        event_id: EventId,
        delivery: &MessageDelivery,
        state: MessageDeliveryState,
    ) -> Self {
        let message = delivery.message();
        let run_id = message
            .target_run_id()
            .expect("MessageDelivery has one target Run")
            .clone();
        let payload = match (delivery.mode(), state) {
            (MessageDeliveryMode::Interrupt, MessageDeliveryState::Queued) => {
                SessionEventPayload::RunInterruptRequested {
                    run_id,
                    message_id: message.id().clone(),
                }
            }
            (_, MessageDeliveryState::Queued) => SessionEventPayload::RunInputQueued {
                run_id,
                message_id: message.id().clone(),
            },
            (_, MessageDeliveryState::Delivered) => SessionEventPayload::RunInputDelivered {
                run_id,
                message_id: message.id().clone(),
            },
            (_, MessageDeliveryState::Failed) => SessionEventPayload::RunInputFailed {
                run_id,
                message_id: message.id().clone(),
            },
            (_, MessageDeliveryState::Cancelled) => SessionEventPayload::RunInputCancelled {
                run_id,
                message_id: message.id().clone(),
            },
        };
        Self {
            event_id,
            session_id: message.session_id().clone(),
            payload,
        }
    }

    pub fn tool_call_requested(
        event_id: EventId,
        session_id: SessionId,
        tool_call: ToolCall,
    ) -> Self {
        Self {
            event_id,
            session_id,
            payload: SessionEventPayload::ToolCallRequested { tool_call },
        }
    }

    pub fn tool_call_state_changed(
        event_id: EventId,
        session_id: SessionId,
        tool_call: ToolCall,
    ) -> Self {
        Self {
            event_id,
            session_id,
            payload: SessionEventPayload::ToolCallStateChanged { tool_call },
        }
    }

    pub fn approval_requested(
        event_id: EventId,
        session_id: SessionId,
        approval: Approval,
    ) -> Self {
        Self {
            event_id,
            session_id,
            payload: SessionEventPayload::ApprovalRequested { approval },
        }
    }

    pub fn approval_decided(event_id: EventId, session_id: SessionId, approval: Approval) -> Self {
        Self {
            event_id,
            session_id,
            payload: SessionEventPayload::ApprovalDecided { approval },
        }
    }

    pub fn tool_call_denied(event_id: EventId, session_id: SessionId, tool_call: ToolCall) -> Self {
        Self {
            event_id,
            session_id,
            payload: SessionEventPayload::ToolCallDenied { tool_call },
        }
    }

    pub fn tool_call_output(
        event_id: EventId,
        session_id: SessionId,
        run_id: RunId,
        tool_call_id: ToolCallId,
        stream: ToolOutputStream,
        content: String,
    ) -> Self {
        Self {
            event_id,
            session_id,
            payload: SessionEventPayload::ToolCallOutput {
                run_id,
                tool_call_id,
                stream,
                content,
            },
        }
    }

    pub fn artifact_registered(
        event_id: EventId,
        session_id: SessionId,
        run_id: RunId,
        tool_call_id: ToolCallId,
        stream: ToolOutputStream,
        artifact: Artifact,
    ) -> Self {
        Self {
            event_id,
            session_id,
            payload: SessionEventPayload::ArtifactRegistered {
                run_id,
                tool_call_id,
                stream,
                artifact,
            },
        }
    }

    pub fn event_id(&self) -> &EventId {
        &self.event_id
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn payload(&self) -> &SessionEventPayload {
        &self.payload
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSessionEvent {
    event_id: EventId,
    session_id: SessionId,
    cursor: EventCursor,
    payload: SessionEventPayload,
}

impl StoredSessionEvent {
    pub fn session_created(
        event_id: EventId,
        session_id: SessionId,
        cursor: EventCursor,
        workspace_id: WorkspaceId,
    ) -> Result<Self, InvalidEventCursor> {
        if cursor == EventCursor::zero() {
            return Err(InvalidEventCursor);
        }
        Ok(Self {
            event_id,
            session_id,
            cursor,
            payload: SessionEventPayload::SessionCreated { workspace_id },
        })
    }

    pub fn message_appended(
        event_id: EventId,
        session_id: SessionId,
        cursor: EventCursor,
        message: Message,
    ) -> Result<Self, InvalidEventCursor> {
        if cursor == EventCursor::zero() {
            return Err(InvalidEventCursor);
        }
        Ok(Self {
            event_id,
            session_id,
            cursor,
            payload: SessionEventPayload::MessageAppended { message },
        })
    }

    pub fn from_event(
        event: &SessionEvent,
        cursor: EventCursor,
    ) -> Result<Self, InvalidEventCursor> {
        if cursor == EventCursor::zero() {
            return Err(InvalidEventCursor);
        }
        Ok(Self {
            event_id: event.event_id.clone(),
            session_id: event.session_id.clone(),
            cursor,
            payload: event.payload.clone(),
        })
    }

    pub fn from_parts(
        event_id: EventId,
        session_id: SessionId,
        cursor: EventCursor,
        payload: SessionEventPayload,
    ) -> Result<Self, InvalidEventCursor> {
        if cursor == EventCursor::zero() {
            return Err(InvalidEventCursor);
        }
        Ok(Self {
            event_id,
            session_id,
            cursor,
            payload,
        })
    }

    pub fn event_id(&self) -> &EventId {
        &self.event_id
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn cursor(&self) -> EventCursor {
        self.cursor
    }

    pub fn activity_run_id(&self) -> Option<&RunId> {
        match &self.payload {
            SessionEventPayload::SessionCreated { .. } => None,
            SessionEventPayload::MessageAppended { message } => message
                .target_run_id()
                .or_else(|| message.origin().map(|origin| &origin.run_id)),
            SessionEventPayload::ModelOutputRecorded { chunk } => Some(&chunk.run_id),
            SessionEventPayload::UsageObserved { observation } => Some(&observation.run_id),
            SessionEventPayload::ModelInvocationCreated { invocation }
            | SessionEventPayload::ModelInvocationStateChanged { invocation } => {
                Some(invocation.run_id())
            }
            SessionEventPayload::TaskCreated { task }
            | SessionEventPayload::TaskUpdated { task }
            | SessionEventPayload::TaskStateChanged { task }
            | SessionEventPayload::TaskAssigned { task } => task.assigned_run_id(),
            SessionEventPayload::RunChildAdded { child_run_id, .. } => Some(child_run_id),
            SessionEventPayload::ToolCallRequested { tool_call }
            | SessionEventPayload::ToolCallDenied { tool_call }
            | SessionEventPayload::ToolCallStateChanged { tool_call } => Some(tool_call.run_id()),
            SessionEventPayload::ApprovalRequested { approval }
            | SessionEventPayload::ApprovalDecided { approval } => Some(approval.run_id()),
            SessionEventPayload::ContextManifestCreated { run_id, .. }
            | SessionEventPayload::RunCreated { run_id, .. }
            | SessionEventPayload::RunQueued { run_id }
            | SessionEventPayload::RunStateChanged { run_id, .. }
            | SessionEventPayload::RunCancellationRequested { run_id }
            | SessionEventPayload::RunInputQueued { run_id, .. }
            | SessionEventPayload::RunInterruptRequested { run_id, .. }
            | SessionEventPayload::RunInputDelivered { run_id, .. }
            | SessionEventPayload::RunInputFailed { run_id, .. }
            | SessionEventPayload::RunInputCancelled { run_id, .. }
            | SessionEventPayload::ToolCallOutput { run_id, .. }
            | SessionEventPayload::ArtifactRegistered { run_id, .. } => Some(run_id),
        }
    }

    pub fn payload(&self) -> &SessionEventPayload {
        &self.payload
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionEventPage {
    events: Vec<StoredSessionEvent>,
    current_cursor: EventCursor,
}

impl SessionEventPage {
    pub fn new(events: Vec<StoredSessionEvent>, current_cursor: EventCursor) -> Self {
        Self {
            events,
            current_cursor,
        }
    }

    pub fn events(&self) -> &[StoredSessionEvent] {
        &self.events
    }

    pub fn current_cursor(&self) -> EventCursor {
        self.current_cursor
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendMessage {
    pub session_id: SessionId,
    pub content: String,
    pub attachments: Vec<Artifact>,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateContextManifest {
    pub run_id: RunId,
    pub entries: Vec<ContextManifestEntryInput>,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateModelInvocation {
    pub run_id: RunId,
    pub context_manifest_id: ContextManifestId,
    pub context_manifest_hash: ContentHash,
    pub provider_account_id: ProviderAccountId,
    pub settings: ModelInvocationSettings,
    pub capabilities: ModelCapabilitySnapshot,
    pub purpose: ModelInvocationPurpose,
    pub retry_of: Option<ModelInvocationId>,
    pub idempotency_key: String,
}

pub fn canonical_model_invocation_request_bytes(command: &CreateModelInvocation) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_context_field(&mut encoded, b"kiln.model-invocation.request");
    push_context_field(&mut encoded, &[1]);
    push_context_field(&mut encoded, command.run_id.as_str().as_bytes());
    push_context_field(
        &mut encoded,
        command.context_manifest_id.as_str().as_bytes(),
    );
    push_context_field(
        &mut encoded,
        command.context_manifest_hash.as_str().as_bytes(),
    );
    push_context_field(
        &mut encoded,
        command.provider_account_id.as_str().as_bytes(),
    );
    push_context_field(
        &mut encoded,
        command.settings.provider().as_str().as_bytes(),
    );
    push_context_field(&mut encoded, command.settings.model().as_str().as_bytes());
    push_context_field(
        &mut encoded,
        &command
            .settings
            .generation()
            .max_output_tokens()
            .map_or_else(Vec::new, |value| value.to_be_bytes().to_vec()),
    );
    push_context_field(
        &mut encoded,
        command
            .settings
            .reasoning()
            .effort()
            .unwrap_or("")
            .as_bytes(),
    );
    push_context_field(&mut encoded, command.capabilities.version().as_bytes());
    for capability in [
        command.capabilities.tool_calls(),
        command.capabilities.vision(),
        command.capabilities.structured_output(),
    ] {
        push_context_field(&mut encoded, capability.as_str().as_bytes());
    }
    push_context_field(&mut encoded, command.purpose.as_str().as_bytes());
    push_context_field(
        &mut encoded,
        command
            .retry_of
            .as_ref()
            .map_or(b"".as_slice(), |id| id.as_str().as_bytes()),
    );
    encoded
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendRunInput {
    pub run_id: RunId,
    pub content: String,
    pub attachments: Vec<Artifact>,
    pub delivery_mode: MessageDeliveryMode,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReactToRunActivity {
    pub run_id: RunId,
    pub content: String,
    pub attachments: Vec<Artifact>,
    pub child_activity: ChildActivityReference,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordRunInputDelivery {
    pub message_id: MessageId,
    pub state: MessageDeliveryState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTask {
    pub session_id: SessionId,
    pub objective: String,
    pub parent_task_id: Option<TaskId>,
    pub dependency_task_ids: Vec<TaskId>,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateTask {
    pub task_id: TaskId,
    pub objective: String,
    pub dependency_task_ids: Vec<TaskId>,
    pub idempotency_key: String,
}

impl UpdateTask {
    fn validate(mut self) -> Result<Self, TaskError> {
        validate_task_input(
            &self.task_id,
            &self.objective,
            None,
            &mut self.dependency_task_ids,
        )?;
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionTask {
    pub task_id: TaskId,
    pub state: TaskState,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignTask {
    pub task_id: TaskId,
    pub run_id: RunId,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateTaskDisposition {
    Created,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTaskMutation {
    pub value: Task,
    pub events: Vec<StoredSessionEvent>,
    pub disposition: CreateTaskDisposition,
}

impl CreateTaskMutation {
    pub fn new(
        value: Task,
        events: Vec<StoredSessionEvent>,
        disposition: CreateTaskDisposition,
    ) -> Self {
        Self {
            value,
            events,
            disposition,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskMutationDisposition {
    Applied,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskMutation {
    pub value: Task,
    pub events: Vec<StoredSessionEvent>,
    pub disposition: TaskMutationDisposition,
}

impl TaskMutation {
    pub fn new(
        value: Task,
        events: Vec<StoredSessionEvent>,
        disposition: TaskMutationDisposition,
    ) -> Self {
        Self {
            value,
            events,
            disposition,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskError {
    SessionNotFound,
    TaskNotFound,
    ObjectiveRequired,
    ParentTaskNotFound,
    DependencyTaskNotFound,
    RunNotFound,
    TaskLinkOutsideSession,
    DuplicateDependency,
    Cycle,
    InvalidTransition,
    InvalidAssignment,
    IdempotencyKeyRequired,
    IdempotencyConflict,
    TaskStoreUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStoreError {
    TaskNotFound,
    ParentTaskNotFound,
    DependencyTaskNotFound,
    RunNotFound,
    TaskLinkOutsideSession,
    IdempotencyKeyRequired,
    IdempotencyConflict,
    Cycle,
    InvalidTransition,
    InvalidAssignment,
    InvalidTask,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionError {
    WorkspaceNotFound,
    WorkspaceRootNotFound,
    SessionNotFound,
    MessageContentRequired,
    IdempotencyKeyRequired,
    IdempotencyConflict,
    InvalidMessageOrigin,
    WorkspaceStoreUnavailable,
    SessionStoreUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextManifestError {
    ContextManifestNotFound,
    RunNotFound,
    RunNotAcceptingWork,
    WorkspaceProvenanceMismatch,
    RunProvenanceMismatch,
    InstructionContentRequired,
    MessageNotFound,
    MessageOutsideSession,
    MessageTargetMismatch,
    MessageDeliveryNotDelivered,
    MessageIncomplete,
    MessageOriginMismatch,
    DuplicateMessage,
    IdempotencyKeyRequired,
    IdempotencyConflict,
    IntegrityViolation,
    StoreUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextManifestStoreError {
    RunNotFound,
    RunNotAcceptingWork,
    WorkspaceProvenanceMismatch,
    RunProvenanceMismatch,
    InstructionContentRequired,
    MessageNotFound,
    MessageOutsideSession,
    MessageTargetMismatch,
    MessageDeliveryNotDelivered,
    MessageIncomplete,
    MessageOriginMismatch,
    DuplicateMessage,
    IdempotencyKeyRequired,
    IdempotencyConflict,
    IntegrityViolation,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelInvocationError {
    ModelInvocationNotFound,
    RunNotFound,
    RunNotRunning,
    ContextManifestNotFound,
    ContextManifestRunMismatch,
    ContextManifestHashMismatch,
    ActiveInvocationExists,
    RetryNotAllowed,
    RetryRequestMismatch,
    IdempotencyKeyRequired,
    IdempotencyConflict,
    InvalidTransition,
    FinalUsageRequired,
    IntegrityViolation,
    StoreUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelInvocationStoreError {
    ModelInvocationNotFound,
    RunNotFound,
    RunNotRunning,
    ContextManifestNotFound,
    ContextManifestRunMismatch,
    ContextManifestHashMismatch,
    ActiveInvocationExists,
    RetryNotAllowed,
    RetryRequestMismatch,
    IdempotencyKeyRequired,
    IdempotencyConflict,
    InvalidTransition,
    FinalUsageRequired,
    IntegrityViolation,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunError {
    InvalidChildActivity,
    SessionNotFound,
    WorkspaceRootNotFound,
    PathOutsideWorkspaceRoot,
    RunNotFound,
    ParentRunNotFound,
    ParentRunTerminal,
    TaskNotFound,
    TaskLinkOutsideSession,
    InvalidTaskAssignment,
    InputContentRequired,
    RunInputReadOnly,
    RunNotAcceptingInput,
    MessageDeliveryNotFound,
    InvalidMessageDelivery,
    MessageDeliveryOutOfOrder,
    ActiveRootRunExists,
    IdempotencyKeyRequired,
    InvalidTransition,
    CancellationFailed,
    ApprovalNotFound,
    ApprovalAlreadyDecided,
    IdempotencyConflict,
    RunStoreUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStoreError {
    InvalidChildActivity,
    ActiveRootRunExists,
    RunNotFound,
    ParentRunNotFound,
    ParentRunTerminal,
    TaskNotFound,
    TaskLinkOutsideSession,
    InvalidTaskAssignment,
    RunInputReadOnly,
    RunNotAcceptingInput,
    MessageDeliveryNotFound,
    InvalidMessageDelivery,
    MessageDeliveryOutOfOrder,
    IdempotencyKeyRequired,
    InvalidTransition,
    WorkspaceRootNotFound,
    PathOutsideWorkspaceRoot,
    ApprovalNotFound,
    ApprovalAlreadyDecided,
    IdempotencyConflict,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartRunDisposition {
    Created,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendRunInputDisposition {
    Created,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordRunInputDisposition {
    Applied,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateContextManifestDisposition {
    Created,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateModelInvocationDisposition {
    Created,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateModelInvocationMutation {
    pub value: ModelInvocation,
    pub events: Vec<StoredSessionEvent>,
    pub disposition: CreateModelInvocationDisposition,
}

impl CreateModelInvocationMutation {
    pub fn new(
        value: ModelInvocation,
        events: Vec<StoredSessionEvent>,
        disposition: CreateModelInvocationDisposition,
    ) -> Self {
        Self {
            value,
            events,
            disposition,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelInvocationMutationDisposition {
    Applied,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelInvocationMutation {
    pub value: ModelInvocation,
    pub events: Vec<StoredSessionEvent>,
    pub disposition: ModelInvocationMutationDisposition,
}

impl ModelInvocationMutation {
    pub fn new(
        value: ModelInvocation,
        events: Vec<StoredSessionEvent>,
        disposition: ModelInvocationMutationDisposition,
    ) -> Self {
        Self {
            value,
            events,
            disposition,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateContextManifestMutation {
    pub value: ContextManifest,
    pub events: Vec<StoredSessionEvent>,
    pub disposition: CreateContextManifestDisposition,
}

impl CreateContextManifestMutation {
    pub fn new(
        value: ContextManifest,
        events: Vec<StoredSessionEvent>,
        disposition: CreateContextManifestDisposition,
    ) -> Self {
        Self {
            value,
            events,
            disposition,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendRunInputMutation {
    pub value: MessageDelivery,
    pub events: Vec<StoredSessionEvent>,
    pub disposition: SendRunInputDisposition,
}

impl SendRunInputMutation {
    pub fn new(
        value: MessageDelivery,
        events: Vec<StoredSessionEvent>,
        disposition: SendRunInputDisposition,
    ) -> Self {
        Self {
            value,
            events,
            disposition,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordRunInputMutation {
    pub value: MessageDelivery,
    pub events: Vec<StoredSessionEvent>,
    pub disposition: RecordRunInputDisposition,
}

impl RecordRunInputMutation {
    pub fn new(
        value: MessageDelivery,
        events: Vec<StoredSessionEvent>,
        disposition: RecordRunInputDisposition,
    ) -> Self {
        Self {
            value,
            events,
            disposition,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalDecisionDisposition {
    Applied,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartRunMutation {
    pub value: RunSnapshot,
    pub events: Vec<StoredSessionEvent>,
    pub disposition: StartRunDisposition,
}

impl StartRunMutation {
    pub fn new(
        value: RunSnapshot,
        events: Vec<StoredSessionEvent>,
        disposition: StartRunDisposition,
    ) -> Self {
        Self {
            value,
            events,
            disposition,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunMutation<T> {
    pub value: T,
    pub events: Vec<StoredSessionEvent>,
}

impl<T> RunMutation<T> {
    pub fn new(value: T, events: Vec<StoredSessionEvent>) -> Self {
        Self { value, events }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalDecisionMutation {
    pub value: RunSnapshot,
    pub events: Vec<StoredSessionEvent>,
    pub disposition: ApprovalDecisionDisposition,
}

impl ApprovalDecisionMutation {
    pub fn new(
        value: RunSnapshot,
        events: Vec<StoredSessionEvent>,
        disposition: ApprovalDecisionDisposition,
    ) -> Self {
        Self {
            value,
            events,
            disposition,
        }
    }
}

pub trait SessionStore: Send + Sync {
    fn create_session(
        &self,
        session: &Session,
        event: &SessionEvent,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
    fn get_session(
        &self,
        id: &SessionId,
    ) -> impl Future<Output = Result<Option<Session>, StoreError>> + Send;
    fn list_sessions(
        &self,
        workspace_id: &WorkspaceId,
    ) -> impl Future<Output = Result<Vec<Session>, StoreError>> + Send;
    fn append_message(
        &self,
        message: &Message,
        event: &SessionEvent,
        idempotency_key: &str,
    ) -> impl Future<Output = Result<Message, StoreError>> + Send;
    fn list_session_events(
        &self,
        session_id: &SessionId,
        after: EventCursor,
    ) -> impl Future<Output = Result<SessionEventPage, StoreError>> + Send;
    fn list_events_after(
        &self,
        after: EventCursor,
    ) -> impl Future<Output = Result<SessionEventPage, StoreError>> + Send;
    fn current_event_cursor(
        &self,
    ) -> impl Future<Output = Result<Option<EventCursor>, StoreError>> + Send;
}

pub trait ContextManifestStore: Send + Sync {
    fn create_context_manifest(
        &self,
        command: &CreateContextManifest,
        context_manifest_id: ContextManifestId,
        event_id: EventId,
    ) -> impl Future<Output = Result<CreateContextManifestMutation, ContextManifestStoreError>> + Send;
    fn get_context_manifest(
        &self,
        context_manifest_id: &ContextManifestId,
    ) -> impl Future<Output = Result<Option<ContextManifest>, ContextManifestStoreError>> + Send;
    fn list_context_manifests(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<Vec<ContextManifest>, ContextManifestStoreError>> + Send;
}

pub trait ModelInvocationStore: Send + Sync {
    fn create_model_invocation(
        &self,
        command: &CreateModelInvocation,
        model_invocation_id: ModelInvocationId,
        model_work_id: ModelWorkId,
        event_id: EventId,
    ) -> impl Future<Output = Result<CreateModelInvocationMutation, ModelInvocationStoreError>> + Send;
    fn get_model_invocation(
        &self,
        model_invocation_id: &ModelInvocationId,
    ) -> impl Future<Output = Result<Option<ModelInvocation>, ModelInvocationStoreError>> + Send;
    fn list_model_invocations(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<Vec<ModelInvocation>, ModelInvocationStoreError>> + Send;
    fn begin_model_invocation(
        &self,
        invocation: &ModelInvocation,
        event_id: EventId,
        run_event_id: EventId,
    ) -> impl Future<Output = Result<ModelInvocationMutation, ModelInvocationStoreError>> + Send;
    fn finish_model_invocation(
        &self,
        invocation: &ModelInvocation,
        outcome: ModelInvocationOutcome,
        event_id: EventId,
    ) -> impl Future<Output = Result<ModelInvocationMutation, ModelInvocationStoreError>> + Send;
}

pub trait TaskStore: Send + Sync {
    fn create_task(
        &self,
        task: &Task,
        event: &SessionEvent,
        idempotency_key: &str,
    ) -> impl Future<Output = Result<CreateTaskMutation, TaskStoreError>> + Send;
    fn get_task(
        &self,
        task_id: &TaskId,
    ) -> impl Future<Output = Result<Option<Task>, StoreError>> + Send;
    fn update_task(
        &self,
        command: &UpdateTask,
        event_ids: [EventId; 2],
    ) -> impl Future<Output = Result<TaskMutation, TaskStoreError>> + Send;
    fn transition_task(
        &self,
        command: &TransitionTask,
        event_id: EventId,
    ) -> impl Future<Output = Result<TaskMutation, TaskStoreError>> + Send;
    fn assign_task(
        &self,
        command: &AssignTask,
        event_id: EventId,
    ) -> impl Future<Output = Result<TaskMutation, TaskStoreError>> + Send;
}

pub trait SessionIdGenerator: Send + Sync {
    fn session_id(&self) -> SessionId;
    fn message_id(&self) -> MessageId;
    fn event_id(&self) -> EventId;
}

pub trait TaskIdGenerator: Send + Sync {
    fn task_id(&self) -> TaskId;
    fn event_id(&self) -> EventId;
}

pub trait RunIdGenerator: Send + Sync {
    fn run_id(&self) -> RunId;
    fn message_id(&self) -> MessageId;
    fn tool_call_id(&self) -> ToolCallId;
    fn approval_id(&self) -> ApprovalId;
    fn event_id(&self) -> EventId;
}

pub trait ContextManifestIdGenerator: Send + Sync {
    fn context_manifest_id(&self) -> ContextManifestId;
    fn event_id(&self) -> EventId;
}

pub trait ModelInvocationIdGenerator: Send + Sync {
    fn model_invocation_id(&self) -> ModelInvocationId;
    fn model_work_id(&self) -> ModelWorkId;
    fn event_id(&self) -> EventId;
}

pub trait RunStore: SessionStore + WorkspaceStore {
    fn start_root_run(
        &self,
        run: &Run,
        events: &[SessionEvent],
        idempotency_key: &str,
    ) -> impl Future<Output = Result<StartRunMutation, RunStoreError>> + Send;
    fn start_child_run(
        &self,
        run: &Run,
        events: &[SessionEvent],
        task_assignment_event_id: Option<&EventId>,
        idempotency_key: &str,
    ) -> impl Future<Output = Result<StartRunMutation, RunStoreError>> + Send;
    fn get_run(
        &self,
        id: &RunId,
    ) -> impl Future<Output = Result<Option<RunSnapshot>, RunStoreError>> + Send;
    fn list_session_runs(
        &self,
        session_id: &SessionId,
    ) -> impl Future<Output = Result<Vec<RunSnapshot>, RunStoreError>> + Send;
    fn send_run_input(
        &self,
        delivery: &MessageDelivery,
        events: &[SessionEvent],
        idempotency_key: &str,
    ) -> impl Future<Output = Result<SendRunInputMutation, RunStoreError>> + Send;
    fn record_run_input_delivery(
        &self,
        command: &RecordRunInputDelivery,
        event_id: EventId,
    ) -> impl Future<Output = Result<RecordRunInputMutation, RunStoreError>> + Send;
    fn next_queued_run_input(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<Option<MessageDelivery>, RunStoreError>> + Send;
    fn list_queued_run_inputs(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<Vec<MessageDelivery>, RunStoreError>> + Send;
    fn get_tool_call(
        &self,
        id: &ToolCallId,
    ) -> impl Future<Output = Result<Option<(Run, ToolCall)>, RunStoreError>> + Send;
    fn get_approval(
        &self,
        id: &ApprovalId,
    ) -> impl Future<Output = Result<Option<(Run, Approval)>, RunStoreError>> + Send;
    fn get_approval_decision(
        &self,
        approval_id: &ApprovalId,
        idempotency_key: &str,
    ) -> impl Future<Output = Result<Option<(ApprovalState, RunSnapshot)>, RunStoreError>> + Send;
    fn begin_execution(
        &self,
        run: &Run,
        tool_call: &ToolCall,
        approval: Option<&Approval>,
        events: &[SessionEvent],
    ) -> impl Future<Output = Result<RunMutation<RunSnapshot>, RunStoreError>> + Send;
    fn decide_approval(
        &self,
        run: &Run,
        tool_call: &ToolCall,
        approval: &Approval,
        idempotency_key: &str,
        events: &[SessionEvent],
    ) -> impl Future<Output = Result<ApprovalDecisionMutation, RunStoreError>> + Send;
    fn begin_tool_call(
        &self,
        tool_call_id: &ToolCallId,
        events: &[SessionEvent],
    ) -> impl Future<Output = Result<RunMutation<ToolCall>, RunStoreError>> + Send;
    fn finish_execution(
        &self,
        run: &Run,
        tool_call: &ToolCall,
        events: &[SessionEvent],
    ) -> impl Future<Output = Result<RunMutation<RunSnapshot>, RunStoreError>> + Send;
    fn finish_denied_execution(
        &self,
        run: &Run,
        tool_call: &ToolCall,
        events: &[SessionEvent],
    ) -> impl Future<Output = Result<RunMutation<RunSnapshot>, RunStoreError>> + Send;
    fn request_cancellation(
        &self,
        run: &Run,
        tool_call: Option<&ToolCall>,
        approval: Option<&Approval>,
        cancelled_inputs: &[MessageDelivery],
        events: &[SessionEvent],
    ) -> impl Future<Output = Result<RunMutation<RunSnapshot>, RunStoreError>> + Send;
    fn finish_cancellation(
        &self,
        run: &Run,
        tool_call: &ToolCall,
        cancelled_inputs: &[MessageDelivery],
        events: &[SessionEvent],
    ) -> impl Future<Output = Result<RunMutation<RunSnapshot>, RunStoreError>> + Send;
}

pub struct ContextManifestApplication<S, I> {
    store: S,
    ids: I,
}

impl<S, I> ContextManifestApplication<S, I> {
    pub fn new(store: S, ids: I) -> Self {
        Self { store, ids }
    }
}

impl<S, I> ContextManifestApplication<S, I>
where
    S: ContextManifestStore,
    I: ContextManifestIdGenerator,
{
    pub async fn create_context_manifest(
        &self,
        command: CreateContextManifest,
    ) -> Result<CreateContextManifestMutation, ContextManifestError> {
        if command.idempotency_key.is_empty() {
            return Err(ContextManifestError::IdempotencyKeyRequired);
        }
        self.store
            .create_context_manifest(
                &command,
                self.ids.context_manifest_id(),
                self.ids.event_id(),
            )
            .await
            .map_err(map_context_manifest_store_error)
    }

    pub async fn get_context_manifest(
        &self,
        context_manifest_id: ContextManifestId,
    ) -> Result<ContextManifest, ContextManifestError> {
        self.store
            .get_context_manifest(&context_manifest_id)
            .await
            .map_err(map_context_manifest_store_error)?
            .ok_or(ContextManifestError::ContextManifestNotFound)
    }

    pub async fn list_context_manifests(
        &self,
        run_id: RunId,
    ) -> Result<Vec<ContextManifest>, ContextManifestError> {
        self.store
            .list_context_manifests(&run_id)
            .await
            .map_err(map_context_manifest_store_error)
    }
}

fn map_context_manifest_store_error(error: ContextManifestStoreError) -> ContextManifestError {
    match error {
        ContextManifestStoreError::RunNotFound => ContextManifestError::RunNotFound,
        ContextManifestStoreError::RunNotAcceptingWork => ContextManifestError::RunNotAcceptingWork,
        ContextManifestStoreError::WorkspaceProvenanceMismatch => {
            ContextManifestError::WorkspaceProvenanceMismatch
        }
        ContextManifestStoreError::RunProvenanceMismatch => {
            ContextManifestError::RunProvenanceMismatch
        }
        ContextManifestStoreError::InstructionContentRequired => {
            ContextManifestError::InstructionContentRequired
        }
        ContextManifestStoreError::MessageNotFound => ContextManifestError::MessageNotFound,
        ContextManifestStoreError::MessageOutsideSession => {
            ContextManifestError::MessageOutsideSession
        }
        ContextManifestStoreError::MessageTargetMismatch => {
            ContextManifestError::MessageTargetMismatch
        }
        ContextManifestStoreError::MessageDeliveryNotDelivered => {
            ContextManifestError::MessageDeliveryNotDelivered
        }
        ContextManifestStoreError::MessageIncomplete => ContextManifestError::MessageIncomplete,
        ContextManifestStoreError::MessageOriginMismatch => {
            ContextManifestError::MessageOriginMismatch
        }
        ContextManifestStoreError::DuplicateMessage => ContextManifestError::DuplicateMessage,
        ContextManifestStoreError::IdempotencyKeyRequired => {
            ContextManifestError::IdempotencyKeyRequired
        }
        ContextManifestStoreError::IdempotencyConflict => ContextManifestError::IdempotencyConflict,
        ContextManifestStoreError::IntegrityViolation => ContextManifestError::IntegrityViolation,
        ContextManifestStoreError::Unavailable => ContextManifestError::StoreUnavailable,
    }
}

pub struct ModelInvocationApplication<S, I> {
    store: S,
    ids: I,
}

impl<S, I> ModelInvocationApplication<S, I> {
    pub fn new(store: S, ids: I) -> Self {
        Self { store, ids }
    }
}

impl<S, I> ModelInvocationApplication<S, I>
where
    S: ModelInvocationStore,
    I: ModelInvocationIdGenerator,
{
    pub async fn create_model_invocation(
        &self,
        command: CreateModelInvocation,
    ) -> Result<CreateModelInvocationMutation, ModelInvocationError> {
        if command.idempotency_key.is_empty() {
            return Err(ModelInvocationError::IdempotencyKeyRequired);
        }
        self.store
            .create_model_invocation(
                &command,
                self.ids.model_invocation_id(),
                self.ids.model_work_id(),
                self.ids.event_id(),
            )
            .await
            .map_err(map_model_invocation_store_error)
    }

    pub async fn get_model_invocation(
        &self,
        model_invocation_id: ModelInvocationId,
    ) -> Result<ModelInvocation, ModelInvocationError> {
        self.store
            .get_model_invocation(&model_invocation_id)
            .await
            .map_err(map_model_invocation_store_error)?
            .ok_or(ModelInvocationError::ModelInvocationNotFound)
    }

    pub async fn list_model_invocations(
        &self,
        run_id: RunId,
    ) -> Result<Vec<ModelInvocation>, ModelInvocationError> {
        self.store
            .list_model_invocations(&run_id)
            .await
            .map_err(map_model_invocation_store_error)
    }

    pub async fn begin_model_invocation(
        &self,
        invocation: ModelInvocation,
    ) -> Result<ModelInvocationMutation, ModelInvocationError> {
        self.store
            .begin_model_invocation(&invocation, self.ids.event_id(), self.ids.event_id())
            .await
            .map_err(map_model_invocation_store_error)
    }

    pub async fn finish_model_invocation(
        &self,
        invocation: ModelInvocation,
        outcome: ModelInvocationOutcome,
    ) -> Result<ModelInvocationMutation, ModelInvocationError> {
        self.store
            .finish_model_invocation(&invocation, outcome, self.ids.event_id())
            .await
            .map_err(map_model_invocation_store_error)
    }
}

fn map_model_invocation_store_error(error: ModelInvocationStoreError) -> ModelInvocationError {
    match error {
        ModelInvocationStoreError::ModelInvocationNotFound => {
            ModelInvocationError::ModelInvocationNotFound
        }
        ModelInvocationStoreError::RunNotFound => ModelInvocationError::RunNotFound,
        ModelInvocationStoreError::RunNotRunning => ModelInvocationError::RunNotRunning,
        ModelInvocationStoreError::ContextManifestNotFound => {
            ModelInvocationError::ContextManifestNotFound
        }
        ModelInvocationStoreError::ContextManifestRunMismatch => {
            ModelInvocationError::ContextManifestRunMismatch
        }
        ModelInvocationStoreError::ContextManifestHashMismatch => {
            ModelInvocationError::ContextManifestHashMismatch
        }
        ModelInvocationStoreError::ActiveInvocationExists => {
            ModelInvocationError::ActiveInvocationExists
        }
        ModelInvocationStoreError::RetryNotAllowed => ModelInvocationError::RetryNotAllowed,
        ModelInvocationStoreError::RetryRequestMismatch => {
            ModelInvocationError::RetryRequestMismatch
        }
        ModelInvocationStoreError::IdempotencyKeyRequired => {
            ModelInvocationError::IdempotencyKeyRequired
        }
        ModelInvocationStoreError::IdempotencyConflict => ModelInvocationError::IdempotencyConflict,
        ModelInvocationStoreError::InvalidTransition => ModelInvocationError::InvalidTransition,
        ModelInvocationStoreError::FinalUsageRequired => ModelInvocationError::FinalUsageRequired,
        ModelInvocationStoreError::IntegrityViolation => ModelInvocationError::IntegrityViolation,
        ModelInvocationStoreError::Unavailable => ModelInvocationError::StoreUnavailable,
    }
}

pub struct RunApplication<S, I> {
    store: S,
    ids: I,
}

impl<S, I> RunApplication<S, I> {
    pub fn new(store: S, ids: I) -> Self {
        Self { store, ids }
    }
}

impl<S, I> RunApplication<S, I>
where
    S: RunStore,
    I: RunIdGenerator,
{
    pub async fn start_root_run(
        &self,
        session_id: SessionId,
        idempotency_key: String,
        approval_policy: ApprovalPolicy,
        requested_scope: WorkspacePathScope,
    ) -> Result<StartRunMutation, RunError> {
        if idempotency_key.is_empty() {
            return Err(RunError::IdempotencyKeyRequired);
        }
        let run_id = self.ids.run_id();
        self.start_root_run_with_id(
            session_id,
            idempotency_key,
            approval_policy,
            requested_scope,
            run_id,
        )
        .await
    }

    async fn start_root_run_with_id(
        &self,
        session_id: SessionId,
        idempotency_key: String,
        approval_policy: ApprovalPolicy,
        requested_scope: WorkspacePathScope,
        run_id: RunId,
    ) -> Result<StartRunMutation, RunError> {
        let session = self
            .store
            .get_session(&session_id)
            .await
            .map_err(|_| RunError::RunStoreUnavailable)?;
        let session = session.ok_or(RunError::SessionNotFound)?;
        let workspace = self
            .store
            .get_workspace(session.workspace_id())
            .await
            .map_err(|_| RunError::RunStoreUnavailable)?
            .ok_or(RunError::WorkspaceRootNotFound)?;
        if workspace
            .root(requested_scope.workspace_root_id())
            .is_none()
        {
            return Err(RunError::WorkspaceRootNotFound);
        }
        if requested_scope.relative_directory().is_empty() {
            return Err(RunError::PathOutsideWorkspaceRoot);
        }
        if session.id() != &session_id {
            return Err(RunError::SessionNotFound);
        }
        let run = Run::new(run_id, session_id, approval_policy, requested_scope);
        let events = [
            SessionEvent::run_created(self.ids.event_id(), &run),
            SessionEvent::run_queued(self.ids.event_id(), &run),
        ];
        self.store
            .start_root_run(&run, &events, &idempotency_key)
            .await
            .map_err(map_run_store_error)
    }

    pub async fn start_child_run(
        &self,
        parent_run_id: RunId,
        task_id: Option<TaskId>,
        user_input_mode: RunInputMode,
        idempotency_key: String,
        approval_policy: ApprovalPolicy,
        requested_scope: WorkspacePathScope,
    ) -> Result<StartRunMutation, RunError> {
        if idempotency_key.is_empty() {
            return Err(RunError::IdempotencyKeyRequired);
        }
        let parent = self
            .store
            .get_run(&parent_run_id)
            .await
            .map_err(|_| RunError::RunStoreUnavailable)?
            .ok_or(RunError::ParentRunNotFound)?
            .run;
        let session = self
            .store
            .get_session(parent.session_id())
            .await
            .map_err(|_| RunError::RunStoreUnavailable)?
            .ok_or(RunError::SessionNotFound)?;
        let workspace = self
            .store
            .get_workspace(session.workspace_id())
            .await
            .map_err(|_| RunError::RunStoreUnavailable)?
            .ok_or(RunError::WorkspaceRootNotFound)?;
        if workspace
            .root(requested_scope.workspace_root_id())
            .is_none()
        {
            return Err(RunError::WorkspaceRootNotFound);
        }
        if requested_scope.relative_directory().is_empty() {
            return Err(RunError::PathOutsideWorkspaceRoot);
        }
        let run = Run::new_child(
            self.ids.run_id(),
            parent.session_id().clone(),
            parent_run_id,
            task_id.clone(),
            user_input_mode,
            approval_policy,
            requested_scope,
        );
        let events = [
            SessionEvent::run_created(self.ids.event_id(), &run),
            SessionEvent::run_queued(self.ids.event_id(), &run),
            SessionEvent::run_child_added(self.ids.event_id(), &parent, &run),
        ];
        let task_event_id = task_id.as_ref().map(|_| self.ids.event_id());
        self.store
            .start_child_run(&run, &events, task_event_id.as_ref(), &idempotency_key)
            .await
            .map_err(map_run_store_error)
    }

    pub async fn list_session_runs(
        &self,
        session_id: SessionId,
    ) -> Result<Vec<RunSnapshot>, RunError> {
        self.store
            .get_session(&session_id)
            .await
            .map_err(|_| RunError::RunStoreUnavailable)?
            .ok_or(RunError::SessionNotFound)?;
        self.store
            .list_session_runs(&session_id)
            .await
            .map_err(map_run_store_error)
    }

    pub async fn send_run_input(
        &self,
        command: SendRunInput,
    ) -> Result<SendRunInputMutation, RunError> {
        self.send_input(command, None).await
    }

    pub async fn react_to_run_activity(
        &self,
        command: ReactToRunActivity,
    ) -> Result<SendRunInputMutation, RunError> {
        self.send_input(
            SendRunInput {
                run_id: command.run_id,
                content: command.content,
                attachments: command.attachments,
                delivery_mode: MessageDeliveryMode::Queued,
                idempotency_key: command.idempotency_key,
            },
            Some(command.child_activity),
        )
        .await
    }

    async fn send_input(
        &self,
        command: SendRunInput,
        reference: Option<ChildActivityReference>,
    ) -> Result<SendRunInputMutation, RunError> {
        if command.idempotency_key.is_empty() {
            return Err(RunError::IdempotencyKeyRequired);
        }
        let run = self.get_run(command.run_id.clone()).await?.run;
        let message = Message::new_targeted_with_attachments(
            self.ids.message_id(),
            run.session_id().clone(),
            MessageRole::User,
            command.content,
            command.run_id,
            command.attachments,
        )
        .map_err(|_| RunError::InputContentRequired)?;
        let message = match reference {
            Some(reference) => message
                .with_child_activity(reference)
                .map_err(|_| RunError::InvalidChildActivity)?,
            None => message,
        };
        let delivery = MessageDelivery::queued(message.clone(), command.delivery_mode)
            .map_err(|_| RunError::InvalidMessageDelivery)?;
        let events = [
            SessionEvent::message_appended(self.ids.event_id(), message),
            SessionEvent::run_input_queued(self.ids.event_id(), &delivery),
        ];
        self.store
            .send_run_input(&delivery, &events, &command.idempotency_key)
            .await
            .map_err(map_run_store_error)
    }

    pub async fn record_run_input_delivery(
        &self,
        command: RecordRunInputDelivery,
    ) -> Result<RecordRunInputMutation, RunError> {
        self.store
            .record_run_input_delivery(&command, self.ids.event_id())
            .await
            .map_err(map_run_store_error)
    }

    pub async fn next_queued_run_input(
        &self,
        run_id: RunId,
    ) -> Result<Option<MessageDelivery>, RunError> {
        self.store
            .next_queued_run_input(&run_id)
            .await
            .map_err(map_run_store_error)
    }

    pub async fn list_run_subtree(&self, run_id: RunId) -> Result<Vec<RunSnapshot>, RunError> {
        let root = self.get_run(run_id.clone()).await?;
        let runs = self
            .store
            .list_session_runs(root.run().session_id())
            .await
            .map_err(map_run_store_error)?;
        Ok(run_subtree(runs, &run_id))
    }

    pub async fn request_cancellation(
        &self,
        run_id: RunId,
    ) -> Result<RunMutation<RunSnapshot>, RunError> {
        let subtree = self.list_run_subtree(run_id).await?;
        let snapshot = subtree.first().ok_or(RunError::RunNotFound)?;
        let descendants_terminal = subtree
            .iter()
            .skip(1)
            .all(|descendant| descendant.run().state().is_terminal());
        let own_work_terminal = snapshot
            .tool_calls()
            .iter()
            .all(|tool_call| tool_call.state().is_terminal())
            && !snapshot.has_active_invocation();
        let (run, tool_call, approval, mut events) = match snapshot.run.state() {
            RunState::Queued => {
                let next = if descendants_terminal && own_work_terminal {
                    RunState::Cancelled
                } else {
                    RunState::Cancelling
                };
                let run = snapshot.run.transition(next)?;
                let events = vec![
                    SessionEvent::run_cancellation_requested(self.ids.event_id(), &snapshot.run),
                    SessionEvent::run_state_changed(self.ids.event_id(), &run),
                ];
                (run, None, None, events)
            }
            RunState::Running => {
                let run = snapshot.run.transition(RunState::Cancelling)?;
                let events = vec![
                    SessionEvent::run_cancellation_requested(self.ids.event_id(), &snapshot.run),
                    SessionEvent::run_state_changed(self.ids.event_id(), &run),
                ];
                (run, None, None, events)
            }
            RunState::WaitingForApproval => {
                let approval = snapshot
                    .approvals()
                    .iter()
                    .find(|approval| approval.state() == ApprovalState::Pending)
                    .ok_or(RunError::InvalidTransition)?
                    .decide(ApprovalState::Rejected)?;
                let tool_call = snapshot
                    .tool_call(approval.tool_call_id())
                    .ok_or(RunError::RunNotFound)?
                    .transition(ToolCallState::Denied)?;
                let cancelling = snapshot.run.transition(RunState::Cancelling)?;
                let run = if descendants_terminal && !snapshot.has_active_invocation() {
                    cancelling.transition(RunState::Cancelled)?
                } else {
                    cancelling.clone()
                };
                let mut events = vec![
                    SessionEvent::run_cancellation_requested(self.ids.event_id(), &snapshot.run),
                    SessionEvent::run_state_changed(self.ids.event_id(), &cancelling),
                    SessionEvent::approval_decided(
                        self.ids.event_id(),
                        snapshot.run.session_id.clone(),
                        approval.clone(),
                    ),
                    SessionEvent::tool_call_denied(
                        self.ids.event_id(),
                        snapshot.run.session_id.clone(),
                        tool_call.clone(),
                    ),
                ];
                if run.state() == RunState::Cancelled {
                    events.push(SessionEvent::run_state_changed(self.ids.event_id(), &run));
                }
                (run, Some(tool_call), Some(approval), events)
            }
            RunState::Cancelling if descendants_terminal && own_work_terminal => {
                let run = snapshot.run.transition(RunState::Cancelled)?;
                let event = SessionEvent::run_state_changed(self.ids.event_id(), &run);
                (run, None, None, vec![event])
            }
            RunState::Cancelling | RunState::Completed | RunState::Failed | RunState::Cancelled => {
                (snapshot.run.clone(), None, None, Vec::new())
            }
        };
        let cancelled_inputs =
            if run.state() == RunState::Cancelled && snapshot.run.state() != RunState::Cancelled {
                self.cancelled_run_inputs(run.run_id()).await?
            } else {
                Vec::new()
            };
        events.extend(
            cancelled_inputs
                .iter()
                .map(|delivery| SessionEvent::run_input_cancelled(self.ids.event_id(), delivery)),
        );
        self.store
            .request_cancellation(
                &run,
                tool_call.as_ref(),
                approval.as_ref(),
                &cancelled_inputs,
                &events,
            )
            .await
            .map_err(map_run_store_error)
    }

    async fn cancelled_run_inputs(&self, run_id: &RunId) -> Result<Vec<MessageDelivery>, RunError> {
        self.store
            .list_queued_run_inputs(run_id)
            .await
            .map_err(map_run_store_error)?
            .into_iter()
            .map(|delivery| {
                delivery
                    .with_state(MessageDeliveryState::Cancelled)
                    .map_err(|_| RunError::InvalidMessageDelivery)
            })
            .collect()
    }

    pub async fn get_run(&self, run_id: RunId) -> Result<RunSnapshot, RunError> {
        self.store
            .get_run(&run_id)
            .await
            .map_err(|_| RunError::RunStoreUnavailable)?
            .ok_or(RunError::RunNotFound)
    }

    pub async fn begin_execution(
        &self,
        run_id: RunId,
    ) -> Result<RunMutation<RunSnapshot>, RunError> {
        let snapshot = self.get_run(run_id.clone()).await?;
        if !snapshot.tool_calls.is_empty() {
            return Err(RunError::InvalidTransition);
        }
        let requested_scope = snapshot
            .run
            .requested_scope()
            .cloned()
            .ok_or(RunError::InvalidTransition)?;
        let approval_policy = snapshot
            .run
            .approval_policy()
            .ok_or(RunError::InvalidTransition)?;
        let requested_tool_call = ToolCall::new(
            self.ids.tool_call_id(),
            run_id,
            DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
            requested_scope.clone(),
        );
        let (run, tool_call, approval, events) = match approval_policy {
            ApprovalPolicy::Ask => {
                let running = snapshot.run.transition(RunState::Running)?;
                let run = running.transition(RunState::WaitingForApproval)?;
                let tool_call = requested_tool_call.transition(ToolCallState::AwaitingApproval)?;
                let approval = Approval::new(
                    self.ids.approval_id(),
                    run.run_id().clone(),
                    tool_call.tool_call_id().clone(),
                    requested_scope.clone(),
                );
                let events = vec![
                    SessionEvent::run_state_changed(self.ids.event_id(), &running),
                    SessionEvent::tool_call_requested(
                        self.ids.event_id(),
                        run.session_id.clone(),
                        requested_tool_call.clone(),
                    ),
                    SessionEvent::tool_call_state_changed(
                        self.ids.event_id(),
                        run.session_id.clone(),
                        tool_call.clone(),
                    ),
                    SessionEvent::approval_requested(
                        self.ids.event_id(),
                        run.session_id.clone(),
                        approval.clone(),
                    ),
                    SessionEvent::run_state_changed(self.ids.event_id(), &run),
                ];
                (run, tool_call, Some(approval), events)
            }
            ApprovalPolicy::FullAccess => {
                let run = snapshot.run.transition(RunState::Running)?;
                let tool_call = requested_tool_call.with_effective_scope(requested_scope)?;
                let events = vec![
                    SessionEvent::run_state_changed(self.ids.event_id(), &run),
                    SessionEvent::tool_call_requested(
                        self.ids.event_id(),
                        run.session_id.clone(),
                        requested_tool_call.clone(),
                    ),
                    SessionEvent::tool_call_state_changed(
                        self.ids.event_id(),
                        run.session_id.clone(),
                        tool_call.clone(),
                    ),
                ];
                (run, tool_call, None, events)
            }
            ApprovalPolicy::ReadOnly => {
                let run = snapshot.run.transition(RunState::Running)?;
                let tool_call = requested_tool_call.transition(ToolCallState::Denied)?;
                let events = vec![
                    SessionEvent::run_state_changed(self.ids.event_id(), &run),
                    SessionEvent::tool_call_requested(
                        self.ids.event_id(),
                        run.session_id.clone(),
                        requested_tool_call.clone(),
                    ),
                    SessionEvent::tool_call_denied(
                        self.ids.event_id(),
                        run.session_id.clone(),
                        tool_call.clone(),
                    ),
                ];
                (run, tool_call, None, events)
            }
        };
        self.store
            .begin_execution(&run, &tool_call, approval.as_ref(), &events)
            .await
            .map_err(map_run_store_error)
    }

    pub async fn begin_tool_call(
        &self,
        tool_call_id: ToolCallId,
    ) -> Result<RunMutation<ToolCall>, RunError> {
        let snapshot = self.get_run_for_tool_call(&tool_call_id).await?;
        let tool_call = snapshot
            .tool_call(&tool_call_id)
            .ok_or(RunError::RunNotFound)?;
        let running = tool_call.transition(ToolCallState::Running)?;
        let event = SessionEvent::tool_call_state_changed(
            self.ids.event_id(),
            snapshot.run.session_id.clone(),
            running,
        );
        self.store
            .begin_tool_call(&tool_call_id, std::slice::from_ref(&event))
            .await
            .map_err(map_run_store_error)
    }

    pub async fn decide_approval(
        &self,
        approval_id: ApprovalId,
        decision: ApprovalState,
        idempotency_key: String,
    ) -> Result<ApprovalDecisionMutation, RunError> {
        if idempotency_key.is_empty() {
            return Err(RunError::IdempotencyKeyRequired);
        }
        if decision == ApprovalState::Pending {
            return Err(RunError::InvalidTransition);
        }
        if let Some((stored_decision, snapshot)) = self
            .store
            .get_approval_decision(&approval_id, &idempotency_key)
            .await
            .map_err(map_run_store_error)?
        {
            if stored_decision != decision {
                return Err(RunError::IdempotencyConflict);
            }
            return Ok(ApprovalDecisionMutation::new(
                snapshot,
                Vec::new(),
                ApprovalDecisionDisposition::Duplicate,
            ));
        }
        let (run, _) = self
            .store
            .get_approval(&approval_id)
            .await
            .map_err(map_run_store_error)?
            .ok_or(RunError::ApprovalNotFound)?;
        let snapshot = self.get_run(run.run_id().clone()).await?;
        let current_approval = snapshot
            .approval(&approval_id)
            .ok_or(RunError::ApprovalNotFound)?;
        if current_approval.state() != ApprovalState::Pending {
            return Err(RunError::ApprovalAlreadyDecided);
        }
        let current_tool_call = snapshot
            .tool_call(current_approval.tool_call_id())
            .ok_or(RunError::RunNotFound)?;
        let approval = current_approval.decide(decision)?;
        let (tool_call, run) = match decision {
            ApprovalState::Approved => (
                current_tool_call.with_effective_scope(approval.scope().clone())?,
                snapshot.run.transition(RunState::Running)?,
            ),
            ApprovalState::Rejected => (
                current_tool_call.transition(ToolCallState::Denied)?,
                snapshot.run.transition(RunState::Running)?,
            ),
            ApprovalState::Pending => return Err(RunError::InvalidTransition),
        };
        let events = vec![
            SessionEvent::approval_decided(
                self.ids.event_id(),
                run.session_id().clone(),
                approval.clone(),
            ),
            match decision {
                ApprovalState::Approved => SessionEvent::tool_call_state_changed(
                    self.ids.event_id(),
                    run.session_id().clone(),
                    tool_call.clone(),
                ),
                ApprovalState::Rejected => SessionEvent::tool_call_denied(
                    self.ids.event_id(),
                    run.session_id().clone(),
                    tool_call.clone(),
                ),
                ApprovalState::Pending => unreachable!(),
            },
            SessionEvent::run_state_changed(self.ids.event_id(), &run),
        ];
        self.store
            .decide_approval(&run, &tool_call, &approval, &idempotency_key, &events)
            .await
            .map_err(map_run_store_error)
    }

    pub async fn finish_denied_execution(
        &self,
        run_id: RunId,
        tool_call_id: ToolCallId,
    ) -> Result<RunMutation<RunSnapshot>, RunError> {
        let snapshot = self.get_run(run_id.clone()).await?;
        if snapshot.tool_calls().len() != 1 || snapshot.run().state() != RunState::Running {
            return Err(RunError::InvalidTransition);
        }
        let tool_call = snapshot
            .tool_call(&tool_call_id)
            .ok_or(RunError::RunNotFound)?;
        if tool_call.run_id() != &run_id || tool_call.state() != ToolCallState::Denied {
            return Err(RunError::InvalidTransition);
        }
        let run = snapshot.run().transition(RunState::Failed)?;
        let events = vec![SessionEvent::run_state_changed(self.ids.event_id(), &run)];
        self.store
            .finish_denied_execution(&run, tool_call, &events)
            .await
            .map_err(map_run_store_error)
    }

    pub async fn finish_execution(
        &self,
        run_id: RunId,
        tool_call_id: ToolCallId,
        output: SubprocessOutput,
    ) -> Result<RunMutation<RunSnapshot>, RunError> {
        let snapshot = self.get_run(run_id.clone()).await?;
        let tool_call = snapshot
            .tool_call(&tool_call_id)
            .ok_or(RunError::RunNotFound)?;
        if tool_call.run_id != run_id {
            return Err(RunError::InvalidTransition);
        }
        let succeeded = output.succeeded();
        let result_state = if succeeded {
            ToolCallState::Completed
        } else {
            ToolCallState::Failed
        };
        let result = ToolCallResult::from_subprocess(result_state, output)?;
        let terminal_tool_call = tool_call.with_result(&result)?;
        let terminal_run = snapshot.run.transition(if succeeded {
            RunState::Completed
        } else {
            RunState::Failed
        })?;
        let mut events = Vec::new();
        if let Some(stdout) = result.stdout.as_ref().filter(|stdout| !stdout.is_empty()) {
            events.push(SessionEvent::tool_call_output(
                self.ids.event_id(),
                snapshot.run.session_id.clone(),
                run_id.clone(),
                tool_call_id.clone(),
                ToolOutputStream::Stdout,
                stdout.clone(),
            ));
        }
        if let Some(artifact) = &result.stdout_artifact {
            events.push(SessionEvent::artifact_registered(
                self.ids.event_id(),
                snapshot.run.session_id.clone(),
                run_id.clone(),
                tool_call_id.clone(),
                ToolOutputStream::Stdout,
                artifact.clone(),
            ));
        }
        if let Some(stderr) = result.stderr.as_ref().filter(|stderr| !stderr.is_empty()) {
            events.push(SessionEvent::tool_call_output(
                self.ids.event_id(),
                snapshot.run.session_id.clone(),
                run_id.clone(),
                tool_call_id.clone(),
                ToolOutputStream::Stderr,
                stderr.clone(),
            ));
        }
        if let Some(artifact) = &result.stderr_artifact {
            events.push(SessionEvent::artifact_registered(
                self.ids.event_id(),
                snapshot.run.session_id.clone(),
                run_id.clone(),
                tool_call_id.clone(),
                ToolOutputStream::Stderr,
                artifact.clone(),
            ));
        }
        events.push(SessionEvent::tool_call_state_changed(
            self.ids.event_id(),
            snapshot.run.session_id.clone(),
            terminal_tool_call.clone(),
        ));
        events.push(SessionEvent::run_state_changed(
            self.ids.event_id(),
            &terminal_run,
        ));
        self.store
            .finish_execution(&terminal_run, &terminal_tool_call, &events)
            .await
            .map_err(map_run_store_error)
    }

    pub async fn finish_cancellation(
        &self,
        run_id: RunId,
        tool_call_id: ToolCallId,
        output: SubprocessOutput,
    ) -> Result<RunMutation<RunSnapshot>, RunError> {
        let subtree = self.list_run_subtree(run_id.clone()).await?;
        let snapshot = subtree.first().ok_or(RunError::RunNotFound)?;
        if snapshot.run.state() != RunState::Cancelling {
            return Err(RunError::InvalidTransition);
        }
        if subtree
            .iter()
            .skip(1)
            .any(|descendant| !descendant.run().state().is_terminal())
        {
            return Err(RunError::InvalidTransition);
        }
        let tool_call = snapshot
            .tool_call(&tool_call_id)
            .ok_or(RunError::RunNotFound)?;
        if tool_call.run_id != run_id
            || !matches!(
                tool_call.state(),
                ToolCallState::Requested | ToolCallState::Ready | ToolCallState::Running
            )
        {
            return Err(RunError::InvalidTransition);
        }
        let cancelled_tool_call = ToolCall {
            state: ToolCallState::Cancelled,
            stdout: output
                .stdout_artifact
                .is_none()
                .then_some(output.stdout.clone()),
            stderr: output
                .stderr_artifact
                .is_none()
                .then_some(output.stderr.clone()),
            stdout_artifact: output.stdout_artifact.clone(),
            stderr_artifact: output.stderr_artifact.clone(),
            exit_code: output.exit_code,
            ..tool_call.clone()
        };
        let cancelled_run = snapshot.run.transition(RunState::Cancelled)?;
        let mut events = Vec::new();
        if output.stdout_artifact.is_none() && !output.stdout.is_empty() {
            events.push(SessionEvent::tool_call_output(
                self.ids.event_id(),
                snapshot.run.session_id.clone(),
                run_id.clone(),
                tool_call_id.clone(),
                ToolOutputStream::Stdout,
                output.stdout,
            ));
        }
        if let Some(artifact) = output.stdout_artifact {
            events.push(SessionEvent::artifact_registered(
                self.ids.event_id(),
                snapshot.run.session_id.clone(),
                run_id.clone(),
                tool_call_id.clone(),
                ToolOutputStream::Stdout,
                artifact,
            ));
        }
        if output.stderr_artifact.is_none() && !output.stderr.is_empty() {
            events.push(SessionEvent::tool_call_output(
                self.ids.event_id(),
                snapshot.run.session_id.clone(),
                run_id.clone(),
                tool_call_id.clone(),
                ToolOutputStream::Stderr,
                output.stderr,
            ));
        }
        if let Some(artifact) = output.stderr_artifact {
            events.push(SessionEvent::artifact_registered(
                self.ids.event_id(),
                snapshot.run.session_id.clone(),
                run_id.clone(),
                tool_call_id.clone(),
                ToolOutputStream::Stderr,
                artifact,
            ));
        }
        events.push(SessionEvent::tool_call_state_changed(
            self.ids.event_id(),
            snapshot.run.session_id.clone(),
            cancelled_tool_call.clone(),
        ));
        events.push(SessionEvent::run_state_changed(
            self.ids.event_id(),
            &cancelled_run,
        ));
        let cancelled_inputs = self.cancelled_run_inputs(&run_id).await?;
        events.extend(
            cancelled_inputs
                .iter()
                .map(|delivery| SessionEvent::run_input_cancelled(self.ids.event_id(), delivery)),
        );
        self.store
            .finish_cancellation(
                &cancelled_run,
                &cancelled_tool_call,
                &cancelled_inputs,
                &events,
            )
            .await
            .map_err(map_run_store_error)
    }

    async fn get_run_for_tool_call(
        &self,
        tool_call_id: &ToolCallId,
    ) -> Result<RunSnapshot, RunError> {
        let (run, _) = self
            .store
            .get_tool_call(tool_call_id)
            .await
            .map_err(|_| RunError::RunStoreUnavailable)?
            .ok_or(RunError::RunNotFound)?;
        self.get_run(run.run_id.clone()).await
    }
}

fn run_subtree(runs: Vec<RunSnapshot>, root_run_id: &RunId) -> Vec<RunSnapshot> {
    // ponytail: This scans one Session in memory; replace it with an indexed descendant query if
    // Session Run counts make cancellation planning expensive.
    let mut included = vec![root_run_id.clone()];
    let mut subtree = runs
        .iter()
        .find(|run| run.run().run_id() == root_run_id)
        .cloned()
        .into_iter()
        .collect::<Vec<_>>();
    loop {
        let mut changed = false;
        for run in &runs {
            if !included.contains(run.run().run_id())
                && run
                    .run()
                    .parent_run_id()
                    .is_some_and(|parent| included.contains(parent))
            {
                included.push(run.run().run_id().clone());
                subtree.push(run.clone());
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    subtree
}

fn map_run_store_error(error: RunStoreError) -> RunError {
    match error {
        RunStoreError::InvalidChildActivity => RunError::InvalidChildActivity,
        RunStoreError::ActiveRootRunExists => RunError::ActiveRootRunExists,
        RunStoreError::RunNotFound => RunError::RunNotFound,
        RunStoreError::ParentRunNotFound => RunError::ParentRunNotFound,
        RunStoreError::ParentRunTerminal => RunError::ParentRunTerminal,
        RunStoreError::TaskNotFound => RunError::TaskNotFound,
        RunStoreError::TaskLinkOutsideSession => RunError::TaskLinkOutsideSession,
        RunStoreError::InvalidTaskAssignment => RunError::InvalidTaskAssignment,
        RunStoreError::RunInputReadOnly => RunError::RunInputReadOnly,
        RunStoreError::RunNotAcceptingInput => RunError::RunNotAcceptingInput,
        RunStoreError::MessageDeliveryNotFound => RunError::MessageDeliveryNotFound,
        RunStoreError::InvalidMessageDelivery => RunError::InvalidMessageDelivery,
        RunStoreError::MessageDeliveryOutOfOrder => RunError::MessageDeliveryOutOfOrder,
        RunStoreError::IdempotencyKeyRequired => RunError::IdempotencyKeyRequired,
        RunStoreError::InvalidTransition => RunError::InvalidTransition,
        RunStoreError::WorkspaceRootNotFound => RunError::WorkspaceRootNotFound,
        RunStoreError::PathOutsideWorkspaceRoot => RunError::PathOutsideWorkspaceRoot,
        RunStoreError::ApprovalNotFound => RunError::ApprovalNotFound,
        RunStoreError::ApprovalAlreadyDecided => RunError::ApprovalAlreadyDecided,
        RunStoreError::IdempotencyConflict => RunError::IdempotencyConflict,
        RunStoreError::Unavailable => RunError::RunStoreUnavailable,
    }
}

pub trait SessionOperations: Send + Sync {
    fn create_session(
        &self,
        workspace_id: WorkspaceId,
    ) -> impl Future<Output = Result<Session, SessionError>> + Send;
    fn get_session(
        &self,
        session_id: SessionId,
    ) -> impl Future<Output = Result<Session, SessionError>> + Send;
    fn list_sessions(
        &self,
        workspace_id: WorkspaceId,
    ) -> impl Future<Output = Result<Vec<Session>, SessionError>> + Send;
    fn append_message(
        &self,
        command: AppendMessage,
    ) -> impl Future<Output = Result<Message, SessionError>> + Send;
    fn list_session_events(
        &self,
        session_id: SessionId,
        after: EventCursor,
    ) -> impl Future<Output = Result<SessionEventPage, SessionError>> + Send;
    fn list_events_after(
        &self,
        after: EventCursor,
    ) -> impl Future<Output = Result<SessionEventPage, SessionError>> + Send;
    fn current_event_cursor(
        &self,
    ) -> impl Future<Output = Result<Option<EventCursor>, SessionError>> + Send;
}

pub trait TaskOperations: Send + Sync {
    fn create_task(
        &self,
        command: CreateTask,
    ) -> impl Future<Output = Result<CreateTaskMutation, TaskError>> + Send;
    fn get_task(&self, task_id: TaskId) -> impl Future<Output = Result<Task, TaskError>> + Send;
    fn update_task(
        &self,
        command: UpdateTask,
    ) -> impl Future<Output = Result<TaskMutation, TaskError>> + Send;
    fn transition_task(
        &self,
        command: TransitionTask,
    ) -> impl Future<Output = Result<TaskMutation, TaskError>> + Send;
    fn assign_task(
        &self,
        command: AssignTask,
    ) -> impl Future<Output = Result<TaskMutation, TaskError>> + Send;
}

pub struct SessionApplication<W, S, I> {
    workspace_store: W,
    session_store: S,
    ids: I,
}

impl<W, S, I> SessionApplication<W, S, I> {
    pub fn new(workspace_store: W, session_store: S, ids: I) -> Self {
        Self {
            workspace_store,
            session_store,
            ids,
        }
    }
}

impl<W, S, I> SessionApplication<W, S, I>
where
    W: WorkspaceStore,
    S: SessionStore,
    I: SessionIdGenerator,
{
    pub async fn create_session(&self, workspace_id: WorkspaceId) -> Result<Session, SessionError> {
        let workspace = self
            .workspace_store
            .get_workspace(&workspace_id)
            .await
            .map_err(|_| SessionError::WorkspaceStoreUnavailable)?;
        let workspace = workspace.ok_or(SessionError::WorkspaceNotFound)?;
        let root = workspace
            .roots()
            .first()
            .ok_or(SessionError::WorkspaceRootNotFound)?;
        let scope = WorkspacePathScope::new(root.id().clone(), ".")
            .map_err(|_| SessionError::WorkspaceRootNotFound)?;
        let checkout = WorkspaceCheckout::new(workspace_id.clone(), root, scope)
            .map_err(|_| SessionError::WorkspaceRootNotFound)?;
        let session = Session::with_checkout(self.ids.session_id(), checkout);
        let event =
            SessionEvent::session_created(self.ids.event_id(), session.id.clone(), workspace_id);
        self.session_store
            .create_session(&session, &event)
            .await
            .map_err(|_| SessionError::SessionStoreUnavailable)?;
        Ok(session)
    }

    pub async fn get_session(&self, session_id: SessionId) -> Result<Session, SessionError> {
        self.session_store
            .get_session(&session_id)
            .await
            .map_err(|_| SessionError::SessionStoreUnavailable)?
            .ok_or(SessionError::SessionNotFound)
    }

    pub async fn list_sessions(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<Session>, SessionError> {
        let workspace = self
            .workspace_store
            .get_workspace(&workspace_id)
            .await
            .map_err(|_| SessionError::WorkspaceStoreUnavailable)?;
        if workspace.is_none() {
            return Err(SessionError::WorkspaceNotFound);
        }
        self.session_store
            .list_sessions(&workspace_id)
            .await
            .map_err(|_| SessionError::SessionStoreUnavailable)
    }

    pub async fn append_message(&self, command: AppendMessage) -> Result<Message, SessionError> {
        if command.idempotency_key.is_empty() {
            return Err(SessionError::IdempotencyKeyRequired);
        }
        self.get_session(command.session_id.clone()).await?;
        let message = Message::new_with_attachments(
            self.ids.message_id(),
            command.session_id,
            MessageRole::User,
            command.content,
            command.attachments,
        )?;
        let event = SessionEvent::message_appended(self.ids.event_id(), message.clone());
        self.session_store
            .append_message(&message, &event, &command.idempotency_key)
            .await
            .map_err(|error| match error {
                StoreError::IdempotencyConflict => SessionError::IdempotencyConflict,
                StoreError::IdempotencyKeyRequired => SessionError::IdempotencyKeyRequired,
                _ => SessionError::SessionStoreUnavailable,
            })
    }

    pub async fn list_session_events(
        &self,
        session_id: SessionId,
        after: EventCursor,
    ) -> Result<SessionEventPage, SessionError> {
        self.get_session(session_id.clone()).await?;
        self.session_store
            .list_session_events(&session_id, after)
            .await
            .map_err(|_| SessionError::SessionStoreUnavailable)
    }

    pub async fn list_events_after(
        &self,
        after: EventCursor,
    ) -> Result<SessionEventPage, SessionError> {
        self.session_store
            .list_events_after(after)
            .await
            .map_err(|_| SessionError::SessionStoreUnavailable)
    }

    pub async fn current_event_cursor(&self) -> Result<Option<EventCursor>, SessionError> {
        self.session_store
            .current_event_cursor()
            .await
            .map_err(|_| SessionError::SessionStoreUnavailable)
    }
}

impl<W, S, I> SessionOperations for SessionApplication<W, S, I>
where
    W: WorkspaceStore,
    S: SessionStore,
    I: SessionIdGenerator,
{
    async fn create_session(&self, workspace_id: WorkspaceId) -> Result<Session, SessionError> {
        SessionApplication::create_session(self, workspace_id).await
    }

    async fn get_session(&self, session_id: SessionId) -> Result<Session, SessionError> {
        SessionApplication::get_session(self, session_id).await
    }

    async fn list_sessions(&self, workspace_id: WorkspaceId) -> Result<Vec<Session>, SessionError> {
        SessionApplication::list_sessions(self, workspace_id).await
    }

    async fn append_message(&self, command: AppendMessage) -> Result<Message, SessionError> {
        SessionApplication::append_message(self, command).await
    }

    async fn list_session_events(
        &self,
        session_id: SessionId,
        after: EventCursor,
    ) -> Result<SessionEventPage, SessionError> {
        SessionApplication::list_session_events(self, session_id, after).await
    }

    async fn list_events_after(
        &self,
        after: EventCursor,
    ) -> Result<SessionEventPage, SessionError> {
        SessionApplication::list_events_after(self, after).await
    }

    async fn current_event_cursor(&self) -> Result<Option<EventCursor>, SessionError> {
        SessionApplication::current_event_cursor(self).await
    }
}

impl<W, S, I> SessionApplication<W, S, I>
where
    S: SessionStore + TaskStore,
    I: TaskIdGenerator,
{
    pub async fn create_task(&self, command: CreateTask) -> Result<CreateTaskMutation, TaskError> {
        if command.idempotency_key.is_empty() {
            return Err(TaskError::IdempotencyKeyRequired);
        }
        if self
            .session_store
            .get_session(&command.session_id)
            .await
            .map_err(|_| TaskError::TaskStoreUnavailable)?
            .is_none()
        {
            return Err(TaskError::SessionNotFound);
        }
        let task = Task::new(
            self.ids.task_id(),
            command.session_id,
            command.objective,
            command.parent_task_id,
            command.dependency_task_ids,
        )?;
        let event = SessionEvent::task_created(self.ids.event_id(), task.clone());
        self.session_store
            .create_task(&task, &event, &command.idempotency_key)
            .await
            .map_err(task_store_error)
    }

    pub async fn get_task(&self, task_id: TaskId) -> Result<Task, TaskError> {
        self.session_store
            .get_task(&task_id)
            .await
            .map_err(|_| TaskError::TaskStoreUnavailable)?
            .ok_or(TaskError::TaskNotFound)
    }

    pub async fn update_task(&self, command: UpdateTask) -> Result<TaskMutation, TaskError> {
        if command.idempotency_key.is_empty() {
            return Err(TaskError::IdempotencyKeyRequired);
        }
        self.session_store
            .update_task(
                &command.validate()?,
                [self.ids.event_id(), self.ids.event_id()],
            )
            .await
            .map_err(task_store_error)
    }

    pub async fn transition_task(
        &self,
        command: TransitionTask,
    ) -> Result<TaskMutation, TaskError> {
        if command.idempotency_key.is_empty() {
            return Err(TaskError::IdempotencyKeyRequired);
        }
        self.session_store
            .transition_task(&command, self.ids.event_id())
            .await
            .map_err(task_store_error)
    }

    pub async fn assign_task(&self, command: AssignTask) -> Result<TaskMutation, TaskError> {
        if command.idempotency_key.is_empty() {
            return Err(TaskError::IdempotencyKeyRequired);
        }
        self.session_store
            .assign_task(&command, self.ids.event_id())
            .await
            .map_err(task_store_error)
    }
}

impl<W, S, I> TaskOperations for SessionApplication<W, S, I>
where
    W: Send + Sync,
    S: SessionStore + TaskStore,
    I: TaskIdGenerator,
{
    async fn create_task(&self, command: CreateTask) -> Result<CreateTaskMutation, TaskError> {
        SessionApplication::create_task(self, command).await
    }

    async fn get_task(&self, task_id: TaskId) -> Result<Task, TaskError> {
        SessionApplication::get_task(self, task_id).await
    }

    async fn update_task(&self, command: UpdateTask) -> Result<TaskMutation, TaskError> {
        SessionApplication::update_task(self, command).await
    }

    async fn transition_task(&self, command: TransitionTask) -> Result<TaskMutation, TaskError> {
        SessionApplication::transition_task(self, command).await
    }

    async fn assign_task(&self, command: AssignTask) -> Result<TaskMutation, TaskError> {
        SessionApplication::assign_task(self, command).await
    }
}

fn task_store_error(error: TaskStoreError) -> TaskError {
    match error {
        TaskStoreError::TaskNotFound => TaskError::TaskNotFound,
        TaskStoreError::ParentTaskNotFound => TaskError::ParentTaskNotFound,
        TaskStoreError::DependencyTaskNotFound => TaskError::DependencyTaskNotFound,
        TaskStoreError::RunNotFound => TaskError::RunNotFound,
        TaskStoreError::TaskLinkOutsideSession => TaskError::TaskLinkOutsideSession,
        TaskStoreError::IdempotencyKeyRequired => TaskError::IdempotencyKeyRequired,
        TaskStoreError::IdempotencyConflict => TaskError::IdempotencyConflict,
        TaskStoreError::Cycle => TaskError::Cycle,
        TaskStoreError::InvalidTransition => TaskError::InvalidTransition,
        TaskStoreError::InvalidAssignment => TaskError::InvalidAssignment,
        TaskStoreError::InvalidTask | TaskStoreError::Unavailable => {
            TaskError::TaskStoreUnavailable
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceRootState {
    Available,
}

impl WorkspaceRootState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceRootInput {
    pub name: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FilesystemIdentity(String);

impl FilesystemIdentity {
    pub fn new(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        (!value.is_empty() && value.trim() == value).then_some(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredWorkspaceRoot {
    pub canonical_path: String,
    pub git_common_directory_path: String,
    pub filesystem_identity: FilesystemIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceRoot {
    id: WorkspaceRootId,
    name: String,
    display_path: String,
    canonical_path: String,
    git_common_directory_path: String,
    filesystem_identity: FilesystemIdentity,
    position: usize,
    state: WorkspaceRootState,
}

impl WorkspaceRoot {
    pub fn new(
        id: WorkspaceRootId,
        name: String,
        display_path: String,
        discovered: DiscoveredWorkspaceRoot,
        position: usize,
        state: WorkspaceRootState,
    ) -> Result<Self, WorkspaceError> {
        if name.trim().is_empty() {
            return Err(WorkspaceError::WorkspaceRootNameRequired);
        }
        if display_path.is_empty() {
            return Err(WorkspaceError::WorkspaceRootMissing);
        }
        if discovered.canonical_path.is_empty() || discovered.git_common_directory_path.is_empty() {
            return Err(WorkspaceError::WorkspaceRootNotGitRepository);
        }
        Ok(Self {
            id,
            name,
            display_path,
            canonical_path: discovered.canonical_path,
            git_common_directory_path: discovered.git_common_directory_path,
            filesystem_identity: discovered.filesystem_identity,
            position,
            state,
        })
    }

    pub fn id(&self) -> &WorkspaceRootId {
        &self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn display_path(&self) -> &str {
        &self.display_path
    }

    pub fn canonical_path(&self) -> &str {
        &self.canonical_path
    }

    pub fn git_common_directory_path(&self) -> &str {
        &self.git_common_directory_path
    }

    pub fn filesystem_identity(&self) -> &FilesystemIdentity {
        &self.filesystem_identity
    }

    pub fn position(&self) -> usize {
        self.position
    }

    pub fn state(&self) -> WorkspaceRootState {
        self.state
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workspace {
    id: WorkspaceId,
    name: String,
    roots: Vec<WorkspaceRoot>,
}

impl Workspace {
    pub fn new(
        id: WorkspaceId,
        name: String,
        roots: Vec<WorkspaceRoot>,
    ) -> Result<Self, WorkspaceError> {
        validate_workspace_name(&name)?;
        if roots.is_empty() {
            return Err(WorkspaceError::WorkspaceRootRequired);
        }

        let mut root_names = Vec::with_capacity(roots.len());
        let mut installations = Vec::with_capacity(roots.len());
        let mut root_ids = Vec::with_capacity(roots.len());
        for (expected_position, root) in roots.iter().enumerate() {
            if root.position != expected_position {
                return Err(WorkspaceError::WorkspaceRootOrderInvalid);
            }
            if root.name.trim().is_empty() {
                return Err(WorkspaceError::WorkspaceRootNameRequired);
            }
            if root_ids.contains(&root.id) {
                return Err(WorkspaceError::WorkspaceRootIdentityConflict);
            }
            if root_names.iter().any(|name| name == &root.name) {
                return Err(WorkspaceError::WorkspaceRootNameConflict);
            }
            if installations
                .iter()
                .any(|path| path == &root.git_common_directory_path)
            {
                return Err(WorkspaceError::WorkspaceRootDuplicate);
            }
            root_ids.push(root.id.clone());
            root_names.push(root.name.as_str());
            installations.push(root.git_common_directory_path.as_str());
        }
        Ok(Self { id, name, roots })
    }

    pub fn id(&self) -> &WorkspaceId {
        &self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn roots(&self) -> &[WorkspaceRoot] {
        &self.roots
    }

    pub fn root(&self, id: &WorkspaceRootId) -> Option<&WorkspaceRoot> {
        self.roots.iter().find(|root| root.id() == id)
    }

    pub fn checkout(&self, scope: WorkspacePathScope) -> Result<WorkspaceCheckout, WorkspaceError> {
        let root = self
            .root(scope.workspace_root_id())
            .ok_or(WorkspaceError::WorkspaceRootNotFound)?;
        WorkspaceCheckout::new(self.id.clone(), root, scope)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateWorkspace {
    pub name: String,
    pub roots: Vec<WorkspaceRootInput>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootDiscoveryError {
    Missing,
    NotDirectory,
    NotGitRepository,
    GitUnavailable,
    ChangeNotFound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreError {
    IdempotencyKeyRequired,
    IdempotencyConflict,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceError {
    WorkspaceNameRequired,
    WorkspaceRootRequired,
    WorkspaceRootNameRequired,
    WorkspaceRootNameConflict,
    WorkspaceRootIdentityConflict,
    WorkspaceRootOrderInvalid,
    WorkspaceRootMissing,
    WorkspaceRootNotDirectory,
    WorkspaceRootNotGitRepository,
    WorkspaceRootDuplicate,
    WorkspaceRootNotFound,
    PathOutsideWorkspaceRoot,
    WorkspaceNotFound,
    GitUnavailable,
    WorkspaceStoreUnavailable,
    ChangeNotFound,
}

pub trait WorkspaceRootDiscovery: Send + Sync {
    fn discover(
        &self,
        path: &Path,
    ) -> impl Future<Output = Result<DiscoveredWorkspaceRoot, RootDiscoveryError>> + Send;

    fn summarize_changes(
        &self,
        checkout: &WorkspaceCheckout,
    ) -> impl Future<Output = Result<WorkspaceChangeSummary, RootDiscoveryError>> + Send {
        let _ = checkout;
        async { Err(RootDiscoveryError::GitUnavailable) }
    }

    fn diff_change(
        &self,
        checkout: &WorkspaceCheckout,
        path: &WorkspaceChangePath,
    ) -> impl Future<Output = Result<WorkspaceChangeDiff, RootDiscoveryError>> + Send {
        let _ = (checkout, path);
        async { Err(RootDiscoveryError::GitUnavailable) }
    }
}

pub trait WorkspaceStore: Send + Sync {
    fn create_workspace(
        &self,
        workspace: &Workspace,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
    fn get_workspace(
        &self,
        id: &WorkspaceId,
    ) -> impl Future<Output = Result<Option<Workspace>, StoreError>> + Send;
    fn list_workspaces(&self) -> impl Future<Output = Result<Vec<Workspace>, StoreError>> + Send;
}

pub trait WorkspaceIdGenerator: Send + Sync {
    fn workspace_id(&self) -> WorkspaceId;
    fn workspace_root_id(&self) -> WorkspaceRootId;
}

pub trait WorkspaceOperations: Send + Sync {
    fn create_workspace(
        &self,
        command: CreateWorkspace,
    ) -> impl Future<Output = Result<Workspace, WorkspaceError>> + Send;
    fn get_workspace(
        &self,
        id: WorkspaceId,
    ) -> impl Future<Output = Result<Workspace, WorkspaceError>> + Send;
    fn list_workspaces(
        &self,
    ) -> impl Future<Output = Result<Vec<Workspace>, WorkspaceError>> + Send;
    fn summarize_changes(
        &self,
        checkout: WorkspaceCheckout,
    ) -> impl Future<Output = Result<WorkspaceChangeSummary, WorkspaceError>> + Send;
    fn diff_change(
        &self,
        checkout: WorkspaceCheckout,
        path: WorkspaceChangePath,
    ) -> impl Future<Output = Result<WorkspaceChangeDiff, WorkspaceError>> + Send;
}

pub struct WorkspaceApplication<D, S, I> {
    discovery: D,
    store: S,
    ids: I,
}

impl<D, S, I> WorkspaceApplication<D, S, I> {
    pub fn new(discovery: D, store: S, ids: I) -> Self {
        Self {
            discovery,
            store,
            ids,
        }
    }
}

impl<D, S, I> WorkspaceApplication<D, S, I>
where
    D: WorkspaceRootDiscovery,
    S: WorkspaceStore,
    I: WorkspaceIdGenerator,
{
    pub async fn create_workspace(
        &self,
        command: CreateWorkspace,
    ) -> Result<Workspace, WorkspaceError> {
        validate_workspace_name(&command.name)?;
        if command.roots.is_empty() {
            return Err(WorkspaceError::WorkspaceRootRequired);
        }

        let mut root_names = Vec::with_capacity(command.roots.len());
        for root in &command.roots {
            if root.name.trim().is_empty() {
                return Err(WorkspaceError::WorkspaceRootNameRequired);
            }
            if root_names.iter().any(|name| name == &root.name) {
                return Err(WorkspaceError::WorkspaceRootNameConflict);
            }
            root_names.push(root.name.as_str());
        }

        let mut roots = Vec::with_capacity(command.roots.len());
        for (position, input) in command.roots.into_iter().enumerate() {
            let discovered = self
                .discovery
                .discover(Path::new(&input.path))
                .await
                .map_err(WorkspaceError::from)?;
            roots.push(WorkspaceRoot::new(
                self.ids.workspace_root_id(),
                input.name,
                input.path,
                discovered,
                position,
                WorkspaceRootState::Available,
            )?);
        }
        let workspace = Workspace::new(self.ids.workspace_id(), command.name, roots)?;
        self.store
            .create_workspace(&workspace)
            .await
            .map_err(|_| WorkspaceError::WorkspaceStoreUnavailable)?;
        Ok(workspace)
    }

    pub async fn get_workspace(&self, id: WorkspaceId) -> Result<Workspace, WorkspaceError> {
        self.store
            .get_workspace(&id)
            .await
            .map_err(|_| WorkspaceError::WorkspaceStoreUnavailable)?
            .ok_or(WorkspaceError::WorkspaceNotFound)
    }

    pub async fn list_workspaces(&self) -> Result<Vec<Workspace>, WorkspaceError> {
        self.store
            .list_workspaces()
            .await
            .map_err(|_| WorkspaceError::WorkspaceStoreUnavailable)
    }

    pub async fn summarize_changes(
        &self,
        checkout: WorkspaceCheckout,
    ) -> Result<WorkspaceChangeSummary, WorkspaceError> {
        let workspace = self
            .store
            .get_workspace(checkout.workspace_id())
            .await
            .map_err(|_| WorkspaceError::WorkspaceStoreUnavailable)?
            .ok_or(WorkspaceError::WorkspaceNotFound)?;
        let root = workspace
            .root(checkout.workspace_root_id())
            .ok_or(WorkspaceError::WorkspaceRootNotFound)?;
        if root.canonical_path() != checkout.root_path()
            || root.git_common_directory_path() != checkout.git_common_directory_path()
            || root.filesystem_identity() != checkout.filesystem_identity()
        {
            return Err(WorkspaceError::WorkspaceRootNotFound);
        }
        self.discovery
            .summarize_changes(&checkout)
            .await
            .map_err(WorkspaceError::from)
    }

    pub async fn diff_change(
        &self,
        checkout: WorkspaceCheckout,
        path: WorkspaceChangePath,
    ) -> Result<WorkspaceChangeDiff, WorkspaceError> {
        if !path.is_within(&checkout) {
            return Err(WorkspaceError::PathOutsideWorkspaceRoot);
        }

        let workspace = self
            .store
            .get_workspace(checkout.workspace_id())
            .await
            .map_err(|_| WorkspaceError::WorkspaceStoreUnavailable)?
            .ok_or(WorkspaceError::WorkspaceNotFound)?;
        let root = workspace
            .root(checkout.workspace_root_id())
            .ok_or(WorkspaceError::WorkspaceRootNotFound)?;
        if root.canonical_path() != checkout.root_path()
            || root.git_common_directory_path() != checkout.git_common_directory_path()
            || root.filesystem_identity() != checkout.filesystem_identity()
        {
            return Err(WorkspaceError::WorkspaceRootNotFound);
        }

        self.discovery
            .diff_change(&checkout, &path)
            .await
            .map_err(WorkspaceError::from)
    }
}

impl<D, S, I> WorkspaceOperations for WorkspaceApplication<D, S, I>
where
    D: WorkspaceRootDiscovery,
    S: WorkspaceStore,
    I: WorkspaceIdGenerator,
{
    async fn create_workspace(
        &self,
        command: CreateWorkspace,
    ) -> Result<Workspace, WorkspaceError> {
        WorkspaceApplication::create_workspace(self, command).await
    }

    async fn get_workspace(&self, id: WorkspaceId) -> Result<Workspace, WorkspaceError> {
        WorkspaceApplication::get_workspace(self, id).await
    }

    async fn list_workspaces(&self) -> Result<Vec<Workspace>, WorkspaceError> {
        WorkspaceApplication::list_workspaces(self).await
    }

    async fn summarize_changes(
        &self,
        checkout: WorkspaceCheckout,
    ) -> Result<WorkspaceChangeSummary, WorkspaceError> {
        WorkspaceApplication::summarize_changes(self, checkout).await
    }

    async fn diff_change(
        &self,
        checkout: WorkspaceCheckout,
        path: WorkspaceChangePath,
    ) -> Result<WorkspaceChangeDiff, WorkspaceError> {
        WorkspaceApplication::diff_change(self, checkout, path).await
    }
}

fn validate_workspace_name(name: &str) -> Result<(), WorkspaceError> {
    if name.trim().is_empty() {
        Err(WorkspaceError::WorkspaceNameRequired)
    } else {
        Ok(())
    }
}

impl From<RootDiscoveryError> for WorkspaceError {
    fn from(error: RootDiscoveryError) -> Self {
        match error {
            RootDiscoveryError::Missing => Self::WorkspaceRootMissing,
            RootDiscoveryError::NotDirectory => Self::WorkspaceRootNotDirectory,
            RootDiscoveryError::NotGitRepository => Self::WorkspaceRootNotGitRepository,
            RootDiscoveryError::GitUnavailable => Self::GitUnavailable,
            RootDiscoveryError::ChangeNotFound => Self::ChangeNotFound,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::VecDeque, sync::Mutex};

    #[derive(Default)]
    struct FakeDiscovery {
        results: Mutex<VecDeque<Result<DiscoveredWorkspaceRoot, RootDiscoveryError>>>,
    }

    impl WorkspaceRootDiscovery for FakeDiscovery {
        async fn discover(
            &self,
            _path: &Path,
        ) -> Result<DiscoveredWorkspaceRoot, RootDiscoveryError> {
            self.results.lock().unwrap().pop_front().unwrap()
        }
    }

    struct FakeStore {
        created: std::sync::Arc<Mutex<Vec<Workspace>>>,
    }

    impl WorkspaceStore for FakeStore {
        async fn create_workspace(&self, workspace: &Workspace) -> Result<(), StoreError> {
            self.created.lock().unwrap().push(workspace.clone());
            Ok(())
        }
        async fn get_workspace(&self, _id: &WorkspaceId) -> Result<Option<Workspace>, StoreError> {
            Ok(None)
        }
        async fn list_workspaces(&self) -> Result<Vec<Workspace>, StoreError> {
            Ok(self.created.lock().unwrap().clone())
        }
    }

    struct FakeIds {
        workspace: WorkspaceId,
        roots: Mutex<VecDeque<WorkspaceRootId>>,
    }

    impl WorkspaceIdGenerator for FakeIds {
        fn workspace_id(&self) -> WorkspaceId {
            self.workspace.clone()
        }
        fn workspace_root_id(&self) -> WorkspaceRootId {
            self.roots.lock().unwrap().pop_front().unwrap()
        }
    }

    fn discovered(path: &str) -> DiscoveredWorkspaceRoot {
        DiscoveredWorkspaceRoot {
            canonical_path: path.to_owned(),
            git_common_directory_path: format!("{path}/.git"),
            filesystem_identity: FilesystemIdentity::new(format!("test:{path}"))
                .expect("test identity"),
        }
    }

    fn ids() -> FakeIds {
        FakeIds {
            workspace: WorkspaceId::parse("wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            roots: Mutex::new(VecDeque::from([
                WorkspaceRootId::parse("wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                WorkspaceRootId::parse("wrt_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap(),
            ])),
        }
    }

    fn application(
        discovery: FakeDiscovery,
        store: FakeStore,
    ) -> WorkspaceApplication<FakeDiscovery, FakeStore, FakeIds> {
        WorkspaceApplication::new(discovery, store, ids())
    }

    #[tokio::test]
    async fn one_root_is_valid_and_ids_are_deterministic() {
        let workspace = application(
            FakeDiscovery {
                results: Mutex::new(VecDeque::from([Ok(discovered("/repo"))])),
            },
            FakeStore {
                created: std::sync::Arc::new(Mutex::new(Vec::new())),
            },
        )
        .create_workspace(CreateWorkspace {
            name: "Workspace".to_owned(),
            roots: vec![WorkspaceRootInput {
                name: "main".to_owned(),
                path: "/requested/repo".to_owned(),
            }],
        })
        .await
        .unwrap();
        assert_eq!(workspace.id().as_str(), "wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert_eq!(
            workspace.roots()[0].id().as_str(),
            "wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
        );
        assert_eq!(workspace.roots()[0].position(), 0);
        assert_eq!(workspace.roots()[0].display_path(), "/requested/repo");
    }

    #[tokio::test]
    async fn empty_roots_are_invalid() {
        let result = application(
            FakeDiscovery::default(),
            FakeStore {
                created: std::sync::Arc::new(Mutex::new(Vec::new())),
            },
        )
        .create_workspace(CreateWorkspace {
            name: "Workspace".to_owned(),
            roots: Vec::new(),
        })
        .await;
        assert_eq!(result, Err(WorkspaceError::WorkspaceRootRequired));
    }

    #[tokio::test]
    async fn order_is_preserved() {
        let workspace = application(
            FakeDiscovery {
                results: Mutex::new(VecDeque::from([
                    Ok(discovered("/one")),
                    Ok(discovered("/two")),
                ])),
            },
            FakeStore {
                created: std::sync::Arc::new(Mutex::new(Vec::new())),
            },
        )
        .create_workspace(CreateWorkspace {
            name: "Workspace".to_owned(),
            roots: vec![
                WorkspaceRootInput {
                    name: "first".to_owned(),
                    path: "/one".to_owned(),
                },
                WorkspaceRootInput {
                    name: "second".to_owned(),
                    path: "/two".to_owned(),
                },
            ],
        })
        .await
        .unwrap();
        assert_eq!(
            workspace
                .roots()
                .iter()
                .map(WorkspaceRoot::name)
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        assert_eq!(
            workspace
                .roots()
                .iter()
                .map(WorkspaceRoot::position)
                .collect::<Vec<_>>(),
            [0, 1]
        );
    }

    #[tokio::test]
    async fn duplicate_names_and_installations_are_rejected() {
        for (first, second, names, expected) in [
            (
                discovered("/one"),
                discovered("/two"),
                ("same", "same"),
                WorkspaceError::WorkspaceRootNameConflict,
            ),
            (
                discovered("/same"),
                discovered("/same"),
                ("one", "two"),
                WorkspaceError::WorkspaceRootDuplicate,
            ),
        ] {
            let result = application(
                FakeDiscovery {
                    results: Mutex::new(VecDeque::from([Ok(first), Ok(second)])),
                },
                FakeStore {
                    created: std::sync::Arc::new(Mutex::new(Vec::new())),
                },
            )
            .create_workspace(CreateWorkspace {
                name: "Workspace".to_owned(),
                roots: vec![
                    WorkspaceRootInput {
                        name: names.0.to_owned(),
                        path: "/one".to_owned(),
                    },
                    WorkspaceRootInput {
                        name: names.1.to_owned(),
                        path: "/two".to_owned(),
                    },
                ],
            })
            .await;
            assert_eq!(result, Err(expected));
        }
    }

    #[tokio::test]
    async fn invalid_discovery_does_not_call_persistence() {
        let created = std::sync::Arc::new(Mutex::new(Vec::new()));
        let store = FakeStore {
            created: created.clone(),
        };
        let result = application(
            FakeDiscovery {
                results: Mutex::new(VecDeque::from([
                    Ok(discovered("/one")),
                    Err(RootDiscoveryError::NotGitRepository),
                ])),
            },
            store,
        )
        .create_workspace(CreateWorkspace {
            name: "Workspace".to_owned(),
            roots: vec![
                WorkspaceRootInput {
                    name: "one".to_owned(),
                    path: "/one".to_owned(),
                },
                WorkspaceRootInput {
                    name: "two".to_owned(),
                    path: "/two".to_owned(),
                },
            ],
        })
        .await;
        assert_eq!(result, Err(WorkspaceError::WorkspaceRootNotGitRepository));
        assert!(created.lock().unwrap().is_empty());
    }

    #[test]
    fn aggregate_rejects_duplicate_root_ids_and_invalid_positions() {
        let root = |id: &str, name: &str, path: &str, position| {
            WorkspaceRoot::new(
                WorkspaceRootId::parse(id).unwrap(),
                name.to_owned(),
                path.to_owned(),
                discovered(path),
                position,
                WorkspaceRootState::Available,
            )
            .unwrap()
        };
        let duplicate_id = Workspace::new(
            WorkspaceId::parse("wsp_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap(),
            "Workspace".to_owned(),
            vec![
                root("wrt_01ARZ3NDEKTSV4RRFFQ69G5FAX", "one", "/one", 0),
                root("wrt_01ARZ3NDEKTSV4RRFFQ69G5FAX", "two", "/two", 1),
            ],
        );
        assert_eq!(
            duplicate_id,
            Err(WorkspaceError::WorkspaceRootIdentityConflict)
        );

        let invalid_position = Workspace::new(
            WorkspaceId::parse("wsp_01ARZ3NDEKTSV4RRFFQ69G5FAX").unwrap(),
            "Workspace".to_owned(),
            vec![root("wrt_01ARZ3NDEKTSV4RRFFQ69G5FAY", "one", "/one", 1)],
        );
        assert_eq!(
            invalid_position,
            Err(WorkspaceError::WorkspaceRootOrderInvalid)
        );
    }

    #[test]
    fn ids_require_their_prefix_and_a_canonical_ulid() {
        assert!(WorkspaceId::parse("wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV").is_ok());
        assert!(WorkspaceRootId::parse("wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV").is_ok());
        assert_eq!(
            WorkspaceId::parse("wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV"),
            Err(InvalidKilnId)
        );
        assert_eq!(WorkspaceId::parse("wsp_invalid"), Err(InvalidKilnId));
        assert_eq!(
            WorkspaceId::parse("wsp_01arz3ndektsv4rrffq69g5fav"),
            Err(InvalidKilnId)
        );
    }

    #[test]
    fn session_ids_and_cursors_require_canonical_values() {
        assert!(SessionId::parse("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV").is_ok());
        assert!(TaskId::parse("tsk_01ARZ3NDEKTSV4RRFFQ69G5FAV").is_ok());
        assert!(MessageId::parse("msg_01ARZ3NDEKTSV4RRFFQ69G5FAV").is_ok());
        assert!(EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV").is_ok());
        assert!(SessionId::parse("ses_01arz3ndektsv4rrffq69g5fav").is_err());
        assert!(MessageId::parse("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV").is_err());
        assert!(TaskId::parse("tsk_invalid").is_err());
        assert!(EventId::parse("evt_invalid").is_err());

        assert_eq!(EventCursor::parse("0").unwrap(), EventCursor::zero());
        assert_eq!(EventCursor::parse("42").unwrap().value(), 42);
        assert_eq!(EventCursor::parse("42").unwrap().to_string(), "42");
        assert!(EventCursor::parse("").is_err());
        assert!(EventCursor::parse("01").is_err());
        assert!(EventCursor::parse("-1").is_err());
        assert!(EventCursor::parse(" 1").is_err());
    }

    #[test]
    fn messages_preserve_input_but_reject_blank_content() {
        let message = Message::new(
            MessageId::parse("msg_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            SessionId::parse("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            MessageRole::User,
            "  keep surrounding whitespace  ".to_owned(),
        )
        .unwrap();
        assert_eq!(message.content(), "  keep surrounding whitespace  ");
        assert_eq!(message.target_run_id(), None);
        let run_id = RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let targeted = Message::new_targeted(
            MessageId::parse("msg_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap(),
            message.session_id().clone(),
            MessageRole::User,
            "guidance".to_owned(),
            run_id.clone(),
        )
        .unwrap();
        assert_eq!(targeted.target_run_id(), Some(&run_id));
        let delivery = MessageDelivery::queued(targeted, MessageDeliveryMode::default()).unwrap();
        assert_eq!(delivery.mode(), MessageDeliveryMode::Queued);
        assert_eq!(delivery.state(), MessageDeliveryState::Queued);
        assert_eq!(
            delivery
                .with_state(MessageDeliveryState::Delivered)
                .unwrap()
                .state(),
            MessageDeliveryState::Delivered
        );
        assert_eq!(
            delivery.with_state(MessageDeliveryState::Queued),
            Err(InvalidMessageDelivery)
        );
        for state in [
            RunState::Queued,
            RunState::Running,
            RunState::WaitingForApproval,
        ] {
            assert!(state.accepts_input());
        }
        for state in [
            RunState::Cancelling,
            RunState::Completed,
            RunState::Failed,
            RunState::Cancelled,
        ] {
            assert!(!state.accepts_input());
        }
        assert_eq!(
            Message::new(
                message.id().clone(),
                message.session_id().clone(),
                MessageRole::User,
                " \n\t ".to_owned(),
            ),
            Err(SessionError::MessageContentRequired)
        );
    }

    #[test]
    fn tasks_validate_objectives_cycles_and_dependency_sets() {
        let task_id = TaskId::parse("tsk_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let first = TaskId::parse("tsk_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap();
        let second = TaskId::parse("tsk_01ARZ3NDEKTSV4RRFFQ69G5FAX").unwrap();
        let task = Task::new(
            task_id.clone(),
            SessionId::parse("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            "objective".to_owned(),
            None,
            vec![second.clone(), first.clone()],
        )
        .unwrap();
        assert_eq!(task.dependency_task_ids(), [first.clone(), second.clone()]);
        assert_eq!(
            Task::new(
                task_id.clone(),
                task.session_id().clone(),
                "objective".to_owned(),
                None,
                vec![first.clone(), first.clone()],
            ),
            Err(TaskError::DuplicateDependency)
        );
        assert_eq!(
            Task::new(
                task_id.clone(),
                task.session_id().clone(),
                "objective".to_owned(),
                Some(task_id.clone()),
                Vec::new(),
            ),
            Err(TaskError::Cycle)
        );
        assert_eq!(
            Task::new(
                task_id.clone(),
                task.session_id().clone(),
                " \n ".to_owned(),
                None,
                Vec::new(),
            ),
            Err(TaskError::ObjectiveRequired)
        );

        let updated = task
            .update(
                "  updated objective  ".to_owned(),
                vec![second.clone(), first.clone()],
            )
            .unwrap();
        assert_eq!(updated.objective(), "  updated objective  ");
        assert_eq!(updated.dependency_task_ids(), [first, second]);
        let blocked = updated.transition(TaskState::Blocked).unwrap();
        assert_eq!(blocked.unblock().unwrap().state(), TaskState::Pending);
        let ready = updated.transition(TaskState::Ready).unwrap();
        assert_eq!(ready.state(), TaskState::Ready);
        let assigned = ready
            .assign(RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap())
            .unwrap();
        assert_eq!(
            assigned.assigned_run_id().map(RunId::as_str),
            Some("run_01ARZ3NDEKTSV4RRFFQ69G5FAV")
        );
        assert_eq!(
            ready.transition(TaskState::Completed),
            Err(TaskError::InvalidTransition)
        );
        assert_eq!(
            ready.update("changed too late".to_owned(), Vec::new()),
            Err(TaskError::InvalidTransition)
        );
        let running = assigned.transition(TaskState::Running).unwrap();
        assert_eq!(
            running.assign(RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap()),
            Err(TaskError::InvalidAssignment)
        );
    }

    struct SessionWorkspaceStore {
        exists: bool,
    }

    impl WorkspaceStore for SessionWorkspaceStore {
        async fn create_workspace(&self, _workspace: &Workspace) -> Result<(), StoreError> {
            Ok(())
        }

        async fn get_workspace(&self, _id: &WorkspaceId) -> Result<Option<Workspace>, StoreError> {
            Ok(self.exists.then(|| {
                Workspace::new(
                    WorkspaceId::parse("wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                    "Workspace".to_owned(),
                    vec![
                        WorkspaceRoot::new(
                            WorkspaceRootId::parse("wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                            "main".to_owned(),
                            "/main".to_owned(),
                            DiscoveredWorkspaceRoot {
                                canonical_path: "/main".to_owned(),
                                git_common_directory_path: "/main/.git".to_owned(),
                                filesystem_identity: FilesystemIdentity::new("test:/main")
                                    .expect("test identity"),
                            },
                            0,
                            WorkspaceRootState::Available,
                        )
                        .unwrap(),
                    ],
                )
                .unwrap()
            }))
        }

        async fn list_workspaces(&self) -> Result<Vec<Workspace>, StoreError> {
            Ok(Vec::new())
        }
    }

    #[derive(Default)]
    struct SessionTestStore {
        session: Mutex<Option<Session>>,
        events: std::sync::Arc<Mutex<Vec<SessionEvent>>>,
    }

    impl SessionStore for SessionTestStore {
        async fn create_session(
            &self,
            session: &Session,
            event: &SessionEvent,
        ) -> Result<(), StoreError> {
            *self.session.lock().unwrap() = Some(session.clone());
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }

        async fn get_session(&self, _id: &SessionId) -> Result<Option<Session>, StoreError> {
            Ok(self.session.lock().unwrap().clone())
        }

        async fn list_sessions(
            &self,
            _workspace_id: &WorkspaceId,
        ) -> Result<Vec<Session>, StoreError> {
            Ok(self.session.lock().unwrap().clone().into_iter().collect())
        }

        async fn append_message(
            &self,
            _message: &Message,
            event: &SessionEvent,
        ) -> Result<(), StoreError> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }

        async fn list_session_events(
            &self,
            _session_id: &SessionId,
            _after: EventCursor,
        ) -> Result<SessionEventPage, StoreError> {
            Ok(SessionEventPage::new(Vec::new(), EventCursor::zero()))
        }

        async fn list_events_after(
            &self,
            _after: EventCursor,
        ) -> Result<SessionEventPage, StoreError> {
            Ok(SessionEventPage::new(Vec::new(), EventCursor::zero()))
        }

        async fn current_event_cursor(&self) -> Result<Option<EventCursor>, StoreError> {
            Ok(None)
        }
    }

    struct SessionTestIds;

    impl SessionIdGenerator for SessionTestIds {
        fn session_id(&self) -> SessionId {
            SessionId::parse("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap()
        }

        fn message_id(&self) -> MessageId {
            MessageId::parse("msg_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap()
        }

        fn event_id(&self) -> EventId {
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap()
        }
    }

    fn session_application(
        workspace_exists: bool,
    ) -> SessionApplication<SessionWorkspaceStore, SessionTestStore, SessionTestIds> {
        SessionApplication::new(
            SessionWorkspaceStore {
                exists: workspace_exists,
            },
            SessionTestStore::default(),
            SessionTestIds,
        )
    }

    #[tokio::test]
    async fn session_application_checks_workspace_and_session_presence() {
        let workspace_id = WorkspaceId::parse("wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        assert_eq!(
            session_application(false)
                .create_session(workspace_id)
                .await,
            Err(SessionError::WorkspaceNotFound)
        );

        let missing = session_application(true);
        let session_id = SessionId::parse("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        assert_eq!(
            missing.get_session(session_id.clone()).await,
            Err(SessionError::SessionNotFound)
        );
        assert_eq!(
            missing
                .append_message(AppendMessage {
                    session_id: session_id.clone(),
                    content: "message".to_owned(),
                })
                .await,
            Err(SessionError::SessionNotFound)
        );
        assert_eq!(
            missing
                .list_session_events(session_id, EventCursor::zero())
                .await,
            Err(SessionError::SessionNotFound)
        );
    }

    #[tokio::test]
    async fn session_commands_create_only_supported_event_payloads() {
        let store = SessionTestStore::default();
        let events = store.events.clone();
        let app = SessionApplication::new(
            SessionWorkspaceStore { exists: true },
            store,
            SessionTestIds,
        );
        let workspace_id = WorkspaceId::parse("wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let session = app.create_session(workspace_id.clone()).await.unwrap();
        let message = app
            .append_message(AppendMessage {
                session_id: session.id().clone(),
                content: "hello".to_owned(),
            })
            .await
            .unwrap();
        assert_eq!(message.role(), MessageRole::User);
        assert_eq!(message.content(), "hello");
        let events = events.lock().unwrap();
        assert!(matches!(
            events[0].payload(),
            SessionEventPayload::SessionCreated { .. }
        ));
        assert!(matches!(
            events[1].payload(),
            SessionEventPayload::MessageAppended { .. }
        ));
    }

    #[tokio::test]
    async fn session_application_rejects_blank_message_content() {
        let app = session_application(true);
        let workspace_id = WorkspaceId::parse("wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let session = app.create_session(workspace_id).await.unwrap();
        assert_eq!(
            app.append_message(AppendMessage {
                session_id: session.id().clone(),
                content: " \n\t ".to_owned(),
            })
            .await,
            Err(SessionError::MessageContentRequired)
        );
    }
}

#[cfg(test)]
mod run_tests {
    use super::*;
    use std::sync::Mutex;

    fn session_id() -> SessionId {
        SessionId::parse("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap()
    }

    fn scope() -> WorkspacePathScope {
        WorkspacePathScope::new(
            WorkspaceRootId::parse("wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            ".",
        )
        .unwrap()
    }

    #[test]
    fn workspace_path_scopes_are_relative_utf8_paths() {
        let root = WorkspaceRootId::parse("wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        assert_eq!(
            WorkspacePathScope::new(root.clone(), "./src/./core")
                .unwrap()
                .relative_directory(),
            "src/core"
        );
        assert_eq!(
            WorkspacePathScope::new(root.clone(), "../outside"),
            Err(WorkspacePathScopeError::ParentTraversal)
        );
        assert_eq!(
            WorkspacePathScope::new(root.clone(), "/outside"),
            Err(WorkspacePathScopeError::Absolute)
        );
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;

            assert_eq!(
                WorkspacePathScope::new(root, std::ffi::OsString::from_vec(vec![0xff])),
                Err(WorkspacePathScopeError::InvalidComponent)
            );
        }
    }

    #[test]
    fn only_terminal_legacy_records_can_be_unscoped() {
        let run_id = RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        assert_eq!(
            Run::from_persisted(run_id.clone(), session_id(), RunState::Queued, None, None,),
            Err(InvalidPersistedRun)
        );
        let legacy_run = Run::from_persisted(
            run_id.clone(),
            session_id(),
            RunState::Completed,
            None,
            None,
        )
        .unwrap();
        assert_eq!(legacy_run.approval_policy(), None);
        assert_eq!(legacy_run.requested_scope(), None);
        assert_eq!(
            Run::from_persisted_hierarchy(
                run_id.clone(),
                session_id(),
                RunState::Completed,
                Some(RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap()),
                None,
                RunInputMode::ReadOnly,
                None,
                None,
            ),
            Err(InvalidPersistedRun)
        );

        let tool_call_id = ToolCallId::parse("tcl_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        assert_eq!(
            ToolCall::from_persisted(PersistedToolCall {
                tool_call_id: tool_call_id.clone(),
                run_id: run_id.clone(),
                capability: DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
                requested_scope: None,
                effective_scope: None,
                state: ToolCallState::Requested,
                stdout: None,
                stderr: None,
                stdout_artifact: None,
                stderr_artifact: None,
                exit_code: None,
            }),
            Err(InvalidPersistedToolCall)
        );
        assert!(
            ToolCall::from_persisted_event(PersistedToolCall {
                tool_call_id: tool_call_id.clone(),
                run_id: run_id.clone(),
                capability: DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
                requested_scope: None,
                effective_scope: None,
                state: ToolCallState::Requested,
                stdout: None,
                stderr: None,
                stdout_artifact: None,
                stderr_artifact: None,
                exit_code: None,
            })
            .is_ok()
        );
        let legacy_tool = ToolCall::from_persisted(PersistedToolCall {
            tool_call_id,
            run_id,
            capability: DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
            requested_scope: None,
            effective_scope: None,
            state: ToolCallState::Completed,
            stdout: Some(String::new()),
            stderr: Some(String::new()),
            stdout_artifact: None,
            stderr_artifact: None,
            exit_code: Some(0),
        })
        .unwrap();
        assert_eq!(legacy_tool.requested_scope(), None);
        assert_eq!(legacy_tool.effective_scope(), None);
    }

    #[test]
    fn run_and_tool_call_ids_are_canonical() {
        assert_eq!(
            RunId::from_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap()).as_str(),
            "run_01ARZ3NDEKTSV4RRFFQ69G5FAV"
        );
        assert_eq!(
            ToolCallId::from_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAW".parse().unwrap()).as_str(),
            "tcl_01ARZ3NDEKTSV4RRFFQ69G5FAW"
        );
        assert!(RunId::parse("run_01arz3ndektsv4rrffq69g5fav").is_err());
        assert!(ToolCallId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV").is_err());
    }

    #[test]
    fn transitions_allow_only_the_declared_paths() {
        for (value, state) in [
            ("queued", RunState::Queued),
            ("running", RunState::Running),
            ("waiting_for_approval", RunState::WaitingForApproval),
            ("cancelling", RunState::Cancelling),
            ("completed", RunState::Completed),
            ("failed", RunState::Failed),
            ("cancelled", RunState::Cancelled),
        ] {
            assert_eq!(RunState::parse(value).unwrap(), state);
            assert_eq!(state.as_str(), value);
        }
        let run = Run::new(
            RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            session_id(),
            ApprovalPolicy::Ask,
            scope(),
        );
        assert_eq!(
            run.transition(RunState::Running).unwrap().state(),
            RunState::Running
        );
        assert_eq!(
            run.transition(RunState::Completed),
            Err(RunError::InvalidTransition)
        );
        assert_eq!(
            run.transition(RunState::Cancelled).unwrap().state(),
            RunState::Cancelled
        );
        assert_eq!(
            run.transition(RunState::Cancelling).unwrap().state(),
            RunState::Cancelling
        );
        let cancelling = run
            .transition(RunState::Running)
            .unwrap()
            .transition(RunState::Cancelling)
            .unwrap();
        assert_eq!(
            cancelling.transition(RunState::Cancelled).unwrap().state(),
            RunState::Cancelled
        );
        assert_eq!(
            cancelling.transition(RunState::Completed),
            Err(RunError::InvalidTransition)
        );
        for terminal in [RunState::Completed, RunState::Failed, RunState::Cancelled] {
            assert!(!terminal.can_transition_to(RunState::Cancelled));
        }
        let tool = ToolCall::new(
            ToolCallId::parse("tcl_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            run.run_id().clone(),
            DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
            scope(),
        );
        assert_eq!(
            tool.transition(ToolCallState::AwaitingApproval)
                .unwrap()
                .state(),
            ToolCallState::AwaitingApproval
        );
        assert_eq!(
            tool.transition(ToolCallState::AwaitingApproval)
                .unwrap()
                .transition(ToolCallState::Completed),
            Err(RunError::InvalidTransition)
        );
        assert_eq!(
            tool.transition(ToolCallState::Completed),
            Err(RunError::InvalidTransition)
        );
        assert_eq!(
            tool.transition(ToolCallState::Denied).unwrap().state(),
            ToolCallState::Denied
        );
        for (value, state) in [
            ("requested", ToolCallState::Requested),
            ("awaiting_approval", ToolCallState::AwaitingApproval),
            ("ready", ToolCallState::Ready),
            ("running", ToolCallState::Running),
            ("completed", ToolCallState::Completed),
            ("failed", ToolCallState::Failed),
            ("cancelled", ToolCallState::Cancelled),
            ("denied", ToolCallState::Denied),
        ] {
            assert_eq!(ToolCallState::parse(value).unwrap(), state);
            assert_eq!(state.as_str(), value);
        }
        for terminal in [
            ToolCallState::Completed,
            ToolCallState::Failed,
            ToolCallState::Cancelled,
            ToolCallState::Denied,
        ] {
            assert!(!terminal.can_transition_to(ToolCallState::Cancelled));
        }
    }

    #[test]
    fn persisted_tool_calls_require_state_consistent_results() {
        let tool_call_id = ToolCallId::parse("tcl_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let run_id = RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        assert_eq!(
            ToolCall::from_persisted(PersistedToolCall {
                tool_call_id: tool_call_id.clone(),
                run_id: run_id.clone(),
                capability: DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
                requested_scope: Some(scope()),
                effective_scope: None,
                state: ToolCallState::Requested,
                stdout: Some(String::new()),
                stderr: None,
                stdout_artifact: None,
                stderr_artifact: None,
                exit_code: None,
            }),
            Err(InvalidPersistedToolCall)
        );
        assert_eq!(
            ToolCall::from_persisted(PersistedToolCall {
                tool_call_id: tool_call_id.clone(),
                run_id: run_id.clone(),
                capability: DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
                requested_scope: Some(scope()),
                effective_scope: Some(scope()),
                state: ToolCallState::Completed,
                stdout: Some(String::new()),
                stderr: Some(String::new()),
                stdout_artifact: None,
                stderr_artifact: None,
                exit_code: Some(7),
            }),
            Err(InvalidPersistedToolCall)
        );
        assert_eq!(
            ToolCall::from_persisted(PersistedToolCall {
                tool_call_id,
                run_id,
                capability: DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
                requested_scope: Some(scope()),
                effective_scope: Some(scope()),
                state: ToolCallState::Failed,
                stdout: Some(String::new()),
                stderr: Some(String::new()),
                stdout_artifact: None,
                stderr_artifact: None,
                exit_code: Some(0),
            }),
            Err(InvalidPersistedToolCall)
        );
        assert_eq!(
            ToolCallResult::new(ToolCallState::Running, String::new(), String::new(), None,),
            Err(RunError::InvalidTransition)
        );
        let cancelled = ToolCall::from_persisted(PersistedToolCall {
            tool_call_id: ToolCallId::parse("tcl_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            run_id: RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            capability: DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
            requested_scope: Some(scope()),
            effective_scope: Some(scope()),
            state: ToolCallState::Cancelled,
            stdout: Some(String::new()),
            stderr: Some(String::new()),
            stdout_artifact: None,
            stderr_artifact: None,
            exit_code: None,
        })
        .unwrap();
        assert_eq!(cancelled.state(), ToolCallState::Cancelled);
        assert_eq!(cancelled.exit_code(), None);
    }

    struct RunTestIds;

    impl RunIdGenerator for RunTestIds {
        fn run_id(&self) -> RunId {
            RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap()
        }

        fn message_id(&self) -> MessageId {
            MessageId::parse("msg_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap()
        }

        fn tool_call_id(&self) -> ToolCallId {
            ToolCallId::parse("tcl_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap()
        }

        fn event_id(&self) -> EventId {
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap()
        }

        fn approval_id(&self) -> ApprovalId {
            ApprovalId::parse("apr_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap()
        }
    }

    struct RunTestStore {
        snapshot: Mutex<RunSnapshot>,
        events: std::sync::Arc<Mutex<Vec<SessionEvent>>>,
        request_count: std::sync::Arc<Mutex<usize>>,
        finish_count: std::sync::Arc<Mutex<usize>>,
        decision_key: Mutex<Option<String>>,
    }

    impl RunTestStore {
        fn mutation(
            &self,
            snapshot: RunSnapshot,
            events: &[SessionEvent],
        ) -> RunMutation<RunSnapshot> {
            *self.snapshot.lock().unwrap() = snapshot.clone();
            self.events.lock().unwrap().extend_from_slice(events);
            let stored_events = events
                .iter()
                .enumerate()
                .map(|(index, event)| {
                    StoredSessionEvent::from_event(event, EventCursor::from_value(index as u64 + 1))
                        .unwrap()
                })
                .collect();
            RunMutation::new(snapshot, stored_events)
        }
    }

    impl SessionStore for RunTestStore {
        async fn create_session(
            &self,
            _session: &Session,
            _event: &SessionEvent,
        ) -> Result<(), StoreError> {
            Ok(())
        }

        async fn get_session(&self, _id: &SessionId) -> Result<Option<Session>, StoreError> {
            Ok(Some(Session::new(
                session_id(),
                WorkspaceId::parse("wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            )))
        }

        async fn list_sessions(
            &self,
            _workspace_id: &WorkspaceId,
        ) -> Result<Vec<Session>, StoreError> {
            Ok(Vec::new())
        }

        async fn append_message(
            &self,
            _message: &Message,
            _event: &SessionEvent,
        ) -> Result<(), StoreError> {
            Ok(())
        }

        async fn list_session_events(
            &self,
            _session_id: &SessionId,
            _after: EventCursor,
        ) -> Result<SessionEventPage, StoreError> {
            Ok(SessionEventPage::new(Vec::new(), EventCursor::zero()))
        }

        async fn list_events_after(
            &self,
            _after: EventCursor,
        ) -> Result<SessionEventPage, StoreError> {
            Ok(SessionEventPage::new(Vec::new(), EventCursor::zero()))
        }

        async fn current_event_cursor(&self) -> Result<Option<EventCursor>, StoreError> {
            Ok(None)
        }
    }

    impl WorkspaceStore for RunTestStore {
        async fn create_workspace(&self, _workspace: &Workspace) -> Result<(), StoreError> {
            Ok(())
        }

        async fn get_workspace(&self, _id: &WorkspaceId) -> Result<Option<Workspace>, StoreError> {
            Ok(Some(
                Workspace::new(
                    WorkspaceId::parse("wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                    "Workspace".to_owned(),
                    vec![
                        WorkspaceRoot::new(
                            WorkspaceRootId::parse("wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                            "main".to_owned(),
                            "/main".to_owned(),
                            DiscoveredWorkspaceRoot {
                                canonical_path: "/main".to_owned(),
                                git_common_directory_path: "/main/.git".to_owned(),
                                filesystem_identity: FilesystemIdentity::new("test:/main")
                                    .expect("test identity"),
                            },
                            0,
                            WorkspaceRootState::Available,
                        )
                        .unwrap(),
                    ],
                )
                .unwrap(),
            ))
        }

        async fn list_workspaces(&self) -> Result<Vec<Workspace>, StoreError> {
            Ok(Vec::new())
        }
    }

    impl RunStore for RunTestStore {
        async fn start_root_run(
            &self,
            _run: &Run,
            _events: &[SessionEvent],
            _idempotency_key: &str,
        ) -> Result<StartRunMutation, RunStoreError> {
            Err(RunStoreError::Unavailable)
        }

        async fn start_child_run(
            &self,
            _run: &Run,
            _events: &[SessionEvent],
            _task_assignment_event_id: Option<&EventId>,
            _idempotency_key: &str,
        ) -> Result<StartRunMutation, RunStoreError> {
            Err(RunStoreError::Unavailable)
        }

        async fn get_run(&self, id: &RunId) -> Result<Option<RunSnapshot>, RunStoreError> {
            let snapshot = self.snapshot.lock().unwrap().clone();
            Ok((snapshot.run().run_id() == id).then_some(snapshot))
        }

        async fn list_session_runs(
            &self,
            session_id: &SessionId,
        ) -> Result<Vec<RunSnapshot>, RunStoreError> {
            let snapshot = self.snapshot.lock().unwrap().clone();
            Ok((snapshot.run().session_id() == session_id)
                .then_some(snapshot)
                .into_iter()
                .collect())
        }

        async fn send_run_input(
            &self,
            _delivery: &MessageDelivery,
            _events: &[SessionEvent],
            _idempotency_key: &str,
        ) -> Result<SendRunInputMutation, RunStoreError> {
            Err(RunStoreError::Unavailable)
        }

        async fn record_run_input_delivery(
            &self,
            _command: &RecordRunInputDelivery,
            _event_id: EventId,
        ) -> Result<RecordRunInputMutation, RunStoreError> {
            Err(RunStoreError::Unavailable)
        }

        async fn next_queued_run_input(
            &self,
            _run_id: &RunId,
        ) -> Result<Option<MessageDelivery>, RunStoreError> {
            Ok(None)
        }

        async fn list_queued_run_inputs(
            &self,
            _run_id: &RunId,
        ) -> Result<Vec<MessageDelivery>, RunStoreError> {
            Ok(Vec::new())
        }

        async fn get_tool_call(
            &self,
            id: &ToolCallId,
        ) -> Result<Option<(Run, ToolCall)>, RunStoreError> {
            let snapshot = self.snapshot.lock().unwrap().clone();
            Ok(snapshot
                .tool_call(id)
                .cloned()
                .map(|tool_call| (snapshot.run().clone(), tool_call)))
        }

        async fn get_approval(
            &self,
            id: &ApprovalId,
        ) -> Result<Option<(Run, Approval)>, RunStoreError> {
            let snapshot = self.snapshot.lock().unwrap().clone();
            Ok(snapshot
                .approval(id)
                .cloned()
                .map(|approval| (snapshot.run().clone(), approval)))
        }

        async fn get_approval_decision(
            &self,
            id: &ApprovalId,
            idempotency_key: &str,
        ) -> Result<Option<(ApprovalState, RunSnapshot)>, RunStoreError> {
            if self.decision_key.lock().unwrap().as_deref() != Some(idempotency_key) {
                return Ok(None);
            }
            let snapshot = self.snapshot.lock().unwrap().clone();
            Ok(snapshot
                .approval(id)
                .map(|approval| (approval.state(), snapshot.clone())))
        }

        async fn begin_execution(
            &self,
            run: &Run,
            tool_call: &ToolCall,
            approval: Option<&Approval>,
            events: &[SessionEvent],
        ) -> Result<RunMutation<RunSnapshot>, RunStoreError> {
            let approvals = approval.cloned().into_iter().collect();
            Ok(self.mutation(
                RunSnapshot::with_approvals(run.clone(), vec![tool_call.clone()], approvals),
                events,
            ))
        }

        async fn begin_tool_call(
            &self,
            tool_call_id: &ToolCallId,
            events: &[SessionEvent],
        ) -> Result<RunMutation<ToolCall>, RunStoreError> {
            let event_tool_call = events
                .first()
                .and_then(|event| match event.payload() {
                    SessionEventPayload::ToolCallStateChanged { tool_call } => Some(tool_call),
                    _ => None,
                })
                .ok_or(RunStoreError::InvalidTransition)?;
            if event_tool_call.tool_call_id() != tool_call_id {
                return Err(RunStoreError::InvalidTransition);
            }
            let snapshot = self.snapshot.lock().unwrap().clone();
            let current = snapshot
                .tool_call(tool_call_id)
                .ok_or(RunStoreError::Unavailable)?;
            if current.state() != ToolCallState::Ready
                || event_tool_call.state() != ToolCallState::Running
            {
                return Err(RunStoreError::InvalidTransition);
            }
            let mut tool_calls = snapshot.tool_calls().to_vec();
            let updated = tool_calls
                .iter_mut()
                .find(|tool_call| tool_call.tool_call_id() == tool_call_id)
                .unwrap();
            *updated = event_tool_call.clone();
            let value = event_tool_call.clone();
            *self.snapshot.lock().unwrap() = RunSnapshot::with_approvals(
                snapshot.run().clone(),
                tool_calls,
                snapshot.approvals().to_vec(),
            );
            self.events.lock().unwrap().extend_from_slice(events);
            let stored_events = events
                .iter()
                .enumerate()
                .map(|(index, event)| {
                    StoredSessionEvent::from_event(event, EventCursor::from_value(index as u64 + 1))
                        .unwrap()
                })
                .collect();
            Ok(RunMutation::new(value, stored_events))
        }

        async fn decide_approval(
            &self,
            run: &Run,
            tool_call: &ToolCall,
            approval: &Approval,
            idempotency_key: &str,
            events: &[SessionEvent],
        ) -> Result<ApprovalDecisionMutation, RunStoreError> {
            let mut key = self.decision_key.lock().unwrap();
            if let Some(existing) = key.as_deref() {
                if existing == idempotency_key {
                    return Ok(ApprovalDecisionMutation::new(
                        self.snapshot.lock().unwrap().clone(),
                        Vec::new(),
                        ApprovalDecisionDisposition::Duplicate,
                    ));
                }
                return Err(RunStoreError::IdempotencyConflict);
            }
            *key = Some(idempotency_key.to_owned());
            let snapshot = self.snapshot.lock().unwrap().clone();
            let mut tool_calls = snapshot.tool_calls().to_vec();
            let current_tool_call = tool_calls
                .iter_mut()
                .find(|current| current.tool_call_id() == tool_call.tool_call_id())
                .ok_or(RunStoreError::Unavailable)?;
            *current_tool_call = tool_call.clone();
            let mut approvals = snapshot.approvals().to_vec();
            let current_approval = approvals
                .iter_mut()
                .find(|current| current.approval_id() == approval.approval_id())
                .ok_or(RunStoreError::Unavailable)?;
            *current_approval = approval.clone();
            let value = RunSnapshot::with_approvals(run.clone(), tool_calls, approvals);
            self.events.lock().unwrap().extend_from_slice(events);
            *self.snapshot.lock().unwrap() = value.clone();
            let stored_events = events
                .iter()
                .enumerate()
                .map(|(index, event)| {
                    StoredSessionEvent::from_event(event, EventCursor::from_value(index as u64 + 1))
                        .unwrap()
                })
                .collect();
            Ok(ApprovalDecisionMutation::new(
                value,
                stored_events,
                ApprovalDecisionDisposition::Applied,
            ))
        }

        async fn finish_execution(
            &self,
            _run: &Run,
            _tool_call: &ToolCall,
            _events: &[SessionEvent],
        ) -> Result<RunMutation<RunSnapshot>, RunStoreError> {
            Err(RunStoreError::Unavailable)
        }

        async fn finish_denied_execution(
            &self,
            run: &Run,
            _tool_call: &ToolCall,
            events: &[SessionEvent],
        ) -> Result<RunMutation<RunSnapshot>, RunStoreError> {
            let snapshot = self.snapshot.lock().unwrap().clone();
            Ok(self.mutation(
                RunSnapshot::with_approvals(
                    run.clone(),
                    snapshot.tool_calls().to_vec(),
                    snapshot.approvals().to_vec(),
                ),
                events,
            ))
        }

        async fn request_cancellation(
            &self,
            run: &Run,
            tool_call: Option<&ToolCall>,
            approval: Option<&Approval>,
            _cancelled_inputs: &[MessageDelivery],
            events: &[SessionEvent],
        ) -> Result<RunMutation<RunSnapshot>, RunStoreError> {
            *self.request_count.lock().unwrap() += 1;
            let snapshot = self.snapshot.lock().unwrap().clone();
            let mut tool_calls = snapshot.tool_calls().to_vec();
            if let Some(tool_call) = tool_call {
                let current = tool_calls
                    .iter_mut()
                    .find(|current| current.tool_call_id() == tool_call.tool_call_id())
                    .ok_or(RunStoreError::Unavailable)?;
                *current = tool_call.clone();
            }
            let mut approvals = snapshot.approvals().to_vec();
            if let Some(approval) = approval {
                let current = approvals
                    .iter_mut()
                    .find(|current| current.approval_id() == approval.approval_id())
                    .ok_or(RunStoreError::Unavailable)?;
                *current = approval.clone();
            }
            Ok(self.mutation(
                RunSnapshot::with_approvals(run.clone(), tool_calls, approvals),
                events,
            ))
        }

        async fn finish_cancellation(
            &self,
            run: &Run,
            tool_call: &ToolCall,
            _cancelled_inputs: &[MessageDelivery],
            events: &[SessionEvent],
        ) -> Result<RunMutation<RunSnapshot>, RunStoreError> {
            *self.finish_count.lock().unwrap() += 1;
            let snapshot = self.snapshot.lock().unwrap().clone();
            let mut tool_calls = snapshot.tool_calls().to_vec();
            let current = tool_calls
                .iter_mut()
                .find(|current| current.tool_call_id() == tool_call.tool_call_id())
                .unwrap();
            *current = tool_call.clone();
            Ok(self.mutation(RunSnapshot::new(run.clone(), tool_calls), events))
        }
    }

    fn run_test_store(state: RunState, tool_state: ToolCallState) -> RunTestStore {
        let run = Run::from_persisted(
            RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            session_id(),
            state,
            Some(ApprovalPolicy::Ask),
            Some(scope()),
        )
        .unwrap();
        let tool_call = ToolCall::new(
            ToolCallId::parse("tcl_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            run.run_id().clone(),
            DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
            scope(),
        );
        let tool_call = match tool_state {
            ToolCallState::Ready => tool_call.with_effective_scope(scope()).unwrap(),
            ToolCallState::Running => tool_call
                .with_effective_scope(scope())
                .unwrap()
                .transition(ToolCallState::Running)
                .unwrap(),
            _ => tool_call,
        };
        RunTestStore {
            snapshot: Mutex::new(RunSnapshot::new(run, vec![tool_call])),
            events: std::sync::Arc::new(Mutex::new(Vec::new())),
            request_count: std::sync::Arc::new(Mutex::new(0)),
            finish_count: std::sync::Arc::new(Mutex::new(0)),
            decision_key: Mutex::new(None),
        }
    }

    fn empty_run_test_store(policy: ApprovalPolicy) -> RunTestStore {
        let run = Run::new(
            RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            session_id(),
            policy,
            scope(),
        );
        RunTestStore {
            snapshot: Mutex::new(RunSnapshot::new(run, Vec::new())),
            events: std::sync::Arc::new(Mutex::new(Vec::new())),
            request_count: std::sync::Arc::new(Mutex::new(0)),
            finish_count: std::sync::Arc::new(Mutex::new(0)),
            decision_key: Mutex::new(None),
        }
    }

    #[tokio::test]
    async fn approval_policies_create_one_durable_tool_decision() {
        for (policy, run_state, tool_state, approval_count) in [
            (
                ApprovalPolicy::Ask,
                RunState::WaitingForApproval,
                ToolCallState::AwaitingApproval,
                1,
            ),
            (
                ApprovalPolicy::FullAccess,
                RunState::Running,
                ToolCallState::Ready,
                0,
            ),
            (
                ApprovalPolicy::ReadOnly,
                RunState::Running,
                ToolCallState::Denied,
                0,
            ),
        ] {
            let app = RunApplication::new(empty_run_test_store(policy), RunTestIds);
            let mutation = app
                .begin_execution(RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap())
                .await
                .unwrap();
            assert_eq!(mutation.value.run().state(), run_state);
            assert_eq!(mutation.value.tool_calls()[0].state(), tool_state);
            assert_eq!(mutation.value.approvals().len(), approval_count);
            assert!(mutation.events.iter().any(|event| matches!(
                event.payload(),
                SessionEventPayload::ToolCallRequested { tool_call }
                    if tool_call.state() == ToolCallState::Requested
            )));
        }
    }

    #[tokio::test]
    async fn approval_decisions_are_idempotent_and_first_decision_wins() {
        let app = RunApplication::new(empty_run_test_store(ApprovalPolicy::Ask), RunTestIds);
        let run_id = RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let pending = app.begin_execution(run_id).await.unwrap().value;
        let approval_id = pending.approvals()[0].approval_id().clone();
        let applied = app
            .decide_approval(
                approval_id.clone(),
                ApprovalState::Approved,
                "approve-once".to_owned(),
            )
            .await
            .unwrap();
        assert_eq!(applied.disposition, ApprovalDecisionDisposition::Applied);
        assert_eq!(applied.value.run().state(), RunState::Running);
        assert_eq!(applied.value.tool_calls()[0].state(), ToolCallState::Ready);

        let duplicate = app
            .decide_approval(
                approval_id.clone(),
                ApprovalState::Approved,
                "approve-once".to_owned(),
            )
            .await
            .unwrap();
        assert_eq!(
            duplicate.disposition,
            ApprovalDecisionDisposition::Duplicate
        );
        assert!(duplicate.events.is_empty());
        assert_eq!(
            app.decide_approval(
                approval_id.clone(),
                ApprovalState::Rejected,
                "approve-once".to_owned(),
            )
            .await,
            Err(RunError::IdempotencyConflict)
        );
        assert_eq!(
            app.decide_approval(
                approval_id,
                ApprovalState::Rejected,
                "new-decision".to_owned(),
            )
            .await,
            Err(RunError::ApprovalAlreadyDecided)
        );
    }

    #[tokio::test]
    async fn cancelling_a_waiting_run_rejects_its_approval() {
        let app = RunApplication::new(empty_run_test_store(ApprovalPolicy::Ask), RunTestIds);
        let run_id = RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        app.begin_execution(run_id.clone()).await.unwrap();
        let cancelled = app.request_cancellation(run_id).await.unwrap().value;
        assert_eq!(cancelled.run().state(), RunState::Cancelled);
        assert_eq!(cancelled.tool_calls()[0].state(), ToolCallState::Denied);
        assert_eq!(cancelled.approvals()[0].state(), ApprovalState::Rejected);
    }

    #[tokio::test]
    async fn queued_and_running_cancellation_have_durable_event_order() {
        for (state, expected_state) in [
            (RunState::Queued, RunState::Cancelled),
            (RunState::Running, RunState::Cancelling),
        ] {
            let store = run_test_store(state, ToolCallState::Requested);
            let events = store.events.clone();
            let app = RunApplication::new(store, RunTestIds);
            let mutation = app
                .request_cancellation(RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap())
                .await
                .unwrap();
            assert_eq!(mutation.value.run().state(), expected_state);
            assert_eq!(mutation.events.len(), 2);
            assert!(matches!(
                mutation.events[0].payload(),
                SessionEventPayload::RunCancellationRequested { .. }
            ));
            assert!(matches!(
                mutation.events[1].payload(),
                SessionEventPayload::RunStateChanged { state, .. } if *state == expected_state
            ));
            assert_eq!(events.lock().unwrap().len(), 2);
        }
    }

    #[tokio::test]
    async fn repeated_and_terminal_cancellation_are_no_op_mutations() {
        for state in [
            RunState::Cancelling,
            RunState::Completed,
            RunState::Failed,
            RunState::Cancelled,
        ] {
            let store = run_test_store(state, ToolCallState::Requested);
            let events = store.events.clone();
            let requests = store.request_count.clone();
            let app = RunApplication::new(store, RunTestIds);
            let run_id = RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
            let first = app.request_cancellation(run_id.clone()).await.unwrap();
            let second = app.request_cancellation(run_id).await.unwrap();
            assert_eq!(first.value.run().state(), state);
            assert_eq!(second.value.run().state(), state);
            assert!(first.events.is_empty());
            assert!(second.events.is_empty());
            assert_eq!(*requests.lock().unwrap(), 2);
            assert!(events.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn cancellation_finalization_captures_output_before_terminal_events() {
        let store = run_test_store(RunState::Cancelling, ToolCallState::Running);
        let events = store.events.clone();
        let finishes = store.finish_count.clone();
        let app = RunApplication::new(store, RunTestIds);
        let run_id = RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let tool_call_id = ToolCallId::parse("tcl_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let mutation = app
            .finish_cancellation(
                run_id,
                tool_call_id.clone(),
                SubprocessOutput::success("out", "err", 0),
            )
            .await
            .unwrap();
        assert_eq!(mutation.value.run().state(), RunState::Cancelled);
        let tool_call = mutation.value.tool_call(&tool_call_id).unwrap();
        assert_eq!(tool_call.state(), ToolCallState::Cancelled);
        assert_eq!(tool_call.stdout(), Some("out"));
        assert_eq!(tool_call.stderr(), Some("err"));
        assert_eq!(tool_call.exit_code(), Some(0));
        assert_eq!(mutation.events.len(), 4);
        assert!(matches!(
            mutation.events[0].payload(),
            SessionEventPayload::ToolCallOutput {
                stream: ToolOutputStream::Stdout,
                content,
                ..
            } if content == "out"
        ));
        assert!(matches!(
            mutation.events[1].payload(),
            SessionEventPayload::ToolCallOutput {
                stream: ToolOutputStream::Stderr,
                content,
                ..
            } if content == "err"
        ));
        assert!(matches!(
            mutation.events[2].payload(),
            SessionEventPayload::ToolCallStateChanged { tool_call }
                if tool_call.state() == ToolCallState::Cancelled
        ));
        assert!(matches!(
            mutation.events[3].payload(),
            SessionEventPayload::RunStateChanged {
                state: RunState::Cancelled,
                ..
            }
        ));
        assert_eq!(*finishes.lock().unwrap(), 1);
        assert_eq!(events.lock().unwrap().len(), 4);
    }

    #[test]
    fn context_manifest_validates_public_entries_and_has_canonical_order() {
        let session_id = SessionId::parse("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let run_id = RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let other_run_id = RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap();
        let manifest_id = ContextManifestId::parse("cmf_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let content_hash = ContentHash::parse("a".repeat(64)).unwrap();
        let message_id = MessageId::parse("msg_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();

        assert_eq!(
            ContextManifest::new(
                manifest_id.clone(),
                session_id.clone(),
                run_id.clone(),
                content_hash.clone(),
                vec![ContextManifestEntry::Instruction {
                    provenance: ContextInstructionProvenance::Runtime,
                    content: " \n ".to_owned(),
                }],
            ),
            Err(InvalidContextManifest::InstructionContentRequired)
        );
        assert_eq!(
            ContextManifest::new(
                manifest_id.clone(),
                session_id.clone(),
                run_id.clone(),
                content_hash.clone(),
                vec![
                    ContextManifestEntry::message_snapshot(
                        message_id.clone(),
                        MessageRole::User,
                        "one".to_owned(),
                    )
                    .unwrap(),
                    ContextManifestEntry::message_snapshot(
                        message_id,
                        MessageRole::User,
                        "two".to_owned(),
                    )
                    .unwrap(),
                ],
            ),
            Err(InvalidContextManifest::DuplicateMessage)
        );
        assert_eq!(
            ContextManifest::new(
                manifest_id,
                session_id.clone(),
                run_id.clone(),
                content_hash,
                vec![
                    ContextManifestEntry::instruction(
                        ContextInstructionProvenance::Run {
                            run_id: other_run_id,
                        },
                        "run rules".to_owned(),
                    )
                    .unwrap()
                ],
            ),
            Err(InvalidContextManifest::InvalidRunProvenance)
        );

        let first = ContextManifestEntry::instruction(
            ContextInstructionProvenance::Runtime,
            "runtime rules".to_owned(),
        )
        .unwrap();
        let second = ContextManifestEntry::instruction(
            ContextInstructionProvenance::User,
            "user rules".to_owned(),
        )
        .unwrap();
        let ordered = canonical_context_manifest_bytes(
            &session_id,
            &run_id,
            &[first.clone(), second.clone()],
        );
        assert_eq!(
            ordered,
            canonical_context_manifest_bytes(
                &session_id,
                &run_id,
                &[first.clone(), second.clone()]
            )
        );
        assert_ne!(
            ordered,
            canonical_context_manifest_bytes(&session_id, &run_id, &[second, first])
        );
        assert_ne!(
            ordered,
            canonical_context_manifest_request_bytes(&CreateContextManifest {
                run_id,
                entries: Vec::new(),
                idempotency_key: "ignored-by-encoding".to_owned(),
            })
        );
    }
}
