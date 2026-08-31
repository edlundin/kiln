//! Process-independent Kiln domain and application operations.

use std::{fmt, future::Future, path::Path};

use ulid::Ulid;

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
}

impl MessageRole {
    pub fn parse(value: &str) -> Result<Self, InvalidMessageRole> {
        match value {
            "user" => Ok(Self::User),
            _ => Err(InvalidMessageRole),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    id: SessionId,
    workspace_id: WorkspaceId,
}

impl Session {
    pub fn new(id: SessionId, workspace_id: WorkspaceId) -> Self {
        Self { id, workspace_id }
    }

    pub fn id(&self) -> &SessionId {
        &self.id
    }

    pub fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    id: MessageId,
    session_id: SessionId,
    role: MessageRole,
    content: String,
}

impl Message {
    pub fn new(
        id: MessageId,
        session_id: SessionId,
        role: MessageRole,
        content: String,
    ) -> Result<Self, SessionError> {
        if content.trim().is_empty() {
            return Err(SessionError::MessageContentRequired);
        }
        Ok(Self {
            id,
            session_id,
            role,
            content,
        })
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
}

pub const DETERMINISTIC_SUBPROCESS_CAPABILITY: &str = "kiln.deterministic.subprocess";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Queued,
    Running,
    Completed,
    Failed,
}

impl RunState {
    pub fn parse(value: &str) -> Result<Self, InvalidRunState> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            _ => Err(InvalidRunState),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Queued, Self::Running) | (Self::Running, Self::Completed | Self::Failed)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidRunState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCallState {
    Requested,
    Running,
    Completed,
    Failed,
}

impl ToolCallState {
    pub fn parse(value: &str) -> Result<Self, InvalidToolCallState> {
        match value {
            "requested" => Ok(Self::Requested),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            _ => Err(InvalidToolCallState),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Requested, Self::Running) | (Self::Running, Self::Completed | Self::Failed)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    run_id: RunId,
    session_id: SessionId,
    state: RunState,
}

impl Run {
    pub fn new(run_id: RunId, session_id: SessionId) -> Self {
        Self::from_persisted(run_id, session_id, RunState::Queued)
    }

    pub fn from_persisted(run_id: RunId, session_id: SessionId, state: RunState) -> Self {
        Self {
            run_id,
            session_id,
            state,
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
    pub fn state(&self) -> RunState {
        self.state
    }

    pub fn transition(&self, state: RunState) -> Result<Self, RunError> {
        if !self.state.can_transition_to(state) {
            return Err(RunError::InvalidTransition);
        }
        Ok(Self::from_persisted(
            self.run_id.clone(),
            self.session_id.clone(),
            state,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCall {
    tool_call_id: ToolCallId,
    run_id: RunId,
    capability: String,
    state: ToolCallState,
    stdout: Option<String>,
    stderr: Option<String>,
    exit_code: Option<i32>,
}

impl ToolCall {
    pub fn new(tool_call_id: ToolCallId, run_id: RunId, capability: String) -> Self {
        Self {
            tool_call_id,
            run_id,
            capability,
            state: ToolCallState::Requested,
            stdout: None,
            stderr: None,
            exit_code: None,
        }
    }

    pub fn from_persisted(
        tool_call_id: ToolCallId,
        run_id: RunId,
        capability: String,
        state: ToolCallState,
        stdout: Option<String>,
        stderr: Option<String>,
        exit_code: Option<i32>,
    ) -> Result<Self, InvalidPersistedToolCall> {
        let valid_result = match state {
            ToolCallState::Requested | ToolCallState::Running => {
                stdout.is_none() && stderr.is_none() && exit_code.is_none()
            }
            ToolCallState::Completed | ToolCallState::Failed => {
                stdout.is_some() && stderr.is_some() && terminal_exit_matches(state, exit_code)
            }
        };
        if !valid_result {
            return Err(InvalidPersistedToolCall);
        }
        Ok(Self::from_parts(
            tool_call_id,
            run_id,
            capability,
            state,
            stdout,
            stderr,
            exit_code,
        ))
    }

    fn from_parts(
        tool_call_id: ToolCallId,
        run_id: RunId,
        capability: String,
        state: ToolCallState,
        stdout: Option<String>,
        stderr: Option<String>,
        exit_code: Option<i32>,
    ) -> Self {
        Self {
            tool_call_id,
            run_id,
            capability,
            state,
            stdout,
            stderr,
            exit_code,
        }
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
    pub fn state(&self) -> ToolCallState {
        self.state
    }
    pub fn stdout(&self) -> Option<&str> {
        self.stdout.as_deref()
    }
    pub fn stderr(&self) -> Option<&str> {
        self.stderr.as_deref()
    }
    pub fn exit_code(&self) -> Option<i32> {
        self.exit_code
    }

    pub fn transition(&self, state: ToolCallState) -> Result<Self, RunError> {
        if (self.state, state) != (ToolCallState::Requested, ToolCallState::Running) {
            return Err(RunError::InvalidTransition);
        }
        Ok(Self::from_parts(
            self.tool_call_id.clone(),
            self.run_id.clone(),
            self.capability.clone(),
            state,
            self.stdout.clone(),
            self.stderr.clone(),
            self.exit_code,
        ))
    }

    pub fn with_result(&self, result: &ToolCallResult) -> Result<Self, RunError> {
        let state = result.state();
        if !self.state.can_transition_to(state) {
            return Err(RunError::InvalidTransition);
        }
        Ok(Self::from_parts(
            self.tool_call_id.clone(),
            self.run_id.clone(),
            self.capability.clone(),
            state,
            Some(result.stdout.clone()),
            Some(result.stderr.clone()),
            result.exit_code,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidPersistedToolCall;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallResult {
    state: ToolCallState,
    stdout: String,
    stderr: String,
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
            stdout,
            stderr,
            exit_code,
        })
    }
    pub fn state(&self) -> ToolCallState {
        self.state
    }
    pub fn stdout(&self) -> &str {
        &self.stdout
    }
    pub fn stderr(&self) -> &str {
        &self.stderr
    }
    pub fn exit_code(&self) -> Option<i32> {
        self.exit_code
    }
}

fn terminal_exit_matches(state: ToolCallState, exit_code: Option<i32>) -> bool {
    match state {
        ToolCallState::Completed => exit_code == Some(0),
        ToolCallState::Failed => exit_code != Some(0),
        ToolCallState::Requested | ToolCallState::Running => false,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSnapshot {
    run: Run,
    tool_calls: Vec<ToolCall>,
}

impl RunSnapshot {
    pub fn new(run: Run, tool_calls: Vec<ToolCall>) -> Self {
        Self { run, tool_calls }
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubprocessOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
    pub spawn_error: Option<String>,
}

impl SubprocessOutput {
    pub fn success(stdout: impl Into<String>, stderr: impl Into<String>, exit_code: i32) -> Self {
        Self {
            stdout: stdout.into(),
            stderr: stderr.into(),
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
            exit_code,
            spawn_error: None,
        }
    }

    pub fn spawn_failure(message: impl Into<String>) -> Self {
        Self {
            stdout: String::new(),
            stderr: message.into(),
            exit_code: None,
            spawn_error: Some("subprocess failed to start".to_owned()),
        }
    }

    pub fn succeeded(&self) -> bool {
        self.spawn_error.is_none() && self.exit_code == Some(0)
    }
}

pub trait SubprocessExecutor: Send + Sync {
    fn execute(&self) -> impl Future<Output = SubprocessOutput> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionEventPayload {
    SessionCreated {
        workspace_id: WorkspaceId,
    },
    MessageAppended {
        message: Message,
    },
    RunCreated {
        run_id: RunId,
        state: RunState,
    },
    RunStateChanged {
        run_id: RunId,
        state: RunState,
    },
    ToolCallRequested {
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

    pub fn run_created(event_id: EventId, run: &Run) -> Self {
        Self {
            event_id,
            session_id: run.session_id.clone(),
            payload: SessionEventPayload::RunCreated {
                run_id: run.run_id.clone(),
                state: run.state,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionError {
    WorkspaceNotFound,
    SessionNotFound,
    MessageContentRequired,
    WorkspaceStoreUnavailable,
    SessionStoreUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunError {
    SessionNotFound,
    RunNotFound,
    ActiveRootRunExists,
    IdempotencyKeyRequired,
    InvalidTransition,
    RunStoreUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStoreError {
    ActiveRootRunExists,
    IdempotencyKeyRequired,
    InvalidTransition,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartRunDisposition {
    Created,
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
    fn append_message(
        &self,
        message: &Message,
        event: &SessionEvent,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
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

pub trait SessionIdGenerator: Send + Sync {
    fn session_id(&self) -> SessionId;
    fn message_id(&self) -> MessageId;
    fn event_id(&self) -> EventId;
}

pub trait RunIdGenerator: Send + Sync {
    fn run_id(&self) -> RunId;
    fn tool_call_id(&self) -> ToolCallId;
    fn event_id(&self) -> EventId;
}

pub trait RunStore: SessionStore {
    fn start_root_run(
        &self,
        run: &Run,
        event: &SessionEvent,
        idempotency_key: &str,
    ) -> impl Future<Output = Result<StartRunMutation, RunStoreError>> + Send;
    fn get_run(
        &self,
        id: &RunId,
    ) -> impl Future<Output = Result<Option<RunSnapshot>, RunStoreError>> + Send;
    fn get_tool_call(
        &self,
        id: &ToolCallId,
    ) -> impl Future<Output = Result<Option<(Run, ToolCall)>, RunStoreError>> + Send;
    fn begin_execution(
        &self,
        run: &Run,
        tool_call: &ToolCall,
        events: &[SessionEvent],
    ) -> impl Future<Output = Result<RunMutation<RunSnapshot>, RunStoreError>> + Send;
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
    ) -> Result<StartRunMutation, RunError> {
        if idempotency_key.is_empty() {
            return Err(RunError::IdempotencyKeyRequired);
        }
        let run_id = self.ids.run_id();
        self.start_root_run_with_id(session_id, idempotency_key, run_id)
            .await
    }

    async fn start_root_run_with_id(
        &self,
        session_id: SessionId,
        idempotency_key: String,
        run_id: RunId,
    ) -> Result<StartRunMutation, RunError> {
        let session = self
            .store
            .get_session(&session_id)
            .await
            .map_err(|_| RunError::RunStoreUnavailable)?;
        if session.is_none() {
            return Err(RunError::SessionNotFound);
        }
        let run = Run::new(run_id, session_id);
        let event = SessionEvent::run_created(self.ids.event_id(), &run);
        self.store
            .start_root_run(&run, &event, &idempotency_key)
            .await
            .map_err(map_run_store_error)
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
        let run = snapshot.run.transition(RunState::Running)?;
        if !snapshot.tool_calls.is_empty() {
            return Err(RunError::InvalidTransition);
        }
        let tool_call = ToolCall::new(
            self.ids.tool_call_id(),
            run_id,
            DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
        );
        let events = vec![
            SessionEvent::run_state_changed(self.ids.event_id(), &run),
            SessionEvent::tool_call_requested(
                self.ids.event_id(),
                run.session_id.clone(),
                tool_call.clone(),
            ),
        ];
        self.store
            .begin_execution(&run, &tool_call, &events)
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
        let result =
            ToolCallResult::new(result_state, output.stdout, output.stderr, output.exit_code)?;
        let terminal_tool_call = tool_call.with_result(&result)?;
        let terminal_run = snapshot.run.transition(if succeeded {
            RunState::Completed
        } else {
            RunState::Failed
        })?;
        let mut events = Vec::new();
        if !result.stdout.is_empty() {
            events.push(SessionEvent::tool_call_output(
                self.ids.event_id(),
                snapshot.run.session_id.clone(),
                run_id.clone(),
                tool_call_id.clone(),
                ToolOutputStream::Stdout,
                result.stdout.clone(),
            ));
        }
        if !result.stderr.is_empty() {
            events.push(SessionEvent::tool_call_output(
                self.ids.event_id(),
                snapshot.run.session_id.clone(),
                run_id,
                tool_call_id,
                ToolOutputStream::Stderr,
                result.stderr.clone(),
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

fn map_run_store_error(error: RunStoreError) -> RunError {
    match error {
        RunStoreError::ActiveRootRunExists => RunError::ActiveRootRunExists,
        RunStoreError::IdempotencyKeyRequired => RunError::IdempotencyKeyRequired,
        RunStoreError::InvalidTransition => RunError::InvalidTransition,
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
        if workspace.is_none() {
            return Err(SessionError::WorkspaceNotFound);
        }
        let session = Session::new(self.ids.session_id(), workspace_id.clone());
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

    pub async fn append_message(&self, command: AppendMessage) -> Result<Message, SessionError> {
        self.get_session(command.session_id.clone()).await?;
        let message = Message::new(
            self.ids.message_id(),
            command.session_id,
            MessageRole::User,
            command.content,
        )?;
        let event = SessionEvent::message_appended(self.ids.event_id(), message.clone());
        self.session_store
            .append_message(&message, &event)
            .await
            .map_err(|_| SessionError::SessionStoreUnavailable)?;
        Ok(message)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredWorkspaceRoot {
    pub canonical_path: String,
    pub git_common_directory_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceRoot {
    id: WorkspaceRootId,
    name: String,
    display_path: String,
    canonical_path: String,
    git_common_directory_path: String,
    position: usize,
    state: WorkspaceRootState,
}

impl WorkspaceRoot {
    pub fn new(
        id: WorkspaceRootId,
        name: String,
        display_path: String,
        canonical_path: String,
        git_common_directory_path: String,
        position: usize,
        state: WorkspaceRootState,
    ) -> Result<Self, WorkspaceError> {
        if name.trim().is_empty() {
            return Err(WorkspaceError::WorkspaceRootNameRequired);
        }
        if display_path.is_empty() {
            return Err(WorkspaceError::WorkspaceRootMissing);
        }
        if canonical_path.is_empty() || git_common_directory_path.is_empty() {
            return Err(WorkspaceError::WorkspaceRootNotGitRepository);
        }
        Ok(Self {
            id,
            name,
            display_path,
            canonical_path,
            git_common_directory_path,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreError {
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
    WorkspaceNotFound,
    GitUnavailable,
    WorkspaceStoreUnavailable,
}

pub trait WorkspaceRootDiscovery: Send + Sync {
    fn discover(
        &self,
        path: &Path,
    ) -> impl Future<Output = Result<DiscoveredWorkspaceRoot, RootDiscoveryError>> + Send;
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
                discovered.canonical_path,
                discovered.git_common_directory_path,
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
                path.to_owned(),
                format!("{path}/.git"),
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
        assert!(MessageId::parse("msg_01ARZ3NDEKTSV4RRFFQ69G5FAV").is_ok());
        assert!(EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV").is_ok());
        assert!(SessionId::parse("ses_01arz3ndektsv4rrffq69g5fav").is_err());
        assert!(MessageId::parse("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV").is_err());
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
                            "/main".to_owned(),
                            "/main/.git".to_owned(),
                            0,
                            WorkspaceRootState::Available,
                        )
                        .unwrap(),
                    ],
                )
                .unwrap()
            }))
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

    fn session_id() -> SessionId {
        SessionId::parse("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap()
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
        let run = Run::new(
            RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            session_id(),
        );
        assert_eq!(
            run.transition(RunState::Running).unwrap().state(),
            RunState::Running
        );
        assert_eq!(
            run.transition(RunState::Completed),
            Err(RunError::InvalidTransition)
        );
        let tool = ToolCall::new(
            ToolCallId::parse("tcl_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            run.run_id().clone(),
            DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
        );
        assert_eq!(
            tool.transition(ToolCallState::Running).unwrap().state(),
            ToolCallState::Running
        );
        assert_eq!(
            tool.transition(ToolCallState::Running)
                .unwrap()
                .transition(ToolCallState::Completed),
            Err(RunError::InvalidTransition)
        );
        assert_eq!(
            tool.transition(ToolCallState::Completed),
            Err(RunError::InvalidTransition)
        );
    }

    #[test]
    fn persisted_tool_calls_require_state_consistent_results() {
        let tool_call_id = ToolCallId::parse("tcl_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let run_id = RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        assert_eq!(
            ToolCall::from_persisted(
                tool_call_id.clone(),
                run_id.clone(),
                DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
                ToolCallState::Requested,
                Some(String::new()),
                None,
                None,
            ),
            Err(InvalidPersistedToolCall)
        );
        assert_eq!(
            ToolCall::from_persisted(
                tool_call_id.clone(),
                run_id.clone(),
                DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
                ToolCallState::Completed,
                Some(String::new()),
                Some(String::new()),
                Some(7),
            ),
            Err(InvalidPersistedToolCall)
        );
        assert_eq!(
            ToolCall::from_persisted(
                tool_call_id,
                run_id,
                DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
                ToolCallState::Failed,
                Some(String::new()),
                Some(String::new()),
                Some(0),
            ),
            Err(InvalidPersistedToolCall)
        );
        assert_eq!(
            ToolCallResult::new(ToolCallState::Running, String::new(), String::new(), None,),
            Err(RunError::InvalidTransition)
        );
    }
}
