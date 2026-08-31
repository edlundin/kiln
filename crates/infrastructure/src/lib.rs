//! SQLite, Git, filesystem, and identifier adapters for Kiln core.

use std::{
    env,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
};

use directories::ProjectDirs;
use kiln_core::{
    DiscoveredWorkspaceRoot, EventCursor, EventId, Message, MessageId, MessageRole,
    RootDiscoveryError, Run, RunId, RunIdGenerator, RunMutation, RunSnapshot, RunState, RunStore,
    RunStoreError, Session, SessionEvent, SessionEventPage, SessionEventPayload, SessionId,
    SessionIdGenerator, SessionStore, StartRunDisposition, StartRunMutation, StoreError,
    StoredSessionEvent, SubprocessExecutor, SubprocessOutput, ToolCall, ToolCallId, ToolCallState,
    ToolOutputStream, Workspace, WorkspaceId, WorkspaceIdGenerator, WorkspaceRoot,
    WorkspaceRootDiscovery, WorkspaceRootId, WorkspaceRootState, WorkspaceStore,
};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};
use tokio::{process::Command, sync::Mutex};
use ulid::Ulid;

#[derive(Debug)]
pub enum InfrastructureError {
    DataDirectoryUnavailable,
    Filesystem(std::io::Error),
    Database(sqlx::Error),
    Migration(sqlx::migrate::MigrateError),
}

#[derive(Clone)]
pub struct SqliteStore {
    connection: Arc<Mutex<SqliteConnection>>,
}

pub const DETERMINISTIC_SUBPROCESS_ARGUMENT: &str = "--kiln-deterministic-subprocess";
pub const DETERMINISTIC_SUCCESS_ARGUMENT: &str = "success";
pub const DETERMINISTIC_FAILURE_ARGUMENT: &str = "failure";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeterministicOutcome {
    Success,
    Failure,
}

impl DeterministicOutcome {
    pub const fn argument(self) -> &'static str {
        match self {
            Self::Success => DETERMINISTIC_SUCCESS_ARGUMENT,
            Self::Failure => DETERMINISTIC_FAILURE_ARGUMENT,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DeterministicSubprocessExecutor {
    outcome: DeterministicOutcome,
}

impl DeterministicSubprocessExecutor {
    pub const fn new(outcome: DeterministicOutcome) -> Self {
        Self { outcome }
    }
}

impl SubprocessExecutor for DeterministicSubprocessExecutor {
    async fn execute(&self) -> SubprocessOutput {
        let executable = match env::current_exe() {
            Ok(path) => path,
            Err(_) => {
                return SubprocessOutput::spawn_failure("deterministic subprocess unavailable");
            }
        };
        let output = Command::new(executable)
            .arg(DETERMINISTIC_SUBPROCESS_ARGUMENT)
            .arg(self.outcome.argument())
            .env_clear()
            .kill_on_drop(true)
            .output()
            .await;
        match output {
            Ok(output) => SubprocessOutput {
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                exit_code: output.status.code(),
                spawn_error: None,
            },
            Err(_) => SubprocessOutput::spawn_failure("deterministic subprocess failed to start"),
        }
    }
}

impl SqliteStore {
    pub async fn open_default() -> Result<Self, InfrastructureError> {
        let data_directory =
            data_directory().ok_or(InfrastructureError::DataDirectoryUnavailable)?;
        Self::open(data_directory).await
    }

    pub async fn open(data_dir: impl AsRef<Path>) -> Result<Self, InfrastructureError> {
        let data_dir = data_dir.as_ref();
        std::fs::create_dir_all(data_dir).map_err(InfrastructureError::Filesystem)?;
        let database_path = data_dir.join("kiln.sqlite3");
        let options = SqliteConnectOptions::new()
            .filename(database_path)
            .create_if_missing(true)
            .foreign_keys(true);
        let mut connection = SqliteConnection::connect_with(&options)
            .await
            .map_err(InfrastructureError::Database)?;
        sqlx::migrate!("./migrations")
            .run(&mut connection)
            .await
            .map_err(InfrastructureError::Migration)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }
}

impl WorkspaceStore for SqliteStore {
    async fn create_workspace(&self, workspace: &Workspace) -> Result<(), StoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query("INSERT INTO workspaces (workspace_id, name) VALUES (?, ?)")
            .bind(workspace.id().as_str())
            .bind(workspace.name())
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        for root in workspace.roots() {
            sqlx::query("INSERT INTO workspace_roots (workspace_root_id, workspace_id, name, display_path, canonical_path, git_common_directory_path, position, state) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
                .bind(root.id().as_str())
                .bind(workspace.id().as_str())
                .bind(root.name())
                .bind(root.display_path())
                .bind(root.canonical_path())
                .bind(root.git_common_directory_path())
                .bind(i64::try_from(root.position()).map_err(|_| StoreError::Unavailable)?)
                .bind(root.state().as_str())
                .execute(&mut *transaction)
                .await
                .map_err(|_| StoreError::Unavailable)?;
        }
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)
    }

    async fn get_workspace(&self, id: &WorkspaceId) -> Result<Option<Workspace>, StoreError> {
        let mut connection = self.connection.lock().await;
        let workspace =
            sqlx::query("SELECT workspace_id, name FROM workspaces WHERE workspace_id = ?")
                .bind(id.as_str())
                .fetch_optional(&mut *connection)
                .await
                .map_err(|_| StoreError::Unavailable)?;
        let Some(workspace) = workspace else {
            return Ok(None);
        };
        let roots = sqlx::query("SELECT workspace_root_id, name, display_path, canonical_path, git_common_directory_path, position, state FROM workspace_roots WHERE workspace_id = ? ORDER BY position")
            .bind(id.as_str())
            .fetch_all(&mut *connection)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut domain_roots = Vec::with_capacity(roots.len());
        for (expected_position, row) in roots.into_iter().enumerate() {
            let position: i64 = row
                .try_get("position")
                .map_err(|_| StoreError::Unavailable)?;
            let state: String = row.try_get("state").map_err(|_| StoreError::Unavailable)?;
            if position < 0 || state != WorkspaceRootState::Available.as_str() {
                return Err(StoreError::Unavailable);
            }
            let position = usize::try_from(position).map_err(|_| StoreError::Unavailable)?;
            if position != expected_position {
                return Err(StoreError::Unavailable);
            }
            domain_roots.push(
                WorkspaceRoot::new(
                    WorkspaceRootId::parse(
                        row.try_get::<String, _>("workspace_root_id")
                            .map_err(|_| StoreError::Unavailable)?,
                    )
                    .map_err(|_| StoreError::Unavailable)?,
                    row.try_get("name").map_err(|_| StoreError::Unavailable)?,
                    row.try_get("display_path")
                        .map_err(|_| StoreError::Unavailable)?,
                    row.try_get("canonical_path")
                        .map_err(|_| StoreError::Unavailable)?,
                    row.try_get("git_common_directory_path")
                        .map_err(|_| StoreError::Unavailable)?,
                    position,
                    WorkspaceRootState::Available,
                )
                .map_err(|_| StoreError::Unavailable)?,
            );
        }
        Workspace::new(
            WorkspaceId::parse(
                workspace
                    .try_get::<String, _>("workspace_id")
                    .map_err(|_| StoreError::Unavailable)?,
            )
            .map_err(|_| StoreError::Unavailable)?,
            workspace
                .try_get("name")
                .map_err(|_| StoreError::Unavailable)?,
            domain_roots,
        )
        .map(Some)
        .map_err(|_| StoreError::Unavailable)
    }
}

impl SessionStore for SqliteStore {
    async fn create_session(
        &self,
        session: &Session,
        event: &SessionEvent,
    ) -> Result<(), StoreError> {
        let SessionEventPayload::SessionCreated { workspace_id } = event.payload() else {
            return Err(StoreError::Unavailable);
        };
        if event.session_id() != session.id() || workspace_id != session.workspace_id() {
            return Err(StoreError::Unavailable);
        }

        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query("INSERT INTO sessions (session_id, workspace_id) VALUES (?, ?)")
            .bind(session.id().as_str())
            .bind(session.workspace_id().as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query(
            "INSERT INTO session_events (event_id, session_id, event_type, message_id) VALUES (?, ?, 'session.created', NULL)",
        )
        .bind(event.event_id().as_str())
        .bind(event.session_id().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)
    }

    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        let mut connection = self.connection.lock().await;
        let row = sqlx::query("SELECT session_id, workspace_id FROM sessions WHERE session_id = ?")
            .bind(id.as_str())
            .fetch_optional(&mut *connection)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let session_id = SessionId::parse(
            row.try_get::<String, _>("session_id")
                .map_err(|_| StoreError::Unavailable)?,
        )
        .map_err(|_| StoreError::Unavailable)?;
        let workspace_id = WorkspaceId::parse(
            row.try_get::<String, _>("workspace_id")
                .map_err(|_| StoreError::Unavailable)?,
        )
        .map_err(|_| StoreError::Unavailable)?;
        Ok(Some(Session::new(session_id, workspace_id)))
    }

    async fn append_message(
        &self,
        message: &Message,
        event: &SessionEvent,
    ) -> Result<(), StoreError> {
        let SessionEventPayload::MessageAppended {
            message: event_message,
        } = event.payload()
        else {
            return Err(StoreError::Unavailable);
        };
        if event.session_id() != message.session_id() || event_message != message {
            return Err(StoreError::Unavailable);
        }

        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query(
            "INSERT INTO messages (message_id, session_id, role, content) VALUES (?, ?, ?, ?)",
        )
        .bind(message.id().as_str())
        .bind(message.session_id().as_str())
        .bind(message.role().as_str())
        .bind(message.content())
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        sqlx::query(
            "INSERT INTO session_events (event_id, session_id, event_type, message_id) VALUES (?, ?, 'message.appended', ?)",
        )
        .bind(event.event_id().as_str())
        .bind(event.session_id().as_str())
        .bind(message.id().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)
    }

    async fn list_session_events(
        &self,
        session_id: &SessionId,
        after: EventCursor,
    ) -> Result<SessionEventPage, StoreError> {
        self.list_events(after, Some(session_id)).await
    }

    async fn list_events_after(&self, after: EventCursor) -> Result<SessionEventPage, StoreError> {
        self.list_events(after, None).await
    }

    async fn current_event_cursor(&self) -> Result<Option<EventCursor>, StoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let cursor = current_cursor(&mut transaction).await?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if cursor == EventCursor::zero() {
            Ok(None)
        } else {
            Ok(Some(cursor))
        }
    }
}

impl SqliteStore {
    async fn list_events(
        &self,
        after: EventCursor,
        session_id: Option<&SessionId>,
    ) -> Result<SessionEventPage, StoreError> {
        let after = i64::try_from(after.value()).unwrap_or(i64::MAX);
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let rows = match session_id {
            Some(session_id) => {
                sqlx::query(
                    "SELECT e.event_id, e.session_id, e.cursor, e.event_type, e.message_id,
                        e.run_id, e.tool_call_id, e.run_state, e.tool_call_state,
                        e.capability, e.stdout, e.stderr, e.exit_code,
                        e.output_stream, e.output_content,
                        m.message_id AS loaded_message_id, m.session_id AS message_session_id,
                        m.role, m.content, s.workspace_id
                 FROM session_events e
                 JOIN sessions s ON s.session_id = e.session_id
                 LEFT JOIN messages m ON m.message_id = e.message_id
                 WHERE e.session_id = ? AND e.cursor > ?
                 ORDER BY e.cursor ASC",
                )
                .bind(session_id.as_str())
                .bind(after)
                .fetch_all(&mut *transaction)
                .await
            }
            None => {
                sqlx::query(
                    "SELECT e.event_id, e.session_id, e.cursor, e.event_type, e.message_id,
                        e.run_id, e.tool_call_id, e.run_state, e.tool_call_state,
                        e.capability, e.stdout, e.stderr, e.exit_code,
                        e.output_stream, e.output_content,
                        m.message_id AS loaded_message_id, m.session_id AS message_session_id,
                        m.role, m.content, s.workspace_id
                 FROM session_events e
                 JOIN sessions s ON s.session_id = e.session_id
                 LEFT JOIN messages m ON m.message_id = e.message_id
                 WHERE e.cursor > ?
                 ORDER BY e.cursor ASC",
                )
                .bind(after)
                .fetch_all(&mut *transaction)
                .await
            }
        }
        .map_err(|_| StoreError::Unavailable)?;
        let events = parse_event_rows(rows)?;
        let current_cursor = current_cursor(&mut transaction).await?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        Ok(SessionEventPage::new(events, current_cursor))
    }
}

fn parse_event_rows(
    rows: Vec<sqlx::sqlite::SqliteRow>,
) -> Result<Vec<StoredSessionEvent>, StoreError> {
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let event_id = EventId::parse(
            row.try_get::<String, _>("event_id")
                .map_err(|_| StoreError::Unavailable)?,
        )
        .map_err(|_| StoreError::Unavailable)?;
        let stored_session_id = SessionId::parse(
            row.try_get::<String, _>("session_id")
                .map_err(|_| StoreError::Unavailable)?,
        )
        .map_err(|_| StoreError::Unavailable)?;
        let cursor = committed_cursor(
            row.try_get::<i64, _>("cursor")
                .map_err(|_| StoreError::Unavailable)?,
        )?;
        let event_type: String = row
            .try_get("event_type")
            .map_err(|_| StoreError::Unavailable)?;
        let message_id: Option<String> = row
            .try_get("message_id")
            .map_err(|_| StoreError::Unavailable)?;
        let run_id: Option<String> = row.try_get("run_id").map_err(|_| StoreError::Unavailable)?;
        let tool_call_id: Option<String> = row
            .try_get("tool_call_id")
            .map_err(|_| StoreError::Unavailable)?;
        let run_state: Option<String> = row
            .try_get("run_state")
            .map_err(|_| StoreError::Unavailable)?;
        let tool_call_state: Option<String> = row
            .try_get("tool_call_state")
            .map_err(|_| StoreError::Unavailable)?;
        let capability: Option<String> = row
            .try_get("capability")
            .map_err(|_| StoreError::Unavailable)?;
        let stdout: Option<String> = row.try_get("stdout").map_err(|_| StoreError::Unavailable)?;
        let stderr: Option<String> = row.try_get("stderr").map_err(|_| StoreError::Unavailable)?;
        let exit_code: Option<i64> = row
            .try_get("exit_code")
            .map_err(|_| StoreError::Unavailable)?;
        let output_stream: Option<String> = row
            .try_get("output_stream")
            .map_err(|_| StoreError::Unavailable)?;
        let output_content: Option<String> = row
            .try_get("output_content")
            .map_err(|_| StoreError::Unavailable)?;
        let workspace_id = WorkspaceId::parse(
            row.try_get::<String, _>("workspace_id")
                .map_err(|_| StoreError::Unavailable)?,
        )
        .map_err(|_| StoreError::Unavailable)?;

        let event = match event_type.as_str() {
            "session.created" => {
                if message_id.is_some() {
                    return Err(StoreError::Unavailable);
                }
                StoredSessionEvent::session_created(
                    event_id,
                    stored_session_id,
                    cursor,
                    workspace_id,
                )
                .map_err(|_| StoreError::Unavailable)?
            }
            "message.appended" => {
                let message_id = MessageId::parse(message_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let loaded_message_id = MessageId::parse(
                    row.try_get::<String, _>("loaded_message_id")
                        .map_err(|_| StoreError::Unavailable)?,
                )
                .map_err(|_| StoreError::Unavailable)?;
                if loaded_message_id != message_id {
                    return Err(StoreError::Unavailable);
                }
                let message_session_id = SessionId::parse(
                    row.try_get::<String, _>("message_session_id")
                        .map_err(|_| StoreError::Unavailable)?,
                )
                .map_err(|_| StoreError::Unavailable)?;
                if message_session_id != stored_session_id {
                    return Err(StoreError::Unavailable);
                }
                let role = MessageRole::parse(
                    row.try_get::<String, _>("role")
                        .map_err(|_| StoreError::Unavailable)?
                        .as_str(),
                )
                .map_err(|_| StoreError::Unavailable)?;
                let message = Message::new(
                    message_id,
                    message_session_id,
                    role,
                    row.try_get::<String, _>("content")
                        .map_err(|_| StoreError::Unavailable)?,
                )
                .map_err(|_| StoreError::Unavailable)?;
                StoredSessionEvent::message_appended(event_id, stored_session_id, cursor, message)
                    .map_err(|_| StoreError::Unavailable)?
            }
            "run.created" | "run.state_changed" => {
                if message_id.is_some()
                    || tool_call_id.is_some()
                    || run_state.is_none()
                    || tool_call_state.is_some()
                    || capability.is_some()
                    || stdout.is_some()
                    || stderr.is_some()
                    || exit_code.is_some()
                    || output_stream.is_some()
                    || output_content.is_some()
                {
                    return Err(StoreError::Unavailable);
                }
                let run_id = RunId::parse(run_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let state = RunState::parse(&run_state.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                if (event_type == "run.created" && state != RunState::Queued)
                    || (event_type == "run.state_changed" && state == RunState::Queued)
                {
                    return Err(StoreError::Unavailable);
                }
                let payload = if event_type == "run.created" {
                    SessionEventPayload::RunCreated { run_id, state }
                } else {
                    SessionEventPayload::RunStateChanged { run_id, state }
                };
                StoredSessionEvent::from_parts(event_id, stored_session_id, cursor, payload)
                    .map_err(|_| StoreError::Unavailable)?
            }
            "tool_call.requested" | "tool_call.state_changed" => {
                let run_id = RunId::parse(run_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let tool_call_id = ToolCallId::parse(tool_call_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let state = ToolCallState::parse(&tool_call_state.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let capability = capability.ok_or(StoreError::Unavailable)?;
                if message_id.is_some()
                    || run_state.is_some()
                    || output_stream.is_some()
                    || output_content.is_some()
                    || (event_type == "tool_call.requested"
                        && (state != ToolCallState::Requested
                            || stdout.is_some()
                            || stderr.is_some()
                            || exit_code.is_some()))
                {
                    return Err(StoreError::Unavailable);
                }
                let exit_code = exit_code
                    .map(|value| i32::try_from(value).map_err(|_| StoreError::Unavailable))
                    .transpose()?;
                let tool_call = ToolCall::from_persisted(
                    tool_call_id,
                    run_id,
                    capability,
                    state,
                    stdout,
                    stderr,
                    exit_code,
                )
                .map_err(|_| StoreError::Unavailable)?;
                let payload = if event_type == "tool_call.requested" {
                    SessionEventPayload::ToolCallRequested { tool_call }
                } else {
                    SessionEventPayload::ToolCallStateChanged { tool_call }
                };
                StoredSessionEvent::from_parts(event_id, stored_session_id, cursor, payload)
                    .map_err(|_| StoreError::Unavailable)?
            }
            "tool_call.output" => {
                if message_id.is_some()
                    || run_state.is_some()
                    || tool_call_state.is_some()
                    || capability.is_some()
                    || stdout.is_some()
                    || stderr.is_some()
                    || exit_code.is_some()
                {
                    return Err(StoreError::Unavailable);
                }
                let run_id = RunId::parse(run_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let tool_call_id = ToolCallId::parse(tool_call_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let stream =
                    ToolOutputStream::parse(&output_stream.ok_or(StoreError::Unavailable)?)
                        .map_err(|_| StoreError::Unavailable)?;
                let payload = SessionEventPayload::ToolCallOutput {
                    run_id,
                    tool_call_id,
                    stream,
                    content: output_content.ok_or(StoreError::Unavailable)?,
                };
                StoredSessionEvent::from_parts(event_id, stored_session_id, cursor, payload)
                    .map_err(|_| StoreError::Unavailable)?
            }
            _ => return Err(StoreError::Unavailable),
        };
        events.push(event);
    }

    Ok(events)
}

impl RunStore for SqliteStore {
    async fn start_root_run(
        &self,
        run: &Run,
        event: &SessionEvent,
        idempotency_key: &str,
    ) -> Result<StartRunMutation, RunStoreError> {
        if idempotency_key.is_empty() {
            return Err(RunStoreError::IdempotencyKeyRequired);
        }
        if run.state() != RunState::Queued
            || event != &SessionEvent::run_created(event.event_id().clone(), run)
        {
            return Err(RunStoreError::InvalidTransition);
        }
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let existing_run_id: Option<String> = sqlx::query_scalar(
            "SELECT run_id FROM start_run_idempotencies WHERE session_id = ? AND idempotency_key = ?",
        )
        .bind(run.session_id().as_str())
        .bind(idempotency_key)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        if let Some(existing_run_id) = existing_run_id {
            let existing_run_id =
                RunId::parse(existing_run_id).map_err(|_| RunStoreError::Unavailable)?;
            let existing_run = load_run(&mut transaction, &existing_run_id)
                .await?
                .ok_or(RunStoreError::Unavailable)?;
            if existing_run.session_id() != run.session_id() {
                return Err(RunStoreError::Unavailable);
            }
            let snapshot = RunSnapshot::new(
                Run::from_persisted(
                    existing_run_id,
                    existing_run.session_id().clone(),
                    RunState::Queued,
                ),
                Vec::new(),
            );
            transaction
                .commit()
                .await
                .map_err(|_| RunStoreError::Unavailable)?;
            return Ok(StartRunMutation::new(
                snapshot,
                Vec::new(),
                StartRunDisposition::Duplicate,
            ));
        }

        let result = sqlx::query("INSERT INTO runs (run_id, session_id, state) VALUES (?, ?, ?)")
            .bind(run.run_id().as_str())
            .bind(run.session_id().as_str())
            .bind(run.state().as_str())
            .execute(&mut *transaction)
            .await;
        if let Err(error) = result {
            if is_unique_constraint(&error) {
                let active: Option<String> = sqlx::query_scalar(
                    "SELECT run_id FROM runs WHERE session_id = ? AND state IN ('queued', 'running') LIMIT 1",
                )
                .bind(run.session_id().as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|_| RunStoreError::Unavailable)?;
                if active.is_some() {
                    return Err(RunStoreError::ActiveRootRunExists);
                }
            }
            return Err(RunStoreError::Unavailable);
        }
        let stored_event = insert_run_event(&mut transaction, event).await?;
        sqlx::query(
            "INSERT INTO start_run_idempotencies (session_id, idempotency_key, run_id) VALUES (?, ?, ?)",
        )
        .bind(run.session_id().as_str())
        .bind(idempotency_key)
        .bind(run.run_id().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(StartRunMutation::new(
            RunSnapshot::new(run.clone(), Vec::new()),
            vec![stored_event],
            StartRunDisposition::Created,
        ))
    }

    async fn get_run(&self, id: &RunId) -> Result<Option<RunSnapshot>, RunStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let snapshot = load_snapshot(&mut transaction, id).await?;
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(snapshot)
    }

    async fn get_tool_call(
        &self,
        id: &ToolCallId,
    ) -> Result<Option<(Run, ToolCall)>, RunStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let row = sqlx::query(
            "SELECT r.run_id, r.session_id, r.state, t.tool_call_id, t.run_id AS tool_run_id,
                    t.capability, t.state AS tool_state, t.stdout, t.stderr, t.exit_code
             FROM tool_calls t JOIN runs r ON r.run_id = t.run_id
             WHERE t.tool_call_id = ?",
        )
        .bind(id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        let result = row.map(|row| parse_run_and_tool_call(&row)).transpose()?;
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(result)
    }

    async fn begin_execution(
        &self,
        run: &Run,
        tool_call: &ToolCall,
        events: &[SessionEvent],
    ) -> Result<RunMutation<RunSnapshot>, RunStoreError> {
        if events.len() != 2 {
            return Err(RunStoreError::Unavailable);
        }
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let current = load_run(&mut transaction, run.run_id())
            .await?
            .ok_or(RunStoreError::Unavailable)?;
        if current.state() != RunState::Queued
            || run.state() != RunState::Running
            || current.session_id() != run.session_id()
        {
            return Err(RunStoreError::InvalidTransition);
        }
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tool_calls WHERE run_id = ?")
            .bind(run.run_id().as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        if count != 0
            || tool_call.run_id() != run.run_id()
            || tool_call.state() != ToolCallState::Requested
            || events[0] != SessionEvent::run_state_changed(events[0].event_id().clone(), run)
            || events[1]
                != SessionEvent::tool_call_requested(
                    events[1].event_id().clone(),
                    run.session_id().clone(),
                    tool_call.clone(),
                )
        {
            return Err(RunStoreError::InvalidTransition);
        }
        sqlx::query("UPDATE runs SET state = ? WHERE run_id = ? AND state = 'queued'")
            .bind(run.state().as_str())
            .bind(run.run_id().as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        sqlx::query(
            "INSERT INTO tool_calls (tool_call_id, run_id, capability, state) VALUES (?, ?, ?, ?)",
        )
        .bind(tool_call.tool_call_id().as_str())
        .bind(tool_call.run_id().as_str())
        .bind(tool_call.capability())
        .bind(tool_call.state().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        let stored_events = insert_events(&mut transaction, events).await?;
        let snapshot = load_snapshot(&mut transaction, run.run_id())
            .await?
            .ok_or(RunStoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(RunMutation::new(snapshot, stored_events))
    }

    async fn begin_tool_call(
        &self,
        tool_call_id: &ToolCallId,
        events: &[SessionEvent],
    ) -> Result<RunMutation<ToolCall>, RunStoreError> {
        if events.len() != 1 {
            return Err(RunStoreError::Unavailable);
        }
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let row = sqlx::query(
            "SELECT tool_call_id, run_id, capability, state, stdout, stderr, exit_code
             FROM tool_calls WHERE tool_call_id = ?",
        )
        .bind(tool_call_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?
        .ok_or(RunStoreError::Unavailable)?;
        let current = parse_tool_call(&row)?;
        let current_run = load_run(&mut transaction, current.run_id())
            .await?
            .ok_or(RunStoreError::Unavailable)?;
        if current.state() != ToolCallState::Requested || current_run.state() != RunState::Running {
            return Err(RunStoreError::InvalidTransition);
        }
        let running = current
            .transition(ToolCallState::Running)
            .map_err(|_| RunStoreError::InvalidTransition)?;
        if events[0]
            != SessionEvent::tool_call_state_changed(
                events[0].event_id().clone(),
                current_run.session_id().clone(),
                running.clone(),
            )
        {
            return Err(RunStoreError::InvalidTransition);
        }
        sqlx::query("UPDATE tool_calls SET state = 'running' WHERE tool_call_id = ? AND state = 'requested'")
            .bind(tool_call_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let stored_events = insert_events(&mut transaction, events).await?;
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(RunMutation::new(running, stored_events))
    }

    async fn finish_execution(
        &self,
        run: &Run,
        tool_call: &ToolCall,
        events: &[SessionEvent],
    ) -> Result<RunMutation<RunSnapshot>, RunStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let current_run = load_run(&mut transaction, run.run_id())
            .await?
            .ok_or(RunStoreError::Unavailable)?;
        let current_tool = sqlx::query(
            "SELECT tool_call_id, run_id, capability, state, stdout, stderr, exit_code
             FROM tool_calls WHERE tool_call_id = ?",
        )
        .bind(tool_call.tool_call_id().as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?
        .ok_or(RunStoreError::Unavailable)
        .and_then(|row| parse_tool_call(&row))?;
        if current_run.state() != RunState::Running
            || current_tool.state() != ToolCallState::Running
            || current_run.session_id() != run.session_id()
            || current_tool.tool_call_id() != tool_call.tool_call_id()
            || current_tool.run_id() != run.run_id()
            || tool_call.run_id() != run.run_id()
            || current_tool.capability() != tool_call.capability()
            || !matches!(
                (run.state(), tool_call.state()),
                (RunState::Completed, ToolCallState::Completed)
                    | (RunState::Failed, ToolCallState::Failed)
            )
            || !finish_events_match(events, run, tool_call)
        {
            return Err(RunStoreError::InvalidTransition);
        }
        let stdout = tool_call.stdout().ok_or(RunStoreError::Unavailable)?;
        let stderr = tool_call.stderr().ok_or(RunStoreError::Unavailable)?;
        sqlx::query("UPDATE tool_calls SET state = ?, stdout = ?, stderr = ?, exit_code = ? WHERE tool_call_id = ? AND state = 'running'")
            .bind(tool_call.state().as_str())
            .bind(stdout)
            .bind(stderr)
            .bind(tool_call.exit_code())
            .bind(tool_call.tool_call_id().as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        sqlx::query("UPDATE runs SET state = ? WHERE run_id = ? AND state = 'running'")
            .bind(run.state().as_str())
            .bind(run.run_id().as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let stored_events = insert_events(&mut transaction, events).await?;
        let snapshot = load_snapshot(&mut transaction, run.run_id())
            .await?
            .ok_or(RunStoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(RunMutation::new(snapshot, stored_events))
    }
}

fn finish_events_match(events: &[SessionEvent], run: &Run, tool_call: &ToolCall) -> bool {
    let Some(stdout) = tool_call.stdout() else {
        return false;
    };
    let Some(stderr) = tool_call.stderr() else {
        return false;
    };
    let mut index = 0;
    if !stdout.is_empty() {
        let Some(event) = events.get(index) else {
            return false;
        };
        if event
            != &SessionEvent::tool_call_output(
                event.event_id().clone(),
                run.session_id().clone(),
                run.run_id().clone(),
                tool_call.tool_call_id().clone(),
                ToolOutputStream::Stdout,
                stdout.to_owned(),
            )
        {
            return false;
        }
        index += 1;
    }
    if !stderr.is_empty() {
        let Some(event) = events.get(index) else {
            return false;
        };
        if event
            != &SessionEvent::tool_call_output(
                event.event_id().clone(),
                run.session_id().clone(),
                run.run_id().clone(),
                tool_call.tool_call_id().clone(),
                ToolOutputStream::Stderr,
                stderr.to_owned(),
            )
        {
            return false;
        }
        index += 1;
    }
    let Some(tool_event) = events.get(index) else {
        return false;
    };
    if tool_event
        != &SessionEvent::tool_call_state_changed(
            tool_event.event_id().clone(),
            run.session_id().clone(),
            tool_call.clone(),
        )
    {
        return false;
    }
    index += 1;
    let Some(run_event) = events.get(index) else {
        return false;
    };
    run_event == &SessionEvent::run_state_changed(run_event.event_id().clone(), run)
        && index + 1 == events.len()
}

fn committed_cursor(value: i64) -> Result<EventCursor, StoreError> {
    if value < 1 {
        return Err(StoreError::Unavailable);
    }
    Ok(EventCursor::from_value(
        u64::try_from(value).map_err(|_| StoreError::Unavailable)?,
    ))
}

async fn current_cursor(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<EventCursor, StoreError> {
    let row = sqlx::query("SELECT MAX(cursor) AS current_cursor FROM session_events")
        .fetch_one(&mut **transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    match row
        .try_get::<Option<i64>, _>("current_cursor")
        .map_err(|_| StoreError::Unavailable)?
    {
        Some(value) => committed_cursor(value),
        None => Ok(EventCursor::zero()),
    }
}

async fn insert_events(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    events: &[SessionEvent],
) -> Result<Vec<StoredSessionEvent>, RunStoreError> {
    let mut stored = Vec::with_capacity(events.len());
    for event in events {
        stored.push(insert_run_event(transaction, event).await?);
    }
    Ok(stored)
}

struct RunEventColumns<'a> {
    event_type: &'static str,
    message_id: Option<&'a str>,
    run_id: Option<&'a str>,
    tool_call_id: Option<&'a str>,
    run_state: Option<&'a str>,
    tool_call_state: Option<&'a str>,
    capability: Option<&'a str>,
    stdout: Option<&'a str>,
    stderr: Option<&'a str>,
    exit_code: Option<i32>,
    output_stream: Option<&'a str>,
    output_content: Option<&'a str>,
}

fn run_event_columns(event: &SessionEvent) -> Result<RunEventColumns<'_>, RunStoreError> {
    match event.payload() {
        SessionEventPayload::RunCreated { run_id, state } => Ok(RunEventColumns {
            event_type: "run.created",
            message_id: None,
            run_id: Some(run_id.as_str()),
            tool_call_id: None,
            run_state: Some(state.as_str()),
            tool_call_state: None,
            capability: None,
            stdout: None,
            stderr: None,
            exit_code: None,
            output_stream: None,
            output_content: None,
        }),
        SessionEventPayload::RunStateChanged { run_id, state } => Ok(RunEventColumns {
            event_type: "run.state_changed",
            message_id: None,
            run_id: Some(run_id.as_str()),
            tool_call_id: None,
            run_state: Some(state.as_str()),
            tool_call_state: None,
            capability: None,
            stdout: None,
            stderr: None,
            exit_code: None,
            output_stream: None,
            output_content: None,
        }),
        SessionEventPayload::ToolCallRequested { tool_call } => Ok(RunEventColumns {
            event_type: "tool_call.requested",
            message_id: None,
            run_id: Some(tool_call.run_id().as_str()),
            tool_call_id: Some(tool_call.tool_call_id().as_str()),
            run_state: None,
            tool_call_state: Some(tool_call.state().as_str()),
            capability: Some(tool_call.capability()),
            stdout: tool_call.stdout(),
            stderr: tool_call.stderr(),
            exit_code: tool_call.exit_code(),
            output_stream: None,
            output_content: None,
        }),
        SessionEventPayload::ToolCallStateChanged { tool_call } => Ok(RunEventColumns {
            event_type: "tool_call.state_changed",
            message_id: None,
            run_id: Some(tool_call.run_id().as_str()),
            tool_call_id: Some(tool_call.tool_call_id().as_str()),
            run_state: None,
            tool_call_state: Some(tool_call.state().as_str()),
            capability: Some(tool_call.capability()),
            stdout: tool_call.stdout(),
            stderr: tool_call.stderr(),
            exit_code: tool_call.exit_code(),
            output_stream: None,
            output_content: None,
        }),
        SessionEventPayload::ToolCallOutput {
            run_id,
            tool_call_id,
            stream,
            content,
        } => Ok(RunEventColumns {
            event_type: "tool_call.output",
            message_id: None,
            run_id: Some(run_id.as_str()),
            tool_call_id: Some(tool_call_id.as_str()),
            run_state: None,
            tool_call_state: None,
            capability: None,
            stdout: None,
            stderr: None,
            exit_code: None,
            output_stream: Some(stream.as_str()),
            output_content: Some(content.as_str()),
        }),
        SessionEventPayload::SessionCreated { .. }
        | SessionEventPayload::MessageAppended { .. } => Err(RunStoreError::Unavailable),
    }
}

async fn insert_run_event(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    event: &SessionEvent,
) -> Result<StoredSessionEvent, RunStoreError> {
    let mut query = sqlx::query(
        "INSERT INTO session_events (
            event_id, session_id, event_type, message_id, run_id, tool_call_id,
            run_state, tool_call_state, capability, stdout, stderr, exit_code,
            output_stream, output_content
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    );
    let columns = run_event_columns(event)?;
    query = query
        .bind(event.event_id().as_str())
        .bind(event.session_id().as_str())
        .bind(columns.event_type)
        .bind(columns.message_id)
        .bind(columns.run_id)
        .bind(columns.tool_call_id)
        .bind(columns.run_state)
        .bind(columns.tool_call_state)
        .bind(columns.capability)
        .bind(columns.stdout)
        .bind(columns.stderr)
        .bind(columns.exit_code)
        .bind(columns.output_stream)
        .bind(columns.output_content);
    query
        .execute(&mut **transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
    let cursor: i64 = sqlx::query_scalar("SELECT last_insert_rowid()")
        .fetch_one(&mut **transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
    StoredSessionEvent::from_event(
        event,
        committed_cursor(cursor).map_err(|_| RunStoreError::Unavailable)?,
    )
    .map_err(|_| RunStoreError::Unavailable)
}

async fn load_run(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &RunId,
) -> Result<Option<Run>, RunStoreError> {
    let row = sqlx::query("SELECT run_id, session_id, state FROM runs WHERE run_id = ?")
        .bind(id.as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
    row.map(|row| {
        let run_id = RunId::parse(
            row.try_get::<String, _>("run_id")
                .map_err(|_| RunStoreError::Unavailable)?,
        )
        .map_err(|_| RunStoreError::Unavailable)?;
        let session_id = SessionId::parse(
            row.try_get::<String, _>("session_id")
                .map_err(|_| RunStoreError::Unavailable)?,
        )
        .map_err(|_| RunStoreError::Unavailable)?;
        let state = RunState::parse(
            row.try_get::<String, _>("state")
                .map_err(|_| RunStoreError::Unavailable)?
                .as_str(),
        )
        .map_err(|_| RunStoreError::Unavailable)?;
        Ok(Run::from_persisted(run_id, session_id, state))
    })
    .transpose()
}

fn parse_tool_call(row: &sqlx::sqlite::SqliteRow) -> Result<ToolCall, RunStoreError> {
    let tool_call_id = ToolCallId::parse(
        row.try_get::<String, _>("tool_call_id")
            .map_err(|_| RunStoreError::Unavailable)?,
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let run_id = RunId::parse(
        row.try_get::<String, _>("run_id")
            .or_else(|_| row.try_get::<String, _>("tool_run_id"))
            .map_err(|_| RunStoreError::Unavailable)?,
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let capability = row
        .try_get::<String, _>("capability")
        .map_err(|_| RunStoreError::Unavailable)?;
    let state = ToolCallState::parse(
        row.try_get::<String, _>("tool_state")
            .or_else(|_| row.try_get::<String, _>("state"))
            .map_err(|_| RunStoreError::Unavailable)?
            .as_str(),
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let stdout = row
        .try_get("stdout")
        .map_err(|_| RunStoreError::Unavailable)?;
    let stderr = row
        .try_get("stderr")
        .map_err(|_| RunStoreError::Unavailable)?;
    let exit_code: Option<i64> = row
        .try_get("exit_code")
        .map_err(|_| RunStoreError::Unavailable)?;
    let exit_code = exit_code
        .map(|value| i32::try_from(value).map_err(|_| RunStoreError::Unavailable))
        .transpose()?;
    ToolCall::from_persisted(
        tool_call_id,
        run_id,
        capability,
        state,
        stdout,
        stderr,
        exit_code,
    )
    .map_err(|_| RunStoreError::Unavailable)
}

fn parse_run_and_tool_call(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<(Run, ToolCall), RunStoreError> {
    let run_id = RunId::parse(
        row.try_get::<String, _>("run_id")
            .map_err(|_| RunStoreError::Unavailable)?,
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let session_id = SessionId::parse(
        row.try_get::<String, _>("session_id")
            .map_err(|_| RunStoreError::Unavailable)?,
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let run_state = RunState::parse(
        row.try_get::<String, _>("state")
            .map_err(|_| RunStoreError::Unavailable)?
            .as_str(),
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let tool_call = parse_tool_call(row)?;
    Ok((
        Run::from_persisted(run_id, session_id, run_state),
        tool_call,
    ))
}

async fn load_snapshot(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &RunId,
) -> Result<Option<RunSnapshot>, RunStoreError> {
    let Some(run) = load_run(transaction, id).await? else {
        return Ok(None);
    };
    let rows = sqlx::query(
        "SELECT tool_call_id, run_id, capability, state, stdout, stderr, exit_code
         FROM tool_calls WHERE run_id = ? ORDER BY rowid",
    )
    .bind(id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(|_| RunStoreError::Unavailable)?;
    let tool_calls = rows
        .iter()
        .map(parse_tool_call)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(RunSnapshot::new(run, tool_calls)))
}

fn is_unique_constraint(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(database) if database.code().as_deref() == Some("2067"))
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GitWorkspaceRootDiscovery;

impl WorkspaceRootDiscovery for GitWorkspaceRootDiscovery {
    async fn discover(&self, path: &Path) -> Result<DiscoveredWorkspaceRoot, RootDiscoveryError> {
        let metadata = std::fs::metadata(path).map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                RootDiscoveryError::Missing
            } else {
                RootDiscoveryError::NotDirectory
            }
        })?;
        if !metadata.is_dir() {
            return Err(RootDiscoveryError::NotDirectory);
        }

        let bare = run_git(path, &["rev-parse", "--is-bare-repository"]).await?;
        if bare.trim() == "true" {
            return Err(RootDiscoveryError::NotGitRepository);
        }
        let top = run_git(
            path,
            &["rev-parse", "--path-format=absolute", "--show-toplevel"],
        )
        .await?;
        let common = run_git(
            path,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        )
        .await?;
        let canonical_path = std::fs::canonicalize(git_path(&top)?)
            .map_err(|_| RootDiscoveryError::NotGitRepository)?;
        let git_common_directory_path = std::fs::canonicalize(git_path(&common)?)
            .map_err(|_| RootDiscoveryError::NotGitRepository)?;
        Ok(DiscoveredWorkspaceRoot {
            canonical_path: canonical_path
                .to_str()
                .ok_or(RootDiscoveryError::NotGitRepository)?
                .to_owned(),
            git_common_directory_path: git_common_directory_path
                .to_str()
                .ok_or(RootDiscoveryError::NotGitRepository)?
                .to_owned(),
        })
    }
}

async fn run_git(path: &Path, args: &[&str]) -> Result<String, RootDiscoveryError> {
    let mut command = Command::new("git");
    command.arg("-C").arg(path).args(args);
    for variable in [
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_COMMON_DIR",
        "GIT_DIR",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_WORK_TREE",
    ] {
        command.env_remove(variable);
    }
    let output = command.output().await.map_err(|error| {
        if error.kind() == ErrorKind::NotFound {
            RootDiscoveryError::GitUnavailable
        } else {
            RootDiscoveryError::NotGitRepository
        }
    })?;
    if !output.status.success() {
        return Err(RootDiscoveryError::NotGitRepository);
    }
    String::from_utf8(output.stdout).map_err(|_| RootDiscoveryError::NotGitRepository)
}

fn git_path(output: &str) -> Result<PathBuf, RootDiscoveryError> {
    let output = output.strip_suffix('\n').unwrap_or(output);
    let output = output.strip_suffix('\r').unwrap_or(output);
    if output.is_empty() {
        return Err(RootDiscoveryError::NotGitRepository);
    }
    Ok(PathBuf::from(output))
}

pub fn data_directory() -> Option<PathBuf> {
    if let Some(path) = env::var_os("KILN_DATA_DIR") {
        return Some(PathBuf::from(path));
    }
    ProjectDirs::from("", "", "kiln").map(|directories| directories.data_dir().to_path_buf())
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UlidIdGenerator;

impl WorkspaceIdGenerator for UlidIdGenerator {
    fn workspace_id(&self) -> WorkspaceId {
        WorkspaceId::from_ulid(Ulid::generate())
    }
    fn workspace_root_id(&self) -> WorkspaceRootId {
        WorkspaceRootId::from_ulid(Ulid::generate())
    }
}

impl SessionIdGenerator for UlidIdGenerator {
    fn session_id(&self) -> SessionId {
        SessionId::from_ulid(Ulid::generate())
    }

    fn message_id(&self) -> MessageId {
        MessageId::from_ulid(Ulid::generate())
    }

    fn event_id(&self) -> EventId {
        EventId::from_ulid(Ulid::generate())
    }
}

impl RunIdGenerator for UlidIdGenerator {
    fn run_id(&self) -> RunId {
        RunId::from_ulid(Ulid::generate())
    }

    fn tool_call_id(&self) -> ToolCallId {
        ToolCallId::from_ulid(Ulid::generate())
    }

    fn event_id(&self) -> EventId {
        EventId::from_ulid(Ulid::generate())
    }
}

#[cfg(test)]
mod tests;
