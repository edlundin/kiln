use std::num::NonZeroU64;

use kiln_core::{
    McpInputKind, McpInputMutation, McpInputRecord, McpInputState, McpInputStore,
    McpInvocationError as Error, McpInvocationRecord, McpInvocationState,
};
use sqlx::{Connection, Row, SqliteConnection};

use super::SqliteStore;

impl McpInputStore for SqliteStore {
    async fn mcp_input_root(
        &self,
        expected: &McpInputRecord,
        limits: kiln_core::McpDefinitionLimits,
    ) -> Result<std::path::PathBuf, Error> {
        limits.validate().map_err(|_| Error::InvalidRequest)?;
        if expected.kind != McpInputKind::Roots || expected.state != McpInputState::Required {
            return Err(Error::InvalidRequest);
        }
        let mut connection = self.connection.lock().await;
        let mut tx = connection.begin().await.map_err(|_| Error::Unavailable)?;
        let invocation = current_invocation(&mut tx, &expected.invocation).await?;
        let input = load(&mut tx, invocation, expected.ordinal)
            .await?
            .ok_or(Error::NotFound)?;
        if input.kind != McpInputKind::Roots || input.state != McpInputState::Required {
            return Err(Error::Conflict);
        }
        validate_live(&mut tx, &input.invocation).await?;
        let row = sqlx::query("SELECT g.instance_key, g.definition_version, g.host_instance_id, g.host_binding_revision,
            s.workspace_id, t.effective_workspace_root_id, t.effective_relative_directory
            FROM mcp_instance_generations g JOIN mcp_invocations i ON i.generation_id = g.generation_id
            JOIN tool_calls t ON t.tool_call_id = i.tool_call_id JOIN runs r ON r.run_id = t.run_id
            JOIN sessions s ON s.session_id = r.session_id WHERE i.tool_call_id = ?")
            .bind(input.invocation.tool_call_id.as_str()).fetch_one(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        let string = |name| {
            row.try_get::<String, _>(name)
                .map_err(|_| Error::ScopeMismatch)
        };
        let key = kiln_core::McpInstanceKey::from_canonical_json(
            string("instance_key")?.as_bytes(),
            limits.max_metadata_bytes,
        )
        .map_err(|_| Error::IntegrityViolation)?;
        let version = u64::try_from(
            row.try_get::<i64, _>("definition_version")
                .map_err(|_| Error::IntegrityViolation)?,
        )
        .map_err(|_| Error::IntegrityViolation)?;
        let host = super::mcp_invocation::current_host(&mut tx, &key, version, limits).await?;
        let revision = row
            .try_get::<i64, _>("host_binding_revision")
            .map_err(|_| Error::ScopeMismatch)?;
        if host.bindings.instance_id().as_str() != string("host_instance_id")?
            || host.revision.get()
                != u64::try_from(revision).map_err(|_| Error::IntegrityViolation)?
        {
            return Err(Error::GenerationChanged);
        }
        let directory = host
            .bindings
            .working_directory()
            .ok_or(Error::ScopeMismatch)?;
        if directory.workspace_id().as_str() != string("workspace_id")?
            || directory.workspace_root_id().as_str() != string("effective_workspace_root_id")?
            || directory.relative_directory() != string("effective_relative_directory")?
        {
            return Err(Error::ScopeMismatch);
        }
        super::mcp_host_binding::validate_directory(&mut tx, &host.bindings)
            .await
            .map_err(|_| Error::ScopeMismatch)?;
        let path = std::path::Path::new(directory.root_path()).join(directory.relative_directory());
        if !path.is_absolute() {
            return Err(Error::ScopeMismatch);
        }
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(path)
    }

    async fn require_mcp_input(
        &self,
        invocation: &McpInvocationRecord,
        ordinal: NonZeroU64,
        kind: McpInputKind,
    ) -> Result<McpInputMutation, Error> {
        let number = i64::try_from(ordinal.get()).map_err(|_| Error::InvalidRequest)?;
        let mut connection = self.connection.lock().await;
        let mut tx = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let current = current_invocation(&mut tx, invocation).await?;
        if let Some(record) = load(&mut tx, current.clone(), ordinal).await? {
            if record.kind != kind {
                return Err(Error::Conflict);
            }
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(McpInputMutation::Existing(record));
        }
        validate_live(&mut tx, &current).await?;
        let (last, pending): (i64, i64) = sqlx::query_as(
            "SELECT COALESCE(MAX(ordinal), 0), COALESCE(SUM(state = 'required'), 0)
             FROM mcp_inputs WHERE tool_call_id = ?",
        )
        .bind(current.tool_call_id.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| Error::Unavailable)?;
        if pending != 0 {
            return Err(Error::Busy);
        }
        if last.checked_add(1) != Some(number) {
            return Err(Error::Conflict);
        }
        sqlx::query("INSERT INTO mcp_inputs (tool_call_id, ordinal, kind, state) VALUES (?, ?, ?, 'required')")
            .bind(current.tool_call_id.as_str()).bind(number).bind(kind.as_str())
            .execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        append_event(&mut tx, &current, number, "required").await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        self.mcp_invocation_events.send_replace(());
        Ok(McpInputMutation::Applied(McpInputRecord {
            invocation: current,
            ordinal,
            kind,
            state: McpInputState::Required,
        }))
    }

    async fn resolve_mcp_input(
        &self,
        expected: &McpInputRecord,
    ) -> Result<McpInputMutation, Error> {
        let number = i64::try_from(expected.ordinal.get()).map_err(|_| Error::InvalidRequest)?;
        let mut connection = self.connection.lock().await;
        let mut tx = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let invocation = current_invocation(&mut tx, &expected.invocation).await?;
        let mut current = load(&mut tx, invocation, expected.ordinal)
            .await?
            .ok_or(Error::NotFound)?;
        if current.kind != expected.kind {
            return Err(Error::Conflict);
        }
        if current.state == McpInputState::Resolved {
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(McpInputMutation::Existing(current));
        }
        if current.state != McpInputState::Required || expected.state != McpInputState::Required {
            return Err(Error::Conflict);
        }
        validate_live(&mut tx, &current.invocation).await?;
        sqlx::query(
            "UPDATE mcp_inputs SET state = 'resolved' WHERE tool_call_id = ? AND ordinal = ?",
        )
        .bind(current.invocation.tool_call_id.as_str())
        .bind(number)
        .execute(&mut *tx)
        .await
        .map_err(|_| Error::Unavailable)?;
        append_event(&mut tx, &current.invocation, number, "resolved").await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        self.mcp_invocation_events.send_replace(());
        current.state = McpInputState::Resolved;
        Ok(McpInputMutation::Applied(current))
    }
}

async fn current_invocation(
    connection: &mut SqliteConnection,
    expected: &McpInvocationRecord,
) -> Result<McpInvocationRecord, Error> {
    let current = super::mcp_invocation::load(connection, &expected.tool_call_id)
        .await?
        .ok_or(Error::NotFound)?;
    if current.generation != expected.generation
        || expected.state != McpInvocationState::Dispatching
    {
        return Err(Error::Conflict);
    }
    Ok(current)
}

async fn validate_live(
    connection: &mut SqliteConnection,
    invocation: &McpInvocationRecord,
) -> Result<(), Error> {
    if invocation.state != McpInvocationState::Dispatching {
        return Err(Error::Conflict);
    }
    let live: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM mcp_invocations i
         JOIN tool_calls t ON t.tool_call_id = i.tool_call_id
         JOIN runs r ON r.run_id = t.run_id
         JOIN mcp_instance_generations g ON g.generation_id = i.generation_id
         JOIN mcp_instances owner ON owner.generation_id = g.generation_id
         JOIN mcp_definitions d ON d.definition_id = g.definition_id
         WHERE i.tool_call_id = ? AND i.generation_id = ? AND i.state = 'dispatching'
           AND t.state = 'running' AND r.state = 'running'
           AND g.observed = 'ready' AND g.desired = 'running' AND d.version = g.definition_version)",
    )
    .bind(invocation.tool_call_id.as_str()).bind(invocation.generation.as_str())
    .fetch_one(connection).await.map_err(|_| Error::Unavailable)?;
    if live { Ok(()) } else { Err(Error::Conflict) }
}

async fn load(
    connection: &mut SqliteConnection,
    invocation: McpInvocationRecord,
    ordinal: NonZeroU64,
) -> Result<Option<McpInputRecord>, Error> {
    let row =
        sqlx::query("SELECT kind, state FROM mcp_inputs WHERE tool_call_id = ? AND ordinal = ?")
            .bind(invocation.tool_call_id.as_str())
            .bind(i64::try_from(ordinal.get()).map_err(|_| Error::InvalidRequest)?)
            .fetch_optional(connection)
            .await
            .map_err(|_| Error::Unavailable)?;
    row.map(|row| {
        let kind = match row
            .try_get::<&str, _>("kind")
            .map_err(|_| Error::IntegrityViolation)?
        {
            "roots" => McpInputKind::Roots,
            "sampling" => McpInputKind::Sampling,
            "elicitation" => McpInputKind::Elicitation,
            _ => return Err(Error::IntegrityViolation),
        };
        let state = match row
            .try_get::<&str, _>("state")
            .map_err(|_| Error::IntegrityViolation)?
        {
            "required" => McpInputState::Required,
            "resolved" => McpInputState::Resolved,
            "interrupted" => McpInputState::Interrupted,
            _ => return Err(Error::IntegrityViolation),
        };
        Ok(McpInputRecord {
            invocation,
            ordinal,
            kind,
            state,
        })
    })
    .transpose()
}

async fn append_event(
    connection: &mut SqliteConnection,
    invocation: &McpInvocationRecord,
    ordinal: i64,
    state: &str,
) -> Result<(), Error> {
    sqlx::query("INSERT INTO mcp_input_events (tool_call_id, ordinal, state) VALUES (?, ?, ?)")
        .bind(invocation.tool_call_id.as_str())
        .bind(ordinal)
        .bind(state)
        .execute(&mut *connection)
        .await
        .map_err(|_| Error::Unavailable)?;
    publish_events(connection, &invocation.tool_call_id).await?;
    Ok(())
}

/// Also drains input interruption events inserted by the invocation-end trigger.
/// Every projection is committed with the journal mutation and uses Kiln IDs.
pub(super) async fn publish_events(
    connection: &mut SqliteConnection,
    id: &kiln_core::ToolCallId,
) -> Result<(), Error> {
    loop {
        let sequence: Option<i64> = sqlx::query_scalar(
            "SELECT e.sequence FROM mcp_input_events e
            LEFT JOIN session_events s ON s.mcp_input_sequence = e.sequence
            WHERE e.tool_call_id = ? AND s.cursor IS NULL ORDER BY e.sequence LIMIT 1",
        )
        .bind(id.as_str())
        .fetch_optional(&mut *connection)
        .await
        .map_err(|_| Error::Unavailable)?;
        let Some(sequence) = sequence else {
            return Ok(());
        };
        let inserted = sqlx::query("INSERT INTO session_events(event_id, session_id, event_type, run_id, tool_call_id, mcp_input_sequence)
            SELECT ?, r.session_id, 'mcp.input_state_changed', r.run_id, t.tool_call_id, ?
            FROM tool_calls t JOIN runs r ON r.run_id = t.run_id WHERE t.tool_call_id = ?")
            .bind(kiln_core::EventId::from_ulid(ulid::Ulid::generate()).as_str())
            .bind(sequence).bind(id.as_str()).execute(&mut *connection).await.map_err(|_| Error::Unavailable)?;
        if inserted.rows_affected() != 1 {
            return Err(Error::IntegrityViolation);
        }
    }
}

pub(super) async fn load_event(
    connection: &mut SqliteConnection,
    sequence: i64,
) -> Result<(kiln_core::SessionId, kiln_core::SessionEventPayload), kiln_core::StoreError> {
    use kiln_core::{
        McpGenerationId, RunId, SessionEventPayload, SessionId, StoreError, ToolCallId,
    };
    let row = sqlx::query("SELECT r.session_id, r.run_id, e.tool_call_id, i.generation_id, e.ordinal, p.kind, e.state
        FROM mcp_input_events e JOIN mcp_inputs p ON p.tool_call_id = e.tool_call_id AND p.ordinal = e.ordinal
        JOIN mcp_invocations i ON i.tool_call_id = e.tool_call_id
        JOIN tool_calls t ON t.tool_call_id = e.tool_call_id JOIN runs r ON r.run_id = t.run_id
        WHERE e.sequence = ?").bind(sequence).fetch_one(connection).await.map_err(|_| StoreError::Unavailable)?;
    let string = |name| {
        row.try_get::<String, _>(name)
            .map_err(|_| StoreError::Unavailable)
    };
    let kind = match string("kind")?.as_str() {
        "roots" => McpInputKind::Roots,
        "sampling" => McpInputKind::Sampling,
        "elicitation" => McpInputKind::Elicitation,
        _ => return Err(StoreError::Unavailable),
    };
    let state = match string("state")?.as_str() {
        "required" => McpInputState::Required,
        "resolved" => McpInputState::Resolved,
        "interrupted" => McpInputState::Interrupted,
        _ => return Err(StoreError::Unavailable),
    };
    let ordinal = row
        .try_get::<i64, _>("ordinal")
        .ok()
        .and_then(|v| u64::try_from(v).ok())
        .and_then(NonZeroU64::new)
        .ok_or(StoreError::Unavailable)?;
    Ok((
        SessionId::parse(string("session_id")?).map_err(|_| StoreError::Unavailable)?,
        SessionEventPayload::McpInputStateChanged {
            run_id: RunId::parse(string("run_id")?).map_err(|_| StoreError::Unavailable)?,
            tool_call_id: ToolCallId::parse(string("tool_call_id")?)
                .map_err(|_| StoreError::Unavailable)?,
            generation: McpGenerationId::parse(string("generation_id")?)
                .map_err(|_| StoreError::Unavailable)?,
            ordinal,
            kind,
            state,
        },
    ))
}
