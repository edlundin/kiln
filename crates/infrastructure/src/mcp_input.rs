use std::num::NonZeroU64;

use kiln_core::{
    McpInputKind, McpInputMutation, McpInputRecord, McpInputState, McpInputStore,
    McpInvocationError as Error, McpInvocationRecord, McpInvocationState,
};
use sqlx::{Connection, Row, SqliteConnection};

use super::SqliteStore;

impl McpInputStore for SqliteStore {
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
        .execute(connection)
        .await
        .map_err(|_| Error::Unavailable)?;
    Ok(())
}
