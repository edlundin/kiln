use std::num::NonZeroU64;

use kiln_core::{
    McpElicitationDecision, McpElicitationDecisionMutation, McpElicitationDecisionStore,
    McpElicitationForm, McpElicitationFormLimits, McpElicitationFormMutation,
    McpElicitationFormRecord, McpElicitationFormStore, McpInputKind, McpInputMutation,
    McpInputRecord, McpInputState, McpInputStore, McpInvocationError as Error, McpInvocationRecord,
    McpInvocationState,
};
use sqlx::{Connection, Row, SqliteConnection};

use super::SqliteStore;

impl McpInputStore for SqliteStore {
    async fn mcp_input_interaction_run(
        &self,
        expected: &McpInputRecord,
    ) -> Result<kiln_core::RunId, Error> {
        if !matches!(
            expected.kind,
            McpInputKind::Sampling | McpInputKind::Elicitation
        ) || expected.state != McpInputState::Required
        {
            return Err(Error::InvalidRequest);
        }
        let mut connection = self.connection.lock().await;
        let mut tx = connection.begin().await.map_err(|_| Error::Unavailable)?;
        let invocation = current_invocation(&mut tx, &expected.invocation).await?;
        let current = load(&mut tx, invocation, expected.ordinal)
            .await?
            .ok_or(Error::NotFound)?;
        if current.kind != expected.kind || current.state != McpInputState::Required {
            return Err(Error::Conflict);
        }
        validate_live(&mut tx, &current.invocation).await?;
        let owner = interaction_owner(&mut tx, &current.invocation).await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(owner)
    }

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
        let record = insert_input(&mut tx, current, ordinal, kind).await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        self.mcp_invocation_events.send_replace(());
        Ok(McpInputMutation::Applied(record))
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
        if matches!(
            current.kind,
            McpInputKind::Sampling | McpInputKind::Elicitation
        ) {
            interaction_owner(&mut tx, &current.invocation).await?;
        }
        if current.kind == McpInputKind::Elicitation {
            let awaiting_decision: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM mcp_elicitation_forms f
                LEFT JOIN mcp_elicitation_decisions d USING (tool_call_id, ordinal)
                WHERE f.tool_call_id = ? AND f.ordinal = ? AND d.tool_call_id IS NULL
                UNION ALL SELECT 1 FROM mcp_elicitation_urls u
                LEFT JOIN mcp_elicitation_url_decisions d USING (tool_call_id, ordinal)
                WHERE u.tool_call_id = ? AND u.ordinal = ? AND d.tool_call_id IS NULL)",
            )
            .bind(current.invocation.tool_call_id.as_str())
            .bind(number)
            .bind(current.invocation.tool_call_id.as_str())
            .bind(number)
            .fetch_one(&mut *tx)
            .await
            .map_err(|_| Error::Unavailable)?;
            if awaiting_decision {
                return Err(Error::Conflict);
            }
        }
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

impl McpElicitationFormStore for SqliteStore {
    async fn require_mcp_elicitation_form(
        &self,
        invocation: &McpInvocationRecord,
        ordinal: NonZeroU64,
        form: &McpElicitationForm,
        limits: McpElicitationFormLimits,
    ) -> Result<McpElicitationFormMutation, Error> {
        form.validate(limits)?;
        let mut connection = self.connection.lock().await;
        let mut tx = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let current = current_invocation(&mut tx, invocation).await?;
        validate_live(&mut tx, &current).await?;
        let interaction_run = interaction_owner(&mut tx, &current).await?;
        if let Some(input) = load(&mut tx, current.clone(), ordinal).await? {
            if input.kind != McpInputKind::Elicitation || input.state != McpInputState::Required {
                return Err(Error::Conflict);
            }
            let record = load_form(&mut tx, input, limits)
                .await?
                .ok_or(Error::Conflict)?;
            if record.form != *form || record.interaction_run != interaction_run {
                return Err(Error::Conflict);
            }
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(McpElicitationFormMutation::Existing(record));
        }
        let input = insert_input(&mut tx, current, ordinal, McpInputKind::Elicitation).await?;
        sqlx::query("INSERT INTO mcp_elicitation_forms (tool_call_id, ordinal, interaction_run_id, message, schema_json)
            VALUES (?, ?, ?, ?, ?)")
            .bind(input.invocation.tool_call_id.as_str()).bind(i64::try_from(input.ordinal.get()).map_err(|_| Error::InvalidRequest)?)
            .bind(interaction_run.as_str()).bind(form.message()).bind(form.schema_json())
            .execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        self.mcp_invocation_events.send_replace(());
        Ok(McpElicitationFormMutation::Applied(
            McpElicitationFormRecord {
                input,
                interaction_run,
                form: form.clone(),
            },
        ))
    }

    async fn get_mcp_elicitation_form(
        &self,
        expected: &McpInputRecord,
        interaction_run: &kiln_core::RunId,
        limits: McpElicitationFormLimits,
    ) -> Result<McpElicitationFormRecord, Error> {
        let mut connection = self.connection.lock().await;
        let mut tx = connection.begin().await.map_err(|_| Error::Unavailable)?;
        let record = pending_form(&mut tx, expected, interaction_run, limits).await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(record)
    }
}

impl McpElicitationDecisionStore for SqliteStore {
    fn subscribe_mcp_input_changes(&self) -> tokio::sync::watch::Receiver<()> {
        self.mcp_invocation_events.subscribe()
    }

    async fn decide_mcp_elicitation_form(
        &self,
        expected: &McpElicitationFormRecord,
        decision: &McpElicitationDecision,
        form_limits: McpElicitationFormLimits,
        max_response_bytes: std::num::NonZeroUsize,
    ) -> Result<McpElicitationDecisionMutation, Error> {
        decision.validate(max_response_bytes)?;
        let mut connection = self.connection.lock().await;
        let mut tx = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let current = pending_form(
            &mut tx,
            &expected.input,
            &expected.interaction_run,
            form_limits,
        )
        .await?;
        if current != *expected {
            return Err(Error::Conflict);
        }
        if let Some(existing) = load_decision(&mut tx, &current.input, max_response_bytes).await? {
            if existing != *decision {
                return Err(Error::Conflict);
            }
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(McpElicitationDecisionMutation::Existing);
        }
        sqlx::query("INSERT INTO mcp_elicitation_decisions (tool_call_id, ordinal, decision_json) VALUES (?, ?, ?)")
            .bind(current.input.invocation.tool_call_id.as_str())
            .bind(i64::try_from(current.input.ordinal.get()).map_err(|_| Error::InvalidRequest)?)
            .bind(decision.as_json()).execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        self.mcp_invocation_events.send_replace(());
        Ok(McpElicitationDecisionMutation::Applied)
    }

    async fn get_mcp_elicitation_decision(
        &self,
        expected: &McpElicitationFormRecord,
        form_limits: McpElicitationFormLimits,
        max_response_bytes: std::num::NonZeroUsize,
    ) -> Result<Option<McpElicitationDecision>, Error> {
        let mut connection = self.connection.lock().await;
        let mut tx = connection.begin().await.map_err(|_| Error::Unavailable)?;
        let current = pending_form(
            &mut tx,
            &expected.input,
            &expected.interaction_run,
            form_limits,
        )
        .await?;
        if current != *expected {
            return Err(Error::Conflict);
        }
        let decision = load_decision(&mut tx, &current.input, max_response_bytes).await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(decision)
    }
}

async fn pending_form(
    connection: &mut SqliteConnection,
    expected: &McpInputRecord,
    interaction_run: &kiln_core::RunId,
    limits: McpElicitationFormLimits,
) -> Result<McpElicitationFormRecord, Error> {
    if expected.kind != McpInputKind::Elicitation || expected.state != McpInputState::Required {
        return Err(Error::InvalidRequest);
    }
    let invocation = current_invocation(connection, &expected.invocation).await?;
    let input = load(connection, invocation, expected.ordinal)
        .await?
        .ok_or(Error::NotFound)?;
    if input.kind != McpInputKind::Elicitation || input.state != McpInputState::Required {
        return Err(Error::Conflict);
    }
    validate_live(connection, &input.invocation).await?;
    if interaction_owner(connection, &input.invocation).await? != *interaction_run {
        return Err(Error::Conflict);
    }
    let record = load_form(connection, input, limits)
        .await?
        .ok_or(Error::NotFound)?;
    if record.interaction_run != *interaction_run {
        return Err(Error::Conflict);
    }
    Ok(record)
}

async fn load_decision(
    connection: &mut SqliteConnection,
    input: &McpInputRecord,
    max_bytes: std::num::NonZeroUsize,
) -> Result<Option<McpElicitationDecision>, Error> {
    let row = sqlx::query("SELECT CASE WHEN length(CAST(decision_json AS BLOB)) <= ? THEN decision_json END AS decision_json
        FROM mcp_elicitation_decisions WHERE tool_call_id = ? AND ordinal = ?")
        .bind(i64::try_from(max_bytes.get()).unwrap_or(i64::MAX))
        .bind(input.invocation.tool_call_id.as_str())
        .bind(i64::try_from(input.ordinal.get()).map_err(|_| Error::InvalidRequest)?)
        .fetch_optional(connection).await.map_err(|_| Error::Unavailable)?;
    row.map(|row| {
        let json: Option<String> = row
            .try_get("decision_json")
            .map_err(|_| Error::IntegrityViolation)?;
        McpElicitationDecision::from_json(&json.ok_or(Error::InvalidRequest)?, max_bytes)
    })
    .transpose()
}

async fn load_form(
    connection: &mut SqliteConnection,
    input: McpInputRecord,
    limits: McpElicitationFormLimits,
) -> Result<Option<McpElicitationFormRecord>, Error> {
    // Check byte lengths in SQLite before copying a retained body into memory.
    let row = sqlx::query(
        "SELECT interaction_run_id,
        CASE WHEN length(CAST(message AS BLOB)) <= ? THEN message END AS message,
        CASE WHEN length(CAST(schema_json AS BLOB)) <= ? THEN schema_json END AS schema_json
        FROM mcp_elicitation_forms WHERE tool_call_id = ? AND ordinal = ?",
    )
    .bind(i64::try_from(limits.max_message_bytes.get()).unwrap_or(i64::MAX))
    .bind(i64::try_from(limits.max_schema_bytes.get()).unwrap_or(i64::MAX))
    .bind(input.invocation.tool_call_id.as_str())
    .bind(i64::try_from(input.ordinal.get()).map_err(|_| Error::InvalidRequest)?)
    .fetch_optional(connection)
    .await
    .map_err(|_| Error::Unavailable)?;
    row.map(|row| {
        let owner: String = row
            .try_get("interaction_run_id")
            .map_err(|_| Error::IntegrityViolation)?;
        let message: Option<String> = row
            .try_get("message")
            .map_err(|_| Error::IntegrityViolation)?;
        let schema: Option<String> = row
            .try_get("schema_json")
            .map_err(|_| Error::IntegrityViolation)?;
        Ok(McpElicitationFormRecord {
            input,
            interaction_run: kiln_core::RunId::parse(owner)
                .map_err(|_| Error::IntegrityViolation)?,
            form: McpElicitationForm::new(
                message.ok_or(Error::InvalidRequest)?,
                &schema.ok_or(Error::InvalidRequest)?,
                limits,
            )?,
        })
    })
    .transpose()
}

pub(super) async fn insert_input(
    connection: &mut SqliteConnection,
    current: McpInvocationRecord,
    ordinal: NonZeroU64,
    kind: McpInputKind,
) -> Result<McpInputRecord, Error> {
    let number = i64::try_from(ordinal.get()).map_err(|_| Error::InvalidRequest)?;
    let (last, pending): (i64, i64) = sqlx::query_as(
        "SELECT COALESCE(MAX(ordinal), 0), COALESCE(SUM(state = 'required'), 0)
         FROM mcp_inputs WHERE tool_call_id = ?",
    )
    .bind(current.tool_call_id.as_str())
    .fetch_one(&mut *connection)
    .await
    .map_err(|_| Error::Unavailable)?;
    if pending != 0 {
        return Err(Error::Busy);
    }
    if last.checked_add(1) != Some(number) {
        return Err(Error::Conflict);
    }
    sqlx::query(
        "INSERT INTO mcp_inputs (tool_call_id, ordinal, kind, state) VALUES (?, ?, ?, 'required')",
    )
    .bind(current.tool_call_id.as_str())
    .bind(number)
    .bind(kind.as_str())
    .execute(&mut *connection)
    .await
    .map_err(|_| Error::Unavailable)?;
    append_event(connection, &current, number, "required").await?;
    Ok(McpInputRecord {
        invocation: current,
        ordinal,
        kind,
        state: McpInputState::Required,
    })
}

// Keep ancestry validation inside the caller's transaction so a cancelled
// ancestor cannot race a fresh resolution after an earlier ownership lookup.
pub(super) async fn interaction_owner(
    connection: &mut SqliteConnection,
    invocation: &McpInvocationRecord,
) -> Result<kiln_core::RunId, Error> {
    // UNION, rather than UNION ALL, makes corrupt cycles terminate without
    // an arbitrary ancestry-depth limit. Any broken chain fails closed.
    let owner: Option<String> = sqlx::query_scalar("WITH RECURSIVE
        source AS (SELECT r.* FROM runs r JOIN tool_calls t ON t.run_id = r.run_id WHERE t.tool_call_id = ?),
        lineage(run_id,parent_run_id,session_id,user_input_mode,state) AS (
            SELECT run_id,parent_run_id,session_id,user_input_mode,state FROM source
            UNION
            SELECT r.run_id,r.parent_run_id,r.session_id,r.user_input_mode,r.state
            FROM runs r JOIN lineage c ON r.run_id = c.parent_run_id
        )
        SELECT CASE WHEN source.user_input_mode = 'interactive' THEN source.run_id ELSE root.run_id END
        FROM source JOIN lineage root ON root.parent_run_id IS NULL
        WHERE root.user_input_mode = 'interactive'
          AND NOT EXISTS (SELECT 1 FROM lineage a WHERE a.session_id != source.session_id
            OR a.state NOT IN ('queued','running','waiting_for_approval'))")
        .bind(invocation.tool_call_id.as_str()).fetch_optional(&mut *connection).await.map_err(|_| Error::Unavailable)?;
    let owner = kiln_core::RunId::parse(owner.ok_or(Error::Conflict)?)
        .map_err(|_| Error::IntegrityViolation)?;
    // Once elicitation is presented, changed ancestry cannot silently retarget it.
    let recorded: Option<String> = sqlx::query_scalar(
        "SELECT f.interaction_run_id
        FROM mcp_elicitation_forms f JOIN mcp_inputs i USING (tool_call_id, ordinal)
        WHERE f.tool_call_id = ? AND i.state = 'required'
        UNION ALL SELECT u.interaction_run_id
        FROM mcp_elicitation_urls u JOIN mcp_inputs i USING (tool_call_id, ordinal)
        WHERE u.tool_call_id = ? AND i.state = 'required'",
    )
    .bind(invocation.tool_call_id.as_str())
    .bind(invocation.tool_call_id.as_str())
    .fetch_optional(connection)
    .await
    .map_err(|_| Error::Unavailable)?;
    if recorded.is_some_and(|recorded| recorded != owner.as_str()) {
        return Err(Error::Conflict);
    }
    Ok(owner)
}

pub(super) async fn current_invocation(
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

pub(super) async fn validate_live(
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

pub(super) async fn load(
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
