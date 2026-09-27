use std::num::NonZeroU64;

use kiln_core::{
    McpElicitationDecisionMutation, McpElicitationUrl, McpElicitationUrlContext,
    McpElicitationUrlDecision, McpElicitationUrlLimits, McpElicitationUrlMutation,
    McpElicitationUrlPolicy, McpElicitationUrlRecord, McpElicitationUrlStore, McpInputKind,
    McpInputRecord, McpInputState, McpInvocationError as Error, McpInvocationRecord,
};
use sqlx::{Connection, Row, SqliteConnection};

use super::{
    SqliteStore,
    mcp_input::{current_invocation, insert_input, interaction_owner, load, validate_live},
};

impl McpElicitationUrlStore for SqliteStore {
    async fn require_mcp_elicitation_url(
        &self,
        invocation: &McpInvocationRecord,
        ordinal: NonZeroU64,
        request: &McpElicitationUrl,
        limits: McpElicitationUrlLimits,
        policy: McpElicitationUrlPolicy,
    ) -> Result<McpElicitationUrlMutation, Error> {
        request.validate(limits, policy)?;
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
            let record = load_url(&mut tx, input, limits, policy)
                .await?
                .ok_or(Error::Conflict)?;
            if record.request != *request || record.interaction_run != interaction_run {
                return Err(Error::Conflict);
            }
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(McpElicitationUrlMutation::Existing(record));
        }
        let input = insert_input(&mut tx, current, ordinal, McpInputKind::Elicitation).await?;
        sqlx::query("INSERT INTO mcp_elicitation_urls (tool_call_id, ordinal, interaction_run_id, message, url, legacy_elicitation_id)
            VALUES (?, ?, ?, ?, ?, ?)")
            .bind(input.invocation.tool_call_id.as_str()).bind(i64::try_from(input.ordinal.get()).map_err(|_| Error::InvalidRequest)?)
            .bind(interaction_run.as_str()).bind(request.message()).bind(request.url())
            .bind(match request.context() { McpElicitationUrlContext::Legacy { elicitation_id } => Some(elicitation_id.as_str()), McpElicitationUrlContext::Stateless => None })
            .execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        self.mcp_invocation_events.send_replace(());
        Ok(McpElicitationUrlMutation::Applied(
            McpElicitationUrlRecord {
                input,
                interaction_run,
                request: request.clone(),
            },
        ))
    }

    async fn get_mcp_elicitation_url(
        &self,
        expected: &McpInputRecord,
        interaction_run: &kiln_core::RunId,
        limits: McpElicitationUrlLimits,
        policy: McpElicitationUrlPolicy,
    ) -> Result<McpElicitationUrlRecord, Error> {
        let mut connection = self.connection.lock().await;
        let mut tx = connection.begin().await.map_err(|_| Error::Unavailable)?;
        let record = pending_url(&mut tx, expected, interaction_run, limits, policy).await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(record)
    }
    async fn decide_mcp_elicitation_url(
        &self,
        expected: &McpElicitationUrlRecord,
        decision: &McpElicitationUrlDecision,
        limits: McpElicitationUrlLimits,
        policy: McpElicitationUrlPolicy,
    ) -> Result<McpElicitationDecisionMutation, Error> {
        let mut connection = self.connection.lock().await;
        let mut tx = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let current = pending_url(
            &mut tx,
            &expected.input,
            &expected.interaction_run,
            limits,
            policy,
        )
        .await?;
        if current != *expected {
            return Err(Error::Conflict);
        }
        if let Some(existing) = load_url_decision(&mut tx, &current.input).await? {
            if existing != *decision {
                return Err(Error::Conflict);
            }
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(McpElicitationDecisionMutation::Existing);
        }
        sqlx::query("INSERT INTO mcp_elicitation_url_decisions (tool_call_id, ordinal, action) VALUES (?, ?, ?)")
            .bind(current.input.invocation.tool_call_id.as_str())
            .bind(i64::try_from(current.input.ordinal.get()).map_err(|_| Error::InvalidRequest)?)
            .bind(decision.as_str()).execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        self.mcp_invocation_events.send_replace(());
        Ok(McpElicitationDecisionMutation::Applied)
    }

    async fn get_mcp_elicitation_url_decision(
        &self,
        expected: &McpElicitationUrlRecord,
        limits: McpElicitationUrlLimits,
        policy: McpElicitationUrlPolicy,
    ) -> Result<Option<McpElicitationUrlDecision>, Error> {
        let mut connection = self.connection.lock().await;
        let mut tx = connection.begin().await.map_err(|_| Error::Unavailable)?;
        let current = pending_url(
            &mut tx,
            &expected.input,
            &expected.interaction_run,
            limits,
            policy,
        )
        .await?;
        if current != *expected {
            return Err(Error::Conflict);
        }
        let decision = load_url_decision(&mut tx, &current.input).await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(decision)
    }
}

async fn pending_url(
    connection: &mut SqliteConnection,
    expected: &McpInputRecord,
    interaction_run: &kiln_core::RunId,
    limits: McpElicitationUrlLimits,
    policy: McpElicitationUrlPolicy,
) -> Result<McpElicitationUrlRecord, Error> {
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
    let record = load_url(connection, input, limits, policy)
        .await?
        .ok_or(Error::NotFound)?;
    if record.interaction_run != *interaction_run {
        return Err(Error::Conflict);
    }
    Ok(record)
}

async fn load_url_decision(
    connection: &mut SqliteConnection,
    input: &McpInputRecord,
) -> Result<Option<McpElicitationUrlDecision>, Error> {
    let action: Option<String> = sqlx::query_scalar(
        "SELECT action FROM mcp_elicitation_url_decisions WHERE tool_call_id = ? AND ordinal = ?",
    )
    .bind(input.invocation.tool_call_id.as_str())
    .bind(i64::try_from(input.ordinal.get()).map_err(|_| Error::InvalidRequest)?)
    .fetch_optional(connection)
    .await
    .map_err(|_| Error::Unavailable)?;
    action
        .map(|action| match action.as_str() {
            "accept" => Ok(McpElicitationUrlDecision::Accept),
            "decline" => Ok(McpElicitationUrlDecision::Decline),
            "cancel" => Ok(McpElicitationUrlDecision::Cancel),
            _ => Err(Error::IntegrityViolation),
        })
        .transpose()
}

async fn load_url(
    connection: &mut SqliteConnection,
    input: McpInputRecord,
    limits: McpElicitationUrlLimits,
    policy: McpElicitationUrlPolicy,
) -> Result<Option<McpElicitationUrlRecord>, Error> {
    // Enforce byte budgets before copying retained private strings into memory.
    let row = sqlx::query(
        "SELECT interaction_run_id,
        CASE WHEN length(CAST(message AS BLOB)) <= ? THEN message END AS message,
        CASE WHEN length(CAST(url AS BLOB)) <= ? THEN url END AS url,
        legacy_elicitation_id IS NOT NULL AS legacy,
        CASE WHEN length(CAST(legacy_elicitation_id AS BLOB)) <= ? THEN legacy_elicitation_id END AS legacy_id
        FROM mcp_elicitation_urls WHERE tool_call_id = ? AND ordinal = ?",
    )
    .bind(i64::try_from(limits.max_message_bytes.get()).unwrap_or(i64::MAX))
    .bind(i64::try_from(limits.max_url_bytes.get()).unwrap_or(i64::MAX))
    .bind(i64::try_from(limits.max_legacy_id_bytes.get()).unwrap_or(i64::MAX))
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
        let url: Option<String> = row.try_get("url").map_err(|_| Error::IntegrityViolation)?;
        let legacy: bool = row
            .try_get("legacy")
            .map_err(|_| Error::IntegrityViolation)?;
        let legacy_id: Option<String> = row
            .try_get("legacy_id")
            .map_err(|_| Error::IntegrityViolation)?;
        let context = if legacy {
            McpElicitationUrlContext::Legacy {
                elicitation_id: legacy_id.ok_or(Error::InvalidRequest)?,
            }
        } else {
            McpElicitationUrlContext::Stateless
        };
        Ok(McpElicitationUrlRecord {
            input,
            interaction_run: kiln_core::RunId::parse(owner)
                .map_err(|_| Error::IntegrityViolation)?,
            request: McpElicitationUrl::new(
                message.ok_or(Error::InvalidRequest)?,
                url.ok_or(Error::InvalidRequest)?,
                context,
                limits,
                policy,
            )?,
        })
    })
    .transpose()
}
