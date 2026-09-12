use kiln_core::{
    AssistantMessageIdGenerator, AssistantMessageMutation, AssistantMessageOrigin,
    AssistantMessageStore, AssistantMessageStoreError, ChildActivityReference, MessageStatus,
    ModelInvocationCompletionKind, ModelOutputStream, PersistedMessage,
};

use super::*;

impl AssistantMessageIdGenerator for UlidIdGenerator {
    fn message_id(&self) -> MessageId {
        MessageId::from_ulid(Ulid::generate())
    }

    fn event_id(&self) -> EventId {
        EventId::from_ulid(Ulid::generate())
    }
}

pub(super) fn parse_message_row(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<Message, AssistantMessageStoreError> {
    let field = |name| {
        row.try_get::<String, _>(name)
            .map_err(|_| AssistantMessageStoreError::IntegrityViolation)
    };
    let optional = |name| {
        row.try_get::<Option<String>, _>(name)
            .map_err(|_| AssistantMessageStoreError::IntegrityViolation)
    };
    let origin = match (optional("origin_run_id")?, optional("model_invocation_id")?) {
        (None, None) => None,
        (Some(run_id), Some(model_invocation_id)) => Some(AssistantMessageOrigin {
            run_id: RunId::parse(run_id)
                .map_err(|_| AssistantMessageStoreError::IntegrityViolation)?,
            model_invocation_id: ModelInvocationId::parse(model_invocation_id)
                .map_err(|_| AssistantMessageStoreError::IntegrityViolation)?,
        }),
        _ => return Err(AssistantMessageStoreError::IntegrityViolation),
    };
    let message = Message::from_persisted(PersistedMessage {
        id: MessageId::parse(field("message_id")?)
            .map_err(|_| AssistantMessageStoreError::IntegrityViolation)?,
        session_id: SessionId::parse(field("session_id")?)
            .map_err(|_| AssistantMessageStoreError::IntegrityViolation)?,
        role: MessageRole::parse(&field("role")?)
            .map_err(|_| AssistantMessageStoreError::IntegrityViolation)?,
        content: field("content")?,
        target_run_id: optional("target_run_id")?
            .map(RunId::parse)
            .transpose()
            .map_err(|_| AssistantMessageStoreError::IntegrityViolation)?,
        status: MessageStatus::parse(&field("status")?)
            .map_err(|_| AssistantMessageStoreError::IntegrityViolation)?,
        origin,
    })
    .map_err(|_| AssistantMessageStoreError::IntegrityViolation)?;
    match (
        optional("child_activity_run_id")?,
        optional("child_activity_event_id")?,
    ) {
        (None, None) => Ok(message),
        (Some(run_id), Some(event_id)) => message
            .with_child_activity(ChildActivityReference {
                run_id: RunId::parse(run_id)
                    .map_err(|_| AssistantMessageStoreError::IntegrityViolation)?,
                event_id: EventId::parse(event_id)
                    .map_err(|_| AssistantMessageStoreError::IntegrityViolation)?,
            })
            .map_err(|_| AssistantMessageStoreError::IntegrityViolation),
        _ => Err(AssistantMessageStoreError::IntegrityViolation),
    }
}

pub(super) async fn load_message(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    message_id: &MessageId,
) -> Result<Option<Message>, AssistantMessageStoreError> {
    let row = sqlx::query(
        "SELECT message_id, session_id, role, content, target_run_id,
                status, origin_run_id, model_invocation_id,
                child_activity_run_id, child_activity_event_id
         FROM messages WHERE message_id = ?",
    )
    .bind(message_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| AssistantMessageStoreError::Unavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let message = parse_message_row(&row)?;
    if message.id() != message_id {
        return Err(AssistantMessageStoreError::IntegrityViolation);
    }
    validate_origin(transaction, &message).await?;
    Ok(Some(message))
}

async fn validate_origin(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    message: &Message,
) -> Result<(), AssistantMessageStoreError> {
    let Some(origin) = message.origin() else {
        if let Some(target_run_id) = message.target_run_id() {
            let session_id =
                sqlx::query_scalar::<_, String>("SELECT session_id FROM runs WHERE run_id = ?")
                    .bind(target_run_id.as_str())
                    .fetch_optional(&mut **transaction)
                    .await
                    .map_err(|_| AssistantMessageStoreError::Unavailable)?;
            if session_id.as_deref() != Some(message.session_id().as_str()) {
                return Err(AssistantMessageStoreError::IntegrityViolation);
            }
        }
        return Ok(());
    };
    // Context validation uses this loader; loading the full invocation here
    // would recursively validate its context and the same source messages.
    let row = sqlx::query(
        "SELECT i.run_id, i.state AS invocation_state, i.completion_kind,
                i.terminal_reason, i.purpose, r.session_id, r.state AS run_state
         FROM model_invocations i JOIN runs r ON r.run_id = i.run_id
         WHERE i.model_invocation_id = ?",
    )
    .bind(origin.model_invocation_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| AssistantMessageStoreError::Unavailable)?
    .ok_or(AssistantMessageStoreError::IntegrityViolation)?;
    let field = |name| {
        row.try_get::<String, _>(name)
            .map_err(|_| AssistantMessageStoreError::IntegrityViolation)
    };
    let state = ModelInvocationState::parse(&field("invocation_state")?)
        .map_err(|_| AssistantMessageStoreError::IntegrityViolation)?;
    let completion = row
        .try_get::<Option<String>, _>("completion_kind")
        .map_err(|_| AssistantMessageStoreError::IntegrityViolation)?;
    let reason = row
        .try_get::<Option<String>, _>("terminal_reason")
        .map_err(|_| AssistantMessageStoreError::IntegrityViolation)?;
    let outcome =
        ModelInvocationOutcome::from_persisted(state, completion.as_deref(), reason.as_deref())
            .map_err(|_| AssistantMessageStoreError::IntegrityViolation)?
            .ok_or(AssistantMessageStoreError::IntegrityViolation)?;
    let run_state = RunState::parse(&field("run_state")?)
        .map_err(|_| AssistantMessageStoreError::IntegrityViolation)?;
    if field("run_id")? != origin.run_id.as_str()
        || field("session_id")? != message.session_id().as_str()
        || field("purpose")? != ModelInvocationPurpose::Generation.as_str()
        || outcome.completion_kind() == Some(ModelInvocationCompletionKind::ToolRequests)
        || match message.status() {
            MessageStatus::Complete => {
                run_state != RunState::Completed
                    || outcome.completion_kind()
                        != Some(ModelInvocationCompletionKind::AssistantOutput)
            }
            MessageStatus::Incomplete => {
                !matches!(run_state, RunState::Failed | RunState::Cancelled)
            }
        }
    {
        return Err(AssistantMessageStoreError::IntegrityViolation);
    }
    Ok(())
}

async fn assistant_text(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    invocation_id: &ModelInvocationId,
) -> Result<String, AssistantMessageStoreError> {
    let chunks = model_output::load_chunks(transaction, invocation_id)
        .await
        .map_err(|error| match error {
            kiln_core::ModelOutputStoreError::Unavailable => {
                AssistantMessageStoreError::Unavailable
            }
            _ => AssistantMessageStoreError::IntegrityViolation,
        })?;
    Ok(chunks
        .into_iter()
        .filter(|chunk| chunk.stream == ModelOutputStream::AssistantText)
        .map(|chunk| chunk.content)
        .collect())
}

async fn validate_successful_completion(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    invocation: &ModelInvocation,
) -> Result<(), AssistantMessageStoreError> {
    if invocation
        .outcome()
        .and_then(ModelInvocationOutcome::completion_kind)
        != Some(ModelInvocationCompletionKind::AssistantOutput)
    {
        return Err(AssistantMessageStoreError::InvocationNotComplete);
    }
    let latest = sqlx::query_scalar::<_, String>(
        "SELECT model_invocation_id FROM model_invocations
         WHERE run_id = ? ORDER BY sequence DESC LIMIT 1",
    )
    .bind(invocation.run_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| AssistantMessageStoreError::Unavailable)?;
    if latest.as_deref() != Some(invocation.invocation_id().as_str()) {
        return Err(AssistantMessageStoreError::InvocationNotLatest);
    }
    if !usage::has_final_usage(transaction, invocation)
        .await
        .map_err(|error| match error {
            kiln_core::UsageStoreError::Unavailable => AssistantMessageStoreError::Unavailable,
            _ => AssistantMessageStoreError::IntegrityViolation,
        })?
    {
        return Err(AssistantMessageStoreError::FinalUsageRequired);
    }
    let queued = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM message_deliveries WHERE run_id = ? AND state = 'queued')",
    )
    .bind(invocation.run_id().as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(|_| AssistantMessageStoreError::Unavailable)?;
    if queued {
        return Err(AssistantMessageStoreError::QueuedInputPending);
    }
    let active_tools = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM tool_calls WHERE run_id = ?
         AND state IN ('requested', 'awaiting_approval', 'ready', 'running'))",
    )
    .bind(invocation.run_id().as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(|_| AssistantMessageStoreError::Unavailable)?;
    if active_tools {
        return Err(AssistantMessageStoreError::ActiveToolCalls);
    }
    if has_active_model_invocation(transaction, invocation.run_id())
        .await
        .map_err(|_| AssistantMessageStoreError::Unavailable)?
    {
        return Err(AssistantMessageStoreError::ActiveInvocations);
    }
    if has_non_terminal_descendants(transaction, invocation.run_id())
        .await
        .map_err(|_| AssistantMessageStoreError::Unavailable)?
    {
        return Err(AssistantMessageStoreError::ActiveDescendants);
    }
    Ok(())
}

impl AssistantMessageStore for SqliteStore {
    async fn finalize_assistant_message(
        &self,
        invocation_id: &ModelInvocationId,
        message_id: MessageId,
        message_event_id: EventId,
        run_event_id: EventId,
    ) -> Result<AssistantMessageMutation, AssistantMessageStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| AssistantMessageStoreError::Unavailable)?;
        let invocation = load_model_invocation(&mut transaction, invocation_id)
            .await
            .map_err(|error| match error {
                ModelInvocationStoreError::Unavailable => AssistantMessageStoreError::Unavailable,
                _ => AssistantMessageStoreError::IntegrityViolation,
            })?
            .ok_or(AssistantMessageStoreError::ModelInvocationNotFound)?;
        let run = load_run(&mut transaction, invocation.run_id())
            .await
            .map_err(|_| AssistantMessageStoreError::Unavailable)?
            .ok_or(AssistantMessageStoreError::RunNotFound)?;
        if let Some(stored_id) = sqlx::query_scalar::<_, String>(
            "SELECT message_id FROM messages WHERE model_invocation_id = ?",
        )
        .bind(invocation_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| AssistantMessageStoreError::Unavailable)?
        {
            let stored_id = MessageId::parse(stored_id)
                .map_err(|_| AssistantMessageStoreError::IntegrityViolation)?;
            let message = load_message(&mut transaction, &stored_id)
                .await?
                .ok_or(AssistantMessageStoreError::IntegrityViolation)?;
            if message
                .origin()
                .is_none_or(|origin| origin.model_invocation_id != *invocation_id)
                || message.content() != assistant_text(&mut transaction, invocation_id).await?
            {
                return Err(AssistantMessageStoreError::IntegrityViolation);
            }
            transaction
                .commit()
                .await
                .map_err(|_| AssistantMessageStoreError::Unavailable)?;
            return Ok(AssistantMessageMutation {
                message,
                run,
                events: Vec::new(),
                disposition: ModelInvocationMutationDisposition::Duplicate,
            });
        }
        if invocation.purpose() != ModelInvocationPurpose::Generation
            || !invocation.state().is_terminal()
            || invocation
                .outcome()
                .and_then(ModelInvocationOutcome::completion_kind)
                == Some(ModelInvocationCompletionKind::ToolRequests)
        {
            return Err(AssistantMessageStoreError::InvocationNotComplete);
        }
        let (status, next_run) = match run.state() {
            RunState::Running => {
                validate_successful_completion(&mut transaction, &invocation).await?;
                (
                    MessageStatus::Complete,
                    run.transition(RunState::Completed)
                        .map_err(|_| AssistantMessageStoreError::RunNotEligible)?,
                )
            }
            RunState::Failed | RunState::Cancelled => (MessageStatus::Incomplete, run.clone()),
            _ => return Err(AssistantMessageStoreError::RunNotEligible),
        };
        let content = assistant_text(&mut transaction, invocation_id).await?;
        if content.is_empty() {
            return Err(AssistantMessageStoreError::NoAssistantText);
        }
        let message = Message::new_assistant(
            message_id,
            run.session_id().clone(),
            AssistantMessageOrigin {
                run_id: run.run_id().clone(),
                model_invocation_id: invocation_id.clone(),
            },
            status,
            content,
        )
        .map_err(|_| AssistantMessageStoreError::IntegrityViolation)?;
        let inserted = sqlx::query(
            "INSERT INTO messages (
                message_id, session_id, role, content, status, origin_run_id, model_invocation_id
             ) VALUES (?, ?, 'assistant', ?, ?, ?, ?)",
        )
        .bind(message.id().as_str())
        .bind(message.session_id().as_str())
        .bind(message.content())
        .bind(message.status().as_str())
        .bind(run.run_id().as_str())
        .bind(invocation_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| AssistantMessageStoreError::Unavailable)?;
        if inserted.rows_affected() != 1 {
            return Err(AssistantMessageStoreError::IntegrityViolation);
        }
        let mut events = vec![
            insert_message_event(
                &mut transaction,
                &SessionEvent::message_appended(message_event_id, message.clone()),
            )
            .await
            .map_err(|_| AssistantMessageStoreError::Unavailable)?,
        ];
        if status == MessageStatus::Complete {
            let updated = sqlx::query(
                "UPDATE runs SET state = 'completed' WHERE run_id = ? AND state = 'running'",
            )
            .bind(run.run_id().as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|_| AssistantMessageStoreError::Unavailable)?;
            if updated.rows_affected() != 1 {
                return Err(AssistantMessageStoreError::RunNotEligible);
            }
            events.push(
                insert_run_event(
                    &mut transaction,
                    &SessionEvent::run_state_changed(run_event_id, &next_run),
                )
                .await
                .map_err(|_| AssistantMessageStoreError::Unavailable)?,
            );
        }
        transaction
            .commit()
            .await
            .map_err(|_| AssistantMessageStoreError::Unavailable)?;
        Ok(AssistantMessageMutation {
            message,
            run: next_run,
            events,
            disposition: ModelInvocationMutationDisposition::Applied,
        })
    }
}
