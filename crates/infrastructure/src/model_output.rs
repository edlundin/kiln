use std::collections::HashMap;

use kiln_core::{
    ModelOutputChunk, ModelOutputChunkId, ModelOutputIdGenerator, ModelOutputMutation,
    ModelOutputStore, ModelOutputStoreError, ModelOutputStream, RecordModelOutput,
};

use super::*;

impl ModelOutputIdGenerator for UlidIdGenerator {
    fn output_chunk_id(&self) -> ModelOutputChunkId {
        ModelOutputChunkId::from_ulid(Ulid::generate())
    }

    fn event_id(&self) -> EventId {
        EventId::from_ulid(Ulid::generate())
    }
}

async fn load_owner(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    invocation_id: &ModelInvocationId,
) -> Result<(ModelInvocation, SessionId), ModelOutputStoreError> {
    let invocation = load_model_invocation(transaction, invocation_id)
        .await
        .map_err(|error| match error {
            ModelInvocationStoreError::Unavailable => ModelOutputStoreError::Unavailable,
            _ => ModelOutputStoreError::IntegrityViolation,
        })?
        .ok_or(ModelOutputStoreError::ModelInvocationNotFound)?;
    let run = load_run(transaction, invocation.run_id())
        .await
        .map_err(|_| ModelOutputStoreError::Unavailable)?
        .ok_or(ModelOutputStoreError::IntegrityViolation)?;
    Ok((invocation, run.session_id().clone()))
}

fn parse_chunk(
    row: &sqlx::sqlite::SqliteRow,
    invocation: &ModelInvocation,
    session_id: &SessionId,
) -> Result<ModelOutputChunk, ModelOutputStoreError> {
    let field = |name| {
        row.try_get::<String, _>(name)
            .map_err(|_| ModelOutputStoreError::IntegrityViolation)
    };
    let output_chunk_id = ModelOutputChunkId::parse(field("output_chunk_id")?)
        .map_err(|_| ModelOutputStoreError::IntegrityViolation)?;
    let model_invocation_id = ModelInvocationId::parse(field("model_invocation_id")?)
        .map_err(|_| ModelOutputStoreError::IntegrityViolation)?;
    let run_id =
        RunId::parse(field("run_id")?).map_err(|_| ModelOutputStoreError::IntegrityViolation)?;
    let stored_session_id = SessionId::parse(field("session_id")?)
        .map_err(|_| ModelOutputStoreError::IntegrityViolation)?;
    let position = u64::try_from(
        row.try_get::<i64, _>("position")
            .map_err(|_| ModelOutputStoreError::IntegrityViolation)?,
    )
    .map_err(|_| ModelOutputStoreError::IntegrityViolation)?;
    let stream = ModelOutputStream::parse(&field("stream")?)
        .map_err(|_| ModelOutputStoreError::IntegrityViolation)?;
    let command = RecordModelOutput::new(
        model_invocation_id,
        field("update_id")?,
        position,
        stream,
        field("content")?,
    )
    .map_err(|_| ModelOutputStoreError::IntegrityViolation)?;
    if command.model_invocation_id() != invocation.invocation_id()
        || &run_id != invocation.run_id()
        || &stored_session_id != session_id
    {
        return Err(ModelOutputStoreError::IntegrityViolation);
    }
    Ok(ModelOutputChunk {
        output_chunk_id,
        model_invocation_id: invocation.invocation_id().clone(),
        run_id,
        session_id: stored_session_id,
        update_id: command.update_id().to_owned(),
        position,
        stream,
        content: command.content().to_owned(),
    })
}

pub(super) async fn load_chunks(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    invocation_id: &ModelInvocationId,
) -> Result<Vec<ModelOutputChunk>, ModelOutputStoreError> {
    let (invocation, session_id) = load_owner(transaction, invocation_id).await?;
    let rows = sqlx::query(
        "SELECT output_chunk_id, model_invocation_id, run_id, session_id,
                update_id, position, stream, content
         FROM model_output_chunks WHERE model_invocation_id = ? ORDER BY position",
    )
    .bind(invocation_id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(|_| ModelOutputStoreError::Unavailable)?;
    let mut chunks = Vec::with_capacity(rows.len());
    let mut expected_position = 0u64;
    for row in rows {
        let chunk = parse_chunk(&row, &invocation, &session_id)?;
        expected_position = expected_position
            .checked_add(1)
            .ok_or(ModelOutputStoreError::IntegrityViolation)?;
        if chunk.position != expected_position {
            return Err(ModelOutputStoreError::IntegrityViolation);
        }
        chunks.push(chunk);
    }
    Ok(chunks)
}

pub(super) async fn load_event_chunk(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    output_chunk_id: &str,
    invocation_id: &str,
    cache: &mut HashMap<ModelInvocationId, HashMap<String, ModelOutputChunk>>,
) -> Result<ModelOutputChunk, StoreError> {
    let invocation_id =
        ModelInvocationId::parse(invocation_id).map_err(|_| StoreError::Unavailable)?;
    if !cache.contains_key(&invocation_id) {
        let chunks = load_chunks(transaction, &invocation_id)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        cache.insert(
            invocation_id.clone(),
            chunks
                .into_iter()
                .map(|chunk| (chunk.output_chunk_id.as_str().to_owned(), chunk))
                .collect(),
        );
    }
    cache
        .get(&invocation_id)
        .and_then(|chunks| chunks.get(output_chunk_id))
        .cloned()
        .ok_or(StoreError::Unavailable)
}

impl ModelOutputStore for SqliteStore {
    async fn record_model_output(
        &self,
        command: &RecordModelOutput,
        output_chunk_id: ModelOutputChunkId,
        event_id: EventId,
    ) -> Result<ModelOutputMutation, ModelOutputStoreError> {
        let position =
            i64::try_from(command.position()).map_err(|_| ModelOutputStoreError::InvalidChunk)?;
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| ModelOutputStoreError::Unavailable)?;
        let (invocation, session_id) =
            load_owner(&mut transaction, command.model_invocation_id()).await?;
        if let Some(row) = sqlx::query(
            "SELECT output_chunk_id, model_invocation_id, run_id, session_id,
                    update_id, position, stream, content
             FROM model_output_chunks WHERE model_invocation_id = ? AND update_id = ?",
        )
        .bind(command.model_invocation_id().as_str())
        .bind(command.update_id())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| ModelOutputStoreError::Unavailable)?
        {
            let chunk = parse_chunk(&row, &invocation, &session_id)?;
            if chunk.position != command.position()
                || chunk.stream != command.stream()
                || chunk.content != command.content()
            {
                return Err(ModelOutputStoreError::IdempotencyConflict);
            }
            transaction
                .commit()
                .await
                .map_err(|_| ModelOutputStoreError::Unavailable)?;
            return Ok(ModelOutputMutation {
                value: chunk,
                events: Vec::new(),
                disposition: ModelInvocationMutationDisposition::Duplicate,
            });
        }
        if invocation.state() != ModelInvocationState::InFlight {
            return Err(ModelOutputStoreError::InvocationNotInFlight);
        }
        // ponytail: checking all position index entries per append is O(n²) over
        // a stream; use validated next-position metadata if measured streams require it.
        let sequence = sqlx::query(
            "SELECT COUNT(*) AS chunk_count, COALESCE(MAX(position), 0) AS last_position
             FROM model_output_chunks WHERE model_invocation_id = ?",
        )
        .bind(command.model_invocation_id().as_str())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| ModelOutputStoreError::Unavailable)?;
        let count = sequence
            .try_get::<i64, _>("chunk_count")
            .map_err(|_| ModelOutputStoreError::IntegrityViolation)?;
        let last_position = sequence
            .try_get::<i64, _>("last_position")
            .map_err(|_| ModelOutputStoreError::IntegrityViolation)?;
        if count < 0 || count != last_position {
            return Err(ModelOutputStoreError::IntegrityViolation);
        }
        if last_position.checked_add(1) != Some(position) {
            return Err(ModelOutputStoreError::OutOfOrder);
        }
        let chunk = ModelOutputChunk {
            output_chunk_id,
            model_invocation_id: invocation.invocation_id().clone(),
            run_id: invocation.run_id().clone(),
            session_id,
            update_id: command.update_id().to_owned(),
            position: command.position(),
            stream: command.stream(),
            content: command.content().to_owned(),
        };
        sqlx::query(
            "INSERT INTO model_output_chunks (
                output_chunk_id, model_invocation_id, run_id, session_id,
                update_id, position, stream, content
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(chunk.output_chunk_id.as_str())
        .bind(chunk.model_invocation_id.as_str())
        .bind(chunk.run_id.as_str())
        .bind(chunk.session_id.as_str())
        .bind(&chunk.update_id)
        .bind(position)
        .bind(chunk.stream.as_str())
        .bind(&chunk.content)
        .execute(&mut *transaction)
        .await
        .map_err(|_| ModelOutputStoreError::Unavailable)?;
        let result = sqlx::query(
            "INSERT INTO session_events (
                event_id, session_id, event_type, run_id, model_invocation_id, output_chunk_id
             ) VALUES (?, ?, 'model_invocation.output', ?, ?, ?)",
        )
        .bind(event_id.as_str())
        .bind(chunk.session_id.as_str())
        .bind(chunk.run_id.as_str())
        .bind(chunk.model_invocation_id.as_str())
        .bind(chunk.output_chunk_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| ModelOutputStoreError::Unavailable)?;
        let cursor = committed_cursor(result.last_insert_rowid())
            .map_err(|_| ModelOutputStoreError::IntegrityViolation)?;
        let event = SessionEvent::model_output_recorded(event_id, chunk.clone());
        let stored = StoredSessionEvent::from_event(&event, cursor)
            .map_err(|_| ModelOutputStoreError::IntegrityViolation)?;
        transaction
            .commit()
            .await
            .map_err(|_| ModelOutputStoreError::Unavailable)?;
        Ok(ModelOutputMutation {
            value: chunk,
            events: vec![stored],
            disposition: ModelInvocationMutationDisposition::Applied,
        })
    }

    async fn list_model_output(
        &self,
        model_invocation_id: &ModelInvocationId,
    ) -> Result<Vec<ModelOutputChunk>, ModelOutputStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| ModelOutputStoreError::Unavailable)?;
        let chunks = load_chunks(&mut transaction, model_invocation_id).await?;
        transaction
            .commit()
            .await
            .map_err(|_| ModelOutputStoreError::Unavailable)?;
        Ok(chunks)
    }
}
