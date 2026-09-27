use std::num::NonZeroUsize;

use kiln_core::{
    McpCommand, McpDefinitionLimits, McpGenerationId, McpInstanceKey, McpInstanceOwner,
    McpInstanceRecord, McpInvocationError as Error, McpInvocationMutation, McpInvocationRecord,
    McpInvocationState, McpInvocationStore, ModelToolExecutionRequest, ToolCallId,
};
use sqlx::{Connection, Row, SqliteConnection};

use super::SqliteStore;

impl McpInvocationStore for SqliteStore {
    async fn begin_mcp_invocation(
        &self,
        request: &ModelToolExecutionRequest<McpCommand>,
        target: &McpInstanceRecord,
        limits: McpDefinitionLimits,
    ) -> Result<McpInvocationMutation, Error> {
        let command = request.command();
        let tool = request.tool_call();
        if tool.capability() != command.capability()
            || command.server_id() != target.key.definition_id()
            || command.definition_version() != target.definition_version
            || target.key.canonical_json().len() > limits.max_metadata_bytes
        {
            return Err(Error::InvalidRequest);
        }
        let mut connection = self.connection.lock().await;
        let mut tx = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        if let Some(prior) = load(&mut tx, tool.tool_call_id()).await? {
            if prior.generation != target.generation {
                return Err(Error::Conflict);
            }
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(McpInvocationMutation::Existing(prior));
        }
        let (session, workspace) = native_claim_owner(&mut tx, request).await?;
        let owner_matches = match target.key.owner() {
            McpInstanceOwner::Core => true,
            McpInstanceOwner::Session(id) => id == &session,
            McpInstanceOwner::Workspace(id) => id == &workspace,
            McpInstanceOwner::WorkspaceCheckout(checkout) => {
                checkout.workspace_id() == &workspace && checkout.scope() == *request.scope()
            }
        };
        if !owner_matches {
            return Err(Error::ScopeMismatch);
        }
        super::mcp_instance::validate_owner(&mut tx, target.key.owner())
            .await
            .map_err(|_| Error::ScopeMismatch)?;
        let current_version: Option<i64> =
            sqlx::query_scalar("SELECT version FROM mcp_definitions WHERE definition_id = ?")
                .bind(command.server_id().as_str())
                .fetch_optional(&mut *tx)
                .await
                .map_err(|_| Error::Unavailable)?;
        let version =
            i64::try_from(command.definition_version()).map_err(|_| Error::InvalidRequest)?;
        if current_version != Some(version) {
            return Err(Error::DefinitionChanged);
        }
        let definition =
            super::mcp_definition::load_version(&mut tx, command.server_id(), version, limits)
                .await
                .map_err(|_| Error::DefinitionChanged)?;
        if !definition.definition.server().enabled {
            return Err(Error::DefinitionChanged);
        }
        let key = McpInstanceKey::new(
            &definition.definition,
            target.key.owner().clone(),
            limits.max_metadata_bytes,
        )
        .map_err(|_| Error::ScopeMismatch)?;
        if key.canonical_json() != target.key.canonical_json() {
            return Err(Error::ScopeMismatch);
        }
        if let Some(expected) = &target.host_binding_version {
            let host = current_host(&mut tx, &key, command.definition_version(), limits).await?;
            if host.revision != expected.revision
                || host.bindings.instance_id() != &expected.instance_id
            {
                return Err(Error::GenerationChanged);
            }
            let directory = host
                .bindings
                .working_directory()
                .ok_or(Error::ScopeMismatch)?;
            if directory.workspace_id() != &workspace || directory.scope() != *request.scope() {
                return Err(Error::ScopeMismatch);
            }
            super::mcp_host_binding::validate_directory(&mut tx, &host.bindings)
                .await
                .map_err(|_| Error::ScopeMismatch)?;
        }
        let ready: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM mcp_instances i
            JOIN mcp_instance_generations g ON g.generation_id = i.generation_id
            WHERE i.instance_key = ? AND g.generation_id = ? AND g.definition_version = ?
              AND g.state_version = ? AND g.observed = 'ready' AND g.desired = 'running'
              AND g.negotiated_protocol = ? AND g.host_instance_id IS ? AND g.host_binding_revision IS ?)",
        )
        .bind(key.canonical_json())
        .bind(target.generation.as_str())
        .bind(version)
        .bind(i64::try_from(target.state_version).map_err(|_| Error::InvalidRequest)?)
        .bind(target.negotiated_protocol.map(|p| p.as_str()))
        .bind(target.host_binding_version.as_ref().map(|v| v.instance_id.as_str()))
        .bind(target.host_binding_version.as_ref().map(|v| v.revision.get() as i64))
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| Error::Unavailable)?;
        if !ready {
            return Err(Error::GenerationChanged);
        }
        let busy: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM mcp_invocations WHERE generation_id = ? AND state = 'dispatching')")
            .bind(target.generation.as_str()).fetch_one(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        if busy {
            return Err(Error::Busy);
        }
        sqlx::query("INSERT INTO mcp_invocations (tool_call_id, generation_id, state) VALUES (?, ?, 'dispatching')")
            .bind(tool.tool_call_id().as_str()).bind(target.generation.as_str()).execute(&mut *tx).await
            .map_err(|_| Error::Unavailable)?;
        let record = McpInvocationRecord {
            tool_call_id: tool.tool_call_id().clone(),
            generation: target.generation.clone(),
            state: McpInvocationState::Dispatching,
        };
        append_event(&mut tx, &record).await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(McpInvocationMutation::Applied(record))
    }

    async fn finish_mcp_invocation(
        &self,
        expected: &McpInvocationRecord,
        state: McpInvocationState,
    ) -> Result<McpInvocationRecord, Error> {
        if state == McpInvocationState::Dispatching {
            return Err(Error::InvalidRequest);
        }
        let mut connection = self.connection.lock().await;
        let mut tx = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let mut current = load(&mut tx, &expected.tool_call_id)
            .await?
            .ok_or(Error::NotFound)?;
        if current.generation != expected.generation {
            return Err(Error::Conflict);
        }
        if current.state == state {
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(current);
        }
        if current.state != McpInvocationState::Dispatching
            || expected.state != McpInvocationState::Dispatching
        {
            return Err(Error::Conflict);
        }
        sqlx::query(
            "UPDATE mcp_invocations SET state = ? WHERE tool_call_id = ? AND state = 'dispatching'",
        )
        .bind(state.as_str())
        .bind(current.tool_call_id.as_str())
        .execute(&mut *tx)
        .await
        .map_err(|_| Error::Unavailable)?;
        current.state = state;
        append_event(&mut tx, &current).await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(current)
    }

    async fn interrupt_mcp_invocations(
        &self,
        generation: Option<&McpGenerationId>,
        batch_size: NonZeroUsize,
    ) -> Result<usize, Error> {
        let limit = i64::try_from(batch_size.get()).map_err(|_| Error::InvalidRequest)?;
        let mut connection = self.connection.lock().await;
        let mut tx = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let ids: Vec<String> = sqlx::query_scalar("SELECT tool_call_id FROM mcp_invocations
            WHERE state = 'dispatching' AND (? IS NULL OR generation_id = ?) ORDER BY tool_call_id LIMIT ?")
            .bind(generation.map(McpGenerationId::as_str)).bind(generation.map(McpGenerationId::as_str))
            .bind(limit).fetch_all(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        for id in &ids {
            sqlx::query("UPDATE mcp_invocations SET state = 'interrupted' WHERE tool_call_id = ?")
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(|_| Error::Unavailable)?;
            sqlx::query(
                "INSERT INTO mcp_invocation_events (tool_call_id, state) VALUES (?, 'interrupted')",
            )
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(|_| Error::Unavailable)?;
        }
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(ids.len())
    }
}

async fn load(
    connection: &mut SqliteConnection,
    id: &ToolCallId,
) -> Result<Option<McpInvocationRecord>, Error> {
    let row =
        sqlx::query("SELECT generation_id, state FROM mcp_invocations WHERE tool_call_id = ?")
            .bind(id.as_str())
            .fetch_optional(connection)
            .await
            .map_err(|_| Error::Unavailable)?;
    row.map(|row| {
        Ok(McpInvocationRecord {
            tool_call_id: id.clone(),
            generation: McpGenerationId::parse(
                row.try_get::<String, _>("generation_id")
                    .map_err(|_| Error::IntegrityViolation)?,
            )
            .map_err(|_| Error::IntegrityViolation)?,
            state: McpInvocationState::parse(
                &row.try_get::<String, _>("state")
                    .map_err(|_| Error::IntegrityViolation)?,
            )?,
        })
    })
    .transpose()
}

async fn append_event(
    connection: &mut SqliteConnection,
    record: &McpInvocationRecord,
) -> Result<(), Error> {
    sqlx::query("INSERT INTO mcp_invocation_events (tool_call_id, state) VALUES (?, ?)")
        .bind(record.tool_call_id.as_str())
        .bind(record.state.as_str())
        .execute(connection)
        .await
        .map_err(|_| Error::Unavailable)?;
    Ok(())
}

async fn native_claim_owner(
    connection: &mut SqliteConnection,
    request: &ModelToolExecutionRequest<McpCommand>,
) -> Result<(kiln_core::SessionId, kiln_core::WorkspaceId), Error> {
    if request.tool_call().capability() != request.command().capability() {
        return Err(Error::InvalidRequest);
    }
    let tool = request.tool_call();
    let command = request.command();
    let row = sqlx::query("SELECT s.session_id, s.workspace_id FROM tool_calls t
            JOIN runs r ON r.run_id = t.run_id JOIN sessions s ON s.session_id = r.session_id
            JOIN model_tool_adoptions a ON a.tool_call_id = t.tool_call_id
            JOIN model_tool_requests q ON q.model_invocation_id = a.model_invocation_id AND q.provider_call_id = a.provider_call_id
            WHERE t.tool_call_id = ? AND t.run_id = ? AND t.capability = ? AND t.state = 'running'
              AND r.state = 'running' AND a.model_invocation_id = ? AND a.provider_call_id = ?
              AND q.name = ? AND q.arguments_json = ?
              AND t.effective_workspace_root_id = ? AND t.effective_relative_directory = ?")
            .bind(tool.tool_call_id().as_str()).bind(tool.run_id().as_str()).bind(command.capability())
            .bind(request.invocation_id().as_str()).bind(request.provider_call_id())
            .bind(command.tool_name()).bind(command.canonical_json()).bind(request.scope().workspace_root_id().as_str())
            .bind(request.scope().relative_directory()).fetch_optional(&mut *connection).await
            .map_err(|_| Error::Unavailable)?.ok_or(Error::InvalidRequest)?;
    let session: String = row
        .try_get("session_id")
        .map_err(|_| Error::IntegrityViolation)?;
    let workspace: String = row
        .try_get("workspace_id")
        .map_err(|_| Error::IntegrityViolation)?;
    Ok((
        kiln_core::SessionId::parse(session).map_err(|_| Error::IntegrityViolation)?,
        kiln_core::WorkspaceId::parse(workspace).map_err(|_| Error::IntegrityViolation)?,
    ))
}

impl kiln_core::McpLaunchStore for SqliteStore {
    async fn inspect_mcp_launch(
        &self,
        request: &ModelToolExecutionRequest<McpCommand>,
        limits: McpDefinitionLimits,
    ) -> Result<kiln_core::McpLaunchContext, Error> {
        limits.validate().map_err(|_| Error::InvalidRequest)?;
        let mut connection = self.connection.lock().await;
        let mut tx = connection.begin().await.map_err(|_| Error::Unavailable)?;
        let (session, workspace) = native_claim_owner(&mut tx, request).await?;
        if load(&mut tx, request.tool_call().tool_call_id())
            .await?
            .is_some()
        {
            return Err(Error::Conflict);
        }
        let version = i64::try_from(request.command().definition_version())
            .map_err(|_| Error::InvalidRequest)?;
        let current: Option<i64> =
            sqlx::query_scalar("SELECT version FROM mcp_definitions WHERE definition_id = ?")
                .bind(request.command().server_id().as_str())
                .fetch_optional(&mut *tx)
                .await
                .map_err(|_| Error::Unavailable)?;
        if current != Some(version) {
            return Err(Error::DefinitionChanged);
        }
        let definition = super::mcp_definition::load_version(
            &mut tx,
            request.command().server_id(),
            version,
            limits,
        )
        .await
        .map_err(|_| Error::DefinitionChanged)?;
        if !definition.definition.server().enabled
            || !matches!(
                definition.definition.server().transport,
                kiln_core::SharedMcpTransport::Stdio { .. }
            )
        {
            return Err(Error::DefinitionChanged);
        }
        let root = sqlx::query("SELECT canonical_path, git_common_directory_path, filesystem_identity FROM workspace_roots WHERE workspace_id = ? AND workspace_root_id = ? AND state = 'available'")
            .bind(workspace.as_str()).bind(request.scope().workspace_root_id().as_str())
            .fetch_optional(&mut *tx).await.map_err(|_| Error::Unavailable)?.ok_or(Error::ScopeMismatch)?;
        let directory = kiln_core::WorkspaceCheckout::from_resolved_paths(
            workspace.clone(),
            request.scope().workspace_root_id().clone(),
            request.scope().relative_directory(),
            root.try_get::<String, _>("canonical_path")
                .map_err(|_| Error::IntegrityViolation)?,
            root.try_get::<String, _>("git_common_directory_path")
                .map_err(|_| Error::IntegrityViolation)?,
            kiln_core::FilesystemIdentity::new(
                root.try_get::<String, _>("filesystem_identity")
                    .map_err(|_| Error::IntegrityViolation)?,
            )
            .ok_or(Error::IntegrityViolation)?,
        )
        .map_err(|_| Error::ScopeMismatch)?;
        let owner = match definition.definition.scope() {
            kiln_core::McpLifecycleScope::Core => McpInstanceOwner::Core,
            kiln_core::McpLifecycleScope::Session => McpInstanceOwner::Session(session),
            kiln_core::McpLifecycleScope::Workspace => McpInstanceOwner::Workspace(workspace),
            kiln_core::McpLifecycleScope::WorkspaceCheckout => {
                McpInstanceOwner::WorkspaceCheckout(directory.clone())
            }
        };
        let key = McpInstanceKey::new(&definition.definition, owner, limits.max_metadata_bytes)
            .map_err(|_| Error::ScopeMismatch)?;
        let host = current_host(
            &mut tx,
            &key,
            request.command().definition_version(),
            limits,
        )
        .await?;
        if host.bindings.working_directory() != Some(&directory) {
            return Err(Error::ScopeMismatch);
        }
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(kiln_core::McpLaunchContext {
            definition,
            host,
            directory,
        })
    }
}

async fn current_host(
    connection: &mut SqliteConnection,
    key: &McpInstanceKey,
    definition_version: u64,
    limits: McpDefinitionLimits,
) -> Result<kiln_core::McpHostBindingRecord, Error> {
    let revision: Option<i64> =
        sqlx::query_scalar("SELECT revision FROM mcp_host_bindings WHERE instance_key = ?")
            .bind(key.canonical_json())
            .fetch_optional(&mut *connection)
            .await
            .map_err(|_| Error::Unavailable)?;
    let host = super::mcp_host_binding::load_version(
        connection,
        key,
        revision.ok_or(Error::NotFound)?,
        limits,
    )
    .await
    .map_err(|e| match e {
        kiln_core::McpHostBindingError::Unavailable => Error::Unavailable,
        _ => Error::IntegrityViolation,
    })?
    .ok_or(Error::NotFound)?;
    let local: Option<String> =
        sqlx::query_scalar("SELECT instance_id FROM configuration_instance WHERE singleton = 1")
            .fetch_optional(&mut *connection)
            .await
            .map_err(|_| Error::Unavailable)?;
    if host.retired
        || host.bindings.definition_version() != definition_version
        || local.as_deref() != Some(host.bindings.instance_id().as_str())
    {
        return Err(Error::DefinitionChanged);
    }
    Ok(host)
}
