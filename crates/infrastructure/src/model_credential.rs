use kiln_core::{
    ModelInvocation, ModelInvocationCredential, ModelInvocationId,
    ModelInvocationStoreError as Error, ProviderAccountState, SecretRef, WorkspaceId,
};
use sqlx::{Row, SqliteConnection};

pub(super) async fn load(
    connection: &mut SqliteConnection,
    invocation_id: &ModelInvocationId,
) -> Result<Option<ModelInvocationCredential>, Error> {
    let row = sqlx::query(
        "SELECT c.workspace_id, c.secret_ref, s.workspace_id AS session_workspace
        FROM model_invocation_credentials c
        JOIN model_invocations i ON i.model_invocation_id = c.model_invocation_id
        JOIN runs r ON r.run_id = i.run_id JOIN sessions s ON s.session_id = r.session_id
        WHERE c.model_invocation_id = ?",
    )
    .bind(invocation_id.as_str())
    .fetch_optional(&mut *connection)
    .await
    .map_err(|_| Error::Unavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let workspace: String = row
        .try_get("workspace_id")
        .map_err(|_| Error::IntegrityViolation)?;
    let session_workspace: String = row
        .try_get("session_workspace")
        .map_err(|_| Error::IntegrityViolation)?;
    if workspace != session_workspace {
        return Err(Error::IntegrityViolation);
    }
    Ok(Some(ModelInvocationCredential::new(
        invocation_id.clone(),
        WorkspaceId::parse(workspace).map_err(|_| Error::IntegrityViolation)?,
        SecretRef::parse(
            row.try_get::<String, _>("secret_ref")
                .map_err(|_| Error::IntegrityViolation)?,
        )
        .map_err(|_| Error::IntegrityViolation)?,
    )))
}

/// Called inside the pending-to-in-flight transaction, before any Events commit.
/// Account-less fixtures remain unpinned; credential-backed adapters must reject
/// that absence before transport. Every known account is reauthorized here.
pub(super) async fn freeze(
    connection: &mut SqliteConnection,
    invocation: &ModelInvocation,
) -> Result<(), Error> {
    if load(connection, invocation.invocation_id())
        .await?
        .is_some()
    {
        return Err(Error::IntegrityViolation);
    }
    let account = super::provider_account::load_provider_account(
        connection,
        invocation.provider_account_id(),
    )
    .await
    .map_err(|_| Error::Unavailable)?;
    let credential = if let Some(account) = account {
        if account.provider_type() != invocation.settings().provider()
            || account.state() != ProviderAccountState::Connected
        {
            return Err(Error::CredentialRejected);
        }
        let workspace: Option<String> = sqlx::query_scalar(
            "SELECT s.workspace_id FROM runs r
            JOIN sessions s ON s.session_id = r.session_id
            JOIN provider_account_workspaces a ON a.workspace_id = s.workspace_id
            WHERE r.run_id = ? AND a.provider_account_id = ?",
        )
        .bind(invocation.run_id().as_str())
        .bind(invocation.provider_account_id().as_str())
        .fetch_optional(&mut *connection)
        .await
        .map_err(|_| Error::Unavailable)?;
        Some(ModelInvocationCredential::new(
            invocation.invocation_id().clone(),
            WorkspaceId::parse(workspace.ok_or(Error::CredentialRejected)?)
                .map_err(|_| Error::IntegrityViolation)?,
            account
                .secret_ref()
                .ok_or(Error::IntegrityViolation)?
                .clone(),
        ))
    } else {
        None
    };

    // A retry and every private replay source must retain the exact version.
    // Refresh/reconnect continuity is deliberately not inferred from account ID.
    let sources: Vec<String> = sqlx::query_scalar(
        "SELECT source_model_invocation_id FROM context_manifest_entries
        WHERE context_manifest_id = ? AND entry_kind = 'provider_continuation'",
    )
    .bind(invocation.context_manifest_id().as_str())
    .fetch_all(&mut *connection)
    .await
    .map_err(|_| Error::Unavailable)?;
    let mut source_ids = sources
        .into_iter()
        .map(|id| ModelInvocationId::parse(id).map_err(|_| Error::IntegrityViolation))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(retry) = invocation.retry_of() {
        source_ids.push(retry.clone());
    }
    for source in source_ids {
        let prior = load(connection, &source).await?;
        match (&credential, &prior) {
            (Some(current), Some(prior)) if current.same_version(prior) => {}
            (None, None) => {}
            _ => return Err(Error::CredentialRejected),
        }
    }
    if let Some(credential) = credential {
        sqlx::query("INSERT INTO model_invocation_credentials (model_invocation_id, workspace_id, secret_ref) VALUES (?, ?, ?)")
            .bind(credential.invocation_id().as_str()).bind(credential.workspace_id().as_str()).bind(credential.secret_ref().as_str())
            .execute(&mut *connection).await.map_err(|_| Error::Unavailable)?;
    }
    Ok(())
}
