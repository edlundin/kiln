use std::collections::BTreeMap;

use kiln_core::{
    ProviderAccount, ProviderAccountId, ProviderAccountState, ProviderAccountStore,
    ProviderAccountStoreError, ProviderType, SecretRef, WorkspaceId,
};
use sqlx::{Row, SqliteConnection};

use super::SqliteStore;

impl ProviderAccountStore for SqliteStore {
    async fn reserve_provider_account_secret(
        &self,
        expected: &ProviderAccount,
        secret_ref: &SecretRef,
    ) -> Result<(), ProviderAccountStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = sqlx::Connection::begin(&mut *connection)
            .await
            .map_err(|_| ProviderAccountStoreError::Unavailable)?;
        let current = load_provider_account(&mut transaction, expected.id())
            .await?
            .ok_or(ProviderAccountStoreError::AccountNotFound)?;
        if current != *expected || current.secret_ref() == Some(secret_ref) {
            return Err(ProviderAccountStoreError::IntegrityViolation);
        }
        sqlx::query("INSERT INTO provider_account_secret_cleanup (secret_ref, provider_account_id) VALUES (?, ?)")
            .bind(secret_ref.as_str()).bind(expected.id().as_str())
            .execute(&mut *transaction).await
            .map_err(|_| ProviderAccountStoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| ProviderAccountStoreError::Unavailable)
    }

    async fn pending_provider_account_secret_cleanup(
        &self,
        id: &ProviderAccountId,
    ) -> Result<Vec<SecretRef>, ProviderAccountStoreError> {
        let mut connection = self.connection.lock().await;
        sqlx::query_scalar::<_, String>(
            "SELECT secret_ref FROM provider_account_secret_cleanup WHERE provider_account_id = ? ORDER BY secret_ref",
        ).bind(id.as_str()).fetch_all(&mut *connection).await
            .map_err(|_| ProviderAccountStoreError::Unavailable)?
            .into_iter().map(|value| SecretRef::parse(value)
                .map_err(|_| ProviderAccountStoreError::IntegrityViolation)).collect()
    }

    async fn finish_provider_account_secret_cleanup(
        &self,
        id: &ProviderAccountId,
        secret_ref: &SecretRef,
    ) -> Result<(), ProviderAccountStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = sqlx::Connection::begin(&mut *connection)
            .await
            .map_err(|_| ProviderAccountStoreError::Unavailable)?;
        let current = load_provider_account(&mut transaction, id)
            .await?
            .ok_or(ProviderAccountStoreError::AccountNotFound)?;
        if current.secret_ref() == Some(secret_ref) {
            return Err(ProviderAccountStoreError::IntegrityViolation);
        }
        sqlx::query("DELETE FROM provider_account_secret_cleanup WHERE provider_account_id = ? AND secret_ref = ?")
            .bind(id.as_str()).bind(secret_ref.as_str()).execute(&mut *transaction).await
            .map_err(|_| ProviderAccountStoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| ProviderAccountStoreError::Unavailable)
    }

    async fn create_provider_account(
        &self,
        account: &ProviderAccount,
        workspace_ids: &[WorkspaceId],
    ) -> Result<(), ProviderAccountStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = sqlx::Connection::begin(&mut *connection)
            .await
            .map_err(|_| ProviderAccountStoreError::Unavailable)?;
        insert_provider_account(&mut transaction, account, workspace_ids).await?;
        transaction
            .commit()
            .await
            .map_err(|_| ProviderAccountStoreError::Unavailable)
    }

    async fn create_provider_account_idempotent(
        &self,
        account: &ProviderAccount,
        workspace_ids: &[WorkspaceId],
        idempotency_key: &str,
    ) -> Result<ProviderAccount, ProviderAccountStoreError> {
        if idempotency_key.is_empty() || has_duplicate_workspace_ids(workspace_ids) {
            return Err(ProviderAccountStoreError::IntegrityViolation);
        }
        let workspace_ids_json = normalized_workspace_ids_json(workspace_ids)?;
        let mut connection = self.connection.lock().await;
        let mut transaction = sqlx::Connection::begin(&mut *connection)
            .await
            .map_err(|_| ProviderAccountStoreError::Unavailable)?;

        if let Some(row) = sqlx::query(
            "SELECT provider_account_id, provider_type, label, workspace_ids_json
             FROM provider_account_create_idempotencies
             WHERE idempotency_key = ?",
        )
        .bind(idempotency_key)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| ProviderAccountStoreError::Unavailable)?
        {
            let stored_provider_type = row
                .try_get::<String, _>("provider_type")
                .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?;
            let stored_label = row
                .try_get::<String, _>("label")
                .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?;
            let stored_workspace_ids = row
                .try_get::<String, _>("workspace_ids_json")
                .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?;
            if stored_provider_type != account.provider_type().as_str()
                || stored_label != account.label()
                || stored_workspace_ids != workspace_ids_json
            {
                return Err(ProviderAccountStoreError::IdempotencyConflict);
            }
            let account_id = ProviderAccountId::parse(
                row.try_get::<String, _>("provider_account_id")
                    .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?,
            )
            .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?;
            let account = load_provider_account(&mut transaction, &account_id)
                .await?
                .ok_or(ProviderAccountStoreError::IntegrityViolation)?;
            transaction
                .commit()
                .await
                .map_err(|_| ProviderAccountStoreError::Unavailable)?;
            return Ok(account);
        }

        insert_provider_account(&mut transaction, account, workspace_ids).await?;
        sqlx::query(
            "INSERT INTO provider_account_create_idempotencies (
                idempotency_key, provider_account_id, provider_type, label, workspace_ids_json
             ) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(idempotency_key)
        .bind(account.id().as_str())
        .bind(account.provider_type().as_str())
        .bind(account.label())
        .bind(workspace_ids_json)
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            if is_unique_violation(&error) {
                ProviderAccountStoreError::IdempotencyConflict
            } else {
                ProviderAccountStoreError::Unavailable
            }
        })?;
        transaction
            .commit()
            .await
            .map_err(|_| ProviderAccountStoreError::Unavailable)?;
        Ok(account.clone())
    }

    async fn get_provider_account(
        &self,
        id: &ProviderAccountId,
    ) -> Result<Option<ProviderAccount>, ProviderAccountStoreError> {
        let mut connection = self.connection.lock().await;
        load_provider_account(&mut connection, id).await
    }

    async fn list_provider_accounts(
        &self,
        provider_type: Option<&ProviderType>,
    ) -> Result<Vec<ProviderAccount>, ProviderAccountStoreError> {
        let mut connection = self.connection.lock().await;
        let ids = if let Some(provider_type) = provider_type {
            sqlx::query_scalar::<_, String>(
                "SELECT provider_account_id FROM provider_accounts
                 WHERE provider_type = ? ORDER BY provider_account_id",
            )
            .bind(provider_type.as_str())
            .fetch_all(&mut *connection)
            .await
        } else {
            sqlx::query_scalar::<_, String>(
                "SELECT provider_account_id FROM provider_accounts
                 ORDER BY provider_account_id",
            )
            .fetch_all(&mut *connection)
            .await
        }
        .map_err(|_| ProviderAccountStoreError::Unavailable)?;

        let mut accounts = Vec::with_capacity(ids.len());
        for id in ids {
            let id = ProviderAccountId::parse(id)
                .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?;
            accounts.push(
                load_provider_account(&mut connection, &id)
                    .await?
                    .ok_or(ProviderAccountStoreError::IntegrityViolation)?,
            );
        }
        Ok(accounts)
    }

    async fn update_provider_account(
        &self,
        expected: &ProviderAccount,
        account: &ProviderAccount,
    ) -> Result<(), ProviderAccountStoreError> {
        if expected.id() != account.id() || expected.provider_type() != account.provider_type() {
            return Err(ProviderAccountStoreError::IntegrityViolation);
        }
        let metadata_json = serde_json::to_string(account.metadata())
            .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?;
        let updated_at_unix_ms = to_sql_timestamp(account.updated_at_unix_ms())?;
        let last_used_at_unix_ms = account
            .last_used_at_unix_ms()
            .map(to_sql_timestamp)
            .transpose()?;
        let capabilities_refreshed_at_unix_ms = account
            .capabilities_refreshed_at_unix_ms()
            .map(to_sql_timestamp)
            .transpose()?;

        let mut connection = self.connection.lock().await;
        let mut transaction = sqlx::Connection::begin(&mut *connection)
            .await
            .map_err(|_| ProviderAccountStoreError::Unavailable)?;
        let current = load_provider_account(&mut transaction, expected.id()).await?;
        let Some(current) = current else {
            return Err(ProviderAccountStoreError::AccountNotFound);
        };
        if current != *expected {
            return Err(ProviderAccountStoreError::IntegrityViolation);
        }
        if account.state() != ProviderAccountState::Disconnected
            && sqlx::query_scalar::<_, i64>(
                "SELECT EXISTS(
                    SELECT 1 FROM provider_accounts
                    WHERE provider_type = ?
                      AND provider_account_id <> ?
                      AND state IN ('connecting', 'connected', 'reauth_required')
                )",
            )
            .bind(account.provider_type().as_str())
            .bind(account.id().as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(|_| ProviderAccountStoreError::Unavailable)?
                != 0
        {
            return Err(ProviderAccountStoreError::ProviderAccountLimitReached);
        }

        let updated = sqlx::query(
            "UPDATE provider_accounts
             SET label = ?, provider_subject = ?, secret_ref = ?, state = ?,
                 updated_at_unix_ms = ?, last_used_at_unix_ms = ?,
                 capabilities_refreshed_at_unix_ms = ?, metadata_json = ?
             WHERE provider_account_id = ? AND updated_at_unix_ms = ? AND state = ?",
        )
        .bind(account.label())
        .bind(account.subject())
        .bind(account.secret_ref().map(SecretRef::as_str))
        .bind(account.state().as_str())
        .bind(updated_at_unix_ms)
        .bind(last_used_at_unix_ms)
        .bind(capabilities_refreshed_at_unix_ms)
        .bind(metadata_json)
        .bind(account.id().as_str())
        .bind(to_sql_timestamp(expected.updated_at_unix_ms())?)
        .bind(expected.state().as_str())
        .execute(&mut *transaction)
        .await;
        let updated = match updated {
            Ok(updated) => updated,
            Err(error) if is_unique_violation(&error) => {
                if active_provider_account_exists(
                    &mut transaction,
                    account.provider_type(),
                    Some(account.id()),
                )
                .await?
                {
                    return Err(ProviderAccountStoreError::ProviderAccountLimitReached);
                }
                return Err(ProviderAccountStoreError::Unavailable);
            }
            Err(_) => return Err(ProviderAccountStoreError::Unavailable),
        };
        if updated.rows_affected() != 1 {
            return Err(ProviderAccountStoreError::IntegrityViolation);
        }
        if current.secret_ref() != account.secret_ref() {
            if let Some(secret_ref) = account.secret_ref() {
                let consumed = sqlx::query(
                    "DELETE FROM provider_account_secret_cleanup WHERE provider_account_id = ? AND secret_ref = ?",
                ).bind(account.id().as_str()).bind(secret_ref.as_str())
                    .execute(&mut *transaction).await
                    .map_err(|_| ProviderAccountStoreError::Unavailable)?;
                if consumed.rows_affected() != 1 {
                    return Err(ProviderAccountStoreError::IntegrityViolation);
                }
            }
            if let Some(secret_ref) = current.secret_ref() {
                sqlx::query(
                    "INSERT INTO provider_account_secret_cleanup (secret_ref, provider_account_id) VALUES (?, ?)",
                ).bind(secret_ref.as_str()).bind(account.id().as_str())
                    .execute(&mut *transaction).await
                    .map_err(|_| ProviderAccountStoreError::Unavailable)?;
            }
        }
        transaction
            .commit()
            .await
            .map_err(|_| ProviderAccountStoreError::Unavailable)
    }

    async fn account_available_for_workspace(
        &self,
        id: &ProviderAccountId,
        provider_type: &ProviderType,
        workspace_id: &WorkspaceId,
    ) -> Result<ProviderAccount, ProviderAccountStoreError> {
        let mut connection = self.connection.lock().await;
        let account = load_provider_account(&mut connection, id)
            .await?
            .ok_or(ProviderAccountStoreError::AccountNotFound)?;
        if account.provider_type() != provider_type {
            return Err(ProviderAccountStoreError::ProviderTypeMismatch);
        }
        if account.state() != ProviderAccountState::Connected {
            return Err(ProviderAccountStoreError::AccountNotConnected);
        }
        let associated = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                SELECT 1 FROM provider_account_workspaces
                WHERE provider_account_id = ? AND workspace_id = ?
            )",
        )
        .bind(id.as_str())
        .bind(workspace_id.as_str())
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| ProviderAccountStoreError::Unavailable)?;
        if associated == 0 {
            return Err(ProviderAccountStoreError::WorkspaceAssociationMismatch);
        }
        Ok(account)
    }

    async fn associate_provider_account(
        &self,
        id: &ProviderAccountId,
        workspace_id: &WorkspaceId,
    ) -> Result<(), ProviderAccountStoreError> {
        let mut connection = self.connection.lock().await;
        let account_exists = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM provider_accounts WHERE provider_account_id = ?)",
        )
        .bind(id.as_str())
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| ProviderAccountStoreError::Unavailable)?;
        if account_exists == 0 {
            return Err(ProviderAccountStoreError::AccountNotFound);
        }
        let workspace_exists = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM workspaces WHERE workspace_id = ?)",
        )
        .bind(workspace_id.as_str())
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| ProviderAccountStoreError::Unavailable)?;
        if workspace_exists == 0 {
            return Err(ProviderAccountStoreError::WorkspaceNotFound);
        }
        sqlx::query(
            "INSERT OR IGNORE INTO provider_account_workspaces
                (provider_account_id, workspace_id) VALUES (?, ?)",
        )
        .bind(id.as_str())
        .bind(workspace_id.as_str())
        .execute(&mut *connection)
        .await
        .map_err(|_| ProviderAccountStoreError::Unavailable)?;
        Ok(())
    }
}

async fn insert_provider_account(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    account: &ProviderAccount,
    workspace_ids: &[WorkspaceId],
) -> Result<(), ProviderAccountStoreError> {
    if has_duplicate_workspace_ids(workspace_ids) {
        return Err(ProviderAccountStoreError::IntegrityViolation);
    }
    let metadata_json = serde_json::to_string(account.metadata())
        .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?;
    let created_at_unix_ms = to_sql_timestamp(account.created_at_unix_ms())?;
    let updated_at_unix_ms = to_sql_timestamp(account.updated_at_unix_ms())?;
    let last_used_at_unix_ms = account
        .last_used_at_unix_ms()
        .map(to_sql_timestamp)
        .transpose()?;
    let capabilities_refreshed_at_unix_ms = account
        .capabilities_refreshed_at_unix_ms()
        .map(to_sql_timestamp)
        .transpose()?;

    if account.state() != ProviderAccountState::Disconnected
        && sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                SELECT 1 FROM provider_accounts
                WHERE provider_type = ?
                  AND state IN ('connecting', 'connected', 'reauth_required')
            )",
        )
        .bind(account.provider_type().as_str())
        .fetch_one(&mut **transaction)
        .await
        .map_err(|_| ProviderAccountStoreError::Unavailable)?
            != 0
    {
        return Err(ProviderAccountStoreError::ProviderAccountLimitReached);
    }

    let insert = sqlx::query(
        "INSERT INTO provider_accounts (
            provider_account_id, provider_type, label, provider_subject,
            secret_ref, state, created_at_unix_ms, updated_at_unix_ms,
            last_used_at_unix_ms, capabilities_refreshed_at_unix_ms, metadata_json
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(account.id().as_str())
    .bind(account.provider_type().as_str())
    .bind(account.label())
    .bind(account.subject())
    .bind(account.secret_ref().map(SecretRef::as_str))
    .bind(account.state().as_str())
    .bind(created_at_unix_ms)
    .bind(updated_at_unix_ms)
    .bind(last_used_at_unix_ms)
    .bind(capabilities_refreshed_at_unix_ms)
    .bind(metadata_json)
    .execute(&mut **transaction)
    .await;
    if let Err(error) = insert {
        if is_unique_violation(&error) {
            let account_exists = sqlx::query_scalar::<_, i64>(
                "SELECT EXISTS(
                    SELECT 1 FROM provider_accounts WHERE provider_account_id = ?
                )",
            )
            .bind(account.id().as_str())
            .fetch_one(&mut **transaction)
            .await
            .map_err(|_| ProviderAccountStoreError::Unavailable)?;
            if account_exists != 0 {
                return Err(ProviderAccountStoreError::IntegrityViolation);
            }
            let active_exists =
                active_provider_account_exists(transaction, account.provider_type(), None).await?;
            if active_exists {
                return Err(ProviderAccountStoreError::ProviderAccountLimitReached);
            }
        }
        return Err(ProviderAccountStoreError::Unavailable);
    }

    for workspace_id in workspace_ids {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM workspaces WHERE workspace_id = ?)",
        )
        .bind(workspace_id.as_str())
        .fetch_one(&mut **transaction)
        .await
        .map_err(|_| ProviderAccountStoreError::Unavailable)?;
        if exists == 0 {
            return Err(ProviderAccountStoreError::WorkspaceNotFound);
        }
        sqlx::query(
            "INSERT INTO provider_account_workspaces (provider_account_id, workspace_id)
             VALUES (?, ?)",
        )
        .bind(account.id().as_str())
        .bind(workspace_id.as_str())
        .execute(&mut **transaction)
        .await
        .map_err(|_| ProviderAccountStoreError::Unavailable)?;
    }
    Ok(())
}

fn has_duplicate_workspace_ids(workspace_ids: &[WorkspaceId]) -> bool {
    workspace_ids
        .iter()
        .enumerate()
        .any(|(index, id)| workspace_ids[..index].contains(id))
}

fn normalized_workspace_ids_json(
    workspace_ids: &[WorkspaceId],
) -> Result<String, ProviderAccountStoreError> {
    let mut normalized = workspace_ids
        .iter()
        .map(|workspace_id| workspace_id.as_str())
        .collect::<Vec<_>>();
    normalized.sort_unstable();
    serde_json::to_string(&normalized).map_err(|_| ProviderAccountStoreError::IntegrityViolation)
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(error) if error.is_unique_violation())
}

async fn active_provider_account_exists(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    provider_type: &ProviderType,
    excluded_id: Option<&ProviderAccountId>,
) -> Result<bool, ProviderAccountStoreError> {
    let exists = if let Some(excluded_id) = excluded_id {
        sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                SELECT 1 FROM provider_accounts
                WHERE provider_type = ?
                  AND provider_account_id <> ?
                  AND state IN ('connecting', 'connected', 'reauth_required')
            )",
        )
        .bind(provider_type.as_str())
        .bind(excluded_id.as_str())
        .fetch_one(&mut **transaction)
        .await
    } else {
        sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                SELECT 1 FROM provider_accounts
                WHERE provider_type = ?
                  AND state IN ('connecting', 'connected', 'reauth_required')
            )",
        )
        .bind(provider_type.as_str())
        .fetch_one(&mut **transaction)
        .await
    }
    .map_err(|_| ProviderAccountStoreError::Unavailable)?;
    Ok(exists != 0)
}

fn to_sql_timestamp(value: u64) -> Result<i64, ProviderAccountStoreError> {
    i64::try_from(value).map_err(|_| ProviderAccountStoreError::IntegrityViolation)
}

pub(super) async fn load_provider_account(
    connection: &mut SqliteConnection,
    id: &ProviderAccountId,
) -> Result<Option<ProviderAccount>, ProviderAccountStoreError> {
    let row = sqlx::query(
        "SELECT provider_account_id, provider_type, label, provider_subject,
                secret_ref, state, created_at_unix_ms, updated_at_unix_ms,
                last_used_at_unix_ms, capabilities_refreshed_at_unix_ms, metadata_json
         FROM provider_accounts WHERE provider_account_id = ?",
    )
    .bind(id.as_str())
    .fetch_optional(&mut *connection)
    .await
    .map_err(|_| ProviderAccountStoreError::Unavailable)?;
    row.map(parse_provider_account).transpose()
}

fn parse_provider_account(
    row: sqlx::sqlite::SqliteRow,
) -> Result<ProviderAccount, ProviderAccountStoreError> {
    let stored_id = ProviderAccountId::parse(
        row.try_get::<String, _>("provider_account_id")
            .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?,
    )
    .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?;
    let provider_type = ProviderType::parse(
        row.try_get::<String, _>("provider_type")
            .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?,
    )
    .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?;
    let label = row
        .try_get::<String, _>("label")
        .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?;
    let subject = row
        .try_get::<Option<String>, _>("provider_subject")
        .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?;
    let secret_ref = row
        .try_get::<Option<String>, _>("secret_ref")
        .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?
        .map(SecretRef::parse)
        .transpose()
        .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?;
    let state = ProviderAccountState::parse(
        &row.try_get::<String, _>("state")
            .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?,
    )
    .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?;
    let created_at_unix_ms = from_sql_timestamp(
        row.try_get::<i64, _>("created_at_unix_ms")
            .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?,
    )?;
    let updated_at_unix_ms = from_sql_timestamp(
        row.try_get::<i64, _>("updated_at_unix_ms")
            .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?,
    )?;
    let last_used_at_unix_ms = row
        .try_get::<Option<i64>, _>("last_used_at_unix_ms")
        .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?
        .map(from_sql_timestamp)
        .transpose()?;
    let capabilities_refreshed_at_unix_ms = row
        .try_get::<Option<i64>, _>("capabilities_refreshed_at_unix_ms")
        .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?
        .map(from_sql_timestamp)
        .transpose()?;
    let metadata = serde_json::from_str::<BTreeMap<String, String>>(
        &row.try_get::<String, _>("metadata_json")
            .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?,
    )
    .map_err(|_| ProviderAccountStoreError::IntegrityViolation)?;
    ProviderAccount::new(
        stored_id,
        provider_type,
        label,
        subject,
        secret_ref,
        state,
        created_at_unix_ms,
        updated_at_unix_ms,
        last_used_at_unix_ms,
        capabilities_refreshed_at_unix_ms,
        metadata,
    )
    .map_err(|_| ProviderAccountStoreError::IntegrityViolation)
}

fn from_sql_timestamp(value: i64) -> Result<u64, ProviderAccountStoreError> {
    u64::try_from(value).map_err(|_| ProviderAccountStoreError::IntegrityViolation)
}
