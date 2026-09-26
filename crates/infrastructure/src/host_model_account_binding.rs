use super::SqliteStore;
use kiln_core::{
    HOST_MODEL_ACCOUNT_BINDING_KEY_MAX_BYTES, HOST_MODEL_ACCOUNT_BINDING_MAX_PAGE_SIZE,
    HostModelAccountBinding, HostModelAccountBindingError as Error, HostModelAccountBindingStore,
    ProviderAccountId, SharedConfigurationKey,
};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteRow};

const MAX_BINDING_PAGE_SIZE: usize = HOST_MODEL_ACCOUNT_BINDING_MAX_PAGE_SIZE + 1;

impl HostModelAccountBindingStore for SqliteStore {
    async fn get_host_model_account_binding(
        &self,
        key: &SharedConfigurationKey,
    ) -> Result<HostModelAccountBinding, Error> {
        validate_key(key)?;
        let mut connection = self.connection.lock().await;
        Ok(load_binding(&mut connection, key)
            .await?
            .unwrap_or(HostModelAccountBinding {
                key: key.clone(),
                provider_account_id: None,
                version: 0,
            }))
    }

    async fn list_host_model_account_bindings(
        &self,
        after: Option<&SharedConfigurationKey>,
        limit: usize,
    ) -> Result<Vec<HostModelAccountBinding>, Error> {
        if !(1..=MAX_BINDING_PAGE_SIZE).contains(&limit) {
            return Err(Error::InvalidRequest);
        }
        if let Some(after) = after {
            validate_key(after)?;
        }
        let limit = i64::try_from(limit).map_err(|_| Error::InvalidRequest)?;
        let after = after.map(SharedConfigurationKey::as_str);
        let mut connection = self.connection.lock().await;
        let rows = sqlx::query(
            "SELECT binding_key, provider_account_id, version
             FROM host_model_account_bindings
             WHERE provider_account_id IS NOT NULL
               AND (? IS NULL OR binding_key > ?)
             ORDER BY binding_key COLLATE BINARY
             LIMIT ?",
        )
        .bind(after)
        .bind(after)
        .bind(limit)
        .fetch_all(&mut *connection)
        .await
        .map_err(|_| Error::Unavailable)?;
        rows.into_iter().map(parse_binding).collect()
    }

    async fn set_host_model_account_binding(
        &self,
        key: &SharedConfigurationKey,
        expected_version: u64,
        provider_account_id: &ProviderAccountId,
    ) -> Result<HostModelAccountBinding, Error> {
        validate_key(key)?;
        let expected_version = checked_version(expected_version)?;
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;

        let account_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM provider_accounts WHERE provider_account_id = ?)",
        )
        .bind(provider_account_id.as_str())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| Error::Unavailable)?;
        if !account_exists {
            return Err(Error::ProviderAccountNotFound);
        }

        let current = load_binding(&mut transaction, key).await?;
        let current_version = current.as_ref().map_or(0, |binding| binding.version);
        if current_version != expected_version as u64 {
            return Err(Error::Conflict);
        }
        if current
            .as_ref()
            .and_then(|binding| binding.provider_account_id.as_ref())
            == Some(provider_account_id)
        {
            let current = current.ok_or(Error::IntegrityViolation)?;
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(current);
        }

        let next_version = next_version(current_version)?;
        match current {
            None => {
                sqlx::query(
                    "INSERT INTO host_model_account_bindings (binding_key, provider_account_id, version)
                     VALUES (?, ?, ?)",
                )
                .bind(key.as_str())
                .bind(provider_account_id.as_str())
                .bind(next_version)
                .execute(&mut *transaction)
                .await
                .map_err(map_write_error)?;
            }
            Some(_) => {
                let changed = sqlx::query(
                    "UPDATE host_model_account_bindings
                     SET provider_account_id = ?, version = ?
                     WHERE binding_key = ? AND version = ?",
                )
                .bind(provider_account_id.as_str())
                .bind(next_version)
                .bind(key.as_str())
                .bind(expected_version)
                .execute(&mut *transaction)
                .await
                .map_err(map_write_error)?
                .rows_affected();
                if changed != 1 {
                    return Err(Error::Conflict);
                }
            }
        }

        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(HostModelAccountBinding {
            key: key.clone(),
            provider_account_id: Some(provider_account_id.clone()),
            version: next_version as u64,
        })
    }

    async fn remove_host_model_account_binding(
        &self,
        key: &SharedConfigurationKey,
        expected_version: u64,
    ) -> Result<HostModelAccountBinding, Error> {
        validate_key(key)?;
        let expected_version = checked_version(expected_version)?;
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let current = load_binding(&mut transaction, key).await?;
        let current_version = current.as_ref().map_or(0, |binding| binding.version);
        if current_version != expected_version as u64 {
            return Err(Error::Conflict);
        }

        let Some(current) = current else {
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(HostModelAccountBinding {
                key: key.clone(),
                provider_account_id: None,
                version: 0,
            });
        };
        if current.provider_account_id.is_none() {
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(current);
        }

        let next_version = next_version(current_version)?;
        let changed = sqlx::query(
            "UPDATE host_model_account_bindings
             SET provider_account_id = NULL, version = ?
             WHERE binding_key = ? AND version = ?",
        )
        .bind(next_version)
        .bind(key.as_str())
        .bind(expected_version)
        .execute(&mut *transaction)
        .await
        .map_err(map_write_error)?
        .rows_affected();
        if changed != 1 {
            return Err(Error::Conflict);
        }
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(HostModelAccountBinding {
            key: key.clone(),
            provider_account_id: None,
            version: next_version as u64,
        })
    }
}

fn validate_key(key: &SharedConfigurationKey) -> Result<(), Error> {
    SharedConfigurationKey::parse(key.as_str(), HOST_MODEL_ACCOUNT_BINDING_KEY_MAX_BYTES)
        .map(|_| ())
        .map_err(|_| Error::InvalidRequest)
}

fn checked_version(version: u64) -> Result<i64, Error> {
    i64::try_from(version).map_err(|_| Error::InvalidRequest)
}

fn next_version(version: u64) -> Result<i64, Error> {
    let next = version.checked_add(1).ok_or(Error::IntegrityViolation)?;
    checked_version(next).map_err(|_| Error::IntegrityViolation)
}

async fn load_binding(
    connection: &mut SqliteConnection,
    key: &SharedConfigurationKey,
) -> Result<Option<HostModelAccountBinding>, Error> {
    sqlx::query(
        "SELECT binding_key, provider_account_id, version
         FROM host_model_account_bindings WHERE binding_key = ?",
    )
    .bind(key.as_str())
    .fetch_optional(connection)
    .await
    .map_err(|_| Error::Unavailable)?
    .map(parse_binding)
    .transpose()
}

fn parse_binding(row: SqliteRow) -> Result<HostModelAccountBinding, Error> {
    let key = SharedConfigurationKey::parse(
        row.try_get::<String, _>("binding_key")
            .map_err(|_| Error::IntegrityViolation)?,
        HOST_MODEL_ACCOUNT_BINDING_KEY_MAX_BYTES,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let provider_account_id = row
        .try_get::<Option<String>, _>("provider_account_id")
        .map_err(|_| Error::IntegrityViolation)?
        .map(ProviderAccountId::parse)
        .transpose()
        .map_err(|_| Error::IntegrityViolation)?;
    let version = row
        .try_get::<i64, _>("version")
        .map_err(|_| Error::IntegrityViolation)?;
    let version = u64::try_from(version)
        .ok()
        .filter(|version| *version > 0)
        .ok_or(Error::IntegrityViolation)?;
    Ok(HostModelAccountBinding {
        key,
        provider_account_id,
        version,
    })
}

fn map_write_error(error: sqlx::Error) -> Error {
    match error {
        sqlx::Error::Database(database_error)
            if database_error.message().contains("FOREIGN KEY") =>
        {
            Error::ProviderAccountNotFound
        }
        sqlx::Error::Database(_) => Error::IntegrityViolation,
        _ => Error::Unavailable,
    }
}
