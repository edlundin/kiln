use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    future::Future,
    sync::{Arc, Mutex},
};

use tokio::sync::Mutex as AsyncMutex;
use ulid::Ulid;

use crate::{ProviderAccountId, ProviderType, WorkspaceId, parse_id};

const MAX_PROVIDER_ACCOUNT_LABEL_BYTES: usize = 256;
const MAX_PROVIDER_ACCOUNT_SUBJECT_BYTES: usize = 256;
const MAX_PROVIDER_ACCOUNT_METADATA_KEY_BYTES: usize = 64;
const MAX_PROVIDER_ACCOUNT_METADATA_VALUE_BYTES: usize = 1024;

/// An opaque handle into the process-local credential store.
///
/// The reference is safe to persist. Credential bytes are intentionally not
/// representable by this type and never cross the core/storage boundary. The
/// canonical form is `sec_<ULID>`; callers must obtain the reference from the
/// credential adapter rather than passing a credential or free-form token.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SecretRef(String);

impl SecretRef {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidSecretRef> {
        parse_id(value.into(), "sec_")
            .map(Self)
            .map_err(|_| InvalidSecretRef)
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidSecretRef> {
        Self::new(value)
    }

    pub fn from_ulid(value: Ulid) -> Self {
        Self(format!("sec_{value}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("SecretRef")
            .field(&"<redacted>")
            .finish()
    }
}

/// Secret material is only carried between a provider adapter and a secret
/// store. It deliberately has no `Debug`, `Display`, or serialization of its
/// bytes; callers must make the boundary explicit with `as_bytes`.
pub struct SecretValue(Vec<u8>);

impl SecretValue {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, InvalidSecretValue> {
        let bytes = bytes.into();
        if bytes.is_empty()
            || bytes.len() > MAX_SECRET_VALUE_BYTES
            || bytes.iter().any(|byte| matches!(byte, 0 | b'\r' | b'\n'))
        {
            return Err(InvalidSecretValue);
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Drop for SecretValue {
    fn drop(&mut self) {
        for byte in &mut self.0 {
            *byte = 0;
        }
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<secret>")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidSecretValue;

const MAX_SECRET_VALUE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidSecretRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderAccountState {
    Connecting,
    Connected,
    ReauthRequired,
    Disconnected,
}

impl ProviderAccountState {
    pub fn parse(value: &str) -> Result<Self, InvalidProviderAccountState> {
        match value {
            "connecting" => Ok(Self::Connecting),
            "connected" => Ok(Self::Connected),
            "reauth_required" => Ok(Self::ReauthRequired),
            "disconnected" => Ok(Self::Disconnected),
            _ => Err(InvalidProviderAccountState),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connecting => "connecting",
            Self::Connected => "connected",
            Self::ReauthRequired => "reauth_required",
            Self::Disconnected => "disconnected",
        }
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (
                    Self::Connecting,
                    Self::Connected | Self::ReauthRequired | Self::Disconnected
                ) | (Self::Connected, Self::ReauthRequired | Self::Disconnected)
                    | (
                        Self::ReauthRequired,
                        Self::Connecting | Self::Connected | Self::Disconnected
                    )
                    | (Self::Disconnected, Self::Connecting)
            )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidProviderAccountState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderAccount {
    id: ProviderAccountId,
    provider_type: ProviderType,
    label: String,
    subject: Option<String>,
    secret_ref: Option<SecretRef>,
    state: ProviderAccountState,
    created_at_unix_ms: u64,
    updated_at_unix_ms: u64,
    last_used_at_unix_ms: Option<u64>,
    capabilities_refreshed_at_unix_ms: Option<u64>,
    metadata: BTreeMap<String, String>,
}

impl ProviderAccount {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: ProviderAccountId,
        provider_type: ProviderType,
        label: String,
        subject: Option<String>,
        secret_ref: Option<SecretRef>,
        state: ProviderAccountState,
        created_at_unix_ms: u64,
        updated_at_unix_ms: u64,
        last_used_at_unix_ms: Option<u64>,
        capabilities_refreshed_at_unix_ms: Option<u64>,
        metadata: BTreeMap<String, String>,
    ) -> Result<Self, ProviderAccountError> {
        validate_label(&label)?;
        if let Some(subject) = &subject {
            validate_subject(subject)?;
        }
        validate_timestamps(
            created_at_unix_ms,
            updated_at_unix_ms,
            last_used_at_unix_ms,
            capabilities_refreshed_at_unix_ms,
        )?;
        validate_metadata(&metadata)?;
        if state == ProviderAccountState::Connected && secret_ref.is_none() {
            return Err(ProviderAccountError::SecretRefRequired);
        }
        if state == ProviderAccountState::Disconnected && secret_ref.is_some() {
            return Err(ProviderAccountError::SecretRefForbidden);
        }
        Ok(Self {
            id,
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
        })
    }

    pub fn id(&self) -> &ProviderAccountId {
        &self.id
    }

    pub fn provider_type(&self) -> &ProviderType {
        &self.provider_type
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn subject(&self) -> Option<&str> {
        self.subject.as_deref()
    }

    pub fn secret_ref(&self) -> Option<&SecretRef> {
        self.secret_ref.as_ref()
    }

    pub fn state(&self) -> ProviderAccountState {
        self.state
    }

    pub fn created_at_unix_ms(&self) -> u64 {
        self.created_at_unix_ms
    }

    pub fn updated_at_unix_ms(&self) -> u64 {
        self.updated_at_unix_ms
    }

    pub fn last_used_at_unix_ms(&self) -> Option<u64> {
        self.last_used_at_unix_ms
    }

    pub fn capabilities_refreshed_at_unix_ms(&self) -> Option<u64> {
        self.capabilities_refreshed_at_unix_ms
    }

    pub fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    fn transition(
        &self,
        next_state: ProviderAccountState,
        updated_at_unix_ms: u64,
        secret_ref: Option<SecretRef>,
    ) -> Result<Self, ProviderAccountError> {
        if !self.state.can_transition_to(next_state) {
            return Err(ProviderAccountError::InvalidTransition);
        }
        let secret_ref = match next_state {
            ProviderAccountState::Disconnected => None,
            ProviderAccountState::Connected if self.state == ProviderAccountState::Disconnected => {
                Some(secret_ref.ok_or(ProviderAccountError::SecretRefRequired)?)
            }
            _ => secret_ref.or_else(|| self.secret_ref.clone()),
        };
        if next_state == ProviderAccountState::Connected && secret_ref.is_none() {
            return Err(ProviderAccountError::SecretRefRequired);
        }
        if updated_at_unix_ms < self.updated_at_unix_ms {
            return Err(ProviderAccountError::InvalidTimestamp);
        }
        let mut next = self.clone();
        next.state = next_state;
        next.secret_ref = secret_ref;
        next.updated_at_unix_ms = updated_at_unix_ms;
        Ok(next)
    }

    /// Disconnecting clears the reference so a later connection cannot use an
    /// old credential handle accidentally.
    fn disconnect(&self, updated_at_unix_ms: u64) -> Result<Self, ProviderAccountError> {
        let next = self.transition(ProviderAccountState::Disconnected, updated_at_unix_ms, None)?;
        Ok(next)
    }

    pub fn mark_used(&self, used_at_unix_ms: u64) -> Result<Self, ProviderAccountError> {
        if used_at_unix_ms < self.updated_at_unix_ms {
            return Err(ProviderAccountError::InvalidTimestamp);
        }
        let mut next = self.clone();
        next.last_used_at_unix_ms = Some(used_at_unix_ms);
        next.updated_at_unix_ms = used_at_unix_ms;
        Ok(next)
    }

    pub fn mark_capabilities_refreshed(
        &self,
        refreshed_at_unix_ms: u64,
    ) -> Result<Self, ProviderAccountError> {
        if refreshed_at_unix_ms < self.updated_at_unix_ms {
            return Err(ProviderAccountError::InvalidTimestamp);
        }
        let mut next = self.clone();
        next.capabilities_refreshed_at_unix_ms = Some(refreshed_at_unix_ms);
        next.updated_at_unix_ms = refreshed_at_unix_ms;
        Ok(next)
    }
}

fn validate_label(value: &str) -> Result<(), ProviderAccountError> {
    if value.is_empty()
        || value.trim() != value
        || value.len() > MAX_PROVIDER_ACCOUNT_LABEL_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(ProviderAccountError::InvalidLabel);
    }
    Ok(())
}

fn validate_subject(value: &str) -> Result<(), ProviderAccountError> {
    if value.is_empty()
        || value.trim() != value
        || value.len() > MAX_PROVIDER_ACCOUNT_SUBJECT_BYTES
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(ProviderAccountError::InvalidSubject);
    }
    Ok(())
}

fn validate_timestamps(
    created_at_unix_ms: u64,
    updated_at_unix_ms: u64,
    last_used_at_unix_ms: Option<u64>,
    capabilities_refreshed_at_unix_ms: Option<u64>,
) -> Result<(), ProviderAccountError> {
    if updated_at_unix_ms < created_at_unix_ms
        || last_used_at_unix_ms
            .is_some_and(|value| value < created_at_unix_ms || value > updated_at_unix_ms)
        || capabilities_refreshed_at_unix_ms
            .is_some_and(|value| value < created_at_unix_ms || value > updated_at_unix_ms)
    {
        return Err(ProviderAccountError::InvalidTimestamp);
    }
    Ok(())
}

fn validate_metadata(metadata: &BTreeMap<String, String>) -> Result<(), ProviderAccountError> {
    if metadata.iter().any(|(key, value)| {
        key.is_empty()
            || key.len() > MAX_PROVIDER_ACCOUNT_METADATA_KEY_BYTES
            || key.trim() != key
            || key
                .chars()
                .any(|character| character.is_control() || character.is_whitespace())
            || value.len() > MAX_PROVIDER_ACCOUNT_METADATA_VALUE_BYTES
            || value.chars().any(char::is_control)
    }) {
        return Err(ProviderAccountError::InvalidMetadata);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderAccountError {
    InvalidLabel,
    InvalidSubject,
    InvalidSecretRef,
    InvalidMetadata,
    InvalidTimestamp,
    SecretRefRequired,
    SecretRefForbidden,
    InvalidTransition,
    AccountNotFound,
    ProviderTypeMismatch,
    AccountNotConnected,
    WorkspaceAssociationMismatch,
    ProviderAccountLimitReached,
    IdempotencyConflict,
    IntegrityViolation,
    StoreUnavailable,
    CredentialStoreRequired,
    CredentialStore(SecretStoreError),
    CredentialVersionConflict,
    CredentialCleanupRequired { secret_ref: SecretRef },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretStoreError {
    Unsupported,
    Unavailable,
    InvalidSecret,
    InvalidReference,
    NotFound,
    ProviderBindingMismatch,
}

pub trait SecretStore: Send + Sync {
    fn put(
        &self,
        provider_type: &ProviderType,
        account_id: &ProviderAccountId,
        value: SecretValue,
    ) -> impl Future<Output = Result<SecretRef, SecretStoreError>> + Send;
    fn get(
        &self,
        provider_type: &ProviderType,
        account_id: &ProviderAccountId,
        secret_ref: &SecretRef,
    ) -> impl Future<Output = Result<SecretValue, SecretStoreError>> + Send;
    fn delete(
        &self,
        provider_type: &ProviderType,
        account_id: &ProviderAccountId,
        secret_ref: &SecretRef,
    ) -> impl Future<Output = Result<(), SecretStoreError>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProviderAccount {
    pub provider_type: ProviderType,
    pub label: String,
    pub subject: Option<String>,
    pub secret_ref: Option<SecretRef>,
    pub state: ProviderAccountState,
    pub workspace_ids: Vec<WorkspaceId>,
    pub created_at_unix_ms: u64,
    pub metadata: BTreeMap<String, String>,
}

pub trait ProviderAccountStore: Send + Sync {
    fn create_provider_account(
        &self,
        account: &ProviderAccount,
        workspace_ids: &[WorkspaceId],
    ) -> impl Future<Output = Result<(), ProviderAccountStoreError>> + Send;
    fn get_provider_account(
        &self,
        id: &ProviderAccountId,
    ) -> impl Future<Output = Result<Option<ProviderAccount>, ProviderAccountStoreError>> + Send;
    fn list_provider_accounts(
        &self,
        provider_type: Option<&ProviderType>,
    ) -> impl Future<Output = Result<Vec<ProviderAccount>, ProviderAccountStoreError>> + Send;
    fn update_provider_account(
        &self,
        expected: &ProviderAccount,
        account: &ProviderAccount,
    ) -> impl Future<Output = Result<(), ProviderAccountStoreError>> + Send;
    fn account_available_for_workspace(
        &self,
        id: &ProviderAccountId,
        provider_type: &ProviderType,
        workspace_id: &WorkspaceId,
    ) -> impl Future<Output = Result<ProviderAccount, ProviderAccountStoreError>> + Send;
    fn associate_provider_account(
        &self,
        id: &ProviderAccountId,
        workspace_id: &WorkspaceId,
    ) -> impl Future<Output = Result<(), ProviderAccountStoreError>> + Send;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderAccountStoreError {
    AccountNotFound,
    ProviderTypeMismatch,
    AccountNotConnected,
    ProviderAccountLimitReached,
    WorkspaceNotFound,
    WorkspaceAssociationMismatch,
    IdempotencyConflict,
    IntegrityViolation,
    Unavailable,
}

pub trait ProviderAccountIdGenerator: Send + Sync {
    fn provider_account_id(&self) -> ProviderAccountId;
}

pub struct ProviderAccountApplication<S, I> {
    store: S,
    ids: I,
    lifecycle_locks: Arc<Mutex<HashMap<ProviderAccountId, Arc<AsyncMutex<()>>>>>,
}

impl<S, I> ProviderAccountApplication<S, I> {
    pub fn new(store: S, ids: I) -> Self {
        Self {
            store,
            ids,
            lifecycle_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<S, I> ProviderAccountApplication<S, I>
where
    S: ProviderAccountStore,
    I: ProviderAccountIdGenerator,
{
    pub async fn create_provider_account(
        &self,
        command: CreateProviderAccount,
    ) -> Result<ProviderAccount, ProviderAccountError> {
        if command.secret_ref.is_some() || command.state == ProviderAccountState::Connected {
            return Err(ProviderAccountError::CredentialStoreRequired);
        }
        let account = ProviderAccount::new(
            self.ids.provider_account_id(),
            command.provider_type,
            command.label,
            command.subject,
            command.secret_ref,
            command.state,
            command.created_at_unix_ms,
            command.created_at_unix_ms,
            None,
            None,
            command.metadata,
        )?;
        self.store
            .create_provider_account(&account, &command.workspace_ids)
            .await
            .map_err(map_provider_account_store_error)?;
        Ok(account)
    }

    pub async fn get_provider_account(
        &self,
        id: ProviderAccountId,
    ) -> Result<ProviderAccount, ProviderAccountError> {
        self.store
            .get_provider_account(&id)
            .await
            .map_err(map_provider_account_store_error)?
            .ok_or(ProviderAccountError::AccountNotFound)
    }

    pub async fn list_provider_accounts(
        &self,
        provider_type: Option<&ProviderType>,
    ) -> Result<Vec<ProviderAccount>, ProviderAccountError> {
        self.store
            .list_provider_accounts(provider_type)
            .await
            .map_err(map_provider_account_store_error)
    }

    pub async fn transition_provider_account(
        &self,
        id: ProviderAccountId,
        state: ProviderAccountState,
        updated_at_unix_ms: u64,
        secret_ref: Option<SecretRef>,
    ) -> Result<ProviderAccount, ProviderAccountError> {
        let _lock = self.lifecycle_lock(&id).await;
        if state == ProviderAccountState::Connected
            || state == ProviderAccountState::Disconnected
            || secret_ref.is_some()
        {
            return Err(ProviderAccountError::CredentialStoreRequired);
        }
        let current = self.get_provider_account(id).await?;
        let next = current.transition(state, updated_at_unix_ms, secret_ref)?;
        self.store
            .update_provider_account(&current, &next)
            .await
            .map_err(map_provider_account_store_error)?;
        Ok(next)
    }

    pub async fn connect_provider_account<V: SecretStore>(
        &self,
        secret_store: &V,
        id: ProviderAccountId,
        expected_provider_type: ProviderType,
        secret: SecretValue,
        updated_at_unix_ms: u64,
    ) -> Result<ProviderAccount, ProviderAccountError> {
        let _lock = self.lifecycle_lock(&id).await;
        let current = self.get_provider_account(id.clone()).await?;
        self.connect_provider_account_locked(
            secret_store,
            id,
            expected_provider_type,
            secret,
            updated_at_unix_ms,
            current,
            None,
        )
        .await
    }

    async fn connect_provider_account_locked<V: SecretStore>(
        &self,
        secret_store: &V,
        id: ProviderAccountId,
        expected_provider_type: ProviderType,
        secret: SecretValue,
        updated_at_unix_ms: u64,
        mut current: ProviderAccount,
        expected_secret_ref: Option<&SecretRef>,
    ) -> Result<ProviderAccount, ProviderAccountError> {
        if current.provider_type() != &expected_provider_type {
            return Err(ProviderAccountError::ProviderTypeMismatch);
        }
        if let Some(expected_secret_ref) = expected_secret_ref {
            if current.secret_ref() != Some(expected_secret_ref)
                || current.state() != ProviderAccountState::Connected
            {
                return Err(ProviderAccountError::CredentialVersionConflict);
            }
        }
        if current.state() == ProviderAccountState::Disconnected {
            let connecting =
                current.transition(ProviderAccountState::Connecting, updated_at_unix_ms, None)?;
            self.store
                .update_provider_account(&current, &connecting)
                .await
                .map_err(map_provider_account_store_error)?;
            current = connecting;
        }
        let old_secret_ref = current.secret_ref().cloned();
        let new_secret_ref = secret_store
            .put(&expected_provider_type, &id, secret)
            .await
            .map_err(ProviderAccountError::CredentialStore)?;
        let next = match current.transition(
            ProviderAccountState::Connected,
            updated_at_unix_ms,
            Some(new_secret_ref.clone()),
        ) {
            Ok(next) => next,
            Err(error) => {
                let cleanup = secret_store
                    .delete(&expected_provider_type, &id, &new_secret_ref)
                    .await;
                return Err(match cleanup {
                    Ok(()) | Err(SecretStoreError::NotFound) => error,
                    Err(_) => ProviderAccountError::CredentialCleanupRequired {
                        secret_ref: new_secret_ref.clone(),
                    },
                });
            }
        };
        if let Err(error) = self
            .store
            .update_provider_account(&current, &next)
            .await
            .map_err(map_provider_account_store_error)
        {
            let cleanup = secret_store
                .delete(&expected_provider_type, &id, &new_secret_ref)
                .await;
            return Err(match cleanup {
                Ok(()) | Err(SecretStoreError::NotFound) => error,
                Err(_) => ProviderAccountError::CredentialCleanupRequired {
                    secret_ref: new_secret_ref.clone(),
                },
            });
        }
        if let Some(old_secret_ref) = old_secret_ref {
            if old_secret_ref != new_secret_ref
                && secret_store
                    .delete(&expected_provider_type, &id, &old_secret_ref)
                    .await
                    .is_err_and(|error| error != SecretStoreError::NotFound)
            {
                return Err(ProviderAccountError::CredentialCleanupRequired {
                    secret_ref: old_secret_ref,
                });
            }
        }
        Ok(next)
    }

    pub async fn rotate_provider_account<V: SecretStore>(
        &self,
        secret_store: &V,
        id: ProviderAccountId,
        expected_provider_type: ProviderType,
        expected_secret_ref: SecretRef,
        secret: SecretValue,
        updated_at_unix_ms: u64,
    ) -> Result<ProviderAccount, ProviderAccountError> {
        let _lock = self.lifecycle_lock(&id).await;
        let current = self.get_provider_account(id.clone()).await?;
        self.connect_provider_account_locked(
            secret_store,
            id,
            expected_provider_type,
            secret,
            updated_at_unix_ms,
            current,
            Some(&expected_secret_ref),
        )
        .await
    }

    pub async fn read_provider_account_secret<V: SecretStore>(
        &self,
        secret_store: &V,
        id: ProviderAccountId,
        expected_provider_type: ProviderType,
        workspace_id: WorkspaceId,
    ) -> Result<SecretValue, ProviderAccountError> {
        let _lock = self.lifecycle_lock(&id).await;
        let account = self
            .resolve_for_workspace(id.clone(), expected_provider_type.clone(), workspace_id)
            .await?;
        let secret_ref = account
            .secret_ref()
            .ok_or(ProviderAccountError::SecretRefRequired)?;
        secret_store
            .get(&expected_provider_type, &id, secret_ref)
            .await
            .map_err(ProviderAccountError::CredentialStore)
    }

    pub async fn cleanup_provider_account_secret<V: SecretStore>(
        &self,
        secret_store: &V,
        id: ProviderAccountId,
        expected_provider_type: ProviderType,
        secret_ref: SecretRef,
    ) -> Result<(), ProviderAccountError> {
        let _lock = self.lifecycle_lock(&id).await;
        let account = self.get_provider_account(id.clone()).await?;
        if account.provider_type() != &expected_provider_type {
            return Err(ProviderAccountError::ProviderTypeMismatch);
        }
        match secret_store
            .delete(&expected_provider_type, &id, &secret_ref)
            .await
        {
            Ok(()) | Err(SecretStoreError::NotFound) => Ok(()),
            Err(error) => Err(ProviderAccountError::CredentialStore(error)),
        }
    }

    pub async fn disconnect_provider_account<V: SecretStore>(
        &self,
        secret_store: &V,
        id: ProviderAccountId,
        updated_at_unix_ms: u64,
    ) -> Result<ProviderAccount, ProviderAccountError> {
        let _lock = self.lifecycle_lock(&id).await;
        let current = self.get_provider_account(id).await?;
        let provider_type = current.provider_type().clone();
        let old_secret_ref = current.secret_ref().cloned();
        let next = current.disconnect(updated_at_unix_ms)?;
        self.store
            .update_provider_account(&current, &next)
            .await
            .map_err(map_provider_account_store_error)?;
        if let Some(secret_ref) = old_secret_ref {
            if secret_store
                .delete(&provider_type, next.id(), &secret_ref)
                .await
                .is_err_and(|error| error != SecretStoreError::NotFound)
            {
                return Err(ProviderAccountError::CredentialCleanupRequired { secret_ref });
            }
        }
        Ok(next)
    }

    async fn lifecycle_lock(&self, id: &ProviderAccountId) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self
                .lifecycle_locks
                .lock()
                .expect("provider account lifecycle locks are not poisoned");
            locks
                .entry(id.clone())
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone()
        };
        lock.lock_owned().await
    }

    pub async fn mark_provider_account_used(
        &self,
        id: ProviderAccountId,
        used_at_unix_ms: u64,
    ) -> Result<ProviderAccount, ProviderAccountError> {
        let current = self.get_provider_account(id).await?;
        if current.state() != ProviderAccountState::Connected {
            return Err(ProviderAccountError::AccountNotConnected);
        }
        let next = current.mark_used(used_at_unix_ms)?;
        self.store
            .update_provider_account(&current, &next)
            .await
            .map_err(map_provider_account_store_error)?;
        Ok(next)
    }

    pub async fn mark_provider_account_capabilities_refreshed(
        &self,
        id: ProviderAccountId,
        refreshed_at_unix_ms: u64,
    ) -> Result<ProviderAccount, ProviderAccountError> {
        let current = self.get_provider_account(id).await?;
        let next = current.mark_capabilities_refreshed(refreshed_at_unix_ms)?;
        self.store
            .update_provider_account(&current, &next)
            .await
            .map_err(map_provider_account_store_error)?;
        Ok(next)
    }

    pub async fn associate_provider_account(
        &self,
        id: ProviderAccountId,
        workspace_id: WorkspaceId,
    ) -> Result<(), ProviderAccountError> {
        self.store
            .associate_provider_account(&id, &workspace_id)
            .await
            .map_err(map_provider_account_store_error)
    }

    pub async fn resolve_for_workspace(
        &self,
        id: ProviderAccountId,
        provider_type: ProviderType,
        workspace_id: WorkspaceId,
    ) -> Result<ProviderAccount, ProviderAccountError> {
        self.store
            .account_available_for_workspace(&id, &provider_type, &workspace_id)
            .await
            .map_err(map_provider_account_store_error)
    }
}

fn map_provider_account_store_error(error: ProviderAccountStoreError) -> ProviderAccountError {
    match error {
        ProviderAccountStoreError::AccountNotFound => ProviderAccountError::AccountNotFound,
        ProviderAccountStoreError::ProviderTypeMismatch => {
            ProviderAccountError::ProviderTypeMismatch
        }
        ProviderAccountStoreError::AccountNotConnected => ProviderAccountError::AccountNotConnected,
        ProviderAccountStoreError::ProviderAccountLimitReached => {
            ProviderAccountError::ProviderAccountLimitReached
        }
        ProviderAccountStoreError::WorkspaceNotFound
        | ProviderAccountStoreError::WorkspaceAssociationMismatch => {
            ProviderAccountError::WorkspaceAssociationMismatch
        }
        ProviderAccountStoreError::IdempotencyConflict => ProviderAccountError::IdempotencyConflict,
        ProviderAccountStoreError::IntegrityViolation => ProviderAccountError::IntegrityViolation,
        ProviderAccountStoreError::Unavailable => ProviderAccountError::StoreUnavailable,
    }
}
