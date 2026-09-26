//! Host-local resolution metadata for portable model account binding keys.

use crate::{
    ProviderAccountId, ProviderAccountState, ProviderAccountStore, ProviderAccountStoreError,
    ProviderType, SharedConfigurationKey,
};
use std::future::Future;

/// Matches the key budget used by configuration snapshot publication.
pub const HOST_MODEL_ACCOUNT_BINDING_KEY_MAX_BYTES: usize = 2 * 1024 * 1024;
/// Model provider metadata uses the snapshot publication metadata budget.
pub const HOST_MODEL_ACCOUNT_BINDING_PROVIDER_TYPE_MAX_BYTES: usize = 2 * 1024 * 1024;
pub const HOST_MODEL_ACCOUNT_BINDING_DEFAULT_PAGE_SIZE: usize = 50;
pub const HOST_MODEL_ACCOUNT_BINDING_MAX_PAGE_SIZE: usize = 100;

/// Durable mapping state. A missing key has version zero; a removed mapping
/// keeps its last version so a stale update cannot succeed after recreation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostModelAccountBinding {
    pub key: SharedConfigurationKey,
    pub provider_account_id: Option<ProviderAccountId>,
    pub version: u64,
}

/// Safe current account metadata for a local binding. Credential references,
/// provider subjects, arbitrary provider metadata and secret material are not
/// part of this view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostModelAccountBindingAccount {
    pub id: ProviderAccountId,
    pub provider_type: ProviderType,
    pub label: String,
    pub state: ProviderAccountState,
}

/// Binding state with the account's current durable metadata. Account state is
/// informational; callers must recheck execution eligibility when resolving a
/// future Run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostModelAccountBindingView {
    pub key: SharedConfigurationKey,
    pub version: u64,
    pub account: Option<HostModelAccountBindingAccount>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostModelAccountBindingError {
    InvalidRequest,
    Conflict,
    ProviderAccountNotFound,
    ProviderTypeMismatch,
    IntegrityViolation,
    Unavailable,
}

/// Storage boundary for the local mapping table. Listing omits removed keys;
/// `get` retains tombstone versions so mutations can use exact CAS checks.
pub trait HostModelAccountBindingStore: Send + Sync {
    fn get_host_model_account_binding(
        &self,
        key: &SharedConfigurationKey,
    ) -> impl Future<Output = Result<HostModelAccountBinding, HostModelAccountBindingError>> + Send;

    fn list_host_model_account_bindings(
        &self,
        after: Option<&SharedConfigurationKey>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<HostModelAccountBinding>, HostModelAccountBindingError>> + Send;

    fn set_host_model_account_binding(
        &self,
        key: &SharedConfigurationKey,
        expected_version: u64,
        provider_account_id: &ProviderAccountId,
    ) -> impl Future<Output = Result<HostModelAccountBinding, HostModelAccountBindingError>> + Send;

    fn remove_host_model_account_binding(
        &self,
        key: &SharedConfigurationKey,
        expected_version: u64,
    ) -> impl Future<Output = Result<HostModelAccountBinding, HostModelAccountBindingError>> + Send;
}

/// Validates account existence and provider type before saving a local key.
/// It deliberately accepts disconnected and reauthentication-required records:
/// a saved mapping does not imply present provider access.
pub struct HostModelAccountBindingApplication<S> {
    store: S,
}

impl<S> HostModelAccountBindingApplication<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
}

impl<S> HostModelAccountBindingApplication<S>
where
    S: HostModelAccountBindingStore + ProviderAccountStore,
{
    pub async fn get(
        &self,
        key: SharedConfigurationKey,
    ) -> Result<HostModelAccountBindingView, HostModelAccountBindingError> {
        let binding = self.store.get_host_model_account_binding(&key).await?;
        self.resolve(binding).await
    }

    pub async fn list(
        &self,
        after: Option<SharedConfigurationKey>,
        limit: usize,
    ) -> Result<Vec<HostModelAccountBindingView>, HostModelAccountBindingError> {
        if !(1..=HOST_MODEL_ACCOUNT_BINDING_MAX_PAGE_SIZE + 1).contains(&limit) {
            return Err(HostModelAccountBindingError::InvalidRequest);
        }
        let bindings = self
            .store
            .list_host_model_account_bindings(after.as_ref(), limit)
            .await?;
        let mut views = Vec::with_capacity(bindings.len());
        for binding in bindings {
            views.push(self.resolve(binding).await?);
        }
        Ok(views)
    }

    pub async fn set(
        &self,
        key: SharedConfigurationKey,
        expected_version: u64,
        expected_provider_type: ProviderType,
        provider_account_id: ProviderAccountId,
    ) -> Result<HostModelAccountBindingView, HostModelAccountBindingError> {
        if expected_provider_type.as_str().len()
            > HOST_MODEL_ACCOUNT_BINDING_PROVIDER_TYPE_MAX_BYTES
        {
            return Err(HostModelAccountBindingError::InvalidRequest);
        }
        let account = self
            .store
            .get_provider_account(&provider_account_id)
            .await
            .map_err(map_provider_account_store_error)?
            .ok_or(HostModelAccountBindingError::ProviderAccountNotFound)?;
        if account.provider_type() != &expected_provider_type {
            return Err(HostModelAccountBindingError::ProviderTypeMismatch);
        }
        let binding = self
            .store
            .set_host_model_account_binding(&key, expected_version, &provider_account_id)
            .await?;
        self.resolve(binding).await
    }

    pub async fn remove(
        &self,
        key: SharedConfigurationKey,
        expected_version: u64,
    ) -> Result<HostModelAccountBindingView, HostModelAccountBindingError> {
        let binding = self
            .store
            .remove_host_model_account_binding(&key, expected_version)
            .await?;
        self.resolve(binding).await
    }

    async fn resolve(
        &self,
        binding: HostModelAccountBinding,
    ) -> Result<HostModelAccountBindingView, HostModelAccountBindingError> {
        let account = match binding.provider_account_id {
            Some(id) => {
                let account = self
                    .store
                    .get_provider_account(&id)
                    .await
                    .map_err(map_provider_account_store_error)?
                    .ok_or(HostModelAccountBindingError::IntegrityViolation)?;
                if account.provider_type().as_str().len()
                    > HOST_MODEL_ACCOUNT_BINDING_PROVIDER_TYPE_MAX_BYTES
                {
                    return Err(HostModelAccountBindingError::IntegrityViolation);
                }
                Some(HostModelAccountBindingAccount {
                    id: account.id().clone(),
                    provider_type: account.provider_type().clone(),
                    label: account.label().to_owned(),
                    state: account.state(),
                })
            }
            None => None,
        };
        Ok(HostModelAccountBindingView {
            key: binding.key,
            version: binding.version,
            account,
        })
    }
}

fn map_provider_account_store_error(
    error: ProviderAccountStoreError,
) -> HostModelAccountBindingError {
    match error {
        ProviderAccountStoreError::Unavailable => HostModelAccountBindingError::Unavailable,
        ProviderAccountStoreError::IntegrityViolation => {
            HostModelAccountBindingError::IntegrityViolation
        }
        ProviderAccountStoreError::AccountNotFound => {
            HostModelAccountBindingError::ProviderAccountNotFound
        }
        ProviderAccountStoreError::ProviderTypeMismatch
        | ProviderAccountStoreError::AccountNotConnected
        | ProviderAccountStoreError::ProviderAccountLimitReached
        | ProviderAccountStoreError::WorkspaceNotFound
        | ProviderAccountStoreError::WorkspaceAssociationMismatch
        | ProviderAccountStoreError::IdempotencyConflict => {
            HostModelAccountBindingError::IntegrityViolation
        }
    }
}
