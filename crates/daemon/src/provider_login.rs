use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use kiln_core::{
    CreateProviderAccount, ProviderAccount, ProviderAccountApplication, ProviderAccountError,
    ProviderAccountId, ProviderAccountState, ProviderType,
};
use kiln_infrastructure::{OsSecretStore, SqliteStore, UlidIdGenerator};
use kiln_protocol::{ProviderAccountLoginFailure, ProviderAccountLoginState};
use kiln_providers::{
    CodexBrowserAuthorization, CodexBrowserLoginClient, CodexBrowserLoginError,
    CodexDeviceAuthorization, CodexDeviceLoginClient, CodexDeviceLoginError,
    OPENAI_CODEX_SUBSCRIPTION_PROVIDER_TYPE,
};
use kiln_server::{
    ProviderAccountCreateCommand, ProviderAccountLoginStart, ProviderAccountLoginStatus,
    ProviderAccountOperationError, ProviderAccountOperations,
};
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
};
use ulid::Ulid;

pub(crate) struct ProviderAccountLoginCoordinator {
    application: Arc<ProviderAccountApplication<SqliteStore, UlidIdGenerator>>,
    secret_store: OsSecretStore,
    client: Arc<CodexDeviceLoginClient>,
    browser_client: Arc<CodexBrowserLoginClient>,
    control: Mutex<bool>,
    attempts: Mutex<HashMap<ProviderAccountId, Arc<LoginAttempt>>>,
}

enum LoginAuthorization {
    Device(Arc<CodexDeviceLoginClient>, CodexDeviceAuthorization),
    Browser(Arc<CodexBrowserLoginClient>, CodexBrowserAuthorization),
}

struct LoginAttempt {
    id: String,
    cancellation: watch::Sender<bool>,
    task: Mutex<AttemptTask>,
}

enum AttemptTask {
    Running(JoinHandle<Result<ProviderAccount, ProviderAccountLoginError>>),
    Complete(Result<ProviderAccount, ProviderAccountLoginError>),
}

pub(crate) struct ProviderAccountLoginDisplay {
    attempt_id: String,
    verification_url: String,
    user_code: String,
}

#[derive(Debug, Clone)]
pub(crate) enum ProviderAccountLoginError {
    Account(ProviderAccountError),
    DeviceUnavailable,
    DeviceFailed,
    Declined,
    Expired,
    InvalidProvider,
    InvalidState,
    AttemptNotFound,
    Cancelled,
    ClockUnavailable,
    TaskFailed,
}

impl ProviderAccountLoginCoordinator {
    pub(crate) fn new(
        application: Arc<ProviderAccountApplication<SqliteStore, UlidIdGenerator>>,
        secret_store: OsSecretStore,
    ) -> Result<Self, ProviderAccountLoginError> {
        Ok(Self {
            application,
            secret_store,
            client: Arc::new(
                CodexDeviceLoginClient::new()
                    .map_err(|_| ProviderAccountLoginError::DeviceFailed)?,
            ),
            browser_client: Arc::new(
                CodexBrowserLoginClient::new()
                    .map_err(|_| ProviderAccountLoginError::DeviceFailed)?,
            ),
            control: Mutex::new(false),
            attempts: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) async fn begin(
        &self,
        account_id: ProviderAccountId,
        browser: bool,
    ) -> Result<ProviderAccountLoginDisplay, ProviderAccountLoginError> {
        let control = self.control.lock().await;
        if *control {
            return Err(ProviderAccountLoginError::InvalidState);
        }
        // Release the registry lock before joining. The terminal attempt stays
        // addressable until a replacement is actually ready to be published.
        let previous = self.attempts.lock().await.get(&account_id).cloned();
        if let Some(previous) = previous {
            previous.cancellation.send_replace(true);
            if let Err(
                error @ ProviderAccountLoginError::Account(
                    ProviderAccountError::CredentialCleanupRequired { .. },
                ),
            ) = join_attempt(&previous).await
            {
                // This result owns the only retained reference to failed cleanup.
                // A replacement must not erase it or start another credential write.
                return Err(error);
            }
        }
        let account = self
            .application
            .get_provider_account(account_id.clone())
            .await
            .map_err(ProviderAccountLoginError::Account)?;
        if account.provider_type().as_str() != OPENAI_CODEX_SUBSCRIPTION_PROVIDER_TYPE {
            return Err(ProviderAccountLoginError::InvalidProvider);
        }
        if !matches!(
            account.state(),
            ProviderAccountState::Connecting | ProviderAccountState::Disconnected
        ) || account.secret_ref().is_some()
        {
            return Err(ProviderAccountLoginError::InvalidState);
        }
        self.application
            .prepare_provider_account_login(
                account_id.clone(),
                account.provider_type().clone(),
                now()?,
            )
            .await
            .map_err(ProviderAccountLoginError::Account)?;
        let (authorization, verification_url, user_code) = if browser {
            let authorization = self
                .browser_client
                .begin()
                .await
                .map_err(map_browser_error)?;
            let url = authorization.authorization_url().to_owned();
            (
                LoginAuthorization::Browser(Arc::clone(&self.browser_client), authorization),
                url,
                String::new(),
            )
        } else {
            let authorization = self.client.begin().await.map_err(map_device_error)?;
            let url = authorization.verification_url().to_owned();
            let code = authorization.user_code().to_owned();
            (
                LoginAuthorization::Device(Arc::clone(&self.client), authorization),
                url,
                code,
            )
        };
        let display = ProviderAccountLoginDisplay {
            attempt_id: format!("pla_{}", Ulid::generate()),
            verification_url,
            user_code,
        };
        let mut attempts = self.attempts.lock().await;
        let (cancellation, receiver) = watch::channel(false);
        let application = Arc::clone(&self.application);
        let secret_store = self.secret_store.clone();
        let task_account_id = account_id.clone();
        let task = tokio::spawn(async move {
            run_attempt(
                application,
                secret_store,
                task_account_id,
                authorization,
                receiver,
            )
            .await
        });
        attempts.insert(
            account_id,
            Arc::new(LoginAttempt {
                id: display.attempt_id.clone(),
                cancellation,
                task: Mutex::new(AttemptTask::Running(task)),
            }),
        );
        Ok(display)
    }

    pub(crate) async fn cancel(
        &self,
        account_id: ProviderAccountId,
        attempt_id: String,
    ) -> Result<ProviderAccountLoginStatus, ProviderAccountLoginError> {
        let attempt = self.attempt(&account_id, &attempt_id).await?;
        attempt.cancellation.send_replace(true);
        let _ = join_attempt(&attempt).await;
        self.status_for_attempt(account_id, attempt_id, attempt)
            .await
    }

    pub(crate) async fn status(
        &self,
        account_id: ProviderAccountId,
        attempt_id: String,
    ) -> Result<ProviderAccountLoginStatus, ProviderAccountLoginError> {
        let attempt = self.attempt(&account_id, &attempt_id).await?;
        self.status_for_attempt(account_id, attempt_id, attempt)
            .await
    }

    pub(crate) async fn create_account(
        &self,
        command: ProviderAccountCreateCommand,
    ) -> Result<ProviderAccount, ProviderAccountError> {
        self.application
            .create_provider_account_with_idempotency(
                CreateProviderAccount {
                    provider_type: command.provider_type,
                    label: command.label,
                    subject: None,
                    secret_ref: None,
                    state: ProviderAccountState::Connecting,
                    workspace_ids: command.workspace_ids,
                    created_at_unix_ms: now_millis()
                        .map_err(|_| ProviderAccountError::StoreUnavailable)?,
                    metadata: Default::default(),
                },
                command.idempotency_key,
            )
            .await
    }

    pub(crate) async fn list_accounts(&self) -> Result<Vec<ProviderAccount>, ProviderAccountError> {
        self.application.list_provider_accounts(None).await
    }

    pub(crate) async fn disconnect_account(
        &self,
        account_id: ProviderAccountId,
    ) -> Result<ProviderAccount, ProviderAccountLoginError> {
        // Serialize against new attempts and shutdown. A dropped caller leaves
        // either the owned login task or the disabled durable account retryable.
        let control = self.control.lock().await;
        if *control {
            return Err(ProviderAccountLoginError::InvalidState);
        }
        let previous = self.attempts.lock().await.get(&account_id).cloned();
        let mut cleanup_ref = None;
        if let Some(previous) = &previous {
            previous.cancellation.send_replace(true);
            if let Err(ProviderAccountLoginError::Account(
                ProviderAccountError::CredentialCleanupRequired { secret_ref },
            )) = join_attempt(previous).await
            {
                cleanup_ref = Some(secret_ref);
            }
        }
        let account = self
            .application
            .disconnect_provider_account(&self.secret_store, account_id.clone(), now()?)
            .await
            .map_err(ProviderAccountLoginError::Account)?;
        if let Some(secret_ref) = cleanup_ref {
            // A failed login may also retain an unpublished vault entry. Keep
            // the attempt (and its reference) until this deletion succeeds.
            self.application
                .cleanup_provider_account_secret(
                    &self.secret_store,
                    account_id.clone(),
                    account.provider_type().clone(),
                    secret_ref.clone(),
                )
                .await
                .map_err(|_| {
                    ProviderAccountLoginError::Account(
                        ProviderAccountError::CredentialCleanupRequired { secret_ref },
                    )
                })?;
        }
        if let Some(previous) = previous {
            self.remove_if_same(&account_id, &previous).await;
        }
        Ok(account)
    }

    pub(crate) async fn get_account(
        &self,
        account_id: ProviderAccountId,
    ) -> Result<ProviderAccount, ProviderAccountError> {
        self.application.get_provider_account(account_id).await
    }

    pub(crate) async fn shutdown(&self) -> Result<(), ProviderAccountLoginError> {
        let mut control = self.control.lock().await;
        *control = true;
        let attempts = self
            .attempts
            .lock()
            .await
            .iter()
            .map(|(id, attempt)| (id.clone(), Arc::clone(attempt)))
            .collect::<Vec<_>>();
        for (_, attempt) in &attempts {
            attempt.cancellation.send_replace(true);
        }
        let mut first_error = None;
        for (account_id, attempt) in attempts {
            match join_attempt(&attempt).await {
                // Provider failures finish before any account credential write.
                Ok(_)
                | Err(
                    ProviderAccountLoginError::Cancelled
                    | ProviderAccountLoginError::Declined
                    | ProviderAccountLoginError::Expired
                    | ProviderAccountLoginError::DeviceUnavailable
                    | ProviderAccountLoginError::DeviceFailed,
                ) => {}
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
            self.remove_if_same(&account_id, &attempt).await;
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn remove_if_same(&self, account_id: &ProviderAccountId, attempt: &Arc<LoginAttempt>) {
        let mut attempts = self.attempts.lock().await;
        if attempts
            .get(account_id)
            .is_some_and(|current| Arc::ptr_eq(current, attempt))
        {
            attempts.remove(account_id);
        }
    }

    async fn attempt(
        &self,
        account_id: &ProviderAccountId,
        attempt_id: &str,
    ) -> Result<Arc<LoginAttempt>, ProviderAccountLoginError> {
        self.attempts
            .lock()
            .await
            .get(account_id)
            .filter(|attempt| attempt.id == attempt_id)
            .cloned()
            .ok_or(ProviderAccountLoginError::AttemptNotFound)
    }

    async fn status_for_attempt(
        &self,
        account_id: ProviderAccountId,
        attempt_id: String,
        attempt: Arc<LoginAttempt>,
    ) -> Result<ProviderAccountLoginStatus, ProviderAccountLoginError> {
        let (state, completed) = poll_attempt(&attempt).await;
        let failure = completed
            .as_ref()
            .and_then(|outcome| outcome.as_ref().err().and_then(login_failure));
        let account = match completed {
            Some(Ok(account)) => account,
            Some(Err(_)) | None => self
                .application
                .get_provider_account(account_id)
                .await
                .map_err(ProviderAccountLoginError::Account)?,
        };
        Ok(ProviderAccountLoginStatus {
            attempt_id,
            state,
            failure,
            account,
        })
    }
}

async fn join_attempt(
    attempt: &Arc<LoginAttempt>,
) -> Result<ProviderAccount, ProviderAccountLoginError> {
    let mut task = attempt.task.lock().await;
    match &mut *task {
        AttemptTask::Complete(outcome) => outcome.clone(),
        AttemptTask::Running(handle) => {
            let outcome = (&mut *handle)
                .await
                .map_err(|_| ProviderAccountLoginError::TaskFailed)
                .and_then(|outcome| outcome);
            *task = AttemptTask::Complete(outcome.clone());
            outcome
        }
    }
}

async fn poll_attempt(
    attempt: &Arc<LoginAttempt>,
) -> (
    ProviderAccountLoginState,
    Option<Result<ProviderAccount, ProviderAccountLoginError>>,
) {
    // Cancellation/replacement can be joining a provider request or vault write.
    // Status must remain nonblocking while that owner finishes the task.
    let Ok(mut task) = attempt.task.try_lock() else {
        return (ProviderAccountLoginState::Pending, None);
    };
    let outcome = match &mut *task {
        AttemptTask::Complete(outcome) => Some(outcome.clone()),
        AttemptTask::Running(handle) if !handle.is_finished() => None,
        AttemptTask::Running(handle) => {
            let outcome = (&mut *handle)
                .await
                .map_err(|_| ProviderAccountLoginError::TaskFailed)
                .and_then(|outcome| outcome);
            *task = AttemptTask::Complete(outcome.clone());
            Some(outcome)
        }
    };
    let state = match outcome.as_ref() {
        None => ProviderAccountLoginState::Pending,
        Some(Ok(_)) => ProviderAccountLoginState::Connected,
        Some(Err(ProviderAccountLoginError::Cancelled)) => ProviderAccountLoginState::Cancelled,
        Some(Err(ProviderAccountLoginError::Account(
            ProviderAccountError::CredentialCleanupRequired { .. },
        ))) => ProviderAccountLoginState::CleanupRequired,
        Some(Err(_)) => ProviderAccountLoginState::Failed,
    };
    (state, outcome)
}

async fn run_attempt(
    application: Arc<ProviderAccountApplication<SqliteStore, UlidIdGenerator>>,
    secret_store: OsSecretStore,
    account_id: ProviderAccountId,
    authorization: LoginAuthorization,
    cancellation: watch::Receiver<bool>,
) -> Result<ProviderAccount, ProviderAccountLoginError> {
    let result = match authorization {
        LoginAuthorization::Device(client, authorization) => client
            .complete(authorization, cancellation.clone())
            .await
            .map_err(map_device_error),
        LoginAuthorization::Browser(client, authorization) => client
            .complete(authorization, cancellation.clone())
            .await
            .map_err(map_browser_error),
    }?;
    if *cancellation.borrow() {
        return Err(ProviderAccountLoginError::Cancelled);
    }
    let provider_type = ProviderType::parse(OPENAI_CODEX_SUBSCRIPTION_PROVIDER_TYPE)
        .map_err(|_| ProviderAccountLoginError::InvalidProvider)?;
    let connected = application
        .connect_provider_account_if_connecting(
            &secret_store,
            account_id.clone(),
            provider_type,
            result.into_secret(),
            now()?,
        )
        .await
        .map_err(ProviderAccountLoginError::Account)?;
    if *cancellation.borrow() {
        let secret_ref = connected
            .secret_ref()
            .cloned()
            .ok_or(ProviderAccountLoginError::InvalidState)?;
        application
            .disconnect_provider_account_if_current(&secret_store, account_id, secret_ref, now()?)
            .await
            .map_err(ProviderAccountLoginError::Account)?;
        return Err(ProviderAccountLoginError::Cancelled);
    }
    Ok(connected)
}

fn map_device_error(error: CodexDeviceLoginError) -> ProviderAccountLoginError {
    match error {
        CodexDeviceLoginError::Cancelled => ProviderAccountLoginError::Cancelled,
        CodexDeviceLoginError::Rejected => ProviderAccountLoginError::Declined,
        CodexDeviceLoginError::Expired => ProviderAccountLoginError::Expired,
        CodexDeviceLoginError::Unavailable | CodexDeviceLoginError::Transport => {
            ProviderAccountLoginError::DeviceUnavailable
        }
        _ => ProviderAccountLoginError::DeviceFailed,
    }
}

fn login_failure(error: &ProviderAccountLoginError) -> Option<ProviderAccountLoginFailure> {
    match error {
        ProviderAccountLoginError::Declined => Some(ProviderAccountLoginFailure::Declined),
        ProviderAccountLoginError::Expired => Some(ProviderAccountLoginFailure::Expired),
        ProviderAccountLoginError::DeviceUnavailable => {
            Some(ProviderAccountLoginFailure::ProviderUnavailable)
        }
        ProviderAccountLoginError::Account(
            ProviderAccountError::CredentialStore(_)
            | ProviderAccountError::CredentialStoreRequired,
        ) => Some(ProviderAccountLoginFailure::CredentialStoreUnavailable),
        ProviderAccountLoginError::Account(ProviderAccountError::StoreUnavailable) => {
            Some(ProviderAccountLoginFailure::AccountStoreUnavailable)
        }
        _ => None,
    }
}

fn map_browser_error(error: CodexBrowserLoginError) -> ProviderAccountLoginError {
    match error {
        CodexBrowserLoginError::Login(error) => map_device_error(error),
        CodexBrowserLoginError::CallbackUnavailable => ProviderAccountLoginError::DeviceUnavailable,
        CodexBrowserLoginError::RandomUnavailable => ProviderAccountLoginError::DeviceFailed,
    }
}

fn now() -> Result<u64, ProviderAccountLoginError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .ok_or(ProviderAccountLoginError::ClockUnavailable)
}

fn now_millis() -> Result<u64, ProviderAccountLoginError> {
    now()
}

impl ProviderAccountOperations for ProviderAccountLoginCoordinator {
    fn disconnect_provider_account(
        &self,
        account_id: ProviderAccountId,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<ProviderAccount, ProviderAccountOperationError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.disconnect_account(account_id)
                .await
                .map_err(ProviderAccountOperationError::from)
        })
    }

    fn create_provider_account(
        &self,
        command: ProviderAccountCreateCommand,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<ProviderAccount, ProviderAccountOperationError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.create_account(command)
                .await
                .map_err(ProviderAccountOperationError::from)
        })
    }

    fn list_provider_accounts(
        &self,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Vec<ProviderAccount>, ProviderAccountOperationError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.list_accounts()
                .await
                .map_err(ProviderAccountOperationError::from)
        })
    }

    fn get_provider_account(
        &self,
        account_id: ProviderAccountId,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<ProviderAccount, ProviderAccountOperationError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.get_account(account_id)
                .await
                .map_err(ProviderAccountOperationError::from)
        })
    }

    fn start_provider_account_login(
        &self,
        account_id: ProviderAccountId,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ProviderAccountLoginStart, ProviderAccountOperationError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let display = self
                .begin(account_id.clone(), false)
                .await
                .map_err(ProviderAccountOperationError::from)?;
            let account = self
                .get_account(account_id)
                .await
                .map_err(ProviderAccountOperationError::from)?;
            Ok(ProviderAccountLoginStart {
                attempt_id: display.attempt_id,
                verification_url: display.verification_url,
                user_code: display.user_code,
                account,
            })
        })
    }

    fn start_provider_account_browser_login(
        &self,
        account_id: ProviderAccountId,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        kiln_server::ProviderAccountBrowserLoginStart,
                        ProviderAccountOperationError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let display = self
                .begin(account_id.clone(), true)
                .await
                .map_err(ProviderAccountOperationError::from)?;
            let account = self
                .get_account(account_id)
                .await
                .map_err(ProviderAccountOperationError::from)?;
            Ok(kiln_server::ProviderAccountBrowserLoginStart {
                attempt_id: display.attempt_id,
                authorization_url: display.verification_url,
                account,
            })
        })
    }

    fn get_provider_account_login(
        &self,
        account_id: ProviderAccountId,
        attempt_id: String,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ProviderAccountLoginStatus, ProviderAccountOperationError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.status(account_id, attempt_id)
                .await
                .map_err(ProviderAccountOperationError::from)
        })
    }

    fn cancel_provider_account_login(
        &self,
        account_id: ProviderAccountId,
        attempt_id: String,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ProviderAccountLoginStatus, ProviderAccountOperationError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.cancel(account_id, attempt_id)
                .await
                .map_err(ProviderAccountOperationError::from)
        })
    }
}

impl From<ProviderAccountLoginError> for ProviderAccountOperationError {
    fn from(error: ProviderAccountLoginError) -> Self {
        match error {
            ProviderAccountLoginError::Account(error) => error.into(),
            ProviderAccountLoginError::AttemptNotFound => Self::AttemptNotFound,
            ProviderAccountLoginError::InvalidProvider
            | ProviderAccountLoginError::InvalidState => Self::InvalidState,
            ProviderAccountLoginError::Cancelled => Self::Cancelled,
            ProviderAccountLoginError::DeviceUnavailable => Self::LoginUnavailable,
            ProviderAccountLoginError::DeviceFailed
            | ProviderAccountLoginError::Declined
            | ProviderAccountLoginError::Expired
            | ProviderAccountLoginError::ClockUnavailable
            | ProviderAccountLoginError::TaskFailed => Self::LoginFailed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kiln_core::{SecretRef, SecretStoreError};

    #[tokio::test]
    async fn shutdown_drains_provider_failures_but_preserves_credential_cleanup_failures() {
        let data = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(data.path()).await.unwrap();
        let application = Arc::new(ProviderAccountApplication::new(store, UlidIdGenerator));
        let coordinator = ProviderAccountLoginCoordinator::new(
            Arc::clone(&application),
            OsSecretStore::open_default(),
        )
        .unwrap();
        for error in [
            ProviderAccountLoginError::Declined,
            ProviderAccountLoginError::Expired,
            ProviderAccountLoginError::DeviceUnavailable,
            ProviderAccountLoginError::DeviceFailed,
        ] {
            let (cancellation, _) = watch::channel(false);
            coordinator.attempts.lock().await.insert(
                ProviderAccountId::from_ulid(Ulid::generate()),
                Arc::new(LoginAttempt {
                    id: "fixture-attempt".to_owned(),
                    cancellation,
                    task: Mutex::new(AttemptTask::Complete(Err(error))),
                }),
            );
        }
        coordinator.shutdown().await.unwrap();
        assert!(coordinator.attempts.lock().await.is_empty());

        let coordinator =
            ProviderAccountLoginCoordinator::new(application, OsSecretStore::open_default())
                .unwrap();
        let (cancellation, _) = watch::channel(false);
        coordinator.attempts.lock().await.insert(
            ProviderAccountId::from_ulid(Ulid::generate()),
            Arc::new(LoginAttempt {
                id: "fixture-cleanup".to_owned(),
                cancellation,
                task: Mutex::new(AttemptTask::Complete(Err(
                    ProviderAccountLoginError::Account(
                        ProviderAccountError::CredentialCleanupRequired {
                            secret_ref: SecretRef::from_ulid(Ulid::generate()),
                        },
                    ),
                ))),
            }),
        );
        assert!(matches!(
            coordinator.shutdown().await,
            Err(ProviderAccountLoginError::Account(
                ProviderAccountError::CredentialCleanupRequired { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn failed_login_status_preserves_safe_recovery_categories() {
        for (error, expected) in [
            (
                map_device_error(CodexDeviceLoginError::Expired),
                Some(ProviderAccountLoginFailure::Expired),
            ),
            (
                map_browser_error(CodexBrowserLoginError::Login(
                    CodexDeviceLoginError::Rejected,
                )),
                Some(ProviderAccountLoginFailure::Declined),
            ),
            (
                map_device_error(CodexDeviceLoginError::Transport),
                Some(ProviderAccountLoginFailure::ProviderUnavailable),
            ),
            (
                ProviderAccountLoginError::Account(ProviderAccountError::CredentialStore(
                    SecretStoreError::Unavailable,
                )),
                Some(ProviderAccountLoginFailure::CredentialStoreUnavailable),
            ),
            (
                ProviderAccountLoginError::Account(ProviderAccountError::StoreUnavailable),
                Some(ProviderAccountLoginFailure::AccountStoreUnavailable),
            ),
            (ProviderAccountLoginError::DeviceFailed, None),
        ] {
            let (cancellation, _) = watch::channel(false);
            let attempt = Arc::new(LoginAttempt {
                id: "fixture-attempt".to_owned(),
                cancellation,
                task: Mutex::new(AttemptTask::Complete(Err(error))),
            });
            let (state, outcome) = poll_attempt(&attempt).await;
            assert_eq!(state, ProviderAccountLoginState::Failed);
            assert_eq!(login_failure(&outcome.unwrap().unwrap_err()), expected);
        }
        // Cleanup keeps its distinct state and never exports its private reference.
        let error =
            ProviderAccountLoginError::Account(ProviderAccountError::CredentialCleanupRequired {
                secret_ref: SecretRef::from_ulid(Ulid::generate()),
            });
        assert_eq!(login_failure(&error), None);
        assert_eq!(login_failure(&ProviderAccountLoginError::Cancelled), None);
    }
}
