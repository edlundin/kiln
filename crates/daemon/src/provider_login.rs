use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use kiln_core::{
    ProviderAccount, ProviderAccountApplication, ProviderAccountError, ProviderAccountId,
    ProviderAccountState, ProviderType,
};
use kiln_infrastructure::{OsSecretStore, SqliteStore, UlidIdGenerator};
use kiln_providers::{
    CodexDeviceAuthorization, CodexDeviceLoginClient, CodexDeviceLoginError,
    OPENAI_CODEX_SUBSCRIPTION_PROVIDER_TYPE,
};
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
};

pub(crate) struct ProviderAccountLoginCoordinator {
    application: Arc<ProviderAccountApplication<SqliteStore, UlidIdGenerator>>,
    secret_store: OsSecretStore,
    client: Arc<CodexDeviceLoginClient>,
    control: Mutex<bool>,
    attempts: Mutex<HashMap<ProviderAccountId, Arc<LoginAttempt>>>,
}

struct LoginAttempt {
    cancellation: watch::Sender<bool>,
    task: Mutex<AttemptTask>,
}

enum AttemptTask {
    Running(JoinHandle<Result<ProviderAccount, ProviderAccountLoginError>>),
    Complete(Result<ProviderAccount, ProviderAccountLoginError>),
}

pub(crate) struct ProviderAccountLoginDisplay {
    verification_url: String,
    user_code: String,
}

impl ProviderAccountLoginDisplay {
    pub(crate) fn verification_url(&self) -> &str {
        &self.verification_url
    }
    pub(crate) fn user_code(&self) -> &str {
        &self.user_code
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ProviderAccountLoginError {
    Account(ProviderAccountError),
    Device(CodexDeviceLoginError),
    InvalidProvider,
    InvalidState,
    Cancelled,
    ClockUnavailable,
    TaskFailed,
}

impl ProviderAccountLoginCoordinator {
    pub(crate) fn new(
        store: SqliteStore,
        secret_store: OsSecretStore,
    ) -> Result<Self, ProviderAccountLoginError> {
        Ok(Self {
            application: Arc::new(ProviderAccountApplication::new(store, UlidIdGenerator)),
            secret_store,
            client: Arc::new(
                CodexDeviceLoginClient::new().map_err(ProviderAccountLoginError::Device)?,
            ),
            control: Mutex::new(false),
            attempts: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) async fn begin(
        &self,
        account_id: ProviderAccountId,
    ) -> Result<ProviderAccountLoginDisplay, ProviderAccountLoginError> {
        let control = self.control.lock().await;
        if *control {
            return Err(ProviderAccountLoginError::InvalidState);
        }
        if let Some(previous) = self.attempts.lock().await.get(&account_id).cloned() {
            previous.cancellation.send_replace(true);
            match join_attempt(&previous).await {
                Ok(_) | Err(ProviderAccountLoginError::Cancelled) => {}
                Err(error) => return Err(error),
            }
            self.remove_if_same(&account_id, &previous).await;
        }
        let account = self
            .application
            .get_provider_account(account_id.clone())
            .await
            .map_err(ProviderAccountLoginError::Account)?;
        if account.provider_type().as_str() != OPENAI_CODEX_SUBSCRIPTION_PROVIDER_TYPE {
            return Err(ProviderAccountLoginError::InvalidProvider);
        }
        if account.state() != ProviderAccountState::Connecting || account.secret_ref().is_some() {
            return Err(ProviderAccountLoginError::InvalidState);
        }
        let authorization = self
            .client
            .begin()
            .await
            .map_err(ProviderAccountLoginError::Device)?;
        let display = ProviderAccountLoginDisplay {
            verification_url: authorization.verification_url().to_owned(),
            user_code: authorization.user_code().to_owned(),
        };
        let (cancellation, receiver) = watch::channel(false);
        let application = Arc::clone(&self.application);
        let secret_store = self.secret_store.clone();
        let client = Arc::clone(&self.client);
        let task_account_id = account_id.clone();
        let task = tokio::spawn(async move {
            run_attempt(
                application,
                secret_store,
                client,
                task_account_id,
                authorization,
                receiver,
            )
            .await
        });
        self.attempts.lock().await.insert(
            account_id,
            Arc::new(LoginAttempt {
                cancellation,
                task: Mutex::new(AttemptTask::Running(task)),
            }),
        );
        Ok(display)
    }

    pub(crate) async fn finish(
        &self,
        account_id: ProviderAccountId,
    ) -> Result<ProviderAccount, ProviderAccountLoginError> {
        let attempt = self
            .attempts
            .lock()
            .await
            .get(&account_id)
            .cloned()
            .ok_or(ProviderAccountLoginError::InvalidState)?;
        let outcome = join_attempt(&attempt).await;
        self.remove_if_same(&account_id, &attempt).await;
        outcome
    }

    pub(crate) async fn cancel(
        &self,
        account_id: ProviderAccountId,
    ) -> Result<(), ProviderAccountLoginError> {
        let attempt = self
            .attempts
            .lock()
            .await
            .get(&account_id)
            .cloned()
            .ok_or(ProviderAccountLoginError::InvalidState)?;
        attempt.cancellation.send_replace(true);
        let outcome = match join_attempt(&attempt).await {
            Err(ProviderAccountLoginError::Cancelled) => Ok(()),
            Ok(_) => Err(ProviderAccountLoginError::InvalidState),
            Err(error) => Err(error),
        };
        self.remove_if_same(&account_id, &attempt).await;
        outcome
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
                Ok(_) | Err(ProviderAccountLoginError::Cancelled) => {}
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

async fn run_attempt(
    application: Arc<ProviderAccountApplication<SqliteStore, UlidIdGenerator>>,
    secret_store: OsSecretStore,
    client: Arc<CodexDeviceLoginClient>,
    account_id: ProviderAccountId,
    authorization: CodexDeviceAuthorization,
    cancellation: watch::Receiver<bool>,
) -> Result<ProviderAccount, ProviderAccountLoginError> {
    let result = client
        .complete(authorization, cancellation.clone())
        .await
        .map_err(|error| {
            if error == CodexDeviceLoginError::Cancelled {
                ProviderAccountLoginError::Cancelled
            } else {
                ProviderAccountLoginError::Device(error)
            }
        })?;
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

fn now() -> Result<u64, ProviderAccountLoginError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .ok_or(ProviderAccountLoginError::ClockUnavailable)
}
