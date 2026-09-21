//! Explicit local credential import. Secret bytes never enter the public API.

use std::{
    env,
    io::{IsTerminal, Read},
    process::ExitCode,
    time::{SystemTime, UNIX_EPOCH},
};

use kiln_core::{
    CreateProviderAccount, ProviderAccount, ProviderAccountApplication, ProviderAccountError,
    ProviderAccountState, ProviderType, SecretValue,
};
use kiln_infrastructure::{DaemonStoreLock, OsSecretStore, SqliteStore, UlidIdGenerator};
use kiln_providers::{OPENAI_API_PROVIDER_TYPE, OpenAiApiKey};

// Match the core secret ceiling, allowing only one optional trailing CRLF.
const MAX_KEY_BYTES: usize = 1024 * 1024;

pub(crate) async fn run() -> ExitCode {
    let args = env::args_os().skip(2).collect::<Vec<_>>();
    if args.len() == 1 && args[0] == "--help" {
        println!(
            "Usage: kilnd import-openai-api-key --stdin\n\
            Stop kilnd, then pipe one OpenAI API key from a secure input source.\n\
            This explicitly saves the key in the OS vault for the current Kiln data directory.\n\
            Keys are not accepted as arguments, environment variables, or terminal input.\n\
            Active accounts are never overwritten. Provider access is not validated."
        );
        return ExitCode::SUCCESS;
    }
    if args.len() != 1 || args[0] != "--stdin" {
        eprintln!("kilnd: use import-openai-api-key --stdin; keys must not be passed as arguments");
        return ExitCode::FAILURE;
    }
    match import().await {
        Ok(account) => {
            println!(
                "Saved OpenAI API credentials locally for {}.",
                account.id().as_str()
            );
            println!(
                "Restart kilnd to view the account in Settings. OpenAI authentication and model access were not checked; Runs still use the deterministic executor."
            );
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("kilnd: {message}");
            ExitCode::FAILURE
        }
    }
}

async fn import() -> Result<ProviderAccount, &'static str> {
    if std::io::stdin().is_terminal() {
        return Err("pipe the key from a secure input source; terminal input could echo it");
    }
    let _store_lock = DaemonStoreLock::open_default()
        .map_err(|_| "cannot lock the data directory; stop kilnd before importing credentials")?;
    let store = SqliteStore::open_default()
        .await
        .map_err(|_| "cannot open the account store")?;
    let application = ProviderAccountApplication::new(store, UlidIdGenerator);
    let provider_type = ProviderType::parse(OPENAI_API_PROVIDER_TYPE)
        .map_err(|_| "invalid built-in provider type")?;
    let accounts = application
        .list_provider_accounts(Some(&provider_type))
        .await
        .map_err(account_error)?;
    if accounts.iter().any(|account| {
        matches!(
            account.state(),
            ProviderAccountState::Connected | ProviderAccountState::ReauthRequired
        )
    }) {
        return Err(
            "an OpenAI API account is active; disconnect it in Settings before importing a replacement",
        );
    }

    let key = read_key()?;
    let account = match accounts
        .into_iter()
        .min_by_key(|account| account.state() != ProviderAccountState::Connecting)
    {
        Some(account) => account,
        None => application
            .create_provider_account(CreateProviderAccount {
                provider_type: provider_type.clone(),
                label: "OpenAI API".to_owned(),
                subject: None,
                secret_ref: None,
                state: ProviderAccountState::Connecting,
                workspace_ids: Vec::new(),
                created_at_unix_ms: now()?,
                metadata: Default::default(),
            })
            .await
            .map_err(account_error)?,
    };
    let credential = key
        .into_credential(account.id())
        .map_err(|_| "invalid or oversized OpenAI API credential")?;
    application
        .prepare_provider_account_login(account.id().clone(), provider_type.clone(), now()?)
        .await
        .map_err(account_error)?;
    application
        .connect_provider_account_if_connecting(
            &OsSecretStore::open_default(),
            account.id().clone(),
            provider_type,
            credential,
            now()?,
        )
        .await
        .map_err(account_error)
}

fn read_key() -> Result<OpenAiApiKey, &'static str> {
    let mut bytes = Vec::new();
    std::io::stdin()
        .lock()
        .take((MAX_KEY_BYTES + 3) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read the credential from stdin")?;
    if bytes.len() > MAX_KEY_BYTES + 2 {
        return Err("OpenAI API key exceeds the credential size limit");
    }
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    let secret = SecretValue::new(bytes).map_err(|_| "expected one nonempty OpenAI API key")?;
    OpenAiApiKey::new(secret)
        .map_err(|_| "OpenAI API key must contain visible ASCII without whitespace")
}

fn now() -> Result<u64, &'static str> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|value| u64::try_from(value.as_millis()).ok())
        .ok_or("system clock is unavailable")
}

fn account_error(error: ProviderAccountError) -> &'static str {
    match error {
        ProviderAccountError::CredentialCleanupRequired { .. } => {
            "credential cleanup is pending; start kilnd and disconnect this account in Settings before retrying"
        }
        ProviderAccountError::CredentialStore(_) => "the OS credential store is unavailable",
        ProviderAccountError::ProviderAccountLimitReached => "another OpenAI API account is active",
        ProviderAccountError::StoreUnavailable => {
            "the account store is unavailable; check account state before retrying"
        }
        _ => "cannot import credentials into this account; check its state in Settings",
    }
}
