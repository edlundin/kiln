use std::{
    env,
    io::Write,
    net::SocketAddr,
    process::{ExitCode, Stdio},
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use kiln_core::{
    ModelId, ModelInvocationCompletionKind, ModelInvocationOutcome, ProviderAccountApplication,
    ProviderAccountId, ProviderType, RunApplication, SessionApplication, StoreMetadata,
    UsageApplication, UsageCompleteness, WorkspaceApplication,
};
use kiln_infrastructure::{
    DETERMINISTIC_BLOCKING_TREE_ARGUMENT, DETERMINISTIC_FAILURE_ARGUMENT,
    DETERMINISTIC_LARGE_OUTPUT_ARGUMENT, DETERMINISTIC_SUBPROCESS_ARGUMENT,
    DETERMINISTIC_SUCCESS_ARGUMENT, DaemonStoreLock, DeterministicOutcome,
    DeterministicSubprocessExecutor, FileArtifactStore, GitWorkspaceRootDiscovery,
    KILN_DETERMINISTIC_PID_FILE, LocalAuthCredential, SqliteStore, UlidIdGenerator,
};
use kiln_protocol::PROTOCOL_VERSION;
use kiln_providers::{
    DETERMINISTIC_MODEL_ID, DETERMINISTIC_PROVIDER_ACCOUNT_ID, DETERMINISTIC_PROVIDER_TYPE,
    DeterministicModelProvider, DeterministicModelResponse, ProviderRegistry,
};
use kiln_server::{AppState, AuthToken, EventBroadcaster, serve_with_shutdown};

use crate::run_service::RunService;

mod account_import;
mod native_model;
mod provider_login;
mod run_service;

const DETERMINISTIC_BLOCKING_CHILD_ARGUMENT: &str = "blocking-child";

#[tokio::main]
async fn main() -> ExitCode {
    if let Some(status) = deterministic_subprocess_fixture() {
        return status;
    }
    if env::args_os()
        .nth(1)
        .is_some_and(|arg| arg == "import-openai-api-key")
    {
        return account_import::run().await;
    }

    let (native_selection, public_api_config) = match env::var("KILN_RUN_EXECUTOR") {
        Ok(value) if value == "deterministic-model" => {
            match native_model::NativeModelSelection::deterministic() {
                Ok(selection) => (Some(selection), None),
                Err(error) => {
                    eprintln!("kilnd: {error}");
                    return ExitCode::FAILURE;
                }
            }
        }
        Ok(value) if value == "openai-api" => {
            match native_model::OpenAiApiConfig::from_environment() {
                Ok(config) => (Some(config.selection.clone()), Some(config)),
                Err(error) => {
                    eprintln!("kilnd: {error}");
                    return ExitCode::FAILURE;
                }
            }
        }
        Ok(value) if value == "deterministic-subprocess" => (None, None),
        Err(env::VarError::NotPresent) => (None, None),
        _ => {
            eprintln!(
                "kilnd: KILN_RUN_EXECUTOR must be deterministic-subprocess, deterministic-model or openai-api"
            );
            return ExitCode::FAILURE;
        }
    };
    let native_file_read = match run_service::configured_file_read() {
        Ok(tool) => tool,
        Err(error) => {
            eprintln!("kilnd: {error}");
            return ExitCode::FAILURE;
        }
    };
    let subprocess_outcome = match configured_subprocess_outcome() {
        Ok(outcome) => outcome,
        Err(error) => {
            eprintln!("kilnd: {error}");
            return ExitCode::FAILURE;
        }
    };
    let address = match listen_address() {
        Ok(address) => address,
        Err(error) => {
            eprintln!("kilnd: {error}");
            return ExitCode::FAILURE;
        }
    };

    let listener = match tokio::net::TcpListener::bind(address).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("kilnd: cannot bind loopback listener: {error}");
            return ExitCode::FAILURE;
        }
    };
    let bound_address = match listener.local_addr() {
        Ok(address) => address,
        Err(error) => {
            eprintln!("kilnd: cannot read listener address: {error}");
            return ExitCode::FAILURE;
        }
    };

    let _store_lock = match DaemonStoreLock::open_default() {
        Ok(lock) => lock,
        Err(kiln_infrastructure::InfrastructureError::Filesystem(error))
            if error.kind() == std::io::ErrorKind::WouldBlock =>
        {
            eprintln!("kilnd: another daemon owns this data directory");
            return ExitCode::FAILURE;
        }
        Err(_) => {
            eprintln!("kilnd: cannot acquire daemon store lock");
            return ExitCode::FAILURE;
        }
    };

    let credential = match LocalAuthCredential::open_default() {
        Ok(credential) => credential,
        Err(_) => {
            eprintln!("kilnd: cannot open local authentication credential");
            return ExitCode::FAILURE;
        }
    };

    let store = match SqliteStore::open_default().await {
        Ok(store) => store,
        Err(_) => {
            eprintln!("kilnd: cannot open workspace store");
            return ExitCode::FAILURE;
        }
    };
    let artifacts = match FileArtifactStore::open_default() {
        Ok(artifacts) => artifacts,
        Err(_) => {
            eprintln!("kilnd: cannot open artifact store");
            return ExitCode::FAILURE;
        }
    };
    let workspaces =
        WorkspaceApplication::new(GitWorkspaceRootDiscovery, store.clone(), UlidIdGenerator);
    let sessions = SessionApplication::new(store.clone(), store.clone(), UlidIdGenerator);
    let usage = UsageApplication::new(store.clone(), UlidIdGenerator);
    let provider_accounts = Arc::new(ProviderAccountApplication::new(
        store.clone(),
        UlidIdGenerator,
    ));
    let vault = kiln_infrastructure::OsSecretStore::open_default();
    let provider_registry = if let Some(config) = public_api_config {
        match provider_accounts
            .get_provider_account(config.selection.account_id.clone())
            .await
        {
            Ok(account) if account.provider_type() == config.selection.settings.provider() => {}
            _ => {
                eprintln!(
                    "kilnd: configured public API account is missing or belongs to another provider"
                );
                return ExitCode::FAILURE;
            }
        };
        // Cloning OsSecretStore shares its entry locks. The application Arc also
        // shares the lifecycle lock with all account-management operations.
        let provider = match kiln_providers::OpenAiApiModelProvider::new(
            Arc::clone(&provider_accounts),
            Arc::new(vault.clone()),
            kiln_infrastructure::StoredProviderContextReader::new(store.clone(), artifacts.clone()),
            config.context,
            config.transport,
        ) {
            Ok(provider) => provider,
            Err(_) => {
                eprintln!("kilnd: invalid public API transport configuration");
                return ExitCode::FAILURE;
            }
        };
        let mut registry = ProviderRegistry::new();
        if registry
            .register(
                config.selection.settings.provider().clone(),
                config.selection.settings.model().clone(),
                config.selection.account_id.clone(),
                provider,
            )
            .is_err()
        {
            eprintln!("kilnd: cannot register public API provider");
            return ExitCode::FAILURE;
        }
        registry
    } else if native_selection.is_some() {
        match deterministic_provider_registry() {
            Ok(registry) => registry,
            Err(error) => {
                eprintln!("kilnd: cannot configure deterministic provider: {error}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        ProviderRegistry::new()
    };
    let provider_logins = match provider_login::ProviderAccountLoginCoordinator::new(
        Arc::clone(&provider_accounts),
        vault,
    ) {
        Ok(logins) => Arc::new(logins),
        Err(_) => {
            eprintln!("kilnd: cannot configure provider account login");
            return ExitCode::FAILURE;
        }
    };
    let events = EventBroadcaster::default();
    let runs = RunService::new(
        RunApplication::new(store.clone(), UlidIdGenerator),
        DeterministicSubprocessExecutor::new(subprocess_outcome),
        events.clone(),
        store,
        artifacts,
    )
    .with_native_model(native_selection)
    .with_native_file_read(native_file_read)
    .with_provider_registry(provider_registry);

    if let Err(error) = runs.reconcile_deterministic_model_runs().await {
        eprintln!("kilnd: cannot reconcile deterministic native Runs: {error:?}");
        return ExitCode::FAILURE;
    }

    // Install handlers synchronously before readiness. A supervisor may send
    // SIGTERM immediately after reading that line, before a spawned task runs.
    let shutdown_signal = match configured_shutdown_signal() {
        Ok(signal) => signal,
        Err(error) => {
            eprintln!("kilnd: cannot configure shutdown signals: {error}");
            return ExitCode::FAILURE;
        }
    };
    let readiness = serde_json::json!({
        "event": "ready",
        "address": bound_address.to_string(),
        "protocol_version": PROTOCOL_VERSION,
        "credential_path": credential.path().display().to_string(),
    });
    let readiness_result = {
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout, "{readiness}").and_then(|()| stdout.flush())
    };
    if let Err(error) = readiness_result {
        eprintln!("kilnd: cannot report readiness: {error}");
        return ExitCode::FAILURE;
    }

    let provider_account_operations: Arc<dyn kiln_server::ProviderAccountOperations> =
        provider_logins.clone();
    let state = AppState::with_provider_account_operations(
        StoreMetadata::default(),
        bound_address,
        workspaces,
        sessions,
        runs.clone(),
        usage,
        provider_account_operations,
        events,
        AuthToken::from_bytes(credential.token()),
    );
    let lifecycle = state.lifecycle();
    let signal_lifecycle = lifecycle.clone();
    tokio::spawn(async move {
        shutdown_signal.await;
        signal_lifecycle.request_shutdown();
    });
    let graceful_shutdown = async move {
        lifecycle.wait_for_shutdown_request().await;
        lifecycle.wait_for_commands().await;
        if provider_logins.shutdown().await.is_err() {
            eprintln!("kilnd: provider account login cleanup failed");
            std::future::pending::<()>().await;
        }
        if let Err(error) = runs.shutdown().await {
            eprintln!("kilnd: graceful shutdown could not persist all terminal Runs: {error:?}");
            std::future::pending::<()>().await;
        }
    };

    match serve_with_shutdown(listener, state, graceful_shutdown).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("kilnd: server stopped: {error}");
            ExitCode::FAILURE
        }
    }
}

fn deterministic_provider_registry() -> Result<ProviderRegistry, &'static str> {
    let observed_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .ok_or("system clock is before the Unix epoch")?;
    let provider_type =
        ProviderType::parse(DETERMINISTIC_PROVIDER_TYPE).map_err(|_| "invalid provider type")?;
    let model_id = ModelId::parse(DETERMINISTIC_MODEL_ID).map_err(|_| "invalid model ID")?;
    let account_id = ProviderAccountId::parse(DETERMINISTIC_PROVIDER_ACCOUNT_ID)
        .map_err(|_| "invalid provider account ID")?;
    let provider = DeterministicModelProvider::new(DeterministicModelResponse {
        text: vec!["Kiln deterministic native assistant response.".to_owned()],
        quantities: Vec::new(),
        completeness: UsageCompleteness::Unknown,
        outcome: ModelInvocationOutcome::completed(ModelInvocationCompletionKind::AssistantOutput),
        observed_at_unix_ms,
    });
    let mut registry = ProviderRegistry::new();
    registry
        .register(provider_type, model_id, account_id, provider)
        .map_err(|_| "duplicate provider registration")?;
    Ok(registry)
}

fn deterministic_subprocess_fixture() -> Option<ExitCode> {
    let mut arguments = env::args().skip(1);
    if arguments.next().as_deref() != Some(DETERMINISTIC_SUBPROCESS_ARGUMENT) {
        return None;
    }

    let outcome = arguments.next();
    if arguments.next().is_some() {
        eprintln!("kilnd: invalid deterministic subprocess arguments");
        return Some(ExitCode::FAILURE);
    }

    if outcome.as_deref() == Some(DETERMINISTIC_LARGE_OUTPUT_ARGUMENT) {
        if let Some(pid_file) = env::var_os(KILN_DETERMINISTIC_PID_FILE) {
            let pids = serde_json::json!({"parent_pid": std::process::id()});
            if let Err(error) = std::fs::write(&pid_file, pids.to_string()) {
                eprintln!("kilnd: cannot write deterministic PID file: {error}");
                return Some(ExitCode::FAILURE);
            }
        }
        for _ in 0..10_000 {
            println!("kiln large output");
        }
        return Some(ExitCode::SUCCESS);
    }

    println!("kiln deterministic subprocess stdout");
    match outcome.as_deref() {
        Some(DETERMINISTIC_SUCCESS_ARGUMENT) => {
            eprintln!("kiln deterministic subprocess stderr");
            Some(ExitCode::SUCCESS)
        }
        Some(DETERMINISTIC_FAILURE_ARGUMENT) => {
            eprintln!("kiln deterministic subprocess failure");
            Some(ExitCode::FAILURE)
        }
        Some(DETERMINISTIC_BLOCKING_TREE_ARGUMENT) => Some(blocking_tree_fixture()),
        Some(DETERMINISTIC_BLOCKING_CHILD_ARGUMENT) => loop {
            std::thread::park();
        },
        _ => {
            eprintln!("kilnd: invalid deterministic subprocess outcome");
            Some(ExitCode::FAILURE)
        }
    }
}

fn configured_subprocess_outcome() -> Result<DeterministicOutcome, String> {
    match env::var("KILN_DETERMINISTIC_SUBPROCESS_OUTCOME") {
        Ok(value) if value == DETERMINISTIC_SUCCESS_ARGUMENT => Ok(DeterministicOutcome::Success),
        Ok(value) if value == DETERMINISTIC_FAILURE_ARGUMENT => Ok(DeterministicOutcome::Failure),
        Ok(value) if value == DETERMINISTIC_BLOCKING_TREE_ARGUMENT => {
            Ok(DeterministicOutcome::BlockingTree)
        }
        Ok(value) if value == DETERMINISTIC_LARGE_OUTPUT_ARGUMENT => {
            Ok(DeterministicOutcome::LargeOutput)
        }
        Ok(_) => {
            Err(
                "KILN_DETERMINISTIC_SUBPROCESS_OUTCOME must be `success`, `failure`, `blocking-tree`, or `large-output`"
                    .to_owned(),
            )
        }
        Err(env::VarError::NotPresent) => Ok(DeterministicOutcome::Success),
        Err(env::VarError::NotUnicode(_)) => {
            Err("KILN_DETERMINISTIC_SUBPROCESS_OUTCOME must be UTF-8".to_owned())
        }
    }
}

fn blocking_tree_fixture() -> ExitCode {
    let executable = match env::current_exe() {
        Ok(executable) => executable,
        Err(error) => {
            eprintln!("kilnd: cannot locate deterministic descendant fixture: {error}");
            return ExitCode::FAILURE;
        }
    };
    let mut child = match std::process::Command::new(executable)
        .arg(DETERMINISTIC_SUBPROCESS_ARGUMENT)
        .arg(DETERMINISTIC_BLOCKING_CHILD_ARGUMENT)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            eprintln!("kilnd: cannot start deterministic descendant fixture: {error}");
            return ExitCode::FAILURE;
        }
    };
    let Some(pid_file) = env::var_os(KILN_DETERMINISTIC_PID_FILE) else {
        eprintln!("kilnd: KILN_DETERMINISTIC_PID_FILE is required for blocking-tree");
        stop_fixture_child(&mut child);
        return ExitCode::FAILURE;
    };
    let pids = serde_json::json!({
        "parent_pid": std::process::id(),
        "child_pid": child.id(),
    });
    if let Err(error) = std::fs::write(&pid_file, pids.to_string()) {
        eprintln!("kilnd: cannot write deterministic PID file: {error}");
        stop_fixture_child(&mut child);
        return ExitCode::FAILURE;
    }
    loop {
        std::thread::park();
    }
}

fn stop_fixture_child(child: &mut std::process::Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn configured_shutdown_signal() -> std::io::Result<impl std::future::Future<Output = ()> + Send> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    Ok(async move {
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
        }
    })
}

fn listen_address() -> Result<SocketAddr, String> {
    let value = env::var("KILN_LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:0".to_owned());
    let address = SocketAddr::from_str(&value)
        .map_err(|_| "KILN_LISTEN_ADDR must be a socket address".to_owned())?;
    if !address.ip().is_loopback() {
        return Err("KILN_LISTEN_ADDR must use a loopback address".to_owned());
    }
    Ok(address)
}
