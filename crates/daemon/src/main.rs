use std::{
    env,
    io::Write,
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    process::{ExitCode, Stdio},
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use kiln_core::{
    ConfigurationAuthority, ConfigurationFollowerServingIdentity, ConfigurationIdentityStatusStore,
    ConfigurationMasterIdentityId, ConfigurationMasterIdentityPhase,
    ConfigurationMasterIdentityStatus, ConfigurationRole, ConfigurationStateStore, ContentHash,
    KilnInstanceId, ModelId, ModelInvocationCompletionKind, ModelInvocationOutcome,
    ProviderAccountApplication, ProviderAccountId, ProviderType, RunApplication,
    SessionApplication, StoreMetadata, UsageApplication, UsageCompleteness, WorkspaceApplication,
};
use kiln_infrastructure::{
    ConfigurationIdentityProvisioner, DETERMINISTIC_BLOCKING_TREE_ARGUMENT,
    DETERMINISTIC_FAILURE_ARGUMENT, DETERMINISTIC_LARGE_OUTPUT_ARGUMENT,
    DETERMINISTIC_SUBPROCESS_ARGUMENT, DETERMINISTIC_SUCCESS_ARGUMENT, DaemonStoreLock,
    DeterministicOutcome, DeterministicSubprocessExecutor, FileArtifactStore,
    GitWorkspaceRootDiscovery, KILN_DETERMINISTIC_PID_FILE, LocalAuthCredential, SqliteStore,
    UlidIdGenerator,
};
use kiln_protocol::PROTOCOL_VERSION;
use kiln_providers::{
    DETERMINISTIC_MODEL_ID, DETERMINISTIC_PROVIDER_ACCOUNT_ID, DETERMINISTIC_PROVIDER_TYPE,
    DeterministicModelProvider, DeterministicModelResponse, ProviderRegistry,
};
use kiln_server::{
    AppState, AuthToken, ConfigurationFollowerRouter, ConfigurationFollowerTls,
    ConfigurationTlsLimits, EventBroadcaster, LifecycleCoordinator,
    configuration_follower_host_authority, configuration_follower_router,
    serve_configuration_followers, serve_with_shutdown,
};

use crate::configuration_enrollment::{
    PinnedConfigurationFollowerEnrollmentTransport, PinnedConfigurationFollowerSnapshotTransport,
};
use crate::run_service::RunService;

mod account_import;
mod configuration_enrollment;
mod native_model;
mod provider_login;
mod run_service;

const DETERMINISTIC_BLOCKING_CHILD_ARGUMENT: &str = "blocking-child";

struct ConfigurationFollowerListenerConfig {
    listen_address: SocketAddr,
    host_authority: String,
    limits: ConfigurationTlsLimits,
    max_retained_requests_per_authority: NonZeroU32,
}

struct ActiveFollowerIdentity {
    authority: ConfigurationAuthority,
    identity_id: ConfigurationMasterIdentityId,
    server_name: String,
    ca_fingerprint: ContentHash,
    not_before_unix_seconds: i64,
    leaf_not_after_unix_seconds: i64,
    ca_not_after_unix_seconds: i64,
}

struct PreparedConfigurationFollower {
    listener: tokio::net::TcpListener,
    bound_address: SocketAddr,
    canonical_host_authority: String,
    router: ConfigurationFollowerRouter,
    tls: ConfigurationFollowerTls,
    limits: ConfigurationTlsLimits,
    active_identity: ActiveFollowerIdentity,
    expires_at: tokio::time::Instant,
    changes: tokio::sync::watch::Receiver<u64>,
}

#[derive(Debug, Clone, Copy)]
enum ConfigurationFollowerStopCause {
    IdentityInvalidated,
    CertificateExpired,
    StoreUnavailable,
    WatchClosed,
}

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
    let follower_config = match configuration_follower_listener_config() {
        Ok(config) => config,
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
    // Identity creation is idempotent. Startup never designates a master or enrolls
    // a follower implicitly, including after configuration/network failures.
    if store
        .initialize_configuration_instance(KilnInstanceId::from_ulid(ulid::Ulid::generate()))
        .await
        .is_err()
    {
        eprintln!("kilnd: cannot initialize configuration instance identity");
        return ExitCode::FAILURE;
    }
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
        store.clone(),
        artifacts,
    )
    .with_native_model(native_selection)
    .with_native_file_read(native_file_read)
    .with_provider_registry(provider_registry);

    if let Err(error) = runs.reconcile_deterministic_model_runs().await {
        eprintln!("kilnd: cannot reconcile deterministic native Runs: {error:?}");
        return ExitCode::FAILURE;
    }

    let provider_account_operations: Arc<dyn kiln_server::ProviderAccountOperations> =
        provider_logins.clone();
    let configuration_vault = kiln_infrastructure::OsConfigurationSecretStore::open_default();
    let identity_provisioner =
        ConfigurationIdentityProvisioner::new(store.clone(), configuration_vault.clone());
    let identity_commands = kiln_infrastructure::ConfigurationIdentityCommands::new(
        identity_provisioner.clone(),
        unix_time_seconds,
    );
    let follower_enrollment = kiln_infrastructure::ConfigurationFollowerEnrollmentManager::new(
        store.clone(),
        configuration_vault,
    )
    .with_exchange_transport(Arc::new(PinnedConfigurationFollowerEnrollmentTransport))
    .with_snapshot_transport(Arc::new(PinnedConfigurationFollowerSnapshotTransport));
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
    )
    .with_configuration_status_store(store.clone())
    .with_configuration_identity_status_store(store.clone())
    .with_configuration_identity_administration(identity_commands)
    .with_configuration_administration_store(store.clone(), UlidIdGenerator)
    .with_configuration_access_store(store.clone())
    .with_configuration_follower_enrollment_administration(follower_enrollment)
    .with_configuration_publication_store(store.clone());
    let lifecycle = state.lifecycle();

    let prepared_follower = match follower_config {
        Some(config) => match prepare_configuration_follower(
            config,
            store.clone(),
            identity_provisioner,
            lifecycle.clone(),
        )
        .await
        {
            Ok(prepared) => Some(prepared),
            Err(error) => {
                eprintln!("kilnd: cannot start configuration follower TLS listener: {error}");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    // Install handlers synchronously before readiness. A supervisor may send
    // SIGTERM immediately after reading that line, before a spawned task runs.
    let shutdown_signal = match configured_shutdown_signal() {
        Ok(signal) => signal,
        Err(error) => {
            eprintln!("kilnd: cannot configure shutdown signals: {error}");
            return ExitCode::FAILURE;
        }
    };
    let signal_lifecycle = lifecycle.clone();
    tokio::spawn(async move {
        shutdown_signal.await;
        signal_lifecycle.request_shutdown();
    });

    let readiness = match prepared_follower.as_ref() {
        Some(follower) => serde_json::json!({
            "event": "ready",
            "address": bound_address.to_string(),
            "protocol_version": PROTOCOL_VERSION,
            "credential_path": credential.path().display().to_string(),
            "configuration_follower_address": follower.bound_address.to_string(),
            "configuration_follower_host_authority": follower.canonical_host_authority,
        }),
        None => serde_json::json!({
            "event": "ready",
            "address": bound_address.to_string(),
            "protocol_version": PROTOCOL_VERSION,
            "credential_path": credential.path().display().to_string(),
        }),
    };
    let readiness_result = {
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout, "{readiness}").and_then(|()| stdout.flush())
    };
    if let Err(error) = readiness_result {
        eprintln!("kilnd: cannot report readiness: {error}");
        return ExitCode::FAILURE;
    }

    let follower_task = prepared_follower.map(|prepared| {
        let store = store.clone();
        let lifecycle = lifecycle.clone();
        tokio::spawn(async move {
            let PreparedConfigurationFollower {
                listener,
                router,
                tls,
                limits,
                active_identity,
                expires_at,
                changes,
                ..
            } = prepared;
            let immediate_shutdown = async move {
                let cause = wait_for_configuration_follower_stop(
                    store,
                    active_identity,
                    changes,
                    expires_at,
                )
                .await;
                eprintln!("kilnd: configuration follower TLS stopped: {cause:?}");
            };
            let graceful_shutdown = async move {
                lifecycle.wait_for_shutdown_request().await;
            };
            if let Err(error) = serve_configuration_followers(
                listener,
                router,
                tls,
                limits,
                graceful_shutdown,
                immediate_shutdown,
            )
            .await
            {
                eprintln!("kilnd: configuration follower TLS server failed: {error}");
            }
        })
    });

    let shutdown_lifecycle = lifecycle.clone();
    let graceful_shutdown = async move {
        shutdown_lifecycle.wait_for_shutdown_request().await;
        shutdown_lifecycle.wait_for_commands().await;
        if provider_logins.shutdown().await.is_err() {
            eprintln!("kilnd: provider account login cleanup failed");
            std::future::pending::<()>().await;
        }
        if let Err(error) = runs.shutdown().await {
            eprintln!("kilnd: graceful shutdown could not persist all terminal Runs: {error:?}");
            std::future::pending::<()>().await;
        }
    };

    let server_result = serve_with_shutdown(listener, state, graceful_shutdown).await;
    lifecycle.request_shutdown();
    lifecycle.wait_for_commands().await;
    if let Some(follower_task) = follower_task
        && follower_task.await.is_err()
    {
        eprintln!("kilnd: configuration follower TLS supervisor task failed");
    }

    match server_result {
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

fn configuration_follower_listener_config()
-> Result<Option<ConfigurationFollowerListenerConfig>, String> {
    let listen_address = match env::var("KILN_CONFIGURATION_FOLLOWER_LISTEN_ADDR") {
        Ok(value) => SocketAddr::from_str(&value).map_err(|_| {
            "KILN_CONFIGURATION_FOLLOWER_LISTEN_ADDR must be a socket address".to_owned()
        })?,
        Err(env::VarError::NotPresent) => return Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            return Err("KILN_CONFIGURATION_FOLLOWER_LISTEN_ADDR must be UTF-8".to_owned());
        }
    };
    let host_authority: String =
        required_configuration_follower_value("KILN_CONFIGURATION_FOLLOWER_HOST_AUTHORITY")?;
    if host_authority.is_empty() {
        return Err("KILN_CONFIGURATION_FOLLOWER_HOST_AUTHORITY must not be empty".to_owned());
    }
    let max_connections = NonZeroUsize::new(required_configuration_follower_value::<usize>(
        "KILN_CONFIGURATION_FOLLOWER_MAX_CONNECTIONS",
    )?)
    .ok_or_else(|| "KILN_CONFIGURATION_FOLLOWER_MAX_CONNECTIONS must be positive".to_owned())?;
    let max_http_buffer_bytes = required_configuration_follower_value::<usize>(
        "KILN_CONFIGURATION_FOLLOWER_MAX_HTTP_BUFFER_BYTES",
    )?;
    if max_http_buffer_bytes < 8192 {
        return Err(
            "KILN_CONFIGURATION_FOLLOWER_MAX_HTTP_BUFFER_BYTES must be at least 8192".to_owned(),
        );
    }
    let handshake_timeout =
        required_follower_timeout("KILN_CONFIGURATION_FOLLOWER_HANDSHAKE_TIMEOUT_MS")?;
    let request_timeout =
        required_follower_timeout("KILN_CONFIGURATION_FOLLOWER_REQUEST_TIMEOUT_MS")?;
    let shutdown_timeout =
        required_follower_timeout("KILN_CONFIGURATION_FOLLOWER_SHUTDOWN_TIMEOUT_MS")?;
    if [handshake_timeout, request_timeout, shutdown_timeout]
        .into_iter()
        .any(|duration| tokio::time::Instant::now().checked_add(duration).is_none())
    {
        return Err("configuration follower TLS timeout is out of range".to_owned());
    }
    let max_retained_requests_per_authority =
        NonZeroU32::new(required_configuration_follower_value::<u32>(
            "KILN_CONFIGURATION_FOLLOWER_MAX_RETAINED_REQUESTS_PER_AUTHORITY",
        )?)
        .ok_or_else(|| {
            "KILN_CONFIGURATION_FOLLOWER_MAX_RETAINED_REQUESTS_PER_AUTHORITY must be positive"
                .to_owned()
        })?;

    Ok(Some(ConfigurationFollowerListenerConfig {
        listen_address,
        host_authority,
        limits: ConfigurationTlsLimits {
            max_connections,
            max_http_buffer_bytes,
            handshake_timeout,
            request_timeout,
            shutdown_timeout,
        },
        max_retained_requests_per_authority,
    }))
}

fn required_configuration_follower_value<T: FromStr>(name: &str) -> Result<T, String> {
    let value = env::var(name).map_err(|_| format!("{name} is required and must be UTF-8"))?;
    value
        .parse()
        .map_err(|_| format!("{name} has an invalid value"))
}

fn required_follower_timeout(name: &str) -> Result<Duration, String> {
    let milliseconds = required_configuration_follower_value::<u64>(name)?;
    if milliseconds == 0 {
        return Err(format!("{name} must be positive"));
    }
    Ok(Duration::from_millis(milliseconds))
}

async fn prepare_configuration_follower(
    config: ConfigurationFollowerListenerConfig,
    store: SqliteStore,
    identity_provisioner: ConfigurationIdentityProvisioner,
    lifecycle: LifecycleCoordinator,
) -> Result<PreparedConfigurationFollower, String> {
    // Subscribe first, then inspect and acquire. Watch retains the latest
    // generation, so changes racing with either read remain observable.
    let changes = store.subscribe_configuration_serving_changes();
    let initial = store
        .get_configuration_identity_status()
        .await
        .map_err(|_| "cannot read current master identity status".to_owned())?;
    let active_identity = active_follower_identity(&initial, unix_time_seconds())?;
    let canonical_host_authority = configuration_follower_host_authority(
        &config.host_authority,
        &active_identity.server_name,
    )
    .map_err(|_| {
        "KILN_CONFIGURATION_FOLLOWER_HOST_AUTHORITY must match the active certificate name and have a valid port".to_owned()
    })?;

    let identity = identity_provisioner
        .acquire_active_tls_identity(
            &initial.state,
            &active_identity.identity_id,
            unix_time_seconds,
        )
        .await
        .map_err(|_| "cannot acquire the active TLS identity from the OS vault".to_owned())?;
    if identity.authority() != &active_identity.authority
        || identity.identity_id() != &active_identity.identity_id
        || identity.server_name() != active_identity.server_name
    {
        return Err("active TLS identity changed during acquisition".to_owned());
    }

    let acquired_status = store
        .get_configuration_identity_status()
        .await
        .map_err(|_| "cannot recheck master identity status after key acquisition".to_owned())?;
    ensure_active_follower_identity(&acquired_status, &active_identity, unix_time_seconds())?;

    let serving_identity = ConfigurationFollowerServingIdentity {
        authority: identity.authority().clone(),
        identity_id: identity.identity_id().clone(),
        server_name: identity.server_name().to_owned(),
        master_ca_fingerprint: active_identity.ca_fingerprint.clone(),
    };
    let tls = ConfigurationFollowerTls::from_borrowed_der(
        identity.certificate_chain_der().to_vec(),
        identity.private_key_der(),
    )
    .map_err(|_| "the active certificate and private key cannot serve TLS".to_owned())?;
    let router = configuration_follower_router(
        store.clone(),
        |credential| {
            kiln_infrastructure::ConfigurationReadCredential::parse(credential)
                .map(|credential| credential.digest())
        },
        &canonical_host_authority,
        serving_identity,
        config.max_retained_requests_per_authority,
        lifecycle,
    )
    .map_err(|_| {
        "the configured HTTPS authority does not match the active certificate".to_owned()
    })?;
    let listener = tokio::net::TcpListener::bind(config.listen_address)
        .await
        .map_err(|_| "cannot bind the configuration follower listener".to_owned())?;
    let bound_address = listener
        .local_addr()
        .map_err(|_| "cannot read the configuration follower listener address".to_owned())?;

    // Catch a committed change during certificate construction or socket bind
    // before readiness. Later changes remain queued in `changes`.
    let bound_status = store
        .get_configuration_identity_status()
        .await
        .map_err(|_| "cannot recheck master identity status after listener bind".to_owned())?;
    ensure_active_follower_identity(&bound_status, &active_identity, unix_time_seconds())?;
    let expires_at = certificate_expiry_instant(&active_identity)?;

    Ok(PreparedConfigurationFollower {
        listener,
        bound_address,
        canonical_host_authority,
        router,
        tls,
        limits: config.limits,
        active_identity,
        expires_at,
        changes,
    })
}

fn active_follower_identity(
    status: &ConfigurationMasterIdentityStatus,
    now_unix_seconds: i64,
) -> Result<ActiveFollowerIdentity, String> {
    let authority = match status.state.role() {
        ConfigurationRole::Master(authority)
            if authority.master_id() == status.state.instance_id() =>
        {
            authority.clone()
        }
        _ => return Err("the current instance is not a configuration master".to_owned()),
    };
    let identity = status
        .identity
        .as_ref()
        .filter(|identity| identity.phase == ConfigurationMasterIdentityPhase::Active)
        .ok_or_else(|| "the current master has no active TLS identity".to_owned())?;
    if identity.not_before_unix_seconds > now_unix_seconds
        || identity.leaf_not_after_unix_seconds <= now_unix_seconds
        || identity.ca_not_after_unix_seconds <= now_unix_seconds
    {
        return Err(
            "the active TLS identity is outside its certificate validity period".to_owned(),
        );
    }
    Ok(ActiveFollowerIdentity {
        authority,
        identity_id: identity.identity_id.clone(),
        server_name: identity.server_name.clone(),
        ca_fingerprint: identity.certificate_authority_fingerprint.clone(),
        not_before_unix_seconds: identity.not_before_unix_seconds,
        leaf_not_after_unix_seconds: identity.leaf_not_after_unix_seconds,
        ca_not_after_unix_seconds: identity.ca_not_after_unix_seconds,
    })
}

fn ensure_active_follower_identity(
    status: &ConfigurationMasterIdentityStatus,
    expected: &ActiveFollowerIdentity,
    now_unix_seconds: i64,
) -> Result<(), String> {
    let current = active_follower_identity(status, now_unix_seconds)?;
    if current.authority != expected.authority
        || current.identity_id != expected.identity_id
        || current.server_name != expected.server_name
        || current.ca_fingerprint != expected.ca_fingerprint
        || current.not_before_unix_seconds != expected.not_before_unix_seconds
        || current.leaf_not_after_unix_seconds != expected.leaf_not_after_unix_seconds
        || current.ca_not_after_unix_seconds != expected.ca_not_after_unix_seconds
    {
        return Err("the active TLS identity changed during listener startup".to_owned());
    }
    Ok(())
}

fn certificate_expiry_instant(
    identity: &ActiveFollowerIdentity,
) -> Result<tokio::time::Instant, String> {
    let expires_at = identity
        .leaf_not_after_unix_seconds
        .min(identity.ca_not_after_unix_seconds);
    let expires_at = u64::try_from(expires_at)
        .ok()
        .and_then(|seconds| UNIX_EPOCH.checked_add(Duration::from_secs(seconds)))
        .ok_or_else(|| "the active certificate expiry is out of range".to_owned())?;
    let remaining = expires_at
        .duration_since(SystemTime::now())
        .map_err(|_| "the active certificate has expired".to_owned())?;
    tokio::time::Instant::now()
        .checked_add(remaining)
        .ok_or_else(|| "the active certificate expiry is out of range".to_owned())
}

async fn wait_for_configuration_follower_stop(
    store: SqliteStore,
    expected: ActiveFollowerIdentity,
    mut changes: tokio::sync::watch::Receiver<u64>,
    expires_at: tokio::time::Instant,
) -> ConfigurationFollowerStopCause {
    let expiry = tokio::time::sleep_until(expires_at);
    tokio::pin!(expiry);
    loop {
        tokio::select! {
            _ = &mut expiry => return ConfigurationFollowerStopCause::CertificateExpired,
            changed = changes.changed() => {
                if changed.is_err() {
                    return ConfigurationFollowerStopCause::WatchClosed;
                }
                let status = match store.get_configuration_identity_status().await {
                    Ok(status) => status,
                    Err(_) => return ConfigurationFollowerStopCause::StoreUnavailable,
                };
                if ensure_active_follower_identity(&status, &expected, unix_time_seconds()).is_err() {
                    return ConfigurationFollowerStopCause::IdentityInvalidated;
                }
            }
        }
    }
}

fn unix_time_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_secs()).ok())
        .unwrap_or(i64::MIN)
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
