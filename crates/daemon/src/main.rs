use std::{
    env,
    io::Write,
    net::SocketAddr,
    process::{ExitCode, Stdio},
    str::FromStr,
};

use kiln_core::{RunApplication, SessionApplication, StoreMetadata, WorkspaceApplication};
use kiln_infrastructure::{
    DETERMINISTIC_BLOCKING_TREE_ARGUMENT, DETERMINISTIC_FAILURE_ARGUMENT,
    DETERMINISTIC_SUBPROCESS_ARGUMENT, DETERMINISTIC_SUCCESS_ARGUMENT, DeterministicOutcome,
    DeterministicSubprocessExecutor, GitWorkspaceRootDiscovery, LocalAuthCredential, SqliteStore,
    UlidIdGenerator,
};
use kiln_protocol::PROTOCOL_VERSION;
use kiln_server::{AppState, AuthToken, EventBroadcaster, serve_with_shutdown};

use crate::run_service::RunService;

mod run_service;

const DETERMINISTIC_BLOCKING_CHILD_ARGUMENT: &str = "blocking-child";

#[tokio::main]
async fn main() -> ExitCode {
    if let Some(status) = deterministic_subprocess_fixture() {
        return status;
    }

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
    let workspaces =
        WorkspaceApplication::new(GitWorkspaceRootDiscovery, store.clone(), UlidIdGenerator);
    let sessions = SessionApplication::new(store.clone(), store.clone(), UlidIdGenerator);
    let events = EventBroadcaster::default();
    let runs = RunService::new(
        RunApplication::new(store.clone(), UlidIdGenerator),
        DeterministicSubprocessExecutor::new(subprocess_outcome),
        events.clone(),
        store,
    );

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

    let state = AppState::with_operations(
        StoreMetadata::default(),
        bound_address,
        workspaces,
        sessions,
        runs.clone(),
        events,
        AuthToken::from_bytes(credential.token()),
    );
    let lifecycle = state.lifecycle();
    let signal_lifecycle = lifecycle.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        signal_lifecycle.request_shutdown();
    });
    let graceful_shutdown = async move {
        lifecycle.wait_for_shutdown_request().await;
        lifecycle.wait_for_commands().await;
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
        Ok(_) => {
            Err(
                "KILN_DETERMINISTIC_SUBPROCESS_OUTCOME must be `success`, `failure`, or `blocking-tree`"
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
    let Some(pid_file) = env::var_os("KILN_DETERMINISTIC_PID_FILE") else {
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

async fn wait_for_shutdown_signal() {
    let mut terminate =
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(terminate) => terminate,
            Err(error) => {
                eprintln!("kilnd: cannot listen for SIGTERM: {error}");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                eprintln!("kilnd: cannot listen for interrupt: {error}");
            }
        }
        _ = terminate.recv() => {}
    }
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
