use std::{env, io::Write, net::SocketAddr, process::ExitCode, str::FromStr};

use kiln_core::{RunApplication, SessionApplication, StoreMetadata, WorkspaceApplication};
use kiln_infrastructure::{
    DETERMINISTIC_FAILURE_ARGUMENT, DETERMINISTIC_SUBPROCESS_ARGUMENT,
    DETERMINISTIC_SUCCESS_ARGUMENT, DeterministicOutcome, DeterministicSubprocessExecutor,
    GitWorkspaceRootDiscovery, SqliteStore, UlidIdGenerator,
};
use kiln_protocol::PROTOCOL_VERSION;
use kiln_server::{AppState, EventBroadcaster, serve};

use crate::run_service::RunService;

mod run_service;

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
        RunApplication::new(store, UlidIdGenerator),
        DeterministicSubprocessExecutor::new(subprocess_outcome),
        events.clone(),
    );

    let readiness = serde_json::json!({
        "event": "ready",
        "address": bound_address.to_string(),
        "protocol_version": PROTOCOL_VERSION,
    });
    let readiness_result = {
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout, "{readiness}").and_then(|()| stdout.flush())
    };
    if let Err(error) = readiness_result {
        eprintln!("kilnd: cannot report readiness: {error}");
        return ExitCode::FAILURE;
    }

    match serve(
        listener,
        AppState::with_operations(
            StoreMetadata::default(),
            bound_address,
            workspaces,
            sessions,
            runs,
            events,
        ),
    )
    .await
    {
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
        Ok(_) => {
            Err("KILN_DETERMINISTIC_SUBPROCESS_OUTCOME must be `success` or `failure`".to_owned())
        }
        Err(env::VarError::NotPresent) => Ok(DeterministicOutcome::Success),
        Err(env::VarError::NotUnicode(_)) => {
            Err("KILN_DETERMINISTIC_SUBPROCESS_OUTCOME must be UTF-8".to_owned())
        }
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
