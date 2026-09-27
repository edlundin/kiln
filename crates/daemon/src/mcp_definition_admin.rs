//! Offline local definition administration. Registration never launches a server.

use std::{
    env,
    io::{IsTerminal, Read},
    num::NonZeroUsize,
    process::ExitCode,
};

use kiln_core::{
    McpDefinitionError, McpDefinitionLimits, McpDefinitionStore, McpServerDefinition,
    SharedConfigurationKey,
};
use kiln_infrastructure::{DaemonStoreLock, SqliteStore};

const USAGE: &str = "Usage:
  kilnd register-mcp-definition --max-bytes N --expected-version N --idempotency-key KEY --stdin
  kilnd inspect-mcp-definition --max-bytes N --id ID
Stop kilnd first. Registration reads canonical local definition JSON from a pipe.
The positive byte budget is chosen by the operator; no default is supplied.
Expected version 0 creates a definition. Updates require its current version.
Reuse the same key and input to retry an unconfirmed registration.
Registration stores metadata only; it grants no authority and starts no process.";

pub(crate) async fn run() -> ExitCode {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.len() == 2 && args[1] == "--help" {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    match execute(&args).await {
        Ok(output) => {
            println!("{output}");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("kilnd: {message}");
            ExitCode::FAILURE
        }
    }
}

async fn execute(args: &[String]) -> Result<String, &'static str> {
    let register = args
        .first()
        .is_some_and(|arg| arg == "register-mcp-definition");
    let mut max_bytes = None;
    let mut expected_version = None;
    let mut key = None;
    let mut id = None;
    let mut stdin = false;
    let mut flags = args.iter().skip(1);
    while let Some(flag) = flags.next() {
        match flag.as_str() {
            "--max-bytes" if max_bytes.is_none() => {
                max_bytes = Some(
                    flags
                        .next()
                        .ok_or(USAGE)?
                        .parse::<NonZeroUsize>()
                        .map_err(|_| USAGE)?,
                );
            }
            "--expected-version" if register && expected_version.is_none() => {
                expected_version = Some(
                    flags
                        .next()
                        .ok_or(USAGE)?
                        .parse::<u64>()
                        .map_err(|_| USAGE)?,
                );
            }
            "--idempotency-key" if register && key.is_none() => {
                key = Some(flags.next().ok_or(USAGE)?)
            }
            "--id" if !register && id.is_none() => id = Some(flags.next().ok_or(USAGE)?),
            "--stdin" if register && !stdin => stdin = true,
            _ => return Err(USAGE),
        }
    }
    let max_bytes = max_bytes.ok_or(USAGE)?.get();
    // Every key, argument, endpoint and entry consumes at least one byte of the
    // metadata. This single aggregate ceiling therefore also bounds each field
    // and collection without introducing arbitrary per-field product limits.
    let limits = McpDefinitionLimits {
        max_key_bytes: max_bytes,
        max_metadata_bytes: max_bytes,
        max_arguments: max_bytes,
        max_argument_bytes: max_bytes,
        max_environment: max_bytes,
        max_endpoint_bytes: max_bytes,
    };
    limits.validate().map_err(definition_error)?;
    let definition = if register {
        if !stdin || expected_version.is_none() || key.is_none() {
            return Err(USAGE);
        }
        if std::io::stdin().is_terminal() {
            return Err("pipe canonical definition JSON to stdin");
        }
        let read_limit = max_bytes
            .checked_add(3)
            .and_then(|n| u64::try_from(n).ok())
            .ok_or(USAGE)?;
        let mut bytes = Vec::new();
        std::io::stdin()
            .lock()
            .take(read_limit)
            .read_to_end(&mut bytes)
            .map_err(|_| "cannot read definition JSON")?;
        // One optional line ending is not part of canonical metadata.
        if bytes.last() == Some(&b'\n') {
            bytes.pop();
            if bytes.last() == Some(&b'\r') {
                bytes.pop();
            }
        }
        Some(McpServerDefinition::from_metadata_json(&bytes, limits).map_err(definition_error)?)
    } else {
        if id.is_none() {
            return Err(USAGE);
        }
        None
    };
    let id = id
        .map(|id| SharedConfigurationKey::parse(id, max_bytes))
        .transpose()
        .map_err(|_| "invalid definition ID")?;
    let _lock = DaemonStoreLock::open_default().map_err(
        |_| "cannot lock the data directory; stop kilnd before administering MCP definitions",
    )?;
    let store = SqliteStore::open_default()
        .await
        .map_err(|_| "cannot open the definition store")?;
    if let Some(definition) = definition {
        let record = store
            .register_mcp_definition(
                &definition,
                expected_version.ok_or(USAGE)?,
                key.ok_or(USAGE)?,
                limits,
            )
            .await
            .map_err(definition_error)?;
        // A retry receipt can be older than current state; do not call it current.
        Ok(serde_json::json!({"definition_id": record.definition.id().as_str(), "registered_version": record.version}).to_string())
    } else {
        let record = store
            .get_mcp_definition(&id.ok_or(USAGE)?, limits)
            .await
            .map_err(definition_error)?
            .ok_or("MCP definition was not found")?;
        let metadata: serde_json::Value = serde_json::from_str(record.definition.metadata_json())
            .map_err(|_| "stored definition could not be verified")?;
        Ok(serde_json::json!({"version": record.version, "definition": metadata}).to_string())
    }
}

fn definition_error(error: McpDefinitionError) -> &'static str {
    match error {
        McpDefinitionError::InvalidRequest => {
            "invalid canonical definition, version, key or budget"
        }
        McpDefinitionError::LimitExceeded => "definition exceeds the supplied byte budget",
        McpDefinitionError::Conflict => {
            "definition version changed; inspect it before submitting an update"
        }
        McpDefinitionError::IdempotencyConflict => {
            "idempotency key was already used for a different registration"
        }
        McpDefinitionError::IntegrityViolation => "stored definition could not be verified",
        McpDefinitionError::Unavailable => {
            "definition store unavailable; retry with the same input and idempotency key"
        }
    }
}
