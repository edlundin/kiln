//! Resolve portable references from an already authorized host-local snapshot.

use std::{
    collections::BTreeMap,
    ffi::OsString,
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    time::Duration,
};

use kiln_core::{
    McpDefinitionRecord, McpGenerationId, McpInstanceKey, SharedConfigurationKey,
    SharedMcpArgument, SharedMcpTransport,
};
use rustix::fd::OwnedFd;
use tokio::time::Instant;

use crate::{StdioGenerationLaunch, StdioProcessConfig};

/// Materialized values from the local host configuration/credential boundary.
/// No Debug or serialization: values can contain private paths and credentials.
/// Construction confers no authority; the caller must authorize the snapshot
/// for this exact key before resolution. Provider credentials never belong here.
pub struct StdioHostBindings {
    pub key: McpInstanceKey,
    pub definition_version: u64,
    pub revision: NonZeroU64,
    pub runtime_binding: SharedConfigurationKey,
    pub executable: PathBuf,
    pub arguments: BTreeMap<SharedConfigurationKey, OsString>,
    pub environment: BTreeMap<SharedConfigurationKey, OsString>,
}

/// Execution resources chosen by the trusted caller, never a portable server.
pub struct StdioLaunchResources {
    pub generation: McpGenerationId,
    /// Already pinned and checked against the authorized owner scope.
    pub working_directory: OwnedFd,
    pub definition_limits: kiln_core::McpDefinitionLimits,
    /// Total encoded executable/argument/environment string bytes including
    /// argv terminators and environment '=' separators, excluding pointer arrays.
    pub max_resolved_bytes: NonZeroUsize,
    pub max_frame_bytes: NonZeroUsize,
    pub shutdown_grace: Duration,
    pub startup_deadline: Instant,
}

pub struct ResolvedStdioLaunch {
    pub launch: StdioGenerationLaunch,
    pub binding_revision: NonZeroU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StdioBindingError {
    DefinitionChanged,
    Disabled,
    ScopeMismatch,
    UnsupportedTransport,
    RuntimeMismatch,
    MissingArgument,
    MissingEnvironment,
    InvalidValue,
    LimitExceeded,
}

/// This is reference substitution, not shell evaluation, installation, vault
/// access or execution permission. It neither reads ambient environment nor
/// looks up fallback paths. Unused bindings are dropped with the input snapshot.
pub fn resolve_stdio_launch(
    definition: &McpDefinitionRecord,
    bindings: StdioHostBindings,
    resources: StdioLaunchResources,
) -> Result<ResolvedStdioLaunch, StdioBindingError> {
    use StdioBindingError as Error;
    let server = &definition.definition;
    if definition.version != bindings.definition_version {
        return Err(Error::DefinitionChanged);
    }
    if !server.server().enabled {
        return Err(Error::Disabled);
    }
    if bindings.key.definition_id() != server.id()
        || bindings.key.owner().scope() != server.scope()
        || bindings.key.auth_profile() != server.auth_profile()
    {
        return Err(Error::ScopeMismatch);
    }
    let SharedMcpTransport::Stdio {
        runtime_binding,
        arguments,
        environment,
    } = &server.server().transport
    else {
        return Err(Error::UnsupportedTransport);
    };
    if runtime_binding != &bindings.runtime_binding {
        return Err(Error::RuntimeMismatch);
    }
    if !bindings.executable.is_absolute() {
        return Err(Error::InvalidValue);
    }
    let mut remaining = resources.max_resolved_bytes.get();
    consume(&mut remaining, bindings.executable.as_os_str(), 1)?;
    let mut resolved_arguments = Vec::with_capacity(arguments.len());
    for argument in arguments {
        let value = match argument {
            SharedMcpArgument::Literal(value) => std::ffi::OsStr::new(value),
            SharedMcpArgument::HostBinding(key) => bindings
                .arguments
                .get(key)
                .ok_or(Error::MissingArgument)?
                .as_os_str(),
        };
        consume(&mut remaining, value, 1)?;
        resolved_arguments.push(value.to_owned());
    }
    let mut resolved_environment = BTreeMap::new();
    for (name, key) in environment {
        let value = bindings
            .environment
            .get(key)
            .ok_or(Error::MissingEnvironment)?;
        consume(&mut remaining, std::ffi::OsStr::new(name), 1)?;
        consume(&mut remaining, value, 1)?;
        resolved_environment.insert(OsString::from(name), value.clone());
    }
    Ok(ResolvedStdioLaunch {
        binding_revision: bindings.revision,
        launch: StdioGenerationLaunch {
            key: bindings.key,
            definition_version: definition.version,
            generation: resources.generation,
            definition_limits: resources.definition_limits,
            startup_deadline: resources.startup_deadline,
            process: StdioProcessConfig {
                executable: bindings.executable,
                arguments: resolved_arguments,
                working_directory: resources.working_directory,
                environment: resolved_environment,
                max_frame_bytes: resources.max_frame_bytes,
                shutdown_grace: resources.shutdown_grace,
            },
        },
    })
}

fn consume(
    remaining: &mut usize,
    value: &std::ffi::OsStr,
    separators: usize,
) -> Result<(), StdioBindingError> {
    let bytes = value.as_encoded_bytes();
    if bytes.contains(&0) {
        return Err(StdioBindingError::InvalidValue);
    }
    *remaining = remaining
        .checked_sub(bytes.len())
        .and_then(|left| left.checked_sub(separators))
        .ok_or(StdioBindingError::LimitExceeded)?;
    Ok(())
}
