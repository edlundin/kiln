//! Resolve portable references from an already authorized host-local snapshot.

use std::{
    collections::BTreeMap,
    ffi::OsString,
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    time::Duration,
};

use kiln_core::{
    KilnInstanceId, McpDefinitionRecord, McpGenerationId, McpInstanceKey, McpSecretBinding,
    McpSecretPurpose, McpSecretStore, SecretRef, SecretStoreError, SharedConfigurationKey,
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

/// Authorized host snapshot containing references only. Durable reservation and
/// administration of these references belongs to the caller, not the resolver.
/// Secrets never become part of portable server definitions or process Events.
pub struct StdioHostBindingReferences {
    pub instance_id: KilnInstanceId,
    pub key: McpInstanceKey,
    pub definition_version: u64,
    pub revision: NonZeroU64,
    pub runtime_binding: SharedConfigurationKey,
    pub executable: PathBuf,
    pub arguments: BTreeMap<SharedConfigurationKey, SecretRef>,
    pub environment: BTreeMap<SharedConfigurationKey, SecretRef>,
}

/// Resolve a durable snapshot and carry its exact identity into the atomic
/// generation claim. A revision change during vault reads makes startup fail
/// before spawn; a successful claim fences publication until process cleanup.
pub async fn resolve_persisted_stdio_launch<S: McpSecretStore>(
    definition: &McpDefinitionRecord,
    record: kiln_core::McpHostBindingRecord,
    authorized_directory: &kiln_core::WorkspaceCheckout,
    resources: StdioLaunchResources,
    vault: &S,
) -> Result<ResolvedStdioLaunch, StdioBindingError> {
    if record.retired {
        return Err(StdioBindingError::Disabled);
    }
    let bindings = record.bindings;
    // The caller must revalidate this checkout against the effective approval
    // and registered root, then supply the descriptor pinned from that checkout.
    // A legacy snapshot without cwd metadata is administration-only.
    if bindings.working_directory() != Some(authorized_directory) {
        return Err(StdioBindingError::InvalidValue);
    }
    let version = kiln_core::McpHostBindingVersion {
        instance_id: bindings.instance_id().clone(),
        revision: record.revision,
    };
    let mut resolved = resolve_stdio_launch_from_vault(
        definition,
        StdioHostBindingReferences {
            instance_id: bindings.instance_id().clone(),
            key: bindings.key().clone(),
            definition_version: bindings.definition_version(),
            revision: record.revision,
            runtime_binding: bindings.runtime_binding().clone(),
            executable: bindings.executable().into(),
            arguments: bindings.arguments().clone(),
            environment: bindings.environment().clone(),
        },
        resources,
        vault,
    )
    .await?;
    resolved.launch.host_binding_version = Some(version);
    Ok(resolved)
}

/// Resolve only references used by this exact definition. There is no fallback
/// to provider/configuration credentials, ambient environment or another scope.
/// The OS read still uses SecretValue's per-value ceiling; the caller budget
/// bounds retained values and the final encoded launch, not transient OS memory.
pub async fn resolve_stdio_launch_from_vault<S: McpSecretStore>(
    definition: &McpDefinitionRecord,
    refs: StdioHostBindingReferences,
    resources: StdioLaunchResources,
    vault: &S,
) -> Result<ResolvedStdioLaunch, StdioBindingError> {
    use StdioBindingError as Error;
    use std::os::unix::ffi::OsStringExt;
    validate_identity(
        definition,
        &refs.key,
        refs.definition_version,
        &refs.runtime_binding,
    )?;
    if !refs.executable.is_absolute() {
        return Err(Error::InvalidValue);
    }
    let mut remaining = resources.max_resolved_bytes.get();
    consume(&mut remaining, refs.executable.as_os_str(), 1)?;
    let SharedMcpTransport::Stdio {
        arguments,
        environment,
        ..
    } = &definition.definition.server().transport
    else {
        return Err(Error::UnsupportedTransport);
    };
    // Missing references are rejected before any vault access, including when
    // another field happens to have a matching name in the wrong role.
    for argument in arguments {
        if let SharedMcpArgument::HostBinding(name) = argument {
            if !refs.arguments.contains_key(name) {
                return Err(Error::MissingArgument);
            }
        }
    }
    if environment
        .values()
        .any(|name| !refs.environment.contains_key(name))
    {
        return Err(Error::MissingEnvironment);
    }
    let mut resolved_arguments = BTreeMap::new();
    let mut resolved_environment = BTreeMap::new();
    let names = arguments
        .iter()
        .filter_map(|arg| match arg {
            SharedMcpArgument::HostBinding(name) => Some((McpSecretPurpose::Argument, name)),
            _ => None,
        })
        .chain(
            environment
                .values()
                .map(|name| (McpSecretPurpose::Environment, name)),
        );
    for (purpose, name) in names {
        let (references, resolved) = match purpose {
            McpSecretPurpose::Argument => (&refs.arguments, &mut resolved_arguments),
            McpSecretPurpose::Environment => (&refs.environment, &mut resolved_environment),
            McpSecretPurpose::HttpCredential => return Err(Error::UnsupportedTransport),
        };
        if resolved.contains_key(name) {
            continue;
        }
        let binding = McpSecretBinding::new(
            refs.instance_id.clone(),
            refs.key.clone(),
            name.clone(),
            purpose,
            references.get(name).expect("preflighted reference").clone(),
        );
        let value = vault.get(&binding).await.map_err(Error::Secret)?;
        remaining = remaining
            .checked_sub(value.as_bytes().len())
            .ok_or(Error::LimitExceeded)?;
        resolved.insert(name.clone(), OsString::from_vec(value.as_bytes().to_vec()));
    }
    resolve_stdio_launch(
        definition,
        StdioHostBindings {
            key: refs.key,
            definition_version: refs.definition_version,
            revision: refs.revision,
            runtime_binding: refs.runtime_binding,
            executable: refs.executable,
            arguments: resolved_arguments,
            environment: resolved_environment,
        },
        resources,
    )
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
    Secret(SecretStoreError),
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
    validate_identity(
        definition,
        &bindings.key,
        bindings.definition_version,
        &bindings.runtime_binding,
    )?;
    let SharedMcpTransport::Stdio {
        arguments,
        environment,
        ..
    } = &definition.definition.server().transport
    else {
        return Err(Error::UnsupportedTransport);
    };
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
            host_binding_version: None,
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

fn validate_identity(
    definition: &McpDefinitionRecord,
    key: &McpInstanceKey,
    version: u64,
    runtime: &SharedConfigurationKey,
) -> Result<(), StdioBindingError> {
    use StdioBindingError as Error;
    let server = &definition.definition;
    if definition.version != version {
        return Err(Error::DefinitionChanged);
    }
    if !server.server().enabled {
        return Err(Error::Disabled);
    }
    if key.definition_id() != server.id()
        || key.owner().scope() != server.scope()
        || key.auth_profile() != server.auth_profile()
    {
        return Err(Error::ScopeMismatch);
    }
    let SharedMcpTransport::Stdio {
        runtime_binding, ..
    } = &server.server().transport
    else {
        return Err(Error::UnsupportedTransport);
    };
    if runtime_binding != runtime {
        return Err(Error::RuntimeMismatch);
    }
    Ok(())
}
