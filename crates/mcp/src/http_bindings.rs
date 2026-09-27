//! Resolve an independently authorized HTTP snapshot without starting network I/O.

use std::{collections::HashMap, num::NonZeroUsize, time::Duration};

use kiln_core::{
    KilnInstanceId, McpDefinitionLimits, McpDefinitionRecord, McpGenerationId,
    McpHostBindingRecord, McpHostBindingVersion, McpHostTransportBindings, McpInstanceKey,
    McpSecretBinding, McpSecretPurpose, McpSecretStore, SecretStoreError, SharedMcpTransport,
    WorkspaceCheckout,
};
use tokio::time::Instant;

use crate::{McpHttpGenerationConfig, McpHttpLimits, ProtocolPolicy, ProtocolVersion};

/// The caller rechecks approval, local instance identity, scoped owner and any
/// directory against the store before resolution. This value grants no authority.
pub struct HttpLaunchAuthorization<'a> {
    pub instance_id: &'a KilnInstanceId,
    pub key: &'a McpInstanceKey,
    pub directory: Option<&'a WorkspaceCheckout>,
}

/// Explicit host resources; none are selected by portable server configuration.
pub struct HttpLaunchResources {
    pub generation: McpGenerationId,
    pub definition_limits: McpDefinitionLimits,
    /// Sum of retained endpoint and bearer-token bytes, not transient vault or
    /// SDK copies. SecretValue additionally enforces its per-value OS envelope.
    pub max_resolved_bytes: NonZeroUsize,
    pub io: McpHttpLimits,
    pub channel_capacity: NonZeroUsize,
    pub max_exchanges: NonZeroUsize,
    pub max_catalog_lifetime_bytes: NonZeroUsize,
    pub legacy_resume_delay: Option<Duration>,
    /// Includes vault resolution and subsequent protocol startup.
    pub startup_deadline: Instant,
}

/// Inputs for a durable runtime owner, never permission to start network I/O.
/// The owner must claim this exact host revision before constructing a worker.
/// No Debug/serialization: the config can contain a bearer credential.
pub struct ResolvedHttpLaunch {
    pub key: McpInstanceKey,
    pub definition_version: u64,
    pub host_binding_version: McpHostBindingVersion,
    pub generation: McpGenerationId,
    pub definition_limits: McpDefinitionLimits,
    pub policy: ProtocolPolicy,
    pub config: McpHttpGenerationConfig,
    pub startup_deadline: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpBindingError {
    DefinitionChanged,
    Disabled,
    ScopeMismatch,
    UnsupportedTransport,
    InvalidBinding,
    InvalidCredential,
    InvalidLimits,
    LimitExceeded,
    Deadline,
    Secret(SecretStoreError),
}

/// Resolve only the published HTTP credential's exact instance/scope/name/role.
/// No ambient headers, provider credentials, OAuth refresh or fallback identity
/// is consulted. Replacing the snapshot during this read must make the caller's
/// later durable generation claim fail; resolution itself acquires no lease.
pub async fn resolve_persisted_http_launch<S: McpSecretStore>(
    definition: &McpDefinitionRecord,
    record: McpHostBindingRecord,
    authorization: HttpLaunchAuthorization<'_>,
    resources: HttpLaunchResources,
    vault: &S,
) -> Result<ResolvedHttpLaunch, HttpBindingError> {
    use HttpBindingError as Error;
    resources
        .definition_limits
        .validate()
        .map_err(|_| Error::InvalidLimits)?;
    if resources.io.request_timeout.is_zero() {
        return Err(Error::InvalidLimits);
    }
    if Instant::now() >= resources.startup_deadline {
        return Err(Error::Deadline);
    }
    if record.retired || !definition.definition.server().enabled {
        return Err(Error::Disabled);
    }
    let bindings = record.bindings;
    if definition.version != bindings.definition_version() {
        return Err(Error::DefinitionChanged);
    }
    if bindings.instance_id() != authorization.instance_id
        || bindings.key().canonical_json() != authorization.key.canonical_json()
        || bindings.working_directory() != authorization.directory
        || McpInstanceKey::new(
            &definition.definition,
            authorization.key.owner().clone(),
            resources.definition_limits.max_metadata_bytes,
        )
        .map_err(|_| Error::ScopeMismatch)?
        .canonical_json()
            != authorization.key.canonical_json()
    {
        return Err(Error::ScopeMismatch);
    }
    // Reapply the caller's current metadata limits before any vault read.
    kiln_core::McpHostBindings::from_metadata_json(
        bindings.key().clone(),
        bindings.metadata_json().as_bytes(),
        resources.definition_limits,
    )
    .map_err(|error| match error {
        kiln_core::McpHostBindingError::LimitExceeded => Error::LimitExceeded,
        _ => Error::InvalidBinding,
    })?;
    let McpHostTransportBindings::Http {
        endpoint,
        endpoint_binding,
        credential,
    } = bindings.transport()
    else {
        return Err(Error::UnsupportedTransport);
    };
    let valid = match &definition.definition.server().transport {
        SharedMcpTransport::Https {
            endpoint: shared_endpoint,
            credential_binding,
        } => {
            endpoint == shared_endpoint
                && endpoint_binding.is_none()
                && credential_binding.as_ref() == credential.as_ref().map(|(name, _)| name)
        }
        SharedMcpTransport::HostEndpoint {
            endpoint_binding: shared_binding,
        } => endpoint_binding.as_ref() == Some(shared_binding) && credential.is_none(),
        SharedMcpTransport::Stdio { .. } => return Err(Error::UnsupportedTransport),
    };
    if !valid {
        return Err(Error::InvalidBinding);
    }
    let policy = definition.definition.protocol();
    let protocol = match policy {
        ProtocolPolicy::Auto => ProtocolVersion::V20260728,
        ProtocolPolicy::Pinned(ProtocolVersion::V20241105) => {
            return Err(Error::UnsupportedTransport);
        }
        ProtocolPolicy::Pinned(version) => version,
    };
    if matches!(policy, ProtocolPolicy::Pinned(ProtocolVersion::V20260728))
        && resources.legacy_resume_delay.is_some()
    {
        return Err(Error::InvalidLimits);
    }
    let remaining = resources
        .max_resolved_bytes
        .get()
        .checked_sub(endpoint.len())
        .ok_or(Error::LimitExceeded)?;
    let bearer_token = match credential {
        None => None,
        Some((name, reference)) => {
            let identity = McpSecretBinding::new(
                bindings.instance_id().clone(),
                bindings.key().clone(),
                name.clone(),
                McpSecretPurpose::HttpCredential,
                reference.clone(),
            );
            let value = tokio::time::timeout_at(resources.startup_deadline, vault.get(&identity))
                .await
                .map_err(|_| Error::Deadline)?
                .map_err(Error::Secret)?;
            if value.as_bytes().len() > remaining {
                return Err(Error::LimitExceeded);
            }
            let token =
                std::str::from_utf8(value.as_bytes()).map_err(|_| Error::InvalidCredential)?;
            // RFC 6750 b64token; reject whitespace, non-ASCII and misplaced pad.
            // Do not place arbitrary vault bytes into an Authorization header.
            let unpadded = token.trim_end_matches('=');
            if unpadded.is_empty()
                || !unpadded
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-._~+/".contains(&b))
            {
                return Err(Error::InvalidCredential);
            }
            if token.len()
                > resources
                    .io
                    .max_header_bytes
                    .get()
                    .saturating_sub("authorizationBearer ".len())
            {
                return Err(Error::LimitExceeded);
            }
            Some(token.to_owned())
        }
    };
    if Instant::now() >= resources.startup_deadline {
        return Err(Error::Deadline);
    }
    Ok(ResolvedHttpLaunch {
        key: bindings.key().clone(),
        definition_version: definition.version,
        host_binding_version: McpHostBindingVersion {
            instance_id: bindings.instance_id().clone(),
            revision: record.revision,
        },
        generation: resources.generation,
        definition_limits: resources.definition_limits,
        policy,
        config: McpHttpGenerationConfig {
            endpoint: endpoint.clone(),
            protocol,
            io: resources.io,
            bearer_token,
            headers: HashMap::new(),
            channel_capacity: resources.channel_capacity,
            max_exchanges: resources.max_exchanges,
            max_catalog_lifetime_bytes: resources.max_catalog_lifetime_bytes,
            legacy_resume_delay: resources.legacy_resume_delay,
        },
        startup_deadline: resources.startup_deadline,
    })
}
