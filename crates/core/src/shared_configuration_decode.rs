use std::collections::BTreeMap;

use crate::{
    CapabilitySupport, GenerationSettings, ModelCapabilitySnapshot, ModelId,
    ModelInvocationSettings, ProviderType, ReasoningSettings, SHARED_CONFIGURATION_SCHEMA_VERSION,
    SharedConfigurationError as Error, SharedConfigurationInput, SharedConfigurationKey,
    SharedConfigurationLimits, SharedConfigurationSnapshot, SharedMcpArgument,
    SharedMcpServerInput, SharedMcpTransport, SharedModelDefaults, SharedSettings,
    SharedSkillPackage,
};
use serde_json::Value;

impl SharedConfigurationSnapshot {
    /// Restore canonical metadata with the complete, independently validated
    /// package payloads. Exact regeneration rejects extra/duplicate fields,
    /// missing packages and altered hashes rather than silently dropping data.
    /// This is not an enrollment, authentication or filesystem boundary.
    pub fn from_metadata_json(
        bytes: &[u8],
        skills: Vec<SharedSkillPackage>,
        limits: SharedConfigurationLimits,
    ) -> Result<Self, Error> {
        limits.validate()?;
        if bytes.len() > limits.max_metadata_bytes {
            return Err(Error::LimitExceeded);
        }
        let value: Value = serde_json::from_slice(bytes).map_err(|_| Error::InvalidMetadata)?;
        if required(&value, "schema_version")?.as_u64()
            != Some(u64::from(SHARED_CONFIGURATION_SCHEMA_VERSION))
        {
            return Err(Error::InvalidMetadata);
        }
        let settings = decode_settings(required(&value, "settings")?, limits)?;
        let servers = required(&value, "mcp_servers")?
            .as_array()
            .ok_or(Error::InvalidMetadata)?;
        if servers.len() > limits.max_mcp_servers {
            return Err(Error::LimitExceeded);
        }
        let mcp_servers = servers
            .iter()
            .map(|server| decode_mcp(server, limits))
            .collect::<Result<_, _>>()?;
        let snapshot = Self::validate(
            SharedConfigurationInput {
                settings,
                mcp_servers,
                skills,
            },
            limits,
        )?;
        if snapshot.metadata_json().as_bytes() != bytes {
            return Err(Error::InvalidMetadata);
        }
        Ok(snapshot)
    }
}

fn required<'a>(value: &'a Value, name: &str) -> Result<&'a Value, Error> {
    value.get(name).ok_or(Error::InvalidMetadata)
}
fn string(value: &Value) -> Result<&str, Error> {
    value.as_str().ok_or(Error::InvalidMetadata)
}
fn key(value: &Value, limits: SharedConfigurationLimits) -> Result<SharedConfigurationKey, Error> {
    SharedConfigurationKey::parse(string(value)?, limits.max_key_bytes)
}
fn optional_key(
    value: &Value,
    limits: SharedConfigurationLimits,
) -> Result<Option<SharedConfigurationKey>, Error> {
    if value.is_null() {
        Ok(None)
    } else {
        key(value, limits).map(Some)
    }
}
fn support(value: &Value) -> Result<CapabilitySupport, Error> {
    CapabilitySupport::parse(string(value)?).map_err(|_| Error::InvalidMetadata)
}
fn decode_settings(
    value: &Value,
    limits: SharedConfigurationLimits,
) -> Result<SharedSettings, Error> {
    let model = required(value, "model_defaults")?;
    if model.is_null() {
        return Ok(SharedSettings::default());
    }
    let output = required(model, "max_output_tokens")?;
    let max_output_tokens = if output.is_null() {
        None
    } else {
        Some(
            u32::try_from(output.as_u64().ok_or(Error::InvalidMetadata)?)
                .map_err(|_| Error::InvalidMetadata)?,
        )
    };
    let reasoning = required(model, "reasoning_effort")?;
    let effort = if reasoning.is_null() {
        None
    } else {
        Some(string(reasoning)?.to_owned())
    };
    let capabilities = required(model, "capabilities")?;
    Ok(SharedSettings {
        model_defaults: Some(SharedModelDefaults {
            account_binding: key(required(model, "account_binding")?, limits)?,
            settings: ModelInvocationSettings::new(
                ProviderType::parse(string(required(model, "provider")?)?)
                    .map_err(|_| Error::InvalidMetadata)?,
                ModelId::parse(string(required(model, "model")?)?)
                    .map_err(|_| Error::InvalidMetadata)?,
                GenerationSettings::new(max_output_tokens).map_err(|_| Error::InvalidMetadata)?,
                ReasoningSettings::new(effort).map_err(|_| Error::InvalidMetadata)?,
            ),
            capabilities: ModelCapabilitySnapshot::new(
                string(required(capabilities, "version")?)?,
                support(required(capabilities, "tool_calls")?)?,
                support(required(capabilities, "vision")?)?,
                support(required(capabilities, "structured_output")?)?,
            )
            .map_err(|_| Error::InvalidMetadata)?,
        }),
    })
}
pub(crate) fn decode_mcp(
    value: &Value,
    limits: SharedConfigurationLimits,
) -> Result<SharedMcpServerInput, Error> {
    let transport = required(value, "transport")?;
    let transport = match string(required(transport, "kind")?)? {
        "stdio" => {
            let arguments = required(transport, "arguments")?
                .as_array()
                .ok_or(Error::InvalidMetadata)?;
            let environment = required(transport, "environment")?
                .as_object()
                .ok_or(Error::InvalidMetadata)?;
            if arguments.len() > limits.max_mcp_arguments
                || environment.len() > limits.max_mcp_environment
            {
                return Err(Error::LimitExceeded);
            }
            let arguments = arguments
                .iter()
                .map(
                    |argument| match (argument.get("literal"), argument.get("host_binding")) {
                        (Some(literal), None) => {
                            let literal = string(literal)?;
                            if literal.len() > limits.max_mcp_argument_bytes {
                                return Err(Error::LimitExceeded);
                            }
                            Ok(SharedMcpArgument::Literal(literal.to_owned()))
                        }
                        (None, Some(binding)) => {
                            key(binding, limits).map(SharedMcpArgument::HostBinding)
                        }
                        _ => Err(Error::InvalidMetadata),
                    },
                )
                .collect::<Result<_, _>>()?;
            let environment = environment
                .iter()
                .map(|(name, value)| Ok((name.clone(), key(value, limits)?)))
                .collect::<Result<BTreeMap<_, _>, Error>>()?;
            SharedMcpTransport::Stdio {
                runtime_binding: key(required(transport, "runtime_binding")?, limits)?,
                arguments,
                environment,
            }
        }
        "https" => SharedMcpTransport::Https {
            endpoint: string(required(transport, "endpoint")?)?.to_owned(),
            credential_binding: optional_key(required(transport, "credential_binding")?, limits)?,
        },
        "host_endpoint" => SharedMcpTransport::HostEndpoint {
            endpoint_binding: key(required(transport, "endpoint_binding")?, limits)?,
        },
        _ => return Err(Error::InvalidMetadata),
    };
    Ok(SharedMcpServerInput {
        id: key(required(value, "id")?, limits)?,
        enabled: required(value, "enabled")?
            .as_bool()
            .ok_or(Error::InvalidMetadata)?,
        transport,
    })
}
