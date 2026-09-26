//! Effective per-Run model choice and the local policy used to resolve it.

use crate::{
    CapabilitySupport, ConfigurationGroupId, ContentHash, GenerationSettings,
    ModelCapabilitySnapshot, ModelInvocationSettings, ProviderAccountId, ProviderType,
    ReasoningSettings, SharedConfigurationKey,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunExecutionKind {
    NativeModel,
    Subprocess,
}

impl RunExecutionKind {
    pub fn parse(value: &str) -> Result<Self, InvalidRunExecutionKind> {
        match value {
            "native_model" => Ok(Self::NativeModel),
            "subprocess" => Ok(Self::Subprocess),
            _ => Err(InvalidRunExecutionKind),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::NativeModel => "native_model",
            Self::Subprocess => "subprocess",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidRunExecutionKind;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedModelDefaultProvenance {
    group_id: ConfigurationGroupId,
    revision: u64,
    schema_version: u32,
    content_hash: ContentHash,
    account_binding: SharedConfigurationKey,
    account_binding_version: u64,
}

impl SharedModelDefaultProvenance {
    pub fn new(
        group_id: ConfigurationGroupId,
        revision: u64,
        schema_version: u32,
        content_hash: ContentHash,
        account_binding: SharedConfigurationKey,
        account_binding_version: u64,
    ) -> Option<Self> {
        if revision == 0 || schema_version == 0 || account_binding_version == 0 {
            return None;
        }
        Some(Self {
            group_id,
            revision,
            schema_version,
            content_hash,
            account_binding,
            account_binding_version,
        })
    }

    pub fn group_id(&self) -> &ConfigurationGroupId {
        &self.group_id
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn content_hash(&self) -> &ContentHash {
        &self.content_hash
    }

    pub fn account_binding(&self) -> &SharedConfigurationKey {
        &self.account_binding
    }

    pub fn account_binding_version(&self) -> u64 {
        self.account_binding_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunModelSelectionSource {
    HostDefault,
    SharedDefault(SharedModelDefaultProvenance),
    InvocationHistory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunModelSelection {
    provider_account_id: ProviderAccountId,
    settings: ModelInvocationSettings,
    capabilities: ModelCapabilitySnapshot,
    source: RunModelSelectionSource,
}

impl RunModelSelection {
    pub fn new(
        provider_account_id: ProviderAccountId,
        settings: ModelInvocationSettings,
        capabilities: ModelCapabilitySnapshot,
        source: RunModelSelectionSource,
    ) -> Self {
        Self {
            provider_account_id,
            settings,
            capabilities,
            source,
        }
    }

    pub fn provider_account_id(&self) -> &ProviderAccountId {
        &self.provider_account_id
    }

    pub fn settings(&self) -> &ModelInvocationSettings {
        &self.settings
    }

    pub fn capabilities(&self) -> &ModelCapabilitySnapshot {
        &self.capabilities
    }

    pub fn source(&self) -> &RunModelSelectionSource {
        &self.source
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunModelExecutorPolicy {
    provider_type: ProviderType,
    provider_available: bool,
    max_output_tokens: Option<u32>,
    reasoning_effort: Option<String>,
    capabilities: ModelCapabilitySnapshot,
}

impl RunModelExecutorPolicy {
    pub fn new(
        provider_type: ProviderType,
        provider_available: bool,
        max_output_tokens: Option<u32>,
        reasoning_effort: Option<String>,
        capabilities: ModelCapabilitySnapshot,
    ) -> Self {
        Self {
            provider_type,
            provider_available,
            max_output_tokens,
            reasoning_effort,
            capabilities,
        }
    }

    pub fn provider_type(&self) -> &ProviderType {
        &self.provider_type
    }

    pub fn provider_available(&self) -> bool {
        self.provider_available
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RunModelStartPolicy {
    host_default: Option<RunModelSelection>,
    executor: Option<RunModelExecutorPolicy>,
    resolve_shared_defaults: bool,
}

impl RunModelStartPolicy {
    pub fn new(
        host_default: Option<RunModelSelection>,
        executor: Option<RunModelExecutorPolicy>,
    ) -> Self {
        Self {
            host_default,
            executor,
            resolve_shared_defaults: true,
        }
    }

    pub fn disabled() -> Self {
        Self {
            host_default: None,
            executor: None,
            resolve_shared_defaults: false,
        }
    }

    pub fn host_default(&self) -> Option<&RunModelSelection> {
        self.host_default.as_ref()
    }

    pub fn executor(&self) -> Option<&RunModelExecutorPolicy> {
        self.executor.as_ref()
    }

    pub fn resolves_shared_defaults(&self) -> bool {
        self.resolve_shared_defaults
    }

    pub fn resolve_shared_default(
        &self,
        account_id: ProviderAccountId,
        settings: &ModelInvocationSettings,
        capabilities: &ModelCapabilitySnapshot,
        provenance: SharedModelDefaultProvenance,
    ) -> Result<RunModelSelection, RunModelUnavailableReason> {
        let executor = self
            .executor
            .as_ref()
            .filter(|executor| executor.provider_available)
            .ok_or(RunModelUnavailableReason::ExecutorUnavailable)?;
        if executor.provider_type != *settings.provider() {
            return Err(RunModelUnavailableReason::ProviderUnavailable);
        }
        if !capability_fits(
            capabilities.tool_calls(),
            executor.capabilities.tool_calls(),
        ) || !capability_fits(capabilities.vision(), executor.capabilities.vision())
            || !capability_fits(
                capabilities.structured_output(),
                executor.capabilities.structured_output(),
            )
        {
            return Err(RunModelUnavailableReason::CapabilityUnavailable);
        }
        let max_output_tokens = match (
            settings.generation().max_output_tokens(),
            executor.max_output_tokens,
        ) {
            (Some(shared), Some(local)) => Some(shared.min(local)),
            (Some(shared), None) => Some(shared),
            (None, Some(local)) => Some(local),
            (None, None) => None,
        };
        let reasoning_effort = capped_reasoning_effort(
            settings.reasoning().effort(),
            executor.reasoning_effort.as_deref(),
        )?;
        let settings = ModelInvocationSettings::new(
            settings.provider().clone(),
            settings.model().clone(),
            GenerationSettings::new(max_output_tokens)
                .map_err(|_| RunModelUnavailableReason::CapabilityUnavailable)?,
            ReasoningSettings::new(reasoning_effort)
                .map_err(|_| RunModelUnavailableReason::CapabilityUnavailable)?,
        );
        Ok(RunModelSelection::new(
            account_id,
            settings,
            capabilities.clone(),
            RunModelSelectionSource::SharedDefault(provenance),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunModelUnavailableReason {
    DefaultsMissing,
    ExecutorUnavailable,
    ProviderUnavailable,
    BindingUnavailable,
    AccountUnavailable,
    CapabilityUnavailable,
}

fn capability_fits(requested: CapabilitySupport, local_limit: CapabilitySupport) -> bool {
    requested != CapabilitySupport::Supported || local_limit == CapabilitySupport::Supported
}

fn capped_reasoning_effort(
    shared: Option<&str>,
    local_limit: Option<&str>,
) -> Result<Option<String>, RunModelUnavailableReason> {
    const EFFORTS: [&str; 7] = ["none", "minimal", "low", "medium", "high", "xhigh", "max"];
    for value in [shared, local_limit].into_iter().flatten() {
        if !EFFORTS.contains(&value) {
            return Err(RunModelUnavailableReason::CapabilityUnavailable);
        }
    }
    Ok(match (shared, local_limit) {
        (Some(shared), Some(local)) => {
            let shared_rank = EFFORTS
                .iter()
                .position(|value| *value == shared)
                .unwrap_or(0);
            let local_rank = EFFORTS
                .iter()
                .position(|value| *value == local)
                .unwrap_or(0);
            Some(EFFORTS[shared_rank.min(local_rank)].to_owned())
        }
        (Some(shared), None) => Some(shared.to_owned()),
        (None, Some(local)) => Some(local.to_owned()),
        (None, None) => None,
    })
}
