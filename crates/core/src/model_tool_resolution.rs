use crate::{
    CapabilitySupport, ModelInvocation, ModelInvocationCompletionKind, ModelInvocationOutcome,
    ModelInvocationPurpose, ModelInvocationStoreError, ModelToolCatalog, ModelToolCatalogError,
    ModelToolDefinition, ModelToolRequest, ModelToolRequestBatch, ModelToolRequestError,
};

/// Kiln-owned local registry. Providers never receive this port. Implementations
/// must be pure during resolution: no dispatch, filesystem/network operations,
/// approval decisions, or capability grants. A command is validated data only.
pub trait ModelToolArgumentResolver {
    type Command;

    fn definition(&self, capability: &str, revision: &str) -> Option<&ModelToolDefinition>;

    /// Validate against the registered tool's exact argument contract, including
    /// unknown fields and semantic constraints, and return the typed command.
    /// Do not fetch schema references or reinterpret a schema from model output.
    fn parse_arguments(
        &self,
        definition: &ModelToolDefinition,
        arguments_json: &str,
    ) -> Result<Self::Command, ModelToolArgumentError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelToolArgumentError {
    InvalidArguments,
    UnsupportedSchema,
    Unavailable,
}

/// No Debug: commands and argument payloads can contain private model input.
pub struct ResolvedModelToolRequest<C> {
    source: ModelToolRequest,
    definition: ModelToolDefinition,
    command: C,
}

impl<C> ResolvedModelToolRequest<C> {
    pub fn source(&self) -> &ModelToolRequest {
        &self.source
    }
    pub fn definition(&self) -> &ModelToolDefinition {
        &self.definition
    }
    pub fn command(&self) -> &C {
        &self.command
    }
}

/// Constructible only by resolving a complete durable batch. This contains no
/// ToolCall ID, effective scope, approval, or permission to execute a command.
pub struct ResolvedModelToolBatch<C> {
    invocation: ModelInvocation,
    catalog: ModelToolCatalog,
    requests: Vec<ResolvedModelToolRequest<C>>,
}

impl<C> ResolvedModelToolBatch<C> {
    pub fn invocation(&self) -> &ModelInvocation {
        &self.invocation
    }
    pub fn catalog(&self) -> &ModelToolCatalog {
        &self.catalog
    }
    pub fn requests(&self) -> &[ResolvedModelToolRequest<C>] {
        &self.requests
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelToolResolutionError {
    InvocationNotComplete,
    CatalogMissing,
    RequestsMissing,
    IntegrityViolation,
    ToolNotOffered {
        position: usize,
    },
    ImplementationUnavailable {
        position: usize,
    },
    DefinitionChanged {
        position: usize,
    },
    Arguments {
        position: usize,
        reason: ModelToolArgumentError,
    },
    Invocation(ModelInvocationStoreError),
    Catalog(ModelToolCatalogError),
    Requests(ModelToolRequestError),
}

pub(crate) fn resolve_model_tool_requests<R: ModelToolArgumentResolver>(
    invocation: ModelInvocation,
    requests: ModelToolRequestBatch,
    catalog: ModelToolCatalog,
    resolver: &R,
) -> Result<ResolvedModelToolBatch<R::Command>, ModelToolResolutionError> {
    if invocation.outcome()
        != Some(ModelInvocationOutcome::completed(
            ModelInvocationCompletionKind::ToolRequests,
        ))
        || invocation.purpose() != ModelInvocationPurpose::Generation
        || invocation.capabilities().tool_calls() != CapabilitySupport::Supported
    {
        return Err(ModelToolResolutionError::InvocationNotComplete);
    }
    if requests.invocation_id() != invocation.invocation_id() {
        return Err(ModelToolResolutionError::IntegrityViolation);
    }
    catalog
        .validate_for(&invocation)
        .map_err(ModelToolResolutionError::Catalog)?;
    // Check every offered/local binding before invoking any argument parser.
    let mut definitions = Vec::with_capacity(requests.requests().len());
    for (position, request) in requests.requests().iter().enumerate() {
        let offered = catalog
            .find(request.name())
            .ok_or(ModelToolResolutionError::ToolNotOffered { position })?;
        let registered = resolver
            .definition(offered.capability(), offered.revision())
            .ok_or(ModelToolResolutionError::ImplementationUnavailable { position })?;
        if registered != offered {
            return Err(ModelToolResolutionError::DefinitionChanged { position });
        }
        definitions.push(offered);
    }
    let mut resolved = Vec::with_capacity(requests.requests().len());
    for (position, (request, definition)) in requests.requests().iter().zip(definitions).enumerate()
    {
        let command = resolver
            .parse_arguments(definition, request.arguments_json())
            .map_err(|reason| ModelToolResolutionError::Arguments { position, reason })?;
        resolved.push(ResolvedModelToolRequest {
            source: request.clone(),
            definition: definition.clone(),
            command,
        });
    }
    Ok(ResolvedModelToolBatch {
        invocation,
        catalog,
        requests: resolved,
    })
}
