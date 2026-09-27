//! Claimed operations with explicit roots mediation; no SDK automatic redispatch.

use std::{io::Write, num::NonZeroUsize};

use kiln_core::{McpCommand, McpDispatchPermit, McpGenerationId, McpInvocationError, McpOperation};
use rmcp::{
    RoleClient,
    model::*,
    service::{Peer, ServiceError},
};
use tokio::{sync::oneshot, time::Instant};

pub struct StdioCallLimits {
    /// Explicit total roots requests across legacy callbacks and MRTR rounds.
    /// None disables mediation; every accepted MRTR round consumes at least one.
    pub max_input_requests: Option<NonZeroUsize>,
    pub deadline: Instant,
    /// Encoded result ceiling, in addition to the generation's frame ceiling.
    pub max_result_bytes: NonZeroUsize,
    pub catalog: crate::McpCatalogLimits,
}

/// Untrusted server output. No Debug; the broker must apply ordinary output and
/// artifact handling before adding any of it to model context or a client view.
pub struct StdioCallResult {
    pub json: Vec<u8>,
    pub is_error: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StdioCallError {
    Catalog(crate::McpCatalogError),
    InvalidOutput,
    Rejected,
    CancelledBeforeSend,
    DeadlineBeforeSend,
    Interrupted,
    Server,
    UnsupportedContinuation,
    ResultTooLarge,
    Store(McpInvocationError),
    WorkerLost,
}

pub(crate) struct DispatchRequest {
    pub permit: McpDispatchPermit,
    pub limits: StdioCallLimits,
    pub cancellation: oneshot::Receiver<()>,
    pub reply: oneshot::Sender<Result<DispatchOutcome, StdioCallError>>,
}

/// Constructed only after the generation commits the invocation outcome. A
/// missing receipt must not be turned into a terminal native ToolCall result.
pub(crate) struct DispatchOutcome {
    pub receipt: kiln_core::McpInvocationRecord,
    pub result: Result<StdioCallResult, StdioCallError>,
}

pub(crate) async fn send_once<
    S: kiln_core::McpInputStore
        + kiln_core::McpElicitationDecisionStore
        + kiln_core::McpElicitationUrlStore,
>(
    peer: &Peer<RoleClient>,
    command: &McpCommand,
    generation: &McpGenerationId,
    epochs: &crate::catalog_state::CatalogEpochs,
    cache: &mut crate::catalog_cache::CatalogCache,
    limits: &StdioCallLimits,
    handler: &crate::mediation::RuntimeClient<S>,
) -> Result<StdioCallResult, StdioCallError> {
    let operation = command.operation();
    cache.prune(epochs, limits.catalog);
    if let McpOperation::Search { kind, snapshot, .. }
    | McpOperation::Describe { kind, snapshot, .. } = operation
    {
        let version = epochs.version(*kind);
        if version.is_none() {
            return Err(StdioCallError::Catalog(
                crate::McpCatalogError::CatalogChanged,
            ));
        }
        let retained = if let Some(token) = snapshot {
            cache.get(*kind, token).map_err(StdioCallError::Catalog)?
        } else {
            let collected = crate::discovery::collect_catalog(peer, *kind, limits.catalog).await?;
            if !epochs.unchanged(*kind, version) {
                return Err(StdioCallError::Catalog(
                    crate::McpCatalogError::CatalogChanged,
                ));
            }
            cache
                .insert(
                    generation,
                    *kind,
                    version.expect("checked epoch"),
                    collected,
                    limits.catalog,
                )
                .map_err(StdioCallError::Catalog)?
        };
        let value = crate::discovery::project_catalog(&retained.catalog.entries, operation)
            .map_err(StdioCallError::Catalog)?;
        // Projection is synchronous; observe cancellation/deadline again before
        // accepting its result. This does not promise CPU-time preemption.
        tokio::task::yield_now().await;
        if Instant::now() >= limits.deadline {
            return Err(StdioCallError::Interrupted);
        }
        if !epochs.unchanged(*kind, Some(retained.epoch)) {
            return Err(StdioCallError::Catalog(
                crate::McpCatalogError::CatalogChanged,
            ));
        }
        return encode_result(
            &serde_json::json!({
                "server_id":command.server_id().as_str(),
                "definition_version":command.definition_version(),
                "generation":generation.as_str(),
                "protocol_version":peer.peer_info().map(|info| info.protocol_version.as_str().to_owned()),
                "kind":kind.as_str(),
                "catalog_notification_epoch":retained.epoch,
                "catalog_snapshot":retained.token,
                "result":value,
            }),
            false,
            limits.max_result_bytes,
        );
    }
    // Resource links need not occur in a list: list changes cannot turn a
    // previously approved URI into a discovery-membership requirement.
    let catalog_kind = match operation {
        McpOperation::Tool { .. } => Some(kiln_core::McpCatalogKind::Tools),
        McpOperation::Prompt { .. } => Some(kiln_core::McpCatalogKind::Prompts),
        _ => None,
    };
    let catalog_version = catalog_kind.and_then(|kind| epochs.version(kind));
    if catalog_kind.is_some() && catalog_version.is_none() {
        return Err(StdioCallError::Catalog(
            crate::McpCatalogError::CatalogChanged,
        ));
    }
    let output_validator = match operation {
        McpOperation::Tool { name, arguments } => {
            crate::catalog::validate_tool(peer, name, arguments, limits.catalog).await?
        }
        McpOperation::Prompt { name, arguments } => {
            crate::catalog::validate_prompt(peer, name, arguments, limits.catalog).await?;
            None
        }
        McpOperation::Resource { uri } => {
            crate::catalog::validate_resource(peer, uri)?;
            None
        }
        McpOperation::Search { .. } | McpOperation::Describe { .. } => unreachable!(),
    };
    // Schema compilation is synchronous. Let the owner's biased cancellation
    // select run again, then recheck time before sending any operation request.
    tokio::task::yield_now().await;
    if Instant::now() >= limits.deadline {
        return Err(StdioCallError::DeadlineBeforeSend);
    }
    if catalog_kind.is_some_and(|kind| !epochs.unchanged(kind, catalog_version)) {
        return Err(StdioCallError::Catalog(
            crate::McpCatalogError::CatalogChanged,
        ));
    }
    let mut request = match operation {
        McpOperation::Tool { name, arguments } => {
            ClientRequest::CallToolRequest(CallToolRequest::new(
                CallToolRequestParams::new(name.clone()).with_arguments(arguments.clone()),
            ))
        }
        McpOperation::Resource { uri } => ClientRequest::ReadResourceRequest(
            ReadResourceRequest::new(ReadResourceRequestParams::new(uri.clone())),
        ),
        McpOperation::Prompt { name, arguments } => {
            ClientRequest::GetPromptRequest(GetPromptRequest::new(
                GetPromptRequestParams::new(name.clone()).with_arguments(
                    arguments
                        .iter()
                        .map(|(key, value)| (key.clone(), serde_json::Value::String(value.clone())))
                        .collect(),
                ),
            ))
        }
        McpOperation::Search { .. } | McpOperation::Describe { .. } => unreachable!(),
    };
    // The raw request path avoids SDK resource cache fallback and automatic MRTR
    // rounds. Modern metadata/unique IDs still come from the negotiated peer.
    let result = loop {
        let result = peer
            .send_request(request.clone())
            .await
            .map_err(|error| match error {
                ServiceError::McpError(_) => StdioCallError::Server,
                _ => StdioCallError::Interrupted,
            })?;
        let Some(input) = crate::continuation::decode(&result, limits.max_input_requests)? else {
            break result;
        };
        if peer
            .peer_info()
            .is_none_or(|info| info.protocol_version != ProtocolVersion::V_2026_07_28)
        {
            return Err(StdioCallError::UnsupportedContinuation);
        }
        use crate::continuation::Request;
        // A disabled URL round must not partially mediate roots/forms.
        if input.requests.values().any(|request| match request {
            Request::Url { .. }
            | Request::Sdk(InputRequest::Elicitation(ElicitRequest {
                params: ElicitRequestParams::UrlElicitationParams { .. },
                ..
            })) => !handler.supports_url_elicitation(),
            Request::Sdk(InputRequest::ListRoots(_) | InputRequest::Elicitation(_)) => false,
            _ => true,
        }) {
            return Err(StdioCallError::UnsupportedContinuation);
        }
        let mut responses = InputResponses::new();
        for (id, input) in input.requests {
            let response = match input {
                Request::Sdk(InputRequest::ListRoots(_)) => serde_json::to_value(
                    handler
                        .roots()
                        .await
                        .map_err(|_| StdioCallError::UnsupportedContinuation)?,
                ),
                Request::Sdk(InputRequest::Elicitation(ElicitRequest {
                    params: ElicitRequestParams::UrlElicitationParams { message, url, .. },
                    ..
                })) => serde_json::to_value(
                    handler
                        .url_elicitation(
                            message,
                            url,
                            kiln_core::McpElicitationUrlContext::Stateless,
                        )
                        .await
                        .map_err(|_| StdioCallError::UnsupportedContinuation)?,
                ),
                Request::Sdk(InputRequest::Elicitation(request)) => serde_json::to_value(
                    handler
                        .elicitation(request.params)
                        .await
                        .map_err(|_| StdioCallError::UnsupportedContinuation)?,
                ),
                Request::Url { message, url } => serde_json::to_value(
                    handler
                        .url_elicitation(
                            message,
                            url,
                            kiln_core::McpElicitationUrlContext::Stateless,
                        )
                        .await
                        .map_err(|_| StdioCallError::UnsupportedContinuation)?,
                ),
                _ => return Err(StdioCallError::UnsupportedContinuation),
            }
            .map_err(|_| StdioCallError::InvalidOutput)?;
            responses.insert(id, response);
        }
        match &mut request {
            ClientRequest::CallToolRequest(r) => {
                r.params.input_responses = Some(responses);
                r.params.request_state = input.state;
            }
            ClientRequest::ReadResourceRequest(r) => {
                r.params.input_responses = Some(responses);
                r.params.request_state = input.state;
            }
            ClientRequest::GetPromptRequest(r) => {
                r.params.input_responses = Some(responses);
                r.params.request_state = input.state;
            }
            _ => return Err(StdioCallError::UnsupportedContinuation),
        }
        // Re-enter the owner's biased cancellation select before continuing the
        // same operation. Catalogue changes during mediation revoke this send.
        tokio::task::yield_now().await;
        if Instant::now() >= limits.deadline
            || catalog_kind.is_some_and(|kind| !epochs.unchanged(kind, catalog_version))
        {
            return Err(StdioCallError::Interrupted);
        }
    };
    let is_error = match (&result, operation) {
        (ServerResult::CallToolResult(result), McpOperation::Tool { .. }) => {
            // An error result need not satisfy the success output contract.
            // Invalid success output follows a send; it never permits a retry.
            if !result.is_error.unwrap_or(false)
                && output_validator.as_ref().is_some_and(|validator| {
                    result
                        .structured_content
                        .as_ref()
                        .is_none_or(|value| !validator.is_valid(value))
                })
            {
                return Err(StdioCallError::InvalidOutput);
            }
            result.is_error.unwrap_or(false)
        }
        (ServerResult::ReadResourceResult(result), McpOperation::Resource { .. }) => {
            for content in &result.contents {
                let uri = match content {
                    ResourceContents::TextResourceContents { uri, .. }
                    | ResourceContents::BlobResourceContents { uri, .. } => uri,
                    _ => return Err(StdioCallError::InvalidOutput),
                };
                crate::catalog::validate_resource_uri(uri)
                    .map_err(|_| StdioCallError::InvalidOutput)?;
            }
            false
        }
        (ServerResult::GetPromptResult(_), McpOperation::Prompt { .. }) => false,
        (ServerResult::InputRequiredResult(_) | ServerResult::CreateTaskResult(_), _) => {
            return Err(StdioCallError::UnsupportedContinuation);
        }
        _ => return Err(StdioCallError::Interrupted),
    };
    encode_result(&result, is_error, limits.max_result_bytes)
}

fn encode_result(
    result: &impl serde::Serialize,
    is_error: bool,
    limit: NonZeroUsize,
) -> Result<StdioCallResult, StdioCallError> {
    let mut writer = BoundedResult {
        bytes: Vec::new(),
        limit: limit.get(),
    };
    serde_json::to_writer(&mut writer, result).map_err(|_| StdioCallError::ResultTooLarge)?;
    Ok(StdioCallResult {
        json: writer.bytes,
        is_error,
    })
}

struct BoundedResult {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for BoundedResult {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("MCP result exceeds caller budget"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
