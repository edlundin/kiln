//! Fresh metadata for broker search/describe. No catalogue grants execution.

use std::collections::HashSet;

use rmcp::{RoleClient, model::*, service::Peer};
use tokio::time::Instant;

use crate::catalog::{PageBudget, valid_name, validate_resource_uri};
use crate::{McpCatalogError, McpCatalogLimits, StdioCallError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum McpCatalogKind {
    Tools,
    Prompts,
    Resources,
    ResourceTemplates,
}

/// Untrusted metadata, including schemas and annotations. No Debug to avoid
/// accidental logging. This value carries no authorization or cache validity.
pub enum McpCatalogEntries {
    Tools(Vec<Tool>),
    Prompts(Vec<Prompt>),
    Resources(Vec<Resource>),
    ResourceTemplates(Vec<ResourceTemplate>),
}

/// Low-level adapter for an already authorized peer. The caller owns its
/// generation, serial scheduling and durable ToolCall. Fetches a complete list
/// or fails without returning partial entries; dropping the future cancels the
/// waiter. No SDK cache fallback, content reads, template expansion or retries.
/// Results must not be reused across calls as a current catalogue snapshot.
pub async fn discover_catalog(
    peer: &Peer<RoleClient>,
    kind: McpCatalogKind,
    limits: McpCatalogLimits,
    deadline: Instant,
) -> Result<McpCatalogEntries, StdioCallError> {
    if Instant::now() >= deadline {
        return Err(StdioCallError::DeadlineBeforeSend);
    }
    tokio::time::timeout_at(deadline, collect_catalog(peer, kind, limits))
        .await
        .map_err(|_| StdioCallError::Interrupted)?
}

pub(crate) async fn collect_catalog(
    peer: &Peer<RoleClient>,
    kind: McpCatalogKind,
    limits: McpCatalogLimits,
) -> Result<McpCatalogEntries, StdioCallError> {
    let supported = peer.peer_info().is_some_and(|info| match kind {
        McpCatalogKind::Tools => info.capabilities.tools.is_some(),
        McpCatalogKind::Prompts => info.capabilities.prompts.is_some(),
        McpCatalogKind::Resources | McpCatalogKind::ResourceTemplates => {
            info.capabilities.resources.is_some()
        }
    });
    if !supported {
        return Err(StdioCallError::Catalog(McpCatalogError::Unsupported));
    }
    let mut entries = match kind {
        McpCatalogKind::Tools => McpCatalogEntries::Tools(Vec::new()),
        McpCatalogKind::Prompts => McpCatalogEntries::Prompts(Vec::new()),
        McpCatalogKind::Resources => McpCatalogEntries::Resources(Vec::new()),
        McpCatalogKind::ResourceTemplates => McpCatalogEntries::ResourceTemplates(Vec::new()),
    };
    let mut budget = PageBudget::new(limits);
    let mut identifiers = HashSet::new();
    let mut cursor = None;
    for _ in 0..limits.max_pages.get() {
        let params = PaginatedRequestParams::default().with_cursor(cursor);
        let request = match kind {
            McpCatalogKind::Tools => {
                ClientRequest::ListToolsRequest(ListToolsRequest::with_param(params))
            }
            McpCatalogKind::Prompts => {
                ClientRequest::ListPromptsRequest(ListPromptsRequest::with_param(params))
            }
            McpCatalogKind::Resources => {
                ClientRequest::ListResourcesRequest(ListResourcesRequest::with_param(params))
            }
            McpCatalogKind::ResourceTemplates => ClientRequest::ListResourceTemplatesRequest(
                ListResourceTemplatesRequest::with_param(params),
            ),
        };
        let response = peer
            .send_request(request)
            .await
            .map_err(|error| match error {
                rmcp::service::ServiceError::McpError(_) => {
                    StdioCallError::Catalog(McpCatalogError::InvalidCatalog)
                }
                _ => StdioCallError::Interrupted,
            })?;
        cursor = append_page(&mut entries, response, &mut budget, &mut identifiers)
            .map_err(StdioCallError::Catalog)?;
        if cursor.is_none() {
            return Ok(entries);
        }
    }
    Err(StdioCallError::Catalog(McpCatalogError::LimitExceeded))
}

fn append_page(
    entries: &mut McpCatalogEntries,
    response: ServerResult,
    budget: &mut PageBudget,
    identifiers: &mut HashSet<String>,
) -> Result<Option<String>, McpCatalogError> {
    use McpCatalogError::InvalidCatalog;
    match (entries, response) {
        (McpCatalogEntries::Tools(entries), ServerResult::ListToolsResult(page)) => {
            budget.record(&page, page.tools.len(), page.next_cursor.as_deref())?;
            for entry in &page.tools {
                if !valid_name(&entry.name) || !identifiers.insert(entry.name.to_string()) {
                    return Err(InvalidCatalog);
                }
            }
            entries.extend(page.tools);
            Ok(page.next_cursor)
        }
        (McpCatalogEntries::Prompts(entries), ServerResult::ListPromptsResult(page)) => {
            budget.record(&page, page.prompts.len(), page.next_cursor.as_deref())?;
            for entry in &page.prompts {
                if !valid_name(&entry.name) || !identifiers.insert(entry.name.clone()) {
                    return Err(InvalidCatalog);
                }
            }
            entries.extend(page.prompts);
            Ok(page.next_cursor)
        }
        (McpCatalogEntries::Resources(entries), ServerResult::ListResourcesResult(page)) => {
            budget.record(&page, page.resources.len(), page.next_cursor.as_deref())?;
            for entry in &page.resources {
                validate_resource_uri(&entry.uri)?;
                // Resource names need not be unique. The exact URI identifies
                // the selection; normalization could change server semantics.
                if !valid_name(&entry.name) || !identifiers.insert(entry.uri.clone()) {
                    return Err(InvalidCatalog);
                }
            }
            entries.extend(page.resources);
            Ok(page.next_cursor)
        }
        (
            McpCatalogEntries::ResourceTemplates(entries),
            ServerResult::ListResourceTemplatesResult(page),
        ) => {
            budget.record(
                &page,
                page.resource_templates.len(),
                page.next_cursor.as_deref(),
            )?;
            for entry in &page.resource_templates {
                // Preserve templates as opaque untrusted descriptions. This is
                // not RFC 6570 validation and never authorizes expansion.
                if !valid_name(&entry.name)
                    || !valid_name(&entry.uri_template)
                    || !identifiers.insert(entry.uri_template.clone())
                {
                    return Err(InvalidCatalog);
                }
            }
            entries.extend(page.resource_templates);
            Ok(page.next_cursor)
        }
        _ => Err(InvalidCatalog),
    }
}
