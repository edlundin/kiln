//! Bounded, untrusted server metadata. Catalogues validate input, never policy.

use std::{
    collections::{BTreeMap, HashSet},
    num::NonZeroUsize,
};

use rmcp::{RoleClient, model::*, service::Peer};

use crate::StdioCallError;

#[derive(Clone, Copy)]
pub struct McpCatalogLimits {
    pub max_pages: NonZeroUsize,
    pub max_entries: NonZeroUsize,
    /// Cumulative encoded list result bytes, in addition to frame limits.
    pub max_bytes: NonZeroUsize,
    /// Per-pattern compiled/DFA size allowance, not total validator memory.
    pub max_regex_bytes: NonZeroUsize,
    pub max_regex_backtracks: NonZeroUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpCatalogError {
    Unsupported,
    LimitExceeded,
    InvalidCatalog,
    CatalogChanged,
    UnknownTool,
    UnknownPrompt,
    UnknownEntry,
    InvalidUri,
    InvalidSchema,
    InvalidArguments,
}

struct NoRetrieval;
impl jsonschema::Retrieve for NoRetrieval {
    fn retrieve(
        &self,
        _: &jsonschema::Uri<String>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(std::io::Error::other("external schema retrieval is disabled").into())
    }
}

pub(crate) struct PageBudget {
    limits: McpCatalogLimits,
    pages: usize,
    entries: usize,
    bytes: usize,
    cursors: HashSet<String>,
}

impl PageBudget {
    pub(crate) fn new(limits: McpCatalogLimits) -> Self {
        Self {
            limits,
            pages: 0,
            entries: 0,
            bytes: 0,
            cursors: HashSet::new(),
        }
    }

    pub(crate) fn record(
        &mut self,
        page: &impl serde::Serialize,
        entries: usize,
        cursor: Option<&str>,
    ) -> Result<(), McpCatalogError> {
        use McpCatalogError as Error;
        if self.pages >= self.limits.max_pages.get()
            || entries > self.limits.max_entries.get() - self.entries
        {
            return Err(Error::LimitExceeded);
        }
        // The frame bounds the page. Count its encoding without retaining an
        // additional serialized copy across pages, including empty pages.
        let mut counter = ByteBudget {
            remaining: self.limits.max_bytes.get() - self.bytes,
        };
        serde_json::to_writer(&mut counter, page).map_err(|_| Error::LimitExceeded)?;
        self.bytes = self.limits.max_bytes.get() - counter.remaining;
        if let Some(cursor) = cursor {
            if !self.cursors.insert(cursor.to_owned()) {
                return Err(Error::InvalidCatalog);
            }
        }
        self.pages += 1;
        self.entries += entries;
        Ok(())
    }
}

pub(crate) async fn validate_prompt(
    peer: &Peer<RoleClient>,
    name: &str,
    arguments: &BTreeMap<String, String>,
    limits: McpCatalogLimits,
) -> Result<(), StdioCallError> {
    if peer
        .peer_info()
        .is_none_or(|info| info.capabilities.prompts.is_none())
    {
        return Err(StdioCallError::Catalog(McpCatalogError::Unsupported));
    }
    let mut budget = PageBudget::new(limits);
    let mut names = HashSet::new();
    let mut selected = None;
    let mut cursor = None;
    for _ in 0..limits.max_pages.get() {
        let response = peer
            .send_request(ClientRequest::ListPromptsRequest(
                ListPromptsRequest::with_param(
                    PaginatedRequestParams::default().with_cursor(cursor),
                ),
            ))
            .await
            .map_err(|error| match error {
                rmcp::service::ServiceError::McpError(_) => {
                    StdioCallError::Catalog(McpCatalogError::InvalidCatalog)
                }
                _ => StdioCallError::Interrupted,
            })?;
        let ServerResult::ListPromptsResult(page) = response else {
            return Err(StdioCallError::Catalog(McpCatalogError::InvalidCatalog));
        };
        budget
            .record(&page, page.prompts.len(), page.next_cursor.as_deref())
            .map_err(StdioCallError::Catalog)?;
        for prompt in page.prompts {
            if !valid_name(&prompt.name) || !names.insert(prompt.name.clone()) {
                return Err(StdioCallError::Catalog(McpCatalogError::InvalidCatalog));
            }
            if prompt.name == name {
                selected = Some(prompt);
            }
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            return validate_prompt_arguments(selected.as_ref(), arguments)
                .map_err(StdioCallError::Catalog);
        }
    }
    Err(StdioCallError::Catalog(McpCatalogError::LimitExceeded))
}

pub(crate) fn valid_name(name: &str) -> bool {
    !name.is_empty() && !name.chars().any(char::is_control)
}

fn validate_prompt_arguments(
    prompt: Option<&Prompt>,
    arguments: &BTreeMap<String, String>,
) -> Result<(), McpCatalogError> {
    let prompt = prompt.ok_or(McpCatalogError::UnknownPrompt)?;
    let mut declared = HashSet::new();
    for argument in prompt.arguments.iter().flatten() {
        if !valid_name(&argument.name) || !declared.insert(argument.name.as_str()) {
            return Err(McpCatalogError::InvalidCatalog);
        }
        if argument.required.unwrap_or(false) && !arguments.contains_key(&argument.name) {
            return Err(McpCatalogError::InvalidArguments);
        }
    }
    if arguments
        .keys()
        .any(|name| !declared.contains(name.as_str()))
    {
        return Err(McpCatalogError::InvalidArguments);
    }
    Ok(())
}

pub(crate) fn validate_resource(peer: &Peer<RoleClient>, uri: &str) -> Result<(), StdioCallError> {
    if peer
        .peer_info()
        .is_none_or(|info| info.capabilities.resources.is_none())
    {
        return Err(StdioCallError::Catalog(McpCatalogError::Unsupported));
    }
    validate_resource_uri(uri).map_err(StdioCallError::Catalog)
}

pub(crate) fn validate_resource_uri(uri: &str) -> Result<(), McpCatalogError> {
    // RFC 3986 Uri requires a scheme. Do not normalize, expand templates, fetch
    // URLs or open local paths. Resource links need not occur in resources/list.
    fluent_uri::Uri::parse(uri)
        .map(|_| ())
        .map_err(|_| McpCatalogError::InvalidUri)
}

/// Re-list before each call; never treat cached metadata as authorization. A
/// complete bounded traversal is required so ambiguous duplicate names fail.
pub(crate) async fn validate_tool(
    peer: &Peer<RoleClient>,
    name: &str,
    arguments: &serde_json::Map<String, serde_json::Value>,
    limits: McpCatalogLimits,
) -> Result<Option<jsonschema::Validator>, StdioCallError> {
    if peer
        .peer_info()
        .is_none_or(|info| info.capabilities.tools.is_none())
    {
        return Err(StdioCallError::Catalog(McpCatalogError::Unsupported));
    }
    let mut catalog = Catalog::new(limits);
    let mut cursor = None;
    for _ in 0..limits.max_pages.get() {
        let response = peer
            .send_request(ClientRequest::ListToolsRequest(
                ListToolsRequest::with_param(PaginatedRequestParams::default().with_cursor(cursor)),
            ))
            .await
            .map_err(|error| match error {
                rmcp::service::ServiceError::McpError(_) => {
                    StdioCallError::Catalog(McpCatalogError::InvalidCatalog)
                }
                _ => StdioCallError::Interrupted,
            })?;
        let ServerResult::ListToolsResult(page) = response else {
            return Err(StdioCallError::Catalog(McpCatalogError::InvalidCatalog));
        };
        cursor = catalog.push(page).map_err(StdioCallError::Catalog)?;
        if cursor.is_none() {
            return catalog
                .validate(name, arguments)
                .map_err(StdioCallError::Catalog);
        }
    }
    Err(StdioCallError::Catalog(McpCatalogError::LimitExceeded))
}

struct Catalog {
    limits: McpCatalogLimits,
    tools: Vec<Tool>,
    names: HashSet<String>,
    budget: PageBudget,
}

impl Catalog {
    fn new(limits: McpCatalogLimits) -> Self {
        Self {
            limits,
            tools: Vec::new(),
            names: HashSet::new(),
            budget: PageBudget::new(limits),
        }
    }

    fn push(&mut self, page: ListToolsResult) -> Result<Option<String>, McpCatalogError> {
        use McpCatalogError as Error;
        self.budget
            .record(&page, page.tools.len(), page.next_cursor.as_deref())?;
        for tool in &page.tools {
            if tool.name.is_empty()
                || tool.name.chars().any(char::is_control)
                || !self.names.insert(tool.name.to_string())
            {
                return Err(Error::InvalidCatalog);
            }
        }
        self.tools.extend(page.tools);
        Ok(page.next_cursor)
    }

    fn validate(
        &self,
        name: &str,
        arguments: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<Option<jsonschema::Validator>, McpCatalogError> {
        use McpCatalogError as Error;
        let tool = self
            .tools
            .iter()
            .find(|tool| tool.name == name)
            .ok_or(Error::UnknownTool)?;
        let validator = compile_schema(&tool.input_schema, self.limits)?;
        if !validator.is_valid(&serde_json::Value::Object(arguments.clone())) {
            return Err(Error::InvalidArguments);
        }
        tool.output_schema
            .as_ref()
            .map(|schema| compile_schema(schema, self.limits))
            .transpose()
    }
}

fn compile_schema(
    schema: &serde_json::Map<String, serde_json::Value>,
    limits: McpCatalogLimits,
) -> Result<jsonschema::Validator, McpCatalogError> {
    use McpCatalogError as Error;
    // MCP input/output schemas describe objects. Draft detection honors declared
    // standard dialects; the library defaults to 2020-12 when absent. Inline
    // references remain supported, but no URL or file is ever retrieved.
    if schema.get("type").and_then(|v| v.as_str()) != Some("object") {
        return Err(Error::InvalidSchema);
    }
    jsonschema::options()
        .with_retriever(NoRetrieval)
        .with_pattern_options(
            jsonschema::PatternOptions::fancy_regex()
                .backtrack_limit(limits.max_regex_backtracks.get())
                .size_limit(limits.max_regex_bytes.get())
                .dfa_size_limit(limits.max_regex_bytes.get()),
        )
        .build(&serde_json::Value::Object(schema.clone()))
        .map_err(|_| Error::InvalidSchema)
}

struct ByteBudget {
    remaining: usize,
}
impl std::io::Write for ByteBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.remaining = self
            .remaining
            .checked_sub(bytes.len())
            .ok_or_else(|| std::io::Error::other("catalogue exceeds byte budget"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn limits() -> McpCatalogLimits {
        McpCatalogLimits {
            max_pages: NonZeroUsize::new(2).unwrap(),
            max_entries: NonZeroUsize::new(2).unwrap(),
            max_bytes: NonZeroUsize::new(4096).unwrap(),
            max_regex_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
            max_regex_backtracks: NonZeroUsize::new(10_000).unwrap(),
        }
    }
    fn page(schema: serde_json::Value) -> ListToolsResult {
        serde_json::from_value(json!({"tools":[{"name":"write","inputSchema":schema}]})).unwrap()
    }

    #[test]
    fn prompts_check_required_declared_arguments_and_resources_use_uri_syntax() {
        let prompt: Prompt = serde_json::from_value(json!({
            "name":"review", "arguments":[{"name":"language","required":true},{"name":"style"}]
        }))
        .unwrap();
        assert!(
            validate_prompt_arguments(
                Some(&prompt),
                &BTreeMap::from([("language".into(), "en".into())])
            )
            .is_ok()
        );
        assert_eq!(
            validate_prompt_arguments(Some(&prompt), &BTreeMap::new()),
            Err(McpCatalogError::InvalidArguments)
        );
        assert_eq!(
            validate_prompt_arguments(
                Some(&prompt),
                &BTreeMap::from([
                    ("language".into(), "en".into()),
                    ("extra".into(), "x".into())
                ])
            ),
            Err(McpCatalogError::InvalidArguments)
        );
        let duplicate: Prompt = serde_json::from_value(
            json!({"name":"review","arguments":[{"name":"language"},{"name":"language"}]}),
        )
        .unwrap();
        assert_eq!(
            validate_prompt_arguments(Some(&duplicate), &BTreeMap::new()),
            Err(McpCatalogError::InvalidCatalog)
        );
        for uri in [
            "notes://host/a%20b?x=1#part",
            "file:///not-a-local-read",
            "urn:fixture:document",
        ] {
            assert!(validate_resource_uri(uri).is_ok(), "{uri}");
        }
        for uri in [
            "relative/path",
            "notes://host/%zz",
            "notes://host/a b",
            "notes://host/{template}",
        ] {
            assert_eq!(
                validate_resource_uri(uri),
                Err(McpCatalogError::InvalidUri),
                "{uri}"
            );
        }
    }

    #[test]
    fn selected_schema_validates_arguments_and_never_retrieves_external_references() {
        let mut catalog = Catalog::new(limits());
        catalog
            .push(page(json!({
                "type":"object", "$defs":{"text":{"type":"string","minLength":1}},
                "properties":{"text":{"$ref":"#/$defs/text"}},
                "required":["text"], "additionalProperties":false
            })))
            .unwrap();
        assert!(
            catalog
                .validate("write", json!({"text":"hello"}).as_object().unwrap())
                .is_ok()
        );
        for args in [
            json!({}),
            json!({"text":2}),
            json!({"text":""}),
            json!({"text":"ok","extra":true}),
        ] {
            assert_eq!(
                catalog.validate("write", args.as_object().unwrap()).err(),
                Some(McpCatalogError::InvalidArguments)
            );
        }
        assert_eq!(
            catalog.validate("unknown", &Default::default()).err(),
            Some(McpCatalogError::UnknownTool)
        );
        for schema in [
            json!({"type":"object","properties":{"text":{"$ref":"https://example.invalid/schema"}}}),
            json!({"type":"object","properties":{"text":{"$ref":"file:///etc/passwd"}}}),
            json!({"type":"object","$schema":"https://example.invalid/dialect"}),
            json!({"type":"object","required":"invalid"}),
        ] {
            let mut catalog = Catalog::new(limits());
            catalog.push(page(schema)).unwrap();
            assert_eq!(
                catalog
                    .validate("write", json!({"text":"hello"}).as_object().unwrap())
                    .err(),
                Some(McpCatalogError::InvalidSchema)
            );
        }
    }

    #[test]
    fn pagination_rejects_ambiguous_names_cycles_and_excess_metadata() {
        let mut catalog = Catalog::new(limits());
        let mut first = page(json!({"type":"object"}));
        first.next_cursor = Some("next".into());
        assert_eq!(
            catalog.push(first.clone()).unwrap().as_deref(),
            Some("next")
        );
        let mut second = page(json!({"type":"object"}));
        assert_eq!(
            catalog.push(second.clone()),
            Err(McpCatalogError::InvalidCatalog)
        );
        let mut catalog = Catalog::new(limits());
        catalog.push(first).unwrap();
        second.tools.clear();
        second.next_cursor = Some("next".into());
        assert_eq!(catalog.push(second), Err(McpCatalogError::InvalidCatalog));
        let page = page(json!({"type":"object"}));
        let size = serde_json::to_vec(&page).unwrap().len();
        let mut exact = limits();
        exact.max_bytes = NonZeroUsize::new(size).unwrap();
        assert!(Catalog::new(exact).push(page.clone()).is_ok());
        exact.max_bytes = NonZeroUsize::new(size - 1).unwrap();
        assert_eq!(
            Catalog::new(exact).push(page.clone()),
            Err(McpCatalogError::LimitExceeded)
        );
        let mut count = limits();
        count.max_entries = NonZeroUsize::new(1).unwrap();
        let mut catalog = Catalog::new(count);
        catalog.push(page.clone()).unwrap();
        assert_eq!(catalog.push(page), Err(McpCatalogError::LimitExceeded));
    }
}
