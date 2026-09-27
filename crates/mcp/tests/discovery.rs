#![cfg(unix)]

use kiln_mcp::{
    McpCatalogEntries, McpCatalogError, McpCatalogKind, McpCatalogLimits, ProtocolPolicy,
    ProtocolVersion, StdioCallError, discover_catalog, start_stdio_client,
};
use rmcp::{
    RoleClient,
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
    transport::Transport,
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    convert::Infallible,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use tokio::{
    sync::mpsc,
    time::{Duration, Instant},
};

struct Fixture {
    replies: VecDeque<Value>,
    sent: Arc<Mutex<Vec<Value>>>,
    tx: mpsc::UnboundedSender<ServerJsonRpcMessage>,
    rx: mpsc::UnboundedReceiver<ServerJsonRpcMessage>,
}
impl Transport<RoleClient> for Fixture {
    type Error = Infallible;
    fn send(
        &mut self,
        message: ClientJsonRpcMessage,
    ) -> impl Future<Output = Result<(), Infallible>> + Send + 'static {
        let message = serde_json::to_value(message).unwrap();
        if let Some(id) = message.get("id")
            && let Some(result) = self.replies.pop_front()
        {
            self.tx
                .send(
                    serde_json::from_value(json!({"jsonrpc":"2.0","id":id,"result":result}))
                        .unwrap(),
                )
                .unwrap();
        }
        self.sent.lock().unwrap().push(message);
        std::future::ready(Ok(()))
    }
    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        self.rx.recv().await
    }
    async fn close(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
}
fn limits() -> McpCatalogLimits {
    McpCatalogLimits {
        max_pages: NonZeroUsize::new(2).unwrap(),
        max_entries: NonZeroUsize::new(2).unwrap(),
        max_bytes: NonZeroUsize::new(4096).unwrap(),
        max_regex_bytes: NonZeroUsize::new(1024).unwrap(),
        max_regex_backtracks: NonZeroUsize::new(100).unwrap(),
    }
}
async fn discover(
    kind: McpCatalogKind,
    capabilities: Value,
    pages: Vec<Value>,
    limits: McpCatalogLimits,
) -> (Result<McpCatalogEntries, StdioCallError>, Vec<Value>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let sent = Arc::new(Mutex::new(Vec::new()));
    let mut replies = VecDeque::from(pages);
    replies.push_front(json!({"protocolVersion":"2025-11-25","capabilities":capabilities,"serverInfo":{"name":"fixture","version":"1"}}));
    let client = start_stdio_client(
        (),
        Fixture {
            replies,
            sent: sent.clone(),
            tx,
            rx,
        },
        ProtocolPolicy::Pinned(ProtocolVersion::V20251125),
    )
    .await
    .unwrap();
    // Paused Tokio time makes a silent peer exercise the absolute deadline
    // deterministically, without imposing a product timeout.
    let result = discover_catalog(
        &client,
        kind,
        limits,
        Instant::now() + Duration::from_secs(1),
    )
    .await;
    client.cancel().await.unwrap();
    let messages = sent.lock().unwrap().clone();
    (result, messages)
}

#[tokio::test(start_paused = true)]
async fn lists_all_kinds_and_preserves_exact_resource_identifiers_without_reads() {
    let cases = [
        (
            McpCatalogKind::Tools,
            json!({"tools":{}}),
            "tools/list",
            json!({"tools":[{"name":"write","inputSchema":{"type":"object"}}]}),
        ),
        (
            McpCatalogKind::Prompts,
            json!({"prompts":{}}),
            "prompts/list",
            json!({"prompts":[{"name":"review"}]}),
        ),
        (
            McpCatalogKind::ResourceTemplates,
            json!({"resources":{}}),
            "resources/templates/list",
            json!({"resourceTemplates":[{"name":"notes","uriTemplate":"notes:///{+path}{?query}"}]}),
        ),
    ];
    for (kind, capabilities, method, page) in cases {
        let (result, sent) = discover(kind, capabilities, vec![page], limits()).await;
        match result.unwrap() {
            McpCatalogEntries::Tools(entries) => assert_eq!(entries[0].name, "write"),
            McpCatalogEntries::Prompts(entries) => assert_eq!(entries[0].name, "review"),
            McpCatalogEntries::ResourceTemplates(entries) => {
                assert_eq!(entries[0].uri_template, "notes:///{+path}{?query}")
            }
            _ => panic!("wrong catalogue"),
        }
        assert_eq!(
            sent.iter()
                .filter_map(|m| m["method"].as_str())
                .collect::<Vec<_>>(),
            ["initialize", "notifications/initialized", method]
        );
    }
    let (result, sent) = discover(McpCatalogKind::Resources, json!({"resources":{}}), vec![
        json!({"resources":[{"name":"same","uri":"file:///must-not-open"}],"nextCursor":"opaque/+=?"}),
        json!({"resources":[{"name":"same","uri":"notes://host/%61?x=1#part"}]}),
    ], limits()).await;
    let McpCatalogEntries::Resources(entries) = result.unwrap() else {
        panic!("resources");
    };
    assert_eq!(
        entries.iter().map(|r| r.uri.as_str()).collect::<Vec<_>>(),
        ["file:///must-not-open", "notes://host/%61?x=1#part"]
    );
    assert_eq!(sent[3]["params"]["cursor"], "opaque/+=?");
    assert_eq!(
        sent.iter()
            .filter_map(|m| m["method"].as_str())
            .collect::<Vec<_>>(),
        [
            "initialize",
            "notifications/initialized",
            "resources/list",
            "resources/list"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn rejects_partial_ambiguous_oversize_unsupported_and_silent_catalogues() {
    use McpCatalogError::*;
    let resource = json!({"name":"note","uri":"notes:///a"});
    let cases = [
        (
            vec![
                json!({"resources":[resource.clone()],"nextCursor":"next"}),
                json!({"resources":[resource.clone()]}),
            ],
            InvalidCatalog,
        ),
        (
            vec![
                json!({"resources":[],"nextCursor":"same"}),
                json!({"resources":[],"nextCursor":"same"}),
            ],
            InvalidCatalog,
        ),
        (
            vec![
                json!({"resources":[],"nextCursor":"a"}),
                json!({"resources":[],"nextCursor":"b"}),
            ],
            LimitExceeded,
        ),
        (
            vec![json!({"resources":[{"name":"note","uri":"relative"}]})],
            InvalidUri,
        ),
        (
            vec![json!({"resources":[resource.clone(),resource.clone(),resource.clone()]})],
            LimitExceeded,
        ),
        (
            vec![
                json!({"resources":[{"name":"note","uri":"notes:///a","description":"x".repeat(4096)}]}),
            ],
            LimitExceeded,
        ),
    ];
    for (pages, expected) in cases {
        let (result, _) = discover(
            McpCatalogKind::Resources,
            json!({"resources":{}}),
            pages,
            limits(),
        )
        .await;
        assert_eq!(result.err(), Some(StdioCallError::Catalog(expected)));
    }
    let (result, sent) = discover(McpCatalogKind::Resources, json!({}), vec![], limits()).await;
    assert_eq!(result.err(), Some(StdioCallError::Catalog(Unsupported)));
    assert_eq!(sent.len(), 2);
    let (result, _) = discover(
        McpCatalogKind::Resources,
        json!({"resources":{}}),
        vec![],
        limits(),
    )
    .await;
    assert_eq!(result.err(), Some(StdioCallError::Interrupted));
    let (result, _) = discover(McpCatalogKind::ResourceTemplates, json!({"resources":{}}), vec![json!({"resourceTemplates":[{"name":"a","uriTemplate":"notes:///{id}"},{"name":"b","uriTemplate":"notes:///{id}"}]})], limits()).await;
    assert_eq!(result.err(), Some(StdioCallError::Catalog(InvalidCatalog)));
}
