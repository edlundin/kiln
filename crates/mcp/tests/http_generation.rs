use kiln_mcp::{
    McpHttpGenerationConfig, McpHttpLimits, ProtocolPolicy, ProtocolVersion,
    http_generation_transport, start_stdio_client,
};
use rmcp::{ClientLifecycleMode, model::DiscoverResult, serve_client_with_lifecycle};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Clone, Copy)]
enum Mode {
    JsonCatalog,
    SseCatalog,
    BrokenStream,
    CorruptStream,
    Expired,
    AcceptedRequest,
    UncorrelatedJson,
    LegacyResume,
}

struct Fixture {
    endpoint: String,
    received: Arc<Mutex<Vec<(String, Value)>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn catalog() -> Value {
    json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]})
}

impl Fixture {
    async fn new(mode: Mode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
        let received = Arc::new(Mutex::new(Vec::new()));
        let requests = received.clone();
        let task = tokio::spawn(async move {
            let mut pending_id = Value::Null;
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut byte = [0; 1];
                while !request.ends_with(b"\r\n\r\n") {
                    socket.read_exact(&mut byte).await.unwrap();
                    request.push(byte[0]);
                    assert!(request.len() < 8192);
                }
                let headers = String::from_utf8(request).unwrap();
                let method = headers.split_whitespace().next().unwrap().to_owned();
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .map(str::to_owned)
                    })
                    .map(|value| value.parse().unwrap())
                    .unwrap_or(0);
                let mut body = vec![0; length];
                socket.read_exact(&mut body).await.unwrap();
                let mut message: Value = if body.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_slice(&body).unwrap()
                };
                let resume = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("last-event-id: "));
                if let Some(cursor) = resume {
                    message = json!({"last_event_id":cursor});
                }
                requests
                    .lock()
                    .unwrap()
                    .push((method.clone(), message.clone()));
                let mut status = "200 OK";
                let mut extra = "";
                let mut mime = "application/json";
                let body = if method == "GET"
                    && matches!(mode, Mode::LegacyResume)
                    && resume.is_some()
                {
                    mime = "text/event-stream";
                    let result = json!({"jsonrpc":"2.0","id":pending_id,"result":catalog()});
                    format!("data: {result}\n\n")
                } else if method == "GET" {
                    status = "405 Method Not Allowed";
                    String::new()
                } else if method == "DELETE" {
                    String::new()
                } else {
                    match message["method"].as_str().unwrap() {
                    "server/discover" => json!({"jsonrpc":"2.0","id":message["id"],"result":DiscoverResult::new(vec![rmcp::model::ProtocolVersion::V_2026_07_28],Default::default())}).to_string(),
                    "initialize" => {
                        extra = "Mcp-Session-Id: fixture-session\r\n";
                        json!({"jsonrpc":"2.0","id":message["id"],"result":{"protocolVersion":message["params"]["protocolVersion"],"capabilities":{},"serverInfo":{"name":"fixture","version":"1"}}}).to_string()
                    },
                    "notifications/initialized" | "notifications/cancelled" => { status = "202 Accepted"; String::new() },
                        "tools/list" | "ping" => match mode {
                            Mode::UncorrelatedJson => {
                                json!({"jsonrpc":"2.0","id":"unrelated","result":catalog()}).to_string()
                            }
                            Mode::AcceptedRequest => {
                                status = "202 Accepted";
                                String::new()
                            }
                            Mode::LegacyResume => {
                                pending_id = message["id"].clone();
                                mime = "text/event-stream";
                                "id: cursor\nretry: 0\n\n".to_owned()
                            },
                        Mode::Expired => { status = "404 Not Found"; String::new() },
                        Mode::BrokenStream => { mime = "text/event-stream"; "id: cursor\nretry: 0\n\ndata: {incomplete".to_owned() },
                        Mode::CorruptStream => { mime = "text/event-stream"; "id: cursor\n\ndata: secret-invalid-json\n\n".to_owned() },
                        Mode::JsonCatalog | Mode::SseCatalog => {
                            let response = json!({"jsonrpc":"2.0","id":message["id"],"result":catalog()});
                            if matches!(mode, Mode::SseCatalog) { mime = "text/event-stream"; format!("data: {response}\n\n") }
                            else { response.to_string() }
                        },
                    },
                    method => panic!("unexpected fixture method: {method}"),
                }
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nConnection: close\r\nContent-Type: {mime}\r\nContent-Length: {}\r\n{extra}\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
                socket.shutdown().await.unwrap();
            }
        });
        Self {
            endpoint,
            received,
            task,
        }
    }

    fn config(&self, protocol: ProtocolVersion) -> McpHttpGenerationConfig {
        McpHttpGenerationConfig {
            endpoint: self.endpoint.clone(),
            protocol,
            io: McpHttpLimits {
                max_request_bytes: NonZeroUsize::new(4096).unwrap(),
                max_response_bytes: NonZeroUsize::new(4096).unwrap(),
                max_stream_bytes: NonZeroUsize::new(4096).unwrap(),
                max_event_bytes: NonZeroUsize::new(4096).unwrap(),
                max_header_bytes: NonZeroUsize::new(4096).unwrap(),
                request_timeout: Duration::from_secs(5),
            },
            bearer_token: None,
            headers: HashMap::new(),
            channel_capacity: NonZeroUsize::new(4).unwrap(),
            max_exchanges: NonZeroUsize::new(16).unwrap(),
            max_catalog_lifetime_bytes: NonZeroUsize::new(4096).unwrap(),
            legacy_resume_delay: None,
        }
    }
}

#[tokio::test]
async fn http_generation_legacy_resume_requires_policy_and_preserves_cursor() {
    for resume in [true, false] {
        let fixture = Fixture::new(Mode::LegacyResume).await;
        let protocol = ProtocolVersion::V20251125;
        let mut config = fixture.config(protocol);
        if resume {
            config.legacy_resume_delay = Some(Duration::ZERO);
        }
        let client = start_stdio_client(
            (),
            http_generation_transport(config).unwrap(),
            ProtocolPolicy::Pinned(protocol),
        )
        .await
        .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), client.peer().list_tools(None))
            .await
            .unwrap();
        assert_eq!(result.is_ok(), resume);
        client.cancel().await.unwrap();
        let requests = fixture.received.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|(_, value)| value["method"] == "tools/list")
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|(method, value)| method == "GET" && value["last_event_id"] == "cursor")
                .count(),
            usize::from(resume)
        );
    }
}

fn modern() -> ClientLifecycleMode {
    ClientLifecycleMode::Discover {
        preferred_versions: vec![rmcp::model::ProtocolVersion::V_2026_07_28],
    }
}

#[tokio::test]
async fn http_generation_modern_invalid_responses_never_resume_or_continue() {
    for mode in [
        Mode::BrokenStream,
        Mode::CorruptStream,
        Mode::AcceptedRequest,
        Mode::UncorrelatedJson,
    ] {
        let fixture = Fixture::new(mode).await;
        let transport =
            http_generation_transport(fixture.config(ProtocolVersion::V20260728)).unwrap();
        let client = serve_client_with_lifecycle((), transport, modern())
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "{error:?}; fixture requests: {:?}",
                    fixture.received.lock().unwrap()
                )
            });
        assert!(
            tokio::time::timeout(Duration::from_secs(5), client.peer().list_tools(None))
                .await
                .unwrap()
                .is_err()
        );
        assert!(client.peer().list_tools(None).await.is_err());
        client.cancel().await.unwrap();
        let requests = fixture.received.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|(method, _)| method == "POST"));
        assert_eq!(requests[0].1["method"], "server/discover");
        assert_eq!(requests[1].1["method"], "tools/list");
    }
}

#[tokio::test]
async fn http_generation_catalogue_and_exchange_budgets_retire_before_more_io() {
    for mode in [Mode::JsonCatalog, Mode::SseCatalog] {
        let fixture = Fixture::new(mode).await;
        let mut config = fixture.config(ProtocolVersion::V20260728);
        config.max_catalog_lifetime_bytes = NonZeroUsize::new(catalog().to_string().len()).unwrap();
        let client =
            serve_client_with_lifecycle((), http_generation_transport(config).unwrap(), modern())
                .await
                .unwrap();
        assert_eq!(client.peer().list_tools(None).await.unwrap().tools.len(), 1);
        assert!(client.peer().list_tools(None).await.is_err());
        assert!(client.peer().list_tools(None).await.is_err());
        client.cancel().await.unwrap();
        assert_eq!(fixture.received.lock().unwrap().len(), 3);
    }
    let fixture = Fixture::new(Mode::JsonCatalog).await;
    let mut config = fixture.config(ProtocolVersion::V20260728);
    config.max_exchanges = NonZeroUsize::new(1).unwrap();
    let client =
        serve_client_with_lifecycle((), http_generation_transport(config).unwrap(), modern())
            .await
            .unwrap();
    assert!(client.peer().list_tools(None).await.is_err());
    client.cancel().await.unwrap();
    assert_eq!(fixture.received.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn http_generation_legacy_expiry_never_reinitializes_or_replays() {
    let fixture = Fixture::new(Mode::Expired).await;
    let protocol = ProtocolVersion::V20251125;
    // The existing pinned handshake is transport-generic; Auto's stdio fallback
    // rule is deliberately not used for HTTP.
    let client = start_stdio_client(
        (),
        http_generation_transport(fixture.config(protocol)).unwrap(),
        ProtocolPolicy::Pinned(protocol),
    )
    .await
    .unwrap();
    assert!(client.peer().list_tools(None).await.is_err());
    assert!(client.peer().list_tools(None).await.is_err());
    client.cancel().await.unwrap();
    let requests = fixture.received.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|(_, value)| value["method"] == "initialize")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|(_, value)| value["method"] == "tools/list")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|(method, _)| method == "DELETE")
            .count(),
        1
    );
}
