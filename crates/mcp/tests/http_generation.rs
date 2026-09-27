use kiln_mcp::{
    McpHttpGenerationConfig, McpHttpLimits, McpHttpStartError, ProtocolPolicy, ProtocolVersion,
    http_generation_transport, start_http_client, start_managed_http_client, start_stdio_client,
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
    Startup(StartupCase),
}

#[derive(Clone, Copy, Debug)]
enum StartupCase {
    ModernSse,
    UnsupportedJson,
    UnsupportedSse,
    Auth,
    ServerFailure,
    PlainBadRequest,
    WrongId,
    WrongRequested,
    MethodNotFound,
    IncompatibleDiscovery,
    Malformed,
    Stall,
}

fn discovery_reply(case: StartupCase, id: &Value) -> (&'static str, &'static str, String) {
    use StartupCase::*;
    let mut response = json!({"jsonrpc":"2.0","id":id,"error":{"code":-32022,"message":"fixture","data":{"requested":"2026-07-28","supported":["2025-03-26","2025-11-25"]}}});
    match case {
        ModernSse | IncompatibleDiscovery => {
            let version = if matches!(case, ModernSse) {
                rmcp::model::ProtocolVersion::V_2026_07_28
            } else {
                rmcp::model::ProtocolVersion::V_2025_11_25
            };
            response = json!({"jsonrpc":"2.0","id":id,"result":DiscoverResult::new(vec![version],Default::default())});
        }
        WrongId => response["id"] = json!("unrelated"),
        WrongRequested => response["error"]["data"]["requested"] = json!("2099-01-01"),
        MethodNotFound => response["error"]["code"] = json!(-32601),
        PlainBadRequest => {
            return (
                "400 Bad Request",
                "text/plain",
                "private-rejection".to_owned(),
            );
        }
        Malformed => {
            return (
                "400 Bad Request",
                "application/json",
                "private-malformed-body".to_owned(),
            );
        }
        _ => {}
    }
    if matches!(case, ModernSse | UnsupportedSse) {
        return (
            "200 OK",
            "text/event-stream",
            format!("id: startup\n\ndata: {response}\n\n"),
        );
    }
    let status = match case {
        Auth => "401 Unauthorized",
        ServerFailure => "500 Internal Server Error",
        IncompatibleDiscovery => "200 OK",
        _ => "400 Bad Request",
    };
    (status, "application/json", response.to_string())
}

struct Fixture {
    endpoint: String,
    received: Arc<Mutex<Vec<(String, Value)>>>,
    task: tokio::task::JoinHandle<()>,
    release: Option<Arc<tokio::sync::Notify>>,
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
        let release = matches!(mode, Mode::Startup(StartupCase::Stall))
            .then(|| Arc::new(tokio::sync::Notify::new()));
        let gate = release.clone();
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
                        "server/discover" => {
                            if let Some(gate) = &gate {
                                gate.notified().await;
                            }
                            if let Mode::Startup(case) = mode {
                                let reply = discovery_reply(case, &message["id"]);
                                status = reply.0;
                                mime = reply.1;
                                reply.2
                            } else {
                                json!({"jsonrpc":"2.0","id":message["id"],"result":DiscoverResult::new(vec![rmcp::model::ProtocolVersion::V_2026_07_28],Default::default())}).to_string()
                            }
                        }
                        "initialize" => {
                            extra = "Mcp-Session-Id: fixture-session\r\n";
                            json!({"jsonrpc":"2.0","id":message["id"],"result":{"protocolVersion":message["params"]["protocolVersion"],"capabilities":{},"serverInfo":{"name":"fixture","version":"1"}}}).to_string()
                        }
                        "notifications/initialized" | "notifications/cancelled" => {
                            status = "202 Accepted";
                            String::new()
                        }
                        "tools/list" | "ping" => match mode {
                            Mode::UncorrelatedJson => {
                                json!({"jsonrpc":"2.0","id":"unrelated","result":catalog()})
                                    .to_string()
                            }
                            Mode::AcceptedRequest => {
                                status = "202 Accepted";
                                String::new()
                            }
                            Mode::LegacyResume => {
                                pending_id = message["id"].clone();
                                mime = "text/event-stream";
                                "id: cursor\nretry: 0\n\n".to_owned()
                            }
                            Mode::Expired => {
                                status = "404 Not Found";
                                String::new()
                            }
                            Mode::BrokenStream => {
                                mime = "text/event-stream";
                                "id: cursor\nretry: 0\n\ndata: {incomplete".to_owned()
                            }
                            Mode::CorruptStream => {
                                mime = "text/event-stream";
                                "id: cursor\n\ndata: secret-invalid-json\n\n".to_owned()
                            }
                            Mode::JsonCatalog | Mode::SseCatalog | Mode::Startup(_) => {
                                let response =
                                    json!({"jsonrpc":"2.0","id":message["id"],"result":catalog()});
                                if matches!(mode, Mode::SseCatalog) {
                                    mime = "text/event-stream";
                                    format!("data: {response}\n\n")
                                } else {
                                    response.to_string()
                                }
                            }
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
            release,
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
async fn http_startup_modern_pins_and_structured_fallback() {
    for case in [
        StartupCase::ModernSse,
        StartupCase::UnsupportedJson,
        StartupCase::UnsupportedSse,
    ] {
        let fixture = Fixture::new(Mode::Startup(case)).await;
        let mut config = fixture.config(ProtocolVersion::V20260728);
        config.legacy_resume_delay = Some(Duration::ZERO);
        let client = start_http_client(
            (),
            config,
            ProtocolPolicy::Auto,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        let expected = if matches!(case, StartupCase::ModernSse) {
            "2026-07-28"
        } else {
            "2025-11-25"
        };
        assert_eq!(
            client.peer_info().unwrap().protocol_version.as_str(),
            expected
        );
        client.cancel().await.unwrap();
        let requests = fixture.received.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|(_, value)| value["method"] == "server/discover")
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|(_, value)| value["method"] == "initialize")
                .count(),
            usize::from(expected != "2026-07-28")
        );
    }
    for protocol in [
        ProtocolVersion::V20250326,
        ProtocolVersion::V20250618,
        ProtocolVersion::V20251125,
        ProtocolVersion::V20260728,
    ] {
        let fixture = Fixture::new(Mode::JsonCatalog).await;
        let client = start_http_client(
            (),
            fixture.config(protocol),
            ProtocolPolicy::Pinned(protocol),
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(
            client.peer_info().unwrap().protocol_version.as_str(),
            protocol.as_str()
        );
        client.cancel().await.unwrap();
        let requests = fixture.received.lock().unwrap();
        assert_eq!(
            requests[0].1["method"],
            if protocol == ProtocolVersion::V20260728 {
                "server/discover"
            } else {
                "initialize"
            }
        );
    }
}

#[tokio::test]
async fn http_startup_never_downgrades_without_exact_evidence() {
    for case in [
        StartupCase::Auth,
        StartupCase::ServerFailure,
        StartupCase::PlainBadRequest,
        StartupCase::WrongId,
        StartupCase::WrongRequested,
        StartupCase::MethodNotFound,
        StartupCase::IncompatibleDiscovery,
        StartupCase::Malformed,
    ] {
        let fixture = Fixture::new(Mode::Startup(case)).await;
        let result = start_http_client(
            (),
            fixture.config(ProtocolVersion::V20260728),
            ProtocolPolicy::Auto,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await;
        assert!(
            matches!(result, Err(McpHttpStartError::Negotiation)),
            "{case:?}"
        );
        assert_eq!(fixture.received.lock().unwrap().len(), 1, "{case:?}");
    }
    let fixture = Fixture::new(Mode::Startup(StartupCase::UnsupportedJson)).await;
    assert!(matches!(
        start_http_client(
            (),
            fixture.config(ProtocolVersion::V20260728),
            ProtocolPolicy::Pinned(ProtocolVersion::V20260728),
            tokio::time::Instant::now() + Duration::from_secs(5)
        )
        .await,
        Err(McpHttpStartError::Negotiation)
    ));
    assert_eq!(fixture.received.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn http_startup_deadline_waits_for_worker_shutdown() {
    let fixture = Fixture::new(Mode::Startup(StartupCase::Stall)).await;
    let config = fixture.config(ProtocolVersion::V20260728);
    // The response is deliberately gated past this deadline, but within the
    // fixture's five-second I/O timeout. Returning early would detach the worker.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let startup = tokio::spawn(start_http_client(
        (),
        config,
        ProtocolPolicy::Auto,
        deadline,
    ));
    tokio::time::sleep_until(deadline + Duration::from_millis(10)).await;
    assert_eq!(fixture.received.lock().unwrap().len(), 1);
    assert!(
        !startup.is_finished(),
        "startup returned before the in-flight worker joined"
    );
    fixture.release.as_ref().unwrap().notify_one();
    let result = tokio::time::timeout(Duration::from_secs(5), startup)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(McpHttpStartError::Deadline)));
    assert_eq!(fixture.received.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn managed_http_cleanup_tracks_unpolled_cancelled_and_successful_startup() {
    // Dropping before polling cannot create a worker or an HTTP exchange.
    let fixture = Fixture::new(Mode::Startup(StartupCase::ModernSse)).await;
    let (startup, mut cleanup) = start_managed_http_client(
        (),
        fixture.config(ProtocolVersion::V20260728),
        ProtocolPolicy::Auto,
        tokio::time::Instant::now() + Duration::from_secs(5),
    );
    drop(startup);
    cleanup.finish().await.unwrap();
    assert!(fixture.received.lock().unwrap().is_empty());

    // A dropped startup retains evidence for the in-flight worker. Cancelling
    // a cleanup waiter must not consume and lose that worker's receiver.
    let fixture = Fixture::new(Mode::Startup(StartupCase::Stall)).await;
    let (startup, mut cleanup) = start_managed_http_client(
        (),
        fixture.config(ProtocolVersion::V20260728),
        ProtocolPolicy::Auto,
        tokio::time::Instant::now() + Duration::from_secs(5),
    );
    let startup = tokio::spawn(startup);
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.received.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    startup.abort();
    assert!(startup.await.unwrap_err().is_cancelled());
    {
        let finishing = cleanup.finish();
        tokio::pin!(finishing);
        assert!(futures_util::poll!(&mut finishing).is_pending());
    }
    fixture.release.as_ref().unwrap().notify_one();
    tokio::time::timeout(Duration::from_secs(5), cleanup.finish())
        .await
        .unwrap()
        .unwrap();
    cleanup.finish().await.unwrap();
    assert_eq!(fixture.received.lock().unwrap().len(), 1);

    // A success keeps its cleanup owner alive through client cancellation,
    // including when startup traversed both modern and legacy attempts.
    for case in [StartupCase::ModernSse, StartupCase::UnsupportedJson] {
        let fixture = Fixture::new(Mode::Startup(case)).await;
        let (startup, mut cleanup) = start_managed_http_client(
            (),
            fixture.config(ProtocolVersion::V20260728),
            ProtocolPolicy::Auto,
            tokio::time::Instant::now() + Duration::from_secs(5),
        );
        let client = startup.await.unwrap();
        {
            let finishing = cleanup.finish();
            tokio::pin!(finishing);
            assert!(futures_util::poll!(&mut finishing).is_pending());
        }
        client.cancel().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), cleanup.finish())
            .await
            .unwrap()
            .unwrap();
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
