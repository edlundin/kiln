use std::{collections::HashMap, num::NonZeroUsize, sync::Arc, time::Duration};

use futures_util::StreamExt;
use kiln_mcp::{BoundedHttpClient, McpHttpError, McpHttpLimits};
use rmcp::{
    model::ClientJsonRpcMessage,
    transport::streamable_http_client::{
        StreamableHttpClient, StreamableHttpError, StreamableHttpPostResponse,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn limits() -> McpHttpLimits {
    McpHttpLimits {
        max_request_bytes: NonZeroUsize::new(512).unwrap(),
        max_response_bytes: NonZeroUsize::new(128).unwrap(),
        max_stream_bytes: NonZeroUsize::new(512).unwrap(),
        max_event_bytes: NonZeroUsize::new(128).unwrap(),
        max_header_bytes: NonZeroUsize::new(512).unwrap(),
        // Local fixtures should complete immediately; this bounds a broken test.
        request_timeout: Duration::from_secs(5),
    }
}

fn ping() -> ClientJsonRpcMessage {
    serde_json::from_str(r#"{"jsonrpc":"2.0","id":7,"method":"ping"}"#).unwrap()
}

async fn fixture(response: String) -> (Arc<str>, tokio::task::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/mcp?private-query",
        listener.local_addr().unwrap()
    )
    .into();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut byte = [0; 1];
        while !request.ends_with(b"\r\n\r\n") {
            socket.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
            assert!(request.len() < 4096);
        }
        let headers = String::from_utf8(request.clone()).unwrap();
        let length: usize = headers
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
        request.extend(body);
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.shutdown().await.unwrap();
        String::from_utf8(request).unwrap()
    });
    (endpoint, task)
}

fn response(status: &str, headers: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: {}\r\n{headers}\r\n{body}",
        body.len()
    )
}

#[tokio::test]
async fn http_json_and_sse_round_trip() {
    let (uri, server) = fixture(response(
        "200 OK",
        "Content-Type: Application/Json; charset=utf-8\r\nMcp-Session-Id: session\r\n",
        r#"{"jsonrpc":"2.0","id":7,"result":{}}"#,
    ))
    .await;
    let client = BoundedHttpClient::new(&uri, limits()).unwrap();
    let reply = client
        .post_message(
            uri,
            ping(),
            None,
            Some("private-token".into()),
            HashMap::new(),
        )
        .await
        .unwrap();
    assert!(
        matches!(reply, StreamableHttpPostResponse::Json(_, Some(session)) if session == "session")
    );
    let request = server.await.unwrap();
    assert!(request.contains("authorization: Bearer private-token\r\n"));
    assert!(request.ends_with(r#"{"jsonrpc":"2.0","id":7,"method":"ping"}"#));

    let body =
        "id: first\r\n\r\nevent: ignored\nevent: message\nunknown: ignore\ndata: {\ndata: }\n\n";
    let (uri, server) = fixture(response(
        "200 OK",
        "Content-Type: text/event-stream\r\n",
        body,
    ))
    .await;
    let client = BoundedHttpClient::new(&uri, limits()).unwrap();
    let mut stream = client
        .get_stream(
            uri,
            Some("session".into()),
            Some("cursor".into()),
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        stream.next().await.unwrap().unwrap().id.as_deref(),
        Some("first")
    );
    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(event.event.as_deref(), Some("message"));
    assert_eq!(event.data.as_deref(), Some("{\n}"));
    assert!(stream.next().await.is_none());
    let request = server.await.unwrap();
    assert!(request.starts_with("GET "));
    assert!(request.contains("mcp-session-id: session\r\n"));
    assert!(request.contains("last-event-id: cursor\r\n"));
}

#[tokio::test]
async fn http_bounds_chunked_bodies_and_sse_and_sanitizes_errors() {
    // Chunked transfer has no Content-Length preflight. The actual read is bounded.
    let body = "x".repeat(129);
    let (uri, server) = fixture(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{body}\r\n0\r\n\r\n", body.len())).await;
    let client = BoundedHttpClient::new(&uri, limits()).unwrap();
    assert!(matches!(
        client
            .post_message(uri, ping(), None, None, HashMap::new())
            .await,
        Err(StreamableHttpError::Client(McpHttpError::BodyLimit))
    ));
    server.await.unwrap();

    for (body, max_event, max_stream) in [
        ("data: secret-too-long\n\n".to_owned(), 8, 512),
        (": heartbeat\n\n".repeat(20), 128, 16),
    ] {
        let (uri, server) = fixture(response(
            "200 OK",
            "Content-Type: text/event-stream\r\n",
            &body,
        ))
        .await;
        let mut allowance = limits();
        allowance.max_stream_bytes = NonZeroUsize::new(max_stream).unwrap();
        let client = BoundedHttpClient::new(&uri, allowance).unwrap();
        let result = client
            .post_message_with_max_sse_event_size(
                uri,
                ping(),
                None,
                None,
                HashMap::new(),
                max_event,
            )
            .await
            .unwrap();
        let StreamableHttpPostResponse::Sse(mut stream, _) = result else {
            panic!("expected SSE");
        };
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("BodyLimit"));
        assert!(stream.next().await.is_none());
        server.await.unwrap();
    }

    for (headers, body) in [
        ("Content-Type: application/json\r\n", "secret-invalid-json"),
        ("Content-Type: application/json-leak\r\n", "secret-body"),
    ] {
        let (uri, server) = fixture(response("200 OK", headers, body)).await;
        let client = BoundedHttpClient::new(&uri, limits()).unwrap();
        let error = client
            .post_message(uri, ping(), None, None, HashMap::new())
            .await
            .err()
            .unwrap();
        assert_eq!(error.to_string(), "Client error: MCP HTTP InvalidResponse");
        assert!(!format!("{error:?}").contains("secret"));
        server.await.unwrap();
    }
}

#[tokio::test]
async fn http_does_not_redirect_replay_or_recorrelate_errors() {
    let redirect_target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (uri, server) = fixture(response(
        "307 Temporary Redirect",
        &format!(
            "Location: http://{}/leak\r\n",
            redirect_target.local_addr().unwrap()
        ),
        "secret-error",
    ))
    .await;
    let client = BoundedHttpClient::new(&uri, limits()).unwrap();
    assert!(matches!(
        client
            .post_message(
                uri,
                ping(),
                None,
                Some("private-token".into()),
                HashMap::new()
            )
            .await,
        Err(StreamableHttpError::Client(McpHttpError::HttpStatus(307)))
    ));
    server.await.unwrap();
    // An unpolled listener can prove no redirect connection was queued.
    assert!(
        tokio::time::timeout(Duration::from_millis(20), redirect_target.accept())
            .await
            .is_err()
    );

    let (uri, server) = fixture(response("404 Not Found", "", "private-error")).await;
    let client = BoundedHttpClient::new(&uri, limits()).unwrap();
    assert!(matches!(
        client
            .post_message(uri, ping(), Some("expired".into()), None, HashMap::new())
            .await,
        Err(StreamableHttpError::SessionExpired)
    ));
    server.await.unwrap();

    for id in ["7", "8"] {
        let body = format!(
            r#"{{"jsonrpc":"2.0","id":{id},"error":{{"code":-32601,"message":"method not found"}}}}"#
        );
        let (uri, server) = fixture(response(
            "400 Bad Request",
            "Content-Type: application/json\r\n",
            &body,
        ))
        .await;
        let client = BoundedHttpClient::new(&uri, limits()).unwrap();
        let result = client
            .post_message(uri, ping(), None, None, HashMap::new())
            .await;
        if id == "7" {
            assert!(matches!(result, Ok(StreamableHttpPostResponse::Json(_, _))));
        } else {
            assert!(matches!(
                result,
                Err(StreamableHttpError::Client(McpHttpError::HttpStatus(400)))
            ));
        }
        server.await.unwrap();
    }
}

#[tokio::test]
async fn http_preflight_rejects_unapproved_destination_headers_and_request_size() {
    for endpoint in [
        "http://example.org/mcp",
        "https://user:secret@example.org/mcp",
        "https://example.org/mcp#fragment",
    ] {
        assert!(matches!(
            BoundedHttpClient::new(endpoint, limits()),
            Err(McpHttpError::InvalidEndpoint)
        ));
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri: Arc<str> = format!("http://{}/mcp", listener.local_addr().unwrap()).into();
    let client = BoundedHttpClient::new(&uri, limits()).unwrap();
    assert!(matches!(
        client
            .post_message(
                format!("{uri}/other").into(),
                ping(),
                None,
                None,
                HashMap::new()
            )
            .await,
        Err(StreamableHttpError::Client(McpHttpError::EndpointMismatch))
    ));
    let headers = HashMap::from([(
        http::header::HOST,
        http::HeaderValue::from_static("another-host"),
    )]);
    assert!(matches!(
        client
            .post_message(uri.clone(), ping(), None, None, headers)
            .await,
        Err(StreamableHttpError::Client(McpHttpError::InvalidHeader))
    ));
    let mut allowance = limits();
    allowance.max_request_bytes = NonZeroUsize::new(1).unwrap();
    let client = BoundedHttpClient::new(&uri, allowance).unwrap();
    assert!(matches!(
        client
            .post_message(uri, ping(), None, None, HashMap::new())
            .await,
        Err(StreamableHttpError::Client(McpHttpError::BodyLimit))
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn http_header_budget_delete_and_deadline() {
    let (uri, server) = fixture(response(
        "200 OK",
        &format!(
            "Content-Type: application/json\r\nX-Private: {}\r\n",
            "s".repeat(512)
        ),
        "{}",
    ))
    .await;
    let client = BoundedHttpClient::new(&uri, limits()).unwrap();
    assert!(matches!(
        client
            .post_message(uri, ping(), None, None, HashMap::new())
            .await,
        Err(StreamableHttpError::Client(McpHttpError::HeaderLimit))
    ));
    server.await.unwrap();

    let (uri, server) = fixture(response("405 Method Not Allowed", "", "")).await;
    let client = BoundedHttpClient::new(&uri, limits()).unwrap();
    client
        .delete_session(uri, "session".into(), None, HashMap::new())
        .await
        .unwrap();
    assert!(server.await.unwrap().starts_with("DELETE "));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri: Arc<str> = format!("http://{}/mcp", listener.local_addr().unwrap()).into();
    let mut allowance = limits();
    // Deliberately stalled local response: exercise the configured deadline, not
    // a network latency expectation. The outer timeout bounds a broken assertion.
    allowance.request_timeout = Duration::from_millis(25);
    let client = BoundedHttpClient::new(&uri, allowance).unwrap();
    let (result, socket) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            client.post_message(uri, ping(), None, None, HashMap::new()),
            listener.accept()
        )
    })
    .await
    .unwrap();
    let _socket = socket.unwrap();
    assert!(matches!(
        result,
        Err(StreamableHttpError::Client(McpHttpError::Network))
    ));
}
