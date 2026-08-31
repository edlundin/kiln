use std::{
    io::BufRead,
    path::{Path, PathBuf},
    process::Stdio,
};

use futures_util::{SinkExt, StreamExt};
use kiln_protocol::{
    AppendMessageRequest, ClientIdentity, CreateWorkspaceRequest,
    DETERMINISTIC_SUBPROCESS_CAPABILITY, EVENTS_WEBSOCKET_PATH, IDEMPOTENCY_KEY_HEADER,
    MessageResponse, NEGOTIATE_PATH, NegotiateRequest, NegotiateResponse, PROTOCOL_VERSION,
    ProblemDetails, RUN_PATH, RunResponse, RunState, SESSION_EVENTS_PATH, SESSION_MESSAGES_PATH,
    SESSION_PATH, SESSION_RUNS_PATH, SessionEventDataResponse, SessionEventResponse,
    SessionEventsResponse, SessionResponse, ToolCallState, WEBSOCKET_CAPABILITY, WORKSPACE_PATH,
    WORKSPACE_SESSIONS_PATH, WORKSPACES_PATH, WebSocketFrame, WorkspaceResponse,
    WorkspaceRootRequest, error_code,
};
use reqwest::StatusCode;
use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::Message};

struct Daemon {
    child: std::process::Child,
    address: String,
}

impl Daemon {
    fn start(binary: &str, data_directory: &Path) -> Self {
        Self::start_with_outcome(binary, data_directory, None)
    }

    fn start_with_outcome(binary: &str, data_directory: &Path, outcome: Option<&str>) -> Self {
        let mut command = std::process::Command::new(binary);
        command
            .env("KILN_LISTEN_ADDR", "127.0.0.1:0")
            .env("KILN_DATA_DIR", data_directory);
        if let Some(outcome) = outcome {
            command.env("KILN_DETERMINISTIC_SUBPROCESS_OUTCOME", outcome);
        }
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("daemon starts");
        let stdout = child.stdout.take().expect("daemon stdout is piped");
        let mut lines = std::io::BufReader::new(stdout).lines();
        let readiness: Value = serde_json::from_str(
            &lines
                .next()
                .expect("daemon readiness line")
                .expect("daemon readiness output"),
        )
        .expect("readiness is JSON");
        Self {
            child,
            address: readiness["address"]
                .as_str()
                .expect("readiness address")
                .to_owned(),
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn git_repository(parent: &Path, name: &str) -> PathBuf {
    let repository = parent.join(name);
    std::fs::create_dir(&repository).expect("repository directory");
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(&repository)
        .args(["init", "--quiet"])
        .output()
        .expect("git init runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    repository
}

fn canonical_string(path: &Path) -> String {
    std::fs::canonicalize(path)
        .expect("path is canonicalizable")
        .to_str()
        .expect("test path is UTF-8")
        .to_owned()
}

async fn create_run_session(
    http: &reqwest::Client,
    address: &str,
    repository_parent: &Path,
) -> SessionResponse {
    let repository = git_repository(repository_parent, "run");
    let response = http
        .post(format!("http://{address}{WORKSPACES_PATH}"))
        .json(&CreateWorkspaceRequest {
            name: "Run test Workspace".to_owned(),
            roots: vec![WorkspaceRootRequest {
                name: "run".to_owned(),
                path: canonical_string(&repository),
            }],
        })
        .send()
        .await
        .expect("Run test Workspace response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let workspace: WorkspaceResponse = response.json().await.expect("Run test Workspace JSON");

    let path = WORKSPACE_SESSIONS_PATH.replace("{workspace_id}", &workspace.workspace_id);
    let response = http
        .post(format!("http://{address}{path}"))
        .send()
        .await
        .expect("Run test Session response");
    assert_eq!(response.status(), StatusCode::CREATED);
    response.json().await.expect("Run test Session JSON")
}

fn event_kind(event: &SessionEventResponse) -> &'static str {
    match &event.event {
        SessionEventDataResponse::SessionCreated { .. } => "session.created",
        SessionEventDataResponse::MessageAppended { .. } => "message.appended",
        SessionEventDataResponse::RunCreated { .. } => "run.created",
        SessionEventDataResponse::RunStateChanged { .. } => "run.state_changed",
        SessionEventDataResponse::ToolCallRequested { .. } => "tool_call.requested",
        SessionEventDataResponse::ToolCallStateChanged { .. } => "tool_call.state_changed",
        SessionEventDataResponse::ToolCallOutput { .. } => "tool_call.output",
    }
}

type EventSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn open_event_socket(address: &str) -> EventSocket {
    open_event_socket_after(address, None).await.0
}

async fn open_event_socket_after(
    address: &str,
    after: Option<&str>,
) -> (EventSocket, Option<String>) {
    let mut endpoint = format!(
        "ws://{address}{EVENTS_WEBSOCKET_PATH}?version={PROTOCOL_VERSION}&capability={WEBSOCKET_CAPABILITY}"
    );
    if let Some(after) = after {
        endpoint.push_str("&after=");
        endpoint.push_str(after);
    }
    let (mut socket, _) = connect_async(endpoint)
        .await
        .expect("event WebSocket connects");
    let frame = socket
        .next()
        .await
        .expect("event WebSocket acknowledgement")
        .expect("event WebSocket acknowledgement frame");
    let frame: WebSocketFrame = serde_json::from_str(
        frame
            .into_text()
            .expect("event WebSocket acknowledgement is text")
            .as_ref(),
    )
    .expect("event WebSocket acknowledgement JSON");
    let WebSocketFrame::Ack {
        version,
        capability,
        current_event_cursor,
    } = frame
    else {
        panic!("expected event WebSocket acknowledgement");
    };
    assert_eq!(version, PROTOCOL_VERSION);
    assert_eq!(capability, WEBSOCKET_CAPABILITY);
    (socket, current_event_cursor)
}

async fn receive_event(socket: &mut EventSocket) -> SessionEventResponse {
    let frame = socket
        .next()
        .await
        .expect("event WebSocket Event")
        .expect("event WebSocket Event frame");
    let frame: WebSocketFrame = serde_json::from_str(
        frame
            .into_text()
            .expect("event WebSocket Event is text")
            .as_ref(),
    )
    .expect("event WebSocket Event JSON");
    let WebSocketFrame::Event { event } = frame else {
        panic!("expected event WebSocket Event frame");
    };
    event
}

async fn receive_run_events(
    socket: &mut EventSocket,
    run_id: &str,
    terminal_state: RunState,
) -> Vec<SessionEventResponse> {
    let mut events = Vec::new();
    loop {
        let event = receive_event(socket).await;
        let terminal = matches!(
            &event.event,
            SessionEventDataResponse::RunStateChanged {
                run_id: event_run_id,
                state,
            } if event_run_id == run_id && state == &terminal_state
        );
        events.push(event);
        if terminal {
            return events;
        }
    }
}

async fn assert_workspace_problem(
    http: &reqwest::Client,
    address: &str,
    request: Value,
    expected_status: StatusCode,
    expected_code: &str,
) {
    let response = http
        .post(format!("http://{address}{WORKSPACES_PATH}"))
        .json(&request)
        .send()
        .await
        .expect("Workspace error response");
    assert_eq!(response.status(), expected_status);
    let problem: ProblemDetails = response.json().await.expect("Workspace problem body");
    assert_eq!(problem.code, expected_code);
    assert!(!problem.detail.contains("sqlx"));
    assert!(!problem.detail.contains("axum"));
}

async fn assert_problem(
    response: reqwest::Response,
    expected_status: StatusCode,
    expected_code: &str,
) {
    assert_eq!(response.status(), expected_status);
    let problem: ProblemDetails = response.json().await.expect("problem response body");
    assert_eq!(problem.code, expected_code);
    assert!(!problem.detail.contains("sqlx"));
    assert!(!problem.detail.contains("axum"));
}

#[test]
fn daemon_rejects_non_loopback_listener() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_kilnd"))
        .env("KILN_LISTEN_ADDR", "0.0.0.0:0")
        .output()
        .expect("daemon process runs");

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("KILN_LISTEN_ADDR must use a loopback address")
    );
}

#[tokio::test]
async fn real_daemon_negotiates_and_rejects_invalid_protocol_input() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let data = tempfile::tempdir().expect("temporary data directory");
    let daemon = Daemon::start(binary, data.path());
    let address = &daemon.address;
    let http = reqwest::Client::new();
    let request = NegotiateRequest {
        min_version: PROTOCOL_VERSION.to_owned(),
        max_version: PROTOCOL_VERSION.to_owned(),
        client: ClientIdentity {
            name: "black-box".to_owned(),
            build: "test".to_owned(),
        },
        requested_capabilities: vec![WEBSOCKET_CAPABILITY.to_owned()],
    };
    let response = http
        .post(format!("http://{address}{NEGOTIATE_PATH}"))
        .json(&request)
        .send()
        .await
        .expect("negotiation response");
    assert_eq!(response.status(), StatusCode::OK);
    let negotiated: NegotiateResponse = response.json().await.expect("negotiation JSON");
    assert_eq!(negotiated.selected_version, PROTOCOL_VERSION);
    assert_eq!(
        negotiated.selected_capabilities,
        [WEBSOCKET_CAPABILITY.to_owned()]
    );
    assert_eq!(negotiated.store_identity.id, "local");
    assert_eq!(negotiated.store_identity.name, "Kiln local store");
    assert_eq!(negotiated.current_event_cursor, None);

    let unsupported = NegotiateRequest {
        min_version: "2.0.0".to_owned(),
        max_version: "2.0.0".to_owned(),
        ..request.clone()
    };
    let unsupported = http
        .post(format!("http://{address}{NEGOTIATE_PATH}"))
        .json(&unsupported)
        .send()
        .await
        .expect("unsupported version response");
    assert_eq!(unsupported.status(), StatusCode::BAD_REQUEST);
    let unsupported: ProblemDetails = unsupported
        .json()
        .await
        .expect("unsupported version error body");
    assert_eq!(unsupported.code, error_code::UNSUPPORTED_VERSION);

    for request in [
        http.get(format!("http://{address}{NEGOTIATE_PATH}")),
        http.post(format!("http://{address}{EVENTS_WEBSOCKET_PATH}")),
    ] {
        let response = request.send().await.expect("wrong method response");
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .expect("problem content type"),
            "application/problem+json"
        );
        let problem: ProblemDetails = response.json().await.expect("wrong method error body");
        assert_eq!(problem.code, error_code::METHOD_NOT_ALLOWED);
    }

    let event_url = format!(
        "http://{address}{EVENTS_WEBSOCKET_PATH}?version={PROTOCOL_VERSION}&capability={WEBSOCKET_CAPABILITY}"
    );
    let missing_upgrade = http
        .get(&event_url)
        .send()
        .await
        .expect("missing WebSocket upgrade response");
    assert_eq!(missing_upgrade.status(), StatusCode::UPGRADE_REQUIRED);
    let missing_upgrade: ProblemDetails = missing_upgrade
        .json()
        .await
        .expect("missing WebSocket upgrade error body");
    assert_eq!(missing_upgrade.code, error_code::WEBSOCKET_UPGRADE_REQUIRED);

    let unknown_query = http
        .get(format!("{event_url}&unknown=true"))
        .send()
        .await
        .expect("unknown query response");
    assert_eq!(unknown_query.status(), StatusCode::BAD_REQUEST);
    let unknown_query: ProblemDetails = unknown_query
        .json()
        .await
        .expect("unknown query error body");
    assert_eq!(unknown_query.code, error_code::INVALID_REQUEST);

    let endpoint = format!(
        "ws://{address}{EVENTS_WEBSOCKET_PATH}?version={PROTOCOL_VERSION}&capability={WEBSOCKET_CAPABILITY}"
    );
    let (mut socket, _) = connect_async(endpoint).await.expect("WebSocket connects");
    let ack = socket
        .next()
        .await
        .expect("ack frame")
        .expect("ack message");
    let ack: WebSocketFrame =
        serde_json::from_str(ack.into_text().expect("ack is text").as_ref()).expect("ack JSON");
    assert_eq!(
        ack,
        WebSocketFrame::Ack {
            version: PROTOCOL_VERSION.to_owned(),
            capability: WEBSOCKET_CAPABILITY.to_owned(),
            current_event_cursor: None,
        }
    );

    socket
        .send(Message::Text("{}".into()))
        .await
        .expect("invalid input sent");
    let error = socket
        .next()
        .await
        .expect("error frame")
        .expect("error message");
    let error: WebSocketFrame =
        serde_json::from_str(error.into_text().expect("error is text").as_ref())
            .expect("error JSON");
    assert_eq!(
        error,
        WebSocketFrame::Error {
            code: error_code::UNSUPPORTED_INPUT.to_owned(),
            message: "The event WebSocket accepts control frames only.".to_owned(),
        }
    );

    let invalid = http
        .post(format!("http://{address}{NEGOTIATE_PATH}"))
        .header("content-type", "application/json")
        .body("{not-json")
        .send()
        .await
        .expect("invalid JSON response");
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    let invalid_body: ProblemDetails = invalid.json().await.expect("invalid JSON error body");
    assert_eq!(invalid_body.code, error_code::INVALID_JSON);
    assert!(!invalid_body.detail.contains("axum"));

    let unknown_field = http
        .post(format!("http://{address}{NEGOTIATE_PATH}"))
        .json(&serde_json::json!({
            "min_version": PROTOCOL_VERSION,
            "max_version": PROTOCOL_VERSION,
            "client": {"name": "black-box", "build": "test"},
            "requested_capabilities": [WEBSOCKET_CAPABILITY],
            "unknown": true
        }))
        .send()
        .await
        .expect("unknown field response");
    assert_eq!(unknown_field.status(), StatusCode::BAD_REQUEST);
    let unknown_field: ProblemDetails = unknown_field
        .json()
        .await
        .expect("unknown field error body");
    assert_eq!(unknown_field.code, error_code::INVALID_REQUEST);

    let unknown = http
        .get(format!("http://{address}/v1/not-a-route"))
        .send()
        .await
        .expect("unknown route response");
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
    let unknown_body: ProblemDetails = unknown.json().await.expect("unknown route error body");
    assert_eq!(unknown_body.code, error_code::NOT_FOUND);
    assert!(!unknown_body.detail.contains("axum"));
}

#[tokio::test]
async fn real_daemon_creates_and_recovers_a_multi_repository_workspace() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary test directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("repository parent");
    let first = git_repository(&repositories, "first");
    let second = git_repository(&repositories, "second");
    let data_directory = sandbox.path().join("data");
    let http = reqwest::Client::new();

    let daemon = Daemon::start(binary, &data_directory);
    let request = CreateWorkspaceRequest {
        name: "Kiln development".to_owned(),
        roots: vec![
            WorkspaceRootRequest {
                name: "core".to_owned(),
                path: first.to_str().expect("first path is UTF-8").to_owned(),
            },
            WorkspaceRootRequest {
                name: "companion".to_owned(),
                path: second.to_str().expect("second path is UTF-8").to_owned(),
            },
        ],
    };
    let response = http
        .post(format!("http://{}{WORKSPACES_PATH}", daemon.address))
        .json(&request)
        .send()
        .await
        .expect("create Workspace response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let created: WorkspaceResponse = response.json().await.expect("create Workspace JSON");
    assert!(created.workspace_id.starts_with("wsp_"));
    assert_eq!(created.workspace_id.len(), 30);
    assert_eq!(created.name, request.name);
    assert_eq!(created.roots.len(), 2);
    assert_eq!(created.roots[0].name, "core");
    assert_eq!(created.roots[1].name, "companion");
    assert_eq!(created.roots[0].position, 0);
    assert_eq!(created.roots[1].position, 1);
    for root in &created.roots {
        assert!(root.workspace_root_id.starts_with("wrt_"));
        assert_eq!(root.workspace_root_id.len(), 30);
        assert_eq!(root.state, "available");
    }
    assert_eq!(created.roots[0].display_path, request.roots[0].path);
    assert_eq!(created.roots[1].display_path, request.roots[1].path);
    assert_eq!(created.roots[0].canonical_path, canonical_string(&first));
    assert_eq!(created.roots[1].canonical_path, canonical_string(&second));
    assert_eq!(
        created.roots[0].git_common_directory_path,
        canonical_string(&first.join(".git"))
    );
    assert_eq!(
        created.roots[1].git_common_directory_path,
        canonical_string(&second.join(".git"))
    );

    drop(daemon);
    let daemon = Daemon::start(binary, &data_directory);
    let path = WORKSPACE_PATH.replace("{workspace_id}", &created.workspace_id);
    let response = http
        .get(format!("http://{}{}", daemon.address, path))
        .send()
        .await
        .expect("retrieve Workspace response");
    assert_eq!(response.status(), StatusCode::OK);
    let recovered: WorkspaceResponse = response.json().await.expect("retrieve Workspace JSON");
    assert_eq!(recovered, created);

    let plain = sandbox.path().join("plain");
    std::fs::create_dir(&plain).expect("plain directory");
    assert_workspace_problem(
        &http,
        &daemon.address,
        serde_json::json!({
            "name": "Invalid",
            "roots": [{"name": "plain", "path": plain}]
        }),
        StatusCode::BAD_REQUEST,
        error_code::WORKSPACE_ROOT_NOT_GIT_REPOSITORY,
    )
    .await;
    assert_workspace_problem(
        &http,
        &daemon.address,
        serde_json::json!({
            "name": "Invalid",
            "roots": [{"name": "missing", "path": sandbox.path().join("missing")}]
        }),
        StatusCode::BAD_REQUEST,
        error_code::WORKSPACE_ROOT_MISSING,
    )
    .await;
    assert_workspace_problem(
        &http,
        &daemon.address,
        serde_json::json!({
            "name": "Invalid",
            "roots": [
                {"name": "same", "path": first},
                {"name": "same", "path": second}
            ]
        }),
        StatusCode::CONFLICT,
        error_code::WORKSPACE_ROOT_NAME_CONFLICT,
    )
    .await;
    assert_workspace_problem(
        &http,
        &daemon.address,
        serde_json::json!({
            "name": "Invalid",
            "roots": [
                {"name": "first", "path": first},
                {"name": "same installation", "path": first}
            ]
        }),
        StatusCode::CONFLICT,
        error_code::WORKSPACE_ROOT_DUPLICATE,
    )
    .await;
    assert_workspace_problem(
        &http,
        &daemon.address,
        serde_json::json!({
            "name": "Invalid",
            "roots": [{"name": "first", "path": first, "kind": "archive"}]
        }),
        StatusCode::BAD_REQUEST,
        error_code::INVALID_REQUEST,
    )
    .await;

    let response = http
        .post(format!("http://{}{WORKSPACES_PATH}", daemon.address))
        .json(&CreateWorkspaceRequest {
            name: "Another context".to_owned(),
            roots: vec![WorkspaceRootRequest {
                name: "core".to_owned(),
                path: first.to_str().expect("first path is UTF-8").to_owned(),
            }],
        })
        .send()
        .await
        .expect("second Workspace response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let invalid_path = WORKSPACE_PATH.replace("{workspace_id}", "wsp_invalid");
    let response = http
        .get(format!("http://{}{}", daemon.address, invalid_path))
        .send()
        .await
        .expect("invalid Workspace ID response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let problem: ProblemDetails = response.json().await.expect("invalid Workspace ID problem");
    assert_eq!(problem.code, error_code::INVALID_REQUEST);

    let missing_path = WORKSPACE_PATH.replace("{workspace_id}", "wsp_01ARZ3NDEKTSV4RRFFQ69G5FB0");
    let response = http
        .get(format!("http://{}{}", daemon.address, missing_path))
        .send()
        .await
        .expect("missing Workspace response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let problem: ProblemDetails = response.json().await.expect("missing Workspace problem");
    assert_eq!(problem.code, error_code::WORKSPACE_NOT_FOUND);
}

#[tokio::test]
async fn real_daemon_persists_sessions_messages_and_ordered_events() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary test directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("repository parent");
    let repository = git_repository(&repositories, "main");
    let data_directory = sandbox.path().join("data");
    let http = reqwest::Client::new();
    let daemon = Daemon::start(binary, &data_directory);

    let response = http
        .post(format!("http://{}{WORKSPACES_PATH}", daemon.address))
        .json(&CreateWorkspaceRequest {
            name: "Session test Workspace".to_owned(),
            roots: vec![WorkspaceRootRequest {
                name: "main".to_owned(),
                path: repository
                    .to_str()
                    .expect("repository path is UTF-8")
                    .to_owned(),
            }],
        })
        .send()
        .await
        .expect("Workspace response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let workspace: WorkspaceResponse = response.json().await.expect("Workspace JSON");

    let create_session_path =
        WORKSPACE_SESSIONS_PATH.replace("{workspace_id}", &workspace.workspace_id);
    let mut sessions = Vec::new();
    for _ in 0..2 {
        let response = http
            .post(format!("http://{}{}", daemon.address, create_session_path))
            .send()
            .await
            .expect("create Session response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let session: SessionResponse = response.json().await.expect("Session JSON");
        assert!(session.session_id.starts_with("ses_"));
        assert_eq!(session.session_id.len(), 30);
        assert_eq!(session.workspace_id, workspace.workspace_id);
        sessions.push(session);
    }

    for (session_index, content) in [
        (0, "first Session message"),
        (1, "second Session message"),
        (0, "first Session follow-up"),
    ] {
        let path =
            SESSION_MESSAGES_PATH.replace("{session_id}", &sessions[session_index].session_id);
        let response = http
            .post(format!("http://{}{}", daemon.address, path))
            .json(&AppendMessageRequest {
                content: content.to_owned(),
            })
            .send()
            .await
            .expect("append Message response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let message: MessageResponse = response.json().await.expect("Message JSON");
        assert!(message.message_id.starts_with("msg_"));
        assert_eq!(message.message_id.len(), 30);
        assert_eq!(message.session_id, sessions[session_index].session_id);
        assert_eq!(message.role, kiln_protocol::MessageRole::User);
        assert_eq!(message.content, content);
    }

    let first_events_path = SESSION_EVENTS_PATH.replace("{session_id}", &sessions[0].session_id);
    let response = http
        .get(format!("http://{}{}", daemon.address, first_events_path))
        .send()
        .await
        .expect("first Session Events response");
    assert_eq!(response.status(), StatusCode::OK);
    let first_page: SessionEventsResponse = response.json().await.expect("first Events JSON");
    assert_eq!(first_page.events.len(), 3);
    assert_eq!(first_page.current_event_cursor, "5");
    assert!(matches!(
        &first_page.events[0].event,
        SessionEventDataResponse::SessionCreated { .. }
    ));
    assert!(matches!(
        &first_page.events[1].event,
        SessionEventDataResponse::MessageAppended { .. }
    ));
    assert!(matches!(
        &first_page.events[2].event,
        SessionEventDataResponse::MessageAppended { .. }
    ));
    assert!(first_page.events.iter().all(|event| {
        event.event_id.starts_with("evt_")
            && event.event_id.len() == 30
            && event.session_id == sessions[0].session_id
    }));
    let first_cursors = first_page
        .events
        .iter()
        .map(|event| event.cursor.parse::<u64>().expect("numeric Event cursor"))
        .collect::<Vec<_>>();
    assert_eq!(first_cursors, [1, 3, 5]);

    let second_events_path = SESSION_EVENTS_PATH.replace("{session_id}", &sessions[1].session_id);
    let response = http
        .get(format!("http://{}{}", daemon.address, second_events_path))
        .send()
        .await
        .expect("second Session Events response");
    assert_eq!(response.status(), StatusCode::OK);
    let second_page: SessionEventsResponse = response.json().await.expect("second Events JSON");
    assert_eq!(
        second_page
            .events
            .iter()
            .map(|event| event.cursor.as_str())
            .collect::<Vec<_>>(),
        ["2", "4"]
    );
    assert_eq!(second_page.current_event_cursor, "5");

    let response = http
        .get(format!(
            "http://{}{}?after={}",
            daemon.address, first_events_path, first_page.events[0].cursor
        ))
        .send()
        .await
        .expect("Events after cursor response");
    assert_eq!(response.status(), StatusCode::OK);
    let after_page: SessionEventsResponse = response.json().await.expect("Events after JSON");
    assert_eq!(
        after_page
            .events
            .iter()
            .map(|event| event.cursor.as_str())
            .collect::<Vec<_>>(),
        ["3", "5"]
    );
    assert_eq!(after_page.current_event_cursor, "5");

    drop(daemon);
    let daemon = Daemon::start(binary, &data_directory);
    let session_path = SESSION_PATH.replace("{session_id}", &sessions[0].session_id);
    let response = http
        .get(format!("http://{}{}", daemon.address, session_path))
        .send()
        .await
        .expect("recovered Session response");
    assert_eq!(response.status(), StatusCode::OK);
    let recovered: SessionResponse = response.json().await.expect("recovered Session JSON");
    assert_eq!(recovered, sessions[0]);

    let response = http
        .get(format!("http://{}{}", daemon.address, first_events_path))
        .send()
        .await
        .expect("recovered Events response");
    assert_eq!(response.status(), StatusCode::OK);
    let recovered_page: SessionEventsResponse =
        response.json().await.expect("recovered Events JSON");
    assert_eq!(recovered_page, first_page);

    let negotiate = NegotiateRequest {
        min_version: PROTOCOL_VERSION.to_owned(),
        max_version: PROTOCOL_VERSION.to_owned(),
        client: ClientIdentity {
            name: "black-box".to_owned(),
            build: "test".to_owned(),
        },
        requested_capabilities: vec![WEBSOCKET_CAPABILITY.to_owned()],
    };
    let response = http
        .post(format!("http://{}{NEGOTIATE_PATH}", daemon.address))
        .json(&negotiate)
        .send()
        .await
        .expect("negotiation after Events");
    assert_eq!(response.status(), StatusCode::OK);
    let negotiated: NegotiateResponse = response.json().await.expect("negotiation JSON");
    assert_eq!(negotiated.current_event_cursor.as_deref(), Some("5"));

    let endpoint = format!(
        "ws://{}{EVENTS_WEBSOCKET_PATH}?version={PROTOCOL_VERSION}&capability={WEBSOCKET_CAPABILITY}",
        daemon.address
    );
    let (mut socket, _) = connect_async(endpoint)
        .await
        .expect("WebSocket connects after Events");
    let ack = socket
        .next()
        .await
        .expect("ack frame after Events")
        .expect("ack message after Events");
    let ack: WebSocketFrame = serde_json::from_str(ack.into_text().expect("ack is text").as_ref())
        .expect("ack JSON after Events");
    assert_eq!(
        ack,
        WebSocketFrame::Ack {
            version: PROTOCOL_VERSION.to_owned(),
            capability: WEBSOCKET_CAPABILITY.to_owned(),
            current_event_cursor: Some("5".to_owned()),
        }
    );

    let missing_workspace_path =
        WORKSPACE_SESSIONS_PATH.replace("{workspace_id}", "wsp_01ARZ3NDEKTSV4RRFFQ69G5FB0");
    assert_problem(
        http.post(format!(
            "http://{}{}",
            daemon.address, missing_workspace_path
        ))
        .send()
        .await
        .expect("missing Workspace Session response"),
        StatusCode::NOT_FOUND,
        error_code::WORKSPACE_NOT_FOUND,
    )
    .await;

    let invalid_workspace_path = WORKSPACE_SESSIONS_PATH.replace("{workspace_id}", "wsp_invalid");
    assert_problem(
        http.post(format!(
            "http://{}{}",
            daemon.address, invalid_workspace_path
        ))
        .send()
        .await
        .expect("invalid Workspace ID Session response"),
        StatusCode::BAD_REQUEST,
        error_code::INVALID_REQUEST,
    )
    .await;

    let missing_session_messages =
        SESSION_MESSAGES_PATH.replace("{session_id}", "ses_01ARZ3NDEKTSV4RRFFQ69G5FB0");
    assert_problem(
        http.post(format!(
            "http://{}{}",
            daemon.address, missing_session_messages
        ))
        .json(&AppendMessageRequest {
            content: "message".to_owned(),
        })
        .send()
        .await
        .expect("missing Session Message response"),
        StatusCode::NOT_FOUND,
        error_code::SESSION_NOT_FOUND,
    )
    .await;

    let first_messages_path =
        SESSION_MESSAGES_PATH.replace("{session_id}", &sessions[0].session_id);
    assert_problem(
        http.post(format!("http://{}{}", daemon.address, first_messages_path))
            .json(&AppendMessageRequest {
                content: "  \n".to_owned(),
            })
            .send()
            .await
            .expect("empty Message response"),
        StatusCode::BAD_REQUEST,
        error_code::MESSAGE_CONTENT_REQUIRED,
    )
    .await;

    assert_problem(
        http.get(format!(
            "http://{}{}?after=01",
            daemon.address, first_events_path
        ))
        .send()
        .await
        .expect("invalid cursor response"),
        StatusCode::BAD_REQUEST,
        error_code::INVALID_EVENT_CURSOR,
    )
    .await;

    assert_problem(
        http.post(format!("http://{}{}", daemon.address, first_messages_path))
            .json(&serde_json::json!({"content": "message", "event_type": "custom"}))
            .send()
            .await
            .expect("arbitrary Event input response"),
        StatusCode::BAD_REQUEST,
        error_code::INVALID_REQUEST,
    )
    .await;
}

#[tokio::test]
async fn real_daemon_runs_a_subprocess_publishes_events_and_recovers_the_result() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary Run test directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("Run test repository parent");
    let data_directory = sandbox.path().join("data");
    let http = reqwest::Client::new();
    let daemon = Daemon::start_with_outcome(binary, &data_directory, Some("success"));
    let session = create_run_session(&http, &daemon.address, &repositories).await;
    let mut socket = open_event_socket(&daemon.address).await;

    let start_path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);
    assert_problem(
        http.post(format!("http://{}{}", daemon.address, start_path))
            .send()
            .await
            .expect("missing idempotency key response"),
        StatusCode::BAD_REQUEST,
        error_code::IDEMPOTENCY_KEY_REQUIRED,
    )
    .await;
    assert_problem(
        http.post(format!("http://{}{}", daemon.address, start_path))
            .header(IDEMPOTENCY_KEY_HEADER, "")
            .send()
            .await
            .expect("empty idempotency key response"),
        StatusCode::BAD_REQUEST,
        error_code::INVALID_IDEMPOTENCY_KEY,
    )
    .await;

    let response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "run-success")
        .send()
        .await
        .expect("start Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let queued: RunResponse = response.json().await.expect("queued Run JSON");
    assert!(queued.run_id.starts_with("run_"));
    assert_eq!(queued.run_id.len(), 30);
    assert_eq!(queued.session_id, session.session_id);
    assert_eq!(queued.state, RunState::Queued);
    assert!(queued.tool_calls.is_empty());

    let duplicate_response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "run-success")
        .send()
        .await
        .expect("duplicate start Run response");
    assert_eq!(duplicate_response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        duplicate_response
            .json::<RunResponse>()
            .await
            .expect("duplicate start Run JSON"),
        queued
    );
    assert_problem(
        http.post(format!("http://{}{}", daemon.address, start_path))
            .header(IDEMPOTENCY_KEY_HEADER, "different-run")
            .send()
            .await
            .expect("different start Run response"),
        StatusCode::CONFLICT,
        error_code::ACTIVE_ROOT_RUN_EXISTS,
    )
    .await;

    let live_events = receive_run_events(&mut socket, &queued.run_id, RunState::Completed).await;
    assert_eq!(
        live_events.iter().map(event_kind).collect::<Vec<_>>(),
        [
            "run.created",
            "run.state_changed",
            "tool_call.requested",
            "tool_call.state_changed",
            "tool_call.output",
            "tool_call.output",
            "tool_call.state_changed",
            "run.state_changed",
        ]
    );
    assert!(
        live_events
            .iter()
            .all(|event| event.session_id == session.session_id)
    );
    let cursors = live_events
        .iter()
        .map(|event| event.cursor.parse::<u64>().expect("numeric Event cursor"))
        .collect::<Vec<_>>();
    assert!(cursors.windows(2).all(|pair| pair[0] < pair[1]));

    let run_path = RUN_PATH.replace("{run_id}", &queued.run_id);
    let response = http
        .get(format!("http://{}{}", daemon.address, run_path))
        .send()
        .await
        .expect("terminal Run response");
    assert_eq!(response.status(), StatusCode::OK);
    let completed: RunResponse = response.json().await.expect("terminal Run JSON");
    assert_eq!(completed.state, RunState::Completed);
    assert_eq!(completed.tool_calls.len(), 1);
    let tool_call = &completed.tool_calls[0];
    assert!(tool_call.tool_call_id.starts_with("tcl_"));
    assert_eq!(tool_call.tool_call_id.len(), 30);
    assert_eq!(tool_call.run_id, completed.run_id);
    assert_eq!(tool_call.capability, DETERMINISTIC_SUBPROCESS_CAPABILITY);
    assert_eq!(tool_call.state, ToolCallState::Completed);
    assert_eq!(
        tool_call.stdout.as_deref(),
        Some("kiln deterministic subprocess stdout\n")
    );
    assert_eq!(
        tool_call.stderr.as_deref(),
        Some("kiln deterministic subprocess stderr\n")
    );
    assert_eq!(tool_call.exit_code, Some(0));

    let duplicate_response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "run-success")
        .send()
        .await
        .expect("post-terminal duplicate start Run response");
    assert_eq!(duplicate_response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        duplicate_response
            .json::<RunResponse>()
            .await
            .expect("post-terminal duplicate start Run JSON"),
        queued
    );

    let events_path = SESSION_EVENTS_PATH.replace("{session_id}", &session.session_id);
    let response = http
        .get(format!("http://{}{}", daemon.address, events_path))
        .send()
        .await
        .expect("Run Event history response");
    assert_eq!(response.status(), StatusCode::OK);
    let history: SessionEventsResponse = response.json().await.expect("Run Event history JSON");
    let history_run_events = history
        .events
        .iter()
        .filter(|event| event_kind(event) != "session.created")
        .collect::<Vec<_>>();
    assert_eq!(
        history_run_events
            .iter()
            .map(|event| event.cursor.as_str())
            .collect::<Vec<_>>(),
        live_events
            .iter()
            .map(|event| event.cursor.as_str())
            .collect::<Vec<_>>()
    );

    let missing_session_path =
        SESSION_RUNS_PATH.replace("{session_id}", "ses_01ARZ3NDEKTSV4RRFFQ69G5FAW");
    assert_problem(
        http.post(format!("http://{}{}", daemon.address, missing_session_path))
            .header(IDEMPOTENCY_KEY_HEADER, "missing-session")
            .send()
            .await
            .expect("missing Run Session response"),
        StatusCode::NOT_FOUND,
        error_code::SESSION_NOT_FOUND,
    )
    .await;

    assert_problem(
        http.get(format!(
            "http://{}{}",
            daemon.address,
            RUN_PATH.replace("{run_id}", "run_invalid")
        ))
        .send()
        .await
        .expect("invalid Run ID response"),
        StatusCode::BAD_REQUEST,
        error_code::INVALID_REQUEST,
    )
    .await;
    assert_problem(
        http.get(format!(
            "http://{}{}",
            daemon.address,
            RUN_PATH.replace("{run_id}", "run_01ARZ3NDEKTSV4RRFFQ69G5FAW")
        ))
        .send()
        .await
        .expect("missing Run response"),
        StatusCode::NOT_FOUND,
        error_code::RUN_NOT_FOUND,
    )
    .await;

    drop(socket);
    drop(daemon);

    let restarted = Daemon::start(binary, &data_directory);
    let response = http
        .get(format!("http://{}{}", restarted.address, run_path))
        .send()
        .await
        .expect("recovered Run response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .json::<RunResponse>()
            .await
            .expect("recovered Run JSON"),
        completed
    );
}

#[tokio::test]
async fn reconnect_replays_exact_durable_suffix_across_restart() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary reconnect test directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("reconnect repository parent");
    let data_directory = sandbox.path().join("data");
    let http = reqwest::Client::new();
    let daemon = Daemon::start_with_outcome(binary, &data_directory, Some("success"));
    let session = create_run_session(&http, &daemon.address, &repositories).await;
    let mut live_socket = open_event_socket(&daemon.address).await;

    let start_path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);
    let response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "reconnect-run")
        .send()
        .await
        .expect("reconnect Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let queued: RunResponse = response.json().await.expect("reconnect Run JSON");

    let first_event = receive_event(&mut live_socket).await;
    assert!(matches!(
        &first_event.event,
        SessionEventDataResponse::RunCreated { run_id, .. } if run_id == &queued.run_id
    ));
    let after = first_event.cursor;
    drop(live_socket);

    let (mut replay_socket, acknowledged_cursor) =
        open_event_socket_after(&daemon.address, Some(&after)).await;
    assert!(
        acknowledged_cursor
            .as_deref()
            .expect("reconnect acknowledgement cursor")
            .parse::<u64>()
            .expect("numeric reconnect acknowledgement cursor")
            >= after.parse::<u64>().expect("numeric replay cursor")
    );
    let replayed =
        receive_run_events(&mut replay_socket, &queued.run_id, RunState::Completed).await;
    let replayed_cursors = replayed
        .iter()
        .map(|event| event.cursor.parse::<u64>().expect("numeric replay cursor"))
        .collect::<Vec<_>>();
    let after_value = after.parse::<u64>().expect("numeric replay boundary");
    assert!(replayed_cursors.iter().all(|cursor| *cursor > after_value));
    assert!(replayed_cursors.windows(2).all(|pair| pair[0] < pair[1]));

    let events_path = SESSION_EVENTS_PATH.replace("{session_id}", &session.session_id);
    let response = http
        .get(format!(
            "http://{}{}?after={after}",
            daemon.address, events_path
        ))
        .send()
        .await
        .expect("durable replay suffix response");
    assert_eq!(response.status(), StatusCode::OK);
    let durable_suffix: SessionEventsResponse =
        response.json().await.expect("durable replay suffix JSON");
    assert_eq!(replayed.as_slice(), durable_suffix.events.as_slice());

    drop(replay_socket);
    drop(daemon);

    let restarted = Daemon::start(binary, &data_directory);
    let (mut restarted_socket, restarted_cursor) =
        open_event_socket_after(&restarted.address, Some(&after)).await;
    assert_eq!(
        restarted_cursor.as_deref(),
        Some(durable_suffix.current_event_cursor.as_str())
    );
    let replayed_after_restart =
        receive_run_events(&mut restarted_socket, &queued.run_id, RunState::Completed).await;
    assert_eq!(
        replayed_after_restart.as_slice(),
        durable_suffix.events.as_slice()
    );
}

#[tokio::test]
async fn real_daemon_persists_a_failed_subprocess_result() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary failed Run test directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("failed Run test repository parent");
    let data_directory = sandbox.path().join("data");
    let http = reqwest::Client::new();
    let daemon = Daemon::start_with_outcome(binary, &data_directory, Some("failure"));
    let session = create_run_session(&http, &daemon.address, &repositories).await;
    let mut socket = open_event_socket(&daemon.address).await;

    let start_path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);
    let response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "run-failure")
        .send()
        .await
        .expect("start failed Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let queued: RunResponse = response.json().await.expect("queued failed Run JSON");

    let events = receive_run_events(&mut socket, &queued.run_id, RunState::Failed).await;
    assert_eq!(events.len(), 8);
    assert!(matches!(
        &events.last().expect("terminal failed Run Event").event,
        SessionEventDataResponse::RunStateChanged {
            state: RunState::Failed,
            ..
        }
    ));

    let run_path = RUN_PATH.replace("{run_id}", &queued.run_id);
    let response = http
        .get(format!("http://{}{}", daemon.address, run_path))
        .send()
        .await
        .expect("failed Run response");
    assert_eq!(response.status(), StatusCode::OK);
    let failed: RunResponse = response.json().await.expect("failed Run JSON");
    assert_eq!(failed.state, RunState::Failed);
    assert_eq!(failed.tool_calls.len(), 1);
    let tool_call = &failed.tool_calls[0];
    assert_eq!(tool_call.state, ToolCallState::Failed);
    assert_eq!(
        tool_call.stdout.as_deref(),
        Some("kiln deterministic subprocess stdout\n")
    );
    assert_eq!(
        tool_call.stderr.as_deref(),
        Some("kiln deterministic subprocess failure\n")
    );
    assert_eq!(tool_call.exit_code, Some(1));
}

#[tokio::test]
async fn concurrent_runs_publish_in_global_cursor_order() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary concurrent Run test directory");
    let first_repositories = sandbox.path().join("first-repositories");
    let second_repositories = sandbox.path().join("second-repositories");
    std::fs::create_dir(&first_repositories).expect("first repository parent");
    std::fs::create_dir(&second_repositories).expect("second repository parent");
    let data_directory = sandbox.path().join("data");
    let http = reqwest::Client::new();
    let daemon = Daemon::start(binary, &data_directory);
    let first_session = create_run_session(&http, &daemon.address, &first_repositories).await;
    let second_session = create_run_session(&http, &daemon.address, &second_repositories).await;
    let mut socket = open_event_socket(&daemon.address).await;

    let first_path = SESSION_RUNS_PATH.replace("{session_id}", &first_session.session_id);
    let second_path = SESSION_RUNS_PATH.replace("{session_id}", &second_session.session_id);
    let (first_response, second_response) = tokio::join!(
        http.post(format!("http://{}{}", daemon.address, first_path))
            .header(IDEMPOTENCY_KEY_HEADER, "first-run")
            .send(),
        http.post(format!("http://{}{}", daemon.address, second_path))
            .header(IDEMPOTENCY_KEY_HEADER, "second-run")
            .send(),
    );
    let first_response = first_response.expect("first concurrent Run response");
    let second_response = second_response.expect("second concurrent Run response");
    assert_eq!(first_response.status(), StatusCode::ACCEPTED);
    assert_eq!(second_response.status(), StatusCode::ACCEPTED);
    let first: RunResponse = first_response
        .json()
        .await
        .expect("first concurrent Run JSON");
    let second: RunResponse = second_response
        .json()
        .await
        .expect("second concurrent Run JSON");

    let mut first_completed = false;
    let mut second_completed = false;
    let mut events = Vec::new();
    while !first_completed || !second_completed {
        let frame = socket
            .next()
            .await
            .expect("concurrent live Run Event")
            .expect("concurrent live Run Event frame");
        let frame: WebSocketFrame = serde_json::from_str(
            frame
                .into_text()
                .expect("concurrent live Run Event is text")
                .as_ref(),
        )
        .expect("concurrent live Run Event JSON");
        let WebSocketFrame::Event { event } = frame else {
            panic!("expected concurrent live Event frame");
        };
        if let SessionEventDataResponse::RunStateChanged {
            run_id,
            state: RunState::Completed,
        } = &event.event
        {
            first_completed |= run_id == &first.run_id;
            second_completed |= run_id == &second.run_id;
        }
        events.push(event);
    }

    assert_eq!(events.len(), 16);
    let cursors = events
        .iter()
        .map(|event| event.cursor.parse::<u64>().expect("numeric Event cursor"))
        .collect::<Vec<_>>();
    assert!(cursors.windows(2).all(|pair| pair[0] < pair[1]));
}

#[tokio::test]
async fn concurrent_same_key_starts_one_run_and_one_tool_lifecycle() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary idempotency test directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("idempotency repository parent");
    let data_directory = sandbox.path().join("data");
    let http = reqwest::Client::new();
    let daemon = Daemon::start(binary, &data_directory);
    let session = create_run_session(&http, &daemon.address, &repositories).await;
    let mut socket = open_event_socket(&daemon.address).await;
    let path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);

    let (first_response, second_response) = tokio::join!(
        http.post(format!("http://{}{}", daemon.address, path))
            .header(IDEMPOTENCY_KEY_HEADER, "concurrent-same-key")
            .send(),
        http.post(format!("http://{}{}", daemon.address, path))
            .header(IDEMPOTENCY_KEY_HEADER, "concurrent-same-key")
            .send(),
    );
    let first_response = first_response.expect("first concurrent idempotent response");
    let second_response = second_response.expect("second concurrent idempotent response");
    assert_eq!(first_response.status(), StatusCode::ACCEPTED);
    assert_eq!(second_response.status(), StatusCode::ACCEPTED);
    let first: RunResponse = first_response
        .json()
        .await
        .expect("first concurrent idempotent JSON");
    let second: RunResponse = second_response
        .json()
        .await
        .expect("second concurrent idempotent JSON");
    assert_eq!(first, second);

    let events = receive_run_events(&mut socket, &first.run_id, RunState::Completed).await;
    assert_eq!(events.len(), 8);
    assert_eq!(
        events
            .iter()
            .filter(|event| event_kind(event) == "run.created")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event_kind(event) == "tool_call.requested")
            .count(),
        1
    );
}
