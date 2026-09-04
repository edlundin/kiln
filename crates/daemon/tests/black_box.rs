use std::{
    io::BufRead,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use kiln_protocol::{
    ARTIFACT_PATH, AppendMessageRequest, ApprovalDecision, ApprovalDecisionRequest, ApprovalPolicy,
    ApprovalState, AssignTaskRequest, ClientIdentity, CreateTaskRequest, CreateWorkspaceRequest,
    DETERMINISTIC_SUBPROCESS_CAPABILITY, EVENTS_WEBSOCKET_PATH, IDEMPOTENCY_KEY_HEADER,
    MessageResponse, NEGOTIATE_PATH, NegotiateRequest, NegotiateResponse, PROTOCOL_VERSION,
    ProblemDetails, RUN_CANCEL_PATH, RUN_PATH, RunResponse, RunState, SESSION_EVENTS_PATH,
    SESSION_MESSAGES_PATH, SESSION_PATH, SESSION_RUNS_PATH, SESSION_TASKS_PATH,
    SessionEventDataResponse, SessionEventResponse, SessionEventsResponse, SessionResponse,
    StartRunRequest, TASK_ASSIGNMENT_PATH, TASK_PATH, TASK_TRANSITION_PATH,
    TOOL_CALL_APPROVAL_PATH, TaskResponse, TaskState, ToolCallState, ToolOutputStream,
    TransitionTaskRequest, UpdateTaskRequest, WEBSOCKET_CAPABILITY, WORKSPACE_PATH,
    WORKSPACE_SESSIONS_PATH, WORKSPACES_PATH, WebSocketFrame, WorkspaceResponse,
    WorkspaceRootRequest, error_code,
};
use reqwest::{
    StatusCode,
    header::{AUTHORIZATION, HOST, HeaderMap, HeaderValue, ORIGIN, WWW_AUTHENTICATE},
};
use rustix::process::{Pid, test_kill_process, test_kill_process_group};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest, http::header::SEC_WEBSOCKET_PROTOCOL},
};

struct Daemon {
    child: std::process::Child,
    address: String,
    token: String,
}

impl Daemon {
    fn start(binary: &str, data_directory: &Path) -> Self {
        Self::start_with_outcome(binary, data_directory, None)
    }

    fn start_with_outcome(binary: &str, data_directory: &Path, outcome: Option<&str>) -> Self {
        Self::start_with_configuration(binary, data_directory, outcome, None)
    }

    fn start_with_pid_file(
        binary: &str,
        data_directory: &Path,
        outcome: &str,
        pid_file: &Path,
    ) -> Self {
        Self::start_with_configuration(binary, data_directory, Some(outcome), Some(pid_file))
    }

    fn start_with_configuration(
        binary: &str,
        data_directory: &Path,
        outcome: Option<&str>,
        pid_file: Option<&Path>,
    ) -> Self {
        let mut command = std::process::Command::new(binary);
        command
            .env("KILN_LISTEN_ADDR", "127.0.0.1:0")
            .env("KILN_DATA_DIR", data_directory);
        if let Some(outcome) = outcome {
            command.env("KILN_DETERMINISTIC_SUBPROCESS_OUTCOME", outcome);
        }
        if let Some(pid_file) = pid_file {
            command.env("KILN_DETERMINISTIC_PID_FILE", pid_file);
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
        let credential_path = readiness["credential_path"]
            .as_str()
            .expect("readiness credential path");
        let token = std::fs::read_to_string(credential_path).expect("daemon credential");
        assert_eq!(token.len(), 80);
        assert!(
            token
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        );
        Self {
            child,
            address: readiness["address"]
                .as_str()
                .expect("readiness address")
                .to_owned(),
            token,
        }
    }

    fn client(&self) -> reqwest::Client {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", self.token)).expect("auth header"),
        );
        reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .expect("authenticated HTTP client")
    }

    fn signal(&self, signal: &str) {
        let status = std::process::Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(self.child.id().to_string())
            .status()
            .expect("daemon signal command runs");
        assert!(status.success(), "daemon signal is delivered");
    }

    fn wait_for_exit(&mut self) -> std::process::ExitStatus {
        self.child.wait().expect("daemon exits")
    }

    async fn wait_for_exit_within(&mut self, timeout: Duration) -> std::process::ExitStatus {
        tokio::time::timeout(timeout, async {
            loop {
                if let Some(status) = self.child.try_wait().expect("daemon exit can be polled") {
                    return status;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("daemon exits within the measured fixture tripwire")
    }

    fn resident_memory_kb(&self) -> u64 {
        let output = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p"])
            .arg(self.child.id().to_string())
            .output()
            .expect("daemon RSS sample runs");
        assert!(output.status.success(), "daemon RSS sample succeeds");
        String::from_utf8(output.stdout)
            .expect("daemon RSS sample is UTF-8")
            .trim()
            .parse()
            .expect("daemon RSS sample is numeric")
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

async fn fixture_pids(path: &Path) -> (u32, u32) {
    loop {
        if let Ok(contents) = std::fs::read_to_string(path)
            && let Ok(value) = serde_json::from_str::<Value>(&contents)
            && let (Some(parent), Some(child)) =
                (value["parent_pid"].as_u64(), value["child_pid"].as_u64())
            && let (Ok(parent), Ok(child)) = (u32::try_from(parent), u32::try_from(child))
        {
            return (parent, child);
        }
        tokio::task::yield_now().await;
    }
}

fn process_exists(pid: u32) -> bool {
    i32::try_from(pid)
        .ok()
        .and_then(Pid::from_raw)
        .is_some_and(|pid| test_kill_process(pid).is_ok())
}

fn process_group_exists(pid: u32) -> bool {
    i32::try_from(pid)
        .ok()
        .and_then(Pid::from_raw)
        .is_some_and(|pid| test_kill_process_group(pid).is_ok())
}

async fn create_run_session(
    http: &reqwest::Client,
    address: &str,
    repository_parent: &Path,
) -> RunSession {
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
    let session: SessionResponse = response.json().await.expect("Run test Session JSON");
    RunSession {
        session_id: session.session_id,
        workspace_root_id: workspace.roots[0].workspace_root_id.clone(),
        repository,
    }
}

struct RunSession {
    session_id: String,
    workspace_root_id: String,
    repository: PathBuf,
}

impl RunSession {
    fn start_request(&self) -> StartRunRequest {
        self.request_with_policy(ApprovalPolicy::FullAccess)
    }

    fn request_with_policy(&self, approval_policy: ApprovalPolicy) -> StartRunRequest {
        StartRunRequest {
            approval_policy,
            workspace_root_id: self.workspace_root_id.clone(),
            relative_directory: ".".to_owned(),
        }
    }
}

fn event_kind(event: &SessionEventResponse) -> &'static str {
    match &event.event {
        SessionEventDataResponse::SessionCreated { .. } => "session.created",
        SessionEventDataResponse::MessageAppended { .. } => "message.appended",
        SessionEventDataResponse::TaskCreated { .. } => "task.created",
        SessionEventDataResponse::TaskUpdated { .. } => "task.updated",
        SessionEventDataResponse::TaskAssigned { .. } => "task.assigned",
        SessionEventDataResponse::TaskStateChanged { .. } => "task.state_changed",
        SessionEventDataResponse::RunCreated { .. } => "run.created",
        SessionEventDataResponse::RunStateChanged { .. } => "run.state_changed",
        SessionEventDataResponse::RunCancellationRequested { .. } => "run.cancellation_requested",
        SessionEventDataResponse::ToolCallRequested { .. } => "tool_call.requested",
        SessionEventDataResponse::ToolCallStateChanged { .. } => "tool_call.state_changed",
        SessionEventDataResponse::ApprovalRequested { .. } => "approval.requested",
        SessionEventDataResponse::ApprovalDecided { .. } => "approval.decided",
        SessionEventDataResponse::ToolCallDenied { .. } => "tool_call.denied",
        SessionEventDataResponse::ToolCallOutput { .. } => "tool_call.output",
        SessionEventDataResponse::ArtifactRegistered { .. } => "artifact.registered",
    }
}

fn event_belongs_to_run(event: &SessionEventResponse, expected_run_id: &str) -> bool {
    match &event.event {
        SessionEventDataResponse::RunCreated { run_id, .. }
        | SessionEventDataResponse::RunStateChanged { run_id, .. }
        | SessionEventDataResponse::RunCancellationRequested { run_id } => {
            run_id == expected_run_id
        }
        SessionEventDataResponse::ToolCallRequested { tool_call }
        | SessionEventDataResponse::ToolCallStateChanged { tool_call }
        | SessionEventDataResponse::ToolCallDenied { tool_call } => {
            tool_call.run_id == expected_run_id
        }
        SessionEventDataResponse::ApprovalRequested { approval }
        | SessionEventDataResponse::ApprovalDecided { approval } => {
            approval.run_id == expected_run_id
        }
        SessionEventDataResponse::ToolCallOutput { run_id, .. }
        | SessionEventDataResponse::ArtifactRegistered { run_id, .. } => run_id == expected_run_id,
        SessionEventDataResponse::SessionCreated { .. }
        | SessionEventDataResponse::MessageAppended { .. }
        | SessionEventDataResponse::TaskCreated { .. }
        | SessionEventDataResponse::TaskUpdated { .. }
        | SessionEventDataResponse::TaskAssigned { .. }
        | SessionEventDataResponse::TaskStateChanged { .. } => false,
    }
}

type EventSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn open_event_socket(daemon: &Daemon) -> EventSocket {
    open_event_socket_after(daemon, None).await.0
}

async fn open_event_socket_after(
    daemon: &Daemon,
    after: Option<&str>,
) -> (EventSocket, Option<String>) {
    let mut endpoint = format!(
        "ws://{}{EVENTS_WEBSOCKET_PATH}?version={PROTOCOL_VERSION}&capability={WEBSOCKET_CAPABILITY}",
        daemon.address
    );
    if let Some(after) = after {
        endpoint.push_str("&after=");
        endpoint.push_str(after);
    }
    let mut socket = connect_event_socket(daemon, endpoint).await;
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

async fn connect_event_socket(daemon: &Daemon, endpoint: String) -> EventSocket {
    let mut request = endpoint
        .into_client_request()
        .expect("event WebSocket request");
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_str(&format!(
            "{WEBSOCKET_CAPABILITY}, kiln.auth.{}",
            daemon.token
        ))
        .expect("event WebSocket protocols"),
    );
    let (socket, response) = connect_async(request)
        .await
        .expect("event WebSocket connects");
    assert_eq!(
        response
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok()),
        Some(WEBSOCKET_CAPABILITY)
    );
    socket
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

async fn receive_run_events_with_peak_rss(
    daemon: &Daemon,
    socket: &mut EventSocket,
    run_id: &str,
    terminal_state: RunState,
) -> (Vec<SessionEventResponse>, u64) {
    let mut events = Vec::new();
    let mut peak_rss_kb = daemon.resident_memory_kb();
    loop {
        let event = receive_event(socket).await;
        peak_rss_kb = peak_rss_kb.max(daemon.resident_memory_kb());
        let terminal = matches!(
            &event.event,
            SessionEventDataResponse::RunStateChanged {
                run_id: event_run_id,
                state,
            } if event_run_id == run_id && state == &terminal_state
        );
        events.push(event);
        if terminal {
            return (events, peak_rss_kb);
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

#[tokio::test]
async fn loopback_http_and_websocket_require_the_persistent_credential() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let data_directory = tempfile::tempdir().expect("temporary auth data directory");
    let daemon = Daemon::start(binary, data_directory.path());
    let endpoint = format!("http://{}{}", daemon.address, NEGOTIATE_PATH);

    let response = reqwest::Client::new()
        .get(&endpoint)
        .send()
        .await
        .expect("unauthenticated response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response
            .headers()
            .get(WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer")
    );
    assert_eq!(
        response
            .json::<ProblemDetails>()
            .await
            .expect("unauthenticated problem")
            .code,
        error_code::AUTHENTICATION_REQUIRED
    );

    assert_problem(
        reqwest::Client::new()
            .get(&endpoint)
            .header(AUTHORIZATION, "Bearer invalid")
            .send()
            .await
            .expect("invalid credential response"),
        StatusCode::UNAUTHORIZED,
        error_code::INVALID_AUTHENTICATION,
    )
    .await;
    assert_problem(
        daemon
            .client()
            .get(&endpoint)
            .header(ORIGIN, "https://example.invalid")
            .send()
            .await
            .expect("invalid Origin response"),
        StatusCode::BAD_REQUEST,
        error_code::INVALID_ORIGIN,
    )
    .await;
    assert_problem(
        daemon
            .client()
            .get(&endpoint)
            .header(HOST, "localhost.invalid")
            .send()
            .await
            .expect("invalid Host response"),
        StatusCode::BAD_REQUEST,
        error_code::INVALID_HOST,
    )
    .await;

    let websocket_endpoint = format!("ws://{}{}", daemon.address, EVENTS_WEBSOCKET_PATH);
    let mut request = websocket_endpoint
        .into_client_request()
        .expect("invalid WebSocket auth request");
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static("kiln.events.v1, kiln.auth.invalid"),
    );
    let error = connect_async(request)
        .await
        .expect_err("invalid WebSocket credential is rejected");
    let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
        panic!("expected WebSocket HTTP authentication error");
    };
    assert_eq!(
        response.status().as_u16(),
        StatusCode::UNAUTHORIZED.as_u16()
    );
    assert_eq!(
        response
            .headers()
            .get(WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer")
    );
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
    let http = daemon.client();
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
    let mut socket = connect_event_socket(&daemon, endpoint).await;
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

    let daemon = Daemon::start(binary, &data_directory);
    let http = daemon.client();
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
    let daemon = Daemon::start(binary, &data_directory);
    let http = daemon.client();

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
    let mut socket = connect_event_socket(&daemon, endpoint).await;
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
async fn real_daemon_creates_idempotent_task_hierarchy_and_recovers_it() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary Task test directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("Task test repository parent");
    let data_directory = sandbox.path().join("data");
    let mut daemon = Daemon::start(binary, &data_directory);
    let http = daemon.client();
    let session = create_run_session(&http, &daemon.address, &repositories).await;
    let mut socket = open_event_socket(&daemon).await;
    let tasks_path = SESSION_TASKS_PATH.replace("{session_id}", &session.session_id);
    let parent_request = CreateTaskRequest {
        objective: "parent".to_owned(),
        parent_task_id: None,
        dependency_task_ids: Vec::new(),
    };

    assert_problem(
        http.post(format!("http://{}{}", daemon.address, tasks_path))
            .json(&parent_request)
            .send()
            .await
            .expect("missing Task idempotency response"),
        StatusCode::BAD_REQUEST,
        error_code::IDEMPOTENCY_KEY_REQUIRED,
    )
    .await;

    let response = http
        .post(format!("http://{}{}", daemon.address, tasks_path))
        .header(IDEMPOTENCY_KEY_HEADER, "task-parent")
        .json(&parent_request)
        .send()
        .await
        .expect("parent Task response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let parent: TaskResponse = response.json().await.expect("parent Task JSON");
    assert_eq!(parent.state, TaskState::Pending);
    assert_eq!(parent.parent_task_id, None);
    assert!(parent.dependency_task_ids.is_empty());
    assert_eq!(parent.assigned_run_id, None);
    let live = receive_event(&mut socket).await;
    assert!(matches!(
        live.event,
        SessionEventDataResponse::TaskCreated { task } if task == parent
    ));

    let duplicate: TaskResponse = http
        .post(format!("http://{}{}", daemon.address, tasks_path))
        .header(IDEMPOTENCY_KEY_HEADER, "task-parent")
        .json(&parent_request)
        .send()
        .await
        .expect("duplicate parent Task response")
        .json()
        .await
        .expect("duplicate parent Task JSON");
    assert_eq!(duplicate, parent);
    assert_problem(
        http.post(format!("http://{}{}", daemon.address, tasks_path))
            .header(IDEMPOTENCY_KEY_HEADER, "task-parent")
            .json(&CreateTaskRequest {
                objective: "different".to_owned(),
                parent_task_id: None,
                dependency_task_ids: Vec::new(),
            })
            .send()
            .await
            .expect("Task idempotency conflict response"),
        StatusCode::CONFLICT,
        error_code::IDEMPOTENCY_CONFLICT,
    )
    .await;

    let child_request = CreateTaskRequest {
        objective: "child".to_owned(),
        parent_task_id: Some(parent.task_id.clone()),
        dependency_task_ids: vec![parent.task_id.clone()],
    };
    let response = http
        .post(format!("http://{}{}", daemon.address, tasks_path))
        .header(IDEMPOTENCY_KEY_HEADER, "task-child")
        .json(&child_request)
        .send()
        .await
        .expect("child Task response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let child: TaskResponse = response.json().await.expect("child Task JSON");
    assert_eq!(
        child.parent_task_id.as_deref(),
        Some(parent.task_id.as_str())
    );
    assert_eq!(
        child.dependency_task_ids.as_slice(),
        std::slice::from_ref(&parent.task_id)
    );
    assert!(matches!(
        receive_event(&mut socket).await.event,
        SessionEventDataResponse::TaskCreated { task } if task == child
    ));

    let child_path = TASK_PATH.replace("{task_id}", &child.task_id);
    let child_transition_path = TASK_TRANSITION_PATH.replace("{task_id}", &child.task_id);
    let update_request = UpdateTaskRequest {
        objective: "updated child".to_owned(),
        dependency_task_ids: vec![parent.task_id.clone()],
    };
    assert_problem(
        http.patch(format!("http://{}{}", daemon.address, child_path))
            .json(&update_request)
            .send()
            .await
            .expect("missing Task update idempotency response"),
        StatusCode::BAD_REQUEST,
        error_code::IDEMPOTENCY_KEY_REQUIRED,
    )
    .await;
    let updated: TaskResponse = http
        .patch(format!("http://{}{}", daemon.address, child_path))
        .header(IDEMPOTENCY_KEY_HEADER, "task-update")
        .json(&update_request)
        .send()
        .await
        .expect("Task update response")
        .json()
        .await
        .expect("Task update JSON");
    assert_eq!(updated.objective, "updated child");
    assert_eq!(updated.state, TaskState::Pending);
    assert!(matches!(
        receive_event(&mut socket).await.event,
        SessionEventDataResponse::TaskUpdated { task } if task == updated
    ));
    let duplicate: TaskResponse = http
        .patch(format!("http://{}{}", daemon.address, child_path))
        .header(IDEMPOTENCY_KEY_HEADER, "task-update")
        .json(&update_request)
        .send()
        .await
        .expect("duplicate Task update response")
        .json()
        .await
        .expect("duplicate Task update JSON");
    assert_eq!(duplicate, updated);
    assert_problem(
        http.patch(format!("http://{}{}", daemon.address, child_path))
            .header(IDEMPOTENCY_KEY_HEADER, "task-update")
            .json(&UpdateTaskRequest {
                objective: "conflict".to_owned(),
                dependency_task_ids: vec![parent.task_id.clone()],
            })
            .send()
            .await
            .expect("Task update conflict response"),
        StatusCode::CONFLICT,
        error_code::IDEMPOTENCY_CONFLICT,
    )
    .await;
    assert_problem(
        http.post(format!(
            "http://{}{}",
            daemon.address, child_transition_path
        ))
        .header(IDEMPOTENCY_KEY_HEADER, "task-not-ready")
        .json(&TransitionTaskRequest {
            state: TaskState::Ready,
        })
        .send()
        .await
        .expect("guarded Task transition response"),
        StatusCode::CONFLICT,
        error_code::INVALID_TASK_TRANSITION,
    )
    .await;

    let parent_transition_path = TASK_TRANSITION_PATH.replace("{task_id}", &parent.task_id);
    let cancelled_parent: TaskResponse = http
        .post(format!(
            "http://{}{}",
            daemon.address, parent_transition_path
        ))
        .header(IDEMPOTENCY_KEY_HEADER, "task-parent-cancel")
        .json(&TransitionTaskRequest {
            state: TaskState::Cancelled,
        })
        .send()
        .await
        .expect("parent Task cancellation response")
        .json()
        .await
        .expect("parent Task cancellation JSON");
    assert!(matches!(
        receive_event(&mut socket).await.event,
        SessionEventDataResponse::TaskStateChanged { task } if task == cancelled_parent
    ));
    let blocked_child: TaskResponse = http
        .post(format!(
            "http://{}{}",
            daemon.address, child_transition_path
        ))
        .header(IDEMPOTENCY_KEY_HEADER, "task-child-block")
        .json(&TransitionTaskRequest {
            state: TaskState::Blocked,
        })
        .send()
        .await
        .expect("child Task blocked response")
        .json()
        .await
        .expect("child Task blocked JSON");
    assert_eq!(blocked_child.state, TaskState::Blocked);
    assert!(matches!(
        receive_event(&mut socket).await.event,
        SessionEventDataResponse::TaskStateChanged { task } if task == blocked_child
    ));

    let unblocked_child: TaskResponse = http
        .patch(format!("http://{}{}", daemon.address, child_path))
        .header(IDEMPOTENCY_KEY_HEADER, "task-child-unblock")
        .json(&UpdateTaskRequest {
            objective: "updated child".to_owned(),
            dependency_task_ids: Vec::new(),
        })
        .send()
        .await
        .expect("child Task unblock response")
        .json()
        .await
        .expect("child Task unblock JSON");
    assert_eq!(unblocked_child.state, TaskState::Pending);
    assert!(matches!(
        receive_event(&mut socket).await.event,
        SessionEventDataResponse::TaskUpdated { task } if task.state == TaskState::Blocked
    ));
    assert!(matches!(
        receive_event(&mut socket).await.event,
        SessionEventDataResponse::TaskStateChanged { task } if task == unblocked_child
    ));

    let ready_request = TransitionTaskRequest {
        state: TaskState::Ready,
    };
    let ready_child: TaskResponse = http
        .post(format!(
            "http://{}{}",
            daemon.address, child_transition_path
        ))
        .header(IDEMPOTENCY_KEY_HEADER, "task-child-ready")
        .json(&ready_request)
        .send()
        .await
        .expect("child Task ready response")
        .json()
        .await
        .expect("child Task ready JSON");
    assert_eq!(ready_child.state, TaskState::Ready);
    assert!(ready_child.dependency_task_ids.is_empty());
    assert!(matches!(
        receive_event(&mut socket).await.event,
        SessionEventDataResponse::TaskStateChanged { task } if task == ready_child
    ));
    let duplicate: TaskResponse = http
        .post(format!(
            "http://{}{}",
            daemon.address, child_transition_path
        ))
        .header(IDEMPOTENCY_KEY_HEADER, "task-child-ready")
        .json(&ready_request)
        .send()
        .await
        .expect("duplicate Task transition response")
        .json()
        .await
        .expect("duplicate Task transition JSON");
    assert_eq!(duplicate, ready_child);
    assert_problem(
        http.post(format!(
            "http://{}{}",
            daemon.address, child_transition_path
        ))
        .header(IDEMPOTENCY_KEY_HEADER, "task-child-ready")
        .json(&TransitionTaskRequest {
            state: TaskState::Cancelled,
        })
        .send()
        .await
        .expect("Task transition conflict response"),
        StatusCode::CONFLICT,
        error_code::IDEMPOTENCY_CONFLICT,
    )
    .await;

    let run: RunResponse = http
        .post(format!(
            "http://{}{}",
            daemon.address,
            SESSION_RUNS_PATH.replace("{session_id}", &session.session_id)
        ))
        .header(IDEMPOTENCY_KEY_HEADER, "task-assignment-run")
        .json(&session.request_with_policy(ApprovalPolicy::Ask))
        .send()
        .await
        .expect("Task assignment Run response")
        .json()
        .await
        .expect("Task assignment Run JSON");
    let child_assignment_path = TASK_ASSIGNMENT_PATH.replace("{task_id}", &child.task_id);
    let assignment_request = AssignTaskRequest {
        run_id: run.run_id.clone(),
    };
    assert_problem(
        http.post(format!(
            "http://{}{}",
            daemon.address, child_assignment_path
        ))
        .json(&assignment_request)
        .send()
        .await
        .expect("missing Task assignment idempotency response"),
        StatusCode::BAD_REQUEST,
        error_code::IDEMPOTENCY_KEY_REQUIRED,
    )
    .await;
    assert_problem(
        http.post(format!(
            "http://{}{}",
            daemon.address, child_assignment_path
        ))
        .header(IDEMPOTENCY_KEY_HEADER, "missing-task-run")
        .json(&AssignTaskRequest {
            run_id: "run_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        })
        .send()
        .await
        .expect("missing Task Run response"),
        StatusCode::NOT_FOUND,
        error_code::RUN_NOT_FOUND,
    )
    .await;
    let assigned_child: TaskResponse = http
        .post(format!(
            "http://{}{}",
            daemon.address, child_assignment_path
        ))
        .header(IDEMPOTENCY_KEY_HEADER, "task-assignment")
        .json(&assignment_request)
        .send()
        .await
        .expect("Task assignment response")
        .json()
        .await
        .expect("Task assignment JSON");
    assert_eq!(
        assigned_child.assigned_run_id.as_deref(),
        Some(run.run_id.as_str())
    );
    loop {
        if matches!(
            receive_event(&mut socket).await.event,
            SessionEventDataResponse::TaskAssigned { task } if task == assigned_child
        ) {
            break;
        }
    }
    let duplicate: TaskResponse = http
        .post(format!(
            "http://{}{}",
            daemon.address, child_assignment_path
        ))
        .header(IDEMPOTENCY_KEY_HEADER, "task-assignment")
        .json(&assignment_request)
        .send()
        .await
        .expect("duplicate Task assignment response")
        .json()
        .await
        .expect("duplicate Task assignment JSON");
    assert_eq!(duplicate, assigned_child);
    assert_problem(
        http.post(format!(
            "http://{}{}",
            daemon.address, child_assignment_path
        ))
        .header(IDEMPOTENCY_KEY_HEADER, "task-assignment")
        .json(&AssignTaskRequest {
            run_id: "run_01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
        })
        .send()
        .await
        .expect("Task assignment conflict response"),
        StatusCode::CONFLICT,
        error_code::IDEMPOTENCY_CONFLICT,
    )
    .await;

    let fetched: TaskResponse = http
        .get(format!(
            "http://{}{}",
            daemon.address,
            TASK_PATH.replace("{task_id}", &child.task_id)
        ))
        .send()
        .await
        .expect("Task read response")
        .json()
        .await
        .expect("Task read JSON");
    assert_eq!(fetched, assigned_child);
    let history: SessionEventsResponse = http
        .get(format!(
            "http://{}{}",
            daemon.address,
            SESSION_EVENTS_PATH.replace("{session_id}", &session.session_id)
        ))
        .send()
        .await
        .expect("Task history response")
        .json()
        .await
        .expect("Task history JSON");
    assert_eq!(
        history
            .events
            .iter()
            .filter(|event| event_kind(event) == "task.created")
            .count(),
        2
    );
    assert_eq!(
        history
            .events
            .iter()
            .filter(|event| event_kind(event) == "task.assigned")
            .count(),
        1
    );
    assert_eq!(
        history
            .events
            .iter()
            .filter(|event| event_kind(event) == "task.updated")
            .count(),
        2
    );
    assert_eq!(
        history
            .events
            .iter()
            .filter(|event| event_kind(event) == "task.state_changed")
            .count(),
        4
    );

    drop(socket);
    daemon.signal("TERM");
    assert!(
        daemon
            .wait_for_exit_within(Duration::from_secs(5))
            .await
            .success()
    );
    let restarted = Daemon::start(binary, &data_directory);
    let recovered: TaskResponse = restarted
        .client()
        .get(format!(
            "http://{}{}",
            restarted.address,
            TASK_PATH.replace("{task_id}", &child.task_id)
        ))
        .send()
        .await
        .expect("recovered Task response")
        .json()
        .await
        .expect("recovered Task JSON");
    assert_eq!(recovered, assigned_child);
}

#[tokio::test]
async fn real_daemon_runs_a_subprocess_publishes_events_and_recovers_the_result() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary Run test directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("Run test repository parent");
    let data_directory = sandbox.path().join("data");
    let daemon = Daemon::start_with_outcome(binary, &data_directory, Some("success"));
    let http = daemon.client();
    let session = create_run_session(&http, &daemon.address, &repositories).await;
    let mut socket = open_event_socket(&daemon).await;

    let start_path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);
    assert_problem(
        http.post(format!("http://{}{}", daemon.address, start_path))
            .json(&session.start_request())
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
            .json(&session.start_request())
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
        .json(&session.start_request())
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
        .json(&session.start_request())
        .send()
        .await
        .expect("duplicate start Run response");
    assert_eq!(duplicate_response.status(), StatusCode::ACCEPTED);
    let duplicate = duplicate_response
        .json::<RunResponse>()
        .await
        .expect("duplicate start Run JSON");
    assert_eq!(duplicate.run_id, queued.run_id);
    let live_events = receive_run_events(&mut socket, &queued.run_id, RunState::Completed).await;
    assert_eq!(
        live_events.iter().map(event_kind).collect::<Vec<_>>(),
        [
            "run.created",
            "run.state_changed",
            "tool_call.requested",
            "tool_call.state_changed",
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

    let cancel_path = RUN_CANCEL_PATH.replace("{run_id}", &completed.run_id);
    let response = http
        .post(format!("http://{}{}", daemon.address, cancel_path))
        .send()
        .await
        .expect("completed Run cancellation response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .json::<RunResponse>()
            .await
            .expect("completed Run cancellation JSON"),
        completed
    );

    let duplicate_response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "run-success")
        .json(&session.start_request())
        .send()
        .await
        .expect("post-terminal duplicate start Run response");
    assert_eq!(duplicate_response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        duplicate_response
            .json::<RunResponse>()
            .await
            .expect("post-terminal duplicate start Run JSON"),
        completed
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
            .json(&session.start_request())
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
async fn real_daemon_stores_fetches_and_deduplicates_large_output_artifacts() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary artifact test directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("artifact test repository parent");
    let data_directory = sandbox.path().join("data");
    let daemon = Daemon::start_with_outcome(binary, &data_directory, Some("large-output"));
    let http = daemon.client();
    let session = create_run_session(&http, &daemon.address, &repositories).await;
    let mut socket = open_event_socket(&daemon).await;
    let start_path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);

    let response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "large-output-first")
        .json(&session.start_request())
        .send()
        .await
        .expect("first large-output Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let first: RunResponse = response.json().await.expect("first queued Run JSON");
    let first_events = receive_run_events(&mut socket, &first.run_id, RunState::Completed).await;
    let first_artifact = first_events
        .iter()
        .find_map(|event| match &event.event {
            SessionEventDataResponse::ArtifactRegistered {
                run_id,
                tool_call_id,
                stream,
                artifact,
            } if run_id == &first.run_id => {
                assert!(tool_call_id.starts_with("tcl_"));
                assert_eq!(*stream, ToolOutputStream::Stdout);
                Some(artifact.clone())
            }
            _ => None,
        })
        .expect("large output artifact Event");
    assert_eq!(first_artifact.media_type, "text/plain; charset=utf-8");
    assert_eq!(first_artifact.size, "180000");
    assert_eq!(first_artifact.content_hash.len(), 64);

    let run_path = RUN_PATH.replace("{run_id}", &first.run_id);
    let completed: RunResponse = http
        .get(format!("http://{}{}", daemon.address, run_path))
        .send()
        .await
        .expect("completed large-output Run response")
        .json()
        .await
        .expect("completed large-output Run JSON");
    let tool_call = completed.tool_calls.first().expect("large-output ToolCall");
    assert_eq!(tool_call.stdout, None);
    assert_eq!(tool_call.stdout_artifact.as_ref(), Some(&first_artifact));
    assert_eq!(tool_call.stderr.as_deref(), Some(""));
    assert_eq!(tool_call.stderr_artifact, None);
    assert_eq!(tool_call.exit_code, Some(0));

    let artifact_path = ARTIFACT_PATH.replace("{content_hash}", &first_artifact.content_hash);
    let response = http
        .get(format!("http://{}{}", daemon.address, artifact_path))
        .send()
        .await
        .expect("artifact response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some(first_artifact.media_type.as_str())
    );
    assert_eq!(
        response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(first_artifact.size.as_str())
    );
    assert_eq!(
        response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|value| value.to_str().ok()),
        Some(format!("\"{}\"", first_artifact.content_hash).as_str())
    );
    assert_eq!(
        response
            .headers()
            .get("x-content-type-options")
            .and_then(|value| value.to_str().ok()),
        Some("nosniff")
    );
    let bytes = response.bytes().await.expect("artifact bytes");
    assert_eq!(
        bytes.as_ref(),
        "kiln large output\n".repeat(10_000).as_bytes()
    );
    let content_hash = Sha256::digest(&bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(content_hash, first_artifact.content_hash);

    let response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "large-output-second")
        .json(&session.start_request())
        .send()
        .await
        .expect("second large-output Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let second: RunResponse = response.json().await.expect("second queued Run JSON");
    let second_events = receive_run_events(&mut socket, &second.run_id, RunState::Completed).await;
    let second_artifact = second_events
        .iter()
        .find_map(|event| match &event.event {
            SessionEventDataResponse::ArtifactRegistered {
                run_id, artifact, ..
            } if run_id == &second.run_id => Some(artifact.clone()),
            _ => None,
        })
        .expect("repeated large output artifact Event");
    assert_eq!(second_artifact, first_artifact);

    assert_problem(
        http.get(format!(
            "http://{}{}",
            daemon.address,
            ARTIFACT_PATH.replace("{content_hash}", "invalid")
        ))
        .send()
        .await
        .expect("invalid artifact hash response"),
        StatusCode::BAD_REQUEST,
        error_code::INVALID_CONTENT_HASH,
    )
    .await;
    assert_problem(
        http.get(format!(
            "http://{}{}",
            daemon.address,
            ARTIFACT_PATH.replace("{content_hash}", &"0".repeat(64))
        ))
        .send()
        .await
        .expect("missing artifact response"),
        StatusCode::NOT_FOUND,
        error_code::ARTIFACT_NOT_FOUND,
    )
    .await;

    drop(socket);
    drop(daemon);

    let restarted = Daemon::start(binary, &data_directory);
    let http = restarted.client();
    let artifact_path = ARTIFACT_PATH.replace("{content_hash}", &first_artifact.content_hash);
    let response = http
        .get(format!("http://{}{}", restarted.address, artifact_path))
        .send()
        .await
        .expect("restarted artifact response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.bytes().await.expect("restarted artifact bytes"),
        bytes
    );

    let recovered: RunResponse = http
        .get(format!("http://{}{}", restarted.address, run_path))
        .send()
        .await
        .expect("restarted large-output Run response")
        .json()
        .await
        .expect("restarted large-output Run JSON");
    assert_eq!(
        recovered.tool_calls[0].stdout_artifact.as_ref(),
        Some(&first_artifact)
    );

    let events_path = SESSION_EVENTS_PATH.replace("{session_id}", &session.session_id);
    let history: SessionEventsResponse = http
        .get(format!("http://{}{}", restarted.address, events_path))
        .send()
        .await
        .expect("restarted artifact Event history response")
        .json()
        .await
        .expect("restarted artifact Event history JSON");
    let artifacts = history
        .events
        .iter()
        .filter_map(|event| match &event.event {
            SessionEventDataResponse::ArtifactRegistered { artifact, .. } => Some(artifact.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(artifacts, [first_artifact.clone(), first_artifact]);
}

#[tokio::test]
async fn ask_approval_survives_restart_resumes_once_and_is_idempotent() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary approval test directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("approval repository parent");
    let data_directory = sandbox.path().join("data");
    let daemon = Daemon::start_with_outcome(binary, &data_directory, Some("success"));
    let http = daemon.client();
    let session = create_run_session(&http, &daemon.address, &repositories).await;
    let mut socket = open_event_socket(&daemon).await;

    let start_path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);
    let response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "ask-restart")
        .json(&session.request_with_policy(ApprovalPolicy::Ask))
        .send()
        .await
        .expect("Ask Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let queued: RunResponse = response.json().await.expect("Ask Run JSON");
    let waiting_events =
        receive_run_events(&mut socket, &queued.run_id, RunState::WaitingForApproval).await;
    assert_eq!(
        waiting_events.iter().map(event_kind).collect::<Vec<_>>(),
        [
            "run.created",
            "run.state_changed",
            "tool_call.requested",
            "tool_call.state_changed",
            "approval.requested",
            "run.state_changed",
        ]
    );

    let run_path = RUN_PATH.replace("{run_id}", &queued.run_id);
    let waiting: RunResponse = http
        .get(format!("http://{}{}", daemon.address, run_path))
        .send()
        .await
        .expect("waiting Run response")
        .json()
        .await
        .expect("waiting Run JSON");
    assert_eq!(waiting.state, RunState::WaitingForApproval);
    assert_eq!(waiting.tool_calls.len(), 1);
    assert_eq!(waiting.tool_calls[0].state, ToolCallState::AwaitingApproval);
    assert_eq!(waiting.approvals.len(), 1);
    assert_eq!(waiting.approvals[0].state, ApprovalState::Pending);
    let tool_call_id = waiting.tool_calls[0].tool_call_id.clone();
    let after = waiting_events.last().unwrap().cursor.clone();
    let original_token = daemon.token.clone();
    drop(socket);
    drop(daemon);

    let restarted = Daemon::start_with_outcome(binary, &data_directory, Some("success"));
    assert_eq!(restarted.token, original_token);
    let http = restarted.client();
    let (mut socket, current_cursor) = open_event_socket_after(&restarted, Some(&after)).await;
    assert_eq!(current_cursor.as_deref(), Some(after.as_str()));
    let approval_path = TOOL_CALL_APPROVAL_PATH.replace("{tool_call_id}", &tool_call_id);
    let response = http
        .post(format!("http://{}{}", restarted.address, approval_path))
        .header(IDEMPOTENCY_KEY_HEADER, "approve-once")
        .json(&ApprovalDecisionRequest {
            decision: ApprovalDecision::Approved,
        })
        .send()
        .await
        .expect("approval response");
    assert_eq!(response.status(), StatusCode::OK);
    let approved: RunResponse = response.json().await.expect("approval JSON");
    assert_eq!(approved.state, RunState::Running);
    assert_eq!(approved.approvals[0].state, ApprovalState::Approved);
    assert_eq!(approved.tool_calls[0].state, ToolCallState::Ready);

    let resumed = receive_run_events(&mut socket, &queued.run_id, RunState::Completed).await;
    assert_eq!(
        resumed.iter().map(event_kind).collect::<Vec<_>>(),
        [
            "approval.decided",
            "tool_call.state_changed",
            "run.state_changed",
            "tool_call.state_changed",
            "tool_call.output",
            "tool_call.output",
            "tool_call.state_changed",
            "run.state_changed",
        ]
    );

    let duplicate = http
        .post(format!("http://{}{}", restarted.address, approval_path))
        .header(IDEMPOTENCY_KEY_HEADER, "approve-once")
        .json(&ApprovalDecisionRequest {
            decision: ApprovalDecision::Approved,
        })
        .send()
        .await
        .expect("duplicate approval response");
    assert_eq!(duplicate.status(), StatusCode::OK);
    assert_eq!(
        duplicate
            .json::<RunResponse>()
            .await
            .expect("duplicate approval JSON")
            .state,
        RunState::Completed
    );
    assert_problem(
        http.post(format!("http://{}{}", restarted.address, approval_path))
            .header(IDEMPOTENCY_KEY_HEADER, "approve-once")
            .json(&ApprovalDecisionRequest {
                decision: ApprovalDecision::Rejected,
            })
            .send()
            .await
            .expect("conflicting approval response"),
        StatusCode::CONFLICT,
        error_code::IDEMPOTENCY_CONFLICT,
    )
    .await;
    assert_problem(
        http.post(format!("http://{}{}", restarted.address, approval_path))
            .header(IDEMPOTENCY_KEY_HEADER, "approve-again")
            .json(&ApprovalDecisionRequest {
                decision: ApprovalDecision::Approved,
            })
            .send()
            .await
            .expect("second approval response"),
        StatusCode::CONFLICT,
        error_code::APPROVAL_ALREADY_DECIDED,
    )
    .await;

    let events_path = SESSION_EVENTS_PATH.replace("{session_id}", &session.session_id);
    let history: SessionEventsResponse = http
        .get(format!("http://{}{}", restarted.address, events_path))
        .send()
        .await
        .expect("approval history response")
        .json()
        .await
        .expect("approval history JSON");
    assert_eq!(
        history
            .events
            .iter()
            .filter(|event| event_kind(event) == "approval.decided")
            .count(),
        1
    );
}

#[tokio::test]
async fn concurrent_approval_decisions_have_one_winner() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary concurrent approval directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("concurrent approval repository parent");
    let data_directory = sandbox.path().join("data");
    let daemon = Daemon::start_with_outcome(binary, &data_directory, Some("success"));
    let http = daemon.client();
    let session = create_run_session(&http, &daemon.address, &repositories).await;
    let mut socket = open_event_socket(&daemon).await;

    let start_path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);
    let response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "concurrent-ask")
        .json(&session.request_with_policy(ApprovalPolicy::Ask))
        .send()
        .await
        .expect("concurrent approval Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let queued: RunResponse = response.json().await.expect("concurrent approval Run JSON");
    receive_run_events(&mut socket, &queued.run_id, RunState::WaitingForApproval).await;

    let run_path = RUN_PATH.replace("{run_id}", &queued.run_id);
    let waiting: RunResponse = http
        .get(format!("http://{}{}", daemon.address, run_path))
        .send()
        .await
        .expect("concurrent waiting Run response")
        .json()
        .await
        .expect("concurrent waiting Run JSON");
    let approval_path =
        TOOL_CALL_APPROVAL_PATH.replace("{tool_call_id}", &waiting.tool_calls[0].tool_call_id);

    let approve = http
        .post(format!("http://{}{}", daemon.address, approval_path))
        .header(IDEMPOTENCY_KEY_HEADER, "concurrent-approve")
        .json(&ApprovalDecisionRequest {
            decision: ApprovalDecision::Approved,
        });
    let reject = http
        .post(format!("http://{}{}", daemon.address, approval_path))
        .header(IDEMPOTENCY_KEY_HEADER, "concurrent-reject")
        .json(&ApprovalDecisionRequest {
            decision: ApprovalDecision::Rejected,
        });
    let (approve, reject) = tokio::join!(approve.send(), reject.send());
    let approve = approve.expect("approve decision response");
    let reject = reject.expect("reject decision response");
    let approve_status = approve.status();
    let approve_body = approve.bytes().await.expect("approve decision body");
    let reject_status = reject.status();
    let reject_body = reject.bytes().await.expect("reject decision body");

    let approved_won = approve_status == StatusCode::OK;
    assert_ne!(approved_won, reject_status == StatusCode::OK);
    let winner: RunResponse = if approved_won {
        serde_json::from_slice(&approve_body).expect("winning approval JSON")
    } else {
        serde_json::from_slice(&reject_body).expect("winning rejection JSON")
    };
    let loser_problem: ProblemDetails = if approved_won {
        serde_json::from_slice(&reject_body).expect("losing rejection problem JSON")
    } else {
        serde_json::from_slice(&approve_body).expect("losing approval problem JSON")
    };
    assert_eq!(
        if approved_won {
            reject_status
        } else {
            approve_status
        },
        StatusCode::CONFLICT
    );
    assert_eq!(loser_problem.code, error_code::APPROVAL_ALREADY_DECIDED);

    let expected_run_state = if approved_won {
        RunState::Completed
    } else {
        RunState::Failed
    };
    let expected_approval_state = if approved_won {
        ApprovalState::Approved
    } else {
        ApprovalState::Rejected
    };
    let expected_tool_state = if approved_won {
        ToolCallState::Completed
    } else {
        ToolCallState::Denied
    };
    receive_run_events(&mut socket, &queued.run_id, expected_run_state.clone()).await;

    assert_eq!(winner.approvals[0].state, expected_approval_state);
    let response = http
        .get(format!("http://{}{}", daemon.address, run_path))
        .send()
        .await
        .expect("terminal concurrent approval Run response");
    assert_eq!(response.status(), StatusCode::OK);
    let terminal: RunResponse = response
        .json()
        .await
        .expect("terminal concurrent approval Run JSON");
    assert_eq!(terminal.state, expected_run_state);
    assert_eq!(terminal.approvals[0].state, expected_approval_state);
    assert_eq!(terminal.tool_calls[0].state, expected_tool_state);

    let events_path = SESSION_EVENTS_PATH.replace("{session_id}", &session.session_id);
    let history: SessionEventsResponse = http
        .get(format!("http://{}{}", daemon.address, events_path))
        .send()
        .await
        .expect("concurrent approval history response")
        .json()
        .await
        .expect("concurrent approval history JSON");
    assert_eq!(
        history
            .events
            .iter()
            .filter(|event| {
                event_kind(event) == "approval.decided"
                    && event_belongs_to_run(event, &queued.run_id)
            })
            .count(),
        1
    );
}

#[tokio::test]
async fn rejection_and_read_only_deny_without_starting_a_process() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary denied Run test directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("denied Run repository parent");
    let data_directory = sandbox.path().join("data");
    let pid_file = repositories.join("run/subprocess-pids.json");
    let daemon = Daemon::start_with_pid_file(binary, &data_directory, "blocking-tree", &pid_file);
    let http = daemon.client();
    let session = create_run_session(&http, &daemon.address, &repositories).await;
    let mut socket = open_event_socket(&daemon).await;
    let start_path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);

    let response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "reject-run")
        .json(&session.request_with_policy(ApprovalPolicy::Ask))
        .send()
        .await
        .expect("rejected Ask Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let queued: RunResponse = response.json().await.expect("rejected Ask Run JSON");
    receive_run_events(&mut socket, &queued.run_id, RunState::WaitingForApproval).await;
    assert!(!pid_file.exists());

    let run_path = RUN_PATH.replace("{run_id}", &queued.run_id);
    let waiting: RunResponse = http
        .get(format!("http://{}{}", daemon.address, run_path))
        .send()
        .await
        .expect("rejected waiting Run response")
        .json()
        .await
        .expect("rejected waiting Run JSON");
    let approval_path =
        TOOL_CALL_APPROVAL_PATH.replace("{tool_call_id}", &waiting.tool_calls[0].tool_call_id);
    let response = http
        .post(format!("http://{}{}", daemon.address, approval_path))
        .header(IDEMPOTENCY_KEY_HEADER, "reject-once")
        .json(&ApprovalDecisionRequest {
            decision: ApprovalDecision::Rejected,
        })
        .send()
        .await
        .expect("rejection response");
    assert_eq!(response.status(), StatusCode::OK);
    let rejected: RunResponse = response.json().await.expect("rejection JSON");
    assert_eq!(rejected.approvals[0].state, ApprovalState::Rejected);
    assert_eq!(rejected.tool_calls[0].state, ToolCallState::Denied);
    let rejected_events = receive_run_events(&mut socket, &queued.run_id, RunState::Failed).await;
    assert_eq!(
        rejected_events.iter().map(event_kind).collect::<Vec<_>>(),
        [
            "approval.decided",
            "tool_call.denied",
            "run.state_changed",
            "run.state_changed",
        ]
    );
    assert!(!pid_file.exists());

    let response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "read-only-run")
        .json(&session.request_with_policy(ApprovalPolicy::ReadOnly))
        .send()
        .await
        .expect("read-only Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let read_only: RunResponse = response.json().await.expect("read-only Run JSON");
    let read_only_events =
        receive_run_events(&mut socket, &read_only.run_id, RunState::Failed).await;
    assert_eq!(
        read_only_events.iter().map(event_kind).collect::<Vec<_>>(),
        [
            "run.created",
            "run.state_changed",
            "tool_call.requested",
            "tool_call.denied",
            "run.state_changed",
        ]
    );
    assert!(!pid_file.exists());
}

#[tokio::test]
async fn cancelling_a_waiting_run_rejects_approval_without_starting_a_process() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary waiting cancellation directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("waiting cancellation repository parent");
    let data_directory = sandbox.path().join("data");
    let pid_file = repositories.join("run/subprocess-pids.json");
    let daemon = Daemon::start_with_pid_file(binary, &data_directory, "blocking-tree", &pid_file);
    let http = daemon.client();
    let session = create_run_session(&http, &daemon.address, &repositories).await;
    let mut socket = open_event_socket(&daemon).await;
    let start_path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);
    let response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "cancel-waiting")
        .json(&session.request_with_policy(ApprovalPolicy::Ask))
        .send()
        .await
        .expect("waiting cancellation Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let queued: RunResponse = response
        .json()
        .await
        .expect("waiting cancellation Run JSON");
    receive_run_events(&mut socket, &queued.run_id, RunState::WaitingForApproval).await;

    let cancel_path = RUN_CANCEL_PATH.replace("{run_id}", &queued.run_id);
    let response = http
        .post(format!("http://{}{}", daemon.address, cancel_path))
        .send()
        .await
        .expect("waiting cancellation response");
    assert_eq!(response.status(), StatusCode::OK);
    let cancelled: RunResponse = response.json().await.expect("waiting cancellation JSON");
    assert_eq!(cancelled.state, RunState::Cancelled);
    assert_eq!(cancelled.approvals[0].state, ApprovalState::Rejected);
    assert_eq!(cancelled.tool_calls[0].state, ToolCallState::Denied);
    let events = receive_run_events(&mut socket, &queued.run_id, RunState::Cancelled).await;
    assert_eq!(
        events.iter().map(event_kind).collect::<Vec<_>>(),
        [
            "run.cancellation_requested",
            "run.state_changed",
            "approval.decided",
            "tool_call.denied",
            "run.state_changed",
        ]
    );
    assert!(!pid_file.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn an_out_of_root_symlink_is_rejected_before_a_run_is_created() {
    use std::os::unix::fs::symlink;

    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary path-scope test directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("path-scope repository parent");
    let data_directory = sandbox.path().join("data");
    let daemon = Daemon::start_with_outcome(binary, &data_directory, Some("success"));
    let http = daemon.client();
    let session = create_run_session(&http, &daemon.address, &repositories).await;
    let outside = sandbox.path().join("outside");
    std::fs::create_dir(&outside).expect("outside directory");
    symlink(&outside, session.repository.join("escape")).expect("escape symlink");

    let mut request = session.start_request();
    request.relative_directory = "escape".to_owned();
    let start_path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);
    assert_problem(
        http.post(format!("http://{}{}", daemon.address, start_path))
            .header(IDEMPOTENCY_KEY_HEADER, "escape-run")
            .json(&request)
            .send()
            .await
            .expect("escape Run response"),
        StatusCode::BAD_REQUEST,
        error_code::PATH_OUTSIDE_WORKSPACE_ROOT,
    )
    .await;

    let events_path = SESSION_EVENTS_PATH.replace("{session_id}", &session.session_id);
    let history: SessionEventsResponse = http
        .get(format!("http://{}{}", daemon.address, events_path))
        .send()
        .await
        .expect("escape history response")
        .json()
        .await
        .expect("escape history JSON");
    assert_eq!(
        history
            .events
            .iter()
            .filter(|event| event_kind(event) == "run.created")
            .count(),
        0
    );
}

#[cfg(unix)]
#[tokio::test]
async fn replacing_registered_root_with_outside_symlink_fails_approved_run() {
    use std::os::unix::fs::symlink;

    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary replaced-root test directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("replaced-root repository parent");
    let data_directory = sandbox.path().join("data");
    let daemon = Daemon::start_with_outcome(binary, &data_directory, Some("success"));
    let http = daemon.client();
    let session = create_run_session(&http, &daemon.address, &repositories).await;
    let mut socket = open_event_socket(&daemon).await;

    let start_path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);
    let response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "replaced-root")
        .json(&session.request_with_policy(ApprovalPolicy::Ask))
        .send()
        .await
        .expect("replaced-root Ask Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let queued: RunResponse = response.json().await.expect("replaced-root Ask Run JSON");
    receive_run_events(&mut socket, &queued.run_id, RunState::WaitingForApproval).await;

    let run_path = RUN_PATH.replace("{run_id}", &queued.run_id);
    let waiting: RunResponse = http
        .get(format!("http://{}{}", daemon.address, run_path))
        .send()
        .await
        .expect("replaced-root waiting Run response")
        .json()
        .await
        .expect("replaced-root waiting Run JSON");
    let approval_path =
        TOOL_CALL_APPROVAL_PATH.replace("{tool_call_id}", &waiting.tool_calls[0].tool_call_id);

    let original_repository = session.repository.with_file_name("run-original");
    std::fs::rename(&session.repository, &original_repository)
        .expect("replace registered repository root");
    let outside = sandbox.path().join("outside");
    std::fs::create_dir(&outside).expect("outside directory");
    symlink(&outside, &session.repository).expect("outside repository-root symlink");

    let response = http
        .post(format!("http://{}{}", daemon.address, approval_path))
        .header(IDEMPOTENCY_KEY_HEADER, "approve-replaced-root")
        .json(&ApprovalDecisionRequest {
            decision: ApprovalDecision::Approved,
        })
        .send()
        .await
        .expect("replaced-root approval response");
    assert_eq!(response.status(), StatusCode::OK);
    let approved: RunResponse = response.json().await.expect("replaced-root approval JSON");
    assert_eq!(approved.state, RunState::Running);
    assert_eq!(approved.approvals[0].state, ApprovalState::Approved);

    let events = receive_run_events(&mut socket, &queued.run_id, RunState::Failed).await;
    assert_eq!(
        events.iter().map(event_kind).collect::<Vec<_>>(),
        [
            "approval.decided",
            "tool_call.state_changed",
            "run.state_changed",
            "tool_call.state_changed",
            "tool_call.output",
            "tool_call.state_changed",
            "run.state_changed",
        ]
    );
    assert!(events.iter().any(|event| {
        matches!(
            &event.event,
            SessionEventDataResponse::ToolCallOutput {
                stream: ToolOutputStream::Stderr,
                content,
                ..
            }
                if content == "path is outside workspace root"
        )
    }));

    let response = http
        .get(format!("http://{}{}", daemon.address, run_path))
        .send()
        .await
        .expect("replaced-root terminal Run response");
    assert_eq!(response.status(), StatusCode::OK);
    let failed: RunResponse = response
        .json()
        .await
        .expect("replaced-root terminal Run JSON");
    assert_eq!(failed.state, RunState::Failed);
    assert_eq!(failed.tool_calls.len(), 1);
    let tool_call = &failed.tool_calls[0];
    assert_eq!(tool_call.state, ToolCallState::Failed);
    assert_eq!(tool_call.stdout.as_deref(), Some(""));
    assert_eq!(
        tool_call.stderr.as_deref(),
        Some("path is outside workspace root")
    );
    assert_eq!(tool_call.exit_code, None);

    let events_path = SESSION_EVENTS_PATH.replace("{session_id}", &session.session_id);
    let history: SessionEventsResponse = http
        .get(format!("http://{}{}", daemon.address, events_path))
        .send()
        .await
        .expect("replaced-root history response")
        .json()
        .await
        .expect("replaced-root history JSON");
    assert!(history.events.iter().any(|event| {
        event_belongs_to_run(event, &queued.run_id)
            && matches!(
                &event.event,
                SessionEventDataResponse::ToolCallOutput {
                    stream: ToolOutputStream::Stderr,
                    content,
                    ..
                }
                    if content == "path is outside workspace root"
            )
    }));
    assert!(matches!(
        &history
            .events
            .iter()
            .rev()
            .find(|event| event_belongs_to_run(event, &queued.run_id))
            .expect("durable terminal Run Event")
            .event,
        SessionEventDataResponse::RunStateChanged {
            state: RunState::Failed,
            ..
        }
    ));
}

#[tokio::test]
async fn cancellation_is_shared_kills_the_process_group_and_survives_restart() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary cancellation test directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("cancellation repository parent");
    let data_directory = sandbox.path().join("data");
    let pid_file = repositories.join("run/subprocess-pids.json");
    let daemon = Daemon::start_with_pid_file(binary, &data_directory, "blocking-tree", &pid_file);
    let http = daemon.client();
    let session = create_run_session(&http, &daemon.address, &repositories).await;

    let start_path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);
    let response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "cancel-tree")
        .json(&session.start_request())
        .send()
        .await
        .expect("blocking Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let queued: RunResponse = response.json().await.expect("blocking Run JSON");
    let (parent_pid, child_pid) = fixture_pids(&pid_file).await;
    assert!(process_exists(parent_pid));
    assert!(process_exists(child_pid));
    assert!(process_group_exists(parent_pid));

    let cancel_path = RUN_CANCEL_PATH.replace("{run_id}", &queued.run_id);
    let (first, second) = tokio::join!(
        http.post(format!("http://{}{}", daemon.address, cancel_path))
            .send(),
        http.post(format!("http://{}{}", daemon.address, cancel_path))
            .send(),
    );
    let first = first.expect("first cancellation response");
    let second = second.expect("second cancellation response");
    let first_status = first.status();
    let first_body = first.bytes().await.expect("first cancellation body");
    let second_status = second.status();
    let second_body = second.bytes().await.expect("second cancellation body");
    assert_eq!(
        (first_status, second_status),
        (StatusCode::OK, StatusCode::OK),
        "first: {}; second: {}",
        String::from_utf8_lossy(&first_body),
        String::from_utf8_lossy(&second_body),
    );
    let cancelled: RunResponse =
        serde_json::from_slice(&first_body).expect("first cancellation JSON");
    assert_eq!(
        serde_json::from_slice::<RunResponse>(&second_body).expect("second cancellation JSON"),
        cancelled
    );
    assert_eq!(cancelled.state, RunState::Cancelled);
    assert_eq!(cancelled.tool_calls.len(), 1);
    assert_eq!(cancelled.tool_calls[0].state, ToolCallState::Cancelled);
    assert!(cancelled.tool_calls[0].stdout.is_some());
    assert!(cancelled.tool_calls[0].stderr.is_some());
    assert_eq!(cancelled.tool_calls[0].exit_code, None);
    assert!(!process_exists(parent_pid));
    assert!(!process_exists(child_pid));
    assert!(!process_group_exists(parent_pid));

    let events_path = SESSION_EVENTS_PATH.replace("{session_id}", &session.session_id);
    let response = http
        .get(format!("http://{}{}", daemon.address, events_path))
        .send()
        .await
        .expect("cancelled Run Event history response");
    assert_eq!(response.status(), StatusCode::OK);
    let history: SessionEventsResponse = response.json().await.expect("cancel history JSON");
    let run_events = history
        .events
        .iter()
        .filter(|event| event_belongs_to_run(event, &queued.run_id))
        .collect::<Vec<_>>();
    assert_eq!(
        run_events
            .iter()
            .filter(|event| event_kind(event) == "run.cancellation_requested")
            .count(),
        1
    );
    assert_eq!(
        run_events
            .iter()
            .filter_map(|event| match &event.event {
                SessionEventDataResponse::RunStateChanged { state, .. } => Some(state.clone()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        [RunState::Running, RunState::Cancelling, RunState::Cancelled,]
    );
    assert!(matches!(
        &run_events
            .last()
            .expect("terminal cancellation Event")
            .event,
        SessionEventDataResponse::RunStateChanged {
            state: RunState::Cancelled,
            ..
        }
    ));

    let run_path = RUN_PATH.replace("{run_id}", &queued.run_id);
    drop(daemon);
    let restarted = Daemon::start(binary, &data_directory);
    let response = http
        .get(format!("http://{}{}", restarted.address, run_path))
        .send()
        .await
        .expect("recovered cancelled Run response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .json::<RunResponse>()
            .await
            .expect("recovered cancelled Run JSON"),
        cancelled
    );
}

#[tokio::test]
async fn sigterm_gracefully_cancels_active_work_before_exit() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary graceful shutdown test directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("shutdown repository parent");
    let data_directory = sandbox.path().join("data");
    let pid_file = repositories.join("run/shutdown-pids.json");
    let mut daemon =
        Daemon::start_with_pid_file(binary, &data_directory, "blocking-tree", &pid_file);
    let http = daemon.client();
    let session = create_run_session(&http, &daemon.address, &repositories).await;
    let start_path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);
    let response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "shutdown-tree")
        .json(&session.start_request())
        .send()
        .await
        .expect("shutdown Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let queued: RunResponse = response.json().await.expect("shutdown Run JSON");
    let (parent_pid, child_pid) = fixture_pids(&pid_file).await;

    daemon.signal("TERM");
    assert!(daemon.wait_for_exit().success());
    assert!(!process_exists(parent_pid));
    assert!(!process_exists(child_pid));
    assert!(!process_group_exists(parent_pid));

    let restarted = Daemon::start(binary, &data_directory);
    let run_path = RUN_PATH.replace("{run_id}", &queued.run_id);
    let response = http
        .get(format!("http://{}{}", restarted.address, run_path))
        .send()
        .await
        .expect("Run after graceful shutdown response");
    assert_eq!(response.status(), StatusCode::OK);
    let cancelled: RunResponse = response.json().await.expect("Run after shutdown JSON");
    assert_eq!(cancelled.state, RunState::Cancelled);
    assert_eq!(cancelled.tool_calls[0].state, ToolCallState::Cancelled);
}

#[test]
fn sigint_gracefully_stops_an_idle_daemon() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary SIGINT test directory");
    let data_directory = sandbox.path().join("data");
    let mut daemon = Daemon::start(binary, &data_directory);

    daemon.signal("INT");
    assert!(daemon.wait_for_exit().success());
}

#[tokio::test]
async fn reconnect_replays_exact_durable_suffix_across_restart() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary reconnect test directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("reconnect repository parent");
    let data_directory = sandbox.path().join("data");
    let daemon = Daemon::start_with_outcome(binary, &data_directory, Some("success"));
    let http = daemon.client();
    let session = create_run_session(&http, &daemon.address, &repositories).await;
    let mut live_socket = open_event_socket(&daemon).await;

    let start_path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);
    let response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "reconnect-run")
        .json(&session.start_request())
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
        open_event_socket_after(&daemon, Some(&after)).await;
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
        open_event_socket_after(&restarted, Some(&after)).await;
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
    let daemon = Daemon::start_with_outcome(binary, &data_directory, Some("failure"));
    let http = daemon.client();
    let session = create_run_session(&http, &daemon.address, &repositories).await;
    let mut socket = open_event_socket(&daemon).await;

    let start_path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);
    let response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "run-failure")
        .json(&session.start_request())
        .send()
        .await
        .expect("start failed Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let queued: RunResponse = response.json().await.expect("queued failed Run JSON");

    let events = receive_run_events(&mut socket, &queued.run_id, RunState::Failed).await;
    assert_eq!(events.len(), 9);
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
    let daemon = Daemon::start(binary, &data_directory);
    let http = daemon.client();
    let first_session = create_run_session(&http, &daemon.address, &first_repositories).await;
    let second_session = create_run_session(&http, &daemon.address, &second_repositories).await;
    let mut socket = open_event_socket(&daemon).await;

    let first_path = SESSION_RUNS_PATH.replace("{session_id}", &first_session.session_id);
    let second_path = SESSION_RUNS_PATH.replace("{session_id}", &second_session.session_id);
    let (first_response, second_response) = tokio::join!(
        http.post(format!("http://{}{}", daemon.address, first_path))
            .header(IDEMPOTENCY_KEY_HEADER, "first-run")
            .json(&first_session.start_request())
            .send(),
        http.post(format!("http://{}{}", daemon.address, second_path))
            .header(IDEMPOTENCY_KEY_HEADER, "second-run")
            .json(&second_session.start_request())
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

    assert_eq!(events.len(), 18);
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
    let daemon = Daemon::start(binary, &data_directory);
    let http = daemon.client();
    let session = create_run_session(&http, &daemon.address, &repositories).await;
    let mut socket = open_event_socket(&daemon).await;
    let path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);

    let (first_response, second_response) = tokio::join!(
        http.post(format!("http://{}{}", daemon.address, path))
            .header(IDEMPOTENCY_KEY_HEADER, "concurrent-same-key")
            .json(&session.start_request())
            .send(),
        http.post(format!("http://{}{}", daemon.address, path))
            .header(IDEMPOTENCY_KEY_HEADER, "concurrent-same-key")
            .json(&session.start_request())
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
    assert_eq!(first.run_id, second.run_id);

    let events = receive_run_events(&mut socket, &first.run_id, RunState::Completed).await;
    assert_eq!(events.len(), 9);
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

#[tokio::test]
async fn complete_first_vertical_slice_is_repeatable_across_reconnect_and_restart() {
    tokio::time::timeout(Duration::from_secs(5), complete_first_vertical_slice())
        .await
        .expect("complete vertical slice finishes within the measured fixture tripwire");
}

async fn complete_first_vertical_slice() {
    let binary = env!("CARGO_BIN_EXE_kilnd");
    let sandbox = tempfile::tempdir().expect("temporary vertical-slice directory");
    let repositories = sandbox.path().join("repositories");
    std::fs::create_dir(&repositories).expect("vertical-slice repository parent");
    let primary = git_repository(&repositories, "primary");
    let companion = git_repository(&repositories, "companion");
    let data_directory = sandbox.path().join("data");
    let approval_pid_file = primary.join("approval-pids.json");

    let daemon =
        Daemon::start_with_pid_file(binary, &data_directory, "large-output", &approval_pid_file);
    let idle_rss_kb = daemon.resident_memory_kb();
    let http = daemon.client();
    let workspace_request = CreateWorkspaceRequest {
        name: "Vertical slice Workspace".to_owned(),
        roots: vec![
            WorkspaceRootRequest {
                name: "primary".to_owned(),
                path: canonical_string(&primary),
            },
            WorkspaceRootRequest {
                name: "companion".to_owned(),
                path: canonical_string(&companion),
            },
        ],
    };
    let response = http
        .post(format!("http://{}{WORKSPACES_PATH}", daemon.address))
        .json(&workspace_request)
        .send()
        .await
        .expect("vertical-slice Workspace response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let workspace: WorkspaceResponse = response
        .json()
        .await
        .expect("vertical-slice Workspace JSON");
    assert_eq!(workspace.name, workspace_request.name);
    assert_eq!(workspace.roots.len(), 2);
    assert_eq!(
        workspace.roots[0].canonical_path,
        canonical_string(&primary)
    );
    assert_eq!(
        workspace.roots[1].canonical_path,
        canonical_string(&companion)
    );

    let sessions_path = WORKSPACE_SESSIONS_PATH.replace("{workspace_id}", &workspace.workspace_id);
    let response = http
        .post(format!("http://{}{}", daemon.address, sessions_path))
        .send()
        .await
        .expect("vertical-slice Session response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let session: SessionResponse = response.json().await.expect("vertical-slice Session JSON");
    assert_eq!(session.workspace_id, workspace.workspace_id);

    let messages_path = SESSION_MESSAGES_PATH.replace("{session_id}", &session.session_id);
    let response = http
        .post(format!("http://{}{}", daemon.address, messages_path))
        .json(&AppendMessageRequest {
            content: "Run the complete deterministic vertical slice".to_owned(),
        })
        .send()
        .await
        .expect("vertical-slice Message response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let message: MessageResponse = response.json().await.expect("vertical-slice Message JSON");
    assert_eq!(message.session_id, session.session_id);

    let mut socket = open_event_socket(&daemon).await;
    let start_path = SESSION_RUNS_PATH.replace("{session_id}", &session.session_id);
    let run_request = StartRunRequest {
        approval_policy: ApprovalPolicy::Ask,
        workspace_root_id: workspace.roots[0].workspace_root_id.clone(),
        relative_directory: ".".to_owned(),
    };
    let response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "vertical-slice-artifact")
        .json(&run_request)
        .send()
        .await
        .expect("vertical-slice approval Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let queued: RunResponse = response
        .json()
        .await
        .expect("vertical-slice approval Run JSON");
    let waiting_events =
        receive_run_events(&mut socket, &queued.run_id, RunState::WaitingForApproval).await;
    assert_eq!(
        waiting_events.iter().map(event_kind).collect::<Vec<_>>(),
        [
            "run.created",
            "run.state_changed",
            "tool_call.requested",
            "tool_call.state_changed",
            "approval.requested",
            "run.state_changed",
        ]
    );
    assert!(
        !approval_pid_file.exists(),
        "Ask policy must not execute before approval"
    );
    let replay_after = waiting_events[0].cursor.clone();
    let waiting: RunResponse = http
        .get(format!(
            "http://{}{}",
            daemon.address,
            RUN_PATH.replace("{run_id}", &queued.run_id)
        ))
        .send()
        .await
        .expect("vertical-slice waiting Run response")
        .json()
        .await
        .expect("vertical-slice waiting Run JSON");
    assert_eq!(waiting.state, RunState::WaitingForApproval);
    assert_eq!(waiting.approvals[0].state, ApprovalState::Pending);
    assert_eq!(waiting.tool_calls[0].state, ToolCallState::AwaitingApproval);
    let tool_call_id = waiting.tool_calls[0].tool_call_id.clone();
    let original_token = daemon.token.clone();
    drop(socket);
    drop(daemon);

    let mut daemon =
        Daemon::start_with_pid_file(binary, &data_directory, "large-output", &approval_pid_file);
    assert_eq!(daemon.token, original_token);
    let http = daemon.client();
    let workspace_path = WORKSPACE_PATH.replace("{workspace_id}", &workspace.workspace_id);
    let recovered_workspace: WorkspaceResponse = http
        .get(format!("http://{}{}", daemon.address, workspace_path))
        .send()
        .await
        .expect("recovered vertical-slice Workspace response")
        .json()
        .await
        .expect("recovered vertical-slice Workspace JSON");
    assert_eq!(recovered_workspace, workspace);
    let session_path = SESSION_PATH.replace("{session_id}", &session.session_id);
    let recovered_session: SessionResponse = http
        .get(format!("http://{}{}", daemon.address, session_path))
        .send()
        .await
        .expect("recovered vertical-slice Session response")
        .json()
        .await
        .expect("recovered vertical-slice Session JSON");
    assert_eq!(recovered_session, session);
    assert!(
        !approval_pid_file.exists(),
        "pending approval must remain blocked across restart"
    );

    let reconnect_started = Instant::now();
    let (mut socket, acknowledged_cursor) =
        open_event_socket_after(&daemon, Some(&replay_after)).await;
    let reconnect_ms = reconnect_started.elapsed().as_secs_f64() * 1_000.0;
    assert!(
        acknowledged_cursor
            .as_deref()
            .expect("vertical-slice replay acknowledgement cursor")
            .parse::<u64>()
            .expect("numeric vertical-slice acknowledgement cursor")
            > replay_after
                .parse::<u64>()
                .expect("numeric vertical-slice replay cursor")
    );
    let approval_path = TOOL_CALL_APPROVAL_PATH.replace("{tool_call_id}", &tool_call_id);
    let response = http
        .post(format!("http://{}{}", daemon.address, approval_path))
        .header(IDEMPOTENCY_KEY_HEADER, "vertical-slice-approve")
        .json(&ApprovalDecisionRequest {
            decision: ApprovalDecision::Approved,
        })
        .send()
        .await
        .expect("vertical-slice approval response");
    assert_eq!(response.status(), StatusCode::OK);
    let approved: RunResponse = response.json().await.expect("vertical-slice approval JSON");
    assert_eq!(approved.approvals[0].state, ApprovalState::Approved);
    let (replayed, streaming_peak_rss_kb) =
        receive_run_events_with_peak_rss(&daemon, &mut socket, &queued.run_id, RunState::Completed)
            .await;
    let approval_pid: Value = serde_json::from_str(
        &std::fs::read_to_string(&approval_pid_file)
            .expect("approved vertical-slice subprocess PID marker"),
    )
    .expect("approved vertical-slice subprocess PID marker JSON");
    assert!(
        approval_pid["parent_pid"]
            .as_u64()
            .is_some_and(|pid| pid > 0),
        "approved large-output subprocess must write its PID marker"
    );
    let replayed_cursors = replayed
        .iter()
        .map(|event| {
            event
                .cursor
                .parse::<u64>()
                .expect("numeric vertical-slice replay Event cursor")
        })
        .collect::<Vec<_>>();
    let replay_after = replay_after
        .parse::<u64>()
        .expect("numeric vertical-slice replay boundary");
    assert!(replayed_cursors.iter().all(|cursor| *cursor > replay_after));
    assert!(replayed_cursors.windows(2).all(|pair| pair[0] < pair[1]));
    let approval_event_index = replayed
        .iter()
        .position(|event| {
            matches!(
                &event.event,
                SessionEventDataResponse::ApprovalDecided { approval }
                    if approval.run_id == queued.run_id
                        && approval.state == ApprovalState::Approved
            )
        })
        .expect("approved vertical-slice Event");
    let first_output_event_index = replayed
        .iter()
        .position(|event| {
            matches!(
                &event.event,
                SessionEventDataResponse::ToolCallOutput { run_id, .. }
                    | SessionEventDataResponse::ArtifactRegistered { run_id, .. }
                    if run_id == &queued.run_id
            )
        })
        .expect("vertical-slice output or artifact Event");
    assert!(approval_event_index < first_output_event_index);
    let artifact = replayed
        .iter()
        .find_map(|event| match &event.event {
            SessionEventDataResponse::ArtifactRegistered {
                run_id,
                stream: ToolOutputStream::Stdout,
                artifact,
                ..
            } if run_id == &queued.run_id => Some(artifact.clone()),
            _ => None,
        })
        .expect("vertical-slice artifact Event");
    assert_eq!(artifact.size, "180000");

    let events_path = SESSION_EVENTS_PATH.replace("{session_id}", &session.session_id);
    let durable_suffix: SessionEventsResponse = http
        .get(format!(
            "http://{}{}?after={replay_after}",
            daemon.address, events_path
        ))
        .send()
        .await
        .expect("vertical-slice durable replay response")
        .json()
        .await
        .expect("vertical-slice durable replay JSON");
    assert_eq!(replayed, durable_suffix.events);

    let run_path = RUN_PATH.replace("{run_id}", &queued.run_id);
    let completed: RunResponse = http
        .get(format!("http://{}{}", daemon.address, run_path))
        .send()
        .await
        .expect("vertical-slice completed Run response")
        .json()
        .await
        .expect("vertical-slice completed Run JSON");
    assert_eq!(completed.state, RunState::Completed);
    assert_eq!(
        completed.tool_calls[0].stdout_artifact.as_ref(),
        Some(&artifact)
    );
    let artifact_path = ARTIFACT_PATH.replace("{content_hash}", &artifact.content_hash);
    let expected_artifact = "kiln large output\n".repeat(10_000);
    let response = http
        .get(format!("http://{}{}", daemon.address, artifact_path))
        .send()
        .await
        .expect("vertical-slice artifact response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .bytes()
            .await
            .expect("vertical-slice artifact bytes"),
        expected_artifact.as_bytes()
    );

    drop(socket);
    let shutdown_started = Instant::now();
    daemon.signal("INT");
    assert!(
        daemon
            .wait_for_exit_within(Duration::from_secs(5))
            .await
            .success()
    );
    let shutdown_ms = shutdown_started.elapsed().as_secs_f64() * 1_000.0;

    let cancellation_pid_file = primary.join("cancellation-pids.json");
    let mut daemon = Daemon::start_with_pid_file(
        binary,
        &data_directory,
        "blocking-tree",
        &cancellation_pid_file,
    );
    assert_eq!(daemon.token, original_token);
    let http = daemon.client();
    let response = http
        .get(format!("http://{}{}", daemon.address, artifact_path))
        .send()
        .await
        .expect("restarted vertical-slice artifact response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .bytes()
            .await
            .expect("restarted vertical-slice artifact bytes"),
        expected_artifact.as_bytes()
    );

    let mut socket = open_event_socket(&daemon).await;
    let cancellation_request = StartRunRequest {
        approval_policy: ApprovalPolicy::FullAccess,
        workspace_root_id: workspace.roots[0].workspace_root_id.clone(),
        relative_directory: ".".to_owned(),
    };
    let response = http
        .post(format!("http://{}{}", daemon.address, start_path))
        .header(IDEMPOTENCY_KEY_HEADER, "vertical-slice-cancel")
        .json(&cancellation_request)
        .send()
        .await
        .expect("vertical-slice cancellation Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let running: RunResponse = response
        .json()
        .await
        .expect("vertical-slice cancellation Run JSON");
    let (parent_pid, child_pid) = fixture_pids(&cancellation_pid_file).await;
    assert!(process_exists(parent_pid));
    assert!(process_exists(child_pid));
    assert!(process_group_exists(parent_pid));

    let cancel_path = RUN_CANCEL_PATH.replace("{run_id}", &running.run_id);
    let cancellation_started = Instant::now();
    let response = http
        .post(format!("http://{}{}", daemon.address, cancel_path))
        .send()
        .await
        .expect("vertical-slice cancellation response");
    assert_eq!(response.status(), StatusCode::OK);
    let cancelled: RunResponse = response
        .json()
        .await
        .expect("vertical-slice cancellation JSON");
    assert_eq!(cancelled.state, RunState::Cancelled);
    assert!(!process_exists(parent_pid));
    assert!(!process_exists(child_pid));
    assert!(!process_group_exists(parent_pid));
    let cancellation_ms = cancellation_started.elapsed().as_secs_f64() * 1_000.0;
    let cancellation_events =
        receive_run_events(&mut socket, &running.run_id, RunState::Cancelled).await;
    assert!(cancellation_events.iter().any(|event| {
        event_kind(event) == "run.cancellation_requested"
            && event_belongs_to_run(event, &running.run_id)
    }));

    drop(socket);
    daemon.signal("TERM");
    assert!(
        daemon
            .wait_for_exit_within(Duration::from_secs(5))
            .await
            .success()
    );

    let daemon = Daemon::start(binary, &data_directory);
    let http = daemon.client();
    let recovered_cancelled: RunResponse = http
        .get(format!(
            "http://{}{}",
            daemon.address,
            RUN_PATH.replace("{run_id}", &running.run_id)
        ))
        .send()
        .await
        .expect("recovered vertical-slice cancellation response")
        .json()
        .await
        .expect("recovered vertical-slice cancellation JSON");
    assert_eq!(recovered_cancelled, cancelled);
    let history: SessionEventsResponse = http
        .get(format!("http://{}{}", daemon.address, events_path))
        .send()
        .await
        .expect("recovered vertical-slice Event history response")
        .json()
        .await
        .expect("recovered vertical-slice Event history JSON");
    assert!(history.events.iter().any(|event| {
        matches!(
            &event.event,
            SessionEventDataResponse::MessageAppended {
                message: recovered_message
            } if recovered_message == &message
        )
    }));
    assert!(history.events.iter().any(|event| {
        matches!(
            &event.event,
            SessionEventDataResponse::ArtifactRegistered {
                run_id,
                artifact: recovered_artifact,
                ..
            } if run_id == &queued.run_id && recovered_artifact == &artifact
        )
    }));
    eprintln!(
        "EDL-216 metrics: idle_rss_kb={idle_rss_kb} streaming_peak_rss_kb={streaming_peak_rss_kb} reconnect_ms={reconnect_ms:.3} cancellation_ms={cancellation_ms:.3} shutdown_ms={shutdown_ms:.3}"
    );
}
