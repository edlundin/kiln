use kiln_protocol::{
    APPEND_MESSAGE_OPERATION_ID, ARTIFACT_PATH, AppendMessageRequest, CANCEL_RUN_OPERATION_ID,
    CREATE_SESSION_OPERATION_ID, CREATE_WORKSPACE_OPERATION_ID, ClientIdentity,
    CreateWorkspaceRequest, DETERMINISTIC_SUBPROCESS_CAPABILITY, EVENT_STREAM_OPERATION_ID,
    EVENTS_WEBSOCKET_PATH, GET_ARTIFACT_OPERATION_ID, GET_RUN_OPERATION_ID,
    GET_SESSION_OPERATION_ID, GET_WORKSPACE_OPERATION_ID, IDEMPOTENCY_KEY_HEADER,
    LIST_SESSION_EVENTS_OPERATION_ID, MessageResponse, NEGOTIATE_OPERATION_ID, NEGOTIATE_PATH,
    NegotiateRequest, NegotiateResponse, PROTOCOL_VERSION, ProblemDetails, RUN_CANCEL_PATH,
    RUN_PATH, RunResponse, RunState, SESSION_EVENTS_PATH, SESSION_MESSAGES_PATH, SESSION_PATH,
    SESSION_RUNS_PATH, START_RUN_OPERATION_ID, SessionEventDataResponse, SessionEventsResponse,
    SessionResponse, StoreIdentity, ToolCallState, ToolOutputStream, WEBSOCKET_CAPABILITY,
    WORKSPACE_PATH, WORKSPACE_SESSIONS_PATH, WORKSPACES_PATH, WebSocketFrame, WorkspaceResponse,
    error_code,
};
use serde_json::json;

#[test]
fn stored_artifacts_match_rust_protocol_types() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = manifest_dir.join("../../protocol/generated");
    kiln_protocol::check_generated_artifacts(root).expect("generated artifacts are current");

    let accepted_openapi =
        std::fs::read_to_string(manifest_dir.join("../../docs/protocol/openapi.yaml"))
            .expect("accepted OpenAPI contract is readable");
    let artifacts = kiln_protocol::artifact_files();
    assert_eq!(
        accepted_openapi,
        artifacts
            .get("openapi.yaml")
            .expect("OpenAPI artifact")
            .as_str()
    );
}

#[test]
fn command_types_reject_unknown_fields() {
    let value = json!({
        "min_version": PROTOCOL_VERSION,
        "max_version": PROTOCOL_VERSION,
        "client": {"name": "test", "build": "test"},
        "requested_capabilities": [WEBSOCKET_CAPABILITY],
        "unknown": true
    });

    assert!(serde_json::from_value::<NegotiateRequest>(value).is_err());

    let value = json!({
        "name": "Kiln",
        "roots": [{"name": "core", "path": "/work/kiln"}],
        "unknown": true
    });
    assert!(serde_json::from_value::<CreateWorkspaceRequest>(value).is_err());

    let value = json!({"content": "message", "unknown": true});
    assert!(serde_json::from_value::<AppendMessageRequest>(value).is_err());

    let value = json!({
        "name": "Kiln",
        "roots": [{"name": "core", "path": "/work/kiln", "unknown": true}]
    });
    assert!(serde_json::from_value::<CreateWorkspaceRequest>(value).is_err());
}

#[test]
fn response_types_accept_unknown_fields() {
    let value = json!({
        "selected_version": PROTOCOL_VERSION,
        "supported_capabilities": [WEBSOCKET_CAPABILITY],
        "selected_capabilities": [WEBSOCKET_CAPABILITY],
        "store_identity": {"id": "local", "name": "Kiln local store", "future": true},
        "current_event_cursor": null,
        "event_websocket_endpoint": "ws://127.0.0.1:1/v1/events",
        "future": true
    });

    serde_json::from_value::<NegotiateResponse>(value)
        .expect("response types ignore fields added by a compatible server");

    let value = json!({
        "workspace_id": "wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "name": "Kiln",
        "roots": [{
            "workspace_root_id": "wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "name": "core",
            "display_path": "/work/kiln",
            "canonical_path": "/work/kiln",
            "git_common_directory_path": "/work/kiln/.git",
            "position": 0,
            "state": "available",
            "future": true
        }],
        "future": true
    });
    serde_json::from_value::<WorkspaceResponse>(value)
        .expect("Workspace responses ignore fields added by a compatible server");

    let value = json!({
        "session_id": "ses_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "workspace_id": "wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "future": true
    });
    serde_json::from_value::<SessionResponse>(value)
        .expect("Session responses ignore fields added by a compatible server");

    let value = json!({
        "run_id": "run_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "session_id": "ses_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "state": "queued",
        "approval_policy": "ask",
        "requested_scope": {
            "workspace_root_id": "wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "relative_directory": "."
        },
        "tool_calls": [],
        "approvals": [],
        "future": true
    });
    serde_json::from_value::<RunResponse>(value)
        .expect("Run responses ignore fields added by a compatible server");
}

#[test]
fn websocket_frame_contract_matches_json_and_typescript() {
    let frame = WebSocketFrame::Ack {
        version: PROTOCOL_VERSION.to_owned(),
        capability: WEBSOCKET_CAPABILITY.to_owned(),
        current_event_cursor: None,
    };
    assert_eq!(
        serde_json::to_value(&frame).expect("frame is serializable"),
        json!({
            "type": "ack",
            "version": PROTOCOL_VERSION,
            "capability": WEBSOCKET_CAPABILITY,
            "current_event_cursor": null
        })
    );

    let mut future_frame = serde_json::to_value(frame).expect("frame is serializable");
    future_frame["future"] = json!(true);
    serde_json::from_value::<WebSocketFrame>(future_frame)
        .expect("response frames ignore fields added by a compatible server");

    let event_frame: WebSocketFrame = serde_json::from_str(
        kiln_protocol::artifact_files()
            .get("fixtures/websocket-event.json")
            .expect("WebSocket Event fixture"),
    )
    .expect("WebSocket Event fixture follows the public frame DTO");
    assert!(matches!(
        event_frame,
        WebSocketFrame::Event {
            event: kiln_protocol::SessionEventResponse {
                event: SessionEventDataResponse::ToolCallOutput {
                    stream: ToolOutputStream::Stdout,
                    ..
                },
                ..
            }
        }
    ));

    let typescript = kiln_protocol::artifact_files()
        .remove("types.ts")
        .expect("TypeScript artifact");
    assert!(typescript.contains(r#""type": "ack""#));
    assert!(typescript.contains(r#""type": "event""#));
    assert!(typescript.contains("current_event_cursor: string | null"));

    let schema: serde_json::Value = serde_json::from_str(
        kiln_protocol::artifact_files()
            .get("schema.json")
            .expect("JSON Schema artifact"),
    )
    .expect("JSON Schema is valid JSON");
    let required = schema["$defs"]["WebSocketFrame"]["oneOf"][0]["required"]
        .as_array()
        .expect("ack required fields");
    assert!(required.iter().any(|field| field == "current_event_cursor"));
    assert_eq!(
        schema["$defs"]["WebSocketFrame"]["oneOf"][0]["properties"]["current_event_cursor"]["type"],
        json!(["string", "null"])
    );
}

#[test]
fn absent_cursor_is_explicit_in_negotiation_json() {
    let response = NegotiateResponse {
        selected_version: PROTOCOL_VERSION.to_owned(),
        supported_capabilities: vec![WEBSOCKET_CAPABILITY.to_owned()],
        selected_capabilities: vec![WEBSOCKET_CAPABILITY.to_owned()],
        store_identity: StoreIdentity {
            id: "local".to_owned(),
            name: "Kiln local store".to_owned(),
        },
        current_event_cursor: None,
        event_websocket_endpoint: "ws://127.0.0.1:1/v1/events".to_owned(),
    };

    let value = serde_json::to_value(response).expect("response is serializable");
    assert_eq!(value["current_event_cursor"], serde_json::Value::Null);

    let schema: serde_json::Value = serde_json::from_str(
        kiln_protocol::artifact_files()
            .get("schema.json")
            .expect("JSON Schema artifact"),
    )
    .expect("JSON Schema is valid JSON");
    let required = schema["$defs"]["NegotiateResponse"]["required"]
        .as_array()
        .expect("negotiation required fields");
    assert!(required.iter().any(|field| field == "current_event_cursor"));
    assert_eq!(
        schema["$defs"]["NegotiateResponse"]["properties"]["current_event_cursor"]["type"],
        json!(["string", "null"])
    );
}

#[test]
fn client_identity_rejects_unknown_fields() {
    let value = json!({"name": "test", "build": "test", "unknown": true});
    assert!(serde_json::from_value::<ClientIdentity>(value).is_err());
}

#[test]
fn catalogue_and_error_fixture_use_protocol_metadata() {
    let artifacts = kiln_protocol::artifact_files();
    let catalogue: serde_json::Value =
        serde_json::from_str(artifacts.get("catalogue.json").expect("catalogue artifact"))
            .expect("catalogue is valid JSON");

    assert_eq!(catalogue["protocol_version"], PROTOCOL_VERSION);
    assert_eq!(catalogue["http"][0]["path"], NEGOTIATE_PATH);
    assert_eq!(catalogue["http"][0]["operation"], NEGOTIATE_OPERATION_ID);
    assert_eq!(catalogue["http"][1]["path"], WORKSPACES_PATH);
    assert_eq!(
        catalogue["http"][1]["operation"],
        CREATE_WORKSPACE_OPERATION_ID
    );
    assert_eq!(catalogue["http"][2]["path"], WORKSPACE_PATH);
    assert_eq!(
        catalogue["http"][2]["operation"],
        GET_WORKSPACE_OPERATION_ID
    );
    assert_eq!(catalogue["http"][3]["path"], WORKSPACE_SESSIONS_PATH);
    assert_eq!(
        catalogue["http"][3]["operation"],
        CREATE_SESSION_OPERATION_ID
    );
    assert_eq!(catalogue["http"][4]["path"], SESSION_PATH);
    assert_eq!(catalogue["http"][4]["operation"], GET_SESSION_OPERATION_ID);
    assert_eq!(catalogue["http"][5]["path"], SESSION_MESSAGES_PATH);
    assert_eq!(
        catalogue["http"][5]["operation"],
        APPEND_MESSAGE_OPERATION_ID
    );
    assert_eq!(catalogue["http"][6]["path"], SESSION_EVENTS_PATH);
    assert_eq!(
        catalogue["http"][6]["operation"],
        LIST_SESSION_EVENTS_OPERATION_ID
    );
    assert_eq!(catalogue["http"][7]["path"], SESSION_RUNS_PATH);
    assert_eq!(catalogue["http"][7]["operation"], START_RUN_OPERATION_ID);
    assert_eq!(catalogue["http"][8]["path"], RUN_PATH);
    assert_eq!(catalogue["http"][8]["operation"], GET_RUN_OPERATION_ID);
    let artifact = catalogue["http"]
        .as_array()
        .expect("HTTP operations")
        .iter()
        .find(|operation| operation["operation"] == GET_ARTIFACT_OPERATION_ID)
        .expect("artifact operation");
    assert_eq!(artifact["path"], ARTIFACT_PATH);
    assert!(
        catalogue["http"]
            .as_array()
            .expect("HTTP operations")
            .iter()
            .all(|operation| operation["operation"] != "append_event")
    );
    assert_eq!(catalogue["websocket"][0]["path"], EVENTS_WEBSOCKET_PATH);
    assert_eq!(
        catalogue["websocket"][0]["operation"],
        EVENT_STREAM_OPERATION_ID
    );
    assert_eq!(
        catalogue["error_codes"],
        serde_json::to_value(error_code::ALL).expect("error codes are serializable")
    );

    let problem: ProblemDetails = serde_json::from_str(
        artifacts
            .get("fixtures/problem-details.json")
            .expect("problem fixture"),
    )
    .expect("problem fixture follows the public DTO");
    assert_eq!(problem.code, error_code::INVALID_REQUEST);
}

#[test]
fn workspace_fixtures_match_json_schema_typescript_and_openapi() {
    let artifacts = kiln_protocol::artifact_files();
    serde_json::from_str::<CreateWorkspaceRequest>(
        artifacts
            .get("fixtures/create-workspace-request.json")
            .expect("create Workspace fixture"),
    )
    .expect("create Workspace fixture follows the request DTO");
    let workspace = serde_json::from_str::<WorkspaceResponse>(
        artifacts
            .get("fixtures/workspace-response.json")
            .expect("Workspace response fixture"),
    )
    .expect("Workspace fixture follows the response DTO");
    assert_eq!(workspace.roots.len(), 2);
    assert_eq!(workspace.roots[0].position, 0);
    assert_eq!(workspace.roots[1].position, 1);

    let typescript = artifacts.get("types.ts").expect("TypeScript artifact");
    assert!(typescript.contains("type CreateWorkspaceRequest"));
    assert!(typescript.contains("type WorkspaceResponse"));

    let openapi = artifacts.get("openapi.yaml").expect("OpenAPI artifact");
    assert!(openapi.contains(&format!("  {WORKSPACES_PATH}:")));
    assert!(openapi.contains(&format!("operationId: {CREATE_WORKSPACE_OPERATION_ID}")));
    assert!(openapi.contains(&format!("operationId: {GET_WORKSPACE_OPERATION_ID}")));
    let get_workspace = openapi
        .split_once(&format!("  {WORKSPACE_PATH}:"))
        .expect("GET Workspace path")
        .1
        .split_once("components:")
        .expect("OpenAPI components")
        .0;
    assert!(get_workspace.contains("        '400':"));

    let schema: serde_json::Value =
        serde_json::from_str(artifacts.get("schema.json").expect("JSON Schema artifact"))
            .expect("JSON Schema is valid JSON");
    assert!(schema["$defs"]["CreateWorkspaceRequest"].is_object());
    assert!(schema["$defs"]["WorkspaceResponse"].is_object());
}

#[test]
fn session_fixtures_match_json_schema_typescript_and_openapi() {
    let artifacts = kiln_protocol::artifact_files();
    let request = serde_json::from_str::<AppendMessageRequest>(
        artifacts
            .get("fixtures/append-message-request.json")
            .expect("append Message fixture"),
    )
    .expect("append Message fixture follows the request DTO");
    assert!(!request.content.is_empty());

    let session = serde_json::from_str::<SessionResponse>(
        artifacts
            .get("fixtures/session-response.json")
            .expect("Session fixture"),
    )
    .expect("Session fixture follows the response DTO");
    assert!(session.session_id.starts_with("ses_"));

    let message = serde_json::from_str::<MessageResponse>(
        artifacts
            .get("fixtures/message-response.json")
            .expect("Message fixture"),
    )
    .expect("Message fixture follows the response DTO");
    assert_eq!(message.session_id, session.session_id);

    let page = serde_json::from_str::<SessionEventsResponse>(
        artifacts
            .get("fixtures/session-events-response.json")
            .expect("Session Events fixture"),
    )
    .expect("Session Events fixture follows the response DTO");
    assert_eq!(page.events.len(), 2);
    assert_eq!(page.events[0].cursor, "1");
    assert_eq!(page.events[1].cursor, "2");
    assert_eq!(page.current_event_cursor, "2");
    assert!(page.events.iter().all(|event| {
        event.event_id.starts_with("evt_") && event.session_id == session.session_id
    }));
    assert!(matches!(
        &page.events[0].event,
        SessionEventDataResponse::SessionCreated { .. }
    ));
    assert!(matches!(
        &page.events[1].event,
        SessionEventDataResponse::MessageAppended { .. }
    ));

    let typescript = artifacts.get("types.ts").expect("TypeScript artifact");
    assert!(typescript.contains("type AppendMessageRequest"));
    assert!(typescript.contains("type SessionEventsResponse"));

    let openapi = artifacts.get("openapi.yaml").expect("OpenAPI artifact");
    for (path, operation) in [
        (WORKSPACE_SESSIONS_PATH, CREATE_SESSION_OPERATION_ID),
        (SESSION_PATH, GET_SESSION_OPERATION_ID),
        (SESSION_MESSAGES_PATH, APPEND_MESSAGE_OPERATION_ID),
        (SESSION_EVENTS_PATH, LIST_SESSION_EVENTS_OPERATION_ID),
    ] {
        assert!(openapi.contains(&format!("  {path}:")));
        assert!(openapi.contains(&format!("operationId: {operation}")));
    }

    let schema: serde_json::Value =
        serde_json::from_str(artifacts.get("schema.json").expect("JSON Schema artifact"))
            .expect("JSON Schema is valid JSON");
    for definition in [
        "AppendMessageRequest",
        "ArtifactResponse",
        "SessionResponse",
        "MessageResponse",
        "SessionEventResponse",
        "SessionEventsResponse",
    ] {
        assert!(schema["$defs"][definition].is_object());
    }
}

#[test]
fn start_run_contract_requires_opaque_idempotency_key() {
    let openapi = kiln_protocol::artifact_files()
        .remove("openapi.yaml")
        .expect("OpenAPI artifact");
    let start_run = openapi
        .split_once("  /v1/sessions/{session_id}/runs:")
        .expect("start Run path")
        .1
        .split_once("components:")
        .expect("OpenAPI components")
        .0;
    assert!(start_run.contains(&format!("- name: {IDEMPOTENCY_KEY_HEADER}")));
    assert!(start_run.contains("in: header"));
    assert!(start_run.contains("required: true"));
    assert!(start_run.contains("type: string"));
    assert!(openapi.contains(&format!("version: {PROTOCOL_VERSION}")));
}

#[test]
fn event_stream_contract_accepts_an_exclusive_replay_cursor() {
    let openapi = kiln_protocol::artifact_files()
        .remove("openapi.yaml")
        .expect("OpenAPI artifact");
    let event_stream = openapi
        .split_once("  /v1/events:")
        .expect("Event WebSocket path")
        .1
        .split_once("  /v1/workspaces:")
        .expect("next OpenAPI path")
        .0;

    assert!(event_stream.contains("- name: after"));
    assert!(event_stream.contains("Exclusive durable Event cursor"));
}

#[test]
fn run_fixtures_match_json_schema_typescript_and_openapi() {
    let artifacts = kiln_protocol::artifact_files();
    let run = serde_json::from_str::<RunResponse>(
        artifacts
            .get("fixtures/run-response.json")
            .expect("Run fixture"),
    )
    .expect("Run fixture follows the response DTO");
    assert!(run.run_id.starts_with("run_"));
    assert_eq!(run.state, RunState::Completed);
    assert_eq!(run.tool_calls.len(), 1);
    assert!(run.tool_calls[0].tool_call_id.starts_with("tcl_"));
    assert_eq!(
        run.tool_calls[0].capability,
        DETERMINISTIC_SUBPROCESS_CAPABILITY
    );
    assert_eq!(run.tool_calls[0].stdout_artifact, None);
    assert_eq!(run.tool_calls[0].stderr_artifact, None);
    assert_eq!(run.tool_calls[0].state, ToolCallState::Completed);
    assert_eq!(run.tool_calls[0].exit_code, Some(0));

    let event_types = [
        SessionEventDataResponse::RunCreated {
            run_id: run.run_id.clone(),
            state: RunState::Queued,
            approval_policy: None,
            requested_scope: None,
        },
        SessionEventDataResponse::RunStateChanged {
            run_id: run.run_id.clone(),
            state: RunState::Running,
        },
        SessionEventDataResponse::RunCancellationRequested {
            run_id: run.run_id.clone(),
        },
        SessionEventDataResponse::ToolCallRequested {
            tool_call: run.tool_calls[0].clone(),
        },
        SessionEventDataResponse::ToolCallStateChanged {
            tool_call: run.tool_calls[0].clone(),
        },
        SessionEventDataResponse::ToolCallOutput {
            run_id: run.run_id.clone(),
            tool_call_id: run.tool_calls[0].tool_call_id.clone(),
            stream: ToolOutputStream::Stdout,
            content: "output".to_owned(),
        },
    ]
    .map(|event| serde_json::to_value(event).expect("Event is serializable"));
    assert_eq!(
        event_types.map(|event| event["type"].as_str().unwrap().to_owned()),
        [
            "run.created",
            "run.state_changed",
            "run.cancellation_requested",
            "tool_call.requested",
            "tool_call.state_changed",
            "tool_call.output",
        ]
    );

    let typescript = artifacts.get("types.ts").expect("TypeScript artifact");
    assert!(typescript.contains("type RunResponse"));
    assert!(typescript.contains("type ToolCallResponse"));

    let openapi = artifacts.get("openapi.yaml").expect("OpenAPI artifact");
    for (path, operation) in [
        (SESSION_RUNS_PATH, START_RUN_OPERATION_ID),
        (RUN_PATH, GET_RUN_OPERATION_ID),
        (RUN_CANCEL_PATH, CANCEL_RUN_OPERATION_ID),
    ] {
        assert!(openapi.contains(&format!("  {path}:")));
        assert!(openapi.contains(&format!("operationId: {operation}")));
    }

    let schema: serde_json::Value =
        serde_json::from_str(artifacts.get("schema.json").expect("JSON Schema artifact"))
            .expect("JSON Schema is valid JSON");
    for definition in [
        "RunState",
        "ToolCallState",
        "ToolOutputStream",
        "ToolCallResponse",
        "RunResponse",
    ] {
        assert!(schema["$defs"][definition].is_object());
    }
    assert_eq!(
        schema["$defs"]["ToolCallResponse"]["properties"]["exit_code"]["type"],
        json!(["integer", "null"])
    );
}
