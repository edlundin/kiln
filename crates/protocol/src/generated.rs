use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use ts_rs::{Config, TS};

use crate::{
    APPEND_MESSAGE_OPERATION_ID, AppendMessageRequest, CREATE_SESSION_OPERATION_ID,
    CREATE_WORKSPACE_OPERATION_ID, ClientIdentity, CreateWorkspaceRequest,
    DETERMINISTIC_SUBPROCESS_CAPABILITY, EVENT_STREAM_OPERATION_ID, EVENTS_WEBSOCKET_PATH,
    GET_RUN_OPERATION_ID, GET_SESSION_OPERATION_ID, GET_WORKSPACE_OPERATION_ID,
    LIST_SESSION_EVENTS_OPERATION_ID, MessageResponse, MessageRole, NEGOTIATE_OPERATION_ID,
    NEGOTIATE_PATH, NegotiateRequest, NegotiateResponse, PROTOCOL_VERSION, ProblemDetails,
    RUN_PATH, RunResponse, RunState, SESSION_EVENTS_PATH, SESSION_MESSAGES_PATH, SESSION_PATH,
    SESSION_RUNS_PATH, START_RUN_OPERATION_ID, SessionEventDataResponse, SessionEventResponse,
    SessionEventsResponse, SessionResponse, StoreIdentity, ToolCallResponse, ToolCallState,
    ToolOutputStream, WEBSOCKET_CAPABILITY, WORKSPACE_PATH, WORKSPACE_SESSIONS_PATH,
    WORKSPACES_PATH, WorkspaceResponse, WorkspaceRootRequest, WorkspaceRootResponse, error_code,
};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum WebSocketFrame {
    Ack {
        version: String,
        capability: String,
        #[schemars(with = "crate::RequiredNullableString")]
        current_event_cursor: Option<String>,
    },
    Error {
        code: String,
        message: String,
    },
    Event {
        event: SessionEventResponse,
    },
}

pub fn artifact_files() -> BTreeMap<&'static str, String> {
    let mut files = BTreeMap::new();
    files.insert("openapi.yaml", openapi());
    files.insert("schema.json", schema());
    files.insert("types.ts", typescript());
    files.insert("catalogue.json", catalogue());
    files.insert(
        "fixtures/negotiate-request.json",
        fixture_negotiate_request(),
    );
    files.insert(
        "fixtures/negotiate-response.json",
        fixture_negotiate_response(),
    );
    files.insert("fixtures/problem-details.json", fixture_problem_details());
    files.insert("fixtures/websocket-ack.json", fixture_websocket_ack());
    files.insert("fixtures/websocket-error.json", fixture_websocket_error());
    files.insert("fixtures/websocket-event.json", fixture_websocket_event());
    files.insert(
        "fixtures/create-workspace-request.json",
        fixture_create_workspace_request(),
    );
    files.insert(
        "fixtures/workspace-response.json",
        fixture_workspace_response(),
    );
    files.insert(
        "fixtures/append-message-request.json",
        fixture_append_message_request(),
    );
    files.insert("fixtures/session-response.json", fixture_session_response());
    files.insert("fixtures/message-response.json", fixture_message_response());
    files.insert("fixtures/run-response.json", fixture_run_response());
    files.insert(
        "fixtures/session-events-response.json",
        fixture_session_events_response(),
    );
    files.insert("reference.md", reference());
    files
}

pub fn check_generated_artifacts(root: impl AsRef<Path>) -> Result<(), String> {
    let root = root.as_ref();
    for (relative, expected) in artifact_files() {
        let path = root.join(relative);
        let actual = fs::read_to_string(&path)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        if actual != expected {
            return Err(format!("generated artifact differs: {}", path.display()));
        }
    }
    Ok(())
}

pub fn write_generated_artifacts(root: impl AsRef<Path>) -> Result<(), String> {
    let root = root.as_ref();
    for (relative, contents) in artifact_files() {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("cannot create {}: {error}", parent.display()))?;
        }
        fs::write(&path, contents)
            .map_err(|error| format!("cannot write {}: {error}", path.display()))?;
    }
    Ok(())
}

fn schema() -> String {
    let mut defs = serde_json::Map::new();
    for (name, value) in [
        ("ClientIdentity", schema_for!(ClientIdentity)),
        ("NegotiateRequest", schema_for!(NegotiateRequest)),
        ("NegotiateResponse", schema_for!(NegotiateResponse)),
        ("ProblemDetails", schema_for!(ProblemDetails)),
        ("StoreIdentity", schema_for!(StoreIdentity)),
        ("WebSocketFrame", schema_for!(WebSocketFrame)),
        (
            "CreateWorkspaceRequest",
            schema_for!(CreateWorkspaceRequest),
        ),
        ("WorkspaceRootRequest", schema_for!(WorkspaceRootRequest)),
        ("WorkspaceRootResponse", schema_for!(WorkspaceRootResponse)),
        ("WorkspaceResponse", schema_for!(WorkspaceResponse)),
        ("AppendMessageRequest", schema_for!(AppendMessageRequest)),
        ("SessionResponse", schema_for!(SessionResponse)),
        ("MessageRole", schema_for!(MessageRole)),
        ("MessageResponse", schema_for!(MessageResponse)),
        ("RunState", schema_for!(RunState)),
        ("ToolCallState", schema_for!(ToolCallState)),
        ("ToolOutputStream", schema_for!(ToolOutputStream)),
        ("ToolCallResponse", schema_for!(ToolCallResponse)),
        ("RunResponse", schema_for!(RunResponse)),
        (
            "SessionEventDataResponse",
            schema_for!(SessionEventDataResponse),
        ),
        ("SessionEventResponse", schema_for!(SessionEventResponse)),
        ("SessionEventsResponse", schema_for!(SessionEventsResponse)),
    ] {
        defs.insert(
            name.to_owned(),
            serde_json::to_value(value).expect("schema is serializable"),
        );
    }
    serde_json::to_string_pretty(&json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": format!("https://kiln.dev/protocol/{PROTOCOL_VERSION}/schema.json"),
        "title": "Kiln daemon protocol",
        "type": "object",
        "$defs": defs,
    }))
    .expect("schema is serializable")
        + "\n"
}

fn typescript() -> String {
    let config = Config::default();
    [
        ClientIdentity::decl(&config),
        NegotiateRequest::decl(&config),
        NegotiateResponse::decl(&config),
        ProblemDetails::decl(&config),
        StoreIdentity::decl(&config),
        WebSocketFrame::decl(&config),
        CreateWorkspaceRequest::decl(&config),
        WorkspaceRootRequest::decl(&config),
        WorkspaceRootResponse::decl(&config),
        WorkspaceResponse::decl(&config),
        AppendMessageRequest::decl(&config),
        SessionResponse::decl(&config),
        MessageRole::decl(&config),
        MessageResponse::decl(&config),
        RunState::decl(&config),
        ToolCallState::decl(&config),
        ToolOutputStream::decl(&config),
        ToolCallResponse::decl(&config),
        RunResponse::decl(&config),
        SessionEventDataResponse::decl(&config),
        SessionEventResponse::decl(&config),
        SessionEventsResponse::decl(&config),
    ]
    .into_iter()
    .map(|declaration| format!("export {declaration}"))
    .collect::<Vec<_>>()
    .join("\n\n")
        + "\n"
}

fn catalogue() -> String {
    serde_json::to_string_pretty(&json!({
        "protocol_version": PROTOCOL_VERSION,
        "capabilities": [WEBSOCKET_CAPABILITY],
        "error_codes": error_code::ALL,
        "http": [{
            "method": "POST",
            "path": NEGOTIATE_PATH,
            "operation": NEGOTIATE_OPERATION_ID
        }, {
            "method": "POST",
            "path": WORKSPACES_PATH,
            "operation": CREATE_WORKSPACE_OPERATION_ID
        }, {
            "method": "GET",
            "path": WORKSPACE_PATH,
            "operation": GET_WORKSPACE_OPERATION_ID
        }, {
            "method": "POST",
            "path": WORKSPACE_SESSIONS_PATH,
            "operation": CREATE_SESSION_OPERATION_ID
        }, {
            "method": "GET",
            "path": SESSION_PATH,
            "operation": GET_SESSION_OPERATION_ID
        }, {
            "method": "POST",
            "path": SESSION_MESSAGES_PATH,
            "operation": APPEND_MESSAGE_OPERATION_ID
        }, {
            "method": "GET",
            "path": SESSION_EVENTS_PATH,
            "operation": LIST_SESSION_EVENTS_OPERATION_ID
        }, {
            "method": "POST",
            "path": SESSION_RUNS_PATH,
            "operation": START_RUN_OPERATION_ID
        }, {
            "method": "GET",
            "path": RUN_PATH,
            "operation": GET_RUN_OPERATION_ID
        }],
        "websocket": [{
            "method": "GET",
            "path": EVENTS_WEBSOCKET_PATH,
            "operation": EVENT_STREAM_OPERATION_ID
        }]
    }))
    .expect("catalogue is serializable")
        + "\n"
}

fn fixture_negotiate_request() -> String {
    serialize_fixture(&NegotiateRequest {
        min_version: PROTOCOL_VERSION.to_owned(),
        max_version: PROTOCOL_VERSION.to_owned(),
        client: ClientIdentity {
            name: "fixture-client".to_owned(),
            build: "fixture".to_owned(),
        },
        requested_capabilities: vec![WEBSOCKET_CAPABILITY.to_owned()],
    })
}

fn fixture_negotiate_response() -> String {
    serialize_fixture(&NegotiateResponse {
        selected_version: PROTOCOL_VERSION.to_owned(),
        supported_capabilities: vec![WEBSOCKET_CAPABILITY.to_owned()],
        selected_capabilities: vec![WEBSOCKET_CAPABILITY.to_owned()],
        store_identity: StoreIdentity {
            id: "local".to_owned(),
            name: "Kiln local store".to_owned(),
        },
        current_event_cursor: None,
        event_websocket_endpoint: format!("ws://127.0.0.1:0{EVENTS_WEBSOCKET_PATH}"),
    })
}

fn fixture_problem_details() -> String {
    serialize_fixture(&ProblemDetails {
        type_uri: "about:blank".to_owned(),
        title: "Invalid request".to_owned(),
        status: 400,
        code: error_code::INVALID_REQUEST.to_owned(),
        detail: "request fields are invalid".to_owned(),
    })
}

fn fixture_websocket_ack() -> String {
    serialize_fixture(&WebSocketFrame::Ack {
        version: PROTOCOL_VERSION.to_owned(),
        capability: WEBSOCKET_CAPABILITY.to_owned(),
        current_event_cursor: None,
    })
}

fn fixture_websocket_error() -> String {
    serialize_fixture(&WebSocketFrame::Error {
        code: error_code::UNSUPPORTED_INPUT.to_owned(),
        message: "The event WebSocket accepts control frames only.".to_owned(),
    })
}

fn fixture_websocket_event() -> String {
    serialize_fixture(&WebSocketFrame::Event {
        event: SessionEventResponse {
            event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FAY".to_owned(),
            cursor: "7".to_owned(),
            session_id: "ses_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            event: SessionEventDataResponse::ToolCallOutput {
                run_id: "run_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                tool_call_id: "tcl_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                stream: ToolOutputStream::Stdout,
                content: "Kiln deterministic subprocess completed.\n".to_owned(),
            },
        },
    })
}

fn fixture_create_workspace_request() -> String {
    serialize_fixture(&CreateWorkspaceRequest {
        name: "fixture-workspace".to_owned(),
        roots: vec![
            WorkspaceRootRequest {
                name: "first".to_owned(),
                path: "/tmp/first".to_owned(),
            },
            WorkspaceRootRequest {
                name: "second".to_owned(),
                path: "/tmp/second".to_owned(),
            },
        ],
    })
}

fn fixture_workspace_response() -> String {
    serialize_fixture(&WorkspaceResponse {
        workspace_id: "wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        name: "fixture-workspace".to_owned(),
        roots: vec![
            WorkspaceRootResponse {
                workspace_root_id: "wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                name: "first".to_owned(),
                display_path: "/tmp/first".to_owned(),
                canonical_path: "/tmp/first".to_owned(),
                git_common_directory_path: "/tmp/first/.git".to_owned(),
                position: 0,
                state: "available".to_owned(),
            },
            WorkspaceRootResponse {
                workspace_root_id: "wrt_01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
                name: "second".to_owned(),
                display_path: "/tmp/second".to_owned(),
                canonical_path: "/tmp/second".to_owned(),
                git_common_directory_path: "/tmp/second/.git".to_owned(),
                position: 1,
                state: "available".to_owned(),
            },
        ],
    })
}

fn fixture_append_message_request() -> String {
    serialize_fixture(&AppendMessageRequest {
        content: "Inspect the Workspace event model.".to_owned(),
    })
}

fn fixture_session_response() -> String {
    serialize_fixture(&SessionResponse {
        session_id: "ses_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        workspace_id: "wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
    })
}

fn fixture_message() -> MessageResponse {
    MessageResponse {
        message_id: "msg_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        session_id: "ses_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        role: MessageRole::User,
        content: "Inspect the Workspace event model.".to_owned(),
    }
}

fn fixture_message_response() -> String {
    serialize_fixture(&fixture_message())
}

fn fixture_tool_call() -> ToolCallResponse {
    ToolCallResponse {
        tool_call_id: "tcl_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        run_id: "run_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        capability: DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
        state: ToolCallState::Completed,
        stdout: Some("Kiln deterministic subprocess completed.\n".to_owned()),
        stderr: Some(String::new()),
        exit_code: Some(0),
    }
}

fn fixture_run() -> RunResponse {
    RunResponse {
        run_id: "run_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        session_id: "ses_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        state: RunState::Completed,
        tool_calls: vec![fixture_tool_call()],
    }
}

fn fixture_run_response() -> String {
    serialize_fixture(&fixture_run())
}

fn fixture_session_events_response() -> String {
    serialize_fixture(&SessionEventsResponse {
        events: vec![
            SessionEventResponse {
                event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                cursor: "1".to_owned(),
                session_id: "ses_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                event: SessionEventDataResponse::SessionCreated {
                    workspace_id: "wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                },
            },
            SessionEventResponse {
                event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
                cursor: "2".to_owned(),
                session_id: "ses_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                event: SessionEventDataResponse::MessageAppended {
                    message: fixture_message(),
                },
            },
        ],
        current_event_cursor: "2".to_owned(),
    })
}

fn serialize_fixture(value: &impl Serialize) -> String {
    serde_json::to_string_pretty(value).expect("fixture is serializable") + "\n"
}

fn openapi() -> String {
    let mut output = format!(
        r#"openapi: 3.1.1
info:
  title: Kiln daemon protocol
  version: {PROTOCOL_VERSION}
  description: Kiln loopback protocol contract.
paths:
  {NEGOTIATE_PATH}:
    post:
      operationId: {NEGOTIATE_OPERATION_ID}
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/NegotiateRequest'
      responses:
        '200':
          description: Negotiated protocol and capabilities.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/NegotiateResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '405':
          $ref: '#/components/responses/Problem'
  {EVENTS_WEBSOCKET_PATH}:
    get:
      operationId: {EVENT_STREAM_OPERATION_ID}
      parameters:
        - name: version
          in: query
          required: true
          schema: {{type: string}}
        - name: capability
          in: query
          required: true
          schema: {{type: string}}
      responses:
        '101':
          description: WebSocket protocol switch.
        '400':
          $ref: '#/components/responses/Problem'
        '405':
          $ref: '#/components/responses/Problem'
        '426':
          $ref: '#/components/responses/Problem'
  {WORKSPACES_PATH}:
    post:
      operationId: {CREATE_WORKSPACE_OPERATION_ID}
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/CreateWorkspaceRequest'
      responses:
        '201':
          description: Created workspace.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/WorkspaceResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '409':
          $ref: '#/components/responses/Problem'
        '503':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
  {WORKSPACE_PATH}:
    get:
      operationId: {GET_WORKSPACE_OPERATION_ID}
      parameters:
        - name: workspace_id
          in: path
          required: true
          schema: {{type: string}}
      responses:
        '200':
          description: Stored workspace snapshot.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/WorkspaceResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
  {WORKSPACE_SESSIONS_PATH}:
    post:
      operationId: {CREATE_SESSION_OPERATION_ID}
      parameters:
        - name: workspace_id
          in: path
          required: true
          schema: {{type: string}}
      responses:
        '201':
          description: Created Session.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/SessionResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
  {SESSION_PATH}:
    get:
      operationId: {GET_SESSION_OPERATION_ID}
      parameters:
        - name: session_id
          in: path
          required: true
          schema: {{type: string}}
      responses:
        '200':
          description: Stored Session.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/SessionResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
  {SESSION_MESSAGES_PATH}:
    post:
      operationId: {APPEND_MESSAGE_OPERATION_ID}
      parameters:
        - name: session_id
          in: path
          required: true
          schema: {{type: string}}
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/AppendMessageRequest'
      responses:
        '201':
          description: Appended immutable user Message.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/MessageResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
  {SESSION_EVENTS_PATH}:
    get:
      operationId: {LIST_SESSION_EVENTS_OPERATION_ID}
      parameters:
        - name: session_id
          in: path
          required: true
          schema: {{type: string}}
        - name: after
          in: query
          required: false
          schema: {{type: string, default: "0"}}
      responses:
        '200':
          description: Committed Session events after the requested cursor.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/SessionEventsResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
  {SESSION_RUNS_PATH}:
    post:
      operationId: {START_RUN_OPERATION_ID}
      parameters:
        - name: session_id
          in: path
          required: true
          schema: {{type: string}}
      responses:
        '202':
          description: Accepted queued root Run.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/RunResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '409':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
  {RUN_PATH}:
    get:
      operationId: {GET_RUN_OPERATION_ID}
      parameters:
        - name: run_id
          in: path
          required: true
          schema: {{type: string}}
      responses:
        '200':
          description: Durable Run and ToolCall result.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/RunResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
components:
  responses:
    Problem:
      description: Stable public error.
      content:
        application/problem+json:
          schema:
            $ref: '#/components/schemas/ProblemDetails'
  schemas:
"#,
    );
    for (name, schema) in [
        ("ClientIdentity", openapi_schema::<ClientIdentity>()),
        ("NegotiateRequest", openapi_schema::<NegotiateRequest>()),
        ("StoreIdentity", openapi_schema::<StoreIdentity>()),
        ("NegotiateResponse", openapi_schema::<NegotiateResponse>()),
        ("WebSocketFrame", openapi_schema::<WebSocketFrame>()),
        ("ProblemDetails", openapi_schema::<ProblemDetails>()),
        (
            "CreateWorkspaceRequest",
            openapi_schema::<CreateWorkspaceRequest>(),
        ),
        (
            "WorkspaceRootRequest",
            openapi_schema::<WorkspaceRootRequest>(),
        ),
        (
            "WorkspaceRootResponse",
            openapi_schema::<WorkspaceRootResponse>(),
        ),
        ("WorkspaceResponse", openapi_schema::<WorkspaceResponse>()),
        (
            "AppendMessageRequest",
            openapi_schema::<AppendMessageRequest>(),
        ),
        ("SessionResponse", openapi_schema::<SessionResponse>()),
        ("MessageRole", openapi_schema::<MessageRole>()),
        ("MessageResponse", openapi_schema::<MessageResponse>()),
        ("RunState", openapi_schema::<RunState>()),
        ("ToolCallState", openapi_schema::<ToolCallState>()),
        ("ToolOutputStream", openapi_schema::<ToolOutputStream>()),
        ("ToolCallResponse", openapi_schema::<ToolCallResponse>()),
        ("RunResponse", openapi_schema::<RunResponse>()),
        (
            "SessionEventDataResponse",
            openapi_schema::<SessionEventDataResponse>(),
        ),
        (
            "SessionEventResponse",
            openapi_schema::<SessionEventResponse>(),
        ),
        (
            "SessionEventsResponse",
            openapi_schema::<SessionEventsResponse>(),
        ),
    ] {
        let schema = serde_json::to_string(&schema).expect("OpenAPI schema is serializable");
        let _ = writeln!(output, "    {name}: {schema}");
    }
    output
}

fn openapi_schema<T: JsonSchema>() -> Value {
    let mut schema = serde_json::to_value(schema_for!(T)).expect("OpenAPI schema is serializable");
    normalize_openapi_schema(&mut schema);
    schema
}

fn normalize_openapi_schema(value: &mut Value) {
    if let Value::Object(root) = value {
        root.remove("$defs");
        root.remove("$schema");
        root.remove("title");
    }
    normalize_openapi_references(value);
}

fn normalize_openapi_references(value: &mut Value) {
    match value {
        Value::Object(object) => {
            if let Some(Value::String(reference)) = object.get_mut("$ref")
                && let Some(name) = reference.strip_prefix("#/$defs/")
            {
                *reference = format!("#/components/schemas/{name}");
            }
            for child in object.values_mut() {
                normalize_openapi_references(child);
            }
        }
        Value::Array(values) => {
            for child in values {
                normalize_openapi_references(child);
            }
        }
        _ => {}
    }
}

fn reference() -> String {
    format!(
        "# EDL-211 protocol reference\n\nProtocol version: `{PROTOCOL_VERSION}`.\n\nThe client sends `POST {NEGOTIATE_PATH}` with a version range, client identity, and requested capabilities. The server returns the selected version, capability lists, store identity, current event cursor, and the event WebSocket endpoint.\n\nThe client creates a durable Workspace with `POST {WORKSPACES_PATH}`, providing a name and one or more named local Git repository roots. `GET {WORKSPACE_PATH}` returns the stored snapshot.\n\n`POST {WORKSPACE_SESSIONS_PATH}` creates a Session attached to one Workspace. `GET {SESSION_PATH}` returns it. A Session does not own or depend on a worktree.\n\n`POST {SESSION_MESSAGES_PATH}` accepts immutable user message content. Kiln creates the Message and its `message.appended` Event atomically. Clients cannot append arbitrary Events.\n\n`POST {SESSION_RUNS_PATH}` accepts one queued root Run for the Session. The deterministic adapter selects the fixed subprocess; clients cannot provide an executable, arguments, shell, environment, or working directory. `GET {RUN_PATH}` returns the durable Run and ToolCall result.\n\n`GET {SESSION_EVENTS_PATH}?after=0` returns that Session's committed Events after the opaque decimal cursor. Event IDs are stable identities. The daemon-wide cursor orders committed audit records; it does not schedule work between Sessions. The response current cursor and Event rows come from one storage snapshot.\n\nThe client can connect to `GET {EVENTS_WEBSOCKET_PATH}?version={PROTOCOL_VERSION}&capability={WEBSOCKET_CAPABILITY}`. After the acknowledgement, the server publishes newly committed Run and ToolCall Events in cursor order. Reconnect replay is outside this release.\n\nHTTP errors use the protocol-owned `ProblemDetails` shape. Clients make decisions from the stable `code` field. `catalogue.json` lists the error codes implemented by this release.\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openapi_components_preserve_public_fields_and_nullability() {
        let problem = openapi_schema::<ProblemDetails>();
        assert!(problem["properties"].get("title").is_some());

        let response = openapi_schema::<NegotiateResponse>();
        assert_eq!(
            response["properties"]["current_event_cursor"]["type"],
            json!(["string", "null"])
        );

        let frame = openapi_schema::<WebSocketFrame>();
        assert_eq!(
            frame["oneOf"][0]["properties"]["current_event_cursor"]["type"],
            json!(["string", "null"])
        );

        let tool_call = openapi_schema::<ToolCallResponse>();
        assert_eq!(
            tool_call["properties"]["stdout"]["type"],
            json!(["string", "null"])
        );
        assert_eq!(
            tool_call["properties"]["stderr"]["type"],
            json!(["string", "null"])
        );
        assert_eq!(
            tool_call["properties"]["exit_code"]["type"],
            json!(["integer", "null"])
        );
        for field in ["stdout", "stderr", "exit_code"] {
            assert!(
                tool_call["required"]
                    .as_array()
                    .expect("required fields")
                    .iter()
                    .any(|required| required == field)
            );
        }
    }
}
