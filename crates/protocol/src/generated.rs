use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use ts_rs::{Config, TS};

use crate::{
    APPEND_MESSAGE_OPERATION_ID, ARTIFACT_PATH, ASSIGN_TASK_OPERATION_ID, AppendMessageRequest,
    ApprovalDecision, ApprovalDecisionRequest, ApprovalPolicy, ApprovalResponse, ApprovalState,
    ArtifactResponse, AssignTaskRequest, CANCEL_RUN_OPERATION_ID, CREATE_SESSION_OPERATION_ID,
    CREATE_TASK_OPERATION_ID, CREATE_WORKSPACE_OPERATION_ID, ChangedFileResponse,
    ChildActivityReference, ClientIdentity, ContextManifestCreatedResponse, CreateTaskRequest,
    CreateWorkspaceRequest, DECIDE_APPROVAL_OPERATION_ID, DETERMINISTIC_SUBPROCESS_CAPABILITY,
    EVENT_STREAM_OPERATION_ID, EVENTS_WEBSOCKET_PATH, GET_ARTIFACT_OPERATION_ID,
    GET_RUN_OPERATION_ID, GET_SESSION_CHANGE_DIFF_OPERATION_ID, GET_SESSION_OPERATION_ID,
    GET_TASK_OPERATION_ID, GET_WORKSPACE_OPERATION_ID, IDEMPOTENCY_KEY_HEADER,
    LIST_SESSION_CHANGES_OPERATION_ID, LIST_SESSION_EVENTS_OPERATION_ID,
    LIST_SESSION_RUNS_OPERATION_ID, LIST_SESSIONS_OPERATION_ID, LIST_USAGE_OPERATION_ID,
    LIST_WORKSPACES_OPERATION_ID, ListSessionsResponse, ListWorkspacesResponse,
    MessageDeliveryMode, MessageDeliveryResponse, MessageDeliveryState, MessageResponse,
    MessageRole, MessageStatus, ModelInvocationCompletionKind, ModelInvocationEventResponse,
    ModelInvocationFailureReason, ModelInvocationPurpose, ModelInvocationStatus,
    ModelOutputRecordedResponse, ModelOutputStream, NEGOTIATE_OPERATION_ID, NEGOTIATE_PATH,
    NegotiateRequest, NegotiateResponse, PROTOCOL_VERSION, ProblemDetails,
    REACT_TO_RUN_ACTIVITY_OPERATION_ID, RUN_CANCEL_PATH, RUN_CHILDREN_PATH, RUN_INPUT_PATH,
    RUN_PATH, RUN_REACTIONS_PATH, ReactToRunActivityRequest, RunInputMode, RunResponse, RunState,
    SEND_RUN_INPUT_OPERATION_ID, SESSION_CHANGE_DIFF_PATH, SESSION_CHANGES_PATH,
    SESSION_EVENTS_PATH, SESSION_MESSAGES_PATH, SESSION_PATH, SESSION_RUNS_PATH,
    SESSION_TASKS_PATH, START_CHILD_RUN_OPERATION_ID, START_RUN_OPERATION_ID, SendRunInputRequest,
    SessionChangeDiffContent, SessionChangeDiffResponse, SessionChangeDiffUnavailableReason,
    SessionChangesResponse, SessionEventDataResponse, SessionEventResponse, SessionEventsResponse,
    SessionResponse, SessionRunsResponse, StartChildRunRequest, StartRunRequest, StoreIdentity,
    TASK_ASSIGNMENT_PATH, TASK_PATH, TASK_TRANSITION_PATH, TOOL_CALL_APPROVAL_PATH,
    TRANSITION_TASK_OPERATION_ID, TaskResponse, TaskState, ToolCallResponse, ToolCallState,
    ToolOutputStream, TransitionTaskRequest, UPDATE_TASK_OPERATION_ID, USAGE_PATH,
    UpdateTaskRequest, UsageAccounting, UsageCompleteness, UsageFinality, UsageLedgerEntryResponse,
    UsageLedgerResponse, UsageObservedResponse, UsageQuantityRelation, UsageQuantityResponse,
    UsageSource, WEBSOCKET_CAPABILITY, WORKSPACE_PATH, WORKSPACE_SESSIONS_PATH, WORKSPACES_PATH,
    WorkspaceResponse, WorkspaceRootRequest, WorkspaceRootResponse, WorkspaceScopeResponse,
    error_code,
};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "type")]
#[allow(
    clippy::large_enum_variant,
    reason = "wire frame DTOs are short-lived and do not justify mandatory heap allocation"
)]
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
        "fixtures/list-workspaces-response.json",
        fixture_list_workspaces_response(),
    );
    files.insert(
        "fixtures/append-message-request.json",
        fixture_append_message_request(),
    );
    files.insert(
        "fixtures/create-task-request.json",
        fixture_create_task_request(),
    );
    files.insert(
        "fixtures/update-task-request.json",
        fixture_update_task_request(),
    );
    files.insert(
        "fixtures/transition-task-request.json",
        fixture_transition_task_request(),
    );
    files.insert(
        "fixtures/assign-task-request.json",
        fixture_assign_task_request(),
    );
    files.insert("fixtures/session-response.json", fixture_session_response());
    files.insert(
        "fixtures/list-sessions-response.json",
        fixture_list_sessions_response(),
    );
    files.insert("fixtures/message-response.json", fixture_message_response());
    files.insert("fixtures/task-response.json", fixture_task_response());
    files.insert(
        "fixtures/start-run-request.json",
        fixture_start_run_request(),
    );
    files.insert(
        "fixtures/start-child-run-request.json",
        fixture_start_child_run_request(),
    );
    files.insert(
        "fixtures/send-run-input-request.json",
        fixture_send_run_input_request(),
    );
    files.insert(
        "fixtures/message-delivery-response.json",
        fixture_message_delivery_response(),
    );
    files.insert(
        "fixtures/approval-decision-request.json",
        fixture_approval_decision_request(),
    );
    files.insert("fixtures/run-response.json", fixture_run_response());
    files.insert(
        "fixtures/session-runs-response.json",
        fixture_session_runs_response(),
    );
    files.insert(
        "fixtures/session-events-response.json",
        fixture_session_events_response(),
    );
    files.insert(
        "fixtures/session-changes-response.json",
        fixture_session_changes_response(),
    );
    files.insert(
        "fixtures/session-change-diff-response.json",
        fixture_session_change_diff_response(),
    );
    files.insert(
        "fixtures/usage-ledger-response.json",
        fixture_usage_ledger_response(),
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
        (
            "ListWorkspacesResponse",
            schema_for!(ListWorkspacesResponse),
        ),
        ("AppendMessageRequest", schema_for!(AppendMessageRequest)),
        ("CreateTaskRequest", schema_for!(CreateTaskRequest)),
        ("UpdateTaskRequest", schema_for!(UpdateTaskRequest)),
        ("TransitionTaskRequest", schema_for!(TransitionTaskRequest)),
        ("AssignTaskRequest", schema_for!(AssignTaskRequest)),
        ("SessionResponse", schema_for!(SessionResponse)),
        ("ListSessionsResponse", schema_for!(ListSessionsResponse)),
        ("MessageRole", schema_for!(MessageRole)),
        ("MessageStatus", schema_for!(MessageStatus)),
        ("MessageResponse", schema_for!(MessageResponse)),
        ("MessageDeliveryMode", schema_for!(MessageDeliveryMode)),
        ("MessageDeliveryState", schema_for!(MessageDeliveryState)),
        (
            "MessageDeliveryResponse",
            schema_for!(MessageDeliveryResponse),
        ),
        ("TaskState", schema_for!(TaskState)),
        ("TaskResponse", schema_for!(TaskResponse)),
        ("StartRunRequest", schema_for!(StartRunRequest)),
        ("StartChildRunRequest", schema_for!(StartChildRunRequest)),
        ("SendRunInputRequest", schema_for!(SendRunInputRequest)),
        (
            "ChildActivityReference",
            schema_for!(ChildActivityReference),
        ),
        (
            "ReactToRunActivityRequest",
            schema_for!(ReactToRunActivityRequest),
        ),
        ("ApprovalPolicy", schema_for!(ApprovalPolicy)),
        (
            "ApprovalDecisionRequest",
            schema_for!(ApprovalDecisionRequest),
        ),
        ("ApprovalDecision", schema_for!(ApprovalDecision)),
        ("RunState", schema_for!(RunState)),
        ("RunInputMode", schema_for!(RunInputMode)),
        ("ToolCallState", schema_for!(ToolCallState)),
        ("ToolOutputStream", schema_for!(ToolOutputStream)),
        ("ArtifactResponse", schema_for!(ArtifactResponse)),
        (
            "WorkspaceScopeResponse",
            schema_for!(WorkspaceScopeResponse),
        ),
        ("ApprovalState", schema_for!(ApprovalState)),
        ("ApprovalResponse", schema_for!(ApprovalResponse)),
        ("ToolCallResponse", schema_for!(ToolCallResponse)),
        ("RunResponse", schema_for!(RunResponse)),
        ("SessionRunsResponse", schema_for!(SessionRunsResponse)),
        ("ChangedFileResponse", schema_for!(ChangedFileResponse)),
        (
            "SessionChangesResponse",
            schema_for!(SessionChangesResponse),
        ),
        (
            "SessionChangeDiffUnavailableReason",
            schema_for!(SessionChangeDiffUnavailableReason),
        ),
        (
            "SessionChangeDiffContent",
            schema_for!(SessionChangeDiffContent),
        ),
        (
            "SessionChangeDiffResponse",
            schema_for!(SessionChangeDiffResponse),
        ),
        (
            "ContextManifestCreatedResponse",
            schema_for!(ContextManifestCreatedResponse),
        ),
        (
            "ModelInvocationPurpose",
            schema_for!(ModelInvocationPurpose),
        ),
        (
            "ModelInvocationCompletionKind",
            schema_for!(ModelInvocationCompletionKind),
        ),
        (
            "ModelInvocationFailureReason",
            schema_for!(ModelInvocationFailureReason),
        ),
        ("ModelOutputStream", schema_for!(ModelOutputStream)),
        (
            "ModelOutputRecordedResponse",
            schema_for!(ModelOutputRecordedResponse),
        ),
        ("UsageCompleteness", schema_for!(UsageCompleteness)),
        ("UsageAccounting", schema_for!(UsageAccounting)),
        ("UsageFinality", schema_for!(UsageFinality)),
        ("UsageSource", schema_for!(UsageSource)),
        ("UsageQuantityRelation", schema_for!(UsageQuantityRelation)),
        ("UsageQuantityResponse", schema_for!(UsageQuantityResponse)),
        (
            "UsageLedgerEntryResponse",
            schema_for!(UsageLedgerEntryResponse),
        ),
        ("UsageLedgerResponse", schema_for!(UsageLedgerResponse)),
        ("UsageObservedResponse", schema_for!(UsageObservedResponse)),
        ("ModelInvocationStatus", schema_for!(ModelInvocationStatus)),
        (
            "ModelInvocationEventResponse",
            schema_for!(ModelInvocationEventResponse),
        ),
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
        ListWorkspacesResponse::decl(&config),
        AppendMessageRequest::decl(&config),
        CreateTaskRequest::decl(&config),
        UpdateTaskRequest::decl(&config),
        TransitionTaskRequest::decl(&config),
        AssignTaskRequest::decl(&config),
        SessionResponse::decl(&config),
        ListSessionsResponse::decl(&config),
        MessageRole::decl(&config),
        MessageStatus::decl(&config),
        MessageResponse::decl(&config),
        MessageDeliveryMode::decl(&config),
        MessageDeliveryState::decl(&config),
        MessageDeliveryResponse::decl(&config),
        TaskState::decl(&config),
        TaskResponse::decl(&config),
        StartRunRequest::decl(&config),
        StartChildRunRequest::decl(&config),
        SendRunInputRequest::decl(&config),
        ChildActivityReference::decl(&config),
        ReactToRunActivityRequest::decl(&config),
        ApprovalPolicy::decl(&config),
        ApprovalDecisionRequest::decl(&config),
        ApprovalDecision::decl(&config),
        RunState::decl(&config),
        RunInputMode::decl(&config),
        ToolCallState::decl(&config),
        ToolOutputStream::decl(&config),
        ArtifactResponse::decl(&config),
        WorkspaceScopeResponse::decl(&config),
        ApprovalState::decl(&config),
        ApprovalResponse::decl(&config),
        ToolCallResponse::decl(&config),
        RunResponse::decl(&config),
        SessionRunsResponse::decl(&config),
        ChangedFileResponse::decl(&config),
        SessionChangesResponse::decl(&config),
        SessionChangeDiffUnavailableReason::decl(&config),
        SessionChangeDiffContent::decl(&config),
        SessionChangeDiffResponse::decl(&config),
        ContextManifestCreatedResponse::decl(&config),
        ModelInvocationPurpose::decl(&config),
        ModelInvocationCompletionKind::decl(&config),
        ModelInvocationFailureReason::decl(&config),
        ModelOutputStream::decl(&config),
        ModelOutputRecordedResponse::decl(&config),
        UsageCompleteness::decl(&config),
        UsageAccounting::decl(&config),
        UsageFinality::decl(&config),
        UsageSource::decl(&config),
        UsageQuantityRelation::decl(&config),
        UsageQuantityResponse::decl(&config),
        UsageLedgerEntryResponse::decl(&config),
        UsageLedgerResponse::decl(&config),
        UsageObservedResponse::decl(&config),
        ModelInvocationStatus::decl(&config),
        ModelInvocationEventResponse::decl(&config),
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
            "path": WORKSPACES_PATH,
            "operation": LIST_WORKSPACES_OPERATION_ID
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
            "path": WORKSPACE_SESSIONS_PATH,
            "operation": LIST_SESSIONS_OPERATION_ID
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
            "path": SESSION_TASKS_PATH,
            "operation": CREATE_TASK_OPERATION_ID
        }, {
            "method": "GET",
            "path": TASK_PATH,
            "operation": GET_TASK_OPERATION_ID
        }, {
            "method": "PATCH",
            "path": TASK_PATH,
            "operation": UPDATE_TASK_OPERATION_ID
        }, {
            "method": "POST",
            "path": TASK_ASSIGNMENT_PATH,
            "operation": ASSIGN_TASK_OPERATION_ID
        }, {
            "method": "POST",
            "path": TASK_TRANSITION_PATH,
            "operation": TRANSITION_TASK_OPERATION_ID
        }, {
            "method": "POST",
            "path": SESSION_RUNS_PATH,
            "operation": START_RUN_OPERATION_ID
        }, {
            "method": "GET",
            "path": SESSION_RUNS_PATH,
            "operation": LIST_SESSION_RUNS_OPERATION_ID
        }, {
            "method": "POST",
            "path": RUN_CHILDREN_PATH,
            "operation": START_CHILD_RUN_OPERATION_ID
        }, {
            "method": "GET",
            "path": RUN_PATH,
            "operation": GET_RUN_OPERATION_ID
        }, {
            "method": "POST",
            "path": RUN_INPUT_PATH,
            "operation": SEND_RUN_INPUT_OPERATION_ID
        }, {
            "method": "POST",
            "path": RUN_REACTIONS_PATH,
            "operation": REACT_TO_RUN_ACTIVITY_OPERATION_ID
        }, {
            "method": "POST",
            "path": TOOL_CALL_APPROVAL_PATH,
            "operation": DECIDE_APPROVAL_OPERATION_ID
        }, {
            "method": "POST",
            "path": RUN_CANCEL_PATH,
            "operation": CANCEL_RUN_OPERATION_ID
        }, {
            "method": "GET",
            "path": ARTIFACT_PATH,
            "operation": GET_ARTIFACT_OPERATION_ID
        }, {
            "method": "GET",
            "path": SESSION_CHANGES_PATH,
            "operation": LIST_SESSION_CHANGES_OPERATION_ID
        }, {
            "method": "GET",
            "path": SESSION_CHANGE_DIFF_PATH,
            "operation": GET_SESSION_CHANGE_DIFF_OPERATION_ID
        }, {
            "method": "GET",
            "path": USAGE_PATH,
            "operation": LIST_USAGE_OPERATION_ID
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

fn fixture_list_workspaces_response() -> String {
    serialize_fixture(&ListWorkspacesResponse {
        workspaces: vec![WorkspaceResponse {
            workspace_id: "wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            name: "fixture-workspace".to_owned(),
            roots: Vec::new(),
        }],
    })
}

fn fixture_append_message_request() -> String {
    serialize_fixture(&AppendMessageRequest {
        content: "Inspect the Workspace event model.".to_owned(),
    })
}

fn fixture_create_task_request() -> String {
    serialize_fixture(&CreateTaskRequest {
        objective: "Inspect durable Task recovery.".to_owned(),
        parent_task_id: None,
        dependency_task_ids: Vec::new(),
    })
}

fn fixture_update_task_request() -> String {
    serialize_fixture(&UpdateTaskRequest {
        objective: "Inspect durable Task lifecycle recovery.".to_owned(),
        dependency_task_ids: vec!["tsk_01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned()],
    })
}

fn fixture_transition_task_request() -> String {
    serialize_fixture(&TransitionTaskRequest {
        state: TaskState::Ready,
    })
}

fn fixture_assign_task_request() -> String {
    serialize_fixture(&AssignTaskRequest {
        run_id: "run_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
    })
}

fn fixture_session_response() -> String {
    serialize_fixture(&SessionResponse {
        session_id: "ses_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        workspace_id: "wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
    })
}

fn fixture_list_sessions_response() -> String {
    serialize_fixture(&ListSessionsResponse {
        sessions: vec![SessionResponse {
            session_id: "ses_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            workspace_id: "wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        }],
    })
}

fn fixture_message() -> MessageResponse {
    MessageResponse {
        message_id: "msg_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        session_id: "ses_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        role: MessageRole::User,
        status: MessageStatus::Complete,
        origin_run_id: None,
        model_invocation_id: None,
        content: "Inspect the Workspace event model.".to_owned(),
        target_run_id: None,
        child_activity: None,
    }
}

fn fixture_message_response() -> String {
    serialize_fixture(&fixture_message())
}

fn fixture_task() -> TaskResponse {
    TaskResponse {
        task_id: "tsk_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        session_id: "ses_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        objective: "Inspect durable Task recovery.".to_owned(),
        state: TaskState::Pending,
        parent_task_id: None,
        dependency_task_ids: Vec::new(),
        assigned_run_id: None,
    }
}

fn fixture_task_response() -> String {
    serialize_fixture(&fixture_task())
}

fn fixture_scope() -> WorkspaceScopeResponse {
    WorkspaceScopeResponse {
        workspace_root_id: "wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        relative_directory: ".".to_owned(),
    }
}

fn fixture_start_run_request() -> String {
    serialize_fixture(&StartRunRequest {
        approval_policy: ApprovalPolicy::FullAccess,
        workspace_root_id: fixture_scope().workspace_root_id,
        relative_directory: fixture_scope().relative_directory,
    })
}

fn fixture_start_child_run_request() -> String {
    serialize_fixture(&StartChildRunRequest {
        approval_policy: ApprovalPolicy::FullAccess,
        workspace_root_id: fixture_scope().workspace_root_id,
        relative_directory: fixture_scope().relative_directory,
        user_input_mode: RunInputMode::ReadOnly,
        task_id: Some("tsk_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
    })
}

fn fixture_send_run_input_request() -> String {
    serialize_fixture(&SendRunInputRequest {
        content: "Use the durable event cursor as the next checkpoint.".to_owned(),
        delivery_mode: MessageDeliveryMode::Queued,
    })
}

fn fixture_message_delivery_response() -> String {
    let mut message = fixture_message();
    message.target_run_id = Some("run_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned());
    serialize_fixture(&MessageDeliveryResponse {
        message,
        delivery_mode: MessageDeliveryMode::Queued,
        state: MessageDeliveryState::Queued,
    })
}

fn fixture_approval_decision_request() -> String {
    serialize_fixture(&ApprovalDecisionRequest {
        decision: ApprovalDecision::Approved,
    })
}

fn fixture_tool_call() -> ToolCallResponse {
    ToolCallResponse {
        tool_call_id: "tcl_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        run_id: "run_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        capability: DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
        state: ToolCallState::Completed,
        requested_scope: Some(fixture_scope()),
        effective_scope: Some(fixture_scope()),
        stdout: Some("Kiln deterministic subprocess completed.\n".to_owned()),
        stderr: Some(String::new()),
        stdout_artifact: None,
        stderr_artifact: None,
        exit_code: Some(0),
    }
}

fn fixture_run() -> RunResponse {
    RunResponse {
        run_id: "run_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        session_id: "ses_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        parent_run_id: None,
        task_id: None,
        user_input_mode: RunInputMode::Interactive,
        state: RunState::Completed,
        approval_policy: Some(ApprovalPolicy::FullAccess),
        requested_scope: Some(fixture_scope()),
        tool_calls: vec![fixture_tool_call()],
        approvals: Vec::new(),
    }
}

fn fixture_run_response() -> String {
    serialize_fixture(&fixture_run())
}

fn fixture_session_runs_response() -> String {
    serialize_fixture(&SessionRunsResponse {
        runs: vec![fixture_run()],
    })
}

fn fixture_session_changes_response() -> String {
    serialize_fixture(&SessionChangesResponse {
        workspace_root_id: "wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        relative_directory: ".".to_owned(),
        files: vec![ChangedFileResponse {
            path: "src/main.rs".to_owned(),
            kind: "modified".to_owned(),
            additions: Some(4),
            deletions: Some(2),
        }],
    })
}

fn fixture_session_change_diff_response() -> String {
    serialize_fixture(&SessionChangeDiffResponse {
        workspace_root_id: "wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        relative_directory: ".".to_owned(),
        path: "src/main.rs".to_owned(),
        kind: "modified".to_owned(),
        content: SessionChangeDiffContent::Ready {
            patch: "@@ -1,1 +1,1 @@\n-old\n+new\n".to_owned(),
            truncated: false,
        },
    })
}

fn fixture_usage_ledger_response() -> String {
    serialize_fixture(&UsageLedgerResponse {
        entries: vec![UsageLedgerEntryResponse {
            usage_observation_id: "uso_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            model_invocation_id: "miv_01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            work_id: "wrk_01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
            run_id: "run_01ARZ3NDEKTSV4RRFFQ69G5FAY".to_owned(),
            session_id: "ses_01ARZ3NDEKTSV4RRFFQ69G5FAZ".to_owned(),
            workspace_id: "wsp_01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned(),
            provider_account_id: "pac_01ARZ3NDEKTSV4RRFFQ69G5FB1".to_owned(),
            requested_model: "fixture-model".to_owned(),
            revision: 1,
            supersedes_usage_observation_id: None,
            update_id: "upd_fixture".to_owned(),
            accounting: UsageAccounting::Cumulative,
            finality: UsageFinality::Final,
            completeness: UsageCompleteness::Unknown,
            observed_at_unix_ms: 1_700_000_000_000,
            request_id: None,
            resolved_model: None,
            service_tier: None,
            source: UsageSource::NativeProvider,
            quantities: vec![UsageQuantityResponse {
                dimension: "tokens.input".to_owned(),
                unit: "token".to_owned(),
                amount: 0,
                relation: UsageQuantityRelation::Additive,
                subset_of: None,
            }],
            is_terminal: true,
        }],
        next_cursor: None,
    })
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
security:
  - bearerAuth: []
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
        - name: after
          in: query
          required: false
          schema: {{type: string}}
          description: Exclusive durable Event cursor for reconnect replay.
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
    get:
      operationId: {LIST_WORKSPACES_OPERATION_ID}
      responses:
        '200':
          description: Stored workspace snapshots ordered by workspace ID.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ListWorkspacesResponse'
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
        '503':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
    get:
      operationId: {LIST_SESSIONS_OPERATION_ID}
      parameters:
        - name: workspace_id
          in: path
          required: true
          schema: {{type: string}}
      responses:
        '200':
          description: Sessions for the workspace ordered by session ID.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ListSessionsResponse'
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
        '503':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
  {SESSION_TASKS_PATH}:
    post:
      operationId: {CREATE_TASK_OPERATION_ID}
      parameters:
        - name: session_id
          in: path
          required: true
          schema: {{type: string}}
        - name: {IDEMPOTENCY_KEY_HEADER}
          in: header
          required: true
          schema: {{type: string, minLength: 1}}
          description: Opaque non-empty key for idempotent Task creation.
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/CreateTaskRequest'
      responses:
        '201':
          description: Durable pending Task.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/TaskResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '409':
          $ref: '#/components/responses/Problem'
        '503':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
  {TASK_PATH}:
    get:
      operationId: {GET_TASK_OPERATION_ID}
      parameters:
        - name: task_id
          in: path
          required: true
          schema: {{type: string}}
      responses:
        '200':
          description: Durable Task snapshot.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/TaskResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
    patch:
      operationId: {UPDATE_TASK_OPERATION_ID}
      parameters:
        - name: task_id
          in: path
          required: true
          schema: {{type: string}}
        - name: {IDEMPOTENCY_KEY_HEADER}
          in: header
          required: true
          schema: {{type: string, minLength: 1}}
          description: Opaque non-empty key for idempotent Task updates.
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/UpdateTaskRequest'
      responses:
        '200':
          description: Updated durable Task snapshot.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/TaskResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '409':
          $ref: '#/components/responses/Problem'
        '503':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
  {TASK_ASSIGNMENT_PATH}:
    post:
      operationId: {ASSIGN_TASK_OPERATION_ID}
      parameters:
        - name: task_id
          in: path
          required: true
          schema: {{type: string}}
        - name: {IDEMPOTENCY_KEY_HEADER}
          in: header
          required: true
          schema: {{type: string, minLength: 1}}
          description: Opaque non-empty key for idempotent Task assignment.
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/AssignTaskRequest'
      responses:
        '200':
          description: Assigned durable Task snapshot.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/TaskResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '409':
          $ref: '#/components/responses/Problem'
        '503':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
  {TASK_TRANSITION_PATH}:
    post:
      operationId: {TRANSITION_TASK_OPERATION_ID}
      parameters:
        - name: task_id
          in: path
          required: true
          schema: {{type: string}}
        - name: {IDEMPOTENCY_KEY_HEADER}
          in: header
          required: true
          schema: {{type: string, minLength: 1}}
          description: Opaque non-empty key for idempotent Task transitions.
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/TransitionTaskRequest'
      responses:
        '200':
          description: Transitioned durable Task snapshot.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/TaskResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '409':
          $ref: '#/components/responses/Problem'
        '503':
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
  {SESSION_CHANGES_PATH}:

    get:
      operationId: {LIST_SESSION_CHANGES_OPERATION_ID}
      parameters:
        - name: session_id
          in: path
          required: true
          schema: {{type: string}}
      responses:
        '200':
          description: Read-only change summary for the Session's captured checkout.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/SessionChangesResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
        '503':
          $ref: '#/components/responses/Problem'

  {SESSION_CHANGE_DIFF_PATH}:

    get:
      operationId: {GET_SESSION_CHANGE_DIFF_OPERATION_ID}
      parameters:
        - name: session_id
          in: path
          required: true
          schema: {{type: string}}
        - name: path
          in: query
          required: true
          schema: {{type: string, minLength: 1}}
          description: Normalized changed file path relative to the captured checkout.
      responses:
        '200':
          description: Bounded read-only diff for one changed text file.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/SessionChangeDiffResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
        '503':
          $ref: '#/components/responses/Problem'

  {USAGE_PATH}:

    get:
      operationId: {LIST_USAGE_OPERATION_ID}
      parameters:
        - name: after
          in: query
          required: false
          schema: {{type: string}}
          description: Exclusive model invocation ID cursor for the next page.
        - name: limit
          in: query
          required: false
          schema: {{type: integer, minimum: 1, maximum: 1000, default: 100}}
          description: Maximum number of latest physical invocation revisions to return.
      responses:
        '200':
          description: Latest usage revision for each physical model invocation, ordered by invocation ID.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/UsageLedgerResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'

  {SESSION_RUNS_PATH}:
    get:
      operationId: {LIST_SESSION_RUNS_OPERATION_ID}
      parameters:
        - name: session_id
          in: path
          required: true
          schema: {{type: string}}
      responses:
        '200':
          description: Flat durable Run list whose parent IDs form the Session Run tree.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/SessionRunsResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
    post:
      operationId: {START_RUN_OPERATION_ID}
      parameters:
        - name: session_id
          in: path
          required: true
          schema: {{type: string}}
        - name: {IDEMPOTENCY_KEY_HEADER}
          in: header
          required: true
          schema: {{type: string, minLength: 1}}
          description: Opaque non-empty key for idempotent start-run delivery.
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/StartRunRequest'
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
        '503':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
  {RUN_CHILDREN_PATH}:
    post:
      operationId: {START_CHILD_RUN_OPERATION_ID}
      parameters:
        - name: parent_run_id
          in: path
          required: true
          schema: {{type: string}}
        - name: {IDEMPOTENCY_KEY_HEADER}
          in: header
          required: true
          schema: {{type: string, minLength: 1}}
          description: Opaque non-empty key scoped to the parent Run.
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/StartChildRunRequest'
      responses:
        '202':
          description: Accepted queued child Run.
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
        '503':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
  {TOOL_CALL_APPROVAL_PATH}:
    post:
      operationId: {DECIDE_APPROVAL_OPERATION_ID}
      parameters:
        - name: tool_call_id
          in: path
          required: true
          schema: {{type: string}}
        - name: {IDEMPOTENCY_KEY_HEADER}
          in: header
          required: true
          schema: {{type: string, minLength: 1}}
          description: Opaque non-empty key for idempotent approval delivery.
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/ApprovalDecisionRequest'
      responses:
        '200':
          description: Durable Run snapshot after the approval decision.
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
        '503':
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
  {RUN_INPUT_PATH}:
    post:
      operationId: {SEND_RUN_INPUT_OPERATION_ID}
      parameters:
        - name: run_id
          in: path
          required: true
          schema: {{type: string}}
        - name: {IDEMPOTENCY_KEY_HEADER}
          in: header
          required: true
          schema: {{type: string, minLength: 1}}
          description: Opaque non-empty key scoped to the target Run.
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/SendRunInputRequest'
      responses:
        '201':
          description: Durable queued targeted input.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/MessageDeliveryResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '409':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
        '503':
          $ref: '#/components/responses/Problem'
  {RUN_REACTIONS_PATH}:
    post:
      operationId: {REACT_TO_RUN_ACTIVITY_OPERATION_ID}
      parameters:
        - name: run_id
          in: path
          required: true
          schema: {{type: string}}
        - name: {IDEMPOTENCY_KEY_HEADER}
          in: header
          required: true
          schema: {{type: string, minLength: 1}}
          description: Opaque non-empty key scoped to the target Run.
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/ReactToRunActivityRequest'
      responses:
        '201':
          description: Durable queued reaction to child Run activity.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/MessageDeliveryResponse'
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '409':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
        '503':
          $ref: '#/components/responses/Problem'

  {RUN_CANCEL_PATH}:
    post:
      operationId: {CANCEL_RUN_OPERATION_ID}
      parameters:
        - name: run_id
          in: path
          required: true
          schema: {{type: string}}
      responses:
        '200':
          description: Durable terminal Run after cancellation completes.
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
        '503':
          $ref: '#/components/responses/Problem'
  {ARTIFACT_PATH}:
    get:
      operationId: {GET_ARTIFACT_OPERATION_ID}
      parameters:
        - name: content_hash
          in: path
          required: true
          schema: {{type: string, pattern: '^[0-9a-f]{{64}}$'}}
      responses:
        '200':
          description: Immutable artifact bytes.
          headers:
            ETag:
              schema: {{type: string}}
            X-Content-Type-Options:
              schema: {{type: string, const: nosniff}}
          content:
            application/octet-stream:
              schema: {{type: string, format: binary}}
        '400':
          $ref: '#/components/responses/Problem'
        '404':
          $ref: '#/components/responses/Problem'
        '500':
          $ref: '#/components/responses/Problem'
components:
  securitySchemes:
    bearerAuth:
      type: http
      scheme: bearer
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
        ("CreateTaskRequest", openapi_schema::<CreateTaskRequest>()),
        ("UpdateTaskRequest", openapi_schema::<UpdateTaskRequest>()),
        (
            "TransitionTaskRequest",
            openapi_schema::<TransitionTaskRequest>(),
        ),
        ("AssignTaskRequest", openapi_schema::<AssignTaskRequest>()),
        ("SessionResponse", openapi_schema::<SessionResponse>()),
        ("MessageRole", openapi_schema::<MessageRole>()),
        ("MessageStatus", openapi_schema::<MessageStatus>()),
        ("MessageResponse", openapi_schema::<MessageResponse>()),
        (
            "MessageDeliveryMode",
            openapi_schema::<MessageDeliveryMode>(),
        ),
        (
            "MessageDeliveryState",
            openapi_schema::<MessageDeliveryState>(),
        ),
        (
            "MessageDeliveryResponse",
            openapi_schema::<MessageDeliveryResponse>(),
        ),
        ("TaskState", openapi_schema::<TaskState>()),
        ("TaskResponse", openapi_schema::<TaskResponse>()),
        ("StartRunRequest", openapi_schema::<StartRunRequest>()),
        (
            "StartChildRunRequest",
            openapi_schema::<StartChildRunRequest>(),
        ),
        (
            "SendRunInputRequest",
            openapi_schema::<SendRunInputRequest>(),
        ),
        (
            "ChildActivityReference",
            openapi_schema::<ChildActivityReference>(),
        ),
        (
            "ReactToRunActivityRequest",
            openapi_schema::<ReactToRunActivityRequest>(),
        ),
        ("ApprovalPolicy", openapi_schema::<ApprovalPolicy>()),
        (
            "ApprovalDecisionRequest",
            openapi_schema::<ApprovalDecisionRequest>(),
        ),
        ("ApprovalDecision", openapi_schema::<ApprovalDecision>()),
        ("RunState", openapi_schema::<RunState>()),
        ("RunInputMode", openapi_schema::<RunInputMode>()),
        ("ToolCallState", openapi_schema::<ToolCallState>()),
        ("ToolOutputStream", openapi_schema::<ToolOutputStream>()),
        ("ArtifactResponse", openapi_schema::<ArtifactResponse>()),
        (
            "WorkspaceScopeResponse",
            openapi_schema::<WorkspaceScopeResponse>(),
        ),
        ("ApprovalState", openapi_schema::<ApprovalState>()),
        ("ApprovalResponse", openapi_schema::<ApprovalResponse>()),
        ("ToolCallResponse", openapi_schema::<ToolCallResponse>()),
        ("RunResponse", openapi_schema::<RunResponse>()),
        (
            "SessionRunsResponse",
            openapi_schema::<SessionRunsResponse>(),
        ),
        (
            "ChangedFileResponse",
            openapi_schema::<ChangedFileResponse>(),
        ),
        (
            "SessionChangesResponse",
            openapi_schema::<SessionChangesResponse>(),
        ),
        (
            "SessionChangeDiffUnavailableReason",
            openapi_schema::<SessionChangeDiffUnavailableReason>(),
        ),
        (
            "SessionChangeDiffContent",
            openapi_schema::<SessionChangeDiffContent>(),
        ),
        (
            "SessionChangeDiffResponse",
            openapi_schema::<SessionChangeDiffResponse>(),
        ),
        (
            "SessionEventDataResponse",
            openapi_schema::<SessionEventDataResponse>(),
        ),
        (
            "ContextManifestCreatedResponse",
            openapi_schema::<ContextManifestCreatedResponse>(),
        ),
        (
            "ModelInvocationPurpose",
            openapi_schema::<ModelInvocationPurpose>(),
        ),
        (
            "ModelInvocationCompletionKind",
            openapi_schema::<ModelInvocationCompletionKind>(),
        ),
        (
            "ModelInvocationFailureReason",
            openapi_schema::<ModelInvocationFailureReason>(),
        ),
        ("ModelOutputStream", openapi_schema::<ModelOutputStream>()),
        (
            "ModelOutputRecordedResponse",
            openapi_schema::<ModelOutputRecordedResponse>(),
        ),
        ("UsageCompleteness", openapi_schema::<UsageCompleteness>()),
        ("UsageAccounting", openapi_schema::<UsageAccounting>()),
        ("UsageFinality", openapi_schema::<UsageFinality>()),
        ("UsageSource", openapi_schema::<UsageSource>()),
        (
            "UsageQuantityRelation",
            openapi_schema::<UsageQuantityRelation>(),
        ),
        (
            "UsageQuantityResponse",
            openapi_schema::<UsageQuantityResponse>(),
        ),
        (
            "UsageLedgerEntryResponse",
            openapi_schema::<UsageLedgerEntryResponse>(),
        ),
        (
            "UsageLedgerResponse",
            openapi_schema::<UsageLedgerResponse>(),
        ),
        (
            "UsageObservedResponse",
            openapi_schema::<UsageObservedResponse>(),
        ),
        (
            "ModelInvocationStatus",
            openapi_schema::<ModelInvocationStatus>(),
        ),
        (
            "ModelInvocationEventResponse",
            openapi_schema::<ModelInvocationEventResponse>(),
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
        "# Kiln protocol reference\n\nProtocol version: `{PROTOCOL_VERSION}`.\n\nAll loopback HTTP requests require the persistent local credential as `Authorization: Bearer <token>`. The server also validates the exact bound `Host` and, when present, the loopback `Origin`.\n\nMessage responses distinguish `user` and `assistant` roles and `complete` or `incomplete` status. Assistant Messages identify their originating Run and model invocation; user Messages have null origin fields. The user-message append endpoint cannot submit assistant Messages.\n\n`model_invocation.output` carries one durable ordered assistant-text or provider-exposed reasoning-summary chunk. Its immutable chunk ID and per-attempt position support exact replay. It does not carry hidden provider reasoning. Chunks are distinct from finalized assistant Messages. With `KILN_RUN_EXECUTOR=deterministic-model`, the daemon runs the explicit native fixture and publishes committed output and assistant Message Events. The default executor remains `deterministic-subprocess`.\n\n`usage.observed` identifies one immutable usage observation revision for a physical model invocation attempt. It carries the logical work and account identity, revision link, completeness, and terminal status. It contains no quantities, prices, prompt text, output text, or raw provider payloads. The read-only `GET {USAGE_PATH}` ledger lists the latest validated revision for each physical model invocation, ordered by invocation ID. It accepts an exclusive `after` invocation cursor and a bounded `limit` (default 100, maximum 1000); `next_cursor` is null when the page is complete. Quantities preserve explicit zero values and missing dimensions, and entries carry completeness and source metadata. The ledger reports observations only; pricing, cost, valuation, and allowance data are not included.\n\nThe client sends `POST {NEGOTIATE_PATH}` with a version range, client identity, and requested capabilities. The server returns the selected version, capability lists, store identity, current event cursor, and the event WebSocket endpoint.\n\nThe client creates a durable Workspace with `POST {WORKSPACES_PATH}`, providing a name and one or more named local Git repository roots. `GET {WORKSPACES_PATH}` returns all stored Workspaces ordered by `workspace_id`; an empty store returns an empty `workspaces` array. `GET {WORKSPACE_PATH}` returns the stored snapshot.\n\n`POST {WORKSPACE_SESSIONS_PATH}` creates a Session attached to one Workspace. `GET {WORKSPACE_SESSIONS_PATH}` returns only that Workspace's Sessions ordered by `session_id`; an empty selection returns an empty `sessions` array. An unknown Workspace returns `workspace_not_found`. `GET {SESSION_PATH}` returns it. New Sessions capture one registered workspace checkout (the first root at creation and the normalized `.` directory); older Sessions without a persisted checkout remain readable but do not support change summaries.

`GET {SESSION_CHANGES_PATH}` returns a read-only summary for that captured checkout. It combines staged and unstaged tracked changes against `HEAD` once, adds all untracked files under the checkout directory, and sorts paths. Untracked and binary files expose null additions/deletions rather than zero. Each Git invocation is capped at 4 MiB of output and 30 seconds; exceeding either limit returns `git_unavailable`. The operation does not run external diff or text conversion helpers and does not claim worktree isolation.\n\n`GET {SESSION_CHANGE_DIFF_PATH}?path=...` returns a bounded read-only unified diff for one changed text file from the captured checkout. Input files are capped at 4 MiB and the UTF-8 patch preview at 256 KiB. Untracked, binary, conflicted, renamed, unsupported file types, and unsupported encodings return explicit unavailable reasons.\n\n`POST {SESSION_MESSAGES_PATH}` accepts immutable user message content. Kiln creates the untargeted Message and its `message.appended` Event atomically. Clients cannot append arbitrary Events.\n\n`POST {SESSION_TASKS_PATH}` requires a non-empty opaque `{IDEMPOTENCY_KEY_HEADER}` header. It creates one durable pending Task and one `task.created` Event atomically. Parent and dependency links must target Tasks in the same Session. A repeated key with the same normalized request returns the original Task. A mismatched reuse is an idempotency conflict. `GET {TASK_PATH}` returns the durable Task snapshot.\n\n`POST {SESSION_RUNS_PATH}` requires a non-empty opaque `{IDEMPOTENCY_KEY_HEADER}` header and creates an interactive root Run with durable `run.created` and `run.queued` Events. `POST {RUN_CHILDREN_PATH}` creates one child in the same Session with immutable `parent_run_id`, optional Task assignment, and `interactive` or `read_only` user input mode. It atomically records `run.created`, `run.queued`, `run.child_added`, and, when linked, `task.assigned`. Child keys are scoped to the parent Run. Exact retries return the existing Run without new Events; mismatched key reuse conflicts. `GET {SESSION_RUNS_PATH}` returns a flat ordered list whose parent IDs form the Run tree.\n\n`POST {RUN_INPUT_PATH}` requires `{IDEMPOTENCY_KEY_HEADER}` and creates one user Message targeted to an interactive input-accepting Run plus one queued MessageDelivery. The Message and ordered `message.appended` then `run.input_queued` or `run.interrupt_requested` Events commit atomically. `queued` is normal guidance for a future safe boundary; `interrupt` must be explicit. Exact retries return the original delivery even after the Run becomes terminal. Delivery recording is an internal runtime boundary: successful and failed outcomes are FIFO per Run, while cancellation is valid only after Run termination. This release does not claim provider delivery or scheduler integration.\n\n`ask` records a pending Approval before execution. `read_only` durably denies the subprocess. `full_access` makes the requested scope effective without a prompt. `POST {TOOL_CALL_APPROVAL_PATH}` approves or rejects one pending ToolCall. Approval decisions are durable, first-decision-wins, and idempotent by their own `{IDEMPOTENCY_KEY_HEADER}`. An approval after daemon restart resumes the same Run.\n\nTool output larger than 4,096 bytes is stored as one immutable artifact instead of inline Event content. The `artifact.registered` Event and terminal ToolCall include the content hash, media type, and decimal byte size. `GET {ARTIFACT_PATH}` returns the verified bytes with safe download headers.\n\n`POST {RUN_CANCEL_PATH}` is naturally idempotent for the addressed Run. It records `run.cancellation_requested`, stops owned execution, waits for process exit, and returns the durable terminal Run. Cancelling while approval is pending rejects that Approval and denies the ToolCall without starting a process. A completion committed before the cancellation request remains authoritative.\n\nSIGINT and SIGTERM start graceful shutdown. Kiln rejects later mutating commands, lets already accepted commands commit, cancels active Runs, and closes only after their terminal state is durable. Read-only requests remain available while the daemon drains.\n\n`GET {SESSION_EVENTS_PATH}?after=0` returns that Session's committed Events after the opaque decimal cursor. Event IDs are stable identities. The daemon-wide cursor orders committed audit records; it is not required to be numerically contiguous. The response current cursor and Event rows come from one storage snapshot.\n\nThe client connects to `GET {EVENTS_WEBSOCKET_PATH}?version={PROTOCOL_VERSION}&capability={WEBSOCKET_CAPABILITY}&after={{cursor}}` and offers `{WEBSOCKET_CAPABILITY}` plus `kiln.auth.<token>` as WebSocket subprotocols. The server echoes only `{WEBSOCKET_CAPABILITY}`. With `after`, the server acknowledges and replays the exact durable global suffix through one snapshot boundary, then queries durable events after each wake-up. Without `after`, the server preserves live-only delivery from the connection snapshot.\n\nHTTP errors use the protocol-owned `ProblemDetails` shape. Clients make decisions from the stable `code` field. `catalogue.json` lists the error codes implemented by this release.\n"
    )
    .replace(
        "`POST /v1/runs/{run_id}/cancel` is naturally idempotent for the addressed Run. It records `run.cancellation_requested`, stops owned execution, waits for process exit, and returns the durable terminal Run. Cancelling while approval is pending rejects that Approval and denies the ToolCall without starting a process. A completion committed before the cancellation request remains authoritative.",
        "`POST /v1/runs/{run_id}/cancel` is naturally idempotent for the addressed Run and its descendants. It preserves the addressed Run's parent and siblings, records durable cancellation intent across the selected subtree before signalling owned processes, and returns only after descendant Runs and ToolCalls are terminal. Cancelling while approval is pending rejects that Approval and denies the ToolCall without starting a process. Each Run cancellation terminal commit atomically marks its queued MessageDeliveries cancelled and records `run.input_cancelled`; exact input retries return that cancelled delivery. A completion committed before the cancellation request remains authoritative.",
    )
    .replace(
        "A repeated key with the same normalized request returns the original Task. A mismatched reuse is an idempotency conflict. `GET /v1/tasks/{task_id}` returns the durable Task snapshot.",
        "`PATCH /v1/tasks/{task_id}` replaces mutable objective and dependency data and records `task.updated`. `POST /v1/tasks/{task_id}/assignment` assigns one active Run in the same Session and records `task.assigned`. `POST /v1/tasks/{task_id}/transition` applies one guarded lifecycle transition and records `task.state_changed`. All Task commands are idempotent. A repeated key with the same normalized request returns the current Task without another Event. A mismatched reuse is an idempotency conflict. `GET /v1/tasks/{task_id}` returns the durable Task snapshot.",
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
        for field in [
            "requested_scope",
            "effective_scope",
            "stdout",
            "stderr",
            "exit_code",
        ] {
            assert!(
                tool_call["required"]
                    .as_array()
                    .expect("required fields")
                    .iter()
                    .any(|required| required == field)
            );
        }

        let run = openapi_schema::<RunResponse>();
        for field in ["approval_policy", "requested_scope", "approvals"] {
            assert!(
                run["required"]
                    .as_array()
                    .expect("required fields")
                    .iter()
                    .any(|required| required == field)
            );
        }
    }
}
