//! Canonical public wire types for the first Kiln daemon protocol.

mod generated;

use std::borrow::Cow;

pub use generated::WebSocketFrame;
pub use generated::{artifact_files, check_generated_artifacts, write_generated_artifacts};
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

pub const PROTOCOL_VERSION: &str = "0.4.0";
pub const WEBSOCKET_CAPABILITY: &str = "kiln.events.websocket";
pub const DETERMINISTIC_SUBPROCESS_CAPABILITY: &str = "kiln.deterministic.subprocess";
pub const NEGOTIATE_PATH: &str = "/v1/protocol/negotiate";
pub const EVENTS_WEBSOCKET_PATH: &str = "/v1/events";
pub const WORKSPACES_PATH: &str = "/v1/workspaces";
pub const WORKSPACE_PATH: &str = "/v1/workspaces/{workspace_id}";
pub const WORKSPACE_SESSIONS_PATH: &str = "/v1/workspaces/{workspace_id}/sessions";
pub const SESSION_PATH: &str = "/v1/sessions/{session_id}";
pub const SESSION_MESSAGES_PATH: &str = "/v1/sessions/{session_id}/messages";
pub const SESSION_EVENTS_PATH: &str = "/v1/sessions/{session_id}/events";
pub const SESSION_RUNS_PATH: &str = "/v1/sessions/{session_id}/runs";
pub const RUN_PATH: &str = "/v1/runs/{run_id}";
pub const NEGOTIATE_OPERATION_ID: &str = "negotiate_protocol";
pub const EVENT_STREAM_OPERATION_ID: &str = "event_stream";
pub const CREATE_WORKSPACE_OPERATION_ID: &str = "create_workspace";
pub const GET_WORKSPACE_OPERATION_ID: &str = "get_workspace";
pub const CREATE_SESSION_OPERATION_ID: &str = "create_session";
pub const GET_SESSION_OPERATION_ID: &str = "get_session";
pub const APPEND_MESSAGE_OPERATION_ID: &str = "append_message";
pub const LIST_SESSION_EVENTS_OPERATION_ID: &str = "list_session_events";
pub const START_RUN_OPERATION_ID: &str = "start_run";
pub const GET_RUN_OPERATION_ID: &str = "get_run";

pub mod error_code {
    pub const INVALID_JSON: &str = "invalid_json";
    pub const INVALID_REQUEST: &str = "invalid_request";
    pub const INVALID_VERSION: &str = "invalid_version";
    pub const UNSUPPORTED_VERSION: &str = "unsupported_version";
    pub const MISSING_CAPABILITY: &str = "missing_capability";
    pub const MISSING_VERSION: &str = "missing_version";
    pub const WEBSOCKET_UPGRADE_REQUIRED: &str = "websocket_upgrade_required";
    pub const METHOD_NOT_ALLOWED: &str = "method_not_allowed";
    pub const NOT_FOUND: &str = "not_found";
    pub const UNSUPPORTED_INPUT: &str = "unsupported_input";
    pub const WORKSPACE_NAME_REQUIRED: &str = "workspace_name_required";
    pub const WORKSPACE_ROOT_REQUIRED: &str = "workspace_root_required";
    pub const WORKSPACE_ROOT_NAME_REQUIRED: &str = "workspace_root_name_required";
    pub const WORKSPACE_ROOT_NAME_CONFLICT: &str = "workspace_root_name_conflict";
    pub const WORKSPACE_ROOT_MISSING: &str = "workspace_root_missing";
    pub const WORKSPACE_ROOT_NOT_DIRECTORY: &str = "workspace_root_not_directory";
    pub const WORKSPACE_ROOT_NOT_GIT_REPOSITORY: &str = "workspace_root_not_git_repository";
    pub const WORKSPACE_ROOT_DUPLICATE: &str = "workspace_root_duplicate";
    pub const WORKSPACE_NOT_FOUND: &str = "workspace_not_found";
    pub const GIT_UNAVAILABLE: &str = "git_unavailable";
    pub const WORKSPACE_STORE_UNAVAILABLE: &str = "workspace_store_unavailable";
    pub const SESSION_NOT_FOUND: &str = "session_not_found";
    pub const MESSAGE_CONTENT_REQUIRED: &str = "message_content_required";
    pub const INVALID_EVENT_CURSOR: &str = "invalid_event_cursor";
    pub const SESSION_STORE_UNAVAILABLE: &str = "session_store_unavailable";
    pub const RUN_NOT_FOUND: &str = "run_not_found";
    pub const ACTIVE_ROOT_RUN_EXISTS: &str = "active_root_run_exists";
    pub const INVALID_RUN_STATE: &str = "invalid_run_state";
    pub const RUN_STORE_UNAVAILABLE: &str = "run_store_unavailable";

    pub const ALL: &[&str] = &[
        INVALID_JSON,
        INVALID_REQUEST,
        INVALID_VERSION,
        UNSUPPORTED_VERSION,
        MISSING_CAPABILITY,
        MISSING_VERSION,
        WEBSOCKET_UPGRADE_REQUIRED,
        METHOD_NOT_ALLOWED,
        NOT_FOUND,
        UNSUPPORTED_INPUT,
        WORKSPACE_NAME_REQUIRED,
        WORKSPACE_ROOT_REQUIRED,
        WORKSPACE_ROOT_NAME_REQUIRED,
        WORKSPACE_ROOT_NAME_CONFLICT,
        WORKSPACE_ROOT_MISSING,
        WORKSPACE_ROOT_NOT_DIRECTORY,
        WORKSPACE_ROOT_NOT_GIT_REPOSITORY,
        WORKSPACE_ROOT_DUPLICATE,
        WORKSPACE_NOT_FOUND,
        GIT_UNAVAILABLE,
        WORKSPACE_STORE_UNAVAILABLE,
        SESSION_NOT_FOUND,
        MESSAGE_CONTENT_REQUIRED,
        INVALID_EVENT_CURSOR,
        SESSION_STORE_UNAVAILABLE,
        RUN_NOT_FOUND,
        ACTIVE_ROOT_RUN_EXISTS,
        INVALID_RUN_STATE,
        RUN_STORE_UNAVAILABLE,
    ];
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct ClientIdentity {
    pub name: String,
    pub build: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct NegotiateRequest {
    pub min_version: String,
    pub max_version: String,
    pub client: ClientIdentity,
    pub requested_capabilities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct StoreIdentity {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct NegotiateResponse {
    pub selected_version: String,
    pub supported_capabilities: Vec<String>,
    pub selected_capabilities: Vec<String>,
    pub store_identity: StoreIdentity,
    #[schemars(with = "RequiredNullableString")]
    pub current_event_cursor: Option<String>,
    pub event_websocket_endpoint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct ProblemDetails {
    #[serde(rename = "type")]
    pub type_uri: String,
    pub title: String,
    pub status: u16,
    pub code: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct WorkspaceRootRequest {
    pub name: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct CreateWorkspaceRequest {
    pub name: String,
    pub roots: Vec<WorkspaceRootRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct WorkspaceRootResponse {
    pub workspace_root_id: String,
    pub name: String,
    pub display_path: String,
    pub canonical_path: String,
    pub git_common_directory_path: String,
    pub position: usize,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct WorkspaceResponse {
    pub workspace_id: String,
    pub name: String,
    pub roots: Vec<WorkspaceRootResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct AppendMessageRequest {
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct SessionResponse {
    pub session_id: String,
    pub workspace_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    User,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct MessageResponse {
    pub message_id: String,
    pub session_id: String,
    pub role: MessageRole,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Queued,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallState {
    Requested,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolOutputStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct ToolCallResponse {
    pub tool_call_id: String,
    pub run_id: String,
    pub capability: String,
    pub state: ToolCallState,
    #[schemars(with = "RequiredNullableString")]
    pub stdout: Option<String>,
    #[schemars(with = "RequiredNullableString")]
    pub stderr: Option<String>,
    #[schemars(with = "RequiredNullableI32")]
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct RunResponse {
    pub run_id: String,
    pub session_id: String,
    pub state: RunState,
    pub tool_calls: Vec<ToolCallResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum SessionEventDataResponse {
    #[serde(rename = "session.created")]
    SessionCreated { workspace_id: String },
    #[serde(rename = "message.appended")]
    MessageAppended { message: MessageResponse },
    #[serde(rename = "run.created")]
    RunCreated { run_id: String, state: RunState },
    #[serde(rename = "run.state_changed")]
    RunStateChanged { run_id: String, state: RunState },
    #[serde(rename = "tool_call.requested")]
    ToolCallRequested { tool_call: ToolCallResponse },
    #[serde(rename = "tool_call.state_changed")]
    ToolCallStateChanged { tool_call: ToolCallResponse },
    #[serde(rename = "tool_call.output")]
    ToolCallOutput {
        run_id: String,
        tool_call_id: String,
        stream: ToolOutputStream,
        content: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct SessionEventResponse {
    pub event_id: String,
    pub cursor: String,
    pub session_id: String,
    pub event: SessionEventDataResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct SessionEventsResponse {
    pub events: Vec<SessionEventResponse>,
    pub current_event_cursor: String,
}

struct RequiredNullableString;

impl JsonSchema for RequiredNullableString {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "RequiredNullableString".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({"type": ["string", "null"]})
    }
}

struct RequiredNullableI32;

impl JsonSchema for RequiredNullableI32 {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "RequiredNullableI32".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({"type": ["integer", "null"], "format": "int32"})
    }
}
