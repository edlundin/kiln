//! Canonical public wire types for the first Kiln daemon protocol.

mod generated;

use std::borrow::Cow;

pub use generated::WebSocketFrame;
pub use generated::{artifact_files, check_generated_artifacts, write_generated_artifacts};
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

pub const PROTOCOL_VERSION: &str = "0.19.0";
pub const WEBSOCKET_CAPABILITY: &str = "kiln.events.websocket";
pub const DETERMINISTIC_SUBPROCESS_CAPABILITY: &str = "kiln.deterministic.subprocess";
pub const IDEMPOTENCY_KEY_HEADER: &str = "Idempotency-Key";
pub const NEGOTIATE_PATH: &str = "/v1/protocol/negotiate";
pub const EVENTS_WEBSOCKET_PATH: &str = "/v1/events";
pub const WORKSPACES_PATH: &str = "/v1/workspaces";
pub const WORKSPACE_PATH: &str = "/v1/workspaces/{workspace_id}";
pub const WORKSPACE_SESSIONS_PATH: &str = "/v1/workspaces/{workspace_id}/sessions";
pub const SESSION_PATH: &str = "/v1/sessions/{session_id}";
pub const SESSION_MESSAGES_PATH: &str = "/v1/sessions/{session_id}/messages";
pub const SESSION_TASKS_PATH: &str = "/v1/sessions/{session_id}/tasks";
pub const SESSION_EVENTS_PATH: &str = "/v1/sessions/{session_id}/events";
pub const SESSION_RUNS_PATH: &str = "/v1/sessions/{session_id}/runs";
pub const RUN_CHILDREN_PATH: &str = "/v1/runs/{parent_run_id}/children";
pub const RUN_PATH: &str = "/v1/runs/{run_id}";
pub const RUN_INPUT_PATH: &str = "/v1/runs/{run_id}/input";
pub const RUN_REACTIONS_PATH: &str = "/v1/runs/{run_id}/reactions";
pub const TASK_PATH: &str = "/v1/tasks/{task_id}";
pub const TASK_ASSIGNMENT_PATH: &str = "/v1/tasks/{task_id}/assignment";
pub const TASK_TRANSITION_PATH: &str = "/v1/tasks/{task_id}/transition";
pub const RUN_CANCEL_PATH: &str = "/v1/runs/{run_id}/cancel";
pub const TOOL_CALL_APPROVAL_PATH: &str = "/v1/tool-calls/{tool_call_id}/approval";
pub const ARTIFACT_PATH: &str = "/v1/artifacts/{content_hash}";
pub const NEGOTIATE_OPERATION_ID: &str = "negotiate_protocol";
pub const EVENT_STREAM_OPERATION_ID: &str = "event_stream";
pub const CREATE_WORKSPACE_OPERATION_ID: &str = "create_workspace";
pub const GET_WORKSPACE_OPERATION_ID: &str = "get_workspace";
pub const CREATE_SESSION_OPERATION_ID: &str = "create_session";
pub const GET_SESSION_OPERATION_ID: &str = "get_session";
pub const APPEND_MESSAGE_OPERATION_ID: &str = "append_message";
pub const CREATE_TASK_OPERATION_ID: &str = "create_task";
pub const GET_TASK_OPERATION_ID: &str = "get_task";
pub const UPDATE_TASK_OPERATION_ID: &str = "update_task";
pub const ASSIGN_TASK_OPERATION_ID: &str = "assign_task";
pub const TRANSITION_TASK_OPERATION_ID: &str = "transition_task";
pub const LIST_SESSION_EVENTS_OPERATION_ID: &str = "list_session_events";
pub const START_RUN_OPERATION_ID: &str = "start_run";
pub const START_CHILD_RUN_OPERATION_ID: &str = "start_child_run";
pub const LIST_SESSION_RUNS_OPERATION_ID: &str = "list_session_runs";
pub const GET_RUN_OPERATION_ID: &str = "get_run";
pub const SEND_RUN_INPUT_OPERATION_ID: &str = "send_run_input";
pub const REACT_TO_RUN_ACTIVITY_OPERATION_ID: &str = "react_to_run_activity";
pub const CANCEL_RUN_OPERATION_ID: &str = "cancel_run";
pub const DECIDE_APPROVAL_OPERATION_ID: &str = "decide_approval";
pub const GET_ARTIFACT_OPERATION_ID: &str = "get_artifact";

pub mod error_code {
    pub const AUTHENTICATION_REQUIRED: &str = "authentication_required";
    pub const INVALID_AUTHENTICATION: &str = "invalid_authentication";
    pub const INVALID_HOST: &str = "invalid_host";
    pub const INVALID_ORIGIN: &str = "invalid_origin";
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
    pub const TASK_NOT_FOUND: &str = "task_not_found";
    pub const TASK_OBJECTIVE_REQUIRED: &str = "task_objective_required";
    pub const PARENT_TASK_NOT_FOUND: &str = "parent_task_not_found";
    pub const DEPENDENCY_TASK_NOT_FOUND: &str = "dependency_task_not_found";
    pub const TASK_LINK_OUTSIDE_SESSION: &str = "task_link_outside_session";
    pub const DUPLICATE_TASK_DEPENDENCY: &str = "duplicate_task_dependency";
    pub const TASK_CYCLE: &str = "task_cycle";
    pub const INVALID_TASK_TRANSITION: &str = "invalid_task_transition";
    pub const INVALID_TASK_ASSIGNMENT: &str = "invalid_task_assignment";
    pub const TASK_STORE_UNAVAILABLE: &str = "task_store_unavailable";
    pub const INVALID_EVENT_CURSOR: &str = "invalid_event_cursor";
    pub const IDEMPOTENCY_KEY_REQUIRED: &str = "idempotency_key_required";
    pub const INVALID_IDEMPOTENCY_KEY: &str = "invalid_idempotency_key";
    pub const SESSION_STORE_UNAVAILABLE: &str = "session_store_unavailable";
    pub const RUN_NOT_FOUND: &str = "run_not_found";
    pub const PARENT_RUN_NOT_FOUND: &str = "parent_run_not_found";
    pub const PARENT_RUN_TERMINAL: &str = "parent_run_terminal";
    pub const ACTIVE_ROOT_RUN_EXISTS: &str = "active_root_run_exists";
    pub const INVALID_RUN_STATE: &str = "invalid_run_state";
    pub const RUN_INPUT_READ_ONLY: &str = "run_input_read_only";
    pub const RUN_NOT_ACCEPTING_INPUT: &str = "run_not_accepting_input";
    pub const INVALID_CHILD_ACTIVITY: &str = "invalid_child_activity";
    pub const MESSAGE_DELIVERY_NOT_FOUND: &str = "message_delivery_not_found";
    pub const INVALID_MESSAGE_DELIVERY: &str = "invalid_message_delivery";
    pub const MESSAGE_DELIVERY_OUT_OF_ORDER: &str = "message_delivery_out_of_order";
    pub const RUN_STORE_UNAVAILABLE: &str = "run_store_unavailable";
    pub const RUN_CANCELLATION_FAILED: &str = "run_cancellation_failed";
    pub const DAEMON_SHUTTING_DOWN: &str = "daemon_shutting_down";
    pub const WORKSPACE_ROOT_NOT_FOUND: &str = "workspace_root_not_found";
    pub const PATH_OUTSIDE_WORKSPACE_ROOT: &str = "path_outside_workspace_root";
    pub const APPROVAL_NOT_FOUND: &str = "approval_not_found";
    pub const APPROVAL_ALREADY_DECIDED: &str = "approval_already_decided";
    pub const IDEMPOTENCY_CONFLICT: &str = "idempotency_conflict";
    pub const INVALID_CONTENT_HASH: &str = "invalid_content_hash";
    pub const ARTIFACT_NOT_FOUND: &str = "artifact_not_found";
    pub const ARTIFACT_STORE_UNAVAILABLE: &str = "artifact_store_unavailable";

    pub const ALL: &[&str] = &[
        AUTHENTICATION_REQUIRED,
        INVALID_AUTHENTICATION,
        INVALID_HOST,
        INVALID_ORIGIN,
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
        TASK_NOT_FOUND,
        TASK_OBJECTIVE_REQUIRED,
        PARENT_TASK_NOT_FOUND,
        DEPENDENCY_TASK_NOT_FOUND,
        TASK_LINK_OUTSIDE_SESSION,
        DUPLICATE_TASK_DEPENDENCY,
        TASK_CYCLE,
        INVALID_TASK_TRANSITION,
        INVALID_TASK_ASSIGNMENT,
        TASK_STORE_UNAVAILABLE,
        INVALID_EVENT_CURSOR,
        IDEMPOTENCY_KEY_REQUIRED,
        INVALID_IDEMPOTENCY_KEY,
        SESSION_STORE_UNAVAILABLE,
        RUN_NOT_FOUND,
        PARENT_RUN_NOT_FOUND,
        PARENT_RUN_TERMINAL,
        ACTIVE_ROOT_RUN_EXISTS,
        INVALID_RUN_STATE,
        RUN_INPUT_READ_ONLY,
        RUN_NOT_ACCEPTING_INPUT,
        INVALID_CHILD_ACTIVITY,
        MESSAGE_DELIVERY_NOT_FOUND,
        INVALID_MESSAGE_DELIVERY,
        MESSAGE_DELIVERY_OUT_OF_ORDER,
        RUN_STORE_UNAVAILABLE,
        RUN_CANCELLATION_FAILED,
        DAEMON_SHUTTING_DOWN,
        WORKSPACE_ROOT_NOT_FOUND,
        PATH_OUTSIDE_WORKSPACE_ROOT,
        APPROVAL_NOT_FOUND,
        APPROVAL_ALREADY_DECIDED,
        IDEMPOTENCY_CONFLICT,
        INVALID_CONTENT_HASH,
        ARTIFACT_NOT_FOUND,
        ARTIFACT_STORE_UNAVAILABLE,
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
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct CreateTaskRequest {
    pub objective: String,
    #[serde(default)]
    pub parent_task_id: Option<String>,
    #[serde(default)]
    pub dependency_task_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct UpdateTaskRequest {
    pub objective: String,
    #[serde(default)]
    pub dependency_task_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct TransitionTaskRequest {
    pub state: TaskState,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct AssignTaskRequest {
    pub run_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct StartRunRequest {
    pub approval_policy: ApprovalPolicy,
    pub workspace_root_id: String,
    pub relative_directory: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct StartChildRunRequest {
    pub approval_policy: ApprovalPolicy,
    pub workspace_root_id: String,
    pub relative_directory: String,
    pub user_input_mode: RunInputMode,
    #[schemars(with = "RequiredNullableString")]
    #[serde(deserialize_with = "deserialize_required_nullable_string")]
    pub task_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct SendRunInputRequest {
    pub content: String,
    pub delivery_mode: MessageDeliveryMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct ChildActivityReference {
    pub run_id: String,
    pub event_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct ReactToRunActivityRequest {
    pub content: String,
    pub child_activity: ChildActivityReference,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalPolicy {
    Ask,
    ReadOnly,
    FullAccess,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    Approved,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct ApprovalDecisionRequest {
    pub decision: ApprovalDecision,
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
    Assistant,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageStatus {
    Complete,
    Incomplete,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct MessageResponse {
    pub message_id: String,
    pub session_id: String,
    pub role: MessageRole,
    pub content: String,
    pub status: MessageStatus,
    #[schemars(with = "RequiredNullableString")]
    pub origin_run_id: Option<String>,
    #[schemars(with = "RequiredNullableString")]
    pub model_invocation_id: Option<String>,
    #[schemars(with = "RequiredNullableString")]
    pub target_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_activity: Option<ChildActivityReference>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageDeliveryMode {
    Queued,
    Interrupt,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageDeliveryState {
    Queued,
    Delivered,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct MessageDeliveryResponse {
    pub message: MessageResponse,
    pub delivery_mode: MessageDeliveryMode,
    pub state: MessageDeliveryState,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Pending,
    Ready,
    Running,
    Blocked,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct TaskResponse {
    pub task_id: String,
    pub session_id: String,
    pub objective: String,
    pub state: TaskState,
    #[schemars(with = "RequiredNullableString")]
    pub parent_task_id: Option<String>,
    pub dependency_task_ids: Vec<String>,
    #[schemars(with = "RequiredNullableString")]
    pub assigned_run_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Queued,
    Running,
    WaitingForApproval,
    Cancelling,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunInputMode {
    Interactive,
    ReadOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallState {
    Requested,
    AwaitingApproval,
    Ready,
    Running,
    Completed,
    Failed,
    Cancelled,
    Denied,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolOutputStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct ArtifactResponse {
    pub content_hash: String,
    pub media_type: String,
    pub size: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct ToolCallResponse {
    pub tool_call_id: String,
    pub run_id: String,
    pub capability: String,
    pub state: ToolCallState,
    #[schemars(with = "RequiredNullableScope")]
    pub requested_scope: Option<WorkspaceScopeResponse>,
    #[schemars(with = "RequiredNullableScope")]
    pub effective_scope: Option<WorkspaceScopeResponse>,
    #[schemars(with = "RequiredNullableString")]
    pub stdout: Option<String>,
    #[schemars(with = "RequiredNullableString")]
    pub stderr: Option<String>,
    #[schemars(with = "RequiredNullableArtifact")]
    pub stdout_artifact: Option<ArtifactResponse>,
    #[schemars(with = "RequiredNullableArtifact")]
    pub stderr_artifact: Option<ArtifactResponse>,
    #[schemars(with = "RequiredNullableI32")]
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct RunResponse {
    pub run_id: String,
    pub session_id: String,
    #[schemars(with = "RequiredNullableString")]
    pub parent_run_id: Option<String>,
    #[schemars(with = "RequiredNullableString")]
    pub task_id: Option<String>,
    pub user_input_mode: RunInputMode,
    pub state: RunState,
    #[schemars(with = "RequiredNullableApprovalPolicy")]
    pub approval_policy: Option<ApprovalPolicy>,
    #[schemars(with = "RequiredNullableScope")]
    pub requested_scope: Option<WorkspaceScopeResponse>,
    pub tool_calls: Vec<ToolCallResponse>,
    pub approvals: Vec<ApprovalResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct SessionRunsResponse {
    pub runs: Vec<RunResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct WorkspaceScopeResponse {
    pub workspace_root_id: String,
    pub relative_directory: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct ApprovalResponse {
    pub approval_id: String,
    pub run_id: String,
    pub tool_call_id: String,
    pub requested_scope: WorkspaceScopeResponse,
    pub state: ApprovalState,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalState {
    Pending,
    Approved,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ContextManifestCreatedResponse {
    pub context_manifest_id: String,
    pub run_id: String,
    pub content_hash: String,
    pub entry_count: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelInvocationPurpose {
    Generation,
    Compaction,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelInvocationCompletionKind {
    AssistantOutput,
    ToolRequests,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelInvocationFailureReason {
    ProviderError,
    InvalidRequest,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelInvocationStatus {
    Pending,
    InFlight,
    Completed {
        completion_kind: ModelInvocationCompletionKind,
    },
    Failed {
        reason: ModelInvocationFailureReason,
    },
    Cancelled,
    Interrupted,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelInvocationEventResponse {
    pub model_invocation_id: String,
    pub work_id: String,
    pub run_id: String,
    pub context_manifest_id: String,
    pub context_manifest_hash: String,
    pub provider_account_id: String,
    pub provider: String,
    pub model: String,
    pub purpose: ModelInvocationPurpose,
    #[schemars(with = "RequiredNullableString")]
    pub retry_of: Option<String>,
    pub status: ModelInvocationStatus,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UsageCompleteness {
    Complete,
    Partial,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UsageObservedResponse {
    pub usage_observation_id: String,
    pub model_invocation_id: String,
    pub work_id: String,
    pub run_id: String,
    pub provider_account_id: String,
    pub revision: u64,
    #[schemars(with = "RequiredNullableString")]
    pub supersedes_usage_observation_id: Option<String>,
    pub completeness: UsageCompleteness,
    pub is_terminal: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelOutputStream {
    AssistantText,
    ReasoningSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelOutputRecordedResponse {
    pub output_chunk_id: String,
    pub model_invocation_id: String,
    pub run_id: String,
    pub position: u64,
    pub stream: ModelOutputStream,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum SessionEventDataResponse {
    #[serde(rename = "model_invocation.output")]
    ModelOutputRecorded(ModelOutputRecordedResponse),
    #[serde(rename = "usage.observed")]
    UsageObserved(UsageObservedResponse),
    #[serde(rename = "context.manifest_created")]
    ContextManifestCreated(ContextManifestCreatedResponse),
    #[serde(rename = "model_invocation.created")]
    ModelInvocationCreated(ModelInvocationEventResponse),
    #[serde(rename = "model_invocation.state_changed")]
    ModelInvocationStateChanged(ModelInvocationEventResponse),
    #[serde(rename = "session.created")]
    SessionCreated { workspace_id: String },
    #[serde(rename = "message.appended")]
    MessageAppended { message: MessageResponse },
    #[serde(rename = "task.created")]
    TaskCreated { task: TaskResponse },
    #[serde(rename = "task.updated")]
    TaskUpdated { task: TaskResponse },
    #[serde(rename = "task.assigned")]
    TaskAssigned { task: TaskResponse },
    #[serde(rename = "task.state_changed")]
    TaskStateChanged { task: TaskResponse },
    #[serde(rename = "run.created")]
    RunCreated {
        run_id: String,
        state: RunState,
        #[schemars(with = "RequiredNullableString")]
        parent_run_id: Option<String>,
        #[schemars(with = "RequiredNullableString")]
        task_id: Option<String>,
        user_input_mode: RunInputMode,
        #[schemars(with = "RequiredNullableApprovalPolicy")]
        approval_policy: Option<ApprovalPolicy>,
        #[schemars(with = "RequiredNullableScope")]
        requested_scope: Option<WorkspaceScopeResponse>,
    },
    #[serde(rename = "run.queued")]
    RunQueued { run_id: String },
    #[serde(rename = "run.child_added")]
    RunChildAdded {
        parent_run_id: String,
        child_run_id: String,
    },
    #[serde(rename = "run.input_queued")]
    RunInputQueued { run_id: String, message_id: String },
    #[serde(rename = "run.interrupt_requested")]
    RunInterruptRequested { run_id: String, message_id: String },
    #[serde(rename = "run.input_delivered")]
    RunInputDelivered { run_id: String, message_id: String },
    #[serde(rename = "run.input_failed")]
    RunInputFailed { run_id: String, message_id: String },
    #[serde(rename = "run.input_cancelled")]
    RunInputCancelled { run_id: String, message_id: String },
    #[serde(rename = "run.state_changed")]
    RunStateChanged { run_id: String, state: RunState },
    #[serde(rename = "run.cancellation_requested")]
    RunCancellationRequested { run_id: String },
    #[serde(rename = "tool_call.requested")]
    ToolCallRequested { tool_call: ToolCallResponse },
    #[serde(rename = "approval.requested")]
    ApprovalRequested { approval: ApprovalResponse },
    #[serde(rename = "approval.decided")]
    ApprovalDecided { approval: ApprovalResponse },
    #[serde(rename = "tool_call.denied")]
    ToolCallDenied { tool_call: ToolCallResponse },
    #[serde(rename = "tool_call.state_changed")]
    ToolCallStateChanged { tool_call: ToolCallResponse },
    #[serde(rename = "tool_call.output")]
    ToolCallOutput {
        run_id: String,
        tool_call_id: String,
        stream: ToolOutputStream,
        content: String,
    },
    #[serde(rename = "artifact.registered")]
    ArtifactRegistered {
        run_id: String,
        tool_call_id: String,
        stream: ToolOutputStream,
        artifact: ArtifactResponse,
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

fn deserialize_required_nullable_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::deserialize(deserializer)
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

struct RequiredNullableScope;

impl JsonSchema for RequiredNullableScope {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "RequiredNullableScope".into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let scope = generator.subschema_for::<WorkspaceScopeResponse>();
        json_schema!({"anyOf": [scope, {"type": "null"}]})
    }
}

struct RequiredNullableApprovalPolicy;

impl JsonSchema for RequiredNullableApprovalPolicy {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "RequiredNullableApprovalPolicy".into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let approval_policy = generator.subschema_for::<ApprovalPolicy>();
        json_schema!({"anyOf": [approval_policy, {"type": "null"}]})
    }
}

struct RequiredNullableArtifact;

impl JsonSchema for RequiredNullableArtifact {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "RequiredNullableArtifact".into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let artifact = generator.subschema_for::<ArtifactResponse>();
        json_schema!({"anyOf": [artifact, {"type": "null"}]})
    }
}
