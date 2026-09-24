//! Canonical public wire types for the first Kiln daemon protocol.

mod generated;

use std::borrow::Cow;

pub use generated::WebSocketFrame;
pub use generated::{artifact_files, check_generated_artifacts, write_generated_artifacts};
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

pub const PROTOCOL_VERSION: &str = "0.35.0";
pub const WEBSOCKET_CAPABILITY: &str = "kiln.events.websocket";
pub const DETERMINISTIC_SUBPROCESS_CAPABILITY: &str = "kiln.deterministic.subprocess";
pub const IDEMPOTENCY_KEY_HEADER: &str = "Idempotency-Key";
pub const ARTIFACT_SESSION_HEADER: &str = "X-Kiln-Artifact-Session";
pub const MAX_ARTIFACT_UPLOAD_BYTES: usize = 64 * 1024 * 1024;
pub const NEGOTIATE_PATH: &str = "/v1/protocol/negotiate";
pub const EVENTS_WEBSOCKET_PATH: &str = "/v1/events";
pub const WORKSPACES_PATH: &str = "/v1/workspaces";
pub const WORKSPACE_PATH: &str = "/v1/workspaces/{workspace_id}";
pub const CONFIGURATION_SYNC_STATUS_PATH: &str = "/v1/configuration-sync";
pub const CONFIGURATION_MASTER_PATH: &str = "/v1/configuration-sync/master";
pub const CONFIGURATION_IDENTITY_STATUS_PATH: &str = "/v1/configuration-sync/identity";
pub const CONFIGURATION_IDENTITY_RETIRE_PATH: &str = "/v1/configuration-sync/identity/retire";
pub const CONFIGURATION_IDENTITY_RETIRE_BY_ID_PATH: &str =
    "/v1/configuration-sync/identity/retire/by-id";
pub const CONFIGURATION_READ_GRANTS_PATH: &str = "/v1/configuration-sync/grants";
pub const CONFIGURATION_READ_GRANT_PATH: &str = "/v1/configuration-sync/grants/{grant_id}";
pub const CONFIGURATION_READ_GRANT_BY_ATTEMPT_PATH: &str =
    "/v1/configuration-sync/grants/attempts/{attempt_id}";
pub const CONFIGURATION_READ_GRANT_REVOKE_PATH: &str =
    "/v1/configuration-sync/grants/{grant_id}/revoke";
pub const CONFIGURATION_FOLLOWER_ENROLLMENTS_PATH: &str =
    "/v1/configuration-sync/follower-enrollments";
pub const CONFIGURATION_FOLLOWER_ENROLLMENT_PATH: &str =
    "/v1/configuration-sync/follower-enrollments/{attempt_id}";
pub const CONFIGURATION_FOLLOWER_ENROLLMENT_RETIRE_PATH: &str =
    "/v1/configuration-sync/follower-enrollments/{attempt_id}/retire";
pub const CONFIGURATION_FOLLOWER_ENROLLMENT_REQUESTS_PATH: &str =
    "/v1/configuration-sync/follower-enrollment-requests";
pub const CONFIGURATION_FOLLOWER_ENROLLMENT_REQUEST_PATH: &str =
    "/v1/configuration-sync/follower-enrollment-requests/{request_id}";
pub const CONFIGURATION_FOLLOWER_ENROLLMENT_REQUEST_APPROVE_PATH: &str =
    "/v1/configuration-sync/follower-enrollment-requests/{request_id}/approve";
pub const CONFIGURATION_FOLLOWER_ENROLLMENT_REQUEST_REJECT_PATH: &str =
    "/v1/configuration-sync/follower-enrollment-requests/{request_id}/reject";
pub const CONFIGURE_MASTER_IDENTITY_OPERATION_ID: &str = "configure_master_identity";
pub const RETIRE_MASTER_IDENTITY_OPERATION_ID: &str = "retire_master_identity";
pub const RETIRE_MASTER_IDENTITY_BY_ID_OPERATION_ID: &str = "retire_master_identity_by_id";
pub const GET_CONFIGURATION_IDENTITY_STATUS_OPERATION_ID: &str =
    "get_configuration_identity_status";
pub const LIST_CONFIGURATION_READ_GRANTS_OPERATION_ID: &str = "list_configuration_read_grants";
pub const GET_CONFIGURATION_READ_GRANT_OPERATION_ID: &str = "get_configuration_read_grant";
pub const GET_CONFIGURATION_READ_GRANT_BY_ATTEMPT_OPERATION_ID: &str =
    "get_configuration_read_grant_by_attempt";
pub const REVOKE_CONFIGURATION_READ_GRANT_OPERATION_ID: &str = "revoke_configuration_read_grant";
pub const LIST_CONFIGURATION_FOLLOWER_ENROLLMENTS_OPERATION_ID: &str =
    "list_configuration_follower_enrollments";
pub const PREPARE_CONFIGURATION_FOLLOWER_ENROLLMENT_OPERATION_ID: &str =
    "prepare_configuration_follower_enrollment";
pub const GET_CONFIGURATION_FOLLOWER_ENROLLMENT_OPERATION_ID: &str =
    "get_configuration_follower_enrollment";
pub const RETIRE_CONFIGURATION_FOLLOWER_ENROLLMENT_OPERATION_ID: &str =
    "retire_configuration_follower_enrollment";
pub const LIST_CONFIGURATION_FOLLOWER_ENROLLMENT_REQUESTS_OPERATION_ID: &str =
    "list_configuration_follower_enrollment_requests";
pub const GET_CONFIGURATION_FOLLOWER_ENROLLMENT_REQUEST_OPERATION_ID: &str =
    "get_configuration_follower_enrollment_request";
pub const APPROVE_CONFIGURATION_FOLLOWER_ENROLLMENT_REQUEST_OPERATION_ID: &str =
    "approve_configuration_follower_enrollment_request";
pub const REJECT_CONFIGURATION_FOLLOWER_ENROLLMENT_REQUEST_OPERATION_ID: &str =
    "reject_configuration_follower_enrollment_request";
pub const CONFIGURATION_PUBLICATIONS_PATH: &str = "/v1/configuration-sync/publications";
pub const CONFIGURATION_SNAPSHOT_PATH: &str = "/v1/configuration-sync/snapshot";
pub const GET_CONFIGURATION_SNAPSHOT_OPERATION_ID: &str = "get_configuration_snapshot";
pub const PUBLISH_CONFIGURATION_OPERATION_ID: &str = "publish_configuration_snapshot";
/// Keep explicit snapshot imports within the existing Axum JSON request ceiling.
/// This bounds the whole serialized request, including metadata and byte arrays.
pub const CONFIGURATION_PUBLICATION_MAX_BYTES: usize = 2 * 1024 * 1024;
/// Bounds follower-enrollment JSON bodies and their submitted CA DER field.
pub const CONFIGURATION_FOLLOWER_ENROLLMENT_MAX_BYTES: usize = CONFIGURATION_PUBLICATION_MAX_BYTES;
pub const CONFIGURATION_FOLLOWER_ENROLLMENT_REQUEST_DEFAULT_PAGE_SIZE: usize = 50;
pub const CONFIGURATION_FOLLOWER_ENROLLMENT_REQUEST_MAX_PAGE_SIZE: usize = 100;
pub const DESIGNATE_CONFIGURATION_MASTER_OPERATION_ID: &str = "designate_configuration_master";
pub const GET_CONFIGURATION_SYNC_STATUS_OPERATION_ID: &str = "get_configuration_sync_status";
pub const PROVIDER_ACCOUNTS_PATH: &str = "/v1/provider-accounts";
pub const PROVIDER_ACCOUNT_PATH: &str = "/v1/provider-accounts/{provider_account_id}";
pub const PROVIDER_ACCOUNT_LOGIN_PATH: &str = "/v1/provider-accounts/{provider_account_id}/login";
pub const PROVIDER_ACCOUNT_BROWSER_LOGIN_PATH: &str =
    "/v1/provider-accounts/{provider_account_id}/login/browser";
pub const START_PROVIDER_ACCOUNT_BROWSER_LOGIN_OPERATION_ID: &str =
    "start_provider_account_browser_login";
pub const PROVIDER_ACCOUNT_LOGIN_ATTEMPT_PATH: &str =
    "/v1/provider-accounts/{provider_account_id}/login/{attempt_id}";
pub const WORKSPACE_SESSIONS_PATH: &str = "/v1/workspaces/{workspace_id}/sessions";
pub const SESSION_PATH: &str = "/v1/sessions/{session_id}";
pub const SESSION_MESSAGES_PATH: &str = "/v1/sessions/{session_id}/messages";
pub const SESSION_TASKS_PATH: &str = "/v1/sessions/{session_id}/tasks";
pub const SESSION_EVENTS_PATH: &str = "/v1/sessions/{session_id}/events";
pub const SESSION_CHANGES_PATH: &str = "/v1/sessions/{session_id}/changes";
pub const SESSION_CHANGE_DIFF_PATH: &str = "/v1/sessions/{session_id}/change-diff";
pub const USAGE_PATH: &str = "/v1/usage";
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
pub const ARTIFACTS_PATH: &str = "/v1/artifacts";
pub const NEGOTIATE_OPERATION_ID: &str = "negotiate_protocol";
pub const EVENT_STREAM_OPERATION_ID: &str = "event_stream";
pub const CREATE_WORKSPACE_OPERATION_ID: &str = "create_workspace";
pub const LIST_WORKSPACES_OPERATION_ID: &str = "list_workspaces";
pub const GET_WORKSPACE_OPERATION_ID: &str = "get_workspace";
pub const CREATE_PROVIDER_ACCOUNT_OPERATION_ID: &str = "create_provider_account";
pub const LIST_PROVIDER_ACCOUNTS_OPERATION_ID: &str = "list_provider_accounts";
pub const GET_PROVIDER_ACCOUNT_OPERATION_ID: &str = "get_provider_account";
pub const DISCONNECT_PROVIDER_ACCOUNT_OPERATION_ID: &str = "disconnect_provider_account";
pub const PROVIDER_ACCOUNT_DISCONNECT_PATH: &str =
    "/v1/provider-accounts/{provider_account_id}/disconnect";
pub const START_PROVIDER_ACCOUNT_LOGIN_OPERATION_ID: &str = "start_provider_account_login";
pub const GET_PROVIDER_ACCOUNT_LOGIN_OPERATION_ID: &str = "get_provider_account_login";
pub const CANCEL_PROVIDER_ACCOUNT_LOGIN_OPERATION_ID: &str = "cancel_provider_account_login";
pub const CREATE_SESSION_OPERATION_ID: &str = "create_session";
pub const LIST_SESSIONS_OPERATION_ID: &str = "list_sessions";
pub const GET_SESSION_OPERATION_ID: &str = "get_session";
pub const APPEND_MESSAGE_OPERATION_ID: &str = "append_message";
pub const CREATE_TASK_OPERATION_ID: &str = "create_task";
pub const GET_TASK_OPERATION_ID: &str = "get_task";
pub const UPDATE_TASK_OPERATION_ID: &str = "update_task";
pub const ASSIGN_TASK_OPERATION_ID: &str = "assign_task";
pub const TRANSITION_TASK_OPERATION_ID: &str = "transition_task";
pub const LIST_SESSION_EVENTS_OPERATION_ID: &str = "list_session_events";
pub const LIST_SESSION_CHANGES_OPERATION_ID: &str = "list_session_changes";
pub const GET_SESSION_CHANGE_DIFF_OPERATION_ID: &str = "get_session_change_diff";
pub const LIST_USAGE_OPERATION_ID: &str = "list_usage";
pub const START_RUN_OPERATION_ID: &str = "start_run";
pub const START_CHILD_RUN_OPERATION_ID: &str = "start_child_run";
pub const LIST_SESSION_RUNS_OPERATION_ID: &str = "list_session_runs";
pub const GET_RUN_OPERATION_ID: &str = "get_run";
pub const SEND_RUN_INPUT_OPERATION_ID: &str = "send_run_input";
pub const REACT_TO_RUN_ACTIVITY_OPERATION_ID: &str = "react_to_run_activity";
pub const CANCEL_RUN_OPERATION_ID: &str = "cancel_run";
pub const DECIDE_APPROVAL_OPERATION_ID: &str = "decide_approval";
pub const GET_ARTIFACT_OPERATION_ID: &str = "get_artifact";
pub const UPLOAD_ARTIFACT_OPERATION_ID: &str = "upload_artifact";

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
    pub const INVALID_USAGE_CURSOR: &str = "invalid_usage_cursor";
    pub const INVALID_USAGE_LIMIT: &str = "invalid_usage_limit";
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
    pub const USAGE_STORE_UNAVAILABLE: &str = "usage_store_unavailable";
    pub const USAGE_INTEGRITY_VIOLATION: &str = "usage_integrity_violation";
    pub const CHANGE_NOT_FOUND: &str = "change_not_found";
    pub const PROVIDER_ACCOUNT_NOT_FOUND: &str = "provider_account_not_found";
    pub const PROVIDER_ACCOUNT_INVALID: &str = "provider_account_invalid";
    pub const PROVIDER_ACCOUNT_LIMIT_REACHED: &str = "provider_account_limit_reached";
    pub const PROVIDER_ACCOUNT_WORKSPACE_ASSOCIATION_INVALID: &str =
        "provider_account_workspace_association_invalid";
    pub const CONFIGURATION_SYNC_UNAVAILABLE: &str = "configuration_sync_unavailable";
    pub const CONFIGURATION_SYNC_INVALID_REQUEST: &str = "configuration_sync_invalid_request";
    pub const CONFIGURATION_SYNC_CONFLICT: &str = "configuration_sync_conflict";
    pub const CONFIGURATION_READ_GRANT_NOT_FOUND: &str = "configuration_read_grant_not_found";
    pub const CONFIGURATION_FOLLOWER_ENROLLMENT_REQUEST_NOT_FOUND: &str =
        "configuration_follower_enrollment_request_not_found";
    pub const CONFIGURATION_IDENTITY_RECOVERY_REQUIRED: &str =
        "configuration_identity_recovery_required";
    pub const CONFIGURATION_FOLLOWER_ENROLLMENT_NOT_FOUND: &str =
        "configuration_follower_enrollment_not_found";
    pub const CONFIGURATION_FOLLOWER_ENROLLMENT_RETIRED: &str =
        "configuration_follower_enrollment_retired";
    pub const CONFIGURATION_FOLLOWER_ENROLLMENT_RECOVERY_REQUIRED: &str =
        "configuration_follower_enrollment_recovery_required";
    pub const CONFIGURATION_SNAPSHOT_NOT_FOUND: &str = "configuration_snapshot_not_found";
    pub const CONFIGURATION_SNAPSHOT_TOO_LARGE: &str = "configuration_snapshot_too_large";
    pub const PROVIDER_ACCOUNT_STORE_UNAVAILABLE: &str = "provider_account_store_unavailable";
    pub const PROVIDER_ACCOUNT_INVALID_STATE: &str = "provider_account_invalid_state";
    pub const PROVIDER_ACCOUNT_LOGIN_NOT_FOUND: &str = "provider_account_login_not_found";
    pub const PROVIDER_ACCOUNT_LOGIN_UNAVAILABLE: &str = "provider_account_login_unavailable";
    pub const PROVIDER_ACCOUNT_LOGIN_FAILED: &str = "provider_account_login_failed";
    pub const PROVIDER_ACCOUNT_CLEANUP_REQUIRED: &str = "provider_account_cleanup_required";

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
        INVALID_USAGE_CURSOR,
        INVALID_USAGE_LIMIT,
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
        USAGE_STORE_UNAVAILABLE,
        USAGE_INTEGRITY_VIOLATION,
        CHANGE_NOT_FOUND,
        PROVIDER_ACCOUNT_NOT_FOUND,
        PROVIDER_ACCOUNT_INVALID,
        PROVIDER_ACCOUNT_LIMIT_REACHED,
        PROVIDER_ACCOUNT_WORKSPACE_ASSOCIATION_INVALID,
        CONFIGURATION_SYNC_UNAVAILABLE,
        CONFIGURATION_SYNC_INVALID_REQUEST,
        CONFIGURATION_SYNC_CONFLICT,
        CONFIGURATION_READ_GRANT_NOT_FOUND,
        CONFIGURATION_FOLLOWER_ENROLLMENT_REQUEST_NOT_FOUND,
        CONFIGURATION_IDENTITY_RECOVERY_REQUIRED,
        CONFIGURATION_FOLLOWER_ENROLLMENT_NOT_FOUND,
        CONFIGURATION_FOLLOWER_ENROLLMENT_RETIRED,
        CONFIGURATION_FOLLOWER_ENROLLMENT_RECOVERY_REQUIRED,
        CONFIGURATION_SNAPSHOT_NOT_FOUND,
        CONFIGURATION_SNAPSHOT_TOO_LARGE,
        PROVIDER_ACCOUNT_STORE_UNAVAILABLE,
        PROVIDER_ACCOUNT_INVALID_STATE,
        PROVIDER_ACCOUNT_LOGIN_NOT_FOUND,
        PROVIDER_ACCOUNT_LOGIN_UNAVAILABLE,
        PROVIDER_ACCOUNT_LOGIN_FAILED,
        PROVIDER_ACCOUNT_CLEANUP_REQUIRED,
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
#[serde(rename_all = "snake_case")]
pub struct ListWorkspacesResponse {
    pub workspaces: Vec<WorkspaceResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct CreateProviderAccountRequest {
    pub provider_type: String,
    pub label: String,
    #[serde(default)]
    pub workspace_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConfigurationSyncRole {
    Unassigned,
    Master,
    Follower,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConfigurationSyncTransportState {
    Unconfigured,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DesignateConfigurationMasterRequest {
    pub expected_instance_id: String,
    pub expected_state_version: u64,
}

/// Immutable command receipt. Reload status to obtain the current role.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct ConfigurationMasterDesignationResponse {
    pub instance_id: String,
    pub state_version: u64,
    pub group_id: String,
}

// No Debug: explicit imported content may contain private user-authored data.
#[derive(Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublishConfigurationSnapshotRequest {
    pub expected_instance_id: String,
    pub expected_group_id: String,
    pub expected_state_version: u64,
    pub snapshot: SharedConfigurationBundle,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SharedConfigurationBundle {
    /// Exact canonical schema-1 metadata, binding all package hashes.
    pub metadata_json: String,
    pub skills: Vec<SharedSkillPackageBundle>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SharedSkillPackageBundle {
    pub id: String,
    pub version: String,
    pub enabled: bool,
    pub dependencies: Vec<String>,
    pub files: Vec<SharedSkillFileBundle>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SharedSkillFileBundle {
    pub path: String,
    /// Explicit regular-file bytes encoded as JSON integers from 0 to 255.
    pub content: Vec<u8>,
    pub content_hash: String,
}

/// Immutable publication receipt. It is not a current-status or activation claim.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct ConfigurationPublicationResponse {
    pub instance_id: String,
    pub group_id: String,
    pub state_version: u64,
    pub revision: ConfigurationRevisionResponse,
}

/// Verified stored content read with its authority/revision in one transaction.
/// Reading does not activate it or attest to remote connectivity.
#[derive(Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct ConfigurationSnapshotResponse {
    pub instance_id: String,
    pub group_id: String,
    pub master_instance_id: String,
    pub state_version: u64,
    pub revision: ConfigurationRevisionResponse,
    pub snapshot: SharedConfigurationBundle,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct ConfigurationRevisionResponse {
    pub revision: u64,
    pub schema_version: u32,
    pub content_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct ConfigurationSyncStatusResponse {
    pub instance_id: String,
    pub state_version: u64,
    pub role: ConfigurationSyncRole,
    #[schemars(with = "RequiredNullableString")]
    pub group_id: Option<String>,
    #[schemars(with = "RequiredNullableString")]
    pub master_instance_id: Option<String>,
    #[schemars(with = "RequiredNullableConfigurationRevision")]
    pub applied_revision: Option<ConfigurationRevisionResponse>,
    #[schemars(with = "RequiredNullableConfigurationRevision")]
    pub observed_revision: Option<ConfigurationRevisionResponse>,
    pub transport: ConfigurationSyncTransportState,
}

/// Public setup metadata only. identity_id is independent of the private vault
/// references. Active does not imply current certificate validity, accessible
/// private keys, follower enrollment or an enabled remote listener.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct ConfigurationIdentitySummaryResponse {
    pub identity_id: String,
    pub phase: ConfigurationIdentityPhase,
    pub server_name: String,
    pub certificate_authority_fingerprint: String,
    pub not_before_unix_seconds: i64,
    pub leaf_not_after_unix_seconds: i64,
    pub ca_not_after_unix_seconds: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConfigurationIdentityPhase {
    Pending,
    Active,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct ConfigurationIdentityStatusResponse {
    pub instance_id: String,
    pub state_version: u64,
    pub role: ConfigurationSyncRole,
    #[schemars(with = "RequiredNullableString")]
    pub group_id: Option<String>,
    #[schemars(with = "RequiredNullableString")]
    pub master_instance_id: Option<String>,
    #[schemars(with = "RequiredNullableConfigurationIdentity")]
    pub identity: Option<ConfigurationIdentitySummaryResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConfigureMasterIdentityRequest {
    pub expected_instance_id: String,
    pub expected_group_id: String,
    pub expected_state_version: u64,
    pub server_name: String,
    pub not_before_unix_seconds: i64,
    pub leaf_not_after_unix_seconds: i64,
    pub ca_not_after_unix_seconds: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct ConfigurationIdentitySetupResponse {
    pub instance_id: String,
    pub group_id: String,
    pub reserved_state_version: u64,
    pub identity_id: String,
}

/// Public grant metadata only. Bearer credentials and their SHA-256 digests are
/// deliberately excluded from local status/list responses.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct ConfigurationReadGrantResponse {
    pub grant_id: String,
    #[serde(deserialize_with = "deserialize_required_nullable_string")]
    #[schemars(with = "RequiredNullableString")]
    pub issuance_attempt_id: Option<String>,
    pub group_id: String,
    pub master_instance_id: String,
    pub follower_instance_id: String,
    pub issued_state_version: u64,
    pub revoked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct ConfigurationReadGrantListResponse {
    pub grants: Vec<ConfigurationReadGrantResponse>,
    #[serde(deserialize_with = "deserialize_required_nullable_string")]
    #[schemars(with = "RequiredNullableString")]
    pub next_cursor: Option<String>,
}

/// Current lifecycle state of one master-side follower enrollment request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConfigurationFollowerEnrollmentRequestPhase {
    Pending,
    Approved,
    Rejected,
}

/// Credential-free metadata for one exact master-side follower request.
/// `follower_id`, `server_name`, and `master_ca_fingerprint` are claims made by
/// the follower and are not proof of its identity or validation against the
/// master's currently managed TLS identity. `credential_fingerprint` binds the
/// full request and credential digest, but is not the digest itself.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct ConfigurationFollowerEnrollmentRequestResponse {
    pub request_id: String,
    pub attempt_id: String,
    pub follower_id: String,
    pub follower_state_version: u64,
    pub group_id: String,
    pub master_instance_id: String,
    /// Follower-asserted server name, checked against active managed identity
    /// metadata on new admission and first approval, not on metadata reads.
    pub server_name: String,
    /// Follower-asserted CA fingerprint, checked against active managed identity
    /// metadata on new admission and first approval, not on metadata reads.
    pub master_ca_fingerprint: String,
    /// Full confirmation fingerprint over the immutable request and the
    /// credential digest. The digest and bearer remain private.
    pub credential_fingerprint: String,
    pub received_master_state_version: u64,
    pub phase: ConfigurationFollowerEnrollmentRequestPhase,
    #[serde(deserialize_with = "deserialize_required_nullable_configuration_read_grant")]
    #[schemars(with = "RequiredNullableConfigurationReadGrant")]
    pub grant: Option<ConfigurationReadGrantResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct ConfigurationFollowerEnrollmentRequestListResponse {
    pub requests: Vec<ConfigurationFollowerEnrollmentRequestResponse>,
    #[serde(deserialize_with = "deserialize_required_nullable_string")]
    #[schemars(with = "RequiredNullableString")]
    pub next_cursor: Option<String>,
}

/// Exact confirmation values displayed by the master before a local decision.
/// The request ID must match the URL path. Expected instance/version fence the
/// current master state; all remaining fields must match the immutable journal.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConfigurationFollowerEnrollmentDecisionRequest {
    pub expected_instance_id: String,
    pub expected_state_version: u64,
    pub request_id: String,
    pub attempt_id: String,
    /// Follower-asserted instance ID; this is not an identity proof.
    pub follower_id: String,
    pub follower_state_version: u64,
    pub group_id: String,
    pub master_instance_id: String,
    /// First approval requires this name to match the active managed identity.
    pub server_name: String,
    /// First approval requires this pin to match the active managed identity.
    pub master_ca_fingerprint: String,
    pub received_master_state_version: u64,
    pub credential_fingerprint: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConfigurationFollowerEnrollmentPhase {
    Reserved,
    Prepared,
    Retired,
}

/// Public follower-request metadata only. It omits CA bytes, vault references,
/// credential digests, and bearer credentials.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct ConfigurationFollowerEnrollmentResponse {
    pub attempt_id: String,
    pub follower_instance_id: String,
    pub expected_state_version: u64,
    pub group_id: String,
    pub master_instance_id: String,
    pub server_name: String,
    pub certificate_authority_fingerprint: String,
    pub phase: ConfigurationFollowerEnrollmentPhase,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct ConfigurationFollowerEnrollmentListResponse {
    pub enrollments: Vec<ConfigurationFollowerEnrollmentResponse>,
    #[serde(deserialize_with = "deserialize_required_nullable_string")]
    #[schemars(with = "RequiredNullableString")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PrepareConfigurationFollowerEnrollmentRequest {
    pub attempt_id: String,
    pub expected_instance_id: String,
    pub expected_state_version: u64,
    pub group_id: String,
    pub master_instance_id: String,
    pub server_name: String,
    pub certificate_authority_der: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RetireConfigurationFollowerEnrollmentRequest {
    pub expected_instance_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RevokeConfigurationReadGrantRequest {
    pub expected_instance_id: String,
    pub expected_state_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RetireMasterIdentityRequest {
    pub expected_instance_id: String,
    pub setup_idempotency_key: String,
}

/// Retire a known identity by its stable public ID. Exact repeats safely retry
/// cleanup, including for historical identities after a role change.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RetireMasterIdentityByIdRequest {
    pub expected_instance_id: String,
    pub identity_id: String,
}

struct RequiredNullableConfigurationIdentity;
impl JsonSchema for RequiredNullableConfigurationIdentity {
    fn inline_schema() -> bool {
        true
    }
    fn schema_name() -> Cow<'static, str> {
        "RequiredNullableConfigurationIdentity".into()
    }
    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let identity = generator.subschema_for::<ConfigurationIdentitySummaryResponse>();
        json_schema!({"anyOf": [identity, {"type": "null"}]})
    }
}

struct RequiredNullableConfigurationRevision;
impl JsonSchema for RequiredNullableConfigurationRevision {
    fn inline_schema() -> bool {
        true
    }
    fn schema_name() -> Cow<'static, str> {
        "RequiredNullableConfigurationRevision".into()
    }
    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let revision = generator.subschema_for::<ConfigurationRevisionResponse>();
        json_schema!({"anyOf": [revision, {"type": "null"}]})
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct ProviderAccountResponse {
    pub provider_account_id: String,
    pub provider_type: String,
    pub label: String,
    pub state: String,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
    #[schemars(with = "RequiredNullableU64")]
    pub last_used_at_unix_ms: Option<u64>,
    #[schemars(with = "RequiredNullableU64")]
    pub capabilities_refreshed_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct ListProviderAccountsResponse {
    pub provider_accounts: Vec<ProviderAccountResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct StartProviderAccountLoginResponse {
    pub attempt_id: String,
    pub verification_url: String,
    pub user_code: String,
    pub account: ProviderAccountResponse,
}

/// The authorization URL contains short-lived login state; keep it out of logs.
#[derive(Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct StartProviderAccountBrowserLoginResponse {
    pub attempt_id: String,
    pub authorization_url: String,
    pub account: ProviderAccountResponse,
}

impl std::fmt::Debug for StartProviderAccountBrowserLoginResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StartProviderAccountBrowserLoginResponse")
            .field("attempt_id", &self.attempt_id)
            .field("authorization_url", &"[REDACTED]")
            .field("account", &self.account)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderAccountLoginState {
    Pending,
    Connected,
    Failed,
    Cancelled,
    CleanupRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct ProviderAccountLoginResponse {
    pub attempt_id: String,
    pub state: ProviderAccountLoginState,
    pub account: ProviderAccountResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct AppendMessageRequest {
    pub content: String,
    #[serde(default)]
    pub attachments: Vec<ArtifactResponse>,
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
    #[serde(default)]
    pub attachments: Vec<ArtifactResponse>,
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
    #[serde(default)]
    pub attachments: Vec<ArtifactResponse>,
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
pub struct ListSessionsResponse {
    pub sessions: Vec<SessionResponse>,
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
    #[serde(default)]
    pub attachments: Vec<ArtifactResponse>,
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
pub struct ChangedFileResponse {
    pub path: String,
    pub kind: String,
    #[schemars(with = "RequiredNullableU64")]
    pub additions: Option<u64>,
    #[schemars(with = "RequiredNullableU64")]
    pub deletions: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct SessionChangesResponse {
    pub workspace_root_id: String,
    pub relative_directory: String,
    pub files: Vec<ChangedFileResponse>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionChangeDiffUnavailableReason {
    Untracked,
    Binary,
    Conflicted,
    Renamed,
    UnsupportedFileType,
    UnsupportedEncoding,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionChangeDiffContent {
    Ready {
        patch: String,
        truncated: bool,
    },
    Unavailable {
        reason: SessionChangeDiffUnavailableReason,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct SessionChangeDiffResponse {
    pub workspace_root_id: String,
    pub relative_directory: String,
    pub path: String,
    pub kind: String,
    pub content: SessionChangeDiffContent,
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
pub enum UsageAccounting {
    Delta,
    Cumulative,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UsageFinality {
    Partial,
    Final,
    Correction,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UsageSource {
    NativeProvider,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UsageQuantityRelation {
    Additive,
    Subset,
    Informational,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct UsageQuantityResponse {
    pub dimension: String,
    pub unit: String,
    pub amount: u64,
    pub relation: UsageQuantityRelation,
    #[schemars(with = "RequiredNullableString")]
    pub subset_of: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct UsageLedgerEntryResponse {
    pub usage_observation_id: String,
    pub model_invocation_id: String,
    pub work_id: String,
    pub run_id: String,
    pub session_id: String,
    pub workspace_id: String,
    pub provider_account_id: String,
    pub requested_model: String,
    pub revision: u64,
    #[schemars(with = "RequiredNullableString")]
    pub supersedes_usage_observation_id: Option<String>,
    pub update_id: String,
    pub accounting: UsageAccounting,
    pub finality: UsageFinality,
    pub completeness: UsageCompleteness,
    pub observed_at_unix_ms: u64,
    #[schemars(with = "RequiredNullableString")]
    pub request_id: Option<String>,
    #[schemars(with = "RequiredNullableString")]
    pub resolved_model: Option<String>,
    #[schemars(with = "RequiredNullableString")]
    pub service_tier: Option<String>,
    pub source: UsageSource,
    pub quantities: Vec<UsageQuantityResponse>,
    pub is_terminal: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS, PartialEq, Eq)]
pub struct UsageLedgerResponse {
    pub entries: Vec<UsageLedgerEntryResponse>,
    #[schemars(with = "RequiredNullableString")]
    pub next_cursor: Option<String>,
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

fn deserialize_required_nullable_configuration_read_grant<'de, D>(
    deserializer: D,
) -> Result<Option<ConfigurationReadGrantResponse>, D::Error>
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

struct RequiredNullableU64;

impl JsonSchema for RequiredNullableU64 {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "RequiredNullableU64".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({"type": ["integer", "null"], "format": "uint64"})
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

struct RequiredNullableConfigurationReadGrant;

impl JsonSchema for RequiredNullableConfigurationReadGrant {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "RequiredNullableConfigurationReadGrant".into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let grant = generator.subschema_for::<ConfigurationReadGrantResponse>();
        json_schema!({"anyOf": [grant, {"type": "null"}]})
    }
}
