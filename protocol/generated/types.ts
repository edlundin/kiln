export type ClientIdentity = { name: string, build: string, };

export type NegotiateRequest = { min_version: string, max_version: string, client: ClientIdentity, requested_capabilities: Array<string>, };

export type NegotiateResponse = { selected_version: string, supported_capabilities: Array<string>, selected_capabilities: Array<string>, store_identity: StoreIdentity, current_event_cursor: string | null, event_websocket_endpoint: string, };

export type ProblemDetails = { type: string, title: string, status: number, code: string, detail: string, };

export type StoreIdentity = { id: string, name: string, };

export type WebSocketFrame = { "type": "ack", version: string, capability: string, current_event_cursor: string | null, } | { "type": "error", code: string, message: string, } | { "type": "event", event: SessionEventResponse, };

export type CreateWorkspaceRequest = { name: string, roots: Array<WorkspaceRootRequest>, };

export type WorkspaceRootRequest = { name: string, path: string, };

export type WorkspaceRootResponse = { workspace_root_id: string, name: string, display_path: string, canonical_path: string, git_common_directory_path: string, position: number, state: string, };

export type WorkspaceResponse = { workspace_id: string, name: string, roots: Array<WorkspaceRootResponse>, };

export type AppendMessageRequest = { content: string, };

export type CreateTaskRequest = { objective: string, parent_task_id: string | null, dependency_task_ids: Array<string>, };

export type UpdateTaskRequest = { objective: string, dependency_task_ids: Array<string>, };

export type TransitionTaskRequest = { state: TaskState, };

export type SessionResponse = { session_id: string, workspace_id: string, };

export type MessageRole = "user";

export type MessageResponse = { message_id: string, session_id: string, role: MessageRole, content: string, };

export type TaskState = "pending" | "ready" | "running" | "blocked" | "completed" | "failed" | "cancelled";

export type TaskResponse = { task_id: string, session_id: string, objective: string, state: TaskState, parent_task_id: string | null, dependency_task_ids: Array<string>, assigned_run_id: string | null, };

export type StartRunRequest = { approval_policy: ApprovalPolicy, workspace_root_id: string, relative_directory: string, };

export type ApprovalPolicy = "ask" | "read_only" | "full_access";

export type ApprovalDecisionRequest = { decision: ApprovalDecision, };

export type ApprovalDecision = "approved" | "rejected";

export type RunState = "queued" | "running" | "waiting_for_approval" | "cancelling" | "completed" | "failed" | "cancelled";

export type ToolCallState = "requested" | "awaiting_approval" | "ready" | "running" | "completed" | "failed" | "cancelled" | "denied";

export type ToolOutputStream = "stdout" | "stderr";

export type ArtifactResponse = { content_hash: string, media_type: string, size: string, };

export type WorkspaceScopeResponse = { workspace_root_id: string, relative_directory: string, };

export type ApprovalState = "pending" | "approved" | "rejected";

export type ApprovalResponse = { approval_id: string, run_id: string, tool_call_id: string, requested_scope: WorkspaceScopeResponse, state: ApprovalState, };

export type ToolCallResponse = { tool_call_id: string, run_id: string, capability: string, state: ToolCallState, requested_scope: WorkspaceScopeResponse | null, effective_scope: WorkspaceScopeResponse | null, stdout: string | null, stderr: string | null, stdout_artifact: ArtifactResponse | null, stderr_artifact: ArtifactResponse | null, exit_code: number | null, };

export type RunResponse = { run_id: string, session_id: string, state: RunState, approval_policy: ApprovalPolicy | null, requested_scope: WorkspaceScopeResponse | null, tool_calls: Array<ToolCallResponse>, approvals: Array<ApprovalResponse>, };

export type SessionEventDataResponse = { "type": "session.created", workspace_id: string, } | { "type": "message.appended", message: MessageResponse, } | { "type": "task.created", task: TaskResponse, } | { "type": "task.updated", task: TaskResponse, } | { "type": "task.state_changed", task: TaskResponse, } | { "type": "run.created", run_id: string, state: RunState, approval_policy: ApprovalPolicy | null, requested_scope: WorkspaceScopeResponse | null, } | { "type": "run.state_changed", run_id: string, state: RunState, } | { "type": "run.cancellation_requested", run_id: string, } | { "type": "tool_call.requested", tool_call: ToolCallResponse, } | { "type": "approval.requested", approval: ApprovalResponse, } | { "type": "approval.decided", approval: ApprovalResponse, } | { "type": "tool_call.denied", tool_call: ToolCallResponse, } | { "type": "tool_call.state_changed", tool_call: ToolCallResponse, } | { "type": "tool_call.output", run_id: string, tool_call_id: string, stream: ToolOutputStream, content: string, } | { "type": "artifact.registered", run_id: string, tool_call_id: string, stream: ToolOutputStream, artifact: ArtifactResponse, };

export type SessionEventResponse = { event_id: string, cursor: string, session_id: string, event: SessionEventDataResponse, };

export type SessionEventsResponse = { events: Array<SessionEventResponse>, current_event_cursor: string, };
