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

export type ListWorkspacesResponse = { workspaces: Array<WorkspaceResponse>, };

export type AppendMessageRequest = { content: string, };

export type CreateTaskRequest = { objective: string, parent_task_id: string | null, dependency_task_ids: Array<string>, };

export type UpdateTaskRequest = { objective: string, dependency_task_ids: Array<string>, };

export type TransitionTaskRequest = { state: TaskState, };

export type AssignTaskRequest = { run_id: string, };

export type SessionResponse = { session_id: string, workspace_id: string, };

export type ListSessionsResponse = { sessions: Array<SessionResponse>, };

export type MessageRole = "user" | "assistant";

export type MessageStatus = "complete" | "incomplete";

export type MessageResponse = { message_id: string, session_id: string, role: MessageRole, content: string, status: MessageStatus, origin_run_id: string | null, model_invocation_id: string | null, target_run_id: string | null, child_activity?: ChildActivityReference | null, };

export type MessageDeliveryMode = "queued" | "interrupt";

export type MessageDeliveryState = "queued" | "delivered" | "failed" | "cancelled";

export type MessageDeliveryResponse = { message: MessageResponse, delivery_mode: MessageDeliveryMode, state: MessageDeliveryState, };

export type TaskState = "pending" | "ready" | "running" | "blocked" | "completed" | "failed" | "cancelled";

export type TaskResponse = { task_id: string, session_id: string, objective: string, state: TaskState, parent_task_id: string | null, dependency_task_ids: Array<string>, assigned_run_id: string | null, };

export type StartRunRequest = { approval_policy: ApprovalPolicy, workspace_root_id: string, relative_directory: string, };

export type StartChildRunRequest = { approval_policy: ApprovalPolicy, workspace_root_id: string, relative_directory: string, user_input_mode: RunInputMode, task_id: string | null, };

export type SendRunInputRequest = { content: string, delivery_mode: MessageDeliveryMode, };

export type ChildActivityReference = { run_id: string, event_id: string, };

export type ReactToRunActivityRequest = { content: string, child_activity: ChildActivityReference, };

export type ApprovalPolicy = "ask" | "read_only" | "full_access";

export type ApprovalDecisionRequest = { decision: ApprovalDecision, };

export type ApprovalDecision = "approved" | "rejected";

export type RunState = "queued" | "running" | "waiting_for_approval" | "cancelling" | "completed" | "failed" | "cancelled";

export type RunInputMode = "interactive" | "read_only";

export type ToolCallState = "requested" | "awaiting_approval" | "ready" | "running" | "completed" | "failed" | "cancelled" | "denied";

export type ToolOutputStream = "stdout" | "stderr";

export type ArtifactResponse = { content_hash: string, media_type: string, size: string, };

export type WorkspaceScopeResponse = { workspace_root_id: string, relative_directory: string, };

export type ApprovalState = "pending" | "approved" | "rejected";

export type ApprovalResponse = { approval_id: string, run_id: string, tool_call_id: string, requested_scope: WorkspaceScopeResponse, state: ApprovalState, };

export type ToolCallResponse = { tool_call_id: string, run_id: string, capability: string, state: ToolCallState, requested_scope: WorkspaceScopeResponse | null, effective_scope: WorkspaceScopeResponse | null, stdout: string | null, stderr: string | null, stdout_artifact: ArtifactResponse | null, stderr_artifact: ArtifactResponse | null, exit_code: number | null, };

export type RunResponse = { run_id: string, session_id: string, parent_run_id: string | null, task_id: string | null, user_input_mode: RunInputMode, state: RunState, approval_policy: ApprovalPolicy | null, requested_scope: WorkspaceScopeResponse | null, tool_calls: Array<ToolCallResponse>, approvals: Array<ApprovalResponse>, };

export type SessionRunsResponse = { runs: Array<RunResponse>, };

export type ContextManifestCreatedResponse = { context_manifest_id: string, run_id: string, content_hash: string, entry_count: bigint, };

export type ModelInvocationPurpose = "generation" | "compaction";

export type ModelInvocationCompletionKind = "assistant_output" | "tool_requests";

export type ModelInvocationFailureReason = "provider_error" | "invalid_request" | "unknown";

export type ModelOutputStream = "assistant_text" | "reasoning_summary";

export type ModelOutputRecordedResponse = { output_chunk_id: string, model_invocation_id: string, run_id: string, position: bigint, stream: ModelOutputStream, content: string, };

export type UsageCompleteness = "complete" | "partial" | "unknown";

export type UsageObservedResponse = { usage_observation_id: string, model_invocation_id: string, work_id: string, run_id: string, provider_account_id: string, revision: bigint, supersedes_usage_observation_id: string | null, completeness: UsageCompleteness, is_terminal: boolean, };

export type ModelInvocationStatus = { "state": "pending" } | { "state": "in_flight" } | { "state": "completed", completion_kind: ModelInvocationCompletionKind, } | { "state": "failed", reason: ModelInvocationFailureReason, } | { "state": "cancelled" } | { "state": "interrupted" };

export type ModelInvocationEventResponse = { model_invocation_id: string, work_id: string, run_id: string, context_manifest_id: string, context_manifest_hash: string, provider_account_id: string, provider: string, model: string, purpose: ModelInvocationPurpose, retry_of: string | null, status: ModelInvocationStatus, };

export type SessionEventDataResponse = { "type": "model_invocation.output" } & ModelOutputRecordedResponse | { "type": "usage.observed" } & UsageObservedResponse | { "type": "context.manifest_created" } & ContextManifestCreatedResponse | { "type": "model_invocation.created" } & ModelInvocationEventResponse | { "type": "model_invocation.state_changed" } & ModelInvocationEventResponse | { "type": "session.created", workspace_id: string, } | { "type": "message.appended", message: MessageResponse, } | { "type": "task.created", task: TaskResponse, } | { "type": "task.updated", task: TaskResponse, } | { "type": "task.assigned", task: TaskResponse, } | { "type": "task.state_changed", task: TaskResponse, } | { "type": "run.created", run_id: string, state: RunState, parent_run_id: string | null, task_id: string | null, user_input_mode: RunInputMode, approval_policy: ApprovalPolicy | null, requested_scope: WorkspaceScopeResponse | null, } | { "type": "run.queued", run_id: string, } | { "type": "run.child_added", parent_run_id: string, child_run_id: string, } | { "type": "run.input_queued", run_id: string, message_id: string, } | { "type": "run.interrupt_requested", run_id: string, message_id: string, } | { "type": "run.input_delivered", run_id: string, message_id: string, } | { "type": "run.input_failed", run_id: string, message_id: string, } | { "type": "run.input_cancelled", run_id: string, message_id: string, } | { "type": "run.state_changed", run_id: string, state: RunState, } | { "type": "run.cancellation_requested", run_id: string, } | { "type": "tool_call.requested", tool_call: ToolCallResponse, } | { "type": "approval.requested", approval: ApprovalResponse, } | { "type": "approval.decided", approval: ApprovalResponse, } | { "type": "tool_call.denied", tool_call: ToolCallResponse, } | { "type": "tool_call.state_changed", tool_call: ToolCallResponse, } | { "type": "tool_call.output", run_id: string, tool_call_id: string, stream: ToolOutputStream, content: string, } | { "type": "artifact.registered", run_id: string, tool_call_id: string, stream: ToolOutputStream, artifact: ArtifactResponse, };

export type SessionEventResponse = { event_id: string, cursor: string, session_id: string, event: SessionEventDataResponse, };

export type SessionEventsResponse = { events: Array<SessionEventResponse>, current_event_cursor: string, };
