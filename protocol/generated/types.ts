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

export type SessionResponse = { session_id: string, workspace_id: string, };

export type MessageRole = "user";

export type MessageResponse = { message_id: string, session_id: string, role: MessageRole, content: string, };

export type RunState = "queued" | "running" | "cancelling" | "completed" | "failed" | "cancelled";

export type ToolCallState = "requested" | "running" | "completed" | "failed" | "cancelled";

export type ToolOutputStream = "stdout" | "stderr";

export type ToolCallResponse = { tool_call_id: string, run_id: string, capability: string, state: ToolCallState, stdout: string | null, stderr: string | null, exit_code: number | null, };

export type RunResponse = { run_id: string, session_id: string, state: RunState, tool_calls: Array<ToolCallResponse>, };

export type SessionEventDataResponse = { "type": "session.created", workspace_id: string, } | { "type": "message.appended", message: MessageResponse, } | { "type": "run.created", run_id: string, state: RunState, } | { "type": "run.state_changed", run_id: string, state: RunState, } | { "type": "run.cancellation_requested", run_id: string, } | { "type": "tool_call.requested", tool_call: ToolCallResponse, } | { "type": "tool_call.state_changed", tool_call: ToolCallResponse, } | { "type": "tool_call.output", run_id: string, tool_call_id: string, stream: ToolOutputStream, content: string, };

export type SessionEventResponse = { event_id: string, cursor: string, session_id: string, event: SessionEventDataResponse, };

export type SessionEventsResponse = { events: Array<SessionEventResponse>, current_event_cursor: string, };
