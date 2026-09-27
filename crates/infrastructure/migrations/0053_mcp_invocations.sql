-- One external dispatch attempt per normal, already-claimed native ToolCall.
-- Bodies remain in frozen native requests and normal ToolCall results/artifacts.
CREATE TABLE mcp_invocations (
    tool_call_id TEXT PRIMARY KEY NOT NULL REFERENCES tool_calls(tool_call_id) ON DELETE RESTRICT,
    generation_id TEXT NOT NULL REFERENCES mcp_instance_generations(generation_id) ON DELETE RESTRICT,
    state TEXT NOT NULL CHECK (state IN ('dispatching','completed','failed','cancelled','interrupted'))
) WITHOUT ROWID;
-- Unknown/stateful operations serialize regardless of MCP annotations.
CREATE UNIQUE INDEX mcp_one_dispatch_per_generation ON mcp_invocations(generation_id)
    WHERE state = 'dispatching';
CREATE TABLE mcp_invocation_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    tool_call_id TEXT NOT NULL REFERENCES mcp_invocations(tool_call_id) ON DELETE RESTRICT,
    state TEXT NOT NULL CHECK (state IN ('dispatching','completed','failed','cancelled','interrupted')),
    UNIQUE (tool_call_id, state)
);
CREATE TRIGGER mcp_invocation_identity_immutable
BEFORE UPDATE OF tool_call_id, generation_id ON mcp_invocations
BEGIN SELECT RAISE(ABORT, 'MCP invocation identity is immutable'); END;
CREATE TRIGGER mcp_invocation_terminal_immutable
BEFORE UPDATE OF state ON mcp_invocations WHEN OLD.state != 'dispatching'
BEGIN SELECT RAISE(ABORT, 'MCP invocation outcome is terminal'); END;
CREATE TRIGGER mcp_invocations_retained BEFORE DELETE ON mcp_invocations
BEGIN SELECT RAISE(ABORT, 'MCP invocations are retained'); END;
CREATE TRIGGER mcp_invocation_events_immutable BEFORE UPDATE ON mcp_invocation_events
BEGIN SELECT RAISE(ABORT, 'MCP invocation events are immutable'); END;
CREATE TRIGGER mcp_invocation_events_retained BEFORE DELETE ON mcp_invocation_events
BEGIN SELECT RAISE(ABORT, 'MCP invocation events are retained'); END;
