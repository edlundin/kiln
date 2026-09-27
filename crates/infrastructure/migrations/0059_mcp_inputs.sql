-- Mediation receipts stay attached to the original, exclusively owned invocation.
-- No request IDs, bodies, responses, opaque requestState or credentials belong here.
CREATE TABLE mcp_inputs (
    tool_call_id TEXT NOT NULL REFERENCES mcp_invocations(tool_call_id) ON DELETE RESTRICT,
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    kind TEXT NOT NULL CHECK (kind IN ('roots','sampling','elicitation')),
    state TEXT NOT NULL CHECK (state IN ('required','resolved','interrupted')),
    PRIMARY KEY (tool_call_id, ordinal)
) WITHOUT ROWID;
CREATE UNIQUE INDEX mcp_one_pending_input ON mcp_inputs(tool_call_id) WHERE state = 'required';
CREATE TABLE mcp_input_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    tool_call_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('required','resolved','interrupted')),
    FOREIGN KEY (tool_call_id, ordinal) REFERENCES mcp_inputs(tool_call_id, ordinal) ON DELETE RESTRICT,
    UNIQUE (tool_call_id, ordinal, state)
);
CREATE TRIGGER mcp_input_identity_immutable
BEFORE UPDATE OF tool_call_id, ordinal, kind ON mcp_inputs
BEGIN SELECT RAISE(ABORT, 'MCP input identity is immutable'); END;
CREATE TRIGGER mcp_input_terminal_immutable
BEFORE UPDATE OF state ON mcp_inputs WHEN OLD.state != 'required'
BEGIN SELECT RAISE(ABORT, 'MCP input outcome is terminal'); END;
CREATE TRIGGER mcp_inputs_retained BEFORE DELETE ON mcp_inputs
BEGIN SELECT RAISE(ABORT, 'MCP inputs are retained'); END;
CREATE TRIGGER mcp_input_events_immutable BEFORE UPDATE ON mcp_input_events
BEGIN SELECT RAISE(ABORT, 'MCP input events are immutable'); END;
CREATE TRIGGER mcp_input_events_retained BEFORE DELETE ON mcp_input_events
BEGIN SELECT RAISE(ABORT, 'MCP input events are retained'); END;
-- Closing dispatch and interrupting any unresolved mediation are one transaction,
-- including startup reconciliation and callers that update the invocation directly.
CREATE TRIGGER mcp_input_interrupt_on_invocation_end
AFTER UPDATE OF state ON mcp_invocations WHEN NEW.state != 'dispatching'
BEGIN
    INSERT INTO mcp_input_events (tool_call_id, ordinal, state)
    SELECT tool_call_id, ordinal, 'interrupted' FROM mcp_inputs
    WHERE tool_call_id = NEW.tool_call_id AND state = 'required';
    UPDATE mcp_inputs SET state = 'interrupted'
    WHERE tool_call_id = NEW.tool_call_id AND state = 'required';
END;
