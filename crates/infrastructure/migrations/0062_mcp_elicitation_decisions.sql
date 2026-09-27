-- Private normalized decisions. The immutable parent form pins their meaning;
-- parent input state still determines whether runtime response is possible.
CREATE TABLE mcp_elicitation_decisions (
    tool_call_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    decision_json TEXT NOT NULL CHECK (json_valid(decision_json) AND json_type(decision_json) = 'object'),
    PRIMARY KEY (tool_call_id, ordinal),
    FOREIGN KEY (tool_call_id, ordinal) REFERENCES mcp_elicitation_forms(tool_call_id, ordinal) ON DELETE RESTRICT
) WITHOUT ROWID;
CREATE TRIGGER mcp_elicitation_decision_requires_pending_input
BEFORE INSERT ON mcp_elicitation_decisions
WHEN NOT EXISTS (SELECT 1 FROM mcp_inputs WHERE tool_call_id = NEW.tool_call_id
    AND ordinal = NEW.ordinal AND kind = 'elicitation' AND state = 'required')
BEGIN SELECT RAISE(ABORT, 'MCP decision requires pending elicitation'); END;
CREATE TRIGGER mcp_elicitation_decisions_immutable BEFORE UPDATE ON mcp_elicitation_decisions
BEGIN SELECT RAISE(ABORT, 'MCP decisions are immutable'); END;
CREATE TRIGGER mcp_elicitation_decisions_retained BEFORE DELETE ON mcp_elicitation_decisions
BEGIN SELECT RAISE(ABORT, 'MCP decisions are retained'); END;
-- Resolution cannot bypass the recorded user decision for an attached form.
CREATE TRIGGER mcp_elicitation_resolution_requires_decision
BEFORE UPDATE OF state ON mcp_inputs
WHEN NEW.state = 'resolved'
    AND EXISTS (SELECT 1 FROM mcp_elicitation_forms WHERE tool_call_id = NEW.tool_call_id AND ordinal = NEW.ordinal)
    AND NOT EXISTS (SELECT 1 FROM mcp_elicitation_decisions WHERE tool_call_id = NEW.tool_call_id AND ordinal = NEW.ordinal)
BEGIN SELECT RAISE(ABORT, 'MCP form resolution requires a decision'); END;
