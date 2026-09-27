-- Normalized untrusted form fields stay private. Input Events contain no body.
-- The parent input owns pending/resolved/interrupted state and retention.
CREATE TABLE mcp_elicitation_forms (
    tool_call_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    interaction_run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
    message TEXT NOT NULL,
    schema_json TEXT NOT NULL CHECK (json_valid(schema_json) AND json_type(schema_json) = 'object'),
    PRIMARY KEY (tool_call_id, ordinal),
    FOREIGN KEY (tool_call_id, ordinal) REFERENCES mcp_inputs(tool_call_id, ordinal) ON DELETE RESTRICT
) WITHOUT ROWID;
CREATE TRIGGER mcp_elicitation_form_requires_pending_input
BEFORE INSERT ON mcp_elicitation_forms
WHEN NOT EXISTS (SELECT 1 FROM mcp_inputs WHERE tool_call_id = NEW.tool_call_id
    AND ordinal = NEW.ordinal AND kind = 'elicitation' AND state = 'required')
BEGIN SELECT RAISE(ABORT, 'MCP form requires pending elicitation'); END;
CREATE TRIGGER mcp_elicitation_forms_immutable BEFORE UPDATE ON mcp_elicitation_forms
BEGIN SELECT RAISE(ABORT, 'MCP forms are immutable'); END;
CREATE TRIGGER mcp_elicitation_forms_retained BEFORE DELETE ON mcp_elicitation_forms
BEGIN SELECT RAISE(ABORT, 'MCP forms are retained'); END;
