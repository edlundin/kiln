-- URL consent is private and contentless. It never proves external completion.
CREATE TABLE mcp_elicitation_urls (
    tool_call_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    interaction_run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
    message TEXT NOT NULL,
    url TEXT NOT NULL,
    legacy_elicitation_id TEXT CHECK (legacy_elicitation_id IS NULL OR length(legacy_elicitation_id) > 0),
    PRIMARY KEY (tool_call_id, ordinal),
    FOREIGN KEY (tool_call_id, ordinal) REFERENCES mcp_inputs(tool_call_id, ordinal) ON DELETE RESTRICT
) WITHOUT ROWID;
CREATE TRIGGER mcp_elicitation_url_requires_pending_input
BEFORE INSERT ON mcp_elicitation_urls
WHEN NOT EXISTS (SELECT 1 FROM mcp_inputs WHERE tool_call_id = NEW.tool_call_id
    AND ordinal = NEW.ordinal AND kind = 'elicitation' AND state = 'required')
BEGIN SELECT RAISE(ABORT, 'MCP URL requires pending elicitation'); END;
CREATE TRIGGER mcp_elicitation_url_excludes_form
BEFORE INSERT ON mcp_elicitation_urls
WHEN EXISTS (SELECT 1 FROM mcp_elicitation_forms WHERE tool_call_id = NEW.tool_call_id AND ordinal = NEW.ordinal)
BEGIN SELECT RAISE(ABORT, 'MCP elicitation mode is immutable'); END;
CREATE TRIGGER mcp_elicitation_form_excludes_url
BEFORE INSERT ON mcp_elicitation_forms
WHEN EXISTS (SELECT 1 FROM mcp_elicitation_urls WHERE tool_call_id = NEW.tool_call_id AND ordinal = NEW.ordinal)
BEGIN SELECT RAISE(ABORT, 'MCP elicitation mode is immutable'); END;
CREATE TRIGGER mcp_elicitation_urls_immutable BEFORE UPDATE ON mcp_elicitation_urls
BEGIN SELECT RAISE(ABORT, 'MCP URLs are immutable'); END;
CREATE TRIGGER mcp_elicitation_urls_retained BEFORE DELETE ON mcp_elicitation_urls
BEGIN SELECT RAISE(ABORT, 'MCP URLs are retained'); END;
CREATE TABLE mcp_elicitation_url_decisions (
    tool_call_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('accept', 'decline', 'cancel')),
    PRIMARY KEY (tool_call_id, ordinal),
    FOREIGN KEY (tool_call_id, ordinal) REFERENCES mcp_elicitation_urls(tool_call_id, ordinal) ON DELETE RESTRICT
) WITHOUT ROWID;
CREATE TRIGGER mcp_elicitation_url_decision_requires_pending_input
BEFORE INSERT ON mcp_elicitation_url_decisions
WHEN NOT EXISTS (SELECT 1 FROM mcp_inputs WHERE tool_call_id = NEW.tool_call_id
    AND ordinal = NEW.ordinal AND kind = 'elicitation' AND state = 'required')
BEGIN SELECT RAISE(ABORT, 'MCP URL decision requires pending elicitation'); END;
CREATE TRIGGER mcp_elicitation_url_decisions_immutable BEFORE UPDATE ON mcp_elicitation_url_decisions
BEGIN SELECT RAISE(ABORT, 'MCP URL decisions are immutable'); END;
CREATE TRIGGER mcp_elicitation_url_decisions_retained BEFORE DELETE ON mcp_elicitation_url_decisions
BEGIN SELECT RAISE(ABORT, 'MCP URL decisions are retained'); END;
CREATE TRIGGER mcp_elicitation_url_resolution_requires_decision
BEFORE UPDATE OF state ON mcp_inputs
WHEN NEW.state = 'resolved'
    AND EXISTS (SELECT 1 FROM mcp_elicitation_urls WHERE tool_call_id = NEW.tool_call_id AND ordinal = NEW.ordinal)
    AND NOT EXISTS (SELECT 1 FROM mcp_elicitation_url_decisions WHERE tool_call_id = NEW.tool_call_id AND ordinal = NEW.ordinal)
BEGIN SELECT RAISE(ABORT, 'MCP URL resolution requires a decision'); END;
