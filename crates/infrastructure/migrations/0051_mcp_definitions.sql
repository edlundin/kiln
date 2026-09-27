-- Registration is inert metadata. Immutable revisions and command receipts
-- prevent a delayed retry from restoring an obsolete definition.
CREATE TABLE mcp_definition_versions (
    definition_id TEXT NOT NULL CHECK (length(definition_id) > 0),
    version INTEGER NOT NULL CHECK (version > 0),
    metadata_json TEXT NOT NULL CHECK (json_valid(metadata_json)),
    PRIMARY KEY (definition_id, version)
) WITHOUT ROWID;

CREATE TABLE mcp_definitions (
    definition_id TEXT PRIMARY KEY NOT NULL,
    version INTEGER NOT NULL,
    FOREIGN KEY (definition_id, version)
        REFERENCES mcp_definition_versions(definition_id, version) ON DELETE RESTRICT
) WITHOUT ROWID;

CREATE TABLE mcp_definition_commands (
    idempotency_key TEXT PRIMARY KEY NOT NULL CHECK (length(idempotency_key) > 0),
    definition_id TEXT NOT NULL,
    expected_version INTEGER NOT NULL CHECK (expected_version >= 0),
    result_version INTEGER NOT NULL CHECK (result_version > 0),
    FOREIGN KEY (definition_id, result_version)
        REFERENCES mcp_definition_versions(definition_id, version) ON DELETE RESTRICT
) WITHOUT ROWID;

-- Metadata-only audit: command arguments, server content and credential values
-- are deliberately absent. Runtime lifecycle/invocation events are separate work.
CREATE TABLE mcp_definition_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    definition_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type = 'mcp_server.registered'),
    FOREIGN KEY (definition_id, version)
        REFERENCES mcp_definition_versions(definition_id, version) ON DELETE RESTRICT
);

CREATE TRIGGER mcp_definition_versions_immutable BEFORE UPDATE ON mcp_definition_versions
BEGIN SELECT RAISE(ABORT, 'MCP definition versions are immutable'); END;
CREATE TRIGGER mcp_definition_versions_retained BEFORE DELETE ON mcp_definition_versions
BEGIN SELECT RAISE(ABORT, 'MCP definition versions are retained'); END;
CREATE TRIGGER mcp_definition_commands_immutable BEFORE UPDATE ON mcp_definition_commands
BEGIN SELECT RAISE(ABORT, 'MCP definition commands are immutable'); END;
CREATE TRIGGER mcp_definition_commands_retained BEFORE DELETE ON mcp_definition_commands
BEGIN SELECT RAISE(ABORT, 'MCP definition commands are retained'); END;
CREATE TRIGGER mcp_definition_events_immutable BEFORE UPDATE ON mcp_definition_events
BEGIN SELECT RAISE(ABORT, 'MCP definition events are immutable'); END;
CREATE TRIGGER mcp_definition_events_retained BEFORE DELETE ON mcp_definition_events
BEGIN SELECT RAISE(ABORT, 'MCP definition events are retained'); END;
