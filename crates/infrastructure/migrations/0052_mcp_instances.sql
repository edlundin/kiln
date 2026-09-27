CREATE TABLE mcp_instance_generations (
    generation_id TEXT PRIMARY KEY NOT NULL CHECK (length(generation_id) = 30),
    instance_key TEXT NOT NULL CHECK (json_valid(instance_key)),
    definition_id TEXT NOT NULL,
    definition_version INTEGER NOT NULL,
    state_version INTEGER NOT NULL CHECK (state_version > 0),
    desired TEXT NOT NULL CHECK (desired IN ('running', 'stopped')),
    observed TEXT NOT NULL CHECK (observed IN ('starting', 'ready', 'stopping', 'stopped', 'interrupted', 'failed')),
    negotiated_protocol TEXT CHECK (negotiated_protocol IN ('2024-11-05','2025-03-26','2025-06-18','2025-11-25','2026-07-28')),
    CHECK (json_extract(instance_key, '$.definition_id') = definition_id),
    CHECK (observed != 'ready' OR negotiated_protocol IS NOT NULL),
    CHECK (observed NOT IN ('starting','ready') OR desired = 'running'),
    CHECK (observed != 'stopping' OR desired = 'stopped'),
    UNIQUE (instance_key, generation_id),
    FOREIGN KEY (definition_id, definition_version) REFERENCES mcp_definition_versions(definition_id, version) ON DELETE RESTRICT
);
CREATE TABLE mcp_instances (
    instance_key TEXT PRIMARY KEY NOT NULL,
    generation_id TEXT UNIQUE NOT NULL,
    FOREIGN KEY (instance_key, generation_id) REFERENCES mcp_instance_generations(instance_key, generation_id) ON DELETE RESTRICT
) WITHOUT ROWID;
CREATE TABLE mcp_instance_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    generation_id TEXT NOT NULL REFERENCES mcp_instance_generations(generation_id) ON DELETE RESTRICT,
    state_version INTEGER NOT NULL CHECK (state_version > 0),
    desired TEXT NOT NULL CHECK (desired IN ('running','stopped')),
    observed TEXT NOT NULL CHECK (observed IN ('starting','ready','stopping','stopped','interrupted','failed')),
    negotiated_protocol TEXT CHECK (negotiated_protocol IN ('2024-11-05','2025-03-26','2025-06-18','2025-11-25','2026-07-28')),
    reason TEXT NOT NULL CHECK (reason IN ('start','ready','stop_requested','stopped','connection_lost','startup_failed','daemon_restart')),
    CHECK (observed != 'ready' OR negotiated_protocol IS NOT NULL),
    CHECK (observed NOT IN ('starting','ready') OR desired = 'running'),
    CHECK (observed != 'stopping' OR desired = 'stopped'),
    UNIQUE (generation_id, state_version)
);
CREATE TRIGGER mcp_generation_identity_immutable
BEFORE UPDATE OF generation_id, instance_key, definition_id, definition_version ON mcp_instance_generations
BEGIN SELECT RAISE(ABORT, 'MCP generation identity is immutable'); END;
CREATE TRIGGER mcp_generations_retained BEFORE DELETE ON mcp_instance_generations
BEGIN SELECT RAISE(ABORT, 'MCP generations are retained'); END;
CREATE TRIGGER mcp_instance_events_immutable BEFORE UPDATE ON mcp_instance_events
BEGIN SELECT RAISE(ABORT, 'MCP instance events are immutable'); END;
CREATE TRIGGER mcp_instance_events_retained BEFORE DELETE ON mcp_instance_events
BEGIN SELECT RAISE(ABORT, 'MCP instance events are retained'); END;
