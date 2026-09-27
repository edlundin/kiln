CREATE TABLE mcp_host_binding_versions (
    instance_key TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    metadata_json TEXT NOT NULL CHECK (json_valid(metadata_json)),
    PRIMARY KEY (instance_key, revision)
) WITHOUT ROWID;
CREATE TABLE mcp_host_bindings (
    instance_key TEXT PRIMARY KEY NOT NULL,
    revision INTEGER NOT NULL,
    FOREIGN KEY (instance_key, revision) REFERENCES mcp_host_binding_versions(instance_key, revision) ON DELETE RESTRICT
) WITHOUT ROWID;
CREATE TABLE mcp_host_binding_refs (
    instance_key TEXT NOT NULL REFERENCES mcp_host_bindings(instance_key) ON DELETE RESTRICT,
    purpose TEXT NOT NULL CHECK (purpose IN ('argument', 'environment')),
    binding_name TEXT NOT NULL,
    secret_ref TEXT NOT NULL UNIQUE REFERENCES mcp_secret_reservations(secret_ref) ON DELETE RESTRICT,
    PRIMARY KEY (instance_key, purpose, binding_name)
) WITHOUT ROWID;
CREATE TRIGGER mcp_host_binding_versions_immutable BEFORE UPDATE ON mcp_host_binding_versions
BEGIN SELECT RAISE(ABORT, 'MCP host snapshots are immutable'); END;
CREATE TRIGGER mcp_host_binding_versions_retained BEFORE DELETE ON mcp_host_binding_versions
BEGIN SELECT RAISE(ABORT, 'MCP host snapshots are retained'); END;
CREATE TRIGGER mcp_published_secret_retirement BEFORE UPDATE OF state ON mcp_secret_reservations
WHEN EXISTS (SELECT 1 FROM mcp_host_binding_refs WHERE secret_ref = OLD.secret_ref)
BEGIN SELECT RAISE(ABORT, 'Published MCP secrets cannot be retired'); END;
ALTER TABLE mcp_instance_generations ADD COLUMN host_instance_id TEXT;
ALTER TABLE mcp_instance_generations ADD COLUMN host_binding_revision INTEGER CHECK (host_binding_revision > 0);
CREATE TRIGGER mcp_generation_host_identity_immutable
BEFORE UPDATE OF host_instance_id, host_binding_revision ON mcp_instance_generations
BEGIN SELECT RAISE(ABORT, 'MCP generation host snapshot is immutable'); END;
