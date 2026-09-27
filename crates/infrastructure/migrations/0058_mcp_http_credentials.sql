-- Rebuild both sides of the reference relation in this migration transaction.
-- Preserve all single-use receipts, including deleted tombstones, and published
-- references. Foreign-key enforcement remains enabled throughout the upgrade.
CREATE TEMP TABLE mcp_secret_upgrade AS SELECT * FROM mcp_secret_reservations;
CREATE TEMP TABLE mcp_refs_upgrade AS SELECT * FROM mcp_host_binding_refs;
DROP TRIGGER mcp_published_secret_retirement;
DROP TABLE mcp_host_binding_refs;
DROP TRIGGER mcp_secret_reservations_retained;
DROP TABLE mcp_secret_reservations;
-- Vault values stay in dev.kiln.mcp. References are globally single-use within
-- this store, even after deletion or a definition/scope/profile change.
CREATE TABLE mcp_secret_reservations (
    secret_ref TEXT PRIMARY KEY NOT NULL,
    kiln_instance_id TEXT NOT NULL,
    instance_key TEXT NOT NULL,
    binding_name TEXT NOT NULL,
    purpose TEXT NOT NULL CHECK (purpose IN ('argument', 'environment', 'http_credential')),
    definition_version INTEGER NOT NULL CHECK (definition_version > 0),
    state TEXT NOT NULL CHECK (state IN ('reserved', 'retired', 'deleted'))
) WITHOUT ROWID;
CREATE INDEX mcp_secret_pending_scope
    ON mcp_secret_reservations(kiln_instance_id, instance_key, secret_ref)
    WHERE state != 'deleted';
CREATE TRIGGER mcp_secret_reservation_identity_immutable
BEFORE UPDATE OF secret_ref, kiln_instance_id, instance_key, binding_name, purpose, definition_version
ON mcp_secret_reservations
BEGIN SELECT RAISE(ABORT, 'MCP secret reservation identity is immutable'); END;
CREATE TRIGGER mcp_secret_reservation_transition
BEFORE UPDATE OF state ON mcp_secret_reservations
WHEN NOT ((OLD.state = 'reserved' AND NEW.state = 'retired')
    OR (OLD.state = 'retired' AND NEW.state = 'deleted'))
BEGIN SELECT RAISE(ABORT, 'Invalid MCP secret reservation transition'); END;
CREATE TRIGGER mcp_secret_reservations_retained BEFORE DELETE ON mcp_secret_reservations
BEGIN SELECT RAISE(ABORT, 'MCP secret reservation tombstones are retained'); END;
INSERT INTO mcp_secret_reservations SELECT * FROM mcp_secret_upgrade;
CREATE TABLE mcp_host_binding_refs (
    instance_key TEXT NOT NULL REFERENCES mcp_host_bindings(instance_key) ON DELETE RESTRICT,
    purpose TEXT NOT NULL CHECK (purpose IN ('argument', 'environment', 'http_credential')),
    binding_name TEXT NOT NULL,
    secret_ref TEXT NOT NULL UNIQUE REFERENCES mcp_secret_reservations(secret_ref) ON DELETE RESTRICT,
    PRIMARY KEY (instance_key, purpose, binding_name)
) WITHOUT ROWID;
INSERT INTO mcp_host_binding_refs SELECT * FROM mcp_refs_upgrade;
CREATE TRIGGER mcp_published_secret_retirement BEFORE UPDATE OF state ON mcp_secret_reservations
WHEN EXISTS (SELECT 1 FROM mcp_host_binding_refs WHERE secret_ref = OLD.secret_ref)
BEGIN SELECT RAISE(ABORT, 'Published MCP secrets cannot be retired'); END;
DROP TABLE mcp_refs_upgrade;
DROP TABLE mcp_secret_upgrade;
