-- Vault values stay in dev.kiln.mcp. References are globally single-use within
-- this store, even after deletion or a definition/scope/profile change.
CREATE TABLE mcp_secret_reservations (
    secret_ref TEXT PRIMARY KEY NOT NULL,
    kiln_instance_id TEXT NOT NULL,
    instance_key TEXT NOT NULL,
    binding_name TEXT NOT NULL,
    purpose TEXT NOT NULL CHECK (purpose IN ('argument', 'environment')),
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
