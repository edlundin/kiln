-- Keep a current revision tombstone after removal. Deleting the current row
-- would reopen the unbound/materialized launch path for a previously bound key.
ALTER TABLE mcp_host_binding_versions ADD COLUMN retired INTEGER NOT NULL DEFAULT 0
    CHECK (retired IN (0, 1));
CREATE TRIGGER mcp_host_binding_current_retained BEFORE DELETE ON mcp_host_bindings
BEGIN SELECT RAISE(ABORT, 'MCP host binding revision tombstones are retained'); END;
