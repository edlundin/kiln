-- Portable model binding keys stay separate from shared snapshots. Retained
-- tombstones carry per-key versions across removal and later recreation.
CREATE TABLE host_model_account_bindings (
    binding_key TEXT PRIMARY KEY NOT NULL CHECK (
        length(CAST(binding_key AS BLOB)) BETWEEN 1 AND 2097152
        AND instr(binding_key, char(0)) = 0
        AND binding_key NOT GLOB '*[^a-z0-9_-]*'
    ),
    provider_account_id TEXT
        REFERENCES provider_accounts(provider_account_id) ON DELETE RESTRICT,
    version INTEGER NOT NULL CHECK (version > 0)
) WITHOUT ROWID;
