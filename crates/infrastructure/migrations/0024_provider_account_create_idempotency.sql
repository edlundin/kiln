CREATE TABLE provider_account_create_idempotencies (
    idempotency_key TEXT PRIMARY KEY NOT NULL CHECK (length(idempotency_key) > 0),
    provider_account_id TEXT NOT NULL UNIQUE
        REFERENCES provider_accounts(provider_account_id) ON DELETE CASCADE,
    provider_type TEXT NOT NULL CHECK (length(trim(provider_type)) > 0),
    label TEXT NOT NULL CHECK (length(CAST(label AS BLOB)) > 0),
    workspace_ids_json TEXT NOT NULL
        CHECK (json_valid(workspace_ids_json) AND json_type(workspace_ids_json) = 'array')
);
