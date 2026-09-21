-- Private, immutable version captured with the first execution claim. Historical
-- invocations remain unpinned; do not backfill from mutable current credentials.
CREATE TABLE model_invocation_credentials (
    model_invocation_id TEXT PRIMARY KEY NOT NULL REFERENCES model_invocations(model_invocation_id),
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
    secret_ref TEXT NOT NULL CHECK (
        length(CAST(secret_ref AS BLOB)) = 30
        AND substr(secret_ref, 1, 4) = 'sec_'
        AND substr(secret_ref, 5) NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'
    )
);
