-- Provider-private continuation data. No public Artifact or Event carries it.
CREATE TABLE model_invocation_continuations (
    model_invocation_id TEXT PRIMARY KEY NOT NULL REFERENCES model_invocations(model_invocation_id),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    provider_account_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    format TEXT NOT NULL,
    payload BLOB NOT NULL,
    payload_size INTEGER NOT NULL CHECK (payload_size > 0),
    content_hash TEXT NOT NULL
);
