CREATE TABLE start_run_idempotencies (
    session_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    run_id TEXT NOT NULL UNIQUE,
    PRIMARY KEY (session_id, idempotency_key),
    FOREIGN KEY (session_id) REFERENCES sessions (session_id) ON DELETE CASCADE,
    FOREIGN KEY (run_id) REFERENCES runs (run_id) ON DELETE CASCADE
);
