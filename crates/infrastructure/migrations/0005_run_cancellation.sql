CREATE TEMP TABLE kiln_0005_session_events_sequence (
    seq INTEGER NOT NULL
);

INSERT INTO kiln_0005_session_events_sequence (seq)
SELECT seq
FROM sqlite_sequence
WHERE name = 'session_events';

CREATE TABLE runs_new (
    run_id TEXT PRIMARY KEY NOT NULL,
    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK (
        state IN ('queued', 'running', 'cancelling', 'completed', 'failed', 'cancelled')
    )
);

CREATE TABLE tool_calls_new (
    tool_call_id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT NOT NULL REFERENCES runs_new(run_id) ON DELETE CASCADE,
    capability TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('requested', 'running', 'completed', 'failed', 'cancelled')
    ),
    stdout TEXT,
    stderr TEXT,
    exit_code INTEGER
);

CREATE TABLE session_events_new (
    cursor INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    message_id TEXT REFERENCES messages(message_id) ON DELETE CASCADE,
    run_id TEXT REFERENCES runs_new(run_id),
    tool_call_id TEXT REFERENCES tool_calls_new(tool_call_id),
    run_state TEXT,
    tool_call_state TEXT,
    capability TEXT,
    stdout TEXT,
    stderr TEXT,
    exit_code INTEGER,
    output_stream TEXT,
    output_content TEXT,
    CHECK (
        (event_type = 'session.created' AND message_id IS NULL AND run_id IS NULL AND tool_call_id IS NULL)
        OR (event_type = 'message.appended' AND message_id IS NOT NULL AND run_id IS NULL AND tool_call_id IS NULL)
        OR (event_type IN ('run.created', 'run.state_changed', 'run.cancellation_requested') AND message_id IS NULL AND run_id IS NOT NULL)
        OR (event_type IN ('tool_call.requested', 'tool_call.state_changed') AND message_id IS NULL AND run_id IS NOT NULL AND tool_call_id IS NOT NULL)
        OR (event_type = 'tool_call.output' AND message_id IS NULL AND run_id IS NOT NULL AND tool_call_id IS NOT NULL AND output_stream IS NOT NULL AND output_content IS NOT NULL)
    )
);

CREATE TABLE start_run_idempotencies_new (
    session_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    run_id TEXT NOT NULL UNIQUE,
    PRIMARY KEY (session_id, idempotency_key),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
    FOREIGN KEY (run_id) REFERENCES runs_new(run_id) ON DELETE CASCADE
);

INSERT INTO runs_new (run_id, session_id, state)
SELECT run_id, session_id, state
FROM runs;

INSERT INTO tool_calls_new (
    tool_call_id, run_id, capability, state, stdout, stderr, exit_code
)
SELECT tool_call_id, run_id, capability, state, stdout, stderr, exit_code
FROM tool_calls;

INSERT INTO session_events_new (
    cursor,
    event_id,
    session_id,
    event_type,
    message_id,
    run_id,
    tool_call_id,
    run_state,
    tool_call_state,
    capability,
    stdout,
    stderr,
    exit_code,
    output_stream,
    output_content
)
SELECT
    cursor,
    event_id,
    session_id,
    event_type,
    message_id,
    run_id,
    tool_call_id,
    run_state,
    tool_call_state,
    capability,
    stdout,
    stderr,
    exit_code,
    output_stream,
    output_content
FROM session_events
ORDER BY cursor;

INSERT INTO start_run_idempotencies_new (session_id, idempotency_key, run_id)
SELECT session_id, idempotency_key, run_id
FROM start_run_idempotencies;

DROP TABLE start_run_idempotencies;
DROP TABLE session_events;
DROP TABLE tool_calls;
DROP TABLE runs;

ALTER TABLE runs_new RENAME TO runs;
ALTER TABLE tool_calls_new RENAME TO tool_calls;
ALTER TABLE session_events_new RENAME TO session_events;
ALTER TABLE start_run_idempotencies_new RENAME TO start_run_idempotencies;

INSERT INTO sqlite_sequence (name, seq)
SELECT 'session_events', seq
FROM kiln_0005_session_events_sequence
WHERE NOT EXISTS (
    SELECT 1 FROM sqlite_sequence WHERE name = 'session_events'
);

UPDATE sqlite_sequence
SET seq = MAX(
    seq,
    (SELECT seq FROM kiln_0005_session_events_sequence)
)
WHERE name = 'session_events'
  AND EXISTS (SELECT 1 FROM kiln_0005_session_events_sequence);

DROP TABLE kiln_0005_session_events_sequence;

CREATE INDEX runs_session_id
    ON runs (session_id);

CREATE UNIQUE INDEX runs_one_active_root
    ON runs (session_id)
    WHERE state IN ('queued', 'running', 'cancelling');

CREATE INDEX tool_calls_run_id
    ON tool_calls (run_id);

CREATE INDEX session_events_session_cursor
    ON session_events (session_id, cursor);

CREATE INDEX session_events_message_id
    ON session_events (message_id);

CREATE INDEX session_events_run_id
    ON session_events (run_id);

CREATE INDEX session_events_tool_call_id
    ON session_events (tool_call_id);
