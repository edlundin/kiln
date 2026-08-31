CREATE TEMP TABLE kiln_0003_session_events_sequence (
    seq INTEGER NOT NULL
);

INSERT INTO kiln_0003_session_events_sequence (seq)
SELECT seq
FROM sqlite_sequence
WHERE name = 'session_events';

CREATE TABLE runs (
    run_id TEXT PRIMARY KEY NOT NULL,
    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'completed', 'failed'))
);

CREATE INDEX runs_session_id
    ON runs (session_id);

CREATE UNIQUE INDEX runs_one_active_root
    ON runs (session_id)
    WHERE state IN ('queued', 'running');

CREATE TABLE tool_calls (
    tool_call_id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    capability TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('requested', 'running', 'completed', 'failed')),
    stdout TEXT,
    stderr TEXT,
    exit_code INTEGER
);

CREATE INDEX tool_calls_run_id
    ON tool_calls (run_id);

CREATE TABLE session_events_new (
    cursor INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    message_id TEXT REFERENCES messages(message_id) ON DELETE CASCADE,
    run_id TEXT REFERENCES runs(run_id),
    tool_call_id TEXT REFERENCES tool_calls(tool_call_id),
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
        OR (event_type IN ('run.created', 'run.state_changed') AND message_id IS NULL AND run_id IS NOT NULL)
        OR (event_type IN ('tool_call.requested', 'tool_call.state_changed') AND message_id IS NULL AND run_id IS NOT NULL AND tool_call_id IS NOT NULL)
        OR (event_type = 'tool_call.output' AND message_id IS NULL AND run_id IS NOT NULL AND tool_call_id IS NOT NULL AND output_stream IS NOT NULL AND output_content IS NOT NULL)
    )
);

INSERT INTO session_events_new (
    cursor, event_id, session_id, event_type, message_id
)
SELECT cursor, event_id, session_id, event_type, message_id
FROM session_events
ORDER BY cursor;

DROP TABLE session_events;
ALTER TABLE session_events_new RENAME TO session_events;

INSERT INTO sqlite_sequence (name, seq)
SELECT 'session_events', seq
FROM kiln_0003_session_events_sequence
WHERE NOT EXISTS (
    SELECT 1 FROM sqlite_sequence WHERE name = 'session_events'
);

UPDATE sqlite_sequence
SET seq = MAX(
    seq,
    (SELECT seq FROM kiln_0003_session_events_sequence)
)
WHERE name = 'session_events'
  AND EXISTS (SELECT 1 FROM kiln_0003_session_events_sequence);

DROP TABLE kiln_0003_session_events_sequence;

CREATE INDEX session_events_session_cursor
    ON session_events (session_id, cursor);

CREATE INDEX session_events_message_id
    ON session_events (message_id);

CREATE INDEX session_events_run_id
    ON session_events (run_id);

CREATE INDEX session_events_tool_call_id
    ON session_events (tool_call_id);
