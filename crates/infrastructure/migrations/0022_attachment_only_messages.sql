-- no-transaction
PRAGMA foreign_keys = OFF;
PRAGMA legacy_alter_table = ON;

CREATE TEMP TABLE kiln_0022_messages AS SELECT * FROM messages;
CREATE TEMP TABLE kiln_0022_send_run_input_idempotencies AS
SELECT run_id, idempotency_key, message_id, content, attachments_json, delivery_mode
FROM send_run_input_idempotencies;

DROP TABLE send_run_input_idempotencies;
DROP TABLE messages;

CREATE TABLE messages (
    message_id TEXT PRIMARY KEY NOT NULL,
    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
    content TEXT NOT NULL,
    target_run_id TEXT REFERENCES runs(run_id),
    status TEXT NOT NULL DEFAULT 'complete' CHECK (status IN ('complete', 'incomplete')),
    origin_run_id TEXT REFERENCES runs(run_id),
    model_invocation_id TEXT UNIQUE REFERENCES model_invocations(model_invocation_id),
    child_activity_run_id TEXT REFERENCES runs(run_id),
    child_activity_event_id TEXT REFERENCES session_events(event_id)
        CHECK (
            (child_activity_run_id IS NULL AND child_activity_event_id IS NULL)
            OR (
                child_activity_run_id IS NOT NULL
                AND child_activity_event_id IS NOT NULL
                AND role = 'user'
                AND status = 'complete'
                AND target_run_id IS NOT NULL
            )
        ),
    CHECK (
        (role = 'user' AND status = 'complete' AND origin_run_id IS NULL AND model_invocation_id IS NULL)
        OR
        (role = 'assistant' AND target_run_id IS NULL AND origin_run_id IS NOT NULL AND model_invocation_id IS NOT NULL)
    )
);

INSERT INTO messages (
    message_id, session_id, role, content, target_run_id, status, origin_run_id,
    model_invocation_id, child_activity_run_id, child_activity_event_id
)
SELECT message_id, session_id, role, content, target_run_id, status, origin_run_id,
       model_invocation_id, child_activity_run_id, child_activity_event_id
FROM kiln_0022_messages;
DROP TABLE kiln_0022_messages;

CREATE INDEX messages_session_id ON messages (session_id);
CREATE INDEX messages_target_run_id ON messages (target_run_id);
CREATE INDEX messages_origin_run_id ON messages (origin_run_id);
CREATE INDEX messages_child_activity_run_id ON messages (child_activity_run_id);
CREATE INDEX messages_child_activity_event_id ON messages (child_activity_event_id);

CREATE TABLE send_run_input_idempotencies (
    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) > 0),
    message_id TEXT NOT NULL UNIQUE REFERENCES messages(message_id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    attachments_json TEXT NOT NULL DEFAULT '[]',
    delivery_mode TEXT NOT NULL CHECK (delivery_mode IN ('queued', 'interrupt')),
    PRIMARY KEY (run_id, idempotency_key)
);

INSERT INTO send_run_input_idempotencies (
    run_id, idempotency_key, message_id, content, attachments_json, delivery_mode
)
SELECT run_id, idempotency_key, message_id, content, attachments_json, delivery_mode
FROM kiln_0022_send_run_input_idempotencies;
DROP TABLE kiln_0022_send_run_input_idempotencies;

PRAGMA legacy_alter_table = OFF;
PRAGMA foreign_keys = ON;
PRAGMA foreign_key_check;
