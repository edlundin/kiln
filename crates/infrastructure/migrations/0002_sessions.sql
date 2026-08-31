CREATE TABLE sessions (
    session_id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE
);

CREATE INDEX sessions_workspace_id
    ON sessions (workspace_id);

CREATE TABLE messages (
    message_id TEXT PRIMARY KEY NOT NULL,
    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role = 'user'),
    content TEXT NOT NULL
);

CREATE INDEX messages_session_id
    ON messages (session_id);

CREATE TABLE session_events (
    cursor INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    message_id TEXT REFERENCES messages(message_id) ON DELETE CASCADE,
    CHECK (cursor >= 1),
    CHECK (
        (event_type = 'session.created' AND message_id IS NULL)
        OR (event_type = 'message.appended' AND message_id IS NOT NULL)
    )
);

CREATE INDEX session_events_session_cursor
    ON session_events (session_id, cursor);

CREATE INDEX session_events_message_id
    ON session_events (message_id);
