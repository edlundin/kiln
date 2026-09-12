ALTER TABLE messages
ADD COLUMN child_activity_run_id TEXT REFERENCES runs(run_id);

ALTER TABLE messages
ADD COLUMN child_activity_event_id TEXT REFERENCES session_events(event_id)
CHECK (
    (child_activity_run_id IS NULL AND child_activity_event_id IS NULL)
    OR (
        child_activity_run_id IS NOT NULL
        AND child_activity_event_id IS NOT NULL
        AND role = 'user'
        AND status = 'complete'
        AND target_run_id IS NOT NULL
    )
);

CREATE INDEX messages_child_activity_run_id
ON messages (child_activity_run_id);

CREATE INDEX messages_child_activity_event_id
ON messages (child_activity_event_id);
