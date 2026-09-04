CREATE TABLE assign_task_idempotencies (
    task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) > 0),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    PRIMARY KEY (task_id, idempotency_key)
);

CREATE UNIQUE INDEX tasks_assigned_run_id
    ON tasks (assigned_run_id)
    WHERE assigned_run_id IS NOT NULL;

CREATE TEMP TABLE kiln_0010_session_events_sequence (seq INTEGER NOT NULL);

INSERT INTO kiln_0010_session_events_sequence (seq)
SELECT seq FROM sqlite_sequence WHERE name = 'session_events';

CREATE TABLE session_events_new (
    cursor INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    message_id TEXT REFERENCES messages(message_id) ON DELETE CASCADE,
    task_id TEXT REFERENCES tasks(task_id),
    run_id TEXT REFERENCES runs(run_id),
    tool_call_id TEXT REFERENCES tool_calls(tool_call_id),
    approval_id TEXT,
    task_objective TEXT,
    task_state TEXT,
    parent_task_id TEXT,
    dependency_task_ids TEXT,
    assigned_run_id TEXT,
    run_state TEXT,
    tool_call_state TEXT,
    approval_state TEXT,
    approval_policy TEXT,
    requested_workspace_root_id TEXT,
    requested_relative_directory TEXT,
    effective_workspace_root_id TEXT,
    effective_relative_directory TEXT,
    capability TEXT,
    stdout TEXT,
    stderr TEXT,
    exit_code INTEGER,
    output_stream TEXT,
    output_content TEXT,
    artifact_hash TEXT REFERENCES artifacts(content_hash),
    stdout_artifact_hash TEXT REFERENCES artifacts(content_hash),
    stderr_artifact_hash TEXT REFERENCES artifacts(content_hash),
    CHECK (
        (event_type = 'session.created' AND message_id IS NULL AND task_id IS NULL AND run_id IS NULL AND tool_call_id IS NULL AND approval_id IS NULL)
        OR (event_type = 'message.appended' AND message_id IS NOT NULL AND task_id IS NULL AND run_id IS NULL AND tool_call_id IS NULL AND approval_id IS NULL)
        OR (event_type IN ('task.created', 'task.updated', 'task.state_changed') AND message_id IS NULL AND task_id IS NOT NULL AND run_id IS NULL AND tool_call_id IS NULL AND approval_id IS NULL AND task_objective IS NOT NULL AND task_state IS NOT NULL AND dependency_task_ids IS NOT NULL)
        OR (event_type = 'task.assigned' AND message_id IS NULL AND task_id IS NOT NULL AND run_id IS NULL AND tool_call_id IS NULL AND approval_id IS NULL AND task_objective IS NOT NULL AND task_state IS NOT NULL AND dependency_task_ids IS NOT NULL AND assigned_run_id IS NOT NULL)
        OR (event_type IN ('run.created', 'run.state_changed', 'run.cancellation_requested') AND message_id IS NULL AND task_id IS NULL AND run_id IS NOT NULL AND tool_call_id IS NULL AND approval_id IS NULL)
        OR (event_type IN ('tool_call.requested', 'tool_call.state_changed', 'tool_call.denied') AND message_id IS NULL AND task_id IS NULL AND run_id IS NOT NULL AND tool_call_id IS NOT NULL AND approval_id IS NULL)
        OR (event_type IN ('approval.requested', 'approval.decided') AND message_id IS NULL AND task_id IS NULL AND run_id IS NOT NULL AND tool_call_id IS NOT NULL AND approval_id IS NOT NULL)
        OR (event_type = 'tool_call.output' AND message_id IS NULL AND task_id IS NULL AND run_id IS NOT NULL AND tool_call_id IS NOT NULL AND approval_id IS NULL AND output_stream IS NOT NULL AND output_content IS NOT NULL AND artifact_hash IS NULL)
        OR (event_type = 'artifact.registered' AND message_id IS NULL AND task_id IS NULL AND run_id IS NOT NULL AND tool_call_id IS NOT NULL AND approval_id IS NULL AND output_stream IS NOT NULL AND output_content IS NULL AND artifact_hash IS NOT NULL)
    )
);

INSERT INTO session_events_new (
    cursor, event_id, session_id, event_type, message_id, task_id, run_id,
    tool_call_id, approval_id, task_objective, task_state, parent_task_id,
    dependency_task_ids, assigned_run_id, run_state, tool_call_state,
    approval_state, approval_policy, requested_workspace_root_id,
    requested_relative_directory, effective_workspace_root_id,
    effective_relative_directory, capability, stdout, stderr, exit_code,
    output_stream, output_content, artifact_hash, stdout_artifact_hash,
    stderr_artifact_hash
)
SELECT
    cursor, event_id, session_id, event_type, message_id, task_id, run_id,
    tool_call_id, approval_id, task_objective, task_state, parent_task_id,
    dependency_task_ids, assigned_run_id, run_state, tool_call_state,
    approval_state, approval_policy, requested_workspace_root_id,
    requested_relative_directory, effective_workspace_root_id,
    effective_relative_directory, capability, stdout, stderr, exit_code,
    output_stream, output_content, artifact_hash, stdout_artifact_hash,
    stderr_artifact_hash
FROM session_events
ORDER BY cursor;

DROP TABLE session_events;
ALTER TABLE session_events_new RENAME TO session_events;

INSERT INTO sqlite_sequence (name, seq)
SELECT 'session_events', seq FROM kiln_0010_session_events_sequence
WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'session_events');
UPDATE sqlite_sequence
SET seq = MAX(seq, (SELECT seq FROM kiln_0010_session_events_sequence))
WHERE name = 'session_events'
  AND EXISTS (SELECT 1 FROM kiln_0010_session_events_sequence);
DROP TABLE kiln_0010_session_events_sequence;

CREATE INDEX session_events_session_cursor ON session_events (session_id, cursor);
CREATE INDEX session_events_message_id ON session_events (message_id);
CREATE INDEX session_events_task_id ON session_events (task_id);
CREATE INDEX session_events_run_id ON session_events (run_id);
CREATE INDEX session_events_tool_call_id ON session_events (tool_call_id);
CREATE INDEX session_events_artifact_hash ON session_events (artifact_hash);
