CREATE TEMP TABLE kiln_0017_context_manifest_entries AS SELECT * FROM context_manifest_entries;
CREATE TEMP TABLE kiln_0017_send_run_input_idempotencies AS SELECT * FROM send_run_input_idempotencies;
CREATE TEMP TABLE kiln_0017_message_deliveries AS SELECT * FROM message_deliveries;
CREATE TEMP TABLE kiln_0017_session_events AS SELECT * FROM session_events;
CREATE TEMP TABLE kiln_0017_messages AS SELECT * FROM messages;
CREATE TEMP TABLE kiln_0017_session_events_sequence (seq INTEGER NOT NULL);
INSERT INTO kiln_0017_session_events_sequence (seq)
SELECT seq FROM sqlite_sequence WHERE name = 'session_events';

DROP TABLE context_manifest_entries;
DROP TABLE send_run_input_idempotencies;
DROP TABLE message_deliveries;
DROP TABLE session_events;
DROP TABLE messages;

CREATE TABLE messages (
    message_id TEXT PRIMARY KEY NOT NULL,
    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
    content TEXT NOT NULL CHECK (length(CAST(content AS BLOB)) > 0),
    target_run_id TEXT REFERENCES runs(run_id),
    status TEXT NOT NULL DEFAULT 'complete' CHECK (status IN ('complete', 'incomplete')),
    origin_run_id TEXT REFERENCES runs(run_id),
    model_invocation_id TEXT UNIQUE REFERENCES model_invocations(model_invocation_id),
    CHECK (
        (role = 'user' AND status = 'complete' AND origin_run_id IS NULL AND model_invocation_id IS NULL)
        OR (role = 'assistant' AND target_run_id IS NULL AND origin_run_id IS NOT NULL AND model_invocation_id IS NOT NULL)
    )
);

INSERT INTO messages (message_id, session_id, role, content, target_run_id)
SELECT message_id, session_id, role, content, target_run_id FROM kiln_0017_messages;
DROP TABLE kiln_0017_messages;
CREATE INDEX messages_session_id ON messages (session_id);
CREATE INDEX messages_target_run_id ON messages (target_run_id);
CREATE INDEX messages_origin_run_id ON messages (origin_run_id);

CREATE TABLE context_manifest_entries (
    context_manifest_id TEXT NOT NULL
        REFERENCES context_manifests(context_manifest_id) ON DELETE CASCADE,
    position INTEGER NOT NULL CHECK (position >= 0),
    entry_kind TEXT NOT NULL CHECK (entry_kind IN ('instruction', 'message')),
    provenance TEXT NOT NULL CHECK (
        provenance IN ('runtime', 'user', 'workspace', 'run', 'session_message')
    ),
    workspace_root_id TEXT REFERENCES workspace_roots(workspace_root_id),
    source_run_id TEXT REFERENCES runs(run_id),
    message_id TEXT REFERENCES messages(message_id),
    message_role TEXT CHECK (message_role IN ('user', 'assistant')),
    content TEXT NOT NULL CHECK (length(CAST(content AS BLOB)) > 0),
    PRIMARY KEY (context_manifest_id, position),
    CHECK (
        (entry_kind = 'instruction' AND provenance IN ('runtime', 'user')
            AND workspace_root_id IS NULL AND source_run_id IS NULL
            AND message_id IS NULL AND message_role IS NULL)
        OR (entry_kind = 'instruction' AND provenance = 'workspace'
            AND workspace_root_id IS NOT NULL AND source_run_id IS NULL
            AND message_id IS NULL AND message_role IS NULL)
        OR (entry_kind = 'instruction' AND provenance = 'run'
            AND workspace_root_id IS NULL AND source_run_id IS NOT NULL
            AND message_id IS NULL AND message_role IS NULL)
        OR (entry_kind = 'message' AND provenance = 'session_message'
            AND workspace_root_id IS NULL AND source_run_id IS NULL
            AND message_id IS NOT NULL AND message_role IS NOT NULL)
    )
);

INSERT INTO context_manifest_entries SELECT * FROM kiln_0017_context_manifest_entries;
DROP TABLE kiln_0017_context_manifest_entries;

CREATE TABLE send_run_input_idempotencies (
    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) > 0),
    message_id TEXT NOT NULL UNIQUE REFERENCES messages(message_id) ON DELETE CASCADE,
    content TEXT NOT NULL CHECK (length(trim(content)) > 0),
    delivery_mode TEXT NOT NULL CHECK (delivery_mode IN ('queued', 'interrupt')),
    PRIMARY KEY (run_id, idempotency_key)
);

INSERT INTO send_run_input_idempotencies SELECT * FROM kiln_0017_send_run_input_idempotencies;
DROP TABLE kiln_0017_send_run_input_idempotencies;

CREATE TABLE session_events (
    cursor INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    message_id TEXT REFERENCES messages(message_id) ON DELETE CASCADE,
    task_id TEXT REFERENCES tasks(task_id),
    run_id TEXT REFERENCES runs(run_id),
    parent_run_id TEXT REFERENCES runs(run_id),
    child_run_id TEXT REFERENCES runs(run_id),
    tool_call_id TEXT REFERENCES tool_calls(tool_call_id),
    approval_id TEXT,
    task_objective TEXT,
    task_state TEXT,
    parent_task_id TEXT,
    dependency_task_ids TEXT,
    assigned_run_id TEXT,
    run_state TEXT,
    user_input_mode TEXT,
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
    context_manifest_id TEXT REFERENCES context_manifests(context_manifest_id),
    model_invocation_id TEXT REFERENCES model_invocations(model_invocation_id),
    model_work_id TEXT,
    model_context_manifest_hash TEXT,
    model_provider_account_id TEXT,
    model_provider TEXT,
    model_model TEXT,
    model_generation_max_output_tokens INTEGER,
    model_reasoning_effort TEXT,
    model_capability_version TEXT,
    model_capability_tool_calls TEXT,
    model_capability_vision TEXT,
    model_capability_structured_output TEXT,
    model_purpose TEXT,
    model_retry_of TEXT REFERENCES model_invocations(model_invocation_id),
    model_invocation_state TEXT,
    model_completion_kind TEXT,
    model_terminal_reason TEXT,
    usage_observation_id TEXT REFERENCES usage_observations(observation_id),
    output_chunk_id TEXT REFERENCES model_output_chunks(output_chunk_id),
    CHECK (
        (output_chunk_id IS NULL AND (
        (usage_observation_id IS NULL AND (
        (context_manifest_id IS NULL AND model_invocation_id IS NULL AND (
            (event_type = 'session.created' AND message_id IS NULL AND task_id IS NULL AND run_id IS NULL AND parent_run_id IS NULL AND child_run_id IS NULL AND tool_call_id IS NULL AND approval_id IS NULL)
            OR (event_type = 'message.appended' AND message_id IS NOT NULL AND task_id IS NULL AND run_id IS NULL AND parent_run_id IS NULL AND child_run_id IS NULL AND tool_call_id IS NULL AND approval_id IS NULL)
            OR (event_type IN ('task.created', 'task.updated', 'task.state_changed') AND message_id IS NULL AND task_id IS NOT NULL AND run_id IS NULL AND parent_run_id IS NULL AND child_run_id IS NULL AND tool_call_id IS NULL AND approval_id IS NULL AND task_objective IS NOT NULL AND task_state IS NOT NULL AND dependency_task_ids IS NOT NULL)
            OR (event_type = 'task.assigned' AND message_id IS NULL AND task_id IS NOT NULL AND run_id IS NULL AND parent_run_id IS NULL AND child_run_id IS NULL AND tool_call_id IS NULL AND approval_id IS NULL AND task_objective IS NOT NULL AND task_state IS NOT NULL AND dependency_task_ids IS NOT NULL AND assigned_run_id IS NOT NULL)
            OR (event_type = 'run.created' AND message_id IS NULL AND run_id IS NOT NULL AND child_run_id IS NULL AND tool_call_id IS NULL AND approval_id IS NULL AND user_input_mode IS NOT NULL)
            OR (event_type IN ('run.queued', 'run.state_changed', 'run.cancellation_requested') AND message_id IS NULL AND task_id IS NULL AND run_id IS NOT NULL AND parent_run_id IS NULL AND child_run_id IS NULL AND tool_call_id IS NULL AND approval_id IS NULL)
            OR (event_type = 'run.child_added' AND message_id IS NULL AND task_id IS NULL AND run_id IS NULL AND parent_run_id IS NOT NULL AND child_run_id IS NOT NULL AND tool_call_id IS NULL AND approval_id IS NULL)
            OR (event_type IN ('run.input_queued', 'run.interrupt_requested', 'run.input_delivered', 'run.input_failed', 'run.input_cancelled') AND message_id IS NOT NULL AND task_id IS NULL AND run_id IS NOT NULL AND parent_run_id IS NULL AND child_run_id IS NULL AND tool_call_id IS NULL AND approval_id IS NULL)
            OR (event_type IN ('tool_call.requested', 'tool_call.state_changed', 'tool_call.denied') AND message_id IS NULL AND task_id IS NULL AND run_id IS NOT NULL AND parent_run_id IS NULL AND child_run_id IS NULL AND tool_call_id IS NOT NULL AND approval_id IS NULL)
            OR (event_type IN ('approval.requested', 'approval.decided') AND message_id IS NULL AND task_id IS NULL AND run_id IS NOT NULL AND parent_run_id IS NULL AND child_run_id IS NULL AND tool_call_id IS NOT NULL AND approval_id IS NOT NULL)
            OR (event_type = 'tool_call.output' AND message_id IS NULL AND task_id IS NULL AND run_id IS NOT NULL AND parent_run_id IS NULL AND child_run_id IS NULL AND tool_call_id IS NOT NULL AND approval_id IS NULL AND output_stream IS NOT NULL AND output_content IS NOT NULL AND artifact_hash IS NULL)
            OR (event_type = 'artifact.registered' AND message_id IS NULL AND task_id IS NULL AND run_id IS NOT NULL AND parent_run_id IS NULL AND child_run_id IS NULL AND tool_call_id IS NOT NULL AND approval_id IS NULL AND output_stream IS NOT NULL AND output_content IS NULL AND artifact_hash IS NOT NULL)
        ))
        OR (event_type = 'context.manifest_created' AND context_manifest_id IS NOT NULL
            AND model_invocation_id IS NULL
            AND message_id IS NULL AND task_id IS NULL AND run_id IS NOT NULL
            AND parent_run_id IS NULL AND child_run_id IS NULL
            AND tool_call_id IS NULL AND approval_id IS NULL)
        OR (event_type IN ('model_invocation.created', 'model_invocation.state_changed')
            AND context_manifest_id IS NOT NULL
            AND model_invocation_id IS NOT NULL
            AND model_work_id IS NOT NULL
            AND model_context_manifest_hash IS NOT NULL
            AND model_provider_account_id IS NOT NULL
            AND model_provider IS NOT NULL
            AND model_model IS NOT NULL
            AND model_capability_version IS NOT NULL
            AND model_capability_tool_calls IS NOT NULL
            AND model_capability_vision IS NOT NULL
            AND model_capability_structured_output IS NOT NULL
            AND model_purpose IS NOT NULL
            AND model_invocation_state IS NOT NULL
            AND message_id IS NULL AND task_id IS NULL AND run_id IS NOT NULL
            AND parent_run_id IS NULL AND child_run_id IS NULL
            AND tool_call_id IS NULL AND approval_id IS NULL)
        ))
        OR (event_type = 'usage.observed' AND usage_observation_id IS NOT NULL
            AND model_invocation_id IS NOT NULL AND run_id IS NOT NULL
            AND context_manifest_id IS NULL AND message_id IS NULL AND task_id IS NULL
            AND parent_run_id IS NULL AND child_run_id IS NULL
            AND tool_call_id IS NULL AND approval_id IS NULL)
        ))
        OR (event_type = 'model_invocation.output' AND output_chunk_id IS NOT NULL
            AND model_invocation_id IS NOT NULL AND run_id IS NOT NULL
            AND usage_observation_id IS NULL AND context_manifest_id IS NULL
            AND message_id IS NULL AND task_id IS NULL
            AND parent_run_id IS NULL AND child_run_id IS NULL
            AND tool_call_id IS NULL AND approval_id IS NULL)
    )
);

INSERT INTO session_events SELECT * FROM kiln_0017_session_events ORDER BY cursor;
DROP TABLE kiln_0017_session_events;

INSERT INTO sqlite_sequence (name, seq)
SELECT 'session_events', seq FROM kiln_0017_session_events_sequence
WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'session_events');
UPDATE sqlite_sequence
SET seq = MAX(seq, (SELECT seq FROM kiln_0017_session_events_sequence))
WHERE name = 'session_events'
  AND EXISTS (SELECT 1 FROM kiln_0017_session_events_sequence);
DROP TABLE kiln_0017_session_events_sequence;

CREATE INDEX session_events_session_cursor ON session_events (session_id, cursor);
CREATE INDEX session_events_message_id ON session_events (message_id);
CREATE INDEX session_events_task_id ON session_events (task_id);
CREATE INDEX session_events_run_id ON session_events (run_id);
CREATE INDEX session_events_assigned_run_id ON session_events (assigned_run_id);
CREATE INDEX session_events_parent_run_id ON session_events (parent_run_id);
CREATE INDEX session_events_child_run_id ON session_events (child_run_id);
CREATE INDEX session_events_tool_call_id ON session_events (tool_call_id);
CREATE INDEX session_events_artifact_hash ON session_events (artifact_hash);
CREATE INDEX session_events_context_manifest_id ON session_events (context_manifest_id);
CREATE INDEX session_events_model_invocation_id ON session_events (model_invocation_id);
CREATE UNIQUE INDEX session_events_usage_observation_id
ON session_events (usage_observation_id) WHERE usage_observation_id IS NOT NULL;
CREATE UNIQUE INDEX session_events_output_chunk_id
ON session_events (output_chunk_id) WHERE output_chunk_id IS NOT NULL;

CREATE TABLE message_deliveries (
    message_id TEXT PRIMARY KEY REFERENCES messages(message_id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    delivery_mode TEXT NOT NULL CHECK (delivery_mode IN ('queued', 'interrupt')),
    state TEXT NOT NULL CHECK (state IN ('queued', 'delivered', 'failed', 'cancelled')),
    queued_cursor INTEGER NOT NULL UNIQUE REFERENCES session_events(cursor)
);

INSERT INTO message_deliveries SELECT * FROM kiln_0017_message_deliveries;
DROP TABLE kiln_0017_message_deliveries;
CREATE INDEX message_deliveries_fifo ON message_deliveries (run_id, state, queued_cursor);
