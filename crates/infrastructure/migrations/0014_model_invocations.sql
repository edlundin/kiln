CREATE TABLE model_invocations (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    model_invocation_id TEXT NOT NULL UNIQUE,
    work_id TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    context_manifest_id TEXT NOT NULL REFERENCES context_manifests(context_manifest_id),
    context_manifest_hash TEXT NOT NULL CHECK (
        length(context_manifest_hash) = 64
        AND context_manifest_hash NOT GLOB '*[^0-9a-f]*'
    ),
    provider_account_id TEXT NOT NULL CHECK (length(provider_account_id) > 0),
    provider TEXT NOT NULL CHECK (length(provider) > 0),
    model TEXT NOT NULL CHECK (length(model) > 0),
    generation_max_output_tokens INTEGER CHECK (generation_max_output_tokens > 0),
    reasoning_effort TEXT,
    capability_version TEXT NOT NULL CHECK (length(capability_version) > 0),
    capability_tool_calls TEXT NOT NULL
        CHECK (capability_tool_calls IN ('supported', 'unsupported', 'unknown')),
    capability_vision TEXT NOT NULL
        CHECK (capability_vision IN ('supported', 'unsupported', 'unknown')),
    capability_structured_output TEXT NOT NULL
        CHECK (capability_structured_output IN ('supported', 'unsupported', 'unknown')),
    purpose TEXT NOT NULL CHECK (purpose IN ('generation', 'compaction')),
    retry_of TEXT REFERENCES model_invocations(model_invocation_id),
    state TEXT NOT NULL
        CHECK (state IN ('pending', 'in_flight', 'completed', 'failed', 'cancelled', 'interrupted')),
    completion_kind TEXT CHECK (completion_kind IN ('assistant_output', 'tool_requests')),
    terminal_reason TEXT CHECK (
        terminal_reason IN ('completed', 'provider_error', 'invalid_request',
                            'cancelled', 'interrupted', 'unknown')
    ),
    CHECK (
        (state IN ('pending', 'in_flight') AND completion_kind IS NULL AND terminal_reason IS NULL)
        OR (state = 'completed' AND completion_kind IS NOT NULL AND terminal_reason IS NOT NULL
            AND terminal_reason = 'completed')
        OR (state = 'failed' AND completion_kind IS NULL AND terminal_reason IS NOT NULL
            AND terminal_reason NOT IN ('completed', 'cancelled', 'interrupted'))
        OR (state = 'cancelled' AND completion_kind IS NULL AND terminal_reason IS NOT NULL
            AND terminal_reason = 'cancelled')
        OR (state = 'interrupted' AND completion_kind IS NULL AND terminal_reason IS NOT NULL
            AND terminal_reason = 'interrupted')
    )
);

CREATE INDEX model_invocations_run_sequence
ON model_invocations (run_id, sequence);

CREATE UNIQUE INDEX model_invocations_one_root_per_work
ON model_invocations (work_id)
WHERE retry_of IS NULL;

CREATE UNIQUE INDEX model_invocations_one_active_per_run
ON model_invocations (run_id)
WHERE state IN ('pending', 'in_flight');

CREATE TABLE create_model_invocation_idempotencies (
    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) > 0),
    request BLOB NOT NULL CHECK (length(request) > 0),
    model_invocation_id TEXT NOT NULL UNIQUE
        REFERENCES model_invocations(model_invocation_id),
    PRIMARY KEY (run_id, idempotency_key)
);

CREATE TEMP TABLE kiln_0014_message_deliveries AS
SELECT message_id, run_id, delivery_mode, state, queued_cursor
FROM message_deliveries;

DROP TABLE message_deliveries;

CREATE TEMP TABLE kiln_0014_session_events_sequence (seq INTEGER NOT NULL);

INSERT INTO kiln_0014_session_events_sequence (seq)
SELECT seq FROM sqlite_sequence WHERE name = 'session_events';

CREATE TABLE session_events_new (
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
    CHECK (
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
    )
);

INSERT INTO session_events_new (
    cursor, event_id, session_id, event_type, message_id, task_id, run_id,
    parent_run_id, child_run_id, tool_call_id, approval_id, task_objective,
    task_state, parent_task_id, dependency_task_ids, assigned_run_id,
    run_state, user_input_mode, tool_call_state, approval_state,
    approval_policy, requested_workspace_root_id, requested_relative_directory,
    effective_workspace_root_id, effective_relative_directory, capability,
    stdout, stderr, exit_code, output_stream, output_content, artifact_hash,
    stdout_artifact_hash, stderr_artifact_hash, context_manifest_id
)
SELECT
    cursor, event_id, session_id, event_type, message_id, task_id, run_id,
    parent_run_id, child_run_id, tool_call_id, approval_id, task_objective,
    task_state, parent_task_id, dependency_task_ids, assigned_run_id,
    run_state, user_input_mode, tool_call_state, approval_state,
    approval_policy, requested_workspace_root_id, requested_relative_directory,
    effective_workspace_root_id, effective_relative_directory, capability,
    stdout, stderr, exit_code, output_stream, output_content, artifact_hash,
    stdout_artifact_hash, stderr_artifact_hash, context_manifest_id
FROM session_events
ORDER BY cursor;

DROP TABLE session_events;
ALTER TABLE session_events_new RENAME TO session_events;

INSERT INTO sqlite_sequence (name, seq)
SELECT 'session_events', seq FROM kiln_0014_session_events_sequence
WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'session_events');
UPDATE sqlite_sequence
SET seq = MAX(seq, (SELECT seq FROM kiln_0014_session_events_sequence))
WHERE name = 'session_events'
  AND EXISTS (SELECT 1 FROM kiln_0014_session_events_sequence);
DROP TABLE kiln_0014_session_events_sequence;

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

CREATE TABLE message_deliveries (
    message_id TEXT PRIMARY KEY REFERENCES messages(message_id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    delivery_mode TEXT NOT NULL CHECK (delivery_mode IN ('queued', 'interrupt')),
    state TEXT NOT NULL CHECK (state IN ('queued', 'delivered', 'failed', 'cancelled')),
    queued_cursor INTEGER NOT NULL UNIQUE REFERENCES session_events(cursor)
);

INSERT INTO message_deliveries (message_id, run_id, delivery_mode, state, queued_cursor)
SELECT message_id, run_id, delivery_mode, state, queued_cursor
FROM kiln_0014_message_deliveries;

DROP TABLE kiln_0014_message_deliveries;

CREATE INDEX message_deliveries_fifo
ON message_deliveries (run_id, state, queued_cursor);
