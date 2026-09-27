-- Preserve the parent table's name while restoring its rows so deferred
-- references from messages, deliveries and context entries resolve at commit.
-- No referencing foreign key uses ON DELETE CASCADE for session_events.
PRAGMA defer_foreign_keys = ON;
CREATE TEMP TABLE kiln_0060_events AS SELECT * FROM session_events;
CREATE TEMP TABLE kiln_0060_sequence AS SELECT seq FROM sqlite_sequence WHERE name = 'session_events';
DROP TABLE session_events;

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
    mcp_invocation_sequence INTEGER REFERENCES mcp_invocation_events(sequence),
    mcp_input_sequence INTEGER REFERENCES mcp_input_events(sequence),
    CHECK (
        (mcp_input_sequence IS NULL AND (
        (mcp_invocation_sequence IS NULL AND (
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
        ))
        OR (event_type = 'mcp.invocation_state_changed' AND mcp_invocation_sequence IS NOT NULL
            AND run_id IS NOT NULL AND tool_call_id IS NOT NULL
            AND message_id IS NULL AND task_id IS NULL
            AND parent_run_id IS NULL AND child_run_id IS NULL AND approval_id IS NULL
            AND context_manifest_id IS NULL AND model_invocation_id IS NULL
            AND usage_observation_id IS NULL AND output_chunk_id IS NULL
            AND stdout IS NULL AND stderr IS NULL AND output_content IS NULL
            AND artifact_hash IS NULL AND stdout_artifact_hash IS NULL AND stderr_artifact_hash IS NULL)
        ))
        OR (event_type = 'mcp.input_state_changed' AND mcp_input_sequence IS NOT NULL
            AND mcp_invocation_sequence IS NULL AND run_id IS NOT NULL AND tool_call_id IS NOT NULL
            AND message_id IS NULL AND task_id IS NULL
            AND parent_run_id IS NULL AND child_run_id IS NULL AND approval_id IS NULL
            AND context_manifest_id IS NULL AND model_invocation_id IS NULL
            AND usage_observation_id IS NULL AND output_chunk_id IS NULL
            AND stdout IS NULL AND stderr IS NULL AND output_content IS NULL
            AND artifact_hash IS NULL AND stdout_artifact_hash IS NULL AND stderr_artifact_hash IS NULL)
    )
);

INSERT INTO session_events (
    cursor, event_id, session_id, event_type, message_id, task_id, run_id, parent_run_id,
    child_run_id, tool_call_id, approval_id, task_objective, task_state, parent_task_id,
    dependency_task_ids, assigned_run_id, run_state, user_input_mode, tool_call_state,
    approval_state, approval_policy, requested_workspace_root_id, requested_relative_directory,
    effective_workspace_root_id, effective_relative_directory, capability, stdout, stderr,
    exit_code, output_stream, output_content, artifact_hash, stdout_artifact_hash,
    stderr_artifact_hash, context_manifest_id, model_invocation_id, model_work_id,
    model_context_manifest_hash, model_provider_account_id, model_provider, model_model,
    model_generation_max_output_tokens, model_reasoning_effort, model_capability_version,
    model_capability_tool_calls, model_capability_vision, model_capability_structured_output,
    model_purpose, model_retry_of, model_invocation_state, model_completion_kind,
    model_terminal_reason, usage_observation_id, output_chunk_id, mcp_invocation_sequence
)
SELECT
    cursor, event_id, session_id, event_type, message_id, task_id, run_id, parent_run_id,
    child_run_id, tool_call_id, approval_id, task_objective, task_state, parent_task_id,
    dependency_task_ids, assigned_run_id, run_state, user_input_mode, tool_call_state,
    approval_state, approval_policy, requested_workspace_root_id, requested_relative_directory,
    effective_workspace_root_id, effective_relative_directory, capability, stdout, stderr,
    exit_code, output_stream, output_content, artifact_hash, stdout_artifact_hash,
    stderr_artifact_hash, context_manifest_id, model_invocation_id, model_work_id,
    model_context_manifest_hash, model_provider_account_id, model_provider, model_model,
    model_generation_max_output_tokens, model_reasoning_effort, model_capability_version,
    model_capability_tool_calls, model_capability_vision, model_capability_structured_output,
    model_purpose, model_retry_of, model_invocation_state, model_completion_kind,
    model_terminal_reason, usage_observation_id, output_chunk_id, mcp_invocation_sequence
FROM kiln_0060_events ORDER BY cursor;
DROP TABLE kiln_0060_events;

INSERT INTO sqlite_sequence (name, seq)
SELECT 'session_events', seq FROM kiln_0060_sequence
WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'session_events');
UPDATE sqlite_sequence SET seq = MAX(seq, (SELECT seq FROM kiln_0060_sequence))
WHERE name = 'session_events' AND EXISTS (SELECT 1 FROM kiln_0060_sequence);
DROP TABLE kiln_0060_sequence;

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

CREATE UNIQUE INDEX session_events_mcp_invocation_sequence
ON session_events(mcp_invocation_sequence) WHERE mcp_invocation_sequence IS NOT NULL;

CREATE UNIQUE INDEX session_events_mcp_input_sequence
ON session_events(mcp_input_sequence) WHERE mcp_input_sequence IS NOT NULL;
