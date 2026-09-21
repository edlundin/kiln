-- Add tagged tool-exchange entries without changing historical manifest hashes.
CREATE TEMP TABLE kiln_0030_context_manifest_entries AS
SELECT * FROM context_manifest_entries;

DROP TABLE context_manifest_entries;

CREATE TABLE context_manifest_entries (
    context_manifest_id TEXT NOT NULL
        REFERENCES context_manifests(context_manifest_id) ON DELETE CASCADE,
    position INTEGER NOT NULL CHECK (position >= 0),
    entry_kind TEXT NOT NULL
        CHECK (entry_kind IN ('instruction', 'message', 'child_activity', 'tool_exchange')),
    provenance TEXT NOT NULL
        CHECK (provenance IN (
            'runtime', 'user', 'workspace', 'run', 'session_message', 'child_activity', 'tool_exchange'
        )),
    workspace_root_id TEXT REFERENCES workspace_roots(workspace_root_id),
    source_run_id TEXT REFERENCES runs(run_id),
    source_event_id TEXT REFERENCES session_events(event_id),
    source_tool_call_id TEXT REFERENCES tool_calls(tool_call_id),
    message_id TEXT REFERENCES messages(message_id),
    message_role TEXT CHECK (message_role IN ('user', 'assistant')),
    content TEXT NOT NULL CHECK (length(CAST(content AS BLOB)) > 0),
    PRIMARY KEY (context_manifest_id, position),
    CHECK ((entry_kind = 'tool_exchange') = (source_tool_call_id IS NOT NULL)),
    CHECK (
        (entry_kind = 'instruction' AND provenance IN ('runtime', 'user')
            AND workspace_root_id IS NULL AND source_run_id IS NULL
            AND source_event_id IS NULL AND message_id IS NULL AND message_role IS NULL)
        OR (entry_kind = 'instruction' AND provenance = 'workspace'
            AND workspace_root_id IS NOT NULL AND source_run_id IS NULL
            AND source_event_id IS NULL AND message_id IS NULL AND message_role IS NULL)
        OR (entry_kind = 'instruction' AND provenance = 'run'
            AND workspace_root_id IS NULL AND source_run_id IS NOT NULL
            AND source_event_id IS NULL AND message_id IS NULL AND message_role IS NULL)
        OR (entry_kind = 'message' AND provenance = 'session_message'
            AND workspace_root_id IS NULL AND source_run_id IS NULL
            AND source_event_id IS NULL AND message_id IS NOT NULL AND message_role IS NOT NULL)
        OR (entry_kind = 'child_activity' AND provenance = 'child_activity'
            AND workspace_root_id IS NULL AND source_run_id IS NOT NULL
            AND source_event_id IS NOT NULL AND message_id IS NOT NULL AND message_role IS NULL)
        OR (entry_kind = 'tool_exchange' AND provenance = 'tool_exchange'
            AND workspace_root_id IS NULL AND source_run_id IS NOT NULL
            AND source_event_id IS NULL AND message_id IS NULL AND message_role IS NULL)
    )
);

INSERT INTO context_manifest_entries (
    context_manifest_id, position, entry_kind, provenance,
    workspace_root_id, source_run_id, source_event_id,
    message_id, message_role, content
)
SELECT
    context_manifest_id, position, entry_kind, provenance,
    workspace_root_id, source_run_id, source_event_id,
    message_id, message_role, content
FROM kiln_0030_context_manifest_entries;

DROP TABLE kiln_0030_context_manifest_entries;
