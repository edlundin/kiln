-- Inert provider proposals, committed together with terminal invocation/usage.
-- Local ToolCall creation, schema validation, policy, and dispatch are separate.
CREATE TABLE model_tool_request_batches (
    model_invocation_id TEXT PRIMARY KEY NOT NULL
        REFERENCES model_invocations(model_invocation_id),
    content_hash TEXT NOT NULL CHECK (
        length(content_hash) = 64 AND content_hash NOT GLOB '*[^0-9a-f]*'
    ),
    request_count INTEGER NOT NULL CHECK (request_count > 0)
);

CREATE TABLE model_tool_requests (
    model_invocation_id TEXT NOT NULL
        REFERENCES model_tool_request_batches(model_invocation_id) ON DELETE CASCADE,
    position INTEGER NOT NULL CHECK (position >= 0),
    provider_call_id TEXT NOT NULL CHECK (length(provider_call_id) > 0),
    name TEXT NOT NULL CHECK (length(name) > 0),
    arguments_json TEXT NOT NULL CHECK (json_valid(arguments_json) AND json_type(arguments_json) = 'object'),
    PRIMARY KEY (model_invocation_id, position),
    UNIQUE (model_invocation_id, provider_call_id)
);
