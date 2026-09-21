-- One durable local ToolCall per finalized provider call. Arguments remain in
-- the immutable proposal; this link never turns a provider name into authority.
CREATE TABLE model_tool_adoptions (
    model_invocation_id TEXT NOT NULL,
    provider_call_id TEXT NOT NULL,
    tool_call_id TEXT NOT NULL UNIQUE REFERENCES tool_calls(tool_call_id),
    PRIMARY KEY (model_invocation_id, provider_call_id),
    FOREIGN KEY (model_invocation_id, provider_call_id)
        REFERENCES model_tool_requests(model_invocation_id, provider_call_id)
);
