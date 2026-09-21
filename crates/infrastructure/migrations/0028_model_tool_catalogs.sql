-- Frozen before dispatch. Missing historical catalogs do not authorize tools.
CREATE TABLE model_tool_catalogs (
    model_invocation_id TEXT PRIMARY KEY NOT NULL
        REFERENCES model_invocations(model_invocation_id),
    content_hash TEXT NOT NULL CHECK (
        length(content_hash) = 64 AND content_hash NOT GLOB '*[^0-9a-f]*'
    ),
    tool_count INTEGER NOT NULL CHECK (tool_count >= 0)
);

CREATE TABLE model_tool_definitions (
    model_invocation_id TEXT NOT NULL
        REFERENCES model_tool_catalogs(model_invocation_id) ON DELETE CASCADE,
    position INTEGER NOT NULL CHECK (position >= 0),
    definition_json TEXT NOT NULL CHECK (
        json_valid(definition_json) AND json_type(definition_json) = 'object'
    ),
    PRIMARY KEY (model_invocation_id, position)
);
