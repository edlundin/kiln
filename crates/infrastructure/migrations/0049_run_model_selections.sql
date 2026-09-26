-- New Runs retain the effective local selection and only bounded provenance.
-- Local account IDs are opaque and contain no credential references or secrets.
CREATE TABLE run_model_selections (
    run_id TEXT PRIMARY KEY NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    provider_account_id TEXT NOT NULL,
    provider_type TEXT NOT NULL,
    model_id TEXT NOT NULL,
    max_output_tokens INTEGER CHECK (max_output_tokens IS NULL OR max_output_tokens > 0),
    reasoning_effort TEXT,
    capabilities_version TEXT NOT NULL,
    tool_calls TEXT NOT NULL CHECK (tool_calls IN ('supported', 'unsupported', 'unknown')),
    vision TEXT NOT NULL CHECK (vision IN ('supported', 'unsupported', 'unknown')),
    structured_output TEXT NOT NULL CHECK (structured_output IN ('supported', 'unsupported', 'unknown')),
    source TEXT NOT NULL CHECK (source IN ('host_default', 'shared_default')),
    configuration_group_id TEXT,
    configuration_revision INTEGER,
    configuration_schema_version INTEGER,
    configuration_content_hash TEXT,
    account_binding_key TEXT,
    account_binding_version INTEGER,
    CHECK (
        (source = 'host_default'
         AND configuration_group_id IS NULL
         AND configuration_revision IS NULL
         AND configuration_schema_version IS NULL
         AND configuration_content_hash IS NULL
         AND account_binding_key IS NULL
         AND account_binding_version IS NULL)
        OR
        (source = 'shared_default'
         AND configuration_group_id IS NOT NULL
         AND configuration_revision IS NOT NULL
         AND configuration_revision > 0
         AND configuration_schema_version IS NOT NULL
         AND configuration_schema_version > 0
         AND configuration_content_hash IS NOT NULL
         AND account_binding_key IS NOT NULL
         AND account_binding_version IS NOT NULL
         AND account_binding_version > 0)
    )
);
