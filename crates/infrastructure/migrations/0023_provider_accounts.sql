CREATE TABLE provider_accounts (
    provider_account_id TEXT PRIMARY KEY NOT NULL,
    provider_type TEXT NOT NULL CHECK (length(trim(provider_type)) > 0),
    label TEXT NOT NULL CHECK (
        length(CAST(label AS BLOB)) > 0
        AND length(CAST(label AS BLOB)) <= 256
        AND label = trim(label)
        AND instr(label, char(0)) = 0
        AND label NOT GLOB ('*['
            || char(1) || char(2) || char(3) || char(4) || char(5)
            || char(6) || char(7) || char(8) || char(9) || char(10)
            || char(11) || char(12) || char(13) || char(14) || char(15)
            || char(16) || char(17) || char(18) || char(19) || char(20)
            || char(21) || char(22) || char(23) || char(24) || char(25)
            || char(26) || char(27) || char(28) || char(29) || char(30)
            || char(31) || char(127) || ']*')
    ),
    provider_subject TEXT CHECK (
        provider_subject IS NULL
        OR (
            length(CAST(provider_subject AS BLOB)) > 0
            AND length(CAST(provider_subject AS BLOB)) <= 256
            AND provider_subject = trim(provider_subject)
            AND instr(provider_subject, char(0)) = 0
            AND provider_subject NOT GLOB ('*['
                || char(1) || char(2) || char(3) || char(4) || char(5)
                || char(6) || char(7) || char(8) || char(9) || char(10)
                || char(11) || char(12) || char(13) || char(14) || char(15)
                || char(16) || char(17) || char(18) || char(19) || char(20)
                || char(21) || char(22) || char(23) || char(24) || char(25)
                || char(26) || char(27) || char(28) || char(29) || char(30)
                || char(31) || char(127) || ']*')
        )
    ),
    secret_ref TEXT CHECK (
        secret_ref IS NULL
        OR (
            length(CAST(secret_ref AS BLOB)) = 30
            AND substr(secret_ref, 1, 4) = 'sec_'
            AND substr(secret_ref, 5) NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'
        )
    ),
    state TEXT NOT NULL CHECK (
        state IN ('connecting', 'connected', 'reauth_required', 'disconnected')
    ),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    last_used_at_unix_ms INTEGER CHECK (
        last_used_at_unix_ms IS NULL
        OR (
            last_used_at_unix_ms >= created_at_unix_ms
            AND last_used_at_unix_ms <= updated_at_unix_ms
        )
    ),
    capabilities_refreshed_at_unix_ms INTEGER CHECK (
        capabilities_refreshed_at_unix_ms IS NULL
        OR (
            capabilities_refreshed_at_unix_ms >= created_at_unix_ms
            AND capabilities_refreshed_at_unix_ms <= updated_at_unix_ms
        )
    ),
    metadata_json TEXT NOT NULL DEFAULT '{}'
        CHECK (json_valid(metadata_json) AND json_type(metadata_json) = 'object'),
    CHECK (state <> 'connected' OR secret_ref IS NOT NULL),
    CHECK (state <> 'disconnected' OR secret_ref IS NULL)
);

CREATE UNIQUE INDEX provider_accounts_one_active_per_provider
ON provider_accounts (provider_type)
WHERE state IN ('connecting', 'connected', 'reauth_required');

CREATE INDEX provider_accounts_provider_state
ON provider_accounts (provider_type, state);

CREATE TABLE provider_account_workspaces (
    provider_account_id TEXT NOT NULL REFERENCES provider_accounts(provider_account_id)
        ON DELETE CASCADE,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    PRIMARY KEY (provider_account_id, workspace_id)
);

CREATE INDEX provider_account_workspaces_workspace
ON provider_account_workspaces (workspace_id, provider_account_id);
