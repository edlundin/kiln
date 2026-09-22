-- Raw credentials never enter SQLite. Retained tombstones prevent replay from
-- restoring a revoked credential, including after leaving/rejoining a group.
CREATE TABLE configuration_read_grants (
    credential_digest TEXT PRIMARY KEY NOT NULL CHECK (length(credential_digest) = 64 AND credential_digest NOT GLOB '*[^0-9a-f]*'),
    group_id TEXT NOT NULL REFERENCES configuration_authorities(group_id),
    master_instance_id TEXT NOT NULL,
    follower_instance_id TEXT NOT NULL CHECK (follower_instance_id != master_instance_id),
    issued_state_version INTEGER NOT NULL CHECK (issued_state_version > 0),
    revoked INTEGER NOT NULL DEFAULT 0 CHECK (revoked IN (0, 1))
);
CREATE INDEX configuration_read_grants_group ON configuration_read_grants(group_id) WHERE revoked = 0;
