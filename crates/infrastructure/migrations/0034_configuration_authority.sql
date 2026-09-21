-- Authority history and observed watermarks survive leaving/rejoining a group.
-- A group cannot silently change master on this host.
CREATE TABLE configuration_authorities (
    group_id TEXT PRIMARY KEY NOT NULL,
    master_instance_id TEXT NOT NULL,
    observed_revision INTEGER CHECK (observed_revision > 0),
    observed_schema_version INTEGER CHECK (observed_schema_version > 0 AND observed_schema_version <= 4294967295),
    observed_content_hash TEXT CHECK (length(observed_content_hash) = 64 AND observed_content_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK (
        (observed_revision IS NULL AND observed_schema_version IS NULL AND observed_content_hash IS NULL)
        OR (observed_revision IS NOT NULL AND observed_schema_version IS NOT NULL AND observed_content_hash IS NOT NULL)
    )
);
CREATE TABLE configuration_instance (
    singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
    instance_id TEXT NOT NULL UNIQUE,
    version INTEGER NOT NULL CHECK (version > 0),
    role TEXT NOT NULL CHECK (role IN ('unassigned', 'master', 'follower')),
    group_id TEXT REFERENCES configuration_authorities(group_id),
    CHECK ((role = 'unassigned' AND group_id IS NULL) OR (role != 'unassigned' AND group_id IS NOT NULL))
);
