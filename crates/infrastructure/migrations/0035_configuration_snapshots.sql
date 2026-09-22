-- One complete active payload per authority. Replacing it and advancing the
-- instance CAS version happen atomically; other authority groups stay retained.
CREATE TABLE configuration_snapshots (
    group_id TEXT PRIMARY KEY NOT NULL REFERENCES configuration_authorities(group_id),
    revision INTEGER NOT NULL CHECK (revision > 0),
    schema_version INTEGER NOT NULL CHECK (schema_version > 0 AND schema_version <= 4294967295),
    content_hash TEXT NOT NULL CHECK (length(content_hash) = 64 AND content_hash NOT GLOB '*[^0-9a-f]*'),
    metadata_json TEXT NOT NULL
);
CREATE TABLE configuration_skill_packages (
    group_id TEXT NOT NULL REFERENCES configuration_snapshots(group_id) ON DELETE CASCADE,
    skill_id TEXT NOT NULL,
    version TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    dependencies_json TEXT NOT NULL,
    content_hash TEXT NOT NULL CHECK (length(content_hash) = 64 AND content_hash NOT GLOB '*[^0-9a-f]*'),
    PRIMARY KEY (group_id, skill_id)
);
CREATE TABLE configuration_skill_files (
    group_id TEXT NOT NULL,
    skill_id TEXT NOT NULL,
    path TEXT NOT NULL,
    content BLOB NOT NULL CHECK (typeof(content) = 'blob'),
    content_hash TEXT NOT NULL CHECK (length(content_hash) = 64 AND content_hash NOT GLOB '*[^0-9a-f]*'),
    PRIMARY KEY (group_id, skill_id, path),
    FOREIGN KEY (group_id, skill_id) REFERENCES configuration_skill_packages(group_id, skill_id) ON DELETE CASCADE
);
