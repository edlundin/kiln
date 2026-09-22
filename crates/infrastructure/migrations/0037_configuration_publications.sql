-- Immutable publication receipts survive payload replacement and role changes.
-- They retain metadata only, not historical configuration/skill bytes.
CREATE TABLE configuration_publications (
    idempotency_key TEXT PRIMARY KEY NOT NULL CHECK (length(idempotency_key) > 0),
    instance_id TEXT NOT NULL REFERENCES configuration_instance(instance_id),
    group_id TEXT NOT NULL REFERENCES configuration_authorities(group_id),
    expected_version INTEGER NOT NULL CHECK (expected_version > 0 AND expected_version < 9223372036854775807),
    revision INTEGER NOT NULL CHECK (revision > 0),
    schema_version INTEGER NOT NULL CHECK (schema_version > 0 AND schema_version <= 4294967295),
    content_hash TEXT NOT NULL CHECK (length(content_hash) = 64 AND content_hash NOT GLOB '*[^0-9a-f]*'),
    UNIQUE (group_id, revision)
);
