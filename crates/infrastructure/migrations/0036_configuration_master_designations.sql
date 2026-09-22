-- A designation receipt outlives later role changes; replay never changes role.
CREATE TABLE configuration_master_designations (
    idempotency_key TEXT PRIMARY KEY NOT NULL CHECK (length(idempotency_key) > 0),
    instance_id TEXT NOT NULL REFERENCES configuration_instance(instance_id),
    expected_version INTEGER NOT NULL CHECK (expected_version > 0 AND expected_version < 9223372036854775807),
    group_id TEXT NOT NULL UNIQUE REFERENCES configuration_authorities(group_id)
);
