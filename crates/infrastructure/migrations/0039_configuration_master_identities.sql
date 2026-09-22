-- Keys stay in the OS vault. Immutable envelope hashes bind recovery to the
-- originally generated material. Retained rows permanently reserve both refs.
CREATE TABLE configuration_master_identities (
    ca_ref TEXT PRIMARY KEY NOT NULL,
    tls_ref TEXT NOT NULL UNIQUE CHECK (tls_ref != ca_ref),
    group_id TEXT NOT NULL REFERENCES configuration_authorities(group_id),
    master_instance_id TEXT NOT NULL,
    reserved_state_version INTEGER NOT NULL CHECK (reserved_state_version > 0),
    server_name TEXT NOT NULL,
    not_before INTEGER NOT NULL,
    leaf_not_after INTEGER NOT NULL CHECK (leaf_not_after > not_before),
    ca_not_after INTEGER NOT NULL CHECK (ca_not_after >= leaf_not_after),
    ca_der BLOB NOT NULL CHECK (length(ca_der) > 0),
    tls_der BLOB NOT NULL CHECK (length(tls_der) > 0),
    ca_key_hash TEXT NOT NULL CHECK (length(ca_key_hash) = 64 AND ca_key_hash NOT GLOB '*[^0-9a-f]*'),
    tls_key_hash TEXT NOT NULL CHECK (length(tls_key_hash) = 64 AND tls_key_hash NOT GLOB '*[^0-9a-f]*'),
    status TEXT NOT NULL CHECK (status IN ('pending', 'active', 'retired'))
);
-- Setup/rotation cannot silently replace an existing identity or pending work.
CREATE UNIQUE INDEX configuration_master_identity_live
ON configuration_master_identities(group_id) WHERE status != 'retired';
