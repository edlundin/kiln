-- Public identity IDs are independent of both private-key vault references.
ALTER TABLE configuration_master_identities ADD COLUMN identity_id TEXT
    CHECK (
        identity_id IS NULL OR (
            length(identity_id) = 36
            AND substr(identity_id, 1, 4) = 'cmi_'
            AND substr(identity_id, 5) NOT GLOB '*[^0-9a-f]*'
        )
    );

-- Preserve each existing identity's public target without deriving it from a
-- SecretRef. The unique index fails migration rather than accepting a collision.
UPDATE configuration_master_identities
SET identity_id = 'cmi_' || lower(hex(randomblob(16)))
WHERE identity_id IS NULL;

CREATE UNIQUE INDEX configuration_master_identity_public_id
ON configuration_master_identities(identity_id);

CREATE TRIGGER configuration_master_identity_public_id_required
BEFORE INSERT ON configuration_master_identities
WHEN NEW.identity_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'configuration master identity ID is required');
END;

CREATE TRIGGER configuration_master_identity_public_id_immutable
BEFORE UPDATE OF identity_id ON configuration_master_identities
WHEN NEW.identity_id IS NULL OR NEW.identity_id != OLD.identity_id
BEGIN
    SELECT RAISE(ABORT, 'configuration master identity ID is immutable');
END;
