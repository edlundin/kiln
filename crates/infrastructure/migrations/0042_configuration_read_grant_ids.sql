-- Stable public IDs and durable request attempts let local administration
-- recover metadata after an uncertain response without retaining bearer bytes.
ALTER TABLE configuration_read_grants ADD COLUMN grant_id TEXT
    CHECK (
        grant_id IS NULL OR (
            length(grant_id) = 36
            AND substr(grant_id, 1, 4) = 'crg_'
            AND substr(grant_id, 5) NOT GLOB '*[^0-9a-f]*'
        )
    );
ALTER TABLE configuration_read_grants ADD COLUMN issuance_attempt_id TEXT
    CHECK (
        issuance_attempt_id IS NULL OR (
            length(issuance_attempt_id) = 36
            AND substr(issuance_attempt_id, 1, 4) = 'cra_'
            AND substr(issuance_attempt_id, 5) NOT GLOB '*[^0-9a-f]*'
        )
    );
-- Existing digests remain usable and revocation-safe; they receive stable
-- metadata IDs but no fabricated issuance-attempt history.
UPDATE configuration_read_grants
SET grant_id = 'crg_' || lower(hex(randomblob(16)))
WHERE grant_id IS NULL;

CREATE UNIQUE INDEX configuration_read_grant_public_id
ON configuration_read_grants(grant_id);
CREATE UNIQUE INDEX configuration_read_grant_attempt_id
ON configuration_read_grants(issuance_attempt_id)
WHERE issuance_attempt_id IS NOT NULL;
CREATE INDEX configuration_read_grants_master_cursor
ON configuration_read_grants(master_instance_id, grant_id);
-- Legacy rows can contain multiple active grants per follower. New attempts
-- are unique; issuance requires explicit local revocation before another grant
-- exists, so legacy rows are not silently revoked by this migration.
CREATE UNIQUE INDEX configuration_read_grants_active_attempt_follower
ON configuration_read_grants(group_id, follower_instance_id)
WHERE revoked = 0 AND issuance_attempt_id IS NOT NULL;

CREATE TRIGGER configuration_read_grant_public_id_required
BEFORE INSERT ON configuration_read_grants
WHEN NEW.grant_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'configuration read grant ID is required');
END;

CREATE TRIGGER configuration_read_grant_attempt_id_required
BEFORE INSERT ON configuration_read_grants
WHEN NEW.issuance_attempt_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'configuration read grant attempt ID is required');
END;

CREATE TRIGGER configuration_read_grant_binding_immutable
BEFORE UPDATE OF credential_digest, group_id, master_instance_id,
    follower_instance_id, issued_state_version, grant_id,
    issuance_attempt_id ON configuration_read_grants
WHEN NEW.credential_digest != OLD.credential_digest
    OR NEW.group_id != OLD.group_id
    OR NEW.master_instance_id != OLD.master_instance_id
    OR NEW.follower_instance_id != OLD.follower_instance_id
    OR NEW.issued_state_version != OLD.issued_state_version
    OR NEW.grant_id IS NULL OR NEW.grant_id != OLD.grant_id
    OR NEW.issuance_attempt_id IS NULL
    OR NEW.issuance_attempt_id != OLD.issuance_attempt_id
BEGIN
    SELECT RAISE(ABORT, 'configuration read grant binding is immutable');
END;

CREATE TRIGGER configuration_read_grant_revocation_monotonic
BEFORE UPDATE OF revoked ON configuration_read_grants
WHEN OLD.revoked = 1 AND NEW.revoked = 0
BEGIN
    SELECT RAISE(ABORT, 'configuration read grant revocation is permanent');
END;
