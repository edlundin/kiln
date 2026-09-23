-- The request identity is immutable and contains no bearer bytes. Mutable
-- recovery state lives separately and is permanently retired, never deleted.
CREATE TABLE configuration_follower_enrollment_requests (
    attempt_id TEXT PRIMARY KEY NOT NULL CHECK (
        length(attempt_id) = 36
        AND substr(attempt_id, 1, 4) = 'cra_'
        AND substr(attempt_id, 5) NOT GLOB '*[^0-9a-f]*'
    ),
    follower_instance_id TEXT NOT NULL,
    expected_state_version INTEGER NOT NULL CHECK (expected_state_version > 0),
    group_id TEXT NOT NULL,
    master_instance_id TEXT NOT NULL,
    server_name TEXT NOT NULL,
    ca_der BLOB NOT NULL CHECK (length(ca_der) BETWEEN 1 AND 16777215),
    ca_fingerprint TEXT NOT NULL CHECK (
        length(ca_fingerprint) = 64
        AND ca_fingerprint NOT GLOB '*[^0-9a-f]*'
    ),
    secret_ref TEXT NOT NULL UNIQUE CHECK (
        length(secret_ref) = 30
        AND substr(secret_ref, 1, 4) = 'sec_'
        AND substr(secret_ref, 5, 1) BETWEEN '0' AND '7'
        AND substr(secret_ref, 5) NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'
    ),
    credential_digest TEXT NOT NULL CHECK (
        length(credential_digest) = 64
        AND credential_digest NOT GLOB '*[^0-9a-f]*'
    ),
    CHECK (follower_instance_id != master_instance_id)
);

CREATE TABLE configuration_follower_enrollment_lifecycle (
    attempt_id TEXT PRIMARY KEY NOT NULL
        REFERENCES configuration_follower_enrollment_requests(attempt_id),
    follower_instance_id TEXT NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('reserved', 'prepared', 'retired'))
);

CREATE UNIQUE INDEX configuration_follower_enrollment_one_live_per_follower
ON configuration_follower_enrollment_lifecycle(follower_instance_id)
WHERE phase != 'retired';

CREATE TRIGGER configuration_follower_enrollment_lifecycle_owner
BEFORE INSERT ON configuration_follower_enrollment_lifecycle
WHEN NEW.follower_instance_id != (
    SELECT follower_instance_id
    FROM configuration_follower_enrollment_requests
    WHERE attempt_id = NEW.attempt_id
)
BEGIN
    SELECT RAISE(ABORT, 'configuration follower enrollment owner mismatch');
END;

CREATE TRIGGER configuration_follower_enrollment_request_immutable
BEFORE UPDATE ON configuration_follower_enrollment_requests
WHEN NEW.attempt_id IS NOT OLD.attempt_id
    OR NEW.follower_instance_id IS NOT OLD.follower_instance_id
    OR NEW.expected_state_version IS NOT OLD.expected_state_version
    OR NEW.group_id IS NOT OLD.group_id
    OR NEW.master_instance_id IS NOT OLD.master_instance_id
    OR NEW.server_name IS NOT OLD.server_name
    OR NEW.ca_der IS NOT OLD.ca_der
    OR NEW.ca_fingerprint IS NOT OLD.ca_fingerprint
    OR NEW.secret_ref IS NOT OLD.secret_ref
    OR NEW.credential_digest IS NOT OLD.credential_digest
BEGIN
    SELECT RAISE(ABORT, 'configuration follower enrollment request is immutable');
END;

CREATE TRIGGER configuration_follower_enrollment_request_retained
BEFORE DELETE ON configuration_follower_enrollment_requests
BEGIN
    SELECT RAISE(ABORT, 'configuration follower enrollment request is retained');
END;

CREATE TRIGGER configuration_follower_enrollment_lifecycle_immutable
BEFORE UPDATE ON configuration_follower_enrollment_lifecycle
WHEN NEW.attempt_id IS NOT OLD.attempt_id
    OR NEW.follower_instance_id IS NOT OLD.follower_instance_id
BEGIN
    SELECT RAISE(ABORT, 'configuration follower enrollment lifecycle binding is immutable');
END;

CREATE TRIGGER configuration_follower_enrollment_lifecycle_monotonic
BEFORE UPDATE OF phase ON configuration_follower_enrollment_lifecycle
WHEN NOT (
    NEW.phase = OLD.phase
    OR (OLD.phase = 'reserved' AND NEW.phase IN ('prepared', 'retired'))
    OR (OLD.phase = 'prepared' AND NEW.phase = 'retired')
)
BEGIN
    SELECT RAISE(ABORT, 'configuration follower enrollment phase cannot be restored');
END;

CREATE TRIGGER configuration_follower_enrollment_lifecycle_retained
BEFORE DELETE ON configuration_follower_enrollment_lifecycle
BEGIN
    SELECT RAISE(ABORT, 'configuration follower enrollment lifecycle is retained');
END;
