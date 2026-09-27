-- Incoming follower IDs and endpoint trust are claims, not peer identity.
-- Do not impose one-pending-request-per-follower uniqueness: an unauthenticated
-- claimant must not be able to reserve another instance's identity.
CREATE TABLE configuration_master_enrollment_requests (
    request_id TEXT PRIMARY KEY NOT NULL CHECK (
        length(request_id) = 36
        AND substr(request_id, 1, 4) = 'cfr_'
        AND substr(request_id, 5) NOT GLOB '*[^0-9a-f]*'
    ),
    attempt_id TEXT NOT NULL UNIQUE CHECK (
        length(attempt_id) = 36
        AND substr(attempt_id, 1, 4) = 'cra_'
        AND substr(attempt_id, 5) NOT GLOB '*[^0-9a-f]*'
    ),
    follower_instance_id TEXT NOT NULL,
    follower_state_version INTEGER NOT NULL CHECK (follower_state_version > 0),
    group_id TEXT NOT NULL REFERENCES configuration_authorities(group_id),
    master_instance_id TEXT NOT NULL,
    server_name TEXT NOT NULL,
    master_ca_fingerprint TEXT NOT NULL CHECK (
        length(master_ca_fingerprint) = 64
        AND master_ca_fingerprint NOT GLOB '*[^0-9a-f]*'
    ),
    credential_digest TEXT NOT NULL CHECK (
        length(credential_digest) = 64
        AND credential_digest NOT GLOB '*[^0-9a-f]*'
    ),
    received_master_state_version INTEGER NOT NULL CHECK (received_master_state_version > 0),
    CHECK (follower_instance_id != master_instance_id)
);

CREATE TABLE configuration_follower_enrollment_request_lifecycle (
    request_id TEXT PRIMARY KEY NOT NULL
        REFERENCES configuration_master_enrollment_requests(request_id),
    phase TEXT NOT NULL CHECK (phase IN ('pending', 'approved', 'rejected')),
    grant_id TEXT UNIQUE REFERENCES configuration_read_grants(grant_id),
    CHECK ((phase = 'approved') = (grant_id IS NOT NULL))
);

CREATE TRIGGER configuration_master_enrollment_request_immutable
BEFORE UPDATE ON configuration_master_enrollment_requests
BEGIN
    SELECT RAISE(ABORT, 'configuration follower enrollment request is immutable');
END;

CREATE TRIGGER configuration_master_enrollment_request_retained
BEFORE DELETE ON configuration_master_enrollment_requests
BEGIN
    SELECT RAISE(ABORT, 'configuration follower enrollment request is retained');
END;

CREATE TRIGGER configuration_follower_enrollment_request_lifecycle_transition
BEFORE UPDATE ON configuration_follower_enrollment_request_lifecycle
WHEN NEW.request_id IS NOT OLD.request_id
    OR NOT (
        (NEW.phase = OLD.phase AND NEW.grant_id IS OLD.grant_id)
        OR (OLD.phase = 'pending' AND NEW.phase = 'approved'
            AND OLD.grant_id IS NULL AND NEW.grant_id IS NOT NULL)
        OR (OLD.phase = 'pending' AND NEW.phase = 'rejected'
            AND OLD.grant_id IS NULL AND NEW.grant_id IS NULL)
    )
BEGIN
    SELECT RAISE(ABORT, 'configuration follower enrollment decision is permanent');
END;

CREATE TRIGGER configuration_follower_enrollment_request_lifecycle_retained
BEFORE DELETE ON configuration_follower_enrollment_request_lifecycle
BEGIN
    SELECT RAISE(ABORT, 'configuration follower enrollment lifecycle is retained');
END;
