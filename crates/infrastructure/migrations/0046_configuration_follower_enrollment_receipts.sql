-- Master responses are historical observations. They may be refreshed only
-- while pending; terminal local outcomes and their receipt never change.
CREATE TABLE configuration_follower_enrollment_observations (
    attempt_id TEXT PRIMARY KEY NOT NULL
        REFERENCES configuration_follower_enrollment_requests(attempt_id),
    result TEXT NOT NULL CHECK (
        result IN ('pending', 'approved', 'rejected', 'revoked', 'role_conflict')
    ),
    receipt_json TEXT CHECK (
        receipt_json IS NULL
        OR (json_valid(receipt_json) AND length(CAST(receipt_json AS BLOB)) <= 4096)
    ),
    CHECK (result IN ('pending', 'role_conflict') OR receipt_json IS NOT NULL)
);

CREATE TRIGGER configuration_follower_enrollment_observation_state
BEFORE INSERT ON configuration_follower_enrollment_observations
WHEN (NEW.result = 'pending' AND (
        SELECT phase FROM configuration_follower_enrollment_lifecycle
        WHERE attempt_id = NEW.attempt_id
    ) != 'prepared')
    OR (NEW.result != 'pending' AND (
        SELECT phase FROM configuration_follower_enrollment_lifecycle
        WHERE attempt_id = NEW.attempt_id
    ) != 'retired')
BEGIN
    SELECT RAISE(ABORT, 'configuration follower enrollment observation has invalid local phase');
END;

CREATE TRIGGER configuration_follower_enrollment_observation_monotonic
BEFORE UPDATE ON configuration_follower_enrollment_observations
WHEN NEW.attempt_id IS NOT OLD.attempt_id
    OR NOT (
        (OLD.result = 'pending' AND NEW.result IN (
            'pending', 'approved', 'rejected', 'revoked', 'role_conflict'
        ))
        OR (
            OLD.result != 'pending'
            AND NEW.result = OLD.result
            AND NEW.receipt_json IS OLD.receipt_json
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'configuration follower enrollment observation cannot be restored');
END;

CREATE TRIGGER configuration_follower_enrollment_observation_retained
BEFORE DELETE ON configuration_follower_enrollment_observations
BEGIN
    SELECT RAISE(ABORT, 'configuration follower enrollment observation is retained');
END;
