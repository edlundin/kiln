-- Receipt/lifecycle phase is historical metadata; it does not prove that a
-- follower credential remains active after explicit retirement or old cleanup.
-- Only still-live pre-approval reservations are safe to continue as pending.
CREATE TABLE configuration_follower_enrollment_credentials (
    attempt_id TEXT PRIMARY KEY NOT NULL
        REFERENCES configuration_follower_enrollment_requests(attempt_id),
    state TEXT NOT NULL CHECK (state IN ('pending', 'active', 'retired'))
);

INSERT INTO configuration_follower_enrollment_credentials (attempt_id, state)
SELECT r.attempt_id,
       CASE
           WHEN l.phase IN ('reserved', 'prepared')
                AND (o.result IS NULL OR o.result = 'pending')
           THEN 'pending'
           ELSE 'retired'
       END
FROM configuration_follower_enrollment_requests r
JOIN configuration_follower_enrollment_lifecycle l USING (attempt_id)
LEFT JOIN configuration_follower_enrollment_observations o USING (attempt_id);

CREATE TRIGGER configuration_follower_enrollment_credential_identity_immutable
BEFORE UPDATE ON configuration_follower_enrollment_credentials
WHEN NEW.attempt_id IS NOT OLD.attempt_id
BEGIN
    SELECT RAISE(ABORT, 'configuration follower enrollment credential binding is immutable');
END;

CREATE TRIGGER configuration_follower_enrollment_credential_state_monotonic
BEFORE UPDATE OF state ON configuration_follower_enrollment_credentials
WHEN NOT (
    NEW.state = OLD.state
    OR (OLD.state = 'pending' AND NEW.state IN ('active', 'retired'))
    OR (OLD.state = 'active' AND NEW.state = 'retired')
)
BEGIN
    SELECT RAISE(ABORT, 'configuration follower enrollment credential state cannot be restored');
END;

CREATE TRIGGER configuration_follower_enrollment_credential_activation
BEFORE UPDATE OF state ON configuration_follower_enrollment_credentials
WHEN NEW.state = 'active'
    AND NOT (
        OLD.state = 'pending'
        AND EXISTS (
            SELECT 1
            FROM configuration_follower_enrollment_requests r
            JOIN configuration_follower_enrollment_lifecycle l USING (attempt_id)
            JOIN configuration_follower_enrollment_observations o USING (attempt_id)
            JOIN configuration_instance i ON i.singleton = 1
            JOIN configuration_authorities a ON a.group_id = r.group_id
            WHERE r.attempt_id = NEW.attempt_id
                AND l.phase = 'retired'
                AND o.result = 'approved'
                AND i.role = 'follower'
                AND i.instance_id = r.follower_instance_id
                AND i.version = r.expected_state_version + 1
                AND i.group_id = r.group_id
                AND a.master_instance_id = r.master_instance_id
                AND json_extract(o.receipt_json, '$.phase') = 'approved'
                AND json_extract(o.receipt_json, '$.attempt_id') = r.attempt_id
                AND json_extract(o.receipt_json, '$.follower_id') = r.follower_instance_id
                AND json_extract(o.receipt_json, '$.group_id') = r.group_id
                AND json_extract(o.receipt_json, '$.master_instance_id') = r.master_instance_id
                AND json_extract(o.receipt_json, '$.grant.revoked') = 0
                AND json_extract(o.receipt_json, '$.grant.issuance_attempt_id') = r.attempt_id
                AND json_extract(o.receipt_json, '$.grant.group_id') = r.group_id
                AND json_extract(o.receipt_json, '$.grant.master_instance_id') = r.master_instance_id
                AND json_extract(o.receipt_json, '$.grant.follower_instance_id') = r.follower_instance_id
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'configuration follower enrollment credential activation is not approved');
END;

CREATE TRIGGER configuration_follower_enrollment_credential_retained
BEFORE DELETE ON configuration_follower_enrollment_credentials
BEGIN
    SELECT RAISE(ABORT, 'configuration follower enrollment credential state is retained');
END;
