-- Admission counts every retained request for one authority, including
-- terminal requests. Keep the bounded transactional count indexed.
CREATE INDEX configuration_follower_enrollment_request_authority
ON configuration_follower_enrollment_requests(group_id, master_instance_id);
