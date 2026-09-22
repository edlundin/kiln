-- Command retries reuse the original identity journal, including tombstones.
-- NULL preserves identities created through the lower-level explicit-ref API.
ALTER TABLE configuration_master_identities ADD COLUMN request_key TEXT
    CHECK (request_key IS NULL OR length(request_key) > 0);
CREATE UNIQUE INDEX configuration_identity_request_key
ON configuration_master_identities(request_key) WHERE request_key IS NOT NULL;
