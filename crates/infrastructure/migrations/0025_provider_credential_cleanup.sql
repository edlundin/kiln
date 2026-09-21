-- Only opaque references are journaled; credential bytes remain in the OS vault.
-- A reservation precedes vault writes. Account publication consumes it and
-- journals a retired reference atomically, so cleanup can resume after restart.
CREATE TABLE provider_account_secret_cleanup (
    secret_ref TEXT PRIMARY KEY NOT NULL CHECK (
        length(CAST(secret_ref AS BLOB)) = 30
        AND substr(secret_ref, 1, 4) = 'sec_'
        AND substr(secret_ref, 5) NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'
    ),
    provider_account_id TEXT NOT NULL REFERENCES provider_accounts(provider_account_id)
        ON DELETE RESTRICT
);

CREATE INDEX provider_account_secret_cleanup_account
ON provider_account_secret_cleanup (provider_account_id, secret_ref);
