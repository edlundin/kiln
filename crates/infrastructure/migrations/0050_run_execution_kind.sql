-- Null preserves that the execution kind of pre-migration Runs is unknown.
-- New Runs set this in the same transaction that inserts the Run.
ALTER TABLE runs ADD COLUMN execution_kind TEXT
    CHECK (execution_kind IS NULL OR execution_kind IN ('native_model', 'subprocess'));
