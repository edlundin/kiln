# EDL-213 backend rollout

## Purpose and scope

Deploy the EDL-213 backend change from protocol 0.5.0 to 0.6.0. The change adds
durable Run cancellation and graceful daemon shutdown.

When the 0.6.0 daemon opens its SQLite store, it applies migration
`0005_run_cancellation.sql` automatically. The migration rebuilds `runs`,
`tool_calls`, `session_events`, and `start_run_idempotencies`. It adds the
`cancelling` and `cancelled` states and the `run.cancellation_requested` event.
The database file is `kiln.sqlite3` in Kiln's platform data directory.

## Pre-deployment checks

1. Confirm the target release reports protocol version 0.6.0.
2. Confirm that the Rust stable toolchain meets the repository requirement:
   Rust 1.98 or later.
3. Identify Kiln's platform data directory. Do not assume an operating-system
   path.
4. Stop the daemon that serves protocol 0.5.0 with SIGINT or SIGTERM. Do not
   use an HTTP shutdown route.
5. Confirm that the daemon has stopped.
6. Back up the complete Kiln data directory while the daemon is stopped. Keep
   the backup separate from the directory that the 0.6.0 daemon will open.

## Deployment

1. Start the approved daemon build that serves protocol 0.6.0 with the existing
   Kiln platform data directory.
2. On store open, let the daemon apply migration `0005_run_cancellation.sql`.
3. Confirm that the daemon reports readiness and protocol version 0.6.0.
4. Do not start a daemon that implements protocol 0.5.0 against this migrated
   data directory.

## Verification

Run these checks from the repository with the stable Rust toolchain selected:

```sh
cargo test -p kiln-core
cargo test -p kiln-infrastructure
cargo test -p kiln-daemon --test black_box
```

The checks prove Run state transitions and repeated cancellation, atomic SQLite
cancellation and migration behavior, and daemon cancellation plus SIGINT and
SIGTERM shutdown behavior. The migration check preserves foreign keys,
idempotency rows, active-root uniqueness, and the `session_events`
AUTOINCREMENT high-water mark.

After deployment, request cancellation for an active Run. Confirm that the
Run reaches the durable `cancelled` state. Then send SIGINT or SIGTERM to the
daemon. Confirm that it stops accepting mutating commands, drains accepted
commands, cancels active Runs, and exits only after terminal Run state is
durable. Read-only requests can remain available during the drain.

## Rollback

Migration rollback is restore-only.

1. Stop the daemon that serves protocol 0.6.0 with SIGINT or SIGTERM.
2. Preserve the failed data directory for diagnosis.
3. Restore the complete pre-migration Kiln data directory backup.
4. Start the approved prior daemon build against the restored directory.

If no pre-migration backup exists, do not attempt an in-place downgrade.

## Failure handling and escalation

Escalate when the migration does not complete, readiness does not report
protocol 0.6.0, a verification command fails, a Run does not reach a durable
terminal state, or graceful shutdown reports an execution or terminal-state
persistence failure.

For a shutdown failure, the daemon leaves the process and storage open. Force
stop is the recovery boundary. Preserve the data directory for diagnosis
before recovery. Do not start the prior daemon against a 0.6.0-migrated
database.
