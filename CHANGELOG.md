# Changelog

## 2026-08-31 — `feat`

- Added durable WebSocket reconnect replay after an exclusive cursor.
- Made start-run retry-safe with required `Idempotency-Key`, atomic SQLite storage, and no duplicate Run/subprocess.
- Added protocol 0.5.0 contracts and regression tests.

[Commit](pending)

## 2026-08-31 — `feat`

- Added the Rust headless core through EDL-211, including the workspace, sessions and messages, durable events, and deterministic subprocess Run/ToolCall lifecycle.
- Added generated HTTP and WebSocket contracts.
- Added architecture, specification, and learning documentation.
- Added the `kiln.pen` UI design artifact.

[Commit](../../commit/5da910f215d97a4d0124eacdb92213bd05c6b29d)
