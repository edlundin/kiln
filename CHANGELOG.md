# Changelog

## 2026-09-05 — `feat`

- Added durable immutable ContextManifest snapshots with exact instruction/Message ordering, provenance and hashes, same-Session and delivered-targeted Message validation, and retry-safe storage/replay metadata.
- Added the protocol `0.14.0` metadata-only `context.manifest_created` event; provider calls, ModelInvocation claims, native loop/context building, and input consumption remain out of scope.

[Commit](pending)

## 2026-09-05 — `feat`

- Added targeted Run cancellation for a selected Run and its descendants while preserving the parent and siblings when a child is selected.
- Waits for owned execution and descendants before entering cancelled state, and cancels queued targeted input with durable replay events.
- Scheduler/provider/unknown-process restart reconciliation remains out of scope.

[Commit](pending)

## 2026-09-05 — `feat`

- Added the storage/API boundary for immutable targeted user Messages, queued and explicit interrupt requests, and durable FIFO delivery state with retry and recovery semantics.
- Added the HTTP Run input endpoint in protocol version `0.13.0`; provider delivery and scheduler execution remain out of scope.

[Commit](pending)

## 2026-09-04 — `feat`

- Added durable parent/child Runs with immutable input mode and optional atomic Task assignment.
- Added root/child queued and child-added events with replay and restart recovery.
- Added child-start and Session Run-tree HTTP APIs in protocol version `0.12.0`.

[Commit](pending)

## 2026-09-04 — `feat`

- Added idempotent Task-to-Run assignment for active Runs in the same Session.
- Enforced one-to-one Task and Run assignment with durable `task.assigned` recovery.
- Published the assignment endpoint and protocol version `0.11.0`.

[Commit](pending)

## 2026-09-04 — `feat`

- Added idempotent Task objective and dependency updates.
- Added guarded Task lifecycle transitions with dependency and cycle validation.
- Added durable lifecycle events, restart recovery, and protocol version `0.10.0`.

[Commit](pending)

## 2026-09-04 — `feat`

- Added durable Task create/get HTTP operations with idempotent creation.
- Added same-Session parent and dependency validation, plus streamed and replayable `task.created` events.
- Added SQLite restart recovery and the generated protocol contract for Tasks.

[Commit](pending)

## 2026-09-04 — `test`

- Added a complete real-daemon vertical-slice acceptance scenario across multi-root Workspace, Session/Message persistence, approval ordering, WebSocket reconnect/replay, cancellation and process-group termination, artifact retrieval, and graceful restart.
- Recorded repeatable build, memory, and timing baselines, with Rust learning evidence.

[Commit](pending)

## 2026-09-04 — `feat`

- Added content-addressed storage for tool output larger than 4,096 bytes.
- Added durable artifact metadata, deduplication, and verified restart recovery.
- Added protocol 0.9.0 with `artifact.registered` events and authenticated artifact downloads.
- Added black-box coverage for artifact creation, fetch, missing content, deduplication, and restart.

[Commit](pending)

## 2026-09-02 — `feat`

- Added persistent loopback HTTP and WebSocket authentication with exact Host and Origin validation.
- Added protocol 0.8.0, workspace-root-scoped deterministic tool execution, and Ask/read_only/full_access approval policies.
- Added durable approval decisions with restart and cancellation behavior.
- Persisted filesystem identity rejects replaced workspace roots.
- Made invalid approved scope a durable failed ToolCall and Run.
- Made concurrent approval decisions first-writer-wins.
- Made Unix cancellation avoid post-reap process-group-ID probes and retry interrupted kill calls.

[Commit](pending)

## 2026-08-31 — `feat`

- Added durable, naturally idempotent `POST /v1/runs/{run_id}/cancel`.
- Persisted Run cancellation states, cancelled ToolCall state, and cancellation events.
- Added Unix process-group termination with captured output and completion-versus-cancel ordering.
- Added graceful SIGINT/SIGTERM shutdown that quiesces mutations, drains accepted commands, cancels active work, and waits for durable terminal state.
- Added protocol 0.6.0 contracts, a SQLite migration, and regression and black-box tests.

[Commit](pending)

## 2026-08-31 — `feat`

- Added durable WebSocket reconnect replay after an exclusive cursor.
- Made start-run retry-safe with required `Idempotency-Key`, atomic SQLite storage, and no duplicate Run/subprocess.
- Added protocol 0.5.0 contracts and regression tests.

[Commit](../../commit/faa9006855ba6dec6a655094586c585d1f4318f5)

## 2026-08-31 — `feat`

- Added the Rust headless core through EDL-211, including the workspace, sessions and messages, durable events, and deterministic subprocess Run/ToolCall lifecycle.
- Added generated HTTP and WebSocket contracts.
- Added architecture, specification, and learning documentation.
- Added the `kiln.pen` UI design artifact.

[Commit](../../commit/5da910f215d97a4d0124eacdb92213bd05c6b29d)
