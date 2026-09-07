# Changelog

## 2026-09-07 — `feat`

- Added assistant Message finalization from stored chunks, preserving immutable text with Run and ModelInvocation origin.
- Eligible successful Runs now complete atomically with the assistant Message and its events; incomplete output remains for failed or cancelled Runs and is excluded from context.
- Complete assistant context is scoped to the same Run or the root-to-root scope; native daemon dispatch remains pending.

[Commit](pending)

## 2026-09-07 — `feat`

- Defined protocol `0.18.0` assistant Messages with an exposed `assistant` role, complete/incomplete status, and originating Run and ModelInvocation identifiers.
- Kept public Message append operations user-only.

[Commit](pending)

## 2026-09-07 — `feat`

- Added durable immutable assistant-text and exposed reasoning-summary chunks committed with their events, with contiguous per-attempt ordering and exact duplicate retries after terminal state.
- Partial output is retained and replayed; provider updates carry storage command identity, and the deterministic adapter generates stable positions and IDs.
- Final assistant Messages and daemon dispatch remain pending.

[Commit](pending)

## 2026-09-07 — `feat`

- Defined protocol `0.17.0` metadata-only `model_invocation.output` events with immutable chunk IDs, per-attempt ordered positions, `assistant_text`/`reasoning_summary` streams, and visible content.
- Updated the generated JSON Schema, TypeScript, and OpenAPI definitions.
- Final assistant assembly and daemon dispatch remain pending.

[Commit](pending)

## 2026-09-07 — `feat`

- Added the `kiln-providers` deterministic runtime adapter with exact `kiln_deterministic`/`deterministic_text` selection, configured text chunks and terminal outcome, and explicit synthetic counted usage and time.
- Cancellation discards pending output and returns final unknown usage; the adapter adds no external dependency, login, network access, or daemon activation.
- Native daemon orchestration and live adapters remain pending.

[Commit](pending)

## 2026-09-07 — `feat`

- Added the core native provider port, issuing non-clone requests only after a fresh durable claim with verified stored context.
- Added text, usage, and terminal update validation with typed errors; repeated and terminal claims do not dispatch.
- Native daemon orchestration, persisted streamed assistant output, tool payloads, and live authentication/transport remain pending.

[Commit](pending)

## 2026-09-07 — `feat`

- Added atomic final usage and invocation outcome recording, with the usage event emitted before the terminal event; errors roll both back together and retries remain idempotent.
- Separate completion now requires final usage for dispatched attempts, while pending cancellation and existing terminal retries retain their behavior.
- Provider orchestration and automatic unknown terminal observations remain pending.

[Commit](pending)

## 2026-09-07 — `feat`

- Added normalized counted-unit provider usage updates validated against the dispatched invocation, account, and logical work.
- Added durable per-attempt immutable usage revisions with deduplicated delta and cumulative updates, final and correction handling, safe replay metadata, and canonical cached/reasoning subsets that preserve missing versus zero values.
- Explicit provider orchestration, automatic terminal observations, aggregate endpoints, valuation, and allowance remain pending.

[Commit](pending)

## 2026-09-07 — `feat`

- Defined protocol `0.16.0` metadata-only `usage.observed` events identifying the immutable observation revision, physical invocation attempt, logical work, account, completeness, and terminal status.
- Updated the generated JSON Schema, TypeScript, and OpenAPI definitions.

[Commit](pending)

## 2026-09-07 — `feat`

- Added internal durable ModelInvocation attempt records with verified same-Run context manifests, immutable settings, guarded queued Run claims, distinct retry attempts with stable work identity, and replayed metadata events.
- Added Run cancellation and terminal guards, including cancellation of undispatched pending attempts.
- Provider transport, streaming, usage accounting, and automatic in-flight restart reconciliation remain pending.

[Commit](pending)

## 2026-09-07 — `feat`

- Defined protocol `0.15.0` metadata-only `model_invocation.created` and `model_invocation.state_changed` events, including retry work identity and typed terminal status.
- Updated the generated JSON Schema, TypeScript, and OpenAPI definitions.

[Commit](pending)

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
