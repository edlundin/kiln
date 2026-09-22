# Changelog

## 2026-09-22 — `feat`

- Expose authenticated read-only configuration authority and revision status through protocol 0.26.0 and the Rust client.

EDL-322: https://linear.app/edlundin/issue/EDL-322

[Commit](pending)

## 2026-09-22 — `feat`

- Store and restore complete shared configuration snapshots atomically, preserving master authority, follower revisions, and coherent settings/MCP/skill content.

EDL-322: https://linear.app/edlundin/issue/EDL-322

[Commit](pending)

## 2026-09-22 — `feat`

- Validate coherent shared snapshots of model preferences, global MCP definitions, and skills with canonical hashes and dependency checks.

EDL-322: https://linear.app/edlundin/issue/EDL-322

[Commit](pending)

## 2026-09-22 — `feat`

- Validate bounded portable global skill packages and canonical content hashes before synchronization or filesystem materialization.

EDL-322: https://linear.app/edlundin/issue/EDL-322

[Commit](pending)

## 2026-09-22 — `feat`

- Persist stable Kiln instance identity, explicit configuration roles, and master revision history with stale-enrollment protection.

EDL-322: https://linear.app/edlundin/issue/EDL-322

[Commit](pending)

## 2026-09-22 — `feat`

- Define master-instance configuration synchronization authority, scope, enrollment, offline behavior, and revision checks for shared settings, global MCP, and global skills.

EDL-322: https://linear.app/edlundin/issue/EDL-322

[Commit](pending)

## 2026-09-22 — `feat`

- Add opt-in native public API Runs with an explicit account/model/capability selection and caller-defined context, transport, and response budgets.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-22 — `fix`

- Fail native Runs cleanly when credential authorization or replay-version checks reject a model claim before dispatch.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-22 — `feat`

- Composed the public API model-provider adapter with verified input preparation before pinned credential access and lazy HTTP dispatch.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-22 — `fix`

- Preserve the pending Responses HTTP send across daemon notification polls so dropping an update future neither interrupts nor resends the request.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Added a fixed-endpoint public Responses HTTP operation using pinned credentials, bounded streaming, explicit deadlines, and cancellation that preserves known usage without resending requests.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Pin credential versions atomically with model claims and reject replay/retry across rotations without exposing secret references in model Events or requests.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `fix`

- Require the expected credential version during provider vault lookup so reconnect or refresh cannot silently substitute credentials.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Reconcile Responses stream identities, deltas, completed items, and terminal output before releasing buffered generation results at clean EOF.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `fix`

- Preserve valid final usage from failed, incomplete, and provider-cancelled Responses without adopting partial tools or replay.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Added bounded incremental Responses SSE framing with payload-safe diagnostics and explicit malformed/truncated stream failures.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Added bounded successful Responses normalization that derives output, inert tool proposals, final usage, and private replay from one validated envelope.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Added bounded public Responses request serialization with ordered replay, matched tool results, preserved provenance, and explicit attachment support.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Bound private provider replay into context manifests and added verified, bounded continuation loading with same-Run/account/model checks.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Routed provider continuation updates through atomic terminal storage and native cancellation/error handling while keeping replay payloads private.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Added invocation-bound private continuation storage committed atomically with model completion, final usage and tool proposals, with bounded reads and integrity checks.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Added bounded Responses output replay data that preserves encrypted reasoning and assistant phase for future native tool continuation; durable binding and live transport remain pending.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `fix`

- Registered daemon shutdown signals before readiness, closing a startup race that could bypass graceful cleanup on immediate SIGTERM.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Connected native tool proposals to sequential approval, fresh-claim file execution, cancellation, and ordered result context, with explicit opt-in byte limits. Live provider activation remains pending.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `fix`

- Discarded buffered tool proposals after native provider cancellation while retaining final usage, preventing stopped work from blocking a steered generation.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `fix`

- Allowed native Run failure and recovery after verified terminal ToolCalls, while retaining active tool work for explicit reconciliation without redispatch.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Added a scoped UTF-8 file-read tool with local argument validation, descriptor-based traversal, bounded output, and artifact storage for larger results. Native coordinator activation remains pending.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Added durable tool-exchange context snapshots and ordered provider projection with source validation, bounded output-artifact loading, and preserved historical manifest hashes.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Added verified terminal tool-exchange projections that retain call provenance, canonical arguments, outcomes, and output metadata for subsequent model context.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Added fresh native ToolCall execution claims that bind typed commands to approved scope and prevent duplicate or restarted work from being dispatched again.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Added atomic native ToolCall completion and partial-output cancellation that retain results without ending the Run, with bounded inline output and exact terminal retries.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Added durable sequential adoption of model proposals into scoped Kiln ToolCalls and approvals, preventing duplicate adoption, skipped proposals, and accidental dispatch through the subprocess fixture.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Added whole-batch model tool resolution against frozen offered definitions and exact local capability revisions, with typed argument validation and no execution authority.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Froze bounded, ordered tool descriptions and schema documents per model invocation, preserving tool definitions across retries and exposing the snapshot at the provider boundary.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Connected finalized tool-request payloads to provider stream persistence and daemon terminal handling, retaining final usage while discarding proposals from invalid streams.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Persisted ordered, bounded model tool proposals atomically with final usage and invocation completion, preserving exact retries without granting tool execution authority.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Added ordered provider-context assembly with explicit byte budgets, source-bound attachment payloads, hash verification, and whole-request failure for missing or invalid artifacts.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Added versioned context-manifest attachment snapshots that bind source messages, artifact metadata, and attachment order into the durable hash while preserving historical manifests.

EDL-311: https://linear.app/edlundin/issue/EDL-311

[Commit](pending)

## 2026-09-21 — `feat`

- Added browser PKCE sign-in to the authenticated daemon API, Rust client, and desktop Settings, sharing cancellation and credential lifecycle with device sign-in and retaining an explicit device fallback.

EDL-309: https://linear.app/edlundin/issue/EDL-309

[Commit](pending)

## 2026-09-21 — `feat`

- Added the native ChatGPT browser PKCE adapter with an expiring loopback callback, state validation, cancellation, and private one-shot credential exchange.

EDL-309: https://linear.app/edlundin/issue/EDL-309

[Commit](pending)

## 2026-09-21 — `feat`

- Added explicit local stdin-only OpenAI API credential import with an account-bound vault envelope, active-account replacement protection, and a saved-but-unverified account label in Settings.

EDL-309: https://linear.app/edlundin/issue/EDL-309

[Commit](pending)

## 2026-09-21 — `fix`

- Journaled provider credential cleanup before vault writes and atomically with credential replacement, allowing disconnect to recover unpublished or retired entries after restart without deleting the current credential.

EDL-309: https://linear.app/edlundin/issue/EDL-309

[Commit](pending)

## 2026-09-21 — `feat`

- Added authenticated local provider-account disconnect through protocol `0.24.0`, the Rust client, and confirmed desktop Settings controls, including explicit cleanup retry and disconnect-then-sign-in recovery.

EDL-309: https://linear.app/edlundin/issue/EDL-309

[Commit](pending)

## 2026-09-21 — `fix`

- Made provider-account disconnect disable credential use before vault deletion and retain its durable reference until cleanup succeeds, so interrupted disconnects can be retried after restart.

EDL-309: https://linear.app/edlundin/issue/EDL-309

[Commit](pending)

## 2026-09-21 — `fix`

- Fixed Codex sign-in retries to reuse disconnected accounts, preserve terminal results and unresolved credential-cleanup failures, and avoid blocking account status during cancellation or replacement.

EDL-309: https://linear.app/edlundin/issue/EDL-309

[Commit](pending)

## 2026-09-21 — `feat`

- Added global desktop Settings for provider accounts with Codex device sign-in, safe account summaries, manual status checks, and exact-attempt cancellation.

EDL-309: https://linear.app/edlundin/issue/EDL-309

[Commit](pending)

## 2026-09-21 — `feat`

- Added the authenticated provider-account create/list/get API with durable idempotency and the public Codex device-login start, status, and cancellation continuation through protocol `0.23.0`, including opaque attempt IDs and retained terminal states.

EDL-309: https://linear.app/edlundin/issue/EDL-309


[Commit](pending)

## 2026-09-21 — `feat`

- Added the native Codex device-code login continuation with bounded polling, credential validation, coordinator-owned cancellation and joining, conditional account cleanup, and daemon shutdown integration.

EDL-309: https://linear.app/edlundin/issue/EDL-309

[Commit](pending)

## 2026-09-21 — `feat`

- Added the internal Codex subscription credential envelope and bounded HTTPS refresh adapter with provider/account binding, optional-token rotation, and typed reauthentication outcomes.

EDL-309: https://linear.app/edlundin/issue/EDL-309

[Commit](pending)


## 2026-09-21 — `feat`

- Added serialized provider credential refresh with successful-version coalescing, durable rotation before waiter release, and explicit reauthentication state after permanent rejection.

EDL-309: https://linear.app/edlundin/issue/EDL-309

[Commit](pending)


## 2026-09-21 — `feat`

- Added bounded composer attachment upload and durable attachment references across the desktop, protocol, client, server, core, infrastructure, and storage migration.
- Repaired attachment-only persistence and replay across append, targeted run input, child reactions, and context manifests; empty content without attachments remains rejected, and artifact uploads remain capped at 64 MiB.
- Added the provider registry slice and deterministic native provider wiring with explicit provider and model metadata.

[Commit](pending)


## 2026-09-21 — `feat`

- Added the EDL-309 provider-account metadata and OS-backed secret-store boundary with typed lifecycle state, opaque secret references, serialized rotation, and recoverable cleanup handling.

EDL-309: https://linear.app/edlundin/issue/EDL-309

[Commit](pending)


## 2026-09-16 — `feat`

- Added the authenticated, bounded selected-file diff flow for the desktop Changes inspector through protocol `0.22.0`, including explicit unsupported and truncation states.

[Commit](pending)


## 2026-09-16 — `feat`

- Added an authenticated Session checkout changes summary and a bounded read-only Usage ledger API with keyset pagination over the latest revision for each physical invocation.
- Added a desktop Usage ledger view and refreshed generated protocol artifacts for protocol `0.21.0`.

[Commit](pending)


## 2026-09-14 — `feat`

- Added bounded desktop artifact inspection for stored transcript artifacts, with authenticated 64 KiB previews for UTF-8 text, JSON, and XML, download-only handling for unsupported content, retries, and stale-session guards.

EDL-249: https://linear.app/edlundin/issue/EDL-249

[Commit](pending)



## 2026-09-13 — `chore`

- Ignore generated Graphify output and Serena cache/memory directories while leaving reusable Serena project configuration visible for review.

EDL-288: https://linear.app/edlundin/issue/EDL-288


[Commit](pending)


## 2026-09-13 — `feat`

- Added deterministic workspace and session browsing through protocol `0.20.0`, authenticated server routes, Rust client methods, and the native desktop. Saved session replay and reconnect restoration preserve session-scoped drafts, reactions, pending submissions, child starts, and guidance drafts.

EDL-288: https://linear.app/edlundin/issue/EDL-288


[Commit](pending)


## 2026-09-13 — `fix`

- Repaired the EDL-287 child-tree black-box sequence so the root remains pending until descendants are terminal, then verifies replay, restart, approval, and cancellation states.

EDL-287: https://linear.app/edlundin/issue/EDL-287


[Commit](pending)


## 2026-09-12 — `test`

- Completed EDL-216's repeatable real-daemon vertical-slice proof across path rejection, approval, reconnect/replay, restart, artifacts, cancellation, and recovery.
- Recorded current build and runtime measurements, and documented the existing child-tree stall for EDL-287 follow-up.

EDL-216: https://linear.app/edlundin/issue/EDL-216

[Commit](pending)

## 2026-09-12 — `fix`

- Fixed EDL-249 subprocess completion, failure, preflight failure, and approval denial so they wait for non-terminal descendants outside the commit lock.
- Atomic store guards now prevent parent termination while descendants run; cancellation remains handled.

[Commit](pending)

## 2026-09-12 — `feat`

- Added desktop child-activity reactions for EDL-249: select a child transcript update without sending, show and clear durable Run/Event references in the root composer, and preserve the original root and reference through submission, retry, and replay.

[Commit](../../commit/5280dace1babee8e0097b19e6a15026440d1921d)

## 2026-09-12 — `feat`

- Added immutable child-activity ContextManifest snapshots for EDL-249, capturing only the selected event while retaining root reaction provenance.
- Snapshots exclude full child transcript and artifact bytes, and preserve older hashes.

[Commit](../../commit/a5e77a8193594a1f90abab922d27bd44ca65ea53)

## 2026-09-12 — `feat`

- Added durable root references to child Run activity for EDL-249, with typed Message links and queued reaction commands.
- SQLite references use paired nullable foreign keys with transactional idempotency and validation of same-session event ownership and ancestry.
- Message replay and queued input hydration preserve the references while plain SendRunInput remains compatible.

[Commit](../../commit/e47650cf5d9dee4c0e6175f854900132a89826da)

## 2026-09-12 — `build`

- Packaged the native desktop as a debug macOS Kiln.app for EDL-249, with application metadata and documented launch steps; the daemon remains separate.

[Commit](../../commit/d176eba2f5aedb2825855734d0b79e62cc33f1f1)

## 2026-09-12 — `feat`

- Added native GPUI connection and Session restore for EDL-249, with root conversation and approvals.
- Added a temporary hierarchical Run drawer, separate child guidance drafts with exact retries, durable status and activity attribution, and Task progress.
- Escape returns to the root composer.

[Commit](../../commit/cda9753a1e0a44d8071acbad273dbc30e32fc5e8)

## 2026-09-12 — `feat`

- Added shared kiln-client HTTP and WebSocket transport for EDL-249, with loopback bearer authentication, protocol negotiation, session and Run operations, and artifact downloads.
- Child creation is idempotent; retries, replay, and client operations guidance are included.
- HTTP redirects and proxies are disabled to keep the local credential on the loopback endpoint.

[Commit](../../commit/adc86368a7dd0be2022660f74190f16520bf06d1)

## 2026-09-08 — `feat`

- Added an exclusive data-directory lock before store open; startup reconciles known deterministic fixture attempts without redispatch, preserving final usage and counts.
- Reconciliation completes stored-success Runs or retains incomplete assistant Messages, processes descendants first, and leaves foreign ownership for explicit reconciliation.
- Provider-error cleanup now consumes remaining valid terminal usage before falling back to unknown usage.

[Commit](pending)

## 2026-09-08 — `feat`

- Added deterministic native daemon execution selected explicitly with `KILN_RUN_EXECUTOR=deterministic-model`, using stored context and claims with durable output and usage persistence.
- Wired assistant finalization, FIFO guidance, interrupt handling, and cancellation/failed output retention into the execution path.
- The default subprocess executor remains unchanged.

[Commit](pending)

## 2026-09-08 — `feat`

- Added the native Run failure boundary, atomically failing Running Runs without ToolCalls after generation attempts complete and descendants are terminal.
- Duplicate failed requests now return the existing Run without creating new events.

[Commit](pending)

## 2026-09-08 — `feat`

- Added `ProviderApplication` validation of provider updates against stored invocation identity before routing output, partial usage, and terminal outcome/final usage to the existing atomic stores.
- Provider application now returns committed mutations and events while preserving existing duplicate handling.
- Native daemon dispatch remains pending.

[Commit](pending)

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
