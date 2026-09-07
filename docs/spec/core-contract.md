# Kiln core contract

Status: draft implementation baseline

This document defines the behavior owned by `crates/core`. It is normative for
the first product release. HTTP, database, provider, plugin, MCP, terminal, Git,
and UI details are outside this contract.

## 1. Core responsibility

The core owns:

- domain identifiers and records;
- command validation and authorization decisions;
- run and task scheduling policy;
- state-machine transitions;
- parent and child run rules;
- permission and approval policy;
- durable event creation;
- idempotency decisions; and
- restart recovery decisions.

The core does not own:

- HTTP or WebSocket types;
- SQL queries, migrations, or filesystem layouts;
- provider SDK values or network clients;
- JSON-RPC or MCP framing;
- operating-system process or PTY APIs; or
- client rendering and local view state.

The public protocol converts into core commands at the server boundary. An
adapter converts core ports into SQLite, filesystem, provider, plugin, MCP,
process, and terminal operations.

## 2. Contract language

`MUST` is required for conformance. `MUST NOT` is forbidden. `MAY` is optional.
An operation is a command when it can change durable state. It is a query when
it only reads accepted state.

All identifiers are UUIDs, are opaque to clients, and remain stable across
daemon restart. A client MUST NOT derive an entity type, time, owner, or
storage location from an identifier.

## 3. Aggregate boundaries

| Aggregate | Owns | Consistency boundary |
| --- | --- | --- |
| Project | project metadata and workspace-root membership | one project command |
| Thread | messages, runs, tasks, tool calls, approvals, and ordered events | one accepted thread command |
| Artifact | immutable metadata keyed by content hash | one artifact registration |
| Extension registry | agents, providers, plugin installations, MCP servers, and grants | one registry command |

A child run and its parent MUST belong to the same thread. A task assigned to a
run MUST belong to that thread. Cross-thread parent, task, message-target, tool,
and approval links are invalid.

## 4. Command acceptance

Every command follows this sequence:

1. Authenticate the caller at the transport boundary.
2. Convert the request into core identifiers and values.
3. Load the required aggregate version.
4. Validate existence, state, ownership, relationships, and capability scope.
5. Compute state changes and durable events without external side effects.
6. Commit state changes, events, idempotency result, and outbox work in one
   storage transaction.
7. Return the committed result.
8. Dispatch committed outbox work to external adapters.

The core MUST NOT start a provider, process, plugin, MCP call, terminal, or Git
operation before step 6 commits. A failed commit produces no accepted command
and no external side effect.

An external result enters the core as a new command. The result command commits
its state and events with the same sequence.

## 5. Idempotency

Commands marked `required` need an idempotency key from the public protocol.
The key scope is `(caller, operation, owning aggregate)`. The stored result also
includes a hash of the normalized command input.

- The same key and same input MUST return the first committed result.
- The same key and different input MUST return `idempotency_conflict`.
- A failed transaction MUST NOT reserve the key.
- Replay MUST NOT start another external side effect.
- Retention duration is an adapter policy and must be measured before a limit
  is selected.

## 6. Command catalogue

### Projects and roots

| Command | Idempotency | Required behavior | Durable events |
| --- | --- | --- | --- |
| `CreateProject` | required | Create one project with at least one authorized local root | `project.created`, `workspace_root.added` |
| `UpdateProject` | required | Change mutable project metadata without changing identity | `project.updated` |
| `AddWorkspaceRoot` | required | Canonicalize and authorize the root before membership is committed | `workspace_root.added` |
| `RemoveWorkspaceRoot` | required | Reject removal while accepted active work depends on the root | `workspace_root.removed` |

A root location is adapter-owned data. The core stores its stable identity,
kind, display metadata, and effective permission scope. It does not resolve
paths.

### Threads, messages, and artifacts

| Command | Idempotency | Required behavior | Durable events |
| --- | --- | --- | --- |
| `CreateThread` | required | Create a thread in one project | `thread.created` |
| `UpdateThread` | required | Change title or archive state | `thread.updated` |
| `RegisterArtifact` | required | Accept immutable metadata after bytes pass adapter validation | `artifact.registered` |
| `AppendMessage` | required | Append user-visible content and existing artifact references | `message.appended` |

Messages are immutable after append. A correction is another message. An
artifact display name MUST NOT become a trusted path. `AppendMessage` creates
only an untargeted message. `SendRunInput` is the only command that creates a
message targeted to one run in the same thread.

### Tasks and subagents

| Command | Idempotency | Required behavior | Durable events |
| --- | --- | --- | --- |
| `CreateTask` | required | Add one task after cycle and dependency validation | `task.created` |
| `UpdateTask` | required | Change mutable objective or dependency data while allowed by state | `task.updated` |
| `AssignTask` | required | Assign one eligible run in the same thread | `task.assigned` |
| `TransitionTask` | required | Apply one valid task transition | `task.state_changed` |

Task parent links and dependency links MUST be acyclic. Parent links control
presentation. Dependency links control readiness. A task can have at most one
active assigned run.

### Runs and guidance

| Command | Idempotency | Required behavior | Durable events |
| --- | --- | --- | --- |
| `StartRun` | required | Create a root run and committed provider work | `run.created`, `run.queued` |
| `StartChildRun` | required | Create one child in the same Session with one parent and optional Task assignment | `run.created`, `run.queued`, `run.child_added`, and optional `task.assigned` |
| `ClaimRun` | required | Move queued work to running and record an execution claim | `run.state_changed` |
| `SendRunInput` | required | Append targeted guidance with queued or explicit interrupt delivery | `message.appended`, `run.input_queued` or `run.interrupt_requested` |
| `RecordRunInputDelivery` | required | Record delivery, failure, or cancellation for one targeted message | `run.input_delivered`, `run.input_failed`, or `run.input_cancelled` |
| `RequestRunCancellation` | required | Cancel queued work or request cancellation of active work | `run.cancellation_requested`, `run.state_changed` |
| `RecordProviderUpdate` | required | Convert one provider update into validated message, tool-call, usage, or run events | validated protocol events only |
| `FinishRun` | required | Commit one terminal result and release its claim | `run.state_changed` |

Root Runs use immutable `interactive` user input mode. Child Runs store one immutable
`parent_run_id`, optional `task_id`, and `interactive` or `read_only` user input mode.
`GET /v1/sessions/{session_id}/runs` returns the durable flat list whose parent IDs form
the Session Run tree.

`SendRunInput` accepts only non-empty content for an immutable `interactive`
Run in `queued`, `running`, or `waiting_for_approval` state. Normal guidance
uses `queued`; interrupt delivery must be explicit. The targeted Message,
queued MessageDelivery, idempotency result, `message.appended`, and the matching
`run.input_queued` or `run.interrupt_requested` Event commit atomically. An exact
retry returns the first MessageDelivery without new Events, including after the
Run becomes terminal. A mismatched key reuse conflicts.

`RecordRunInputDelivery` applies only to the oldest queued delivery for that
Run when it records `delivered` or `failed`. An already recorded exact outcome
is idempotent; a different later outcome is invalid. A new `delivered` outcome
is invalid after Run termination. A `cancelled` outcome requires a terminal
Run. The durable query for pending runtime work returns the oldest queued
delivery per Run.

Provider delivery, safe-boundary consumption, queued-input handling on successful
or failed completion, and the terminal eligibility guard for accepted guidance
remain pending runtime and scheduler work.

Cancelling a child does not cancel its parent or siblings. Cancelling a run
requests cancellation for its descendants. A completion already committed
before cancellation wins; the cancellation command returns the terminal run.

The daemon commits cancellation intent before signalling owned execution. It
signals the selected subtree before waiting for any owned process to stop.
New child Runs are rejected below a cancelling ancestor. A queued or
approval-waiting parent remains `cancelling` until its own ToolCalls and all
descendants are terminal. The terminal storage transaction checks these
conditions again.

When a Run becomes `cancelled`, its remaining queued MessageDeliveries become
`cancelled` in the same transaction, with one `run.input_cancelled` Event per
Message. Messages remain immutable and are never redirected. Exact input
retries return the stored cancelled delivery without new Events.

After restart, durable queued work and pending approvals can be cancelled.
Missing ownership of non-terminal external execution returns
`cancellation_failed`; process reconciliation remains pending.

### Context manifests

The internal context boundary stores an immutable, ordered snapshot for one Run.
It does not invoke a provider, change Run state, or acknowledge input delivery.
There is no public manifest mutation endpoint in this slice.

Manifest entries contain explicit instruction text with provenance or selected
Message IDs. Storage loads Message content from the immutable source records;
callers cannot supply replacement Message content. Sources must share the Run's
Session. A targeted Message must target that exact Run and already have a
`delivered` MessageDelivery. Queued, failed, and cancelled deliveries cannot
enter a new manifest. Duplicate Message references are rejected.

Instruction provenance is attribution supplied by the trusted internal caller,
not proof of a file read or user authentication. Workspace and Run identities
are checked against the owning Run. Attribution never grants permission or
causes a filesystem read.

The caller supplies the order. Kiln does not automatically copy a Session or
parent transcript into a child Run. Instruction text retains its exact bytes;
blank text is invalid. A versioned, domain-separated, length-prefixed encoding
covers Session and Run identity, entry order, kind, provenance, and exact content.
The manifest hash is computed from this encoding and verified when read. Reads
also check Message snapshots against their immutable sources and delivery state.

A new manifest requires a Run that accepts work. The manifest, ordered entries,
idempotency result, and `context.manifest_created` Event commit together. The
Event exposes metadata, not instruction content. An exact retry returns the
original manifest before checking current Run acceptability; stored source and
delivery integrity are still verified. Changed input with the same key conflicts.
Queries retain committed snapshots after Run termination and list them in
durable creation order.

The first slice supports text instructions and Message snapshots only. Context
assembly, attachment and tool representations, provider
streaming, and safe-boundary input consumption remain pending native-loop work.

### Model invocations

The internal ModelInvocation boundary records one provider request attempt for
one Run. It does not call a provider or expose public mutation endpoints.
The contract follows [EDL-250](https://linear.app/edlundin/issue/EDL-250).

Each attempt references an immutable ContextManifest for the same Run. Creation
requires a queued or running Run and checks the manifest hash and source
integrity. The selected provider account, model, capability snapshot, settings,
and purpose are fixed for that attempt.
Provider account references contain no credentials. Account connection and
entitlement validation remain part of the later provider boundary.

The initial settings snapshot stores provider and model identifiers, an optional
output-token setting, optional reasoning effort, and versioned tool, vision,
and structured-output support. Unknown capability support remains explicit.
It does not claim to validate a provider's complete request contract.

States are `pending`, `in_flight`, `completed`, `failed`, `cancelled`, and
`interrupted`. A retry creates a new attempt linked to its predecessor; it does
not rewrite the old result. Retry attempts retain the logical work identity.
Only failed or interrupted attempts can be retried. A retry must retain the
original manifest, account, settings, capabilities, and purpose. Changed context
or settings require a new logical work item.
This boundary does not decide whether a provider error is safe to retry.
Completion distinguishes assistant output from tool requests. Terminal records
carry a normalized outcome rather than raw provider error text.

A fresh claim moves a queued Run to running in the same transaction as the
invocation's `in_flight` transition. Active ToolCalls prevent the claim. A
duplicate claim cannot authorize another provider request. The caller must
dispatch only after an `Applied` claim result.

Only one non-terminal invocation can exist for a Run. Run terminal transitions
must check for active invocation work in the same storage transaction. Creating
or changing an invocation and writing its metadata Event are atomic.
Events retain the state and outcome at the time of the change; replay does not
replace earlier states with the invocation's latest state.

Exact creation retries return the stored attempt before checking current Run
eligibility. Changed input with the same key conflicts. Matching claim and
finish retries create no new Events; a changed terminal outcome conflicts.
Reads validate manifest ownership and hashes, and the ordering and identity
of the retry chain.

Run cancellation preserves `cancelling` while invocation work is active. The
daemon can cancel a pending invocation because dispatch has not begun. It does
not claim ownership of an in-flight provider request. After restart, an
in-flight record remains durable and blocks another claim; cancellation returns
`cancellation_failed` until explicit internal reconciliation finishes it.
Finishing an invocation does not itself complete its Run.

Provider transport, streamed output, automatic retries,
and native-loop scheduling remain pending. The usage contract in
[EDL-268](https://linear.app/edlundin/issue/EDL-268) records each physical
attempt separately while retaining its logical work identity.

### Native provider port

The internal `ProviderApplication::claim` operation creates a provider request
only after a fresh durable invocation claim. It loads the invocation and its
validated ContextManifest, checks their identity and hash links, and preserves
the exact stored context order. An already claimed or terminal invocation
returns no request. This includes an invocation cancelled before dispatch.

`ProviderRequest` has no public constructor and cannot be cloned. A provider's
`start` operation consumes it. This prevents accidental reuse of a claim within
the native runtime; it does not establish a provider account connection or
validate its credentials. The storage adapter remains responsible for verifying
the context content hash and source ownership on reads.

The first port supports text deltas, partial usage updates, and a terminal
outcome paired with final usage. Updates have a validation operation that checks
the invocation, work, and account attribution and usage finality. Empty text
deltas and tool-request completions without a tool payload are invalid.
Provider errors use typed categories without raw provider diagnostics. Requests
and output updates have no automatic debug representation that exposes text.

The caller must validate each update before persistence, consume the terminal
update through `finish_model_invocation_with_usage`, and treat end-of-stream
without a terminal update as interruption. Cancellation is an explicit operation
on the active provider operation. Successful cancellation must stop external
work before the caller records a cancelled outcome. The port has no clock,
network, secret store, tool executor, or retry policy.

Daemon dispatch, durable streamed assistant output, tool-request payloads,
provider accounts, authentication, and live provider adapters remain pending.

### Native usage observations

The internal usage boundary accepts normalized counted-unit updates for an
invocation that has been dispatched. It does not call a provider or expose a
public mutation endpoint. Each update carries the physical invocation ID, its
logical work ID, and its immutable ProviderAccount ID. Storage checks these
against the invocation and derives the Run, Session, Workspace, and requested
model from durable ownership. The observation time is an explicit UTC Unix
millisecond value supplied by the adapter, not a hidden clock read.

An update has a stable per-attempt update ID, `delta` or `cumulative` accounting,
`partial`, `final`, or `correction` finality, and explicit `complete`, `partial`,
or `unknown` completeness. Exact duplicate updates return their original
observation without another Event. Changed input with the same update ID
conflicts. The store serializes deduplication, reduction, revision creation,
and `usage.observed` Event insertion in one transaction.

Deltas add each reported quantity once with checked integer arithmetic.
Cumulative updates replace the entire effective quantity set. Missing
quantities remain absent; an observed zero is stored as zero. A final update
closes usage for the attempt. Only a cumulative correction can follow a final
update, and it creates a new immutable revision linked to the previous revision.
Consumers must use only the latest revision per physical attempt for totals.
Retries have separate attempt histories even when they share a logical work ID.

Quantities use namespaced dimensions, units, and additive, subset, or
informational relations. `tokens.input` and `tokens.output` are additive token
counts. `tokens.input.cached` is a subset of input, and
`tokens.output.reasoning` is a subset of output. Subsets cannot exceed their
known parent quantity and must not be added to totals. This first boundary
supports unsigned integer counted units. Fractional units, monetary valuation,
pricing catalogs, allowance snapshots, and aggregate query endpoints remain
pending.

Observations retain normalized update metadata, effective quantities, revision
links, and ownership across restart. Reads validate the revision chain and
recompute effective quantities. Events refer to the immutable revision that
caused them, not the latest revision. Public Events contain metadata only.
The update DTO has no fields for prompt text, output text, headers, cookies,
or provider request and response bodies. Optional request, resolved-model, and
service-tier references must be normalized identifiers.

Usage finality is separate from invocation lifecycle state. The internal
`finish_model_invocation_with_usage` operation commits a final usage update and
the invocation outcome in one transaction. It accepts `final` usage with
explicit completeness, including partial or unknown quantities. It does not
accept a partial update or correction as the completion command. When both Events are new, the usage
Event precedes the invocation terminal Event. If either update fails, neither
is committed. Matching retries return the original usage revision and terminal
invocation without new Events, including after a later usage correction.

A dispatched invocation cannot enter a terminal state through the separate
finish operation unless its latest usage observation is final. Pending
cancellation requires no usage because dispatch has not occurred. Existing
terminal records remain readable, and exact terminal retries remain valid.
This boundary permits late usage for an already terminal invocation only if a
durable dispatch transition exists. Automatic terminal observations and
provider orchestration remain pending; the caller supplies the normalized
final update and its observation time explicitly.

### Tools, permissions, and approvals

| Command | Idempotency | Required behavior | Durable events |
| --- | --- | --- | --- |
| `RequestToolCall` | required | Record one capability and one effective scope | `tool_call.requested` |
| `DecideToolCallPolicy` | required | Grant direct execution, require approval, or deny | `approval.requested` or `tool_call.denied` |
| `DecideApproval` | required | Record the first valid user or policy decision | `approval.decided`, `tool_call.state_changed` |
| `ExpireApproval` | required | Expire a pending approval only when an accepted deadline policy applies | `approval.decided`, `tool_call.state_changed` |
| `ClaimToolCall` | required | Record committed external work before dispatch | `tool_call.state_changed` |
| `FinishToolCall` | required | Record normalized result or artifact references | `tool_call.state_changed` |
| `GrantPermission` | required | Add an explicit capability and scope grant | `permission.granted` |
| `RevokePermission` | required | Prevent new work under a grant | `permission.revoked` |

The requested scope and effective scope are separate values. The effective
scope MUST be equal to or narrower than the request and every active grant. A
plugin, MCP server, provider, agent, or tool cannot create its own grant.
Rejection or expiry denies the tool call. The denial is delivered to the
provider as a normalized tool result, after which the run may continue.

### Registered execution boundaries

| Command | Idempotency | Required behavior | Durable events |
| --- | --- | --- | --- |
| `RegisterAgent` | required | Store an agent profile that references one provider | `agent.registered` |
| `RegisterProvider` | required | Store non-secret provider configuration metadata | `provider.registered` |
| `InstallPlugin` | required | Store validated manifest data and requested grants | `plugin.installed` |
| `SetPluginGrant` | required | Add or revoke one explicit plugin grant | `plugin.grant_changed` |
| `RegisterMcpServer` | required | Store non-secret server configuration metadata | `mcp_server.registered` |

Lifecycle work is executed by its owning adapter after the registry command
commits. Secret values MUST NOT enter these core records.

### Terminal sessions

| Command | Idempotency | Required behavior | Durable events |
| --- | --- | --- | --- |
| `RequestTerminalSession` | required | Record a scoped terminal request for one root and run | `terminal.requested` |
| `ClaimTerminalStart` | required | Claim committed terminal-start work before process creation | `terminal.state_changed` |
| `RecordTerminalStarted` | required | Accept the adapter result after process start | `terminal.state_changed` |
| `DetachTerminalSession` | required | Keep the process active and remove one client attachment | `terminal.detached` |
| `RequestTerminalStop` | required | Commit stop intent before process signalling | `terminal.stop_requested` |
| `RecordTerminalExit` | required | Commit exit status and release runtime ownership | `terminal.state_changed` |

Terminal byte streams are transient transport data. Lifecycle, ownership, exit
status, and artifact references are durable.

## 7. Queries

Queries return committed state only. They MUST NOT repair state or start work.

- `GetProject` and `ListProjects`
- `GetThread` and `ListThreads`
- `ListMessages`
- `GetRun` and `ListRuns`
- `ListTasks`
- `GetToolCall` and `GetApproval`
- `ListPermissions`
- `ListEventsAfter`
- `GetArtifactMetadata`
- `ListAgents`, `ListProviders`, `ListPlugins`, and `ListMcpServers`
- `GetTerminalSession`

`ListEventsAfter` orders by the per-thread cursor. Clients may receive a live
event twice during reconnect and MUST deduplicate by event ID. The server may
hydrate a query result, but hydration cannot change core state.

The first event cursor in a thread is `1`. A query cursor of `0` means “before
the first event.” No durable event has cursor `0`.

## 8. Durable events and outbox work

Every accepted domain event has one event ID, thread ID, cursor, type, payload,
and occurrence time. Event cursors are unique and strictly increasing within a
thread. Ordering between threads is not defined.

Events are append-only. Corrections use new events. An event is visible only
after its state transaction commits. Live delivery reads committed events; it
is not the source of truth.

External work is an outbox record with a stable work ID and owning entity. An
adapter claims work, records execution ownership, and reports normalized
updates. Adapter retries MUST use the same work ID. A retry cannot create a
second logical provider request, process, tool call, or terminal.

## 9. Recovery

On startup, recovery MUST:

1. load non-terminal runs, tasks, tool calls, and terminal sessions;
2. inspect committed outbox work and execution claims;
3. ask the owning adapter whether claimed external work still exists when that
   boundary supports inspection;
4. resume, cancel, or fail work through normal core commands; and
5. emit events for every recovery decision.

Recovery MUST NOT silently change a terminal state or replay user-visible
output. If an external boundary cannot prove that work is still active, the
core marks the operation failed with a restart-specific reason. Provider-specific
resume support is an adapter capability, not a core assumption.

## 10. Error contract

Core errors use stable codes:

- `not_found`
- `already_exists`
- `invalid_argument`
- `invalid_state_transition`
- `relationship_conflict`
- `permission_denied`
- `approval_required`
- `idempotency_conflict`
- `concurrency_conflict`
- `boundary_unavailable`
- `recovery_failed`

Errors may include safe structured details. They MUST NOT contain secrets,
provider payloads, raw command output, or unrestricted filesystem paths.

## 11. Required ports

The first core crate defines behavior-oriented ports for:

- atomic state, event, idempotency, and outbox transactions;
- immutable artifact metadata lookup;
- provider work dispatch and cancellation;
- capability-scoped tool dispatch and cancellation;
- terminal lifecycle control;
- clock and identifier generation; and
- committed-event notification.

Ports use core values. They do not expose SQL rows, Axum requests, provider SDK
objects, JSON-RPC frames, MCP frames, or PTY handles.

## 12. First vertical slice

The first implementation proves:

1. create a Workspace with two local Git roots;
2. create a Session and append one message;
3. start one deterministic Run that waits for approval without executing;
4. stream ordered events, disconnect, restart the daemon, and replay the exact
   non-empty durable suffix after the first observed Run Event cursor;
5. approve the ToolCall, complete the Run, and retrieve its large-output
   artifact;
6. start and cancel a second Run, including its owned process group;
7. stop and restart the daemon gracefully; and
8. recover the Workspace, Session, Runs, message, events, and artifact through
   the public protocol.

The slice is complete only when it passes through the public protocol and a real
SQLite adapter. In-memory adapters support unit tests but are not acceptance
proof.

## 13. Open implementation decisions

These choices remain open and must be decided before their owning operation is
implemented:

- local client authentication mechanism;
- execution-claim representation and adapter inspection support;
- Git isolation model for concurrent work;
- direct provider and provider data-retention policy;
- terminal backend and platform PTY behavior; and
- signed client UI packages for plugin-provided UI.

No numeric timeout, payload, queue, retry, concurrency, or retention limit is
specified yet. Each limit needs a measured workload and a stated reason.
