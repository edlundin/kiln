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
artifact display name MUST NOT become a trusted path. A targeted message may
name one active run in the same thread.

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

Cancelling a child does not cancel its parent or siblings. Cancelling a run
requests cancellation for its descendants. A completion already committed
before cancellation wins; the cancellation command returns the terminal run.

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
