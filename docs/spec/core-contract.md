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
only an untargeted, complete user message. `SendRunInput` and
`ReactToRunActivity` create messages targeted to one run in the same thread.

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

`AppendMessage`, `SendRunInput`, and `ReactToRunActivity` accept empty or
whitespace content when one or more validated attachments are present. A
message with no attachments still requires non-whitespace content. `SendRunInput`
targets an immutable `interactive` Run in `queued`, `running`, or
`waiting_for_approval` state. Normal guidance uses `queued`; interrupt delivery
must be explicit. The targeted Message, queued MessageDelivery, idempotency
result, `message.appended`, and the matching `run.input_queued` or
`run.interrupt_requested` Event commit atomically. An exact retry returns the
first MessageDelivery without new Events, including after the Run becomes
terminal. A mismatched key reuse conflicts.

`ReactToRunActivity` queues a root-targeted user Message with a typed
`child_activity` reference containing the source Run ID and Event ID. It uses
the same idempotency and delivery transaction as `SendRunInput`. The source
Event must belong to the declared child Run in the same Session, and that Run
must descend from the target root. The daemon validates these relations before
storing the Message. Missing, mismatched, root-self, and unrelated references
are rejected. Reusing a key with another reference conflicts.

References survive Message replay, delivery, cancellation, and exact retries.
The selected Event identifies the child Message, ToolCall, artifact, or other
durable activity; the client cannot supply replacement source content.

`RecordRunInputDelivery` applies only to the oldest queued delivery for that
Run when it records `delivered` or `failed`. An already recorded exact outcome
is idempotent; a different later outcome is invalid. A new `delivered` outcome
is invalid after Run termination. A `cancelled` outcome requires a terminal
Run. The durable query for pending runtime work returns the oldest queued
delivery per Run.

The deterministic native mode consumes input at generation boundaries. Accepted
guidance prevents successful finalization until it is consumed. Live-provider
delivery and broader scheduler policy remain pending.

Subprocess completion, failure, and approval denial wait for non-terminal
descendants before committing a terminal Run state. The daemon waits without
holding the commit lock and rechecks state after each wake. Storage checks the
same descendant condition inside the terminal transaction, including native
Run failure, so a concurrent child start cannot bypass it.

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
enter a new manifest. Incomplete assistant Messages cannot enter a manifest.
Complete assistant Messages can be selected within their originating Run or
between root Runs in the same Session. Child transcripts do not cross Run
boundaries through Message selection. Duplicate Message references are rejected.

Selecting a delivered root reaction also adds one `ChildActivitySnapshot`
immediately after its user Message snapshot. Kiln loads the referenced durable
Event and checks its Session, owning child Run, and ancestry. The snapshot
retains the reaction Message ID and source Run and Event IDs. It contains only
the selected Event's exposed text or metadata, not the full child transcript,
child context, or artifact bytes. This entry is untrusted context data, not an
instruction. Provider adapters must preserve this distinction.

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
Child activity reads validate the reference and use the stored projection; they
do not regenerate it. Historical version-1 manifests retain their original bytes,
hashes, invocation bindings, and idempotency results.

New manifests use version 2. Migration `0026` adds an explicit encoding version
and ordered attachment snapshot rows. Each snapshot binds a source Message ID
to artifact hash, media type, and byte size; ordering follows message-entry order
and the original attachment order within each message. Only user-message entries
can own attachments; duplicate bindings, unknown messages, and reordered message
groups are invalid. The canonical hash covers the original ordered entries and
every attachment binding, including the empty attachment set. Creation validates
artifact ownership against the same Session and commits metadata with the manifest.
Reads compare snapshots with source attachments, enforce contiguous positions,
recheck session ownership, and verify the version-appropriate hash. Missing,
extra, reordered, or altered metadata fails closed.

Version 1 deliberately exposes no complete attachment snapshot and must be
reassembled before a live provider consumes attachments; it is never silently
upgraded or assigned a different hash. The provider request exposes the manifest
version and attachment references. The subsequent provider-context assembler resolves those references into bounded,
hash-verified bytes; this does not send attachments to a model. Attachment-only
messages retain the existing readable hash-summary placeholder in their text
entry alongside the new typed references.

A new manifest requires a Run that accepts work. The manifest, ordered entries,
attachment snapshots, idempotency result, and `context.manifest_created` Event commit together. The
Event exposes metadata, not instruction content. An exact retry returns the
original manifest before checking current Run acceptability; stored source and
delivery integrity are still verified. Changed input with the same key conflicts.
Queries retain committed snapshots after Run termination and list them in
durable creation order.

Context supports text instructions, Message snapshots, selected child activity
snapshots, and versioned attachment references. `ProviderRequest::assemble_context`
returns an ordered, provider-neutral projection with instruction provenance,
message IDs/roles, attachment bytes/metadata, and selected child references. It
rejects version-1 snapshots explicitly. The caller supplies nonzero text,
per-attachment, and total attachment byte ceilings; these are resource budgets,
not token counts or provider model limits. Preflight rejects excess bytes or
arithmetic overflow before any artifact read. Repeated artifact occurrences count
toward the total each time because each occurrence is represented in the output.

The filesystem reader runs one bounded read off the async executor, rejects
non-regular files and size mismatches, and reads no more than expected size plus
one sentinel byte. Unix opens reject symlinks and use nonblocking open so a FIFO
cannot stall before file-type validation. Assembly verifies exact size and SHA-256
against the immutable metadata before accepting each payload. Missing, unavailable,
corrupt, or oversized artifacts fail the whole assembly; nothing is silently
truncated or omitted. Dropping assembly stops further reads and discards partial
results; an already-started blocking filesystem read may finish within its byte
bound. No wall-clock filesystem cancellation guarantee is claimed.

The assembled context has no content-bearing Debug implementation and does not
upload data, infer provider media support, execute tools, or append inherited
parent/session history. Tool descriptions/results, model-specific serialization,
context-window/token accounting/compaction policy, and live transport integration
remain pending. The deterministic native mode stores
explicit fixture instructions and consumed Run input.

### Model invocations

The internal ModelInvocation boundary records one provider request attempt for
one Run. It does not call a provider or expose public mutation endpoints.
The contract follows [EDL-250](https://linear.app/edlundin/issue/EDL-250).

Each attempt references an immutable ContextManifest for the same Run. Creation
requires a queued or running Run and checks the manifest hash and source
integrity. The selected provider account, model, capability snapshot, settings,
and purpose are fixed for that attempt.
Provider account references contain no credentials. EDL-309 adds a durable
`ProviderAccount` metadata boundary with an opaque `secret_ref`, the states
`connecting`, `connected`, `reauth_required`, and `disconnected`, durable
timestamps, and safe provider metadata. Secret references use the canonical
`sec_<ULID>` form and contain no credential material. Account records are
associated with workspaces explicitly; model selection can resolve only a
`connected` account whose provider type matches the requested model and whose workspace
association matches the Run's Session workspace. The store permits historical
disconnected records while enforcing one active account per provider type and
never silently replacing an existing account.

Account connection and entitlement validation remain part of the later
provider boundary. EDL-309's secret-store slice keeps secret bytes in an
OS-backed vault adapter and carries only the opaque `SecretRef` through core,
SQLite, events, and protocol boundaries. Secret wrappers are redacted and
non-serializable. Before connect/rotate writes a vault entry, migration `0025`'s
cleanup journal reserves its opaque reference against the observed account.
Publishing the connected reference consumes the reservation and journals any
retired reference in the same SQLite transaction. A failed or interrupted write
leaves its reservation for cleanup; an ambiguous publication failure never causes
the application to delete a possibly committed credential. Cleanup refuses the
account's current reference and clears a journal entry only after confirmed vault
deletion (including an already missing entry). Disconnect first disables credential use with `reauth_required`,
retaining the durable reference until vault deletion succeeds (an already missing
entry also counts as deleted). Only then does it clear the reference and publish
`disconnected`. Failed or interrupted deletion can be retried after restart; a
failure to publish the final state leaves the disabled reference available for
the same retry. Disconnect also drains the journal, including for already
disconnected accounts, so new connection/rotation cleanup survives restart.
Pre-journal orphan entries cannot be reconstructed by the migration. Sign-in
preparation blocks on pending cleanup until disconnect resolves it. The OS adapter
retains a per-entry lock through writes/deletes that outlive their caller within
the shared adapter instance; this is not a cross-store transaction or a claim
about recovery from OS-vault corruption. Account lifecycle operations serialize
per account and reject provider/account mismatches.
Rotation accepts the expected current `SecretRef` and rejects a stale caller
before writing a replacement. Provider-owned refresh consumes the redacted
vault value under the same per-account lock. A successful replacement is
vaulted and durably rotated before waiters return; a waiter carrying the old
reference then reuses that committed account version without another provider
request. Permanent provider rejection durably moves the still-current account
to `reauth_required`, while transient rejection leaves it connected and does
not cache the failure. This is serialized refresh with successful-version
coalescing, not general single-flight for transient failures.

The authenticated protocol now exposes safe provider-account create, list, and
get snapshots plus the Codex device-code login start, nonblocking status,
cancellation, and local disconnect routes. Protocol `0.24.0` adds authenticated
`POST /v1/provider-accounts/{provider_account_id}/disconnect`: it cancels and joins
the current login, disables account use, removes local credentials, retries cleanup
retained by the current login attempt, and returns a safe disconnected account
snapshot. Successful disconnect invalidates that login attempt. Deletion failure
returns `provider_account_cleanup_required` without exposing credential references.
Already-disconnected accounts are idempotent, but a new disconnect request also
affects a subsequently reconnected account; clients do not automatically retry.
Provider-side revocation is not performed.

The separate `openai_api` provider uses a versioned, account-bound vault envelope.
Its key wrapper and errors are redacted; syntax validation does not establish
OpenAI authentication, billing, model access, or entitlement. Explicit local
`kilnd import-openai-api-key --stdin` import requires an exclusive daemon-store
lock, refuses active-account replacement, and reuses an empty account through the
guarded lifecycle/journal path. It accepts no key argument or implicit environment
credential and refuses terminal stdin to avoid echo. No API-key input is added to
the renderer, HTTP/WebSocket protocol, Events, or artifacts. Provider transport
can decode only an envelope bound to the requested OpenAI API account; Codex
subscription envelopes remain separate. Live API transport is not implemented.

Account creation requires a non-empty `Idempotency-Key`;
an exact retry returns the original account and changed payload reuse is a
typed conflict. Account responses omit secret references, provider subjects,
arbitrary metadata, and credential material. Login attempt IDs are
opaque and process-local; the latest terminal result remains addressable until
an explicit replacement, while daemon restart makes prior attempts unavailable.
The deterministic fixture continues to use its explicit registry-only account
identity and does not require a persisted account or secret. Live OAuth,
vault, and UI runtime verification remain pending.

The first `openai_codex_subscription` credential adapter stores a versioned
provider-private envelope inside `SecretValue`; its ChatGPT account identifier
and tokens never enter SQLite, Events, or protocol types. Its HTTPS refresh
request uses the Codex OAuth refresh grant, rejects redirects, and merges
optional replacement tokens without clearing an omitted prior token. Kiln's
operational defaults allow 10 seconds to connect, 30 seconds for the complete
request, and at most 1 MiB of response bytes; callers can lower or otherwise
tune the time bounds, while the response cap cannot exceed core's 1 MiB
`SecretValue` limit. These bound account-lock occupancy and memory under a
stalled or malformed peer; they are not OAuth service requirements. A
replacement ID token must retain the stored
ChatGPT account claim. This reads identity metadata from the trusted HTTPS/vault
token but does not claim to verify the JWT signature. Unauthorized,
`invalid_grant`, and known expired, reused, or invalidated refresh-token errors
require reauthentication; transport, malformed response, and unknown failures
remain transient. Browser UI integration, revocation, and live model transport
remain pending.

The browser PKCE adapter follows the official Codex OAuth protocol at
`openai/codex` commit `deb0a08f240fb9b630e514417ad2256cf8e4afba`
(`codex-rs/login/src/server.rs` and `oauth/{authorization,pkce,client}.rs`).
It generates the verifier and state with OS randomness, uses S256, and binds
only IPv4 loopback on registered port 1455 or fallback 1457. It never cancels
another process occupying those ports. The localhost callback validates Host,
path, method, unique state/code/error fields, and exact state before accepting
a code or provider rejection. Invalid callbacks leave the attempt pending.
A 16 KiB request ceiling and five-second per-peer I/O limits bound local input;
the fifteen-minute attempt deadline covers the callback and token exchange.
These are application resource limits, not claimed provider payload guarantees.
Dropping or cancelling completion closes the listener; a valid code closes it
before a single token exchange, with redirects and automatic HTTP retries disabled.
The browser response contains only static text and asks the user to check Kiln;
it does not claim vault publication succeeded. Codes/verifiers remain private.
Only identity/offline scopes are requested; connector scopes are not needed here.
Protocol `0.25.0` exposes a separate authenticated browser-start route with a
no-store response and redacted Debug representation. Both login methods use the
same daemon-owned attempt, cancellation, disconnect, and journaled publication
lifecycle. Settings validates the authorization URL and exposes device sign-in
as an explicit remote-host/callback fallback. Native inspection observed browser
preparation/cancellation and listener closure. Live OAuth, callback parsing,
token exchange, and vault publication remain runtime-unverified.

The native device-code slice follows the Codex service contract rather than a
generic RFC device flow. It polls immediately, treats only `403` and `404` as
pending, exchanges the returned authorization code with the returned verifier,
and requires all three token fields before encoding the credential. A fixed
positive string polling interval is required; missing, numeric, or zero values
are rejected as unusable rather than creating an unpaced loop. A fixed
15-minute outer deadline covers requests and sleeps; each request retains the
shorter configurable transport limits. The daemon coordinator accepts only an
empty `connecting` `openai_codex_subscription` account and owns cancellation
through task join. Replacement starts cancel and join the prior attempt.
The coordinator retains every join handle until completion even if an awaiting
caller is cancelled; cancel remains independently callable, and shutdown joins
all attempts before returning the first cleanup failure.
Connection rechecks that same empty state under the core lifecycle lock, so an
attempt cannot overwrite a credential connected by another internal caller.
Cancellation racing with vault publication conditionally disconnects only the
exact `SecretRef` published by that attempt; a changed reference is a typed
conflict and is never deleted. Daemon shutdown cancels and joins active
attempts and permanently closes the coordinator before taking its join
snapshot, so a queued or later begin cannot register new work. A cancelled
shutdown call can be retried because attempt handles remain owned. The
authenticated protocol and typed client expose the account create/list/get and
device-login start/status/cancel entrypoints; status reports
`cleanup_required` when conditional credential cleanup cannot be completed. No
live device login or vault runtime proof has run.

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
not infer ownership of an unknown in-flight provider request. The deterministic
fixture is reconciled at startup because it owns no external work after process
exit. Other in-flight records remain durable and block another claim; cancellation
returns `cancellation_failed` until explicit internal reconciliation finishes them.
Finishing an invocation does not itself complete its Run.

Live provider transport, automatic retries, and broader native-loop scheduling
remain pending. The usage contract in
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

`ProviderRequest::tool_catalog` exposes the ordered immutable tool descriptions
frozen for that physical invocation. Each definition binds its model-visible
name, description, Kiln capability, implementation/validation revision, and
canonical JSON input-schema object. Names are unique. Explicit caller-supplied
count, per-definition, and aggregate serialized-byte budgets cover all fields.
Definitions require a root object schema and readable canonical JSON; this
envelope validation does not establish JSON Schema semantics or validate calls.
Registered Kiln implementations must perform those checks before adoption.

Migration `0028` stores catalog count, order, canonical definitions, and a
versioned content hash. Attachment is immutable and allowed only while pending;
an exact attachment retry returns the prior snapshot. Claim freezes an empty
catalog when none was supplied. Physical retries inherit their predecessor's
catalog and reject changes; changed tools require a new logical invocation.
Historical missing catalogs confer no tool authority. Nonempty catalogs require
generation purpose and explicitly supported model tool capability.

The provider application reads the frozen catalog after a successful claim,
preventing a race with pending catalog attachment. A failed post-claim read
returns no dispatch request; the durable in-flight invocation remains for
reconciliation. The catalog snapshot is separate from the context-manifest
hash and is validated independently. It grants no policy scope or approval and
has no executor handle, host-path binding, or credential field.

The first port supports assistant-text and exposed reasoning-summary chunks,
partial usage updates, and a terminal outcome paired with final usage. Output
updates carry the invocation ID, stable update ID, ordered position, stream,
and exact content as a validated `RecordModelOutput` command. Updates have a
validation operation that checks invocation attribution, usage work and account
attribution, and usage finality. Empty chunks and tool-request completions
without a tool payload are invalid.
Provider errors use typed categories without raw provider diagnostics. Requests
and output updates have no automatic debug representation that exposes text.

`ProviderApplication::record_update` loads the durable invocation and validates
each update before storage. It routes output to the chunk store, partial usage
to the usage store, and terminal outcomes with final usage to the atomic
invocation completion store. It returns the committed mutation and Events.
The caller publishes only those Events. Exact retries retain the storage
boundaries' duplicate behavior, including after invocation completion.

`ProviderApplication::record_tool_requests` records a separate, inert batch of
finalized model proposals. Each ordered request binds a unique provider call ID,
an untrusted tool name, and a complete JSON argument object. Caller-supplied
count, identifier, per-object, and aggregate byte limits are mandatory. Objects
are recursively key-sorted and serialized within their byte budgets; JSON
round-trip validation preserves numeric values and rejects unreadable nesting.
Arguments and names have no content-bearing automatic debug representation.

Migration `0027` stores the ordered batch and its versioned canonical hash.
The SQLite boundary atomically records proposals, final usage, invocation
completion as `tool_requests`, and their Events. Only generation invocations
with explicit tool support may submit a batch. Exact retries reuse the stored
batch and usage without additional Events; changed batches conflict. A new batch
cannot be attached to an already terminal invocation. Reads validate terminal
kind, capability, count, positions, argument structure, and the batch hash.
Historical records may have no batch; consumers must fail closed when proposals
are absent. The hash detects corruption, not an actor able to rewrite the database.

Recording proposals does not resolve names, validate tool-specific schemas,
create executable ToolCalls, grant scope, approve, or dispatch. Those operations
remain owned by Kiln. The generic provider stream still rejects payload-free
tool completions. Its terminal `ToolRequests` variant requires the validated
batch and final usage and routes through the same atomic proposal store.
The daemon publishes the committed Events and stops reading after that terminal
update. When draining an invalid stream after cancellation, it retains validated
final usage under the failure outcome and discards any proposals. No proposal
from that error path is persisted or executed. The deterministic native loop
still advertises tools as unsupported; executable tool adoption remains pending.

`ProviderApplication::resolve_tool_requests` loads the terminal invocation,
durable batch, and frozen catalog before resolving any proposal. Missing
historical proposals/catalogs fail closed. Every requested name must have been
offered. The local `ModelToolArgumentResolver` must supply the exact registered
capability/revision definition, including its schema and description. Missing
implementations or changed definitions reject the entire batch before argument
parsing. Names from model output are never interpreted as capability IDs.

The local resolver parses complete canonical arguments into typed command data
under the registered tool's contract. It must reject invalid/unknown fields and
semantic violations without filesystem/network effects, schema-reference
fetches, dispatch, or policy decisions. Resolution preserves order, provider call
IDs, invocation identity, and the frozen definitions. A failure reports only
its position and a typed reason and returns no partial resolved batch. Resolved
commands carry no ToolCall ID, effective scope, or approval. Concrete native
resolvers and coordinator dispatch are not wired yet.

Locally resolved batches can prepare `AdoptModelToolRequest` commands for one
position and an explicit requested Workspace scope. The SQLite adoption boundary
rechecks the complete durable source and frozen catalog before creating a
ToolCall. Migration `0029` records a unique invocation/provider-call-to-ToolCall
link atomically with ToolCall state, any Approval, Run state, and Events.
Exact retries return the existing ToolCall and no new Events even after its
completion; changing its requested scope conflicts.

Adoption requires a running Run, its latest completed tool-request invocation,
no active invocation/tool/approval, and terminal adopted predecessors in request
order. The requested scope must retain the Run's Workspace root and stay inside
its directory scope. `ask` creates an awaiting-approval ToolCall and a pending
Approval; `full_access` makes it ready within the requested scope; `read_only`
conservatively denies it under the current generic policy. No capability-specific
read-only exemptions are inferred. This serial boundary preserves the existing
single-approval cancellation invariant.

Both invocation creation and claim reject outstanding stored proposals, so a
next model turn cannot skip their adoption/completion or deadlock adoption by
creating a competing pending invocation. Invalid proposals currently require
Run failure/cancellation; model-visible validation-error results remain pending.
Adoption is an internal operation and does not dispatch. Result context and
coordinator integration remain pending. The
deterministic subprocess executor refuses native model Runs and foreign
capabilities; native approval decisions notify the coordinator without spawning
the subprocess fixture.

`ProviderApplication::finish_tool_call` records a native adopted ToolCall result
without changing Run state or starting more work. The executor must have stopped
external work before calling it. Storage revalidates the adoption link, durable
proposal/catalog, Run ownership, and capability. Completion requires a running
tool and a running/cancelling Run with no conflicting active work. Explicit
cancellation may finish a ready or running tool only while the Run is cancelling;
partial output is retained. A normal completion racing cancellation also leaves
the Run cancelling for its coordinator to finalize.

Inline output uses the existing per-stream `INLINE_TOOL_OUTPUT_LIMIT` (4096
bytes); larger output requires an Artifact already written by the executor.
Artifact metadata, terminal tool state, and output/state Events commit together.
Exact terminal retries return no new Events; changed results conflict. Denied
tools are already terminal and cannot acquire a completion payload. Result bytes
are not read or uploaded by this operation, and Run/child cancellation and the
next model turn remain separate decisions.

`ProviderApplication::claim_tool_call` consumes a locally resolved batch and
selects one adopted ToolCall. The claim transaction revalidates the immutable
proposal/catalog link, current Run scope, latest invocation, effective ToolCall
scope, approval state, and absence of overlapping work. Only a fresh
`ready → running` transition returns a `ModelToolExecutionRequest` and its
committed state Event. The request carries typed command data, source call IDs,
and the effective Workspace scope; it has no public constructor, Clone, or Debug.
It must be consumed by a Kiln-owned executor, never passed to a model provider.

Running and terminal calls return a duplicate result with no execution request
and no new Events, including after restart. Awaiting/denied/cancelled work cannot
receive a new execution request. Dropping a fresh request or losing the process
after claim leaves durable running work requiring explicit reconciliation;
there is no automatic reset or redispatch. A read-only reconciliation decision
alone cannot establish that external effects stopped. The legacy subprocess
`begin_tool_call` entrypoint rejects adopted model calls. Concrete executor
integration and reconciliation handlers remain pending.

`ModelToolExchangeStore::get_model_tool_exchange` reads a terminal adopted call
for an explicitly requested Run in one SQLite snapshot. It verifies the adoption
link, invocation/manifest integrity, complete proposal batch, frozen catalog,
capability, Run/Session ownership, and terminal output shape. Missing, foreign,
incomplete, or inconsistent sources cannot become exchanges. No tool executes
and no output Artifact bytes are loaded by this operation.

The resulting `ModelToolExchange` retains the local ToolCall and provider call
IDs, source invocation, tool name/capability/revision, canonical arguments,
terminal state, inline stdout/stderr, Artifact metadata, and exit code. Its
versioned canonical JSON preserves null versus empty output and denial versus
cancellation; its Debug representation omits payload content.

`ContextManifestEntryInput::ToolExchange` selects a terminal adopted ToolCall
from the same Run. Migration `0030` adds a distinct `tool_exchange` entry kind,
source Run/ToolCall references, and a canonical exchange-content snapshot. The
migration preserves historical message/instruction/child-activity entries,
including their source Event references. Version-2 manifests bind exchange
content and order into their existing hash through a new unambiguous entry tag;
historical entry encodings and hashes remain unchanged. Version-1 construction
rejects tool exchanges, and older readers reject the unknown entry kind.
Duplicate ToolCalls and foreign Run/Session sources are invalid.

Snapshot reads reconstruct terminal exchanges from their immutable proposals,
catalogs, and current terminal ToolCalls and require exact stored-content
equality. Source validation checks invocation retry metadata and the source
manifest header's ownership/hash; the source manifest must precede the containing
manifest. It does not recursively reload all historical manifest content. Each
loaded manifest independently validates its own entries and hash, while the
standalone exchange reader also invokes normal source-invocation integrity
validation. This separation prevents recursive historical context expansion.

`ProviderContextEntry::ToolExchange` retains the typed exchange and separately
verified stdout/stderr Artifact payloads in manifest order. Canonical exchange
JSON counts against the text budget. Tool output Artifacts share the existing
per-artifact and aggregate attachment byte budgets with message attachments;
all payload sizes are preflighted before any read and bytes are size/hash checked.
Missing or corrupt output artifacts fail the entire assembly. No live adapter or
native coordinator consumes these tool-context entries yet.

`WorkspaceFileReadTool` supplies the local `read_file` definition for capability
`kiln.workspace.read_file`, revision `1`. Explicit path/file byte limits are part
of its frozen description. Parsing accepts only a single relative slash-separated
path, rejecting empty, dot, parent, control-character, and backslash components.
The executor consumes a fresh `ModelToolExecutionRequest`, verifies the registered
root and capability, and pins the approved scope using the existing filesystem
identity checks. Unix descriptor-relative traversal rejects symlinks, cross-device
paths, and nonregular files. A bounded sentinel read enforces the file ceiling
even if the file grows; invalid UTF-8 and oversized content produce no partial
output. Results above the existing inline ceiling are stored as Artifacts.
Other platforms fail closed. Explicit cancellation joins started blocking work
before reporting Cancelled, without a filesystem latency guarantee. Dropping the
future may detach blocking work and requires reconciliation of the running claim.
The implementation is not yet activated by the daemon coordinator; generic
ReadOnly adoption still denies all proposed tools.

The caller must treat end-of-stream without a terminal update as interruption.
Cancellation is an explicit operation
on the active provider operation. Successful cancellation must stop external
work before the caller records a cancelled outcome. The port has no clock,
network, secret store, tool executor, or retry policy.

EDL-251 settles `openai_codex_subscription` as the primary native provider and
`openai_api` as a separate BYOK provider type. Tool-request payloads, provider
accounts, authentication, and live provider adapters remain pending
implementation.

The `kiln-providers` crate supplies an explicit deterministic adapter for this
port. It accepts only provider `kiln_deterministic` and model
`deterministic_text`. Its configuration supplies ordered text chunks, a terminal
outcome, synthetic counted usage, completeness, and the observation time. It
does not infer token counts from text, read a clock, resolve credentials, or
make network requests. The adapter rejects empty chunks and invalid normalized
terminal usage before it emits output.

The deterministic operation emits its text chunks, one terminal update, then
end-of-stream. Each chunk has a deterministic per-attempt update ID and ordered
position suitable for the durable output boundary. Cancellation before the
terminal update discards pending text and replaces the configured result with a cancelled outcome and final unknown
usage with no quantities. It does not report configured success counts as
observed cancellation usage. Repeated cancellation is safe, and cancellation
after terminal delivery has no effect.

Set `KILN_RUN_EXECUTOR=deterministic-model` before starting `kilnd` to run new
root and child Runs through this adapter. The default is
`deterministic-subprocess`. Existing approval resumes still use the subprocess
path. This explicit fixture mode uses the synthetic account
`pac_00000000000000000000000000`, fixed assistant text, and unknown usage with
no estimated quantities. It does not connect to a provider account.

The daemon creates a manifest with one runtime fixture instruction. It does not
automatically copy Session or parent transcripts. Queued Run input is delivered
in FIFO order and included in the next manifest before generation. New guidance
prevents finalization until another generation consumes it. Interrupt input
stops the active operation before the next generation. Partial chunks remain
stored.

The daemon claims the invocation before dispatch, publishes only committed
Events, and consumes final usage before completing the Run. Cancellation stops
the operation before recording terminal invocation state, waits for descendants,
and retains incomplete assistant text. Provider errors and streams without a
terminal update stop the operation and consume any valid terminal usage it
still supplies. If none is available, they record unknown final usage while
retaining prior counted quantities. The native Run then fails after all work
is terminal. Storage failure stops execution and leaves the failure visible
to shutdown handling.

The daemon holds an exclusive `auth/daemon.lock` file lock for its process
lifetime. A second daemon using the same data directory exits before opening
the store. The lock file is never removed or truncated.

Before readiness, the daemon reconciles stored deterministic native Runs even
when the current executor setting selects subprocesses. Only the known fixture
provider, model, and synthetic account qualify; fixture-tool Runs and mixed-provider
histories are excluded. Runs whose ToolCalls all have native adoption links remain
eligible, and terminal exchanges are validated before recovery. Active tools and
pending approvals are reported for explicit reconciliation without resetting or
redispatching work. Pending attempts are cancelled without dispatch. In-flight
attempts become interrupted with unknown final usage; existing counted quantities
and final usage are retained. No attempt is dispatched again.

Descendants are handled before parents. A stored successful invocation can still
finalize its assistant Message and Run. Other unfinished generations fail after
all work is terminal. Missing incomplete Messages on failed or cancelled Runs
are rebuilt from stored assistant chunks. Non-fixture descendants that still
require reconciliation are reported, and their parent remains nonterminal.
Runs without a stored invocation are not classified as deterministic model Runs.

### Durable model output

The internal output application records one immutable chunk and its
`model_invocation.output` Event in the same transaction. The caller publishes
only the returned committed Events. Each chunk belongs to one invocation, Run,
and Session. The store derives ownership from the durable invocation and
accepts new chunks only while it is `in_flight`.

Positions start at one and are contiguous across both `assistant_text` and
`reasoning_summary` streams for an attempt. A stable update ID identifies each
chunk within that attempt. An exact retry returns the original chunk without
another Event, even after invocation completion. Changed input with the same
update ID conflicts. A new update with a duplicate or skipped position fails.
Empty content is invalid; whitespace and content bytes are preserved.

Reads validate the ownership and ordered history. Events replay the immutable
stored chunk, including its original content and stream. Failure, interruption,
and cancellation do not delete partial output. Hidden provider reasoning is
not a supported stream. Output command and chunk debug representations omit
content.

Chunks are not Session Messages and cannot be included as Message references in
a ContextManifest. Existing text chunks do not authorize an automatic retry.

### Assistant Message finalization

The internal finalization application derives one immutable assistant Message
from stored `assistant_text` chunks in position order. It preserves content bytes
and excludes reasoning summaries. The Message records its originating Run and
ModelInvocation. Empty assistant output does not create a Message.

For a running Run, finalization requires its latest generation invocation to have
completed with assistant output and final usage. Queued input, active ToolCalls,
active invocations, and nonterminal descendants prevent completion. The complete
Message, `message.appended`, completed Run, and Run state Event commit together.

For an already failed or cancelled Run, a terminal generation invocation can
produce an incomplete Message. Tool-request completions are excluded. This does
not change the terminal Run state or choose a failure policy. Incomplete Messages
remain visible but cannot be selected for context.

The internal native Run failure boundary moves a running Run to failed only
when all ToolCalls, invocations, and descendants are terminal, no approvals are
pending, and at least one generation attempt exists. Every retained ToolCall
must be a validated native exchange; fixture and foreign calls are rejected.
The failed Run and its state Event commit
together. An already failed Run returns without another Event.

An exact retry returns the existing Message without new Events, including after
Run completion. Public append operations remain user-only. The deterministic
native daemon mode publishes the committed finalization Events.

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
pricing catalogs, allowance snapshots, and aggregate valuation queries remain
pending.

The read-only global Usage ledger returns the latest validated revision for
each physical Model Invocation, ordered by invocation ID. It uses an exclusive
invocation ID cursor and bounded pages, preserves explicit zero quantities and
missing dimensions, and carries source, completeness, and normalized provider
metadata. Retries remain separate ledger entries even when they share a
logical work ID. The ledger reports observations only; it does not calculate
prices, costs, or monetary totals.

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
- direct provider implementation and provider data-retention disclosure;
- terminal backend and platform PTY behavior; and
- signed client UI packages for plugin-provided UI.

No numeric timeout, payload, queue, retry, concurrency, or retention limit is
specified yet. Each limit needs a measured workload and a stated reason.
