# MCP broker boundary

This crate begins EDL-314 with a guarded stdio negotiation adapter over the
official `rmcp` SDK, pinned to 3.4.1. The daemon can opt into stdio native tool
execution through explicit host budgets; synchronized MCP definition ingestion
remains open. Its Unix process adapter accepts
explicit locally authorized launch inputs; it does not grant permission to
launch synchronized definitions, invoke tools, read resources, or request prompts.

`start_stdio_client` accepts a caller-owned transport and either `Auto` or a pin
to one of the five final versions in EDL-247. Auto prefers `2026-07-28` discovery.
Only an exact-ID-correlated JSON-RPC `METHOD_NOT_FOUND` permits legacy initialize
on the same transport. Uncorrelated/missing IDs and arbitrary server errors do
not authorize fallback. This adapter is specific to stdio; HTTP requires separate
structured unsupported-version handling.

The SDK's Auto mode attempts legacy initialize when discovery takes ten seconds.
The guard rejects that attempt before sending anything. This inherited SDK
startup ceiling currently means a slow Auto discovery fails; it never silently
downgrades. Callers must supply their own overall deadline and cancellation for
other startup paths. No Kiln-wide timeout or capacity default is established by
this increment.

Legacy pins send the exact pinned version and reject another returned version
before `notifications/initialized`. Auto accepts only final legacy versions from
legacy initialization. Modern discovery is restricted to `2026-07-28`; a modern
pin cannot fall back. Rejected legacy versions close the transport and fail
startup. Protocol acceptance is not evidence that every feature of that version
is implemented or that conformance has passed.

The integration fixtures exercise negotiation messages, including timeout and
error downgrade prevention, exact legacy pins, modern startup, and rejection of
draft/incorrect-era results. Run `cargo test -p kiln-mcp` from the workspace.
These in-memory fixtures are not the official MCP conformance suite.

`StdioTransport` provides newline-delimited JSON framing with an explicit positive
caller-supplied wire budget, including the newline. It rejects oversized input
while reading, without waiting for EOF, and stops on malformed or incomplete
frames. Outgoing serialization obeys the same budget before writing any bytes.
Cancellation preserves a partial receive; cancelling a partial write closes the
writer so another request cannot append to the damaged frame. It logs no raw
frames. This deliberately stricter transport avoids the SDK's unbounded
`read_until` buffer and malformed-input recovery behavior. It supplies no default
frame limit; the host must choose one from its resource budget.

On Unix, `StdioProcess` starts one process generation with an absolute executable
and a pinned working-directory descriptor, explicit arguments and environment, piped protocol I/O,
and discarded stderr. It clears the daemon environment and uses a separate process
group. It is an execution adapter: the caller must authorize and resolve all launch
inputs first. The caller opens and validates the directory descriptor against its
scope before passing ownership to the adapter. The child uses `fchdir` on that
descriptor before exec, and close-on-exec prevents leaking it to the server.
Renaming/replacing the directory's former pathname cannot redirect launch.
The adapter itself does not bind these inputs to a durable scoped instance;
`StdioGeneration` provides that internal runtime composition.

Explicit close shuts stdin, allows the caller's shutdown grace, signals remaining
group members, and reaps the direct child. The leader stays unreaped for the full
grace interval to prevent numeric PID reuse before group cleanup. Drop forces
group cleanup and delegates direct-child reaping to Tokio; orderly daemon shutdown
must therefore close instances while its runtime is alive. On macOS, XNU excludes
zombies when signaling groups and can report `EPERM` after graceful exit. The
adapter accepts that case only after `waitid(WNOWAIT)` confirms its leader exited.
The process-group boundary covers descendants retaining the caller's credentials
and group; processes escaping those constraints require an OS sandbox. Windows
process ownership is not implemented. There is no default grace or startup deadline.

Real-process fixtures cover environment/cwd isolation, legacy negotiation,
idempotent close/reaping, EOF shutdown, descendant cleanup, malformed output,
startup cancellation, and directory-path replacement after pinning. These are local macOS observations; Linux
execution and full MCP conformance remain unverified.

`StdioProcess::into_managed` transfers the transport to the SDK while retaining a
single-use cleanup receipt. Once startup has failed/been cancelled or the running
service has stopped, `StdioProcessCleanup::finish` obtains the returned process
and explicitly closes/reaps it. This allows a lifecycle owner to require observed
cleanup before recording a terminal generation. Dropping either side provides
only the forced-cleanup backstop, never a successful cleanup receipt. The owner
must stop the SDK before awaiting the receipt; it does not interrupt a live SDK
service itself. Canceling `StdioProcess::close` while retaining the process object
retains its child handle for a later close retry; the single-use cleanup receipt
must itself be awaited to completion to establish cleanup. Real-process fixtures
verify the receipt after successful service cancellation, malformed startup, and
caller-cancelled startup.

Core `McpServerDefinition` validates canonical local registration metadata using
the same transport-field validation as shared snapshots. It records a source of
`local`, an exact final protocol pin or `auto`, lifecycle scope (default
`workspace_checkout`), optional host-local auth profile, enablement, and the
`kiln_mediated_serial` trust policy. Transport metadata contains host-binding
references; registration resolves no path or credential and grants no execution
authority. Protocol policy types are shared with this SDK adapter.

Infrastructure migration 51 stores immutable definition versions, the current
version, exact command retry receipts, and metadata-only registration audit rows
in one transaction. Registration requires the expected current version (zero for
an unseen ID). Replaying an earlier command returns its original record without
changing the current version. Reusing a command key with changed content or
preconditions conflicts. Reads preflight stored metadata size in SQLite using
caller-provided definition budgets. There is no default definition size limit.
The offline `kilnd register-mcp-definition` and `inspect-mcp-definition` commands
expose this store under the exclusive daemon lock; see [local administration](../../docs/operations/mcp-runtime.md#local-definition-administration). Online registration,
shared-source ingestion remain open. Native stdio launch is opt-in. Audit rows are not yet
exposed through client event replay.

Core `McpInstanceKey` separates instances by definition, concrete owner, and auth
profile. Checkout owners include workspace/root IDs, resolved paths, relative
directory and filesystem identity; session ownership is a distinct explicit scope.
These keys are metadata and confer no launch or filesystem permission.

Migration 52 journals instance generations, desired/observed state, selected
protocol, state versions and metadata-only lifecycle events. Atomic claims return
`Acquired` only for a newly recorded generation; retries and competing claims see
`Existing` and must never spawn from that result. Claims recheck current definition
version, enablement, scope/profile and stored owner metadata. A changed definition
cannot silently replace an active generation. Readiness also rechecks definition
version and exact protocol pins. Trusted lifecycle owners report process cleanup
before `Stopped` or `StartupFailed`; desired stop alone is not cleanup evidence.

State-transition events double as exact retry receipts. A historical retry may
return an old generation's original state without changing the current generation;
that receipt is not current readiness or execution authority. Restart recovery,
called under exclusive daemon ownership before dispatch, marks active generations
interrupted in caller-sized batches and retains desired state. Interrupted
generations cannot be replaced until cleanup is confirmed. This records lost state
without replaying calls, but it does not locate, kill or adopt orphan processes.

Focused SQLite fixtures prove independent-connection competing claims, scoped
checkout/profile separation, stale/pinned readiness rejection, historical retry
behavior, restart interruption and transaction rollback. The daemon's opt-in
internal runtime consumes the restart and shutdown paths; complete process
orphan recovery remains open; approved stdio tool execution is opt-in.

On Unix, `StdioGeneration` composes the store ports with the process and SDK
adapters. Its trusted caller supplies separately authorized host-resolved inputs,
a generation ID and an absolute startup deadline. The worker reads the stored
protocol policy, rejects non-stdio definitions, and starts a process only after an
`Acquired` claim. `Existing` returns a typed error without launching or stopping
the other owner's process. Scoped in-memory reuse belongs to `StdioRegistry`.

The worker publishes readiness only after the store accepts the negotiated
version. Startup failure, timeout or rejected readiness closes/reaps the process
before a failed state is attempted. Dropping the handle or cancelling `stop`
requests shutdown while the detached worker continues cleanup and journaling.
Explicit `stop` awaits that worker. Disconnect records interruption and then
verified cleanup, retaining desired-running state without restarting or replaying.
Storage conflicts or uncertain cleanup leave the generation nonterminal; no
replacement is authorized on that basis. The lifecycle worker must be the sole
state-transition writer for its generation; stop requests go through its handle.

SQLite plus real-process fixtures exercise duplicate claims, readiness, stop and
replacement, handle drop during silent startup, timeout, changed definitions
during negotiation, disconnect, and cleanup despite a failed stop-journal write.
The latter retains the uncertain active record and blocks replacement rather
than reporting a terminal state. These are macOS observations. Daemon startup
records interrupted generations under exclusive store ownership, and shutdown
drains the registry. Orphan process reconciliation remains open. Readiness snapshots
do not authorize execution; the opt-in native coordinator revalidates current
definitions and passes normal durable ToolCall approval.

`StdioRegistry` retains those owners across Run waiters, keyed by the canonical
definition/owner/auth-profile identity. Concurrent demand joins one startup;
cancelling a waiter does not stop the shared server. Every successful return
checks current definition enablement/version and durable ready generation/state,
instead of treating a cached readiness receipt as current authority. A positive
caller-supplied capacity bounds retained owners; finished entries release slots
only when process cleanup is known. Failed cleanup and lost workers retain their
slots and remain visible to shutdown reporting. Durable uncertain claims also
prevent replacement.

The caller supplies a nonsecret local binding revision and must change it whenever
resolved executable, arguments, environment, credentials, directory authorization
or launch policy changes. Changed revisions or definitions require an explicit
scope stop before reuse. The registry does not resolve or persist these bindings.
It also retains a close-on-exec descriptor for the admitted working directory and
compares device/inode identity on every demand. A different directory is rejected
even with an unchanged revision; a non-directory descriptor is rejected before
startup or reuse. This protects broader lifecycle scopes from silently inheriting
another caller's cwd. It does not establish scope authorization or constrain a
server that changes its own cwd. The opt-in daemon coordinator enforces the durable approval boundary.
Scope stop retains its owner until cleanup completes. Shutdown seals the registry,
signals every owner before waiting, and can be awaited again after caller
cancellation. Dropping the registry requests stop but cannot prove completion;
the daemon must await shutdown before destroying its Tokio runtime.

Real-process registry fixtures verify concurrent reuse, binding/definition change
guards, cancellation of a startup waiter, repeated shutdown after cancellation,
capacity admission and slot release. The registry also routes durable dispatch
permits to the scoped owner, as described below. No Run is made the owner of a
shared server.

`resolve_stdio_launch` substitutes a materialized `StdioHostBindings` snapshot into
one exact definition version and instance key. Runtime, argument and environment
bindings are separate: a missing value never falls back to another role, PATH,
the daemon environment or a shell expression. It checks definition enablement,
scope/profile identity, runtime key, absolute executable and NUL-free strings.
A caller byte budget bounds all resolved executable/argument/environment strings,
including terminators/separators; it is not a total process-memory limit. The
resolver carries the local binding revision into its result for registry admission.

The snapshot and output have no Debug/serialization implementation. Values may
contain private local paths or credentials supplied by a separate authorized
credential boundary; provider credentials must never be passed to an MCP server.
Resolution itself neither reads a vault nor grants launch permission. Its caller
must authorize all inputs and supply a directory descriptor pinned against the
owner scope before using the result. Durable administration stores reference
snapshots rather than these materialized values. Real-process and validation
fixtures cover explicit literal substitution, version/profile/runtime/role
mismatch, NUL rejection and the exact resolved-byte boundary.

`resolve_stdio_launch_from_vault` is the reference-backed companion. It accepts
an already authorized `StdioHostBindingReferences` snapshot and reads only the
argument/environment references used by the exact definition. Invalid versions,
roles, runtime keys, missing mappings or an executable that cannot fit fail before
vault access. Each unique reference name is fetched once per role. Missing vault
entries fail without fallback or further reads. The retained-value budget and
final encoded launch budget are both enforced; one OS read can still allocate up
to SecretValue's existing per-value ceiling. The vault envelope is nonempty and
single-line; it does not replace the materialized resolver for other host values.

Core `McpSecretStore` is separate from provider and configuration-sync secret ports.
`OsMcpSecretStore` uses the `dev.kiln.mcp` OS service. Its hashed, length-framed lookup
identity includes local Kiln instance, canonical scoped server/auth-profile key,
binding name, argument/environment purpose and immutable SecretRef. Paths and
binding names are not exposed in vault account metadata. Writes/deletes reuse the
existing cancellation-safe per-entry serialization; clones share those locks.
Callers must reserve fresh references durably before writes and change binding
revision when values or authorization change. Durable reservation and snapshot
publication have internal store ports and an offline administration command.
This code does not access a user's vault at
startup. Tests verify namespace partitioning and reference resolution with a fake
vault; actual MCP OS-vault read/write integration remains unverified.

`resolve_persisted_stdio_launch` accepts a stored `McpHostBindingRecord`, resolves
its references and carries its local instance/revision into the generation claim.
It requires a caller-authorized checkout equal to the snapshot's working-directory
selection before reading secrets. The caller must revalidate approval and root
registration and supply a descriptor pinned from that exact checkout. Snapshots
without a directory remain readable for administration but cannot use this resolver.
SQLite serializes that claim with host snapshot publication. A stale revision
cannot spawn; an active or interrupted generation blocks replacement until its
cleanup is established. Legacy materialized launches remain available only for
keys without a persisted snapshot. Published references are excluded from pending
cleanup; replacing them retires the old references atomically. Exact publication
retries return immutable receipts without rolling the current snapshot back.
Snapshots persist absolute UTF-8 Unix executable paths, reference maps and an
optional registered working-directory checkout. Publication and generation
admission recheck its available root and filesystem identity in the transaction;
workspace/checkout owners must match, and session owners must belong to its
workspace. Core ownership still requires an explicitly registered checkout.
`retire_mcp_host_bindings` advances the revision and retires its references
atomically after process cleanup. A retained removal tombstone blocks all launch
claims until explicit republication; retired records fail resolution before vault
reads. Exact retirement retries never remove a newer publication. Public
administration is available through the offline `kilnd mcp-host-admin` command;
see `docs/operations/mcp-runtime.md` for its pipe-only schema, import/publication
ordering and explicit cleanup semantics. Neither the command nor these ports grant
host authorization or install MCP in the daemon native catalogue.

`execute_stdio_call` composes the internal approved stdio path. It consumes a
fresh native MCP claim, reads a consistent launch context from `McpLaunchStore`,
pins its checkout through a trusted callback, resolves only that host's vault
references, rechecks the context after preparation, joins the registry owner,
claims one dispatch and captures its receipt-backed output. The SQLite preflight
checks the live Run/ToolCall and immutable proposal, derives all four lifecycle
owners from the definition and approved scope, and requires a current matching
local-instance snapshot. `pin_mcp_working_directory` reuses infrastructure's
no-follow, root-identity and device-boundary directory traversal.

Dispatch also rechecks the bound host revision, directory registration and scope
transactionally; removing the host version from a supplied ready receipt cannot
bypass this check. Preparation cancellation/deadline expiry sends no invocation,
but may leave shared registry startup running. Once dispatch is claimed, its
existing cancellation/uncertainty rules apply. No error permits replay. The trusted
caller must persist completion and events. The opt-in daemon coordinator now does
so after catalogue/schema validation and receipt-backed output capture.

A real macOS fixture verifies one process reused across two approved Runs, exactly
two sends, normal ToolCall completion, uninitialized local-host rejection,
cancellation during preparation and root-change rejection before dispatch. These
checks use no vault credentials and do not establish Linux/full conformance.

The [internal daemon runtime](../../docs/operations/mcp-runtime.md) is opt-in with
explicit instance-capacity and recovery-batch budgets. Its startup runs before
dispatch under the store lock; its Run-service shutdown drains registry owners.
An additional listener-exit drain covers a dropped graceful-shutdown future.
A separate `KILN_NATIVE_MCP_LIMITS` opt-in installs the compact native tools;
lifecycle settings alone do not advertise them.

Core now defines the compact `mcp_call` native proposal contract for tool calls,
resource reads and prompt retrieval. Each proposal pins a server definition ID and
version, and retains the complete operation and arguments for approval inspection.
Its caller supplies a positive whole-request byte budget; that budget is part of
the frozen catalogue definition. Parsing rejects unknown fields, noncanonical or
duplicate keys, invalid versions, control characters in targets, and non-string
prompt argument values. It resolves no server, schema reference or credential.

Migration 53 records one MCP dispatch claim per already-running native ToolCall.
The store rechecks the frozen proposal, live Run/ToolCall scope, current enabled
definition and ready generation in one transaction. A partial unique index
serializes dispatch per generation regardless of annotations. Only a fresh native
claim plus a newly inserted record yields a non-cloneable `McpDispatchPermit`;
existing records, including interrupted ones, yield receipts only. Outcomes and
metadata-only audit rows commit together; exact terminal retries are no-ops and
conflicting outcomes fail. Startup interrupts unfinished invocation records after
interrupting generations. `dispatching` means a send may have occurred, not proof
that a server accepted or completed it. Dropping a permit grants no replay.

The generation-owned SDK worker consumes a permit through `StdioGeneration::dispatch`
or the registry dispatch route. It sends at most one raw operation request, avoiding
SDK resource-cache fallback and automatic MRTR rounds. Results are serialized
within an explicit caller byte ceiling in addition to the transport frame ceiling;
no partial result is returned. Tool `isError` and structured protocol errors record
failure. The worker returns typed errors without including raw server error text. Every call also has an explicit
absolute deadline and cancellation receiver; dropping its sender requests
cancellation, as does abandoning the reply waiter.

Before a tool invocation, the worker traverses `tools/list` with explicit positive
page, entry-count and cumulative encoded-byte budgets from `McpCatalogLimits`.
It requires advertised tool support and rejects duplicate names, repeated cursors,
incomplete traversal and unknown selections. Metadata is fetched afresh for each
call, including reuse of an existing process; annotations confer no permission.

The selected tool's object input schema is validated by `jsonschema` 0.58.1, with
default HTTP/file retrieval features disabled and a retriever that denies all
external schema access. Inline references and declared standard drafts are
supported; absent a dialect, the validator uses draft 2020-12. Caller budgets also
bound regex backtracking and per-pattern compiled/DFA sizes. These are resource
allowances, not a hard wall-clock or total validator-memory guarantee; synchronous
schema compilation/validation cannot be preempted by the async deadline.

Invalid metadata/schema/arguments fail before `tools/call`. A declared output
schema is compiled before sending and checked against successful structured
content afterward. Missing or invalid structured output produces a Failed outcome
that explicitly preserves possible external effects and never retries the call.
Tool error results need not satisfy the success output schema.

Prompt retrieval requires advertised prompt support and a complete bounded
`prompts/list` traversal using the same page/count/byte accounting. Duplicate
names/cursors, unknown prompts, malformed/duplicate argument declarations, missing
required values and undeclared arguments fail before `prompts/get`. Prompt content
remains untrusted ToolCall output; it is not promoted to system instructions.

Resource reads require advertised resource support and an absolute RFC 3986 URI.
`fluent-uri` validates syntax without normalizing the approved URI. The broker
forwards it verbatim to `resources/read`; it never opens a file, fetches a URL or
expands a template locally. Resource response URIs are validated too, while
allowing valid sub-resource URIs different from the requested URI.
[The MCP specification](https://modelcontextprotocol.io/specification/2025-11-25/server/tools#resource-links)
explicitly permits resource links absent from `resources/list`, so discovery is
not used as an authorization allowlist. These validations do not install
`mcp_call` in the daemon catalogue.

`discover_catalog` now provides a low-level, fresh metadata adapter for tools,
prompts, resources and resource templates on an already authorized peer. Each
selected list must complete within explicit page, entry, encoded-byte and absolute
deadline budgets; partial lists are never returned. Duplicate tool/prompt names,
resource URIs or template identifiers fail. Resource names may repeat when their
URIs differ. Resource URIs are syntax-checked and retained verbatim. Templates
remain opaque untrusted descriptions: no RFC 6570 validation, expansion, content
read or local URL/file access occurs. The adapter bypasses SDK cache fallback.

The caller still owns generation identity, serial scheduling and the durable
ToolCall boundary. Returned entries carry no authority or cache validity and must
not be reused across calls as a current snapshot.

Core `McpTools` now defines three compact native contracts: `mcp_call`,
`mcp_search` and `mcp_describe`, with distinct `kiln.mcp.*` capabilities. Its
private parsed `McpCommand` fixes the capability and native name as well as the
server/version/arguments. Launch preflight and dispatch recheck that exact name,
capability and canonical proposal against the live approved native claim. Search
and describe therefore share the normal serial invocation journal, no-replay rule,
receipt-backed output capture and ToolCall completion path; discovery is not an
approval bypass. The daemon installs all three only with explicit native MCP
limits and its lifecycle registry enabled.

Search takes `server_id`, `definition_version`, `kind`, `query`, `offset` and
positive `limit`. Kind is `tool`, `prompt`, `resource` or `resource_template`.
It traverses a fresh complete bounded catalogue, matches a lowercase substring
against identifiers/names/titles/descriptions, sorts by exact identifier and
returns only identifier/name/title summaries, total matches and `next_offset`.
Empty query lists all; no schemas or annotations enter search output. Describe
takes the same server/version/kind plus an exact `identifier` and returns only
that entry's full metadata, including its schema. Resource identifiers are exact
URIs; template identifiers are the opaque template strings. Missing selections
fail without executing a tool, reading a resource or retrieving a prompt.

Both results include definition/version, generation, protocol version and kind
provenance and obey normal output byte/artifact limits. Offsets address the fresh
traversal, not a stable cross-request snapshot; a changing server can change the
next page. Generation-indexed caching/invalidation and stable paging remain open.
Synchronous projection is bounded by metadata budgets but is not CPU-preemptible;
cancellation and deadline are checked again before accepting its output.

Cancellation, deadline expiry during a possible send, unexpected response types,
and connection loss record an interrupted/unknown outcome and retire the process
generation. Durable readiness is removed before releasing an interrupted call's
serial slot. Cleanup also interrupts any claim queued just before retirement.
This conservative cancellation loses server application state; it does not prove
that an external side effect was undone. MRTR/input-required and external-task
responses currently interrupt and retire without automatic follow-up. Their full
mediation is still open. Ordinary successful calls reuse the generation across
Runs; caller abandonment never replays the operation. Journal failure still
triggers process cleanup and is reported as a store error.

`dispatch_tool_call` on the generation or registry adds normal ToolCall output
capture. The worker supplies its committed invocation receipt with the response;
missing or inconsistent receipts leave completion unresolved instead of inventing
a terminal ToolCall result. Exact UTF-8 response bytes stay inline up to the common
4-KiB tool-output allowance. Larger responses go through a caller-supplied archive
callback using `TOOL_OUTPUT_MEDIA_TYPE`, with returned size/media type checked.
Tool `isError` responses retain their output while producing Failed results.
Capture/storage failure produces a Failed result without reissuing external work.

Only proven pre-send cancellation produces Cancelled. Interrupted operations and
unsupported continuations produce Failed diagnostics explicitly preserving an
unknown external outcome; they do not claim effects were undone or process cleanup
has already finished. A lost worker or invocation-journal failure supplies no
normal completion result. The trusted caller must persist returned results through
the ordinary native completion store and publish its events.

The daemon's explicit native MCP opt-in connects these APIs through a combined
file-read/MCP catalogue, normal approval, fresh claims, host pinning and receipt-backed
ToolCall completion. A core-owned consuming split retains the original command,
source and scope; it exposes no arbitrary request transformation. Invocation audit
rows are not yet in public Event replay. A real Python stdio fixture on macOS verifies single sends for success,
server error, oversize output, disconnect, cancellation and deadline expiry;
successive claims from distinct Runs reuse the same process. Inline and large
artifact results also pass through the native ToolCall completion store in that
fixture, preserving output bytes and single-send counts. It requires
`/usr/bin/python3`. Resource/prompt wire execution, modern MRTR and external Tasks
are not covered by that fixture.

Remaining broker work includes process recovery, online registration and online
host-binding/credential administration, catalogue caching/invalidation and stable
result paging, HTTP/OAuth, mediated server requests, and full conformance. Native
MCP remains disabled unless the host supplies all explicit runtime limits.
