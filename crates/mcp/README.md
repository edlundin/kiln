# MCP broker boundary

This crate begins EDL-314 with a guarded stdio negotiation adapter over the
official `rmcp` SDK, pinned to 3.4.1. It is not yet connected to daemon tool
execution or synchronized MCP definitions. Its Unix process adapter accepts
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
The adapter does not yet bind these inputs to a durable scoped instance.

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
The internal store port has no public registration API, shared-source ingestion,
or process-start side effect yet. Audit rows are not yet exposed through client
event replay.

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
behavior, restart interruption and transaction rollback. The daemon does not yet
consume these ports, so end-to-end process reuse/recovery is not claimed.

Remaining broker work includes runtime integration of scoped-instance claims and
process cleanup/recovery, public registration and host-binding resolution, durable
ToolCall lifecycle and invocation events, Kiln grants/approvals, catalogue/result
paging, HTTP/OAuth, mediated server requests, and full conformance. No MCP operation
is offered to models until it is connected to the normal durable ToolCall boundary.
