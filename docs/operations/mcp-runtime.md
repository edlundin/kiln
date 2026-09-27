# Internal MCP runtime

On Unix, `kilnd` can retain scoped MCP stdio and HTTP owners and drain them during
shutdown. Native model Runs can opt into `mcp_search`, `mcp_describe` and `mcp_call`
through the separate limits configuration below. There is no public direct server
launch command. Offline host-binding administration is described below.

### HTTP transport preparation

Runtime clients identify as `kiln` with the workspace package version, both in
legacy initialization and modern per-request metadata. Sampling, elicitation,
roots and other client capabilities remain unadvertised until mediated support
is implemented.

`kiln-mcp` provides an internal `BoundedHttpClient` adapter for Streamable HTTP.
The daemon uses it only through the explicitly enabled native path and a published
host snapshot. It requires explicit request, JSON response, whole SSE stream,
per-event and header byte allowances plus a whole-request timeout. Per-event
accounting treats CRLF as one terminator;
whole-stream accounting counts every wire body byte. HTTP stack buffers, parsed
response headers and decoded values have additional memory costs; these limits
are not a process-wide memory or CPU sandbox.

The adapter pins one endpoint, requires HTTPS except for loopback HTTP, disables
redirects, ambient proxies and reqwest retries, and rejects framing/authentication
header overrides. JSON and SSE responses are bounded before retained parsing;
transport errors omit response bodies, endpoints and credentials. Correlated
JSON-RPC errors remain protocol messages. Arbitrary HTTP failures never synthesize
a discovery response or authorize a version downgrade. Session expiry is reported
without replay in the adapter.

`http_generation_transport` now constructs a protocol-pinned SDK worker with
guarded HTTP I/O. Modern generations reject GET/resume, session IDs and initialize;
legacy generations permit at most one initialize. SDK session recovery/replay is
disabled. Guard-detected network/protocol/limit failures close admission, while
one separate legacy DELETE cleanup attempt remains available. Already admitted I/O can still
finish; failure does not undo remote effects. These are in-memory transport guards,
not durable generation retirement or permission to dispatch.
Requests require a correlated JSON response or an SSE response stream; HTTP 202
cannot leave a request pending indefinitely. Notifications and replies require
the accepted response instead.

Hosts must additionally choose a channel capacity, lifetime POST/GET exchange
allowance and cumulative encoded tools/list allowance. The latter conservatively
bounds the SDK's retained tool-header schema cache, including changing names;
repeated catalogue results also consume it. Exhaustion requires owner retirement,
never silent budget reset. These are encoded-byte and count bounds, not exact
heap-size measurements. The worker serializes ordinary POSTs and keeps its separate
control slot available for cancellation/replies.

Legacy SSE reconnects require an explicit `legacy_resume_delay`; all attempts
consume the same exchange allowance. Network interruptions can resume, while
malformed or over-budget streams cannot. Modern HTTP requires that option unset
and has an I/O guard because SDK `NeverRetry` alone does not prevent every resume.
The 2024 HTTP+SSE adapter is separate and remains unimplemented.

`start_http_client` now supports exact 2025/2026 pins and modern-first Auto startup.
Auto requires a correlated `UnsupportedProtocolVersionError` (`-32022`) naming
the requested modern version and a supported legacy Streamable HTTP revision.
It selects the newest mutually supported legacy revision and makes at most one
legacy attempt. A plain HTTP 400, method-not-found, authentication/server failure,
malformed or uncorrelated response, or an incompatible discovery success cannot
authorize fallback. The configured startup deadline covers both attempts.
Structured startup errors arriving over SSE are preserved before the SDK can
discard or log those frames. Server-request mediation before startup completes
is not yet supported.

This deliberately follows EDL-247's stricter fallback rule. The upstream 2026
HTTP compatibility guidance also permits legacy detection from non-modern HTTP
400 bodies; Kiln rejects that automatic path. Such servers need an explicit legacy
pin. This policy difference is not a claim of full upstream conformance.

`McpHttpTransport` owns the SDK worker through shutdown. Startup failure or timeout
waits for the old worker to join before returning or starting a fallback. Dropping
the transport or cancelling a close waiter still leaves an owned cleanup task.
The startup deadline does not truncate cleanup; shutdown may therefore finish
after it. Completion proves local worker termination, not remote session deletion
or cleanup of a session created by a malformed handshake.

`start_managed_http_client` returns the startup future and a separate
`McpHttpClientCleanup` handle before any I/O. Retain the handle, drop/cancel startup
or cancel the running client, then await `finish` before releasing durable
ownership. It tracks both permitted attempts and remains usable after a cancelled
cleanup wait. Dropping startup without ever polling it creates no worker. The
cleanup handle observes termination; it does not itself request cancellation.

`McpGeneration::spawn_http` now owns an already-authorized resolved HTTP launch.
It claims the exact definition and host revision before creating a transport,
shares the serial dispatch/receipt loop with stdio, and awaits all local worker
cleanup before reporting terminal lifecycle state. Dropping the owner requests
stop while its task retains cleanup and journaling responsibility. Legacy fallback
stays inside one durable startup owner; existing active/uncertain instances cannot
be replaced. `StdioGeneration` remains a compatibility name for the shared owner.
The daemon now routes explicitly enabled HTTP launches through the same registry
and approved broker path. Every reuse rechecks definition/readiness and pinned
directory identity; vault resolution is followed by a fresh metadata/revision
inspection before generation admission and single-dispatch claim.

OAuth, live credential administration and official HTTP conformance remain open. Tests use local HTTP
socket fixtures and the real SDK worker, not remote services or real credentials.

### Stdio lifecycle settings

Both settings below are required to enable the runtime. If neither is set, it is
disabled. Missing, zero, invalid or overflowing values prevent startup when either
setting is supplied. There are no product defaults.

| Environment variable | Meaning |
| --- | --- |
| `KILN_MCP_MAX_INSTANCES` | Positive maximum scoped owners retained by the daemon registry, including uncertain cleanup. Choose from the host's process and memory budget. |
| `KILN_MCP_RECOVERY_BATCH_SIZE` | Positive maximum generation rows interrupted per startup transaction. Choose for the store's transaction and startup budget. |

### Native tool opt-in

`KILN_NATIVE_MCP_LIMITS` enables the three compact MCP tools for native providers
that support tool calls. It requires the lifecycle registry settings above.
Unset means disabled. Its value is one UTF-8 JSON object with every field below;
missing, unknown, duplicate, zero (except shutdown grace), invalid or overflowing
values fail startup. This is host configuration, never model-supplied input.
Choose allowances for actual server metadata, process/memory budgets and permitted
latency; there are no product defaults.

| JSON fields | Meaning |
| --- | --- |
| `max_request_bytes` | Positive canonical native proposal byte ceiling. |
| `max_definition_key_bytes`, `max_definition_bytes` | Positive server/host metadata key and total metadata ceilings. |
| `max_arguments`, `max_argument_bytes`, `max_environment`, `max_endpoint_bytes` | Positive definition argument/environment count and string allowances. |
| `max_resolved_bytes` | Positive total resolved launch-value byte budget. |
| `max_frame_bytes`, `max_result_bytes` | Positive MCP wire-frame and encoded response ceilings. |
| `max_catalog_pages`, `max_catalog_entries`, `max_catalog_bytes` | Positive complete-discovery traversal budgets. The byte ceiling also bounds aggregate retained snapshot metadata per generation. |
| `max_regex_bytes`, `max_regex_backtracks` | Positive per-pattern compiled/DFA size and backtracking allowances. These do not isolate total validator CPU/memory. |
| `startup_timeout_ms`, `call_timeout_ms` | Positive relative startup and whole broker-call deadlines, in milliseconds. |
| `shutdown_grace_ms` | Nonnegative process shutdown grace, in milliseconds. |

HTTP is disabled unless `KILN_NATIVE_MCP_LIMITS` also contains a non-null `http`
object. Its required positive fields are `max_request_bytes`, `max_response_bytes`,
`max_stream_bytes`, `max_event_bytes`, `max_header_bytes`, `request_timeout_ms`,
`channel_capacity`, `max_exchanges` and `max_catalog_lifetime_bytes`. Choose them
for the allowed request/result sizes, retained catalogue workload and permitted
network latency. They are independent from the outer native-proposal and result
budgets. `legacy_resume_delay_ms` is optional/null to disable legacy SSE reconnect;
when supplied it must be positive. It is invalid for an exact modern pin and
applies only to a permitted legacy fallback under Auto. Unknown fields, zero
allowances or missing required fields reject startup. Existing stdio settings
remain valid without this object.

Both transports use the same local approval, exact registered directory and host
revision checks, shared-owner capacity, serial dispatch receipts, result artifacts
and paging. Streamable HTTP supports the 2025 revisions and 2026 stateless mode;
2024 HTTP+SSE, OAuth and server-request mediation are not yet implemented. Use
`publish_http` below to select a host endpoint/credential snapshot. The local
HTTP fixture covers modern and legacy approved search/describe/call, artifact
paging and completed-batch no-replay; it does not establish remote TLS, actual
OS-vault or full protocol conformance acceptance.

The internal mediation journal (migration 59) records input kind and ordinal on
the original invocation, with required/resolved/interrupted states. It retains
the dispatch slot while input is pending. Invocation termination and startup
interruption atomically interrupt any pending input. Exact receipt retries do
not authorize provider calls, responses or operation replay. New input and
resolution require the original live native claim and ready generation at the
current definition version. Bodies, server request IDs, responses and opaque
request state are not stored in this journal. Input transitions now project into
public session events; runtime mediation and interaction routing are not yet
connected.

The internal roots lookup accepts only a pending roots input on a live invocation.
It resolves the generation's exact local host revision and compares its directory
to the ToolCall's approved workspace, root and relative scope. The returned path
includes that relative scope; it does not expose the enclosing workspace root.
Stale registered directory metadata, lost readiness, cancellation and resolved
inputs reject the lookup. This reads metadata only: it neither resolves the
input nor sends an MCP response, and it performs no filesystem or vault access.


Native file reading remains independently configurable. The daemon freezes one
combined native catalogue and routes each approved command through a consuming
core-owned split that retains its original ToolCall, source and scope. MCP uses
the current local-instance host snapshot, pinned authorized checkout and isolated
`dev.kiln.mcp` vault; enabling tools grants no new credential or filesystem scope.
Directory pinning and output artifact writes run off the async executor. Run
cancellation is forwarded to the broker and awaited through its receipt/cleanup
path. Missing dispatch/journal evidence remains unresolved rather than being
converted into an invented terminal result or replay.

Startup runs recovery after acquiring the daemon's exclusive store lock and
before serving requests. It marks active generations interrupted in batches,
retaining desired state and last negotiated protocol. It then marks unfinished
MCP invocation claims interrupted using the same transaction batch budget. Those
records represent unknown external outcomes and never authorize redispatch.
Startup does not adopt or kill orphan processes and does not replay invocations. An interrupted generation stays
blocked against replacement until cleanup is confirmed; enabling this runtime
does not by itself make crash recovery complete.

The registry belongs to the daemon, rather than a Run. Concurrent callers for a
scope share startup and reuse a valid ready generation. Reuse rechecks the stored
definition and current generation. Host-local binding revisions must change when
resolved launch inputs or authorization change; a revision change requires an
explicit stop before replacement. The internal resolver accepts already authorized,
materialized host values and a pinned directory; it substitutes only explicit
runtime/argument/environment references within a caller byte budget. Persistent
bindings have an offline administration command below. A reference-backed
resolver can read scoped argument/environment values through the separate MCP
vault port; the native opt-in installs this path behind durable approval and current
host-snapshot checks. Credential resolution alone does not authorize launch.
Durable reference reservation and snapshot publication are available internally;
credential import/removal uses the offline administration command below.

The registry independently checks the pinned directory's device/inode identity
before reuse, including for session, workspace and core owners. A different
directory requires an explicit stop; an unchanged binding revision cannot bypass
this check. This is a runtime consistency guard, not launch authorization or a
filesystem sandbox. The internal approved-call composition now connects durable
scope checks, directory pinning, host resolution and single dispatch. The native
opt-in connects this path to the daemon coordinator and ordinary ToolCall completion.

Internal tool dispatch now fetches a bounded `tools/list` catalogue and validates
the selected tool's input schema before sending `tools/call`. Hosts must supply
page/count/byte and regex budgets; there are no defaults. External schema URL/file
retrieval is disabled. Successful structured output is checked against any declared
output schema; a failure after sending does not imply effects were undone.
Prompts also require bounded discovery and valid declared arguments. Resource
reads require the resource capability and a valid absolute RFC 3986 URI, forwarded
unchanged to the MCP server; file/HTTP URIs are never dereferenced locally.
Resource links need not appear in `resources/list` under the MCP specification,
so the broker does not impose a catalogue membership requirement. A low-level
fresh catalogue adapter now lists tools, prompts, resources and
resource templates with complete bounded traversal and an absolute deadline.
Templates remain opaque metadata; no expansion or content read occurs. It grants
no execution or cache authority.

Internal `McpTools` contracts now include `mcp_search` and `mcp_describe` alongside
`mcp_call`, each with a separate capability and normal durable approval/claim
checks. Search emits compact identifier/name/title summaries with deterministic
identifier ordering and explicit offset/limit; describe emits the selected full
metadata entry and schema. Fresh requests use complete bounded discovery and include
server definition/version, generation and protocol provenance. For example:

```json
{"server_id":"notes","definition_version":1,"kind":"tool","query":"write","offset":0,"limit":5}
```

The corresponding `mcp_describe` arguments replace query/offset/limit with
`"identifier":"write_note"`. Kinds also include `resource`, `prompt` and
`resource_template`; their identifiers are exact URI, prompt name and template
string respectively. Neither operation executes the selected item. Both return
`catalog_snapshot`; pass it back as `snapshot` for a stable search continuation or
matching description. A nonzero offset requires that token. Omitting it refreshes
the selected kind. Search/describe contracts are revision 2; old revision-1 frozen
proposals are rejected rather than silently reinterpreted. `mcp_call` stays revision 1.

One snapshot per kind is retained within the generation's scope, auth profile and
protocol. Total encoded snapshot metadata is capped by `max_catalog_bytes`;
page/entry limits are also rechecked on reuse. Refresh replaces the same kind's
token; other kinds are evicted oldest-refresh-first when space is needed. Generation
loss, notification invalidation, replacement, eviction or tightened budgets produce
an explicit unavailable-snapshot failure without refetching. Snapshot tokens grant
no authority, and all calls still validate fresh metadata. The daemon advertises
these contracts only with the explicit native opt-in above.

Generation-owned SDK handlers now observe tool/prompt/resource list-change
notifications. They reject discovery results or tool/prompt validation that
crosses an observed change before dispatch; resources and templates share the
resource epoch. Resource reads still do not require list membership. Search and
describe include `catalog_notification_epoch` as provenance. It counts observed
notifications only, not silent server changes or a stable snapshot ID. Retained
snapshots are checked against their own epoch before reuse and before output.
Server cache hints and durable invalidation Event replay remain open.

Protocol `0.44.0` adds `mcp.invocation_state_changed` to ordinary Session Event
queries and WebSocket replay. Each new dispatch receipt and terminal transition
is committed atomically with its event, including interruption during recovery.
The payload contains only `run_id`, `tool_call_id`, Kiln's `generation_id`, and
`state` (`dispatching`, `completed`, `failed`, `cancelled`, or `interrupted`). It
contains no MCP frame, result body, credential or live server session ID.
Duplicate receipt transitions create no extra event or wakeup. Clients use normal
durable cursors and event IDs; notifications only wake the replay reader.

Protocol `0.45.0` adds `mcp.input_state_changed` for mediation receipts. Its
payload contains `run_id`, `tool_call_id`, `generation_id`, a decimal-string
`ordinal`, `kind` (`roots`, `sampling`, `elicitation`) and `state` (`required`,
`resolved`, `interrupted`). The ordinal is a string to preserve exact values in
JavaScript. Each fresh input mutation commits its event atomically. Invocation
termination publishes input interruption before the invocation terminal event.
Exact receipt retries add no event or wakeup. Event replay loads the historical
input state, independently of the invocation's later outcome. This metadata does
not grant approval or authorize another MCP request.

The migration preserves existing event identities, cursors, cursor high-water
marks and incoming references. Older private MCP audit rows are not retroactively
inserted into the public timeline. Previously private input rows are projected
when that invocation next mutates; terminal historical invocations remain private.
Definition, instance, negotiation, authentication and catalogue events remain
separate follow-up work.

The artifact store has a bounded byte-page primitive for result paging.
It scans and hashes the complete file on every read, retaining only the requested
page plus fixed scratch space. A caller-supplied total artifact-size limit bounds
that scan; corruption anywhere in the file rejects the page. Returned bytes may
split UTF-8 code points. This primitive grants no access by itself.

Set `KILN_NATIVE_TOOL_OUTPUT_PAGE_LIMITS` to a JSON object containing
`max_request_bytes`, `max_artifact_bytes` and `max_page_bytes` to enable
`read_tool_output` (`kiln.artifact.read_tool_output`, revision 1). All fields are
required; unknown/duplicate fields fail startup. Request and artifact limits must
be positive; page bytes must be 4–640. The upper bound reserves space for the JSON
envelope and worst-case escaping within the existing 4096-byte inline output
limit. This opt-in is independent of MCP and can page any terminal ToolCall's text
artifact in the current Session whose effective directory is contained in the
page ToolCall's approved directory, on the same Workspace root.

Arguments are `tool_call_id`, `stream` (`stdout` or `stderr`), byte `offset`, and
byte `limit`. Start at zero, then use `next_offset` until `eof` is true. Results
contain `content_hash`, `offset`, `next_offset`, `eof` and `text`. The adapter keeps
complete UTF-8 characters at page boundaries; invalid starting offsets and binary
pages fail. Inline source outputs are already available and are not paged. Normal
native adoption, approval, a fresh claim and persisted completion apply; a repeated
completed request does not execute again. Cancellation joins the bounded scan
before completing and does not promise a hard deadline for filesystem I/O.

When this tool is in the invocation's frozen catalogue, provider context assembly
keeps ToolCall artifact metadata without eagerly loading those output bytes.
Explicit message attachments still load normally. Without this tool, existing
eager ToolCall output assembly remains in effect. Binary output paging, arbitrary
attachment paging and full provider/network acceptance remain open.

Run-service shutdown cancels and drains Runs, then seals and drains MCP owners.
The registry signals all owners before awaiting cleanup, and a cancelled shutdown
waiter can await those same owners again. The daemon retains another reference to
drain the registry if its HTTP listener exits before the graceful-shutdown future
finishes. Cleanup or journal failure is reported rather than treated as a verified
terminal generation. Destroying the Tokio runtime without awaiting this drain
provides only best-effort transport cleanup.

The local tests use synthetic definitions, temporary SQLite stores, real macOS
shell processes and loopback HTTP servers. They verify batched interruption without reacquisition and
Run-service shutdown reaping before the stopped journal record. Linux process
execution, orphan cleanup, remote TLS/OAuth and full MCP conformance remain unverified
or unimplemented. A real macOS fixture now exercises the daemon mixed native
coordinator with file read, search, describe and call: approval before launch,
normal completion persistence, one reused scoped owner and no replay of a completed
batch over stdio and modern/legacy HTTP. This is not full provider/network or MCP
conformance evidence.

The internal `dispatch_tool_call` API requires the generation worker's committed
invocation receipt before constructing a normal ToolCall result. It keeps small
UTF-8 responses inline and archives larger responses through the existing 4-KiB
output boundary and artifact metadata. Server `isError` output remains available
on Failed ToolCalls. Archive failure never causes a second send; interrupted
responses report an unknown external outcome rather than successful cancellation.
The real-process fixture verifies capture followed by normal native completion
storage. The daemon's native coordinator still needs to call this path before MCP
can be offered to models.

## Local definition administration

Stop the daemon before using `register-mcp-definition` or
`inspect-mcp-definition`. Both acquire the same exclusive data-directory lock as
`kilnd` and use `KILN_DATA_DIR`. Registration stores a portable local definition;
it does not resolve host bindings, read credentials, grant permission, or start a
server. The daemon runtime settings are not needed for these offline commands.

Registration accepts canonical schema-version-1 local metadata on piped stdin,
with one optional LF or CRLF. Object keys must be sorted, without insignificant
whitespace; duplicate, missing and unknown fields are rejected by exact canonical
regeneration. This is Kiln metadata, not a common `mcpServers` import format. A
minimal stdio definition is:

```json
{"auth_profile":null,"protocol":"auto","schema_version":1,"scope":"workspace_checkout","server":{"enabled":true,"id":"example","transport":{"arguments":[],"environment":{},"kind":"stdio","runtime_binding":"example-runtime"}},"source":"local","trust_policy":"kiln_mediated_serial"}
```

Save that exact line as `definition.json`, then use these Nushell commands:

```nu
let definition = (open --raw definition.json | str trim --right)
let budget = ($definition | str length --utf-8-bytes)
$definition | rtk cargo run -p kiln-daemon --bin kilnd -- register-mcp-definition --max-bytes $budget --expected-version 0 --idempotency-key example-create-1 --stdin
rtk cargo run -p kiln-daemon --bin kilnd -- inspect-mcp-definition --max-bytes $budget --id example
```

Choose the byte ceiling for the intended metadata and available memory. Each
field and collection is also bounded by that ceiling, since each occupies at
least one metadata byte. It is not a process-memory limit. Inspection needs a
ceiling large enough for the current stored version.

Expected version zero creates an unseen ID; replacement requires the current
version returned by inspection and a new idempotency key. Changing `enabled` to
false uses the same update flow. Retry an unconfirmed write with the exact same
metadata, expected version and key. Success returns `definition_id` and
`registered_version`: an old retry returns its original receipt without changing
the current definition. Inspect separately when current state is needed.
Inspection returns `version` and `definition`, including literal arguments;
resolved credentials are never included. Errors omit supplied metadata.

Online registration, common-format import, shared-source ingestion, public audit
replay, and online host-binding/credential administration remain open.

The internal MCP secret reservation journal (migration 54) records vault identity
and write ownership without secret values. A fresh reservation is the only result
that permits one vault write; an existing receipt never permits another write,
even after restart or deletion. Reservations validate the current enabled stdio
definition, owner, binding role and caller-supplied metadata limits. Cleanup is
scoped and batched, with retained deletion tombstones that prevent reference
reuse across identities. Retired reservations can be reconciled after a definition
is disabled or replaced.

The journal does not call the OS vault or run during startup. Its caller must
serialize writes and cleanup, wait for any cancelled OS write to settle, and
acknowledge deletion only after vault deletion succeeds.

Migration 55 adds immutable host snapshot revisions and atomic publication of
reserved references. Publication validates the exact current enabled definition,
runtime, owner and required argument/environment names. It excludes published
references from cleanup and retires replaced references in the same transaction.
Expected revision zero creates the first snapshot; an exact retry returns the
original revision without restoring older state. Snapshot metadata contains
references and an absolute UTF-8 Unix executable path, never secret values, and
is bounded together with its scoped key by the supplied metadata byte ceiling.

`resolve_persisted_stdio_launch` carries the local instance and revision into the
generation claim. Claims and publication serialize in SQLite: a stale snapshot
is rejected before process spawn, while a claimed generation prevents publication
until stopped/reaped. Interrupted generations also block publication. A key with
a persisted snapshot rejects a launch lacking its revision. Resolution and a
successful claim still require independent host/process authorization.

Migration 56 adds explicit snapshot retirement. It requires the expected current
revision and the same process cleanup boundary as publication, but still works
after a definition is disabled or its owner disappears. Retirement advances the
revision, releases all published references into pending cleanup, and retains a
current tombstone. That tombstone blocks bound and unbound generation claims;
removal cannot silently restore the legacy materialized launch path. Resolvers
reject retired records before any vault access. Re-enabling requires explicit
publication against the tombstone revision and live reserved references. Exact
retirement retries return their original receipt without retiring a later update.

Automated orphan/secret cleanup remains open. The
targeted tests use fake vault values and real macOS process fixtures; actual MCP
OS-vault integration and Linux runtime behavior remain unverified.

## Offline host administration

`kilnd mcp-host-admin --max-bytes N --stdin` accepts one canonical JSON request
from a pipe, with one optional LF/CRLF. It holds the exclusive daemon data-directory
lock through metadata changes and vault operations; stop `kilnd` first. The command
uses `KILN_DATA_DIR` and the same durable local instance ID as daemon startup,
initializing an unassigned ID if necessary. It never enrolls configuration sync,
grants execution authority, resolves PATH, or launches an MCP server.

The envelope has exactly `action` and `key`. `key` is the canonical scoped identity:
`auth_profile`, `definition_id`, and `owner`. Owner fields are:

| Owner kind | Additional fields |
| --- | --- |
| `core` | None |
| `session` | `session_id` |
| `workspace` | `workspace_id` |
| `workspace_checkout` | `workspace_id`, `workspace_root_id`, `relative_directory`, `root_path`, `git_common_directory_path`, `filesystem_identity` |

Use the definition's scope/auth profile and the exact registered owner identity.
Checkout paths/identity must match the stored available root. Decoding a historical
key does not grant access; it permits retirement/reconciliation after a definition
or owner changes. This example inspects a **Core-scoped** definition named `example`:

```json
{"action":{"operation":"inspect"},"key":{"auth_profile":null,"definition_id":"example","owner":{"kind":"core"}}}
```

Save a canonical request as `host-request.json` and run it with a byte allowance
large enough for both the input and the stored snapshot plus scoped key:

```nu
open --raw host-request.json | rtk cargo run -p kiln-daemon --bin kilnd -- mcp-host-admin --max-bytes 4096 --stdin
```

Here 4096 is an example allowance for a small request, not a product default.
Object keys must be sorted with no insignificant whitespace; duplicate and unknown
fields are rejected. Errors omit supplied values. Secret values belong only in
piped input, never command arguments, shell history, portable definitions or logs.

| `action.operation` | Other action fields | Result |
| --- | --- | --- |
| `inspect` | None | Local `instance_id` and nullable snapshot with revision, retired flag and reference metadata; no vault reads |
| `pending` | Positive `batch_size` | Bounded unpublished/retired reference list; no secret values |
| `import_secret` | `definition_version`, `name`, `purpose` (`argument`, `environment` or `http_credential`), `value` | Fresh `secret_ref` after a successful MCP-vault write |
| `publish` | `expected_revision`, `definition_version`, `runtime_binding`, absolute UTF-8 `executable`, `arguments` and `environment` maps of binding names to returned secret refs, optional `working_directory` object below | Immutable `registered_revision` receipt |
| `publish_http` | `expected_revision`, `definition_version`, `endpoint`, nullable `endpoint_binding`, nullable `credential` pair `[binding_name, secret_ref]`, optional `working_directory` | Immutable `registered_revision` receipt in the same host revision history |
| `retire` | Positive `expected_revision` | Immutable `retired_revision` receipt; vault deletion is separate |
| `reconcile` | Positive `batch_size` | `reconciled` count after retiring pending refs, deleting their vault values, and retaining deletion receipts |

`http_credential` imports require the exact `credential_binding` declared by an
enabled HTTPS definition at the requested version and scoped identity. They use
the isolated MCP vault and the same single-write reservation and cleanup receipts
as stdio values. HostEndpoint names do not authorize credential imports.

For an HTTPS definition, `publish_http` requires the exact canonical shared
endpoint, null `endpoint_binding`, and the declared credential name/reference
(or null `credential` when none is declared). For a HostEndpoint definition,
`endpoint_binding` must match its declared name and `credential` must be null.
Host endpoints accept HTTPS or explicit localhost/loopback-IP HTTP; userinfo,
fragments and whitespace/control characters are rejected. URLs are canonicalized
before storage. Endpoint metadata is host-local; do not place credentials in URLs.

HTTP and stdio snapshots share revision checks, active/uncertain-generation
replacement fences, credential retirement, and exact historical retry receipts.
Publication preflights all references before vault reads and revalidates before
commit. Existing stdio metadata remains readable without rewriting history.
HTTP daemon activation requires the explicit HTTP limits described above;
publication grants no execution authority. Unpublished HTTP imports remain eligible for explicit reconciliation.

The internal `resolve_persisted_http_launch` boundary checks an independently
authorized local instance, scoped key and optional directory against that exact
snapshot before reading the MCP vault. It carries the host revision into the
resolved launch so the future runtime owner can atomically reject rotations before
network startup. Endpoint and retained bearer bytes have an explicit combined
budget; vault reads consume the startup deadline. HTTP credentials must satisfy
the RFC 6750 bearer-token syntax. No ambient headers, provider credentials or
credential refresh are used. Auto begins with modern discovery; unsupported 2024
HTTP+SSE pins fail before vault access. This resolver performs no network I/O and
is consumed by the daemon broker only after approval and directory pinning.

Import validates the current definition and exact binding role before reserving
and writing a fresh reference. A failed/ambiguous write leaves its reservation for
explicit reconciliation. Retrying import creates a **new** reference; it never
rewrites an ambiguous reference. Only a successful import response supplies a
reference for publication. Values use SecretValue's existing nonempty, single-line,
NUL-free, at-most-1-MiB envelope in the separate `dev.kiln.mcp` vault namespace.

Publication preflight rejects stale/invalid metadata and references before vault
reads, then verifies that the referenced values exist and revalidates in the
publication transaction. Exact old receipts are returned without vault reads,
even if their old values have since been deleted. Inspect separately for current
state. A changed binding revision requires process cleanup first.

`working_directory` selects a registered checkout with `workspace_id`,
`workspace_root_id`, `relative_directory`, `root_path`,
`git_common_directory_path` and `filesystem_identity`. Use the registered root's
exact paths and identity; relative directories cannot traverse upward. Publication
and generation admission recheck the available root in the same transaction.
Workspace/checkout owners must match the selection; session owners must belong to
its workspace. Core owners also use an explicitly selected registered checkout.
These paths describe registration and never grant filesystem authority.

Old snapshots without `working_directory` remain inspectable, replaceable and
retirable. They cannot use the persisted launch resolver until republished with a
directory. Resolution rejects an absent selection or a mismatch with the caller's
authorized checkout before any vault read. The caller must still recheck approval
and pin the directory from that checkout. The internal `execute_stdio_call` path
does this through `McpLaunchStore` and a trusted pinning callback, then rechecks the
context after preparation. It derives ownership from the approved scope and
definition and requires the snapshot's local instance to match this daemon store.
The dispatch transaction checks directory registration/scope and host revision
again. The daemon coordinator is not wired yet.

**Publish wanted imports before reconciliation.** Reconciliation deletes all
unpublished values in its selected batch, including successful imports that have
not yet been published. Published refs are excluded. If deletion fails, the retired
reference remains pending for retry; completed deletions retain tombstones. Repeat
batches until `pending` is empty. No automatic reconciliation runs at startup.
