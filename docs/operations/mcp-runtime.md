# Internal MCP runtime

On Unix, `kilnd` can retain scoped MCP stdio process owners and drain them during
shutdown. Native model Runs can opt into `mcp_search`, `mcp_describe` and `mcp_call`
through the separate limits configuration below. There is no public direct server
launch command. Offline host-binding administration is described below.

### HTTP transport preparation

`kiln-mcp` provides an internal `BoundedHttpClient` adapter for Streamable HTTP.
It is not connected to daemon execution or host bindings yet. It requires explicit
request, JSON response, whole SSE stream, per-event and header byte allowances plus
a whole-request timeout. Per-event accounting treats CRLF as one terminator;
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

The SDK worker's separate reconnect, session recovery and metadata cache policies
still need lifecycle integration before enabling HTTP in the broker. OAuth,
legacy HTTP+SSE, live credential administration and official HTTP conformance
remain open. Tests for this adapter use local HTTP socket fixtures, not remote
services or real OAuth credentials.

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

The migration preserves existing event identities, cursors, cursor high-water
marks and incoming references. Older private MCP audit rows are not retroactively
inserted into the public timeline. Definition, instance, negotiation, authentication,
catalogue and server-request lifecycle events remain separate follow-up work.

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
provides only best-effort process cleanup.

The local tests use synthetic definitions, temporary SQLite stores and real macOS
shell processes. They verify batched interruption without reacquisition and
Run-service shutdown reaping before the stopped journal record. Linux process
execution, orphan cleanup, HTTP/OAuth and full MCP conformance remain unverified
or unimplemented. A real macOS fixture now exercises the daemon mixed native
coordinator with file read, search, describe and call: approval before launch,
normal completion persistence, one reused process and no replay of a completed
batch. This is not full provider/network or MCP conformance evidence.

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
| `import_secret` | `definition_version`, `name`, `purpose` (`argument` or `environment`), `value` | Fresh `secret_ref` after a successful MCP-vault write |
| `publish` | `expected_revision`, `definition_version`, `runtime_binding`, absolute UTF-8 `executable`, `arguments` and `environment` maps of binding names to returned secret refs, optional `working_directory` object below | Immutable `registered_revision` receipt |
| `retire` | Positive `expected_revision` | Immutable `retired_revision` receipt; vault deletion is separate |
| `reconcile` | Positive `batch_size` | `reconciled` count after retiring pending refs, deleting their vault values, and retaining deletion receipts |

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
