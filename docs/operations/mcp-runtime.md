# Internal MCP runtime

On Unix, `kilnd` can retain scoped MCP stdio process owners and drain them during
shutdown. This is an internal implementation boundary. There is no public server
launch command, host-binding administration, or model-visible MCP tool yet.

Both settings below are required to enable the runtime. If neither is set, it is
disabled. Missing, zero, invalid or overflowing values prevent startup when either
setting is supplied. There are no product defaults.

| Environment variable | Meaning |
| --- | --- |
| `KILN_MCP_MAX_INSTANCES` | Positive maximum scoped owners retained by the daemon registry, including uncertain cleanup. Choose from the host's process and memory budget. |
| `KILN_MCP_RECOVERY_BATCH_SIZE` | Positive maximum generation rows interrupted per startup transaction. Choose for the store's transaction and startup budget. |

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
binding/revision administration remains to be implemented. A reference-backed
resolver can read scoped argument/environment values through the separate MCP
vault port; it is not installed at daemon startup and does not authorize launch.
Durable reference reservation and credential import/removal remain open.

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
execution, orphan cleanup, installed model-visible MCP tools, HTTP/OAuth and full
MCP conformance remain unverified or unimplemented. Internal claimed stdio dispatch
now has a real macOS process fixture, including interruption without replay; this
is not acceptance of the end-to-end daemon broker.

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
replay, and host-binding/credential administration remain open.

The internal MCP secret reservation journal (migration 54) records vault identity
and write ownership without secret values. A fresh reservation is the only result
that permits one vault write; an existing receipt never permits another write,
even after restart or deletion. Reservations validate the current enabled stdio
definition, owner, binding role and caller-supplied metadata limits. Cleanup is
scoped and batched, with retained deletion tombstones that prevent reference
reuse across identities. Retired reservations can be reconciled after a definition
is disabled or replaced.

This is an unpublished-reservation primitive, not credential administration or
launch authorization. It does not call the OS vault, publish host snapshots or
run during startup. Its caller must serialize writes and cleanup, wait for any
cancelled OS write to settle, and acknowledge deletion only after vault deletion
succeeds. Snapshot publication and generation/revision coordination remain open;
no published credential can be represented by this journal yet.
