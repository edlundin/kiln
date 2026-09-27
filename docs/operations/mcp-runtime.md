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
retaining desired state and last negotiated protocol. It does not adopt or kill
orphan processes and does not replay invocations. An interrupted generation stays
blocked against replacement until cleanup is confirmed; enabling this runtime
does not by itself make crash recovery complete.

The registry belongs to the daemon, rather than a Run. Concurrent callers for a
scope share startup and reuse a valid ready generation. Reuse rechecks the stored
definition and current generation. Host-local binding revisions must change when
resolved launch inputs or authorization change; a revision change requires an
explicit stop before replacement. The internal resolver accepts already authorized,
materialized host values and a pinned directory; it substitutes only explicit
runtime/argument/environment references within a caller byte budget. Persistent
binding/revision administration and vault lookup remain to be implemented.

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
execution, orphan cleanup, live tool invocation, HTTP/OAuth and full MCP
conformance remain unverified or unimplemented.
