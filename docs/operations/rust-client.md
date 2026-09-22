# Rust client

The `kiln-client` crate connects Rust applications to the local Kiln daemon.
It is the shared transport boundary used by the native GPUI desktop client.
It uses `kiln-protocol` wire types. It does not import the core, server, or
SQLite adapters.

## Connection

Start `kilnd` as a separate process. Supply its bound loopback socket address
and local credential to the client. The client does not launch the daemon or
read credentials from disk. Closing a client does not stop the daemon or cancel
a Run.

Negotiate the protocol before using the event stream. Keep the returned store
identity with any saved event cursor. A cursor from another store must not be
used to resume this store.

HTTP requests use bearer authentication. Redirects and HTTP proxies are disabled
so a request cannot forward the local credential to another endpoint. The event
connection uses the public WebSocket protocol.

The application supplies its identity and credential:

```rust
use std::net::SocketAddr;
use kiln_client::{Client, Error, EventStream};
use kiln_protocol::ClientIdentity;

async fn connect(
    address: SocketAddr,
    token: &str,
    identity: ClientIdentity,
) -> Result<EventStream, Error> {
    let client = Client::new(address, token)?;
    let negotiated = client.negotiate(identity).await?;
    client.subscribe_events(&negotiated, Some("0")).await
}
```

Read frames with `EventStream::next_frame()`. It returns the protocol's `Ack`,
`Event`, and `Error` variants and returns `None` when the connection closes.

## Conversation operations

The client supports creating and retrieving Workspaces and Sessions, appending
user Messages, starting root and child Runs, listing a Session's Run tree,
retrieving Runs, sending Run input, cancelling Runs, deciding ToolCall approvals,
reading Session events, listing the captured Session checkout's Git changes,
listing the global Usage ledger, and retrieving artifacts.
These operations use the daemon's public contracts. The daemon owns validation,
permission decisions, durable state, and process execution.

Keep one idempotency key for each logical command that requires it. If a response
is lost, retry the same command with the same key. A new key means a new command.
The client does not retry commands automatically.

Use `list_session_runs` after connection to restore the complete root and child
Run topology before live events arrive. `start_child_run` uses the same explicit
idempotency rule as `start_run`; keep its key stable when retrying an uncertain
response.

Use `react_to_run_activity` to queue a root Message about a selected child
Event. Supply the root Run ID, the text, and `child_activity` with the child
Run ID and durable Event ID. Keep all fields and the idempotency key unchanged
on retry. Reactions use queued delivery, not interruption. The daemon rejects
references outside the target root's descendants or with mismatched ownership.
The returned Message and its replayed `message.appended` Event retain the
optional `child_activity` field. Plain `send_run_input` remains unchanged.

Use the stable public problem code to handle a rejected HTTP request. Do not
infer permission or Run state from a transport failure. Retrieve the Run or retry
the same idempotent command when its result is uncertain.

`list_session_changes` is read-only and takes only a Session ID. The daemon uses
the checkout captured when that Session was created; clients cannot supply a
filesystem path. A missing legacy checkout, a changed root identity, or a
different Git common directory is reported as an unavailable workspace root.
`get_session_change_diff(session_id, path)` requests one selected changed file
after `list_session_changes`. The path is sent as a URL query parameter and
the daemon revalidates it against the captured checkout. Each Git invocation is
bounded to 4 MiB of output and 30 seconds. The response is either a UTF-8
unified patch truncated at 256 KiB or an explicit unavailable reason for
untracked, binary, conflicted, renamed, unsupported file types, or unsupported
encodings.

`Error::Api { status, problem }` retains the HTTP status and the protocol's
`ProblemDetails`, including its stable `code`.

`list_usage` is read-only and accepts an optional exclusive Model Invocation ID
`after` cursor and `limit`. The default page size is 100 and the maximum is
1,000. Each entry is the latest validated revision for one physical invocation,
including explicit counted quantities, missing dimensions, zero values,
completeness, source, and normalized provider metadata. `next_cursor` is null
on the final page. The endpoint reports observations and does not provide
pricing, cost, or valuation.

Artifact downloads return bytes
and response metadata through `get_artifact`; they do not use the JSON decoder.

## Event replay

The WebSocket stream contains daemon-wide events. Filter them by Session or Run
when rendering a conversation. Use the Session event query when only that
Session's stored history is needed.

- Pass `after = "0"` to replay from the start of the store.
- Pass the last applied Event cursor to resume after a disconnect.
- Omit `after` only when live events without earlier history are wanted.
- Apply an Event before saving its cursor. Deduplicate by its stable Event ID
  if replay repeats an Event already applied by the application.
- Treat cursors as opaque decimal strings. Do not assume adjacent Events have
  consecutive cursor values.

The initial WebSocket acknowledgement reports the snapshot watermark before the
server sends replayed Events. Do not save that watermark as the last applied
cursor: a disconnect during replay would then skip unapplied Events. Advance the
saved cursor only as Events are applied. Reconnection and backoff remain caller
decisions.

## Current scope

`disconnect_provider_account` cancels active sign-in and removes local credentials
through the authenticated protocol `0.24.0` route. It returns a safe account snapshot
and does not revoke provider-side access. A `provider_account_cleanup_required`
problem requires an explicit retry after checking current account state. The client
does not retry automatically: another disconnect would also affect a newly signed-in
account with the same ID.

Protocol `0.25.0` adds `start_provider_account_browser_login`. Its authenticated
`POST /v1/provider-accounts/{provider_account_id}/login/browser` response contains
an attempt ID, safe account summary, and short-lived authorization URL with
`Cache-Control: no-store`. Debug formatting redacts that URL. The callback code
and verifier are never part of the public protocol. Use the existing status and
cancel methods for either login method. Open browser authorization on the daemon
host; choose device sign-in for remote hosts or unavailable localhost callback ports.
A browser-start failure does not automatically initiate device sign-in.

This crate supplies transport for the native client. The separate
`kiln-desktop` crate implements the GPUI window, transcript state, and desktop
connection flow. Neither crate discovers or launches the daemon, and the
desktop does not connect to a live model provider.

The public protocol types are separate from domain types. This lets the desktop
client compile without process supervision or database dependencies. Async
methods perform I/O; returned protocol values can then be used by UI state code.

## Configuration synchronization status

Protocol `0.26.0` adds `get_configuration_sync_status`. It reads the authenticated
local daemon's stable instance ID, role, master/group, CAS version and committed
applied/observed revision metadata through `GET /v1/configuration-sync`.
Transport currently reports `unconfigured`; matching revisions do not imply a
live master connection or completed synchronization. The method returns no shared
content or credentials and makes no configuration changes. See the
[configuration sync contract](../spec/configuration-sync.md) for pending enrollment
and consumer behavior.

Protocol `0.27.0` adds `designate_configuration_master(key, request)`. Obtain
`expected_instance_id` and `expected_state_version` from status, then submit
`DesignateConfigurationMasterRequest` after the administrator chooses this
unassigned instance as master. Keep the same key and request across uncertain
outcomes. Exact retries return the immutable original
`ConfigurationMasterDesignationResponse`; reload status to learn the current
role. Changed key reuse and stale/already-assigned state return distinct 409
errors. The command creates a new authority group but publishes no configuration
and does not enable remote transport.

## Configuration snapshot publication

Protocol `0.28.0` adds `publish_configuration_snapshot(key, request)`. Prepare a
complete `SharedConfigurationBundle`, then use current master status to fill
`PublishConfigurationSnapshotRequest.expected_instance_id`, `expected_group_id`,
and `expected_state_version`. Every field in the bundle is explicitly supplied;
the client does not discover or read host configuration, skill directories, or
credentials. The [publication contract](../spec/configuration-sync.md#explicit-local-snapshot-publication)
defines canonical metadata, skill file bytes/hashes, replacement semantics, and
the 2 MiB limit on the whole serialized request.

Keep the same key, preconditions and content for retries after uncertain outcomes.
The returned `ConfigurationPublicationResponse` is the original receipt, even
after later publication or role changes. It contains a revision/schema/hash and
the resulting state version, not current content or an activation result. Reload
`get_configuration_sync_status` before preparing another publication. Changed key
reuse is `idempotency_conflict`; stale state/wrong master is
`configuration_sync_conflict`. Publication stores the complete snapshot locally;
remote distribution and runtime consumers are not yet wired.

## Configuration snapshot export

Protocol `0.29.0` adds `get_configuration_snapshot()`. It returns a
`ConfigurationSnapshotResponse` containing the fully verified stored `snapshot`
bundle together with instance/group/master IDs, state version and revision.
`configuration_snapshot_not_found` means the active group has no snapshot.
`configuration_snapshot_too_large` means the daemon cannot export it within the
local data/encoded-response budgets; storage or integrity failure is
`configuration_sync_unavailable`.

The client caps declared and streamed success/error bodies at 2 MiB and rejects
oversized responses as `ConfigurationSnapshotTooLarge` before parsing. It never
returns truncated JSON. Read/export does not save files or activate content.
For a later publication, deliberately prepare the full replacement from the
returned bundle and confirm the current master/state. Export metadata does not
authenticate a remote peer or enroll a follower. See the
[export contract](../spec/configuration-sync.md#verified-local-snapshot-export).
