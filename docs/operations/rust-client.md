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
Each Git invocation is bounded to 4 MiB of output and 30 seconds.

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

This crate supplies transport for the native client. The separate
`kiln-desktop` crate implements the GPUI window, transcript state, and desktop
connection flow. Neither crate discovers or launches the daemon, and the
desktop does not connect to a live model provider.

The public protocol types are separate from domain types. This lets the desktop
client compile without process supervision or database dependencies. Async
methods perform I/O; returned protocol values can then be used by UI state code.
