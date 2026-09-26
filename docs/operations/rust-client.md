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
`configuration_sync_conflict`. Publication stores the complete snapshot locally.
An approved follower can fetch and apply it explicitly through the separate
one-shot method below; ongoing distribution and runtime consumers are not wired.

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

## Restricted follower transport

`ConfigurationSyncClient` is a separate HTTPS-only component for enrolled
followers. It is not the local `Client` and has no general request method. The
daemon also uses its pinned transport for explicit enrollment exchange and
one-shot or scheduled refresh through narrow core ports; infrastructure does not
depend on the client or protocol crates. It does not configure the master's
separate opt-in listener.

Supply `ConfigurationMasterPin` with the HTTPS origin, a dedicated master CA in
DER format, canonical master/group/follower IDs, the follower-specific `kcfg1_`
bearer, and `ConfigurationSyncTimeouts` with nonzero connect/total network budgets.
The caller must have authenticated and approved the whole binding through
enrollment before sending credentials. The client does not load files, fetch
certificates, discover peers, or substitute the local API bearer. It excludes
system roots while retaining hostname/chain verification. It trusts the supplied
CA and hostname, allowing leaf certificate renewal under that binding; it does
not enforce an exact leaf certificate fingerprint.

The request is a fixed snapshot GET with claimed master/group/follower headers
and a sensitive Authorization header. Redirects, proxies, automatic retries,
decompression and TLS key logging are disabled. Only HTTP 200 can yield a candidate;
non-success bodies are discarded. Declared/collected bytes are bounded at 2 MiB.
Errors contain categories/status numbers, without remote bodies, URLs or secrets.
Response authority must match the pin. The consumer must still validate full
snapshot content and revisions and fence enrollment changes before applying it.
See the [transport contract](../spec/configuration-sync.md#pinned-https-follower-client).

## Managed master identity status

Protocol `0.30.0` adds `get_configuration_identity_status()`. The authenticated
local read returns current authority/state and optional pending/active identity
metadata from one transaction, including server name, CA fingerprint and explicit
validity timestamps. Unassigned/follower instances and masters without a live
identity return `identity: None`. Retired/historical identities are excluded.
This reads no vault secrets and performs no setup, recovery or retirement. An
active phase does not establish current key availability, certificate validity
or remote serving. See the [identity status contract](../spec/configuration-sync.md#local-managed-identity-status).

Protocol `0.31.0` adds explicit `configure_master_identity(key, request)` and
`retire_master_identity(request)` commands. Setup requires current master
instance/group/version plus exact name/validity values; retain the same key and
request for uncertain retries. Success returns the original reservation receipt,
so reload status. Retirement uses expected local instance and the original setup
key, allowing cleanup even after a lost setup response; it requires HTTP 204 and
can be retried with the same body. A retired key never provisions a replacement.
These commands write/delete OS-vault keys but enable no listener or follower trust.
See the [setup contract](../spec/configuration-sync.md#explicit-managed-identity-setup-and-retirement).

Protocol `0.32.0` adds `retire_master_identity_by_id(request)` for a known
identity. `ConfigurationIdentityStatusResponse.identity_id` and the setup receipt
now contain a stable public ID independent of the CA/TLS vault references. Use
that ID with the expected local instance to retire after reconnect or app restart;
the daemon verifies ownership and targets only that immutable record, including
after the instance has left its former master role. The existing
`retire_master_identity(request)` remains available for cleanup by the original
setup idempotency key when the setup response was lost. Either retirement request
is safe to repeat after an uncertain result; the client does not retry
automatically. Status does not enumerate retired or historical IDs.

Protocol `0.33.0` adds local grant metadata methods: `list_configuration_read_grants(limit, after)`,
`get_configuration_read_grant(grant_id)`,
`get_configuration_read_grant_by_attempt(attempt_id)`, and
`revoke_configuration_read_grant(grant_id, request)`. The list is bounded to 100
rows per page and returns a stable cursor. Grant and attempt lookups support
metadata recovery after an uncertain enrollment result; responses contain no
bearer or credential digest. Revocation requires the expected local instance and
current state version, is permanent, and is safe to repeat. These endpoints do
not create or deliver a credential, approve a pending request, or enable a
listener.

Protocol `0.34.0` adds local follower enrollment methods:
`prepare_configuration_follower_enrollment(request)`,
`list_configuration_follower_enrollments(limit, after)`,
`get_configuration_follower_enrollment(attempt_id)`, and
`retire_configuration_follower_enrollment(attempt_id, request)`. Prepare takes a
stable `cra_` attempt ID, expected unassigned instance/version, selected
group/master, server name and CA DER. Reuse the exact full request and attempt ID
after an uncertain response. The complete JSON body and CA DER value are each
limited to 2 MiB; the shared JSON extractor reports an over-limit body as HTTP 400
`invalid_json`. Prepare, list and get return lifecycle/authority metadata and a
CA fingerprint only; they never return CA bytes, a vault reference, credential
digest or bearer. The list accepts 1–100 rows (default 50) and uses a stable
continuation cursor. Retirement requires the expected follower instance ID,
permanently tombstones the attempt, and is safe to retry after a lost response.
These authenticated local calls do not contact a master, assign a local role,
approve a remote request, or enable listener composition. See the
[local enrollment request contract](../spec/configuration-sync.md#selected-automatic-enrollment-direction).

Protocol `0.35.0` exposes local master request list/get/approve/reject methods.
First approval requires the request's server name and CA fingerprint to match
the active managed master identity in the same transaction as grant issuance.
A missing, pending, retired or changed identity returns HTTP 409
`configuration_sync_conflict`. Exact approved retries still return the current
grant, including revocation, and pending requests can still be rejected after
identity retirement. Metadata reads do not assert current certificate validity
or live TLS readiness. These calls do not authenticate the claimed follower or
enable remote intake.

Protocol `0.36.0` adds the digest-only
`ConfigurationSyncClient::submit_enrollment_request(attempt_id,
follower_state_version)` method. The pinned client now takes the approved master
server name in addition to the HTTPS origin and CA, and sends its bearer only on
snapshot GET. The enrollment POST contains the stable attempt, follower/version,
authority, server name, CA fingerprint and SHA-256 credential digest; it sends no
Authorization header. Its strict body is capped at 4 KiB. Receipts must echo the
binding, use a canonical request ID, and return a confirmation fingerprint that
matches the submitted digest and complete binding; approved receipts
include the grant's current revoked state. Exact retries recover current pending
or terminal state. The isolated router's caller configures a positive
per-authority cap over all retained rows; at exhaustion, new attempts return HTTP
429 and exact retries remain available. The remote intake remains on the master's
separately configured TLS listener; this client method does not configure it.

Protocol `0.37.0` adds the authenticated local
`exchange_configuration_follower_enrollment(attempt_id, request)` command. It
requires a prepared local attempt and supplies the HTTPS origin plus positive
connect and request timeouts on that call; the origin and budgets are not stored
with the attempt. The strict request body is capped at 4 KiB; timeouts must be
positive, with connect no greater than request. The durable preparation still
pins the master server name and CA. The daemon reads and verifies the follower
bearer in its OS vault and uses the restricted client to send only the digest
and request binding, without an Authorization header.

A pending receipt is stored while the local instance remains unassigned. An
approved, active grant and the follower role compare-and-swap commit together,
and the read credential remains in the OS vault for later snapshot reads.
Rejected, revoked, and role-conflict results retire the attempt before retryable
vault cleanup. Explicit retirement also removes the credential and does not
change a role that already joined. Finalized exchange and exact prepare retries
return durable metadata without another remote request or role transition, and
never recreate a credential removed by explicit retirement. Retry explicit
retirement after a vault-cleanup error. The response includes only
credential-free exchange status and the last-observed receipt; a later master
revocation may change the grant state. The client does not retry the command
automatically. Enrollment exchange does not fetch or apply snapshots or
establish currentness. Daemon-owned periodic refresh is configured separately.

Protocol `0.38.0` adds the authenticated local
`fetch_configuration_follower_snapshot(attempt_id, request)` command. Its strict
4 KiB request supplies the expected follower instance, HTTPS origin, and positive
connect/request timeouts; the origin and budgets are per-call transport options.
The daemon reloads the exact approved enrollment pin and OS-vault credential,
fetches one bounded candidate, records its authenticated revision, validates the
complete bundle, and atomically applies it. The response contains only the
resulting follower instance/state version, revision, and `applied` or
`already_applied` disposition. It does not return snapshot bytes, credentials,
vault references, or private enrollment state. This one-shot command does not
retry automatically; see the separate daemon refresh configuration below.

Migration 47 adds a private monotonic credential marker. Only a credential from
an approved enrollment with an active marker can fetch. Explicit retirement
commits marker retirement before retryable vault cleanup; leaving the follower
role also retires the marker in the role-change transaction, making any leftover
vault value unusable. Rejoining requires a new approval. Historical approved or
uncertain attempts are not treated as active during migration and require
leaving the follower role, retiring the old attempt, and reenrolling. The
command performs one explicit fetch with no automatic retry. If bundle
validation or application fails, the previous snapshot remains intact while a
newer authenticated observation may be retained. See the
[snapshot fetch contract](../spec/configuration-sync.md#explicit-follower-snapshot-fetch-and-application).

Protocol `0.39.0` adds opt-in daemon-owned follower refresh. Set every
`KILN_CONFIGURATION_FOLLOWER_REFRESH_*` environment variable in the
[follower refresh operations guide](configuration-follower-refresh.md) to enable
one initial authenticated check and serialized periodic refreshes for an exact
follower instance and enrollment attempt. Each fetch still reloads the
enrollment's immutable CA, server name, authority and active credential marker.
The per-check HTTPS origin may change if its host still matches that server name;
the polling interval, freshness threshold, connect deadline and request deadline
are caller-supplied. The authenticated `GET /v1/configuration-sync` response now
reports refresh configuration and operation state, the last outcome, successful
revision/check time, and process-local age/recency. After restart, recency remains
unknown until a new successful check; matching revisions or historical enrollment
receipts do not count as a live check. Refresh does not activate consumers.
