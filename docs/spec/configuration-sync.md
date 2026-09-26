# Master-instance configuration synchronization

EDL-322 requires one explicitly designated master Kiln instance to supply shared
settings, global MCP configuration, and global skills to followers, including
remote hosts. Durable authority/snapshot storage, status, initial local master
designation, explicit local snapshot publication, and bounded snapshot export are
implemented. Internal follower read-credential registration, revocation and
authorized snapshot acquisition are also implemented, along with durable local
follower-credential reservation and authenticated request preparation/recovery.
The master also exposes local request review, exact approval and permanent
rejection for its internal follower-request journal.
Active master TLS material can now be acquired from an explicitly selected
managed identity and served by an opt-in daemon TLS listener around the
restricted follower router. An explicit local follower-exchange command now
submits prepared requests over pinned HTTPS and transitions the follower role
after exact master approval. The successful follower-read credential remains in
the OS vault. A separate one-shot local command now fetches, observes, validates,
and atomically applies a follower snapshot. Polling, automatic reconnect, and
runtime settings/MCP/skill activation remain incomplete.

## Authority and enrollment

Each installation has a durable instance ID independent of its hostname, address,
Workspace IDs, and provider accounts. Designating a master creates a new group ID
bound to that instance. A follower explicitly enrolls against that group and
master; a URL, discovery result, hostname, or downloaded snapshot cannot change
that binding. A master cannot enroll itself as its own follower.

Enrollment requires an authenticated local administrative action on the follower,
verified master identity over authenticated transport, and master authorization
for that follower. Remote transport must authenticate and encrypt the connection;
loopback alone does not prove the identity of a forwarded peer. Enrollment must
pin the master identity and provision a follower-specific, revocable credential
with read-only shared-configuration access. The existing unrestricted local API
bearer is not a synchronization credential and must not be copied to followers.
Local preparation reserves and recovers follower request inputs without
contacting a master or changing the local role. A separate authenticated local
exchange command contacts the selected master over pinned HTTPS and changes the
follower role only after observing an approved active grant; the read credential
remains in the OS vault. A master's local API can review and approve or reject an
exact journaled request; that operation does not authenticate the follower. The
isolated intake and snapshot routes are available only through the separately
configured opt-in TLS listener.

The first release has no automatic failover or master election. A master cannot
be replaced by accepting a newer snapshot or a numerically larger revision.
Changing master is explicit reenrollment into a new group. Graceful handover first
stops publication on the old master; the administrator then designates the new
master and reenrolls followers. If the old master is unavailable, the old group
remains a distinct authority. Nothing merges its later edits into the new group.
The UI must identify followers still attached to the old group. Promotion or
reenrollment must never silently reinterpret a follower's pending local edits as
master data.

## Shared scope and host bindings

A shared snapshot contains all three categories at one revision:

- An explicit allowlist of global settings, versioned by schema. New setting keys
  must declare their shared versus host/workspace scope before being exported.
- Global MCP definitions with stable IDs, enabled state, portable launch/transport
  declarations, and logical dependency/credential binding names.
- Global skill packages with stable IDs, enabled state, version metadata, exact
  content hashes, and their declared files and dependencies.

Host addresses, absolute paths, runtime executable locations, local account IDs,
Workspace/repository configuration, local execution grants, local authentication
credentials, vault contents, and SecretRefs are excluded. A logical binding name
refers to a separately maintained host binding; it is not a copied master path or
credential. Missing bindings make that integration unavailable with a visible
reason. They do not make the follower substitute a program or credential.

Skill transfer contains explicitly selected package files, not recursive copying
of the master's home or configuration directory. Reject absolute paths, traversal,
symlinks, duplicate paths, and platform-colliding paths before materialization.
Verify file sizes and hashes under explicit caller limits. Package scripts and
MCP definitions remain untrusted configuration: synchronization does not execute
installers, start servers, or grant access. Local policy continues to authorize
execution. Secret provisioning is a separate explicit operation and is not part
of configuration sync.

## Revisions, application, and offline behavior

Within a group the master publishes immutable, monotonically increasing positive
revisions. A revision identifies a complete canonical snapshot with a content
hash covering all categories, schema metadata, enable/disable state, and file
manifests. Revision zero means no snapshot has been applied; it is never published.
A previously observed revision cannot change its hash. The local authority history
and highest observed revision survive leaving and rejoining the same group. Rollback is a new revision
with the desired older content, never a decrement or rewrite.

Followers can skip revisions because each snapshot is complete. They reject the
wrong group/master, a lower revision, a repeated revision with a different hash,
and unsupported schema. An identical revision/hash is an idempotent duplicate.
A downloaded snapshot is only a candidate: all metadata, files, hashes, limits,
and compatibility checks must pass before one atomic active-revision change.
Preparation occurs in isolation. Failure leaves the prior coherent snapshot
active and preserves a visible error. Removals follow from replacement of the
complete manifest; stale files must not remain enabled. Readers and active Runs
pin one applied revision, never a mixture across a concurrent update.

Followers have no local write path for the shared scope. Offline followers retain
the last applied snapshot and host bindings and show that currentness is unknown.
They do not advertise themselves as current without a recent authenticated master
revision observation. Reconnect rechecks the authority and catches up to the
latest revision. Failed updates remain pending and do not clear known newer-master
status. No timeout silently promotes a follower or discards master authority.

## Visible status and delivery

Settings must show instance ID, group, master, role, applied revision/hash, last
observed master revision, connection state, and content-free synchronization error.
A follower with no applied snapshot is awaiting initial synchronization. A known
newer master revision means awaiting synchronization; matching known and applied
revision/hash means current only while the connection observation remains valid.
Incompatible configuration and missing host bindings must be distinguishable.

Implementation order is authority/revision domain rules, validated portable
snapshot schema, durable atomic storage/application, authenticated enrollment and
transport, then Settings and MCP/skill consumers. Each stage must preserve the
single-master direction and secret boundary. End-to-end acceptance includes
initial enrollment, additions/updates/removals, disabled state, offline/reconnect,
rejected/partial updates, explicit master reassignment, and host-binding failures.
No runtime delivery is complete until those paths exist and are verified.

## Current persistence boundary

Migration 34 stores one durable instance ID, an initially unassigned role, and a
compare-and-swap version. Initial master designation is available through the
local administrative API; a follower role is entered only through the approved
enrollment exchange, with no general role-mutation route. Daemon startup never
chooses a role. Authority history binds every
seen group to its original master and retains follower observation metadata.
Role changes and observations commit atomically with the instance state version.
Stale expected state fails instead of changing a newer enrollment. An identical
observation or unchanged role is a no-op. Leaving a group preserves its history;
rejoining restores its highest observed revision. Integer storage uses SQLite's
signed 64-bit positive range and rejects exhaustion/overflow.

Migration 35 adds coherent snapshot payloads and active revisions as described
below. Migration 38 adds master-side credential digests and permanent revocation
records; migration 42 adds stable grant and request-attempt IDs; migration 43 adds
the immutable follower request journal and permanent lifecycle tombstones.
Migration 44 stores the master-side digest-only request journal, migration 45
indexes it by authority for bounded retained-record admission, and migration 46
stores durable exchange receipts. Migration 47 adds a private monotonic local
credential state. The local API exposes status, master designation, credential-
free grant metadata/list/revoke, follower request preparation/recovery/retirement,
explicit exchange, and one-shot snapshot fetch/application. The remote intake and
snapshot routes are available only through the isolated router; the daemon serves
them only when `KILN_CONFIGURATION_FOLLOWER_LISTEN_ADDR` is set and the current
master has a usable active managed identity.

## Internal follower read credentials

`ConfigurationReadCredential` generates a fresh `kcfg1_` bearer followed by 80
lowercase hexadecimal characters using the existing CSPRNG construction (320
random bits). It never reads or derives from the local API bearer. Parsing accepts
only that exact format; local API tokens and digest strings are not accepted.
The type deliberately has no Debug, Clone or serialization implementation.
Its SHA-256 digest covers the complete prefixed credential and is the only
credential representation stored in SQLite. Raw material must remain confined to
private enrollment delivery/storage and sensitive transport headers.

`ConfigurationAccessStore` is an internal master-side grant issuance/read
boundary. The isolated router accepts digest-only remote request claims, but no
operation delivers bearer material. The daemon exposes this router only through
the separately configured HTTPS listener.
Registration binds one digest permanently to a stable grant ID, the master/group, claimed
follower instance and original master state version. A stable request-attempt ID
deduplicates issuance and permits metadata-only recovery after a lost response.
Exact retries return the original record, including its current revoked flag,
without reactivation. A new request is rejected while any active grant exists
for the same group/follower. It does not advance the configuration state version
or publish content.

Revocation is permanent and repeatable by stable grant ID under the matching
current master state. Leaving a master role revokes all of that group's active
credentials in the role-change transaction; rejoining cannot revive them.
Publication and unchanged-role operations retain credentials. Reenrollment after
revocation requires a new request and explicit approval. Tombstones are retained;
there is no automatic expiry or garbage collection yet. A credential-free local
API lists and reads grant metadata, looks up an issuance attempt after an
uncertain response, and revokes by grant ID. It never returns a bearer or digest.
Existing legacy active duplicates are preserved during migration; new issuance
for that follower remains blocked until an administrator explicitly revokes the
old active grants. Identity retirement or historical-identity cleanup does not
revoke grants. Before re-enrollment, an administrator must revoke the previous
grant by its exact stable ID; any active legacy duplicates must also be revoked
before approval of a replacement grant.

`read_configuration_for_follower` receives the router's exact serving identity.
Before grant lookup, one transaction verifies current master authority and the
exact active managed identity ID, server name, CA fingerprint and leaf/CA
validity. A grant must have an issuance attempt that resolves to its immutable
request and approved lifecycle; the lifecycle's grant ID must equal the exact
grant, and the request authority, follower, server name and CA fingerprint must
match the current serving identity. Legacy grants without a trustworthy issuance
attempt fail closed. Only then does the transaction acquire the bounded, fully
validated snapshot. Unknown, revoked, mismatched and stale-identity bindings
receive the same denied category, including when no snapshot exists. Revocation
prevents subsequent reads; a read authorized before revocation or identity
retirement may still be in flight and its acquired bytes cannot be recalled.
This per-request check is not a serving lease and does not cancel an authorized
handler; listener supervision must cancel or drain affected handlers across
identity retirement before exposing a changed serving identity. The result grants
no general local API, publication, credential-vault or execution access. The
transport must hash a presented bearer, never accept a client-supplied digest as
proof, and no reusable authorization decision is returned.

The digest-only intake route, local exact approval/rejection, pinned client and
receipt validation are implemented. The daemon mounts the isolated router only
with explicit listener configuration and supervises the acquired identity. The
authenticated local exchange connects follower preparation to that pinned
client. A master administrator must still approve each exact request through the
local review API before the follower changes role. No live remote interaction
has been verified.

## Selected automatic enrollment direction

The selected design has the follower generate the final random `kcfg1_` bearer
and reserve it in the follower's OS vault before it contacts the master. The
follower approves the master's CA, server name, group and master instance over a
locally authenticated action, then submits an exact request over HTTPS pinned to
that identity. The request carries a stable request-attempt ID, the claimed
follower instance ID and the digest of the follower-held bearer; only the digest
is registered on the master. The raw bearer stays in the follower vault and is
never escrowed or re-delivered by the master.

The claimed follower ID and request digest are not remote identity proof. The
master's local administrator must approve that exact pending request, with the
request ID, claimed follower ID and displayed digest fingerprint bound together.
That approval is the trust decision for this initial design; the pinned TLS
connection authenticates the master to the follower, not the follower to the
master. A different request or digest requires separate review. Exact retries
reuse the follower's vaulted bearer and request ID. The pinned client validates
the receipt's complete request binding and fingerprint; retries recover current
request/grant state and never reactivate a revoked attempt. A follower that needs
a new credential first receives explicit revocation of its old grant, then
creates a new secret and request for approval.

The stable grant and request-attempt IDs, digest-only registration, exact retry
deduplication, metadata recovery, and local list/revoke API are implemented. An
internal follower preparer now reserves the immutable request binding, approved
master trust data, fresh vault reference, and credential digest in SQLite before
writing the final bearer to the separate OS vault. It verifies vault readback;
exact retries never generate or overwrite reserved material. Missing, malformed,
or changed material permanently retires the attempt before retryable cleanup.
The bounded journal returns credential-free metadata, while a separate outbound
submission value contains the exact request digest only when the local instance
is still unassigned at the recorded state version. Request identity and mutable
reserved/prepared/retired state are stored separately. Trust inputs require a
canonical DNS/IP server name and CA DER no larger than 2^24−1 bytes, the uint24
maximum length of one TLS Certificate entry.

The authenticated local `POST
/v1/configuration-sync/follower-enrollments/{attempt_id}/exchange` accepts the
expected follower instance, an HTTPS origin, and positive connect/request
timeouts on every call. The origin and timeouts are transport options, not part
of the durable enrollment identity. The strict exchange request body is capped
at 4 KiB; timeouts must be positive with connect no greater than request. Invalid
origin/pin or timeout values fail before remote submission. Infrastructure
verifies the vaulted bearer against its reserved digest, then passes the pinned
request and secret through a narrow core transport port; the daemon adapts that
port with `kiln-client`, so infrastructure does not depend on HTTP or protocol
types. The remote POST contains only the digest and binding claims and has no
Authorization header.

A pending receipt is persisted while the follower remains unassigned. A receipt
with an approved, active grant is recorded atomically with the follower's role
compare-and-swap; the credential stays in the OS vault for later snapshot reads.
Rejected, revoked, and role-conflict outcomes retire the local attempt before
retryable vault cleanup. Explicit local retirement also removes the credential
and leaves an already joined follower role unchanged. Finalized exchange retries
and exact re-preparation return durable metadata without another remote request
or role change, and never recreate a credential removed by explicit retirement.
Retry explicit retirement after a vault-cleanup error. The returned receipt is
last-observed metadata, not a live grant check; master-side revocation still
blocks subsequent snapshot reads.

## Explicit follower snapshot fetch and application

Protocol `0.38.0` adds the authenticated local
`fetch_configuration_follower_snapshot(attempt_id, request)` command at
`POST /v1/configuration-sync/follower-enrollments/{attempt_id}/fetch`. The strict
4 KiB request supplies the expected follower instance and per-call HTTPS origin,
connect timeout, and request timeout. These transport settings are not saved with
the enrollment. Only a durably approved attempt whose private credential marker
is active can fetch. Infrastructure reloads the exact request pin and retained
OS-vault credential, verifies the credential digest, and uses the bounded pinned
HTTPS client. It does not return or log bearer material, the vault reference, or
the private active marker.

The response is a bounded candidate, never active configuration. The pinned
client rejects non-success bodies, oversized responses, invalid authority IDs,
and malformed revision metadata. Infrastructure checks the response against the
exact approved master/group and commits its authenticated revision against the
full follower state captured before the network request. This observation commits
before bundle validation. The existing server-owned bundle validator then checks
canonical configuration, skill identifiers, package/file hashes, limits and
dependencies. Application uses the newly observed state and, in the same
transaction as replacement, rechecks the exact enrollment marker and follower
authority. The existing store enforces supported schema, content/revision hash,
monotonic watermark and atomic full replacement. Invalid content or a failed
application leaves the prior snapshot intact while retaining a newer valid
observation. An already-applied identical revision is returned as a no-op.

The enrollment manager holds its per-store operation lock across credential
acquisition, fetch, observation, validation and application. SQLite transactions
independently fence the active marker and exact follower state at acquisition,
observation and apply. Explicit enrollment retirement makes the marker
permanently inactive and commits before retryable vault deletion. Leaving a
follower role also retires the active marker in the role-change transaction, so
rejoining the same group requires a new approved attempt.

Migration 47 retains a private monotonic credential state separate from the
historical exchange receipt. It preserves only clearly pre-approval reserved or
prepared attempts without a terminal receipt as pending. All previously retired,
approved, terminal, or uncertain rows start retired; an old `Approved` receipt or
a credential that happens to remain in the vault is not evidence of current
activation. Existing followers with ambiguous historical approvals must leave
the follower role, retire the old attempt (retrying cleanup if needed), then
prepare and approve a new enrollment before fetching. The private marker is never
included in local metadata responses. The command performs one explicit fetch;
it does not poll, retry automatically, reconnect, or activate consumers.

The master now has an internal durable request journal for digest-only follower
claims. Exact retries are keyed by the follower attempt ID; the stable master
request ID binds that attempt, claimed follower and follower state version,
master/group authority, the follower-asserted server name and master CA
fingerprint, the credential digest, and the master state version at receipt.
Metadata exposes a full SHA-256 confirmation fingerprint over this immutable
binding without returning the raw digest or bearer. Admission is performed only
by the isolated follower router. Its composition binds an explicit serving
context containing the master authority, managed identity ID, server name and CA
fingerprint. Every new submission and exact retry checks that exact active
identity, the current master authority, and both certificate validity windows in
the same transaction before reading the attempt journal. A stale, expired,
retired or replaced identity fails closed, including for retries. The follower's
server-name and CA-fingerprint claims must match this context. There is no
uniqueness rule per claimed follower, so an unverified ID cannot reserve that
identity.

The internal master port lists and recovers requests, then atomically approves
or permanently rejects one only after exact request, attempt, follower, authority,
trust and full fingerprint confirmation. Approval checks the current master state
and requires the recorded server name and CA fingerprint to match the active
managed identity before creating the read grant in the same SQLite transaction
as the permanent decision. Retirement or replacement therefore blocks first
approval of an old pending request. Exact approval retries recover that grant's
current state, including revocation, without rechecking identity metadata; they
never reactivate it. Rejection remains possible after identity retirement.
Master role/authority drift fails closed.
The local authenticated API exposes bounded list/get and exact approve/reject
operations for the master journal. Decisions include every confirmation field
and the expected current master instance/version; approval creates the read
grant atomically and returns its current revoked state on retries. These local
routes do not accept new requests or expose the digest/bearer/vault reference.
Admission checks durable managed-identity metadata and its validity window, but
the router cannot introspect which certificate the TLS acceptor actually used;
its composition must bind the serving context to the active TLS identity. Neither
admission nor approval authenticates the claimed follower.

An authenticated local API exposes `POST
/v1/configuration-sync/follower-enrollments` with a stable `cra_` attempt ID,
expected follower instance/version, group/master IDs, approved server name and CA
DER. An exact retry reuses the original vault reference and bearer; changing any
request input with the same attempt ID conflicts. The response, `GET` lookup and
cursor-based `GET /v1/configuration-sync/follower-enrollments` return only attempt,
authority, follower/version, server name, CA fingerprint and lifecycle phase.
They never return CA bytes, a vault reference, digest or bearer. The list accepts
limits from 1 through 100 (default 50) and reads one lookahead row. CA DER and the
entire JSON prepare request are bounded to 2 MiB. `POST
/v1/configuration-sync/follower-enrollments/{attempt_id}/retire` requires the
expected follower instance ID, checks it against the journal, writes the permanent
tombstone first, then retries vault deletion on repeated requests.

The shared strict JSON extractor rejects a request over its 2 MiB body limit with
HTTP 400 `invalid_json`; it does not return HTTP 413 for this local route.

Master-local `GET /v1/configuration-sync/follower-enrollment-requests` pages
requests by stable `cfr_` ID, defaulting to 50 and accepting 1–100 rows with one
lookahead. `GET .../{request_id}` recovers one metadata record. The `POST
.../{request_id}/approve` and `/reject` routes accept strict confirmation JSON
bounded to 2 MiB. That body binds the path request ID, follower attempt/ID/version,
authority, asserted server name and CA fingerprint, received master version, and
the full confirmation fingerprint; it also fences the current master
instance/version. Responses expose those claims and the full fingerprint but no
credential digest, bearer, or vault reference. Follower ID remains an unverified
claim. Returned TLS metadata is historical request data, not a current identity
or serving-readiness assertion. First approval reports HTTP 409
`configuration_sync_conflict` when the active managed identity is missing or no
longer matches.

The isolated follower router also accepts `POST
/v1/configuration-sync/follower-enrollment-requests` with a strict JSON body
capped at 4 KiB. It carries the stable attempt ID, claimed follower ID and state
version, authority, approved master server name and CA fingerprint, and the
SHA-256 digest of the complete `kcfg1_` bearer. The bearer itself is never sent.
The restricted route does not accept an `Authorization` header; HTTPS authenticates
the master to the follower, while the submitted follower ID remains an unverified
claim. The receipt echoes the exact binding and full confirmation fingerprint,
and reports the current pending, approved or rejected state. The client
recomputes the confirmation fingerprint over the submitted digest and echoed
binding before returning the receipt. If approved, it includes the grant's
current revoked flag. Exact retries return this current receipt only after the
serving identity and certificate validity have been checked in the admission
transaction.

Each router caller sets a positive retained-record cap for its authority.
Admission counts pending, approved and rejected rows together; terminal records
remain retained and do not free capacity. At capacity, new attempt IDs return
HTTP 429 `configuration_follower_enrollment_request_capacity_reached`, while an
exact retry can still recover its receipt. Operators raise the configured cap
deliberately after exhaustion. The remote POST is available only on the isolated
router; the daemon enables it only with the opt-in listener configuration.

The local preparer does not change the local role or make a network request.
The explicit exchange does contact the selected pinned HTTPS origin and only
changes the follower role after observing an approved active grant. The master
journal does not establish device identity: pinned TLS authenticates the master
to the follower, not the follower to the master. A locally authenticated
administrator must review and confirm the exact request before a read grant is
issued. The local review API, exchange, and one-shot snapshot fetch/application
are implemented; a review UI, polling, automatic reconnect, and runtime consumers
remain unimplemented.

## Pinned HTTPS follower client

The Rust `ConfigurationSyncClient` is the restricted outgoing transport
component. The daemon uses it through narrow core transport ports for explicit
follower enrollment exchange and one-shot snapshot fetch; infrastructure stays
independent of client, protocol and server crates. Snapshot fetching is not wired
into ongoing daemon synchronization, and the client does not configure the
master's separate opt-in listener.
The isolated follower router described below serves
`GET /v1/configuration-sync/snapshot` with the existing snapshot response
and checks `kiln-configuration-master`, `kiln-configuration-group` and
`kiln-configuration-follower` headers against the presented read credential. These
headers carry claimed IDs, never authentication proof. The existing local API
does not recognize them as authorization and rejects the distinct sync bearer.

Construction takes an HTTPS origin, the approved canonical server name, one
explicit DER trust anchor, the bound master/group/follower IDs, a `kcfg1_` read
bearer and caller-supplied nonzero connect and total request deadlines.
Enrollment must approve this entire binding before sending the credential. No
snapshot or discovery response can replace it. The
trust anchor must be dedicated to that master's TLS identity; a public/shared CA
would broaden trust. This pins a certificate authority plus hostname, rather than
the exact leaf certificate: leaf renewal under that same authority/name remains
possible. Changing the authority, origin or master/group requires explicit
reenrollment. Certificate provisioning, enrollment preparation, master-side
approval, and listener setup are separate administration steps; this client does
not perform them.

The client uses only the supplied root, with normal hostname and certificate-chain
verification and TLS 1.2 or later. System roots, plaintext HTTP, proxies, redirects,
automatic retries, decompression and TLS key logging are disabled. Only the fixed
snapshot GET and enrollment-request POST are callable. The bearer and identity
headers are attached only to snapshot GET. Enrollment POST sends the typed request
binding and SHA-256 credential digest, without an Authorization header. Connect
timeout cannot exceed the total network deadline, which covers response-body
transfer as well as connection/headers. The caller chooses deployment-appropriate
durations; the client embeds no network-speed assumption. Creating the client
sends no network request and reads no credential or certificate file.

Only HTTP 200 is accepted. Non-success bodies are discarded without parsing or
exposing remote diagnostics. Enrollment receipts must echo the attempt,
follower/version, authority, server name and CA fingerprint exactly; the request
ID must have canonical syntax and the confirmation fingerprint must match the
submitted digest and complete echoed binding. An approved receipt must include
the matching grant and its current revoked flag. Snapshot response
bytes are capped at the existing 2 MiB transfer budget and enrollment receipts at
4 KiB; incomplete/oversized JSON is never returned.
The decoded instance and master IDs must both match the enrolled master and the
group must match the pin. Basic revision/state/hash syntax is checked. Successful
output is still a candidate: the receiving service must fully validate canonical
content, all hashes/paths/schema and monotonic revisions, fence concurrent
enrollment changes, and atomically apply the snapshot. No currentness claim or
runtime activation follows merely from constructing the client or fetching data.

## Isolated follower read service

`kiln-server::configuration_follower_router` builds a separate opaque router with
the snapshot GET and enrollment-request POST only. The daemon serves it only
through the opt-in TLS listener. It must never be merged into the local
administrative router. Its owner supplies authenticated HTTPS for the enrolled
master certificate, connection/request resource limits and shutdown handling.
The router itself binds no socket and
does not provision certificates, create grants, approve requests or change roles.

Construction takes the access store, an explicit expected HTTP Host authority,
the exact serving identity context, a positive retained-request cap, and the
trusted adapter's strict credential parser/one-way digest function.
Composition must use `ConfigurationReadCredential` parsing and hashing; raw
credential storage, digest-as-bearer acceptance and derivation from local API
authentication are forbidden. The server excludes the local bearer and digest
formats before invoking the adapter.

Snapshot reads accept only GET, including explicit rejection of Axum's automatic
HEAD fallback. Intake accepts only POST. Host must occur exactly once and match
the configured authority and serving name. Requests containing Origin or
WebSocket protocol headers are rejected; query strings are rejected. Snapshot
GET requires the sensitive bearer and each claimed identity header exactly once.
Enrollment POST rejects Authorization, bounds the strict JSON body to 4 KiB, and
parses IDs, fingerprints and digest with canonical domain parsers. Its transaction
checks current master role, exact active identity ID/authority/name/CA fingerprint
and leaf/CA validity before retry lookup or retained-count admission. It stores the
digest-only claim and returns the current journal receipt. Pending and terminal
records all count toward the caller's per-authority cap; exact retries remain
recoverable at capacity.

Snapshot GET rechecks the router's exact active serving identity and validity
inside the same transaction, then requires a grant linked by its issuance attempt
to an approved immutable request. Its lifecycle grant ID must equal that exact
grant, and request authority, follower, server name and CA fingerprint must match
the active serving identity. Legacy grants without issuance attempts fail closed.
Unknown, revoked, mismatched and stale-identity grants yield the same content-free
HTTP 401 with a Bearer challenge. Missing content yields 404 only after
authorization. Transfer budget failures yield 413; storage/integrity failures
yield 503. These checks run per request; they are not a serving lease or proof
that an already-authorized handler is cancelled. A request authorized before
identity retirement may remain in flight and return its acquired bytes, so
listener supervision must cancel or drain old handlers when the serving identity
changes. The router shares local export's full bounded JSON encoder and validator
limits. Every response, including fallbacks/errors, has `Cache-Control: no-store`.
Other paths have no registered operations, and no general API, publication, vault
or execution access is available.

Router compilation and managed TLS acquisition are not remote-delivery
acceptance. The daemon composes the active identity, bounded TLS server, and
isolated router at startup only when explicitly configured. End-to-end delivery,
retry and audit/recovery flow still need runtime acceptance; no remote HTTP
interaction has been verified.

## Bounded TLS serving component

`ConfigurationFollowerTls::from_der` consumes an explicitly supplied leaf-first
DER certificate chain and DER private key. Rustls checks key usability and that
the key matches the leaf certificate. The immutable, non-Debug/non-Clone wrapper
does not read files or provision an identity. Its TLS 1.2/1.3 server configuration
advertises only HTTP/1.1, disables session storage/tickets, and retains disabled
early-data/key-logging defaults. Followers authenticate with their restricted
bearer; client certificates are not required. Enrollment must separately approve
the master CA/hostname/group binding, and followers still verify that binding.

`ConfigurationFollowerTls::from_borrowed_der` accepts the same public chain and
a borrowed private DER slice. A managed key owner can retain its zeroizing buffer
through construction without copying that buffer into an ordinary `Vec`.
The ring backend parses the key into its owned signing representation, and
`CertifiedKey::keys_match` rejects an empty chain, unusable leaf, mismatched key
or unknown key consistency. The resulting TLS wrapper retains no borrowed key
bytes. Both constructors use the same protocol/session settings. Neither checks
current master authority, certificate lifetime or certificate-chain trust;
those remain the composition owner's responsibilities.

`serve_configuration_followers` accepts an already-bound listener, the opaque
`ConfigurationFollowerRouter`, TLS configuration, explicit resource budgets,
a graceful-shutdown future and an independent immediate-stop future. An
arbitrary administrative Axum router cannot be supplied. The daemon binds a
separate remote listener only when `KILN_CONFIGURATION_FOLLOWER_LISTEN_ADDR` is
set; its existing local API remains loopback-only. Readiness reports the remote
bind address and canonical HTTPS Host authority when enabled. See the
[configuration follower listener operations guide](../operations/configuration-follower-listener.md)
for required daemon settings and startup order.

`ConfigurationTlsLimits` requires a positive accepted-connection cap, HTTP/1
buffer bytes, and nonzero handshake/request/shutdown durations representable by
the local monotonic clock. Hyper requires a minimum 8192-byte buffer; values below
that documented library minimum are rejected. Other values are chosen by the
caller for the deployment, without invented default throughput or timeouts.
Accepted sockets share one task cap across TLS handshake, HTTP headers, storage
read and response. When full, the server pauses acceptance; the OS manages its
listening backlog. Completed tasks are reaped before more admission.

Each socket has one TLS handshake deadline and then one deadline covering the
entire HTTP request/response. HTTP/1 keep-alive is disabled, so each connection
serves at most one request; HTTP/2 and upgrades are not served. The whole-request
deadline replaces Hyper's separate header timer. Peer TLS/HTTP errors, disconnects
and expired deadlines close only that socket and expose no remote diagnostics.
A listener error or unexpected task failure stops admission and is reported as a
content-free service error.

Graceful daemon shutdown stops accepting and permits existing sockets to finish
for the configured drain duration. Remaining connection tasks are then aborted
and joined. Role loss, current identity replacement, expiry, or a supervisor
storage error stops admission and aborts/joins connection tasks immediately. The
supervisor re-reads current authority and exact identity metadata after committed
role/identity notifications, so unrelated historical-identity cleanup and
ordinary revision/version updates do not stop a valid generation. Once stopped,
that generation stays disabled until daemon restart.

The remote enrollment POST moves an accepted SQLite mutation into an owned task
with the same command permit used by the local daemon. Aborting a network handler
does not cancel that accepted mutation; graceful daemon shutdown waits for the
permit before store teardown. This is operation ownership, not a claim that
SQLite mutation cancellation is safe. Dropping the serving future also aborts
its owned connection tasks. Deadlines and cancellation are cooperative at async
polling boundaries: these are network and admission budgets, not hard CPU or
total process-memory guarantees. The existing snapshot/encoded-content limits
remain in force. No live TLS or remote HTTP acceptance has been performed.

## Host-local configuration vault

`ConfigurationSecretStore` and `OsConfigurationSecretStore` provide a separate
`dev.kiln.configuration-sync` vault service. Lookup keys include the local instance,
authority group, master instance, purpose and opaque `SecretRef`. Master CA and
TLS-identity purposes require the local instance to equal the authority's master;
follower read-credential purpose requires a different instance. These bindings
are metadata, not evidence that an enrollment remains active. The application
must check current role/version before resolving them.

Values reuse the existing redacted `SecretValue` boundary: nonempty, at most 1 MiB,
with no NUL/CR/LF. Binary key material therefore needs an adapter-owned single-line
encoded envelope. Raw secrets and their references are never shared snapshot
fields. macOS uses Keychain and Linux uses Secret Service through the existing
backend; there is no plaintext fallback or provider-account aliasing.

Writes/deletes reuse the existing per-entry locks, retained until an OS operation
finishes even if its caller drops the future. Clones share those locks. Writes
require a fresh reference reserved in durable metadata by the lifecycle owner;
this low-level adapter does not enforce current-role authorization, immutable
reference use or cross-process coordination. The managed-identity owner below
adds reservation, activation and recovery for CA/TLS keys; follower credential
enrollment still needs its own lifecycle. Constructing
the store performs no vault access. No actual vault operation has been exercised
for this synchronization namespace yet.

## Managed certificate and key envelopes

The default provisioning direction is Kiln-managed certificates.
`generate_configuration_identity` is an infrastructure primitive, not an enabled
administrative operation. It takes distinct fresh master CA/TLS bindings for
one authority, an exact canonical lowercase DNS name or canonical IP address,
explicit validity timestamps and a caller-supplied current time. Wildcards,
URLs/ports and noncanonical names are rejected. DNS labels/total length follow
the RFC limits of 63/253 bytes. The leaf must be valid at the supplied time and
cannot outlive its CA. No validity duration is chosen implicitly.

The generator uses rcgen with fresh independent ECDSA P-256/SHA-256 keys. The CA
has path length zero and certificate/CRL-signing usage. The leaf explicitly is not
a CA, has digital-signature/server-auth usage, and contains only the chosen DNS/IP
subject alternative name. It returns the public CA/leaf DER, a SHA-256 CA
fingerprint and two separate redacted vault envelopes. The primitive persists nothing,
installs no system trust and activates nothing; root identity approval remains part of
enrollment. Renewal, revocation distribution and imported-identity handling remain
future lifecycle work.

Schema-1 private-key envelopes contain exact instance/group/master/purpose/reference
bindings and DER key bytes encoded as a compact JSON byte array inside SecretValue.
They remain subject to the existing 1 MiB secret-value limit and are never snapshot
content. `decode_configuration_private_key` rejects unknown fields, mismatched
bindings/schema and unusable DER keys. It returns a non-Debug/non-Clone wrapper
whose owned DER buffer is zeroized on drop; decoding exposes bytes only through
an explicit accessor. Generator key serialization buffers also use rcgen's
zeroization support. Matching the decoded key to its public
certificate remains the TLS adapter's responsibility, and the caller must still
check current role/version before any vault read or activation.

The primitive uses rcgen for certificate construction. It and the envelopes have
compile/source validation only so far; no certificate
generation, vault write, handshake or trust installation has been exercised.

## Durable managed identity setup

`ConfigurationIdentityProvisioner` composes the SQLite journal, managed generator
and configuration OS vault. It is an internal infrastructure adapter, not an
HTTP operation or daemon startup action. A request carries the exact expected
master state version, distinct fresh CA/TLS references, canonical server name
and explicit validity. Keep that exact request for uncertain retries; its CA
reference is the durable command identity.

Migration 39 stores immutable public certificates, original request metadata and
SHA-256 hashes of the two complete secret envelopes. Keys are generated in memory
first, then all metadata and references are reserved in one immediate transaction
before either vault write. Only the exact current master state can reserve work.
Both reference columns are checked against both historical columns, including
retired rows, and only one pending/active identity is allowed per group. Replacing
an identity therefore requires explicit retirement and fresh references; this
is not automatic certificate renewal or follower trust migration.

Provisioning writes each reserved slot once, reads both envelopes back, checks
their immutable hashes and exact binding/key decoding, then marks the identity
active while rechecking current master authority in the activation transaction.
The supplied clock is sampled after queueing and again immediately before
activation: vault delays cannot bypass the configured validity window. Normal
snapshot publication may advance the state version without invalidating this
reservation. Leaving the master role retires all its pending/active identities
inside the role-change transaction, permanently fencing old work even on rejoin.

An exact retry loads the journal instead of generating or writing replacement
keys. If both original envelopes reached the vault it can finish activation;
missing, malformed or changed envelopes retire the attempt and require fresh
references. Temporary vault/storage failures retain the record for retry.
Expired requests cannot activate and require explicit retirement before a fresh
setup. A returned active record describes durable setup state, not a reusable
proof of current role, certificate validity or permission to open a listener.
Matching a loaded public certificate/key and verifying its chain remains part of
TLS composition; database contents are host-local trusted metadata.

`acquire_active_tls_identity(expected, identity_id, clock)` performs read-only
managed TLS material acquisition. It requires the exact current master state
and public identity ID, not the older state version at identity reservation.
It checks active status, matching authority and the validity window before
reading only the server-key vault envelope. The immutable envelope hash and
full reference/authority/purpose binding must match before the DER key is used.
It then reloads state and identity metadata and samples the explicit clock again;
state advancement, retirement, identity change or expiry during the vault read
fails closed. The caller must reload state and retry after even an ordinary
publication advances the expected version.

The returned `ConfigurationTlsIdentity` owns public leaf-first certificate bytes
and a private key in the existing zeroizing wrapper. It implements neither
Debug nor Clone and exposes private DER only by borrowing; vault references and
the CA private key are not returned. Pass the public chain and borrowed key to
`ConfigurationFollowerTls::from_borrowed_der`, which checks leaf/key consistency:

```rust
let identity = provisioner
    .acquire_active_tls_identity(&expected_state, &identity_id, clock)
    .await?;
let tls = ConfigurationFollowerTls::from_borrowed_der(
    identity.certificate_chain_der().to_vec(),
    identity.private_key_der(),
)?;
```

The acquisition task retains the lifecycle operation lock through vault read,
decode and final validation even if its caller disconnects. Read failures do not
write, retire, delete or replace identity material; explicit retirement/recovery
remains available through the existing lifecycle operations.

Acquisition is a checked snapshot, not a lease: authority, identity or time may
change after it returns. The eventual runtime owner must fence activation and
stop or revalidate serving on retirement, role changes and expiry. This method
does not enable a listener, verify a live TLS connection or establish follower
trust, and startup does not call it.

Provisioning and cleanup share a store-owned mutex across clones. Spawned tasks
retain ownership through vault work even when the caller disconnects, preventing
another lifecycle operation from overtaking a late OS effect. Callers must use
one daemon/store owner and cloned vault handles; this is not cross-process
coordination or protection against direct writes through the low-level vault.
Daemon admission/shutdown integration must account for these owned operations
before runtime activation is enabled.

`get` and cursor-based `list_references` expose public journal metadata for
restart recovery, with an explicit nonzero page limit. `retire_and_cleanup`
permanently retires a record before attempting both vault deletions. Tombstones
remain after success, so uncertain deletions can always be retried and references
can never be reused. Role changes and incomplete recovery retain discoverable
cleanup work; they do not perform OS effects inside SQLite transactions.

Validation for the TLS constructor and active identity acquisition included
`rtk cargo check --locked -p kiln-infrastructure -p kiln-server -p kiln-daemon`,
focused Rust formatting checks, and `rtk git diff --check`. No tests, actual
certificate generation, vault operations, cancellation/recovery scenarios or
TLS handshakes have run. Remote credential delivery, follower fetch/apply and
consumers remain open; enabling the optional listener does not complete those
flows.

## Local managed identity status

Protocol `0.30.0` adds authenticated `GET /v1/configuration-sync/identity` and
Rust-client `get_configuration_identity_status()`. It returns current instance,
state version, role, group/master IDs and an explicit nullable `identity`, read in
one SQLite transaction. Only a current master's pending/active identity is
included. Unassigned/follower instances, and masters without a live setup record,
return `identity: null`; retired and historical-group identities are excluded.

The identity summary contains an opaque public identity ID, `pending`/`active`
phase, canonical DNS/IP server name, SHA-256 CA fingerprint and the configured
not-before/leaf-expiry/CA-expiry Unix seconds. The ID names the lifecycle record;
it is independently generated and cannot reveal either vault reference. No
private keys, key envelopes, SecretRefs or certificate bytes are returned, and no
vault read or recovery mutation occurs. `active` describes a past successful
setup transition: it does not guarantee current key availability, current
certificate validity, follower trust or a running remote listener.

Successful responses have `Cache-Control: no-store`. The route inherits the
local API's bearer, Host and Origin protections. Unavailable/invalid persistence
returns content-free `503 configuration_sync_unavailable`; no pending identity is
silently treated as ready. Setup/retirement commands and desktop controls are
described below. The separate follower router does not expose this route.

## Explicit managed identity setup and retirement

Protocol `0.31.0` enables authenticated local
`POST /v1/configuration-sync/identity`. Its strict request supplies
`expected_instance_id`, `expected_group_id`, `expected_state_version`, canonical
`server_name`, and explicit `not_before_unix_seconds`,
`leaf_not_after_unix_seconds`, `ca_not_after_unix_seconds`. A nonempty
`Idempotency-Key` of visible ASCII characters without whitespace is required.
The key is also used in a JSON cleanup request; this restriction prevents HTTP
whitespace normalization from changing its identity. This is an explicit
key-creation command: no
validity defaults or automatic setup are inferred from status reads or startup.

Migration 40 binds the command key to the immutable identity journal in the same
reservation transaction, before any vault write. Fresh commands allocate random
CA/TLS references inside the daemon and require the exact current master/version.
An existing key is resolved before allocating references; changed instance,
group, version, name or validity yields `409 idempotency_conflict`. Exact retries
use the original references and envelopes. No row or vault write exists if
validation/generation fails before reservation, so that attempt has no durable
receipt. Legacy explicit-reference identities have a null command key and remain
manageable through the internal lifecycle API.

A successful `200` receipt contains `instance_id`, `group_id`,
`reserved_state_version` and the public opaque `identity_id`. Migration 41 adds
independent public IDs to the identity journal, backfills existing rows with
random IDs, and enforces uniqueness and immutability. A uniqueness collision
aborts migration instead of changing or aliasing an existing target. Setup/status
responses never serialize the CA or TLS SecretRefs. Reload status after success;
the original reserved version is not current authority status. Malformed input
or invalid validity yields `400 configuration_sync_invalid_request`; stale
state, occupied identity or retired work yields `409 configuration_sync_conflict`.
Incomplete/changed vault material is retired and yields
`409 configuration_identity_recovery_required`. Storage/vault failures yield
content-free `503 configuration_sync_unavailable`. No error includes private
material or vault diagnostics.

`POST /v1/configuration-sync/identity/retire` takes a strict body containing
`expected_instance_id` and the original `setup_idempotency_key`. It resolves the
same permanent journal binding even when setup never returned an identity ID,
checks local ownership, retires it, and attempts deletion of both keys. Success
is `204`; missing/wrong-instance targets are conflicts. Retrying the exact body
is safe after an uncertain result or deletion failure. The original setup key
can never create another identity, even after successful cleanup; use a fresh key
and current state for a replacement.

Protocol `0.33.0` adds credential-free grant list, status, attempt-recovery and
permanent-revocation endpoints under `/v1/configuration-sync/grants`. Exact
request-attempt retries return the same immutable metadata, including a revoked
tombstone; no endpoint returns a bearer or digest.

Protocol `0.32.0` adds `POST /v1/configuration-sync/identity/retire/by-id`, which
takes a strict body with
`expected_instance_id` and the public `identity_id`. It resolves the unique,
immutable ID and verifies the recorded owner before writing the retirement
tombstone and deleting both vault keys. It does not require that the identity's
master role is still current, so a retained historical ID remains cleanable after
role loss. The ID cannot select a replacement identity; replacement receives a
new ID. Exact request retries safely repeat tombstone cleanup. Status continues to
return only the current master's pending/active record and does not enumerate
historical identities. Internal explicit-reference identities and historical
cleanup enumeration remain available through the lifecycle adapter.

Both commands inherit the local API's bearer/Host/Origin protections and return
no-store on success. The command gate rejects new writes once shutdown starts.
An owned handler task retains its command permit while the lifecycle's owned
vault task runs, so a client disconnect cannot cause graceful shutdown to
release database/process ownership early. Shutdown waits for those accepted
operations; there is no arbitrary timeout that abandons an uncertain OS write.
The supplied daemon wall clock is sampled after queueing and at activation.

The Rust client exposes `configure_master_identity(key, request)`,
`retire_master_identity(request)` for original-key recovery, and
`retire_master_identity_by_id(request)` for a known stable target; it does not
automatically retry them. Both retirement clients require HTTP 204. Retain the
exact setup key/request after an uncertain setup and the exact target request
after uncertain retirement. No follower credential, trust installation or
snapshot publication is created by these commands. Replacement root trust still
needs explicit follower approval. Retiring or replacing a serving identity stops
the current listener generation; restart the daemon after provisioning a valid
replacement. Desktop offers explicit setup and retirement confirmation, preserves exact
uncertain requests within the live Settings connection, and reloads both status
views after confirmed changes. App restart reloads the current stable identity ID
from status, allowing retirement without the original setup key. Live vault,
cancellation and shutdown acceptance remain unverified.

## Initial local master designation

Protocol `0.27.0` adds authenticated `POST /v1/configuration-sync/master` with a
required `Idempotency-Key`. Its strict JSON body carries `expected_instance_id`
and `expected_state_version`, obtained from the status endpoint. The expected
version must be positive and below SQLite's maximum signed 64-bit integer.
Only the matching, still-unassigned instance may be designated. A master or
follower cannot be replaced or promoted through this route.

The service generates a fresh group ID. Migration 36 retains the request identity
and original result in the same transaction as authority creation and the instance
version increment. An exact retry returns the original receipt even after a later
role change; it never reapplies designation. Reusing a key with different request
values returns `409 idempotency_conflict`. A stale version, wrong instance, or
already-assigned role returns `409 configuration_sync_conflict`; malformed values
return `400 configuration_sync_invalid_request`. Storage failures remain
content-free `503 configuration_sync_unavailable` responses. Failed transactions
leave no role, authority, or receipt changes.

The `200` response contains the original `instance_id`, resulting `state_version`,
and `group_id`. It is a command receipt, not current status. Callers must reload
status after success. Settings offers **Designate as master** only for an
unassigned instance, identifies that instance in a confirmation, retains the
exact request/key for ambiguous retries while the connection view survives,
and reloads status rather than displaying an old receipt as the current role.
Replacing the connection discards pending UI state and loads durable status.

Designation records authority only: it publishes no snapshot, reads or copies no
credentials, grants no remote access, and starts no MCP/skill consumer. Explicit
publication is a separate command described below. Follower enrollment is a
separate explicit exchange and requires master approval; this command does not
replace or promote an assigned instance. Snapshot fetching and runtime consumers
remain open. Shutdown rejects new designations through the existing command
gate.

## Explicit local snapshot publication

Protocol `0.28.0` adds authenticated `POST /v1/configuration-sync/publications`,
exposed by the Rust client as `publish_configuration_snapshot`. The request carries
`expected_instance_id`, `expected_group_id`, `expected_state_version`, and a complete
`snapshot` bundle. Only the matching current master may publish. The existing
local bearer/Host/Origin checks and shutdown command gate apply; this is not a
follower transport credential or remote publication endpoint.

The bundle contains `metadata_json`, the exact canonical schema-1 metadata string,
and a `skills` array. Each package supplies `id`, `version`, `enabled`,
`dependencies`, and `files`. Each file supplies a portable `path`, explicit
regular-file `content` as a JSON array of integers from 0 through 255, and its
SHA-256 `content_hash`. All object fields are strict. File/package hashes, paths,
dependencies, settings and MCP definitions are revalidated through the existing
core validators. Exact canonical metadata regeneration binds the complete package
contents and rejects missing packages, changed hashes, extra metadata and
unsupported schema. Binary files need no archive extraction or text conversion.

The entire serialized JSON request, including metadata escaping and byte-array
overhead, is capped at 2,097,152 bytes, preserving the existing Axum JSON request
ceiling. Per-field, count, metadata, and aggregate file limits are bounded by that
same request budget. These are transport bounds, not recommended catalogue sizes
or a process-memory ceiling. Larger bundles require a separately designed transfer
protocol. Oversized or malformed JSON follows the existing `400 invalid_json` /
`400 invalid_request` handling; invalid bundle content or preconditions use
`400 configuration_sync_invalid_request`.

Publication replaces all shared categories at once: omitted definitions and files
are removed; an empty snapshot clears the group's shared content. It does not
merge with the previous snapshot. The caller must explicitly prepare the complete
content to share. The command never scans local configuration directories, reads
the vault, resolves host bindings, installs skills, or starts MCP servers. Explicit
user-authored literals/file bytes are not scanned for embedded secrets.

`Idempotency-Key` is required. Migration 37 atomically records the expected
instance/group/version, canonical content hash, and resulting revision alongside
the payload replacement and CAS increment. Exact retries compare validated content
identity and return the original immutable receipt, without publishing another
revision or restoring old content after later changes. Changed content or
preconditions under the same key return `409 idempotency_conflict`. A fresh key
against stale state or the wrong authority/role returns
`409 configuration_sync_conflict`. Receipts retain metadata only, not historical
payloads. Storage/integrity failures return content-free
`503 configuration_sync_unavailable`.

The `200` response includes the original instance/group IDs, resulting state
version, and revision number/schema/content hash. Reload status to see the current
stored revision. Publication does not establish remote currentness or activate
configuration consumers. Desktop Settings supports explicit bundle import with a
replacement preview/confirmation and exact retries within the connection view.
One-shot remote follower fetch/application is implemented; ongoing distribution,
reconnect, and runtime activation remain incomplete.

## Verified local snapshot export

Protocol `0.29.0` adds authenticated `GET /v1/configuration-sync/snapshot` and the
Rust client's `get_configuration_snapshot`. It reads the active group's complete
stored bundle and its instance, group, master, state version, and revision metadata
in one transaction. The existing reader revalidates canonical metadata, package
and file hashes, portable paths and dependency consistency before returning data.
No active group or no stored snapshot returns `404 configuration_snapshot_not_found`.
Corrupt/unavailable content returns `503 configuration_sync_unavailable`.

Before fetching metadata/payload rows, the reader bounds combined canonical,
package, dependency, path and hash metadata at 2 MiB and total file bytes at 2 MiB,
in addition to individual and count budgets. A bounded serializer then caps the
complete encoded JSON response at 2 MiB, including byte-array expansion and the
response envelope. Exceeding a read or encoded-output budget returns
`413 configuration_snapshot_too_large`; no partial JSON or file content is sent.
Import and export envelopes differ, so publication acceptance does not guarantee
that a bundle at the size ceiling is exportable in this format. These budgets
bound data, not total process memory.

Successful responses use `Cache-Control: no-store`. The Rust client bounds both
success and error response bodies while streaming, rejects an oversized declared
or observed body with `ConfigurationSnapshotTooLarge`, and parses only complete
JSON. It returns the original explicit file bytes and hashes; it does not write
files, install packages, alter host bindings, or activate runtime consumers.
Use the response's `snapshot` to prepare a later complete publication, after
checking the current local master and preconditions. Exported identity/hash fields
are metadata, not follower enrollment or remote peer authentication proof.

Desktop **Export stored bundle…** saves only the returned `snapshot` after checking
that the selected instance/group/state still match. It uses an explicitly chosen
new destination and never overwrites an existing file. **Import bundle…** reads
one regular JSON bundle, previews category counts, and freezes the displayed master
preconditions for confirmation. It does not copy the imported file's origin
authority or enroll a follower. Full validation stays in the daemon's publication
boundary; the desktop preview is not an integrity verdict. See the
[desktop controls](../operations/desktop.md#configuration-synchronization-status).

## Portable skill package validation

The initial core package validator accepts explicitly supplied regular-file bytes
with expected SHA-256 hashes. It checks every file before producing a validated
package, requires a nonempty UTF-8 root `SKILL.md`, and rejects duplicate/self skill
dependencies. Package IDs are lowercase ASCII letters, digits, hyphens, and
underscores. Versions are opaque nonempty ASCII graphic strings, not interpreted
as semantic versions or paths. Caller-supplied limits bound IDs, versions,
dependencies, file count, each path, each file and combined file bytes; no default
package or host-memory budget is inferred.

For this first portable format, each path segment uses only ASCII letters, digits,
periods, hyphens, and underscores, separated by forward slashes. Empty, dot and
dot-dot segments, trailing periods, absolute/drive paths and reserved Windows
device stems are rejected. Case-insensitive duplicate files, inconsistent directory
spelling, and file/directory conflicts are rejected on every host. Unicode and
space-containing filenames are outside this initial format. Host-specific length
limits still need validation before materialization.

Canonical package hashes bind ID, version, enabled state, sorted dependencies and
sorted file paths, sizes and hashes using length-delimited fields and a versioned
domain separator. Input order does not change the package hash. Debug output
reports counts and sizes without file content or paths. Hash equality proves
content correspondence, not publisher trust or the absence of secrets in supplied
bytes. Filesystem importers must separately refuse symlinks/special entries and
limit what the user selected for sharing; this pure validator never reads the
filesystem, scans the vault, extracts files, executes scripts, or installs a skill.
Cross-package dependency checks now belong to the complete snapshot validator;
SQLite snapshot persistence is described below; filesystem materialization and
runtime consumers remain open.

## Coherent shared snapshot schema 1

The core snapshot validator accepts all three categories together and returns an
immutable snapshot only after validation. Bounded canonical metadata restoration
reconstructs settings/MCP definitions with the complete validated skill payloads
and requires byte-for-byte regeneration. Extra/duplicate fields, unknown schema,
missing packages, mismatched hashes and noncanonical metadata are rejected. This
is an internal persistence format, not an authenticated sync transport or a
filesystem importer.

The initial global-settings allowlist is `model_defaults`: a logical local account
binding, exact provider/model and generation/reasoning settings, and versioned
model capabilities. There are no local provider-account IDs, credentials, host
resource budgets or paths in this setting. Additional UI or runtime preferences
must declare their shared scope and extend the schema explicitly.

MCP entries have stable portable IDs, enabled state, and one transport:

- `stdio`: a logical runtime binding, ordered literal/host-binding arguments, and
  environment variable names mapped to local bindings. Environment names follow
  portable identifier syntax and cannot collide under ASCII case folding.
- `https`: a normalized HTTPS endpoint plus an optional local credential binding.
  URI user information, query strings, fragments and whitespace/control characters
  are unsupported; use host bindings for local or credential-bearing endpoints.
- `host_endpoint`: a logical endpoint binding resolved and validated on each host.

Bindings remain names until the host explicitly maps them. Literal arguments are
explicit portable data and may not contain NUL; the validator does not interpret
shell syntax, detect secrets inside arbitrary literals, or make master-local paths
portable. Exporters must classify local values into bindings instead of copying
them as literals. Synchronization does not grant network access, launch a server,
install a runtime, or create an approval grant.

The skill catalog rejects duplicate IDs, missing dependencies, enabled packages
that depend on disabled packages, and cycles (including disabled-package cycles).
The graph walk is iterative. Counts, argument/endpoint/key bytes, total skill files
and bytes, and canonical metadata bytes have explicit caller limits. Package-level
validation limits remain independently required before constructing a snapshot.

Canonical metadata sorts object keys and definition IDs while retaining MCP
argument order, and includes each complete skill package hash. The versioned
snapshot hash binds the metadata and therefore settings, server definitions,
enabled state, skill content and dependency metadata. `verify_revision` checks
schema/hash correspondence only; the follower authority/watermark and authenticated
master checks are still separately required. No content is applied by validation.

## Atomic snapshot persistence

Migration 35 stores one complete active snapshot per authority group with its
revision/schema/hash, canonical settings/MCP metadata, skill package metadata and
regular-file bytes. Retained groups remain separate: leaving a group does not
activate its data under a new group. Superseded content within one group is replaced,
including removed packages and files; complete historical payloads are not retained.
The active revision remains monotonic, and rollback is publication of older content
at a new revision.

Internal master publication requires the current master role and exact expected
instance state, then allocates the next positive SQLite-representable revision.
Follower application requires the current follower role, matching enrolled
authority, matching schema/content hash and the applied/observed revision checks.
An identical already-applied revision is a no-op. A valid newer snapshot atomically
replaces all categories and file data, advances the instance CAS version and records
the follower observation. Publication also advances the CAS version. Failed writes
roll back the entire change. These methods assume an already authenticated and
authorized caller; they do not enroll or authenticate peers.

Reads select the current group's state and snapshot in one transaction. They
check metadata size, package/file counts and byte lengths before loading payloads,
including an explicit aggregate metadata budget before fetching package/file rows,
then revalidate file hashes, package hashes/paths/dependencies, canonical metadata,
snapshot hash and revision binding. Lower caller budgets fail explicitly. This is
a data budget, not a process-memory bound. Returned snapshots own their verified
bytes and can be pinned by consumers across a later update. No method extracts
files, changes host bindings, starts MCP servers, provisions secrets, or switches
an already running model invocation.

Fresh schema/startup and SQL preparation have been checked. Populated upgrades,
publication/application/duplicate/concurrency paths and content restoration remain
unverified at runtime; no synchronization transport or end-to-end acceptance is
claimed.

## Authenticated local status API

Protocol 0.26.0 adds read-only `GET /v1/configuration-sync`, exposed by the Rust
client as `get_configuration_sync_status`. It uses the daemon's existing local
bearer, Host and Origin checks. The response contains instance ID, CAS state
version, role, nullable group/master IDs, and nullable applied/observed revision
metadata (revision number, schema and content hash). These fields are read in one
transaction. No snapshot payload, skill file, endpoint, host binding, or credential
is returned.

`transport` remains `unconfigured` until ongoing authenticated snapshot fetching
and application are connected. The explicit one-shot fetch does not enable
ongoing synchronization or establish a freshness lease. Enrollment exchange
alone does not establish currentness. Equal applied/observed revisions alone do
not establish currentness.
The endpoint reports committed metadata, not a fresh validation of every stored
payload byte. Store/integrity failures produce content-free HTTP 503
`configuration_sync_unavailable`. This endpoint cannot designate a master,
enroll a follower or publish/apply configuration. Settings presents this status;
master designation and publication use the separate commands above.
