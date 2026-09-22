# Master-instance configuration synchronization

EDL-322 requires one explicitly designated master Kiln instance to supply shared
settings, global MCP configuration, and global skills to followers, including
remote hosts. Durable authority/snapshot storage, status, initial local master
designation, and explicit local snapshot publication are implemented. Remote enrollment, synchronization transport, and
runtime activation remain incomplete.

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
Network listeners and enrollment routes remain disabled until that boundary exists.

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
compare-and-swap version. Initial master designation is available through the local
administrative API; other role changes remain internal operations. Daemon startup
never chooses a role. Authority history binds every
seen group to its original master and retains follower observation metadata.
Role changes and observations commit atomically with the instance state version.
Stale expected state fails instead of changing a newer enrollment. An identical
observation or unchanged role is a no-op. Leaving a group preserves its history;
rejoining restores its highest observed revision. Integer storage uses SQLite's
signed 64-bit positive range and rejects exhaustion/overflow.

Migration 35 adds coherent snapshot payloads and active revisions as described
below. Enrollment credentials and remote endpoint associations remain absent.
The local API exposes status and initial master designation, described below.
Remote enrollment and consumers remain subsequent work.

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
publication is a separate command described below. Follower enrollment, role
replacement, and remote transport remain open. Shutdown rejects new designations
through the existing command gate.

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
configuration consumers. Desktop bundle import, authenticated remote distribution,
and runtime activation remain incomplete.

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

`transport` is explicitly `unconfigured` until authenticated follower transport
exists. Equal applied/observed revisions alone do not establish currentness.
The endpoint reports committed metadata, not a fresh validation of every stored
payload byte. Store/integrity failures produce content-free HTTP 503
`configuration_sync_unavailable`. This endpoint cannot designate a master,
enroll a follower or publish/apply configuration. Settings presents this status;
master designation and publication use the separate commands above.
