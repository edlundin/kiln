# Master-instance configuration synchronization

EDL-322 requires one explicitly designated master Kiln instance to supply shared
settings, global MCP configuration, and global skills to followers, including
remote hosts. This document specifies the initial contract. It does not claim
that remote enrollment, synchronization transport, storage, or UI already exists.

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
compare-and-swap version. Master/follower assignments are internal administrative
operations; daemon startup never chooses a role. Authority history binds every
seen group to its original master and retains follower observation metadata.
Role changes and observations commit atomically with the instance state version.
Stale expected state fails instead of changing a newer enrollment. An identical
observation or unchanged role is a no-op. Leaving a group preserves its history;
rejoining restores its highest observed revision. Integer storage uses SQLite's
signed 64-bit positive range and rejects exhaustion/overflow.

This storage does not yet contain snapshot payloads, applied revisions, enrollment
credentials or remote endpoints. It exposes no network routes and grants no
transport authentication. Schema/content verification, active snapshot storage,
public status projection and administrative enrollment UI remain subsequent work.

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
Cross-package dependency checks and snapshot persistence/application remain open.
