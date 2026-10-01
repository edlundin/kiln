# Desktop client

Kiln Desktop is a native GPUI client for the local `kilnd` daemon. The daemon
is a separate process and continues to run if the window closes. The current
desktop run path uses the deterministic model fixture. Settings can connect
a Codex subscription account, but Runs do not yet use that live provider.
An explicit [native public API mode](native-public-api.md) is also available for
separately imported public API accounts; live acceptance remains unverified.

The native coordinator includes an optional scoped `read_file` implementation.
It remains disabled unless both `KILN_NATIVE_READ_FILE_MAX_PATH_BYTES` and
`KILN_NATIVE_READ_FILE_MAX_BYTES` are set to positive decimal byte limits.
Choose the file ceiling for the host's memory budget: a read buffers the complete
file and may copy it into artifact storage. Missing, invalid, or overflowing
limits prevent startup when either setting is supplied. These settings register
the local implementation; the deterministic model still advertises no tool
support and cannot invoke it. Public API models must explicitly declare tool
support before the coordinator supplies this catalog.

The desktop has only been verified on macOS. It requests the host fonts `Inter`
and `IBM Plex Mono`; the application bundle does not include font files.

## Build and run

Build the daemon and desktop from the repository root:

```nu
cargo build -p kiln-daemon --bin kilnd
cargo build -p kiln-desktop --bin kiln-desktop
```

Start the daemon in one terminal. Use the deterministic model executor:

```nu
$env.KILN_RUN_EXECUTOR = "deterministic-model"
cargo run -p kiln-daemon --bin kilnd
```

Keep this process running. On startup, it writes one JSON readiness line to
standard output:

```json
{"address":"127.0.0.1:49152","credential_path":"/path/to/credential","event":"ready","protocol_version":"0.29.0"}
```

Use the values from the actual line. `address` is the bound loopback address.
`credential_path` is the path to the bearer-token file; pass the path to the
desktop and do not copy the token into command history. `protocol_version`
reports the daemon protocol version. The `event` value is `ready`.

Start the desktop in another terminal:

```nu
$env.KILN_DESKTOP_ADDRESS = "127.0.0.1:49152"
$env.KILN_DESKTOP_TOKEN_FILE = "/path/from/credential_path"
$env.KILN_DESKTOP_REPOSITORY = "/absolute/path/to/git/repository"
cargo run -p kiln-desktop --bin kiln-desktop
```

These variables fill the connection form. The desktop connects automatically
when the address and token-file path are set. With only those two values, it
opens the saved Workspace and Session browser. Set a repository or session ID
to open a specific Session after connecting. You can also start it without
these variables and enter the values in the form.

To make a debug macOS application bundle, run:

```nu
sh apps/desktop/package-macos.sh
open target/debug/Kiln.app
```

The script builds the desktop binary and creates `target/debug/Kiln.app` with
the binary and `Info.plist`. Start `kilnd` separately before you connect.

The desktop dependencies are pinned exactly to `gpui-pre` 0.3.2,
`gpui-pre-platform` 0.3.2, and `gpui-component` 0.6.1. Update these pins
together only after checking compatibility.

## Configuration synchronization status

Open **Settings** to see the connected daemon's configuration role, instance ID,
group and master IDs, stored snapshot revision, and highest observed revision.
**Refresh status** reloads that metadata; a failed request offers **Retry status**.
Loading this section does not block provider-account actions. Disconnecting
clears the displayed metadata and invalidates this view's pending operation;
reconnecting loads it again. Work already accepted by the daemon may still complete.

Without opt-in daemon refresh settings, automatic follower refresh reports
**unconfigured**; a follower can still fetch manually when it has an approved
attempt record. Stored and observed revisions describe durable metadata, not
remote connectivity or activation in running Sessions. For an unassigned instance,
**Designate as master** shows the instance
ID and asks for confirmation before creating its configuration group. **Keep
unassigned** cancels without changing anything. A stale choice is rejected;
refresh status before choosing again. If the result is unconfirmed, **Retry
designation** reuses the original request and cannot create another group.
The view reloads current status after success. An existing master or follower
cannot be reassigned through this control.

On a master, **Managed master identity** loads the public identity status with the
authority state. The server name is the exact DNS name or IP address followers
will use. Leaf and CA validity are editable whole-day values, initially 90 and
365 days; the CA period must be at least as long as the leaf period. **Review
identity setup…** freezes the displayed master instance/group/state, name and
validity, then asks for confirmation before creating either private key. Kiln
stores both keys in the daemon host's OS vault. Setup does not start a listener,
enroll followers or enable remote synchronization.

For an active identity, **Export public CA certificate…** downloads the public DER
for that exact identity and verifies its SHA-256 against the displayed fingerprint
before writing a new user-selected filename. Existing files are never overwritten.
The authenticated local endpoint checks that the identity still belongs to the
current master and is active. Export reads no vault entry and includes no private
key. Share the DER file, group/master IDs and server name through a trusted channel;
compare the full fingerprint again on the follower before preparing enrollment.
Export is a public trust artifact, not proof of current certificate validity or
an enabled TLS listener. Disconnect before saving rejects stale picker/download
responses; a file write already started may finish.

An uncertain setup offers **Retry identity setup**, which reuses its exact
request and idempotency key. If recovery reports an incomplete reservation,
**Retry identity cleanup** uses that original setup key to retry both deletions.
Refresh or disconnect clears an unsubmitted confirmation and rejects stale
responses; submitted retry state stays in the Settings connection view. After an
app restart, Settings reloads the current public identity ID from daemon status.

**Retire managed identity…** shows the selected server name and asks for a second
confirmation before marking that exact identity retired and deleting both vault
keys. It requires no setup key, so the current identity can still be retired after
reopening Settings or restarting the app. An uncertain result offers **Retry
identity retirement** for the same stable ID; the server keeps the tombstone and
retries both deletions. A replacement identity has a different ID and cannot be
retired by an old retry. Status exposes only the current master's identity and
does not list historical IDs. Identity metadata describes setup state; it does
not confirm that a key is currently available or that remote serving is active.

Designation records the role only. On a master, **Import bundle…** opens a single
explicit JSON bundle file. Settings previews the model-default presence and
MCP/skill/file counts, identifies the target instance and group, and requires
**Publish replacement** before sending it. This replaces all shared categories;
entries absent from the bundle are removed. **Discard import** cancels locally.
The daemon performs the full content/hash validation before storage. Importing
does not inspect local skill directories or extract files from packages.

Import reads at most 2 MiB, rejects symlinks and non-regular files on Unix, and
checks the serialized publication size including preconditions. Refresh or
disconnect clears an unsubmitted preview. Unconfirmed submissions retain the exact
payload/key within the connection view for **Retry publication**; confirmed
rejection requires a fresh status read and import. Success reports the recorded
revision, then reloads current status. Replacing the daemon connection discards
pending UI state and loads durable status, without automatically resubmitting.

**Export stored bundle…** is available when a stored revision exists. Choose a
new filename; existing files are never overwritten. The client fetches verified
content under the API's transfer limit and checks that the instance/group/state
still match the displayed selection before saving. The file contains the portable
bundle only, suitable for a later explicit import; authority IDs and API receipts
are not part of the file. Export writes a temporary file in the chosen folder,
finishes its contents, and persists it without replacing an existing destination.
A save already started may finish after the view disconnects or closes. On Unix,
the temporary/exported file is private to the user.

For a follower, Settings also shows the protocol `0.39.0` automatic-refresh state,
last outcome, freshness recency, time since the last successful check, and last
successful revision. These values are separate from the stored and highest
observed revisions. Disabled automatic refresh does not disable manual fetch.

**Follower snapshot fetch** lists local approval records for this follower in
bounded pages; **Load more attempts** fetches the next page. Selecting a record
does not contact the master, and an approved exchange result is not proof that
the credential or master grant is still active. The master validates the exact
selected attempt during fetch. Enter an HTTPS origin whose host matches that
record's pinned server name, plus positive connection and request timeouts in
milliseconds. The origin and deadlines have no defaults, and the request timeout
must be at least the connection timeout. **Review snapshot fetch…** shows the
target and requires a second action before the daemon fetches and atomically
applies the complete snapshot. Settings warns that absent settings, model
defaults, global MCP servers, skills, and files will be removed.
Disconnecting clears the UI task and its confirmation; work already accepted
by the daemon may still complete.

The Rust client also supports [publication](rust-client.md#configuration-snapshot-publication),
[export](rust-client.md#configuration-snapshot-export), and
[follower snapshot fetch](../spec/configuration-sync.md#explicit-follower-snapshot-fetch-and-application).
Provider credentials remain local to each instance.

## Follower exchange and retirement

Settings → Follower enrollment lists attempts on the connected daemon in bounded
pages, including when the instance is still unassigned. Select an attempt to
reload its durable state, pinned server name/CA fingerprint, authority and latest
receipt.

On an unassigned instance, **Prepare a new follower** accepts the master’s group
ID, instance ID and exact server name. **Choose public CA certificate…** reads an
explicitly selected regular DER file off the UI thread, rejecting links/special
files on Unix and limiting both file bytes and the encoded request to the existing
2 MiB budget. Use **Export public CA certificate…** on the master and obtain that
public file through a trusted channel; do not select a private key.

Compare the preview’s full SHA-256 fingerprint, authority and server name against
the master before **Fingerprint verified — prepare**. The frozen request includes
the local unassigned instance/version and a new stable attempt ID. Preparation
stores the pin and reserves a credential in the daemon vault but does not contact
the master or change the local role. The pinned transport checks certificate
usability at exchange; preparation alone is not proof of TLS readiness.

An uncertain response offers **Retry exact preparation**, retaining the original
attempt, version and certificate bytes across reconnects within this Settings
connection. **Discard local retry (keeps daemon attempt)** clears only that local
retry payload. Refresh enrollments and recover/retire any live attempt before
starting again; the daemon prevents concurrent live preparations. After closing
the app, recover the attempt through the enrollment list; a reserved/incomplete
attempt can be retired to retry vault cleanup. A canceled file picker or preview
makes no daemon mutation, and stale picker/read replies after disconnect are ignored.

For a prepared attempt, enter the master HTTPS origin and explicit positive
connect and whole-request deadlines in milliseconds. Connect must not exceed the
whole-request deadline. **Review submission / approval check…** freezes those
settings and the attempt. Confirmation contacts only the saved pinned master
through the daemon. A pending receipt leaves the instance unassigned; an approved
active receipt may atomically join it as a follower. Exchange does not fetch a
snapshot. Reload the attempt to see its receipt and **Copy request fingerprint**;
compare that fingerprint on the master before approving, then exchange again to
observe approval. Refresh the configuration status section after a role change.

**Retire enrollment…** permanently retires the attempt and deletes its local vault
credential after confirmation. This does not leave an existing follower role,
remove a stored snapshot, or revoke the master’s grant. **Retry credential
cleanup…** remains available on retired attempts for uncertain or failed vault
deletion. After any uncertain operation, reload the same attempt before retrying.
Disconnect discards unsubmitted confirmation and ignores stale replies, but an
accepted daemon operation may still finish. Receipt status is historical; it does
not prove current connectivity or that a grant has not subsequently been revoked.

## Master enrollment decisions

Settings → Follower enrollment requests provides **Refresh requests** and
**Next page** for pending and historical master-side requests. Select a row to
reload its immutable metadata and the current local authority. A pending request
can be approved or rejected only when it belongs to the current master and group.
The daemon independently checks the exact authority version, all confirmation
fields, and (for approval) the current managed identity.

Before choosing **Approve follower…**, compare the complete **Request fingerprint**
with the intended follower over a trusted channel. **Copy request fingerprint**
copies that public fingerprint; no bearer or credential digest reaches the UI.
The follower ID is only a claim. The confirmation displays the request and attempt
IDs, follower and master identities/versions, authority group, server name, CA
fingerprint, and request fingerprint. **Fingerprint verified — approve** grants
read access to that authority’s shared configuration. **Reject request…** requires
separate confirmation and permanently rejects the exact request.

For an approved request with an unrevoked grant, **Revoke follower access…**
requires confirmation of permanent revocation. The request must belong to the
current master and group; the daemon checks the displayed authority state version.
Revocation denies future snapshot reads using that grant. It does not erase data
already fetched by the follower or remove its locally stored credential.

After a decision or uncertain response, select the request again to load its
current state. An approved record with a revoked grant is labeled as revoked;
approval cannot revive it. Disconnect invalidates selection and confirmation,
although already accepted daemon work may still finish. The next decision requires
a fresh read. This section does not start the TLS listener or prepare a follower.

## Model account bindings

Settings → Model account bindings maps a portable shared
`model_defaults.account_binding` key to a provider account on the connected
daemon’s host. Enter the exact key from shared settings and choose **Load
binding**, or choose **Browse bindings** and select a key. **Next page** replaces
the current page; **Browse bindings** returns to the first page.

The editor shows the loaded key, version, current account, provider and account
state. Choose a connected account of the provider type required by the shared
model defaults, then confirm the displayed key and account ID. The server checks
the exact binding version and selected account’s provider type. **Remove mapping**
also requires confirmation. Missing or incompatible mappings prevent new managed
Runs from starting; existing Runs retain their saved selection. Account credentials
and Workspace access remain local, and connection state does not establish access
to a particular Workspace or model.

Every write consumes the loaded version. After success, failure, conflict, or
reconnect, load the key again before editing; the client never blindly retries a
write. A disconnected request may still finish in the daemon. Stale responses
cannot restore editing state after reconnect. Sign in to an account below and
reload the binding if no connected account is available.

## Provider accounts

After connecting to the daemon, open **Settings** in the global toolbar. Provider
accounts are independent of the selected Workspace or Session. **Refresh accounts**
loads the daemon's saved account summaries; **Sign in with browser** reuses a connecting
or disconnected subscription account, or creates one through the idempotent account
API when neither is available. Retrying after cancellation keeps the account ID.

Choose **Open browser sign-in** to finish PKCE authentication on the same host as
the daemon, then **Check sign-in**. Kiln prepares a short-lived localhost callback
on port 1455 or 1457; it does not stop another application using either port.
If both are unavailable, use **Use device sign-in**. Remote daemon users should
also choose device sign-in because localhost in the browser must reach the daemon.
Cancel a pending browser attempt before choosing the device alternative.

For device sign-in, use **Open verification page** and **Copy code**, then
**Check sign-in**. Status checks are user-initiated; the daemon owns callback
handling/device polling and credential storage. **Cancel sign-in** addresses only the
displayed attempt. Expired/replaced attempts require refreshing the account list
before starting again. A cleanup-required result is shown explicitly and blocks
another attempt in that view. Choose **Disconnect** on the account to retry local
cleanup. The daemon rejects replacement while a retained cleanup failure remains.

Failed attempts distinguish declined sign-in, expiry, unavailable sign-in service,
unavailable credential vault and unavailable account storage. Settings shows the
corresponding recovery action as an accessible alert. Unknown failures ask you to
refresh the account state before retrying. These categories contain no provider
response text, callback codes or vault references. Protocol `0.47.0` adds the
optional `failure` category to login status; pending, connected, cancelled and
cleanup-required attempts omit it.

**Disconnect** asks for confirmation, cancels active sign-in, and removes credentials
from this Kiln instance. It does not revoke access at OpenAI. After success, the
account list refreshes and **Sign in with browser** can reuse the same account. For
expired sign-in or an incomplete disconnect, disconnect first, then sign in again.
If deletion fails, the account is disabled and its saved reference remains available
for retry, including after restart. New connection/rotation writes also journal
opaque cleanup references, allowing Disconnect to recover unpublished or retired
entries after restart. The migration cannot recover entries orphaned by earlier
versions. Refresh after an error to
check the current account state; disconnect requests are never automatically retried.

Closing Settings does not cancel sign-in. Reconnecting the desktop discards its
connection-scoped attempt display; refresh the accounts to recover the durable
state. An unfinished connecting or disconnected account can start a replacement
attempt. If requesting a replacement fails, the prior terminal attempt remains
available by its original ID. Closing the desktop leaves daemon-owned work running.
Provider-side revocation and API-key entry are not available in the desktop. OpenAI API credentials can be imported locally as described below.
A connected account does not enable live model Runs.

Only public account metadata and short-lived login presentation enter the view.
Tokens, callback codes, PKCE verifiers, and vault references stay in the daemon.
The desktop validates the fixed device address or the browser issuer, path, client,
redirect, S256, state/challenge shape, and expected scope/parameters before opening
a link. It never opens a returned URL automatically or prints the browser query.
The protocol Debug representation redacts browser authorization URLs.

Compilation and source checks passed, and the global Settings layout/account-list
empty state was inspected in the native app against an isolated local daemon.
Browser preparation and cancellation were also inspected in the native app:
the pending layout displayed correctly, cancellation feedback appeared, and
sign-in controls became available again. Process inspection observed the loopback
listener during the attempt and its closure after cancellation. No browser
authorization page was opened. Live OAuth, credential-vault operations, other
terminal states, callback parsing, and minimum-window layout remain runtime-unverified.

The local disconnect continuation was also inspected against an isolated daemon
with an empty account: confirmation, Keep account, and successful disconnect with
account refresh were observed. Credential deletion, concurrent cancellation, and
failure/retry layouts were not exercised at runtime.

The EDL-341 continuation exercised the real daemon's local callback listener:
invalid state remained pending, a matching declined callback reported `declined`,
replacement rejected the stale attempt, cancellation remained distinct, and
restart preserved the account for disconnect and reauthentication. Focused
terminal-error fixtures checked expiry and vault/storage failure mapping.
Native failure layout and live OAuth/vault acceptance remain unverified; see
[the EDL-341 evidence and isolated scenario](../learning/edl-341.md).

Settings preserves its full content height when centered, allowing the provider
section to be reached by scrolling. Native sign-in preparation and access to
the pending controls were observed at 1060 by 832 pixels after this repair.
Cancellation removed the pending controls from accessibility state; terminal
feedback rendering and live sign-in/vault behavior still require acceptance.

## Import an OpenAI API key locally

OpenAI API billing and credentials are separate from Codex subscription access.
The renderer and public daemon API never receive an API key. To explicitly save
one in the OS vault, stop the daemon for that data directory and use the local
import command. In Nushell, a hidden prompt can feed it without putting the key
in command arguments or shell history:

```nu
input --suppress-output "OpenAI API key: " | ^target/debug/kilnd import-openai-api-key --stdin
```

Use the same `KILN_DATA_DIR` as the daemon if it was set. The command reads stdin
to EOF, permits one trailing newline, and refuses terminal input or an active
OpenAI API account. It never imports environment credentials implicitly. For
replacement, first disconnect the API account in Settings, stop the daemon, and
run the import again. Pending cleanup must also be resolved before import.

Restart the daemon and refresh Settings after success. The API account appears as
**Credentials saved — provider access not verified**. Saving does not contact
OpenAI or validate authentication, billing, models, or entitlement. Live API model
execution remains pending. The command's help is available with
`kilnd import-openai-api-key --help`; it does not open the vault or account store.

## Start or resume a session

For a new session, set `KILN_DESKTOP_REPOSITORY` to an absolute Git repository
path and leave `KILN_DESKTOP_SESSION` empty. Connect creates a Workspace with
that repository as its first root, then creates a Session.

For an existing session, set its ID and omit the repository if it is not
needed:

```nu
$env.KILN_DESKTOP_ADDRESS = "127.0.0.1:49152"
$env.KILN_DESKTOP_TOKEN_FILE = "/path/from/credential_path"
$env.KILN_DESKTOP_SESSION = "ses_01..."
cargo run -p kiln-desktop --bin kiln-desktop
```

The desktop retrieves the Session and its Workspace, restores the Session's Run
tree, loads stored Session events from the start, and then subscribes after the
returned event cursor. The connection view shows the resulting Session ID.

## Composer attachments

Use **Attach**, file drag and drop, or image/file paste to add attachments. Each
file is limited to 64 MiB. The desktop accepts regular files and rejects symlinks
and special files when reading an upload. It rechecks the file at send time, so a
changed source can fail even after selection.

Pasting copied files or image content into the composer adds attachments without
inserting a filename as message text. Plain-text paste uses normal text editing
and undo.

Attachment labels and controls wrap as the composer narrows, and the message
field retains a minimum height. Hover over a shortened scope label to read its
full path; the accessibility label also retains the full scope.

Attachments stay in the draft for their Session. A picker result is discarded
if its Session or daemon connection changes before the dialog finishes. **Retry**
retains successful uploads after a later file fails; **Remove** removes a file
from an unsent request. Attachment edits are disabled while a request is in
flight or its message is already appended or has an uncertain outcome. Reconnect
and review the refreshed history, then use **Retry** to recover an
uncertain append. This reuses the original content, attachments and idempotency
key; it does not append a second Message if the first attempt was saved. Draft
edits stay locked until the append is confirmed. Reconnect never retries a
submission automatically. The accessible action name is **Retry same submission**;
feedback and recovery controls wrap in constrained views.

Attachment-only messages are supported. Blank messages without attachments are
rejected. [EDL-308 evidence](../learning/edl-308.md) distinguishes public-protocol
checks from observed native behavior and remaining acceptance.

## Browse saved Workspaces and Sessions

After the daemon connection succeeds, the Workspace navigator lists saved
Workspaces. Search matches the Workspace name, ID, and repository root. Select a
Workspace to load its Sessions in the Session drawer. Session search matches the
Session ID. The drawer shows loading, empty, no-match, and query-error states.

Select **New** in the Session drawer to create and open a Session in the
selected Workspace. Select **Add** in the Workspace navigator to open the
connection form and create a Workspace from the repository field. **Browse**
and **Hide browser** toggle both panels while leaving the current Session open.

Selecting a saved Session stops the previous event subscription, restores the
selected Session's Run snapshot and event history, and starts a new subscription
from that snapshot's cursor. Responses and events from an older Workspace or
Session request are ignored. The active Session remains bound to its Workspace
root for the composer and Run controls.

The desktop keeps unsent composer text, child guidance drafts, reactions, and
retryable commands under their original Session ID while browsing. Returning to
that Session restores them. A draft is never sent to a different Session, and
the composer is disabled while no Session is selected or while a Session is
opening. Escape clears the focused browser search first, then closes the Run
drawer or child focus and returns focus to the root composer.

## Conversation controls

Enter sends the composer text. Shift+Enter adds a newline. Submit starts a Run
when no Run is active. During an active Run, the same action queues input for
that Run. Stop requests cancellation of the active Run.

Runs use the `ask` approval policy. A pending tool approval shows its repository
scope and relative directory. It also loads the immutable native tool name,
revision, capability, and complete model-supplied arguments as plain text.
Deterministic calls explicitly report that they have no model-supplied arguments.
Approve or Reject sends that decision to the daemon. The composer is disabled while a command is in progress, while the
client is disconnected, while a submission is pending, and while cancellation
is pending. Both approval buttons are disabled while a command is in progress or the client
is disconnected. Approve also remains disabled until request inspection succeeds;
Reject remains available if inspection fails. Retry inspection fetches the details
again. Inspection reuses the 64 KiB text-preview allowance for the complete frozen
request batch and catalogue, plus capability text, needed to verify stored hashes.
An oversized source fails without displaying or approving partial arguments, even
when the selected request alone would fit.

Runs opens a temporary drawer in the conversation area. Close Runs restores the
transcript. The root status and main composer remain visible while the drawer
is open. Child rows follow parent order and show hierarchy level, input mode,
current state, and linked Task counts. Expanded details show the parent Run ID
and each linked Task's state. Task progress uses text as well as color.

Select a child row to open its details. View child activity shows only that
child's transcript and approvals. Back to conversation restores the full
transcript. Escape closes the Run drawer or child focus and focuses the root
composer. The main composer always targets the root, including in child focus.
Messages and tool output show their owning Run. Guidance shows its durable
queued, delivered, failed, or cancelled state.

Discuss latest update with root selects a child transcript entry as a durable
reference. It returns to the conversation and focuses the root composer. It
does not send a message. The composer shows the source child Run and Event IDs;
Clear reference removes the selection without removing the draft text. For
merged streaming output, the reference selects the latest update, not all
output in the row. A completed assistant Message selects its final Event.

Enter sends the reaction to the original root with the selected reference.
It queues while root work is active. The next root context that includes this
delivered reaction also includes a stored projection of the selected Event.
The projection does not copy the full child transcript or artifact bytes. The
replayed reaction shows its source Run and Event. A failed submission keeps its
original root, text, reference, and idempotency key for Retry; it never silently
targets a new root. Reconnect to the same Session and daemon store retains the
pending submission and reference.

New child starts an unlinked interactive child under the active root. It
inherits the root's requested repository scope and uses `ask` approval policy.
If that scope is unavailable, reconnect before creating a child. Stop child
requests cancellation for the selected child only, not the root or a sibling.
A failed child start retains its idempotency key for retry.

Child guidance has a separate draft for each Run. Queue guidance requests
delivery at the next safe boundary. Interrupt child explicitly requests an
interruption before delivery. Neither action sends through the main composer.
Read-only and terminal children cannot receive new guidance. A failed guidance
request offers Retry guidance with the original target, text, delivery mode,
and idempotency key. Its error does not block the root composer. Drafts are held
in memory; they are not saved when the desktop exits.

The deterministic subprocess executor does not consume queued guidance before
successful completion. Such guidance can remain marked Queued. The desktop
shows the daemon's durable state; it does not infer delivery from completion.

## Failures and reconnects

The desktop does not retry commands automatically. A failed message submission
or Run start that is safe to retry keeps its pending submission and original
idempotency key, and shows Retry. Failed cancellation and approval commands do
not show Retry. If appending a new user message ends with a transport or decode
error, its result is uncertain: the daemon might have stored it. Retry stays
disabled until Reconnect successfully reloads the history. Review that history,
then use **Retry** to recover the same submission with its original payload and
idempotency key. Draft edits remain locked until append confirmation. Another
uncertain append requires another history refresh before Retry becomes available.

If the event stream closes or reports an error, the daemon and Run continue.
Use Reconnect. Reconnection reloads the Session history and opens a new event
subscription. Pending submissions, attachment drafts, child-activity references,
child starts and guidance remain in memory under their original Session ID when
reconnecting to the same daemon store. Switching Sessions restores only that
Session's saved draft and retry state. Switching to a different daemon store
clears this state. Reconnect never submits or retries a command automatically.

## Current boundaries

Child reactions use durable Events for the displayed child Messages, output,
and artifact metadata. Selecting an artifact opens a temporary inspector
that fetches its immutable bytes through the existing authenticated artifact
route. The inspector previews UTF-8 text, JSON, and XML responses up to the
first 64 KiB; binary and other media types remain download-only. Fetch
failures show an inline retry action. The Changes inspector loads the
read-only summary for the active Session's captured checkout. It lists tracked
and untracked files in path order, shows each file kind, and displays line
counts only when Git reports meaningful values. A Session created before
checkout persistence may remain readable while its change summary reports an
unavailable checkout. The daemon revalidates the captured root, filesystem
identity, and Git common directory before reading status; Git output is capped
at 4 MiB per Git invocation and each Git invocation is capped at 30 seconds;
the operation does not claim worktree isolation. Select a changed file to load
its authenticated read-only diff. Text previews are capped at 4 MiB per input
file and 256 KiB of rendered patch, with an explicit truncation notice. Added,
deleted, and modified text files show the patch; untracked, binary, conflicted,
renamed, unsupported file types, and unsupported encodings show an explicit
unavailable state. Refreshing the summary or changing Session invalidates an
open diff.
Reactions require the selected child's original root to accept input. The
current executor is not yet a persistent interactive supervisor.

The desktop provides a global Usage ledger for the daemon's latest validated
usage revisions. The ledger uses cursors for pagination, keeps quantities separate,
and exposes observation metadata without calculating totals or valuation.
The desktop does not yet provide the secondary Run graph. The Run drawer
supervises the current root's descendants. EDL-249 is
complete; remaining work is tracked by the current Linear progression.

## MCP form requests

When an explicitly enabled MCP server asks for a form, the conversation shows a
private **Server requests information** card. It identifies the requesting server
from the frozen ToolCall source and shows the source Run. Read-only child requests
route to their live interactive root; interactive children own their requests.
Forms disappear when the input resolves, is interrupted, or its ancestry stops
accepting interaction. Replay reconstructs pending identities, not form contents.

The card supports text, integer/number, boolean, labelled or unlabelled single
choices and multiple choices. Fields have visible labels and constraint hints.
Optional fields are omitted unless explicitly included; server defaults are never
filled. Empty strings remain strings, and whole numbers retain their exact JSON
integer representation. Local schema validation checks required fields, formats
and constraints without fetching external schemas. The daemon remains authoritative
and revalidates every decision.

**Send response**, **Decline** and **Cancel request** submit distinct MCP decisions;
Cancel request does not cancel the entire Run. Form content is rendered as plain
text, never executable markup. It is not appended to the conversation transcript
or public Events. Successful submission disables the card until the input-state
Event arrives. An uncertain result offers **Retry same decision**, preserving the
exact response and locking edits; a definitive invalid response permits editing,
while stale/conflicting requests stop offering decisions.

Disconnecting, changing daemon/Session, or invalidating the input drops private
drafts and disables old views. Already accepted writes may still complete; the
client never automatically resubmits after reconnect. Form inspection uses the
existing desktop 64 KiB frozen-source preview allowance to identify the server;
if that source cannot be inspected completely, sending a response remains disabled.

The daemon must enable form mediation and per-call input quotas as described in
[MCP runtime](mcp-runtime.md). URL-mode elicitation and sampling have no UI yet.
Native form layout, keyboard/screen-reader behavior and live reconnect interaction
remain unverified; compilation and focused model tests alone do not establish them.
