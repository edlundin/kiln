# Desktop client

Kiln Desktop is a native GPUI client for the local `kilnd` daemon. The daemon
is a separate process and continues to run if the window closes. The current
desktop path uses the deterministic model fixture. It does not connect to a
live model provider.

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
{"address":"127.0.0.1:49152","credential_path":"/path/to/credential","event":"ready","protocol_version":"0.20.0"}
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
scope and relative directory. Approve or Reject sends that decision to the
daemon. The composer is disabled while a command is in progress, while the
client is disconnected, while a submission is pending, and while cancellation
is pending. Approval buttons are disabled only while a command is in progress
or the client is disconnected.

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
targets a new root. Reconnect clears the pending root submission and selection.

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
error, its result is uncertain: the daemon might have stored it. Retry is
disabled in this case. Reconnect and review the restored history before you
send another message.

If the event stream closes or reports an error, the daemon and Run continue.
Use Reconnect. Reconnection reloads the Session history, clears any pending
root submission and child-activity reference, and opens a new event subscription.
It retains a pending child
start only for the same Session and daemon store identity. Child guidance
drafts and retry requests remain in memory when reconnecting to that same
Session and store. Switching Sessions keeps them under their original Session
ID while the daemon store is unchanged. Switching to a different daemon store
clears them.

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
the operation does not claim worktree isolation.
Reactions require the selected child's original root to accept input. The
current executor is not yet a persistent interactive supervisor.

The desktop provides a global Usage ledger for the daemon's latest validated
usage revisions. The ledger uses cursors for pagination, keeps quantities separate,
and exposes observation metadata without calculating totals or valuation.
The desktop does not yet provide the secondary Run graph. The Run drawer
supervises the current root's descendants. EDL-249 is
complete; remaining work is tracked by the current Linear progression.
