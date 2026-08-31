# Kiln system and trust perimeter

Status: draft implementation baseline

This document defines what Kiln trusts, what it does not trust, and which
component may cross each boundary. It applies to the first local product
release on macOS and Linux.

## 1. System boundary

Kiln consists of a local daemon, protocol clients, owned persistence, and
explicit adapters to external systems. Running on the same machine does not
make a caller, process, path, server, or byte stream trusted.

```text
Web / desktop / CLI
        │ authenticated HTTP and WebSocket
        ▼
Server ── core policy and commands ── durable state and events
        │
        ├── provider network boundary
        ├── capability-scoped tool and process boundary
        ├── plugin JSON-RPC process boundary
        ├── MCP transport boundary
        ├── filesystem, Git, artifact, and PTY boundary
        └── committed events back to clients
```

## 2. Trust zones

| Zone | Trust | May own | Must not own |
| --- | --- | --- | --- |
| Core | trusted policy code | domain state, transitions, capability decisions | secrets, raw paths, SDK values, process handles |
| Server | trusted transport adapter | authentication, request limits, protocol conversion | domain decisions, direct SQL |
| Persistence adapters | trusted implementation with untrusted input | SQL and artifact storage details | permission policy |
| Web, desktop, CLI | authenticated but not authoritative | user intent and local view state | run state, grants, filesystem access |
| Provider | external and untrusted | model response within its contract | permissions, direct workspace access |
| Plugin | external and untrusted | declared JSON-RPC methods | self-grants, database access, event injection |
| MCP server | external and untrusted | discovered tools and results | self-grants, direct UI or database state |
| Tool process and PTY | external and high impact | one scoped operation | wider roots, daemon secrets, policy changes |
| Workspace content | untrusted data | source files and repository metadata | executable authority by presence alone |
| Artifact bytes | untrusted data | immutable uploaded or generated content | trusted paths or executable authority |

## 3. Local client boundary

The first release MUST bind to loopback only. Binding to a non-loopback address
is a separate remote feature and is forbidden until remote authentication,
enrollment, ownership, audit, and recovery contracts are accepted.

Loopback is not authentication. Before privileged routes are implemented, Kiln
must select and document one local client credential mechanism that works for:

- normal HTTP requests;
- browser WebSocket connections;
- the desktop shell; and
- the CLI.

Until that mechanism is selected, local authentication is a blocking Phase 1
decision. The final mechanism MUST provide these outcomes:

- an untrusted local process cannot issue commands only because it knows a port;
- credentials are generated outside repository content;
- credentials are not placed in URLs, logs, events, shell history, or Git;
- WebSocket authorization has the same effective identity as HTTP;
- cross-origin requests are denied by default;
- the server validates the expected host and origin; and
- shutdown, tool, terminal, permission, plugin, and MCP routes require explicit
  authorization.

Health data exposed without authentication must not contain projects, paths,
versions that increase exploitability, or operational details.

## 4. Workspace-root boundary

Every file, Git, process, and terminal operation MUST name one registered
workspace-root ID and a path relative to that root. A client, provider, plugin,
or MCP server cannot submit an unrestricted absolute path as authority.

The filesystem adapter MUST:

1. resolve and canonicalize a root when it is registered;
2. reject a root the daemon identity cannot inspect safely;
3. normalize the requested relative path;
4. reject absolute paths and parent traversal;
5. evaluate symlinks and mount crossings against explicit policy;
6. confirm containment again when the operation starts; and
7. return a safe domain error without exposing unrelated paths.

A filename attached in the composer is display metadata only. Repository files
can contain hostile instructions, malformed encodings, large data, symlinks,
special files, and executable content. Reading a file never grants permission to
execute it.

The Git isolation model for concurrent agents is open. No implementation may
claim worktree isolation until branch ownership, uncommitted changes, nested
repositories, submodules, cleanup, merge, and recovery behavior are specified.

## 5. Capability and permission boundary

Every high-impact operation has:

- one capability name;
- one requested scope;
- one effective scope;
- one caller identity;
- one owning run or user command; and
- one durable policy decision.

An effective scope MUST NOT be wider than the request, registered roots, active
grants, and user approval. The narrowest value wins.

Capabilities must distinguish at least:

- file read and file write;
- Git read and Git mutation;
- process execution;
- terminal creation and input;
- provider network access;
- general network access when later supported;
- plugin calls; and
- MCP tool calls.

A manifest, discovered MCP tool, provider response, model output, repository
file, or client animation is never a grant.

## 6. Secret boundary

Secret values belong to a secure configuration adapter. Core records contain a
secret reference and safe metadata, never the secret value.

Secrets MUST NOT enter:

- domain events;
- messages or artifacts by default;
- provider prompts unless the user explicitly supplies the value as content;
- plugin or MCP initialization unless one explicit grant requires it;
- subprocess arguments when a safer input channel exists;
- logs, errors, metrics, crash reports, or UI state; or
- checked-in configuration.

Environment variables passed to a child process use an allowlist built for that
boundary. The daemon environment is not inherited wholesale.

## 7. Provider boundary

A provider adapter receives a normalized request with only the context, model
configuration, and tool descriptions required for one run. It does not receive
workspace access. File content enters a provider request only through the
context builder and accepted artifact or file-read operations.

Provider SDK types stop in `crates/providers`. The adapter converts streaming
updates, tool requests, usage data, errors, and completion into normalized core
commands.

The first direct provider and its data-retention policy remain open decisions.
The provider implementation cannot begin until the product states which content
leaves the machine and how the user can inspect that decision.

## 8. Plugin boundary

Plugins are external processes. They do not link into the daemon and cannot
access its database. Before start, the plugin host validates the manifest and
loads only approved grants.

- Plugin stdout is framed protocol data only.
- Diagnostics use stderr and pass secret redaction.
- The environment, current directory, and inherited handles are explicit.
- Each call repeats the capability and effective scope.
- Invalid frames, methods, results, or scope changes fail the call.
- A crash fails active calls and emits a durable event.
- A plugin does not restart silently.
- A plugin cannot publish a durable event directly.

Client-side plugin UI code is not loaded in the first release unless a separate
package trust, signature, origin, and capability contract is accepted.

## 9. MCP boundary

MCP servers are tool providers behind the Kiln permission engine. Discovery
describes available methods; it does not grant them.

Every MCP call uses one registered server, one tool identity, one capability,
and one effective scope. Inputs and outputs are untrusted. Large results become
artifacts before they enter durable events. Connection loss fails active calls
through normal tool-result commands. Reconnection is explicit until a measured
and user-visible retry policy exists.

## 10. Process and terminal boundary

Process execution and terminals are separate capabilities. Permission to run
one fixed tool command is not permission to create an interactive shell.

The process adapter MUST define:

- executable resolution;
- argument boundaries without shell interpolation by default;
- authorized working root;
- allowed environment variables;
- stdin ownership;
- stdout and stderr capture;
- child-process ownership;
- cancellation and exit reporting; and
- daemon-restart behavior.

Shell evaluation requires a specific operation and approval. User, model,
plugin, and MCP text MUST NOT be concatenated into a shell command.

A terminal session is high impact. Its lifecycle is durable, but its byte stream
is transient. Attach and detach do not change process ownership. The terminal
backend and platform PTY behavior remain a Phase 4 decision.

## 11. Artifact boundary

Artifact bytes are immutable and addressed by a verified content hash. Metadata
records media type, size, display name, origin, and storage reference. The
storage reference is adapter-private.

The artifact adapter MUST verify the hash while accepting bytes. A claimed media
type, filename, extension, or image preview does not establish trust. Downloads
use safe response headers. Browser rendering must prevent active content from
gaining the Kiln application origin.

No payload-size or retention limit is specified until representative files and
outputs are measured. The server still needs a tripwire before uploads are
enabled; its value and reason must be recorded with that measurement.

## 12. Event and log boundary

Durable events contain safe domain facts, stable identifiers, and artifact
references. They do not contain secrets, unrestricted process output, provider
SDK payloads, raw plugin frames, terminal byte streams, or unrelated absolute
paths.

Logs are operational data, not product state. A log message cannot complete a
run or grant a capability. Structured logging fields use stable safe IDs. Error
details from external boundaries are normalized and redacted before persistence
or client delivery.

## 13. Failure defaults

Kiln fails closed when it cannot prove identity, root containment, capability,
effective scope, grant validity, protocol shape, or execution ownership.

- A denied or malformed request starts no side effect.
- A lost connection does not imply success.
- A quiet event stream does not imply completion.
- A daemon restart does not silently repeat external work.
- An unknown external process is not adopted without proof of ownership.
- A plugin or MCP failure cannot widen access through a fallback.

## 14. Required conformance proof

The first release needs tests that show:

- unauthenticated privileged commands are rejected;
- cross-origin and unexpected-host requests are rejected;
- path traversal and symlink escape are rejected;
- a narrower effective scope is enforced at the adapter;
- secrets do not appear in events, errors, or logs;
- duplicate commands do not duplicate external work;
- plugin and MCP failures produce normal failed tool calls;
- cancellation reaps owned child processes;
- restart recovery does not replay completed work; and
- artifact active content cannot execute in the application origin.

## 15. Blocking decisions

| Decision | Blocks |
| --- | --- |
| Local HTTP and WebSocket credential mechanism | privileged server routes |
| Git isolation and recovery policy | concurrent repository mutation |
| First provider and retention policy | direct provider adapter |
| Terminal backend and PTY policy | terminal implementation |
| Client plugin package trust | plugin UI extensions |

Remote binding, multi-user ownership, account management, and a general sandbox
for arbitrary third-party code remain outside the first release.

