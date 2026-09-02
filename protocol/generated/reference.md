# EDL-214 protocol reference

Protocol version: `0.8.0`.

All loopback HTTP requests require the persistent local credential as `Authorization: Bearer <token>`. The server also validates the exact bound `Host` and, when present, the loopback `Origin`.

The client sends `POST /v1/protocol/negotiate` with a version range, client identity, and requested capabilities. The server returns the selected version, capability lists, store identity, current event cursor, and the event WebSocket endpoint.

The client creates a durable Workspace with `POST /v1/workspaces`, providing a name and one or more named local Git repository roots. `GET /v1/workspaces/{workspace_id}` returns the stored snapshot.

`POST /v1/workspaces/{workspace_id}/sessions` creates a Session attached to one Workspace. `GET /v1/sessions/{session_id}` returns it. A Session does not own or depend on a worktree.

`POST /v1/sessions/{session_id}/messages` accepts immutable user message content. Kiln creates the Message and its `message.appended` Event atomically. Clients cannot append arbitrary Events.

`POST /v1/sessions/{session_id}/runs` requires a non-empty opaque `Idempotency-Key` header and a `StartRunRequest` containing the approval policy, immutable Workspace root ID, and normalized relative directory. The key is scoped to the start-run operation and Session. A repeated key with the same request returns the current durable Run snapshot and never dispatches another subprocess. A mismatched reuse is an idempotency conflict.

`ask` records a pending Approval before execution. `read_only` durably denies the subprocess. `full_access` makes the requested scope effective without a prompt. `POST /v1/tool-calls/{tool_call_id}/approval` approves or rejects one pending ToolCall. Approval decisions are durable, first-decision-wins, and idempotent by their own `Idempotency-Key`. An approval after daemon restart resumes the same Run.

`POST /v1/runs/{run_id}/cancel` is naturally idempotent for the addressed Run. It records `run.cancellation_requested`, stops owned execution, waits for process exit, and returns the durable terminal Run. Cancelling while approval is pending rejects that Approval and denies the ToolCall without starting a process. A completion committed before the cancellation request remains authoritative.

SIGINT and SIGTERM start graceful shutdown. Kiln rejects later mutating commands, lets already accepted commands commit, cancels active Runs, and closes only after their terminal state is durable. Read-only requests remain available while the daemon drains.

`GET /v1/sessions/{session_id}/events?after=0` returns that Session's committed Events after the opaque decimal cursor. Event IDs are stable identities. The daemon-wide cursor orders committed audit records; it is not required to be numerically contiguous. The response current cursor and Event rows come from one storage snapshot.

The client connects to `GET /v1/events?version=0.8.0&capability=kiln.events.websocket&after={cursor}` and offers `kiln.events.websocket` plus `kiln.auth.<token>` as WebSocket subprotocols. The server echoes only `kiln.events.websocket`. With `after`, the server acknowledges and replays the exact durable global suffix through one snapshot boundary, then queries durable events after each wake-up. Without `after`, the server preserves live-only delivery from the connection snapshot.

HTTP errors use the protocol-owned `ProblemDetails` shape. Clients make decisions from the stable `code` field. `catalogue.json` lists the error codes implemented by this release.
