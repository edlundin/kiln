# EDL-212 protocol reference

Protocol version: `0.5.0`.

The client sends `POST /v1/protocol/negotiate` with a version range, client identity, and requested capabilities. The server returns the selected version, capability lists, store identity, current event cursor, and the event WebSocket endpoint.

The client creates a durable Workspace with `POST /v1/workspaces`, providing a name and one or more named local Git repository roots. `GET /v1/workspaces/{workspace_id}` returns the stored snapshot.

`POST /v1/workspaces/{workspace_id}/sessions` creates a Session attached to one Workspace. `GET /v1/sessions/{session_id}` returns it. A Session does not own or depend on a worktree.

`POST /v1/sessions/{session_id}/messages` accepts immutable user message content. Kiln creates the Message and its `message.appended` Event atomically. Clients cannot append arbitrary Events.

`POST /v1/sessions/{session_id}/runs` requires a non-empty opaque `Idempotency-Key` header. The key is scoped to the start-run operation and Session. A repeated key returns the original Run snapshot, including after terminal completion, and does not dispatch another subprocess. A different key creates a new root Run when no root Run is active.

`GET /v1/sessions/{session_id}/events?after=0` returns that Session's committed Events after the opaque decimal cursor. Event IDs are stable identities. The daemon-wide cursor orders committed audit records; it is not required to be numerically contiguous. The response current cursor and Event rows come from one storage snapshot.

The client can connect to `GET /v1/events?version=0.5.0&capability=kiln.events.websocket&after={cursor}`. With `after`, the server acknowledges and replays the exact durable global suffix through one snapshot boundary, then queries durable events after each wake-up. Without `after`, the server preserves the existing live-only delivery from the connection snapshot.

HTTP errors use the protocol-owned `ProblemDetails` shape. Clients make decisions from the stable `code` field. `catalogue.json` lists the error codes implemented by this release.
