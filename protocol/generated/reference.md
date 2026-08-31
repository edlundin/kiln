# EDL-211 protocol reference

Protocol version: `0.4.0`.

The client sends `POST /v1/protocol/negotiate` with a version range, client identity, and requested capabilities. The server returns the selected version, capability lists, store identity, current event cursor, and the event WebSocket endpoint.

The client creates a durable Workspace with `POST /v1/workspaces`, providing a name and one or more named local Git repository roots. `GET /v1/workspaces/{workspace_id}` returns the stored snapshot.

`POST /v1/workspaces/{workspace_id}/sessions` creates a Session attached to one Workspace. `GET /v1/sessions/{session_id}` returns it. A Session does not own or depend on a worktree.

`POST /v1/sessions/{session_id}/messages` accepts immutable user message content. Kiln creates the Message and its `message.appended` Event atomically. Clients cannot append arbitrary Events.

`POST /v1/sessions/{session_id}/runs` accepts one queued root Run for the Session. The deterministic adapter selects the fixed subprocess; clients cannot provide an executable, arguments, shell, environment, or working directory. `GET /v1/runs/{run_id}` returns the durable Run and ToolCall result.

`GET /v1/sessions/{session_id}/events?after=0` returns that Session's committed Events after the opaque decimal cursor. Event IDs are stable identities. The daemon-wide cursor orders committed audit records; it does not schedule work between Sessions. The response current cursor and Event rows come from one storage snapshot.

The client can connect to `GET /v1/events?version=0.4.0&capability=kiln.events.websocket`. After the acknowledgement, the server publishes newly committed Run and ToolCall Events in cursor order. Reconnect replay is outside this release.

HTTP errors use the protocol-owned `ProblemDetails` shape. Clients make decisions from the stable `code` field. `catalogue.json` lists the error codes implemented by this release.
