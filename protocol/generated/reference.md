# Kiln protocol reference

Protocol version: `0.19.0`.

All loopback HTTP requests require the persistent local credential as `Authorization: Bearer <token>`. The server also validates the exact bound `Host` and, when present, the loopback `Origin`.

Message responses distinguish `user` and `assistant` roles and `complete` or `incomplete` status. Assistant Messages identify their originating Run and model invocation; user Messages have null origin fields. The user-message append endpoint cannot submit assistant Messages.

`model_invocation.output` carries one durable ordered assistant-text or provider-exposed reasoning-summary chunk. Its immutable chunk ID and per-attempt position support exact replay. It does not carry hidden provider reasoning. Chunks are distinct from finalized assistant Messages. With `KILN_RUN_EXECUTOR=deterministic-model`, the daemon runs the explicit native fixture and publishes committed output and assistant Message Events. The default executor remains `deterministic-subprocess`.

`usage.observed` identifies one immutable usage observation revision for a physical model invocation attempt. It carries the logical work and account identity, revision link, completeness, and terminal status. It contains no quantities, prices, prompt text, output text, or raw provider payloads. Usage query endpoints and valuation are not yet implemented.

The client sends `POST /v1/protocol/negotiate` with a version range, client identity, and requested capabilities. The server returns the selected version, capability lists, store identity, current event cursor, and the event WebSocket endpoint.

The client creates a durable Workspace with `POST /v1/workspaces`, providing a name and one or more named local Git repository roots. `GET /v1/workspaces/{workspace_id}` returns the stored snapshot.

`POST /v1/workspaces/{workspace_id}/sessions` creates a Session attached to one Workspace. `GET /v1/sessions/{session_id}` returns it. A Session does not own or depend on a worktree.

`POST /v1/sessions/{session_id}/messages` accepts immutable user message content. Kiln creates the untargeted Message and its `message.appended` Event atomically. Clients cannot append arbitrary Events.

`POST /v1/sessions/{session_id}/tasks` requires a non-empty opaque `Idempotency-Key` header. It creates one durable pending Task and one `task.created` Event atomically. Parent and dependency links must target Tasks in the same Session. `PATCH /v1/tasks/{task_id}` replaces mutable objective and dependency data and records `task.updated`. `POST /v1/tasks/{task_id}/assignment` assigns one active Run in the same Session and records `task.assigned`. `POST /v1/tasks/{task_id}/transition` applies one guarded lifecycle transition and records `task.state_changed`. All Task commands are idempotent. A repeated key with the same normalized request returns the current Task without another Event. A mismatched reuse is an idempotency conflict. `GET /v1/tasks/{task_id}` returns the durable Task snapshot.

`POST /v1/sessions/{session_id}/runs` requires a non-empty opaque `Idempotency-Key` header and creates an interactive root Run with durable `run.created` and `run.queued` Events. `POST /v1/runs/{parent_run_id}/children` creates one child in the same Session with immutable `parent_run_id`, optional Task assignment, and `interactive` or `read_only` user input mode. It atomically records `run.created`, `run.queued`, `run.child_added`, and, when linked, `task.assigned`. Child keys are scoped to the parent Run. Exact retries return the existing Run without new Events; mismatched key reuse conflicts. `GET /v1/sessions/{session_id}/runs` returns a flat ordered list whose parent IDs form the Run tree.

`POST /v1/runs/{run_id}/input` requires `Idempotency-Key` and creates one user Message targeted to an interactive input-accepting Run plus one queued MessageDelivery. The Message and ordered `message.appended` then `run.input_queued` or `run.interrupt_requested` Events commit atomically. `queued` is normal guidance for a future safe boundary; `interrupt` must be explicit. Exact retries return the original delivery even after the Run becomes terminal. Delivery recording is an internal runtime boundary: successful and failed outcomes are FIFO per Run, while cancellation is valid only after Run termination. This release does not claim provider delivery or scheduler integration.

`ask` records a pending Approval before execution. `read_only` durably denies the subprocess. `full_access` makes the requested scope effective without a prompt. `POST /v1/tool-calls/{tool_call_id}/approval` approves or rejects one pending ToolCall. Approval decisions are durable, first-decision-wins, and idempotent by their own `Idempotency-Key`. An approval after daemon restart resumes the same Run.

Tool output larger than 4,096 bytes is stored as one immutable artifact instead of inline Event content. The `artifact.registered` Event and terminal ToolCall include the content hash, media type, and decimal byte size. `GET /v1/artifacts/{content_hash}` returns the verified bytes with safe download headers.

`POST /v1/runs/{run_id}/cancel` is naturally idempotent for the addressed Run and its descendants. It preserves the addressed Run's parent and siblings, records durable cancellation intent across the selected subtree before signalling owned processes, and returns only after descendant Runs and ToolCalls are terminal. Cancelling while approval is pending rejects that Approval and denies the ToolCall without starting a process. Each Run cancellation terminal commit atomically marks its queued MessageDeliveries cancelled and records `run.input_cancelled`; exact input retries return that cancelled delivery. A completion committed before the cancellation request remains authoritative.

SIGINT and SIGTERM start graceful shutdown. Kiln rejects later mutating commands, lets already accepted commands commit, cancels active Runs, and closes only after their terminal state is durable. Read-only requests remain available while the daemon drains.

`GET /v1/sessions/{session_id}/events?after=0` returns that Session's committed Events after the opaque decimal cursor. Event IDs are stable identities. The daemon-wide cursor orders committed audit records; it is not required to be numerically contiguous. The response current cursor and Event rows come from one storage snapshot.

The client connects to `GET /v1/events?version=0.19.0&capability=kiln.events.websocket&after={cursor}` and offers `kiln.events.websocket` plus `kiln.auth.<token>` as WebSocket subprotocols. The server echoes only `kiln.events.websocket`. With `after`, the server acknowledges and replays the exact durable global suffix through one snapshot boundary, then queries durable events after each wake-up. Without `after`, the server preserves live-only delivery from the connection snapshot.

HTTP errors use the protocol-owned `ProblemDetails` shape. Clients make decisions from the stable `code` field. `catalogue.json` lists the error codes implemented by this release.
