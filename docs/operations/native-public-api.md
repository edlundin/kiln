# Native public API mode

`kilnd` can run the native coordinator against the public OpenAI Responses API.
This is a separate `openai_api` account path; Codex subscription transport is not
implemented by this mode. The default executor remains deterministic subprocess,
and the desktop fixture instructions remain available.

First import a public API account using the existing `import-openai-api-key`
command described in [Desktop provider accounts](desktop.md#provider-accounts).
Associate that account with every Workspace that should use it. Configure its
opaque account ID, exact model, capability snapshot, and resource budgets in a
local JSON file. Do not put an API key or SecretRef in that file. Startup checks
that the saved account belongs to `openai_api`; connection state and Workspace
authorization are checked again at each claim. A disconnected account can be
reconnected through account management without restarting the daemon.

Start explicitly:

```nu
$env.KILN_RUN_EXECUTOR = "openai-api"
$env.KILN_OPENAI_API_CONFIG = "/absolute/path/to/native-model.json"
rtk cargo run -p kiln-daemon --bin kilnd
```

On an unassigned instance, this selects the account/model for new Runs. On a
Master or Follower, the shared model default and its locally resolved account
binding select the model and account for each new root or child Run; the
environment account/model fields do not override or constrain that shared
choice. In managed mode, this file still enables the local OpenAI executor and
sets its provider type, output/reasoning ceilings, capability ceilings, context
limits, and transport/resource budgets. Shared settings cannot enable another
provider adapter or raise those local ceilings.

The effective selection and configuration/binding provenance are committed
atomically with the Run. `GET /v1/runs/{run_id}/model-selection` returns the
provider, model, and non-secret source metadata. The local account ID and
credentials are omitted. Changing shared defaults or a local binding affects
new Runs only. Legacy Runs are never rebound from current configuration, and
the daemon does not restart external OpenAI work after a process restart.
Capabilities describe model support, not permission to execute tools. File
reads still require the explicit file-read limits and normal policy/approval
checks.

Migration 50 also stores each new Run's execution kind in the same creation
transaction: `native_model` accompanies a pinned model selection, while
`subprocess` records a legitimate local subprocess Run. A saved kind must match
the currently enabled executor. Pre-migration rows are not guessed from current
environment settings or missing model selections. Only an older no-selection
Run whose durable ToolCall history consists entirely of deterministic
subprocess capability calls, with no model invocation history, can be proven to
use the subprocess path; other unknown or inconsistent Runs fail closed.

## Configuration fields

The file is one JSON object. All fields below are required. Object paths use a
dot only in this documentation: `transport.request` means the `request` object
inside `transport`. Unknown fields, wrong types, nonpositive budgets, and integer
overflow are rejected. There are no implicit model, capability, or budget defaults.

| Object | Fields | Values |
| --- | --- | --- |
| root | `account_id`, `model` | Existing opaque account ID and exact provider model ID, as strings |
| root | `max_output_tokens` | Integer from 16 through the core u32 range; choose within the model's supported range |
| root | `reasoning_effort` | `null` or one of `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, `max`; choose an effort the model supports |
| `capabilities` | `version` | Nonempty ASCII version identifier without whitespace |
| `capabilities` | `tool_calls`, `vision`, `structured_output` | Each is `supported`, `unsupported`, or `unknown` |
| `context` | `max_text_bytes` | Positive integer; assembled text budget |
| `context` | `max_attachment_bytes`, `max_total_attachment_bytes` | Positive integers; one attachment and combined attachment budgets |
| `context` | `max_continuation_bytes`, `max_total_continuation_bytes` | Positive integers; one private continuation and combined replay budgets |
| `transport` | `connect_timeout_ms`, `request_timeout_ms` | Positive millisecond integers; connect must not exceed total request/body deadline |
| `transport.request` | `max_request_bytes`, `max_input_items` | Positive integers; encoded request bytes and input item count |
| `transport.request.replay` | `max_output_bytes`, `max_item_bytes`, `max_items` | Positive integers; replay output bytes, individual item bytes, and item count |
| `transport.stream` | `max_retained_bytes` | Positive integer; retained semantic stream data budget |
| `transport.stream.framing` | `max_frame_bytes`, `max_stream_bytes`, `max_events` | Positive integers; normalized frame bytes, actual wire bytes, and event count |
| `transport.stream.completion` | `max_response_bytes`, `max_identifier_bytes`, `max_visible_output_bytes` | Positive integers; terminal envelope, identifiers, and visible output budgets |
| `transport.stream.completion.replay` | `max_output_bytes`, `max_item_bytes`, `max_items` | Positive integers; accepted terminal replay budgets |
| `transport.stream.completion.requests` | `max_requests`, `max_provider_call_id_bytes`, `max_name_bytes`, `max_arguments_bytes`, `max_total_arguments_bytes` | Positive integers; tool proposal count, identifier/name sizes, one argument object and combined argument budgets |
| `transport.stream.completion.continuation` | `max_format_bytes`, `max_payload_bytes` | Positive integers; private continuation format and payload budgets |

Choose budgets from the host's available resources and intended workload. The
adapter buffers context, encoded requests, and streamed output; these byte limits
are data limits, not a bound on process memory. A single-item budget larger than
the corresponding aggregate budget is permitted but still constrained by that
aggregate. Total wire usage includes repeated semantic events, so it can be larger
than terminal output or replay size. Output is currently released after terminal
validation and clean EOF; it is not token-by-token UI streaming.

The endpoint is fixed to `https://api.openai.com/v1/responses`. Redirects,
automatic retries, and ambient proxy configuration are disabled. Startup performs
no credential read or model request. Starting a Run in this mode can send its
verified context to the public API and incur account charges. Local cancellation
drops the HTTP operation but cannot prove that remote computation or billing has
stopped. A daemon restart does not redispatch or silently reconcile real in-flight
requests; those remain blocked for explicit reconciliation.

Live API, vault, cancellation, and replay acceptance remain unverified. This mode
does not implement master-instance settings, global MCP, or global skills sync
(EDL-322).
