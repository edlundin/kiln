# Kiln plugin protocol

Status: planned boundary

Plugins are external processes. They communicate with the plugin host through
JSON-RPC 2.0 messages over stdin and stdout. A plugin is not a Rust dynamic
library and it does not receive a database handle.

## Lifecycle

1. Kiln reads and validates the manifest.
2. Kiln compares the requested capabilities with policy.
3. The user or administrator approves a grant.
4. Kiln starts the command with a clean environment.
5. Kiln sends `kiln/initialize` with the granted capabilities and protocol
   version.
6. Kiln calls tools through `tools/call`.
7. Kiln records the request, decision, result, and process health as events.
8. Kiln sends `kiln/shutdown` before stopping the process when possible.

## Required messages

| Direction | Method | Purpose |
| --- | --- | --- |
| host → plugin | `kiln/initialize` | Negotiate protocol and pass the grant |
| plugin → host | `kiln/initialized` | Confirm readiness and expose tools |
| host → plugin | `tools/call` | Execute one manifest-declared tool |
| plugin → host | `tools/result` | Return structured result or artifact reference |
| host → plugin | `kiln/health` | Check liveness and version |
| host → plugin | `kiln/shutdown` | Request clean termination |

The exact method schemas will live under `protocol/plugin/` when the host is
implemented. This document defines the boundary and failure rules first.

## Capability rules

- The manifest requests capabilities. It never grants them.
- Each request includes the capability and effective scope used for the call.
- A denied capability returns a typed JSON-RPC error and a durable Kiln event.
- A plugin cannot widen its scope by sending a different path in a request.
- Tool results larger than the event payload threshold become artifacts.
- A crashed plugin fails its active tool calls and does not restart silently.
- Plugin stdout is protocol data. Diagnostics must use stderr.
- The host treats malformed frames, invalid method names, and invalid result
  shapes as plugin errors.

## UI extensions

A plugin may publish a client package later, but that package is not loaded into
the daemon. Client extensions must declare their target client, UI contribution
point, and protocol version. They receive data through the public client model,
not through private core objects.
