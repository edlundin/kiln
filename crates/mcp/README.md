# MCP broker boundary

This crate begins EDL-314 with a guarded stdio negotiation adapter over the
official `rmcp` SDK, pinned to 3.4.1. It is not yet connected to daemon tool
execution or synchronized MCP definitions. It does not launch processes or grant
permission to invoke tools, read resources, or request prompts.

`start_stdio_client` accepts a caller-owned transport and either `Auto` or a pin
to one of the five final versions in EDL-247. Auto prefers `2026-07-28` discovery.
Only an exact-ID-correlated JSON-RPC `METHOD_NOT_FOUND` permits legacy initialize
on the same transport. Uncorrelated/missing IDs and arbitrary server errors do
not authorize fallback. This adapter is specific to stdio; HTTP requires separate
structured unsupported-version handling.

The SDK's Auto mode attempts legacy initialize when discovery takes ten seconds.
The guard rejects that attempt before sending anything. This inherited SDK
startup ceiling currently means a slow Auto discovery fails; it never silently
downgrades. Callers must supply their own overall deadline and cancellation for
other startup paths. No Kiln-wide timeout or capacity default is established by
this increment.

Legacy pins send the exact pinned version and reject another returned version
before `notifications/initialized`. Auto accepts only final legacy versions from
legacy initialization. Modern discovery is restricted to `2026-07-28`; a modern
pin cannot fall back. Rejected legacy versions close the transport and fail
startup. Protocol acceptance is not evidence that every feature of that version
is implemented or that conformance has passed.

The integration fixtures exercise negotiation messages, including timeout and
error downgrade prevention, exact legacy pins, modern startup, and rejection of
draft/incorrect-era results. Run `cargo test -p kiln-mcp` from the workspace.
These in-memory fixtures are not the official MCP conformance suite.

Remaining broker work includes scoped process ownership and explicit environment,
registration and host binding resolution, durable lifecycle and invocation
events, Kiln grants/approvals, catalogue/result paging, interruption/recovery,
HTTP/OAuth, mediated server requests, and full conformance. No MCP operation is
offered to models until it is connected to the normal durable ToolCall boundary.
