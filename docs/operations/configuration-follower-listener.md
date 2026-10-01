# Configuration follower TLS listener

`kilnd` can expose the restricted master-side follower router on a separate TLS
socket. The existing local administration API stays bound to loopback. The
remote listener is disabled unless `KILN_CONFIGURATION_FOLLOWER_LISTEN_ADDR` is
set.

Configure the master role and an active managed TLS identity through the local
administration API first. Keep the remote listener setting unset while doing so.
After identity status reports `active`, restart the daemon with the settings
below. An enabled listener fails startup before readiness if the current role,
identity, certificate, key, authority, or bind is unavailable or invalid.

All settings in this table are required when the listener is enabled. The daemon
supplies no connection, byte, timeout, or retention defaults.

| Environment variable | Value |
| --- | --- |
| `KILN_CONFIGURATION_FOLLOWER_LISTEN_ADDR` | Local socket address to bind, such as an IP address and port. It is separate from the advertised HTTPS authority. |
| `KILN_CONFIGURATION_FOLLOWER_HOST_AUTHORITY` | The public certificate host and optional port, such as `sync.example.net:443`. The host must match the active certificate name. An omitted port means 443. The daemon canonicalizes this authority for incoming `Host` checks. |
| `KILN_CONFIGURATION_FOLLOWER_MAX_CONNECTIONS` | Positive number of accepted sockets, including TLS handshakes and responses. |
| `KILN_CONFIGURATION_FOLLOWER_MAX_HTTP_BUFFER_BYTES` | HTTP/1 buffer budget; at least 8192 bytes. |
| `KILN_CONFIGURATION_FOLLOWER_HANDSHAKE_TIMEOUT_MS` | Positive TLS handshake deadline in milliseconds. |
| `KILN_CONFIGURATION_FOLLOWER_REQUEST_TIMEOUT_MS` | Positive deadline for the full request and response in milliseconds. |
| `KILN_CONFIGURATION_FOLLOWER_SHUTDOWN_TIMEOUT_MS` | Positive maximum graceful network drain in milliseconds. |
| `KILN_CONFIGURATION_FOLLOWER_MAX_RETAINED_REQUESTS_PER_AUTHORITY` | Positive 32-bit cap for pending and terminal enrollment-request rows. |

The bind address controls where the daemon accepts sockets. The host authority
controls the public TLS endpoint and request `Host` value; its port can differ
from the local bind port when a proxy or port mapping is in use. Readiness
includes `configuration_follower_address` and the canonical
`configuration_follower_host_authority` when the listener is enabled.

The remote socket serves only the configuration snapshot GET and digest-only
follower enrollment-request POST. It uses TLS 1.2 or 1.3 and one HTTP/1 request
per connection. Followers authenticate snapshot reads with their dedicated
configuration bearer; the local API bearer is not accepted there. The listener
does not expose local administration, publication, execution, or provider
operations.

The daemon rechecks current master authority and active identity metadata after
committed role or identity changes. Role loss, active identity replacement,
certificate expiry, or a supervisor storage error stops that listener generation
and cancels and joins its connection tasks. Unrelated cleanup of a historical
identity and ordinary snapshot revision/version updates leave a matching active
generation running. A stopped generation remains disabled until daemon restart.

On daemon shutdown, the listener stops admission and gives active connections
the configured drain period before aborting and joining them. An accepted
enrollment POST moves its SQLite operation into an owned task holding the same
daemon command permit as local writes. Canceling the network handler therefore
does not cancel the accepted storage operation; shutdown waits for that permit
before store teardown.

A disposable macOS two-instance fixture exercised pinned TLS enrollment,
OS-vault persistence across restart, snapshot application, grant revocation,
remote-route isolation and explicit credential cleanup. See
[EDL-322 evidence](../learning/edl-322.md). Later fixtures also exercised
nonempty shared model defaults with deterministic Runs, automatic refresh,
offline recovery and listener closure after identity retirement and leaf
certificate expiry, including one pending TLS handshake. A separate same-host
wrong-CA fixture rejected enrollment and acquisition while preserving the
intended follower state. Native interaction, real providers, MCP/skill
consumers, abnormal cancellation/role-loss and remaining expiry races remain
unverified.
