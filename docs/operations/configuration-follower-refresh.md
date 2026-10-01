# Configuration follower refresh

`kilnd` can periodically fetch and apply snapshots for one already approved
follower enrollment. Refresh is disabled by default and sends no network requests
unless every setting in the table is configured. The daemon reads these values
at startup; changing them requires a restart.

| Environment variable | Value |
| --- | --- |
| `KILN_CONFIGURATION_FOLLOWER_REFRESH_INSTANCE_ID` | Exact local follower instance ID. |
| `KILN_CONFIGURATION_FOLLOWER_REFRESH_ATTEMPT_ID` | Exact approved `cra_` enrollment attempt ID. |
| `KILN_CONFIGURATION_FOLLOWER_REFRESH_ORIGIN` | Caller-selected HTTPS origin, with no credentials, path, query or fragment. Its host must match the enrollment's immutable TLS server name. The port may change. |
| `KILN_CONFIGURATION_FOLLOWER_REFRESH_CONNECT_TIMEOUT_MS` | Positive connection timeout in milliseconds. Must not exceed the request timeout. |
| `KILN_CONFIGURATION_FOLLOWER_REFRESH_REQUEST_TIMEOUT_MS` | Positive deadline for connection and the complete response body, in milliseconds. |
| `KILN_CONFIGURATION_FOLLOWER_REFRESH_POLL_INTERVAL_MS` | Positive delay in milliseconds between completed checks. No default cadence is supplied. |
| `KILN_CONFIGURATION_FOLLOWER_REFRESH_FRESHNESS_THRESHOLD_MS` | Positive age threshold in milliseconds used to report the last successful check as fresh or stale. No default is supplied. |

If any refresh setting is present, every setting is required. The daemon rejects
malformed IDs, non-HTTPS or non-origin URLs, zero or inconsistent timeouts, and
non-positive intervals or freshness thresholds before readiness. When the
configured attempt exists for the configured follower, startup also checks that
the origin host matches its pinned server name. A missing, retired, or
role-mismatched enrollment does not prevent local daemon startup; the coordinator
reports it as inactive and makes no fetch through another attempt.

The first check starts after readiness. The coordinator runs one operation at a
time, then waits the configured interval after completion. Committed local role
or enrollment lifecycle changes can prompt an earlier check. Each operation calls
the same durable fetch path as the explicit one-shot command: it reloads the
exact attempt's CA DER, server name, master/group/follower IDs and vault credential,
checks the private active marker, authenticates the pinned HTTPS peer, records
the observed revision, validates the complete snapshot, and atomically applies
it behind the existing state and enrollment fences. The supplied origin is a
transport option only; refresh never discovers peers, switches attempts,
reapproves an enrollment, or creates credentials. Changing the origin alone
does not require reenrollment when it still matches the pinned server name.

Refresh failures do not fail local daemon operations. Transient fetch, storage,
candidate/revision rejection, and state-race failures appear in status and are
retried on the configured schedule. A missing, retired, recovery-required, or
verified no-longer-follower attempt stops refresh until the daemon restarts with
an explicitly configured attempt. Shutdown stops new checks,
waits for any owned fetch/apply operation to finish, and joins the coordinator
before releasing the store.

The authenticated `GET /v1/configuration-sync` response includes `transport`
(`unconfigured` or `configured`) and a `refresh` object. `configured` reports
that scheduling is enabled; it does not claim a live connection. The refresh
object includes the configured follower and attempt IDs, enrolled group/master
when available, HTTPS origin, connect/request timeouts, cadence and freshness
threshold, current operation state, last outcome, last check and successful check
timestamps, last successful revision, age, and recency. Recency is
`unknown` before a success and after daemon restart, `fresh` while the most recent
success is no older than the configured threshold, and `stale` after that
threshold. It describes the age of a completed authenticated check, not a
connection lease, an assertion that the master is still reachable, or activation
of local settings, MCP definitions, or skills. Durable enrollment receipts and
matching observed/applied revisions do not substitute for a successful check.

See the [configuration synchronization contract](../spec/configuration-sync.md)
for the authority, enrollment, validation and application rules.

A disposable macOS fixture exercised automatic nonempty snapshot application,
offline stale status and recovery, later publication, restart rechecks, grant
revocation and inactive retirement over pinned TLS with real configuration
Keychain entries. See [EDL-322 evidence](../learning/edl-322.md) for the explicit
fixture settings and remaining native, provider and platform limitations.
