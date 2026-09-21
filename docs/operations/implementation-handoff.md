# Implementation handoff

This handoff covers the EDL-216 and EDL-288 milestones and the current EDL-308
attachment persistence repair in the Kiln Linear tracker.

## Routing

- Astra owns planning, milestone reviews, and blockers.
- Reused Luna Max agents own routine commands, code, tests, log checks, and polling. Give each worker a bounded task. Workers report only milestone completion or a blocker. The parent waits for those reports and avoids duplicate work.

## Status

- [EDL-214](https://linear.app/edlundin/issue/EDL-214) and [EDL-215](https://linear.app/edlundin/issue/EDL-215) are verified Done.
- [EDL-216](https://linear.app/edlundin/issue/EDL-216) has a passing focused real-daemon proof and current measurements recorded in [docs/learning/edl-216.md](../learning/edl-216.md). The historical baseline links to the current record from [docs/architecture/measurement-baseline.md](../architecture/measurement-baseline.md).
- [EDL-287](https://linear.app/edlundin/issue/EDL-287) repair is complete: the child test keeps the root pending until descendants are terminal, with direct parent/state assertions and no production weakening. The named test passes; the full suite was not rerun.
- [EDL-288](https://linear.app/edlundin/issue/EDL-288) implementation is complete. The backend and protocol add deterministic workspace/session listing with workspace scoping and unknown-workspace handling; the desktop adds workspace/session browsing, saved-session replay, session-scoped drafts and retry state, and reconnect restoration. The protocol contract is version `0.22.0`.
- [EDL-308](https://linear.app/edlundin/issue/EDL-308) attachment-only persistence repair is implemented. Migration `0022` preserves existing `0021` rows while allowing empty or whitespace message content when validated attachments are present; replay loads attachment metadata before Message validation, and context-manifest creation/reload uses the same attachment placeholder. Inputs with no attachments still require non-whitespace content. Wider acceptance remains pending.
- [EDL-309](https://linear.app/edlundin/issue/EDL-309) now has its bounded secret-store continuation in this worktree. Core carries only redacted, non-serializable `SecretValue` instances at the vault boundary and opaque `SecretRef` metadata; infrastructure provides an OS-backed macOS Keychain/Linux Secret Service adapter. Per-account lifecycle operations serialize connect, rotation, refresh, read, and disconnect; provider/account bindings are checked before reads and writes. Connect/rotate publish a durable reference only after vault storage succeeds, disconnect clears metadata before deletion, and cleanup failures return the affected opaque reference for recovery. OAuth flows, provider-specific refresh transport/token envelopes, revocation, protocol/UI routes, and live transport remain pending.
- Refresh requires the caller's expected current `SecretRef`. After one successful durable rotation, stale waiters reuse the committed account version without another provider request. Permanent rejection moves the account to `reauth_required`; transient rejection leaves it connected. The per-account lock serializes transient failures but does not cache or coalesce them.
- EDL-288 verification passed:
  - `rtk cargo run -p kiln-protocol --bin generate-contract`
  - `rtk cargo test -p kiln-protocol --test generated`
  - `rtk cargo test -p kiln-daemon --test black_box real_daemon_lists_workspaces_and_sessions_deterministically_and_recovers -- --exact --nocapture`
  - `rtk cargo test -p kiln-infrastructure --lib session_migration_survives_reopen`
  - `rtk cargo check -p kiln-protocol -p kiln-core -p kiln-infrastructure -p kiln-server -p kiln-client`
  - `rtk cargo check -p kiln-desktop --bin kiln-desktop`
  - `rtk cargo build -p kiln-desktop --bin kiln-desktop`
  - `rtk git diff --check`
- Native isolated smoke verified composer/draft restoration across daemon loss and restart; temporary processes were stopped. Reaction and retry branches were code-reviewed after the reconnect fix but were not separately demonstrated, and no automated UI tests exist.
- Current progression: [EDL-308](https://linear.app/edlundin/issue/EDL-308) remains open pending wider attachment acceptance. The artifact inspector continuation below is implemented in this worktree, but it has not received runtime visual verification. No broad or full test suite was run.

## Artifact preview continuation

- The desktop now opens artifact entries from the transcript in a temporary inspector. The existing authenticated artifact route is fetched with a streaming 64 KiB cap before buffering. UTF-8 text, JSON, and XML previews are supported; binary and other media types remain download-only, and fetch failures offer retry.
- Preview state is scoped to the active Session and request/hash identity. Session changes, reconnects, disconnects, and Escape invalidate an open preview so an older response cannot replace the current view.
- The Session change-summary continuation is implemented in the backend and protocol. New Sessions persist the first Workspace root's canonical checkout, Git common directory, normalized `.` scope, and filesystem identity. `GET /v1/sessions/{session_id}/changes` revalidates those captured values, combines staged and unstaged tracked changes against `HEAD`, includes untracked files, and returns sorted paths with nullable counts for untracked or binary files. Git requests clear inherited environment variables except `PATH`, disable external diff and text conversion, and enforce 4 MiB of output and 30 seconds per Git invocation. Legacy Sessions remain readable and report an unavailable checkout for this endpoint.
- The current continuation adds `GET /v1/sessions/{session_id}/change-diff?path=...` for one selected changed file. It returns a bounded unified text patch or an explicit unavailable reason for untracked, binary, conflicted, renamed, unsupported file types, or unsupported encodings. Git input files are capped at 4 MiB and the rendered patch at 256 KiB; desktop request state is scoped by Session, path, and request ID.
- Composer attachment uploads are included in this slice and capped at 64 MiB per artifact. No persistent supervisor or secondary Run graph is included. The backend exposes a bounded authenticated read-only Usage ledger at `GET /v1/usage`, and the desktop provides its paginated Usage view. The ledger reports the latest validated revision per physical Model Invocation and carries no pricing or valuation.

## Preservation

The initial user-owned files were preserved: `.gitignore`, `docs/architecture/perimeter.md`, `kiln.pen`, `skills-lock.json`, `.serena/`, and `graphify-out/`. Current HEAD `dab728e`, the EDL-287 repair state, and uncommitted user work remain preserved. No commits, staging, or pushes were created.
