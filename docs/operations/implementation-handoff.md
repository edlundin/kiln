# Implementation handoff

This handoff covers the EDL-216 and EDL-288 milestones in the Kiln Linear tracker.

## Routing

- Astra owns planning, milestone reviews, and blockers.
- Reused Luna Max agents own routine commands, code, tests, log checks, and polling. Give each worker a bounded task. Workers report only milestone completion or a blocker. The parent waits for those reports and avoids duplicate work.

## Status

- [EDL-214](https://linear.app/edlundin/issue/EDL-214) and [EDL-215](https://linear.app/edlundin/issue/EDL-215) are verified Done.
- [EDL-216](https://linear.app/edlundin/issue/EDL-216) has a passing focused real-daemon proof and current measurements recorded in [docs/learning/edl-216.md](../learning/edl-216.md). The historical baseline links to the current record from [docs/architecture/measurement-baseline.md](../architecture/measurement-baseline.md).
- [EDL-287](https://linear.app/edlundin/issue/EDL-287) repair is complete: the child test keeps the root pending until descendants are terminal, with direct parent/state assertions and no production weakening. The named test passes; the full suite was not rerun.
- [EDL-288](https://linear.app/edlundin/issue/EDL-288) implementation is complete. The backend and protocol add deterministic workspace/session listing with workspace scoping and unknown-workspace handling; the desktop adds workspace/session browsing, saved-session replay, session-scoped drafts and retry state, and reconnect restoration. The protocol contract is version `0.20.0`.
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
- Remaining limits: no broad or full test suite was run. The next documented gap is overall Pencil acceptance for [EDL-249](https://linear.app/edlundin/issue/EDL-249), which remains unaccepted; edit `kiln.pen` through Pencil MCP only.

## Preservation

The initial user-owned files were preserved: `.gitignore`, `docs/architecture/perimeter.md`, `kiln.pen`, `skills-lock.json`, `.serena/`, and `graphify-out/`. Current HEAD `dab728e`, the EDL-287 repair state, and uncommitted user work remain preserved. No commits, staging, or pushes were created.
