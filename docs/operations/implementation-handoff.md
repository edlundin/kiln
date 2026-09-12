# Implementation handoff

This handoff covers the EDL-216 milestone in the Kiln Linear tracker.

## Routing

- Astra owns planning, milestone reviews, and blockers.
- Reused Luna Max agents own routine commands, code, tests, log checks, and polling. Give each worker a bounded task. Workers report only milestone completion or a blocker. The parent waits for those reports and avoids duplicate work.

## Status

- [EDL-214](https://linear.app/edlundin/issue/EDL-214) and [EDL-215](https://linear.app/edlundin/issue/EDL-215) are verified Done.
- [EDL-216](https://linear.app/edlundin/issue/EDL-216) has a passing focused real-daemon proof and current measurements recorded in [docs/learning/edl-216.md](../learning/edl-216.md). The historical baseline links to the current record from [docs/architecture/measurement-baseline.md](../architecture/measurement-baseline.md).
- [EDL-287](https://linear.app/edlundin/issue/EDL-287) is the next known repair: resolve the child test's root/children terminal-state invariant.
- Edit `kiln.pen` through Pencil MCP only. [EDL-249](https://linear.app/edlundin/issue/EDL-249) remains unaccepted.

## Preservation

The initial user-owned files were preserved: `.gitignore`, `docs/architecture/perimeter.md`, `kiln.pen`, `skills-lock.json`, `.serena/`, and `graphify-out/`. No commits were created.
