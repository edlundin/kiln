# Kiln measurement baseline

Date: 2026-08-06

Performance toolchain: Rust 1.97.1, Cargo 1.97.1

EDL-213 functional verification: Rust 1.98.0, Cargo 1.98.0 on 2026-08-31.

The measurements use the local macOS development machine. They are a baseline, not a performance target.

| Measurement | Result | Method |
| --- | ---: | --- |
| Clean daemon build | 13.09 s | `cargo clean` followed by `cargo build -p kiln-daemon` |
| Incremental daemon build | 0.14 s | Repeated `cargo build -p kiln-daemon` |
| Debug daemon executable | 24,348,192 bytes | `stat -f %z target/debug/kiln-daemon` |
| Idle daemon RSS | 12,752 KB | Started daemon with SQLite and file artifacts, then sampled `ps` after capabilities became available |
| Running daemon RSS | 13,696 KB | Same daemon while a deterministic 30-second sleep tool run was active |
| Cancellation | Passed | The daemon black-box test cancelled one active Run, stopped its process group, and recovered the durable terminal result after restart |
| Graceful signal shutdown | Passed | The daemon black-box tests sent SIGINT and SIGTERM, then checked clean exit and durable cancellation of active work |

The RSS values are one-process samples. They do not represent a capacity limit. Repeat them on the target host before setting operational limits.

## EDL-216 complete vertical slice

Date: 2026-09-04

Host: macOS 27.0 on Apple M1 Pro, arm64, 32 GiB memory

Toolchain: Rust 1.100.0-nightly (0dfb098f3 2026-08-31), Cargo
1.100.0-nightly (e8cb624d5 2026-08-22)

| Measurement | Result | Method |
| --- | ---: | --- |
| Clean daemon build | 26.51 s | `cargo clean`, then `/usr/bin/time -p cargo build -p kiln-daemon` |
| Incremental daemon build | 0.30 s | Repeated `/usr/bin/time -p cargo build -p kiln-daemon` |
| Debug daemon executable | 20,508,408 bytes | `stat -f %z target/debug/kilnd` |
| Idle daemon RSS | 9,696 KB | Sampled the ready daemon with `ps -o rss= -p <pid>` before the scenario sent HTTP work |
| WebSocket streaming peak RSS | 11,440 KB | Maximum daemon RSS sampled after each replayed or live WebSocket Event while a 180,000-byte artifact Run completed |
| WebSocket reconnect and acknowledgement | 1.087 ms | `Instant` elapsed time for authenticated reconnect with an `after` cursor after daemon restart |
| Active Run cancellation | 5.407 ms | `Instant` elapsed time from `POST /v1/runs/{run_id}/cancel` through response decoding, durable cancelled-state verification, and process-group stop verification |
| Graceful idle shutdown | 4.492 ms | `Instant` elapsed time from SIGINT delivery through successful daemon exit |
| Complete vertical-slice scenario | Passed in 0.65 s | `cargo test -p kiln-daemon --test black_box complete_first_vertical_slice_is_repeatable_across_reconnect_and_restart -- --exact --nocapture` |

The test emits the runtime samples as one `EDL-216 metrics` line. The scenario
uses the real daemon, HTTP and WebSocket clients, SQLite, the file artifact
store, and owned subprocess groups. These values are evidence from one run.
They are not timeout, memory, or performance limits.

## Deterministic fixture values

These values make the first slice repeatable. They are test-fixture limits, not production budgets.

| Value | Reason |
| ---: | --- |
| 4,096 bytes | Keep normal event payloads small; larger output uses the artifact store. |
| 1,024 bytes | Cap each live output event; the 10,000-line fixture emitted 180,000 bytes in 176 chunks, with a measured maximum chunk size of 1,024 bytes. |
| 10,000 output lines | Produce a repeatable artifact larger than the inline threshold. |
| 30 seconds | Keep the sleep fixture active long enough to observe and cancel it. |
| 256 event notifications | Bound retained broadcast notifications for a slow subscriber. |
| 32 run updates | Apply bounded backpressure between one executor and one run owner. |
| 5 SQLite connections for files, 1 for in-memory databases | Match one local daemon store to SQLite's file and in-memory access modes. |
| 100 polling attempts at 20 ms in API tests | Bound asynchronous fixture waits at two seconds while keeping tests responsive. |
| 5 seconds | Bound the EDL-216 fixture PID, WebSocket, and polled shutdown waits above the measured 0.65-second scenario; synchronous daemon startup is not preemptible. |
| 16,777,216-byte API test response cap | Bounds test-body allocation above the deterministic large artifact while allowing full artifact retrieval. |

## EDL-216 current verification

The EDL-216 values dated 2026-09-04 in this baseline are historical and remain unchanged. The current EDL-216 evidence dated 2026-09-12 is recorded in [docs/learning/edl-216.md](../learning/edl-216.md#current-verification--2026-09-12).
