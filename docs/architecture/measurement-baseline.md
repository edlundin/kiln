# Kiln measurement baseline

Date: 2026-08-06

Toolchain: Rust 1.97.1, Cargo 1.97.1

The measurements use the local macOS development machine. They are a baseline, not a performance target.

| Measurement | Result | Method |
| --- | ---: | --- |
| Clean daemon build | 13.09 s | `cargo clean` followed by `cargo build -p kiln-daemon` |
| Incremental daemon build | 0.14 s | Repeated `cargo build -p kiln-daemon` |
| Debug daemon executable | 24,348,192 bytes | `stat -f %z target/debug/kiln-daemon` |
| Idle daemon RSS | 12,752 KB | Started daemon with SQLite and file artifacts, then sampled `ps` after capabilities became available |
| Running daemon RSS | 13,696 KB | Same daemon while a deterministic 30-second sleep tool run was active |
| Cancellation | Passed | HTTP cancel command returned, then graceful shutdown completed |
| Reconnect and replay | Passed | `cargo test -p kiln-server --test api` replay test |
| Graceful shutdown exit | 0 | HTTP shutdown state change followed by SIGINT |

The RSS values are one-process samples. They do not represent a capacity limit. Repeat them on the target host before setting operational limits.

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
| 5-second WebSocket receive timeout in API tests | Fail a disconnected stream test instead of waiting indefinitely. |
| 16,777,216-byte API test response cap | Bounds test-body allocation above the deterministic large artifact while allowing full artifact retrieval. |
