#![cfg(unix)]

use std::{collections::BTreeMap, ffi::OsString, num::NonZeroUsize, path::PathBuf, time::Duration};

use kiln_mcp::{
    ProtocolPolicy, ProtocolVersion, StdioProcess, StdioProcessConfig, start_stdio_client,
};
use rmcp::{model::ServerJsonRpcMessage, transport::Transport};
use rustix::{
    io::Errno,
    process::{Pid, test_kill_process},
};

fn config(script: &str) -> StdioProcessConfig {
    // The fixture emits a short fixed JSON reply. This budget is test data,
    // not a runtime default or a measured product capacity claim.
    StdioProcessConfig {
        executable: PathBuf::from("/bin/sh"),
        arguments: vec![OsString::from("-c"), OsString::from(script)],
        working_directory: PathBuf::from("/"),
        environment: BTreeMap::from([(
            OsString::from("KILN_MCP_FIXTURE"),
            OsString::from("explicit"),
        )]),
        max_frame_bytes: NonZeroUsize::new(512).unwrap(),
        shutdown_grace: Duration::ZERO,
    }
}

#[tokio::test]
async fn real_process_negotiates_with_explicit_environment_and_directory() {
    assert!(
        std::env::var_os("HOME").is_some(),
        "fixture needs an inherited HOME to prove exclusion"
    );
    let script = r#"
        test -z "${HOME+x}" || exit 31
        test "$KILN_MCP_FIXTURE" = explicit || exit 32
        test "$PWD" = / || exit 33
        IFS= read -r request || exit 34
        printf '%s\n' '{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2025-11-25","capabilities":{},"serverInfo":{"name":"fixture","version":"1"}}}'
        while IFS= read -r request; do :; done
    "#;
    let process = StdioProcess::spawn(config(script)).unwrap();
    let pid = Pid::from_raw(process.process_id().unwrap() as i32).unwrap();
    let client = start_stdio_client(
        (),
        process,
        ProtocolPolicy::Pinned(ProtocolVersion::V20251125),
    )
    .await
    .unwrap();
    client.cancel().await.unwrap();
    assert_eq!(test_kill_process(pid), Err(Errno::SRCH));
}

#[tokio::test]
async fn close_reaps_child_and_is_idempotent() {
    let mut process = StdioProcess::spawn(config("exec /bin/sleep 60")).unwrap();
    let pid = Pid::from_raw(process.process_id().unwrap() as i32).unwrap();
    process.close().await.unwrap();
    assert_eq!(process.process_id(), None);
    assert_eq!(test_kill_process(pid), Err(Errno::SRCH));
    process.close().await.unwrap();
}

#[tokio::test]
async fn shutdown_closes_stdin_before_forcing_cleanup() {
    let mut launch = config(
        r#"
        while IFS= read -r request; do :; done
        printf '%s\n' '{"jsonrpc":"2.0","id":0,"error":{"code":-32601,"message":"stdin closed"}}'
    "#,
    );
    // Paused time is unsuitable for this OS process. A one-second fixture
    // grace allows its EOF reply; this is not a product timeout default.
    launch.shutdown_grace = Duration::from_secs(1);
    let mut process = StdioProcess::spawn(launch).unwrap();
    process.close().await.unwrap();
    assert!(
        process.exit_status().unwrap().success(),
        "fixture must observe EOF and exit before forced cleanup"
    );
    // The transport is closed permanently even when the child wrote a final reply.
    assert!(process.receive().await.is_none());
    assert!(process.process_id().is_none());
}

#[tokio::test]
async fn close_also_stops_descendants_in_the_owned_group() {
    let mut process = StdioProcess::spawn(config(
        r#"
        /bin/sleep 60 &
        printf '{"jsonrpc":"2.0","id":0,"error":{"code":-32601,"message":"%s"}}\n' "$!"
        wait
    "#,
    ))
    .unwrap();
    let Some(ServerJsonRpcMessage::Error(reply)) = process.receive().await else {
        panic!("missing fixture PID");
    };
    let descendant = Pid::from_raw(reply.error.message.parse().unwrap()).unwrap();
    assert!(test_kill_process(descendant).is_ok());
    process.close().await.unwrap();
    // The orphan reaper runs asynchronously after the group is killed. Bound
    // this fixture's scheduling wait so a cleanup regression cannot hang tests.
    tokio::time::timeout(Duration::from_secs(5), async {
        while test_kill_process(descendant) != Err(Errno::SRCH) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("descendant should be reaped");
}

#[tokio::test]
async fn malformed_server_does_not_leave_a_process_after_close() {
    let mut process =
        StdioProcess::spawn(config("printf 'invalid\\n'; exec /bin/sleep 60")).unwrap();
    let pid = Pid::from_raw(process.process_id().unwrap() as i32).unwrap();
    let reply: Option<ServerJsonRpcMessage> = process.receive().await;
    assert!(reply.is_none());
    process.close().await.unwrap();
    assert_eq!(test_kill_process(pid), Err(Errno::SRCH));
}

#[tokio::test]
async fn cancelling_startup_drops_and_reaps_the_owned_child() {
    let process = StdioProcess::spawn(config("exec /bin/sleep 60")).unwrap();
    let pid = Pid::from_raw(process.process_id().unwrap() as i32).unwrap();
    // The deliberately silent fixture cannot finish negotiation. This short
    // deadline exercises caller cancellation, not a production startup budget.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            start_stdio_client(
                (),
                process,
                ProtocolPolicy::Pinned(ProtocolVersion::V20260728)
            )
        )
        .await
        .is_err()
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while test_kill_process(pid) != Err(Errno::SRCH) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancelled child should be reaped");
}

#[test]
fn relative_launch_paths_are_rejected() {
    let mut launch = config("");
    launch.executable = PathBuf::from("sh");
    assert!(StdioProcess::spawn(launch).is_err());
    let mut launch = config("");
    launch.working_directory = PathBuf::from(".");
    assert!(StdioProcess::spawn(launch).is_err());
}
