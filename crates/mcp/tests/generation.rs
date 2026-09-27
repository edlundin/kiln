#![cfg(unix)]

use std::{
    collections::BTreeMap, ffi::OsString, num::NonZeroUsize, path::Path, sync::Arc, time::Duration,
};

use kiln_core::*;
use kiln_infrastructure::SqliteStore;
use kiln_mcp::{
    StdioGeneration, StdioGenerationError, StdioGenerationLaunch, StdioProcessConfig,
    StdioRegistry, StdioRegistryError,
};
use rustix::{
    io::Errno,
    process::{Pid, test_kill_process},
};
use sqlx::Connection;

fn limits() -> McpDefinitionLimits {
    McpDefinitionLimits {
        max_key_bytes: 64,
        max_metadata_bytes: 4096,
        max_arguments: 4,
        max_argument_bytes: 64,
        max_environment: 4,
        max_endpoint_bytes: 128,
    }
}

fn definition(enabled: bool) -> McpServerDefinition {
    McpServerDefinition::new(
        SharedMcpServerInput {
            id: SharedConfigurationKey::parse("fixture", 64).unwrap(),
            enabled,
            transport: SharedMcpTransport::Stdio {
                runtime_binding: SharedConfigurationKey::parse("runtime", 64).unwrap(),
                arguments: vec![],
                environment: BTreeMap::new(),
            },
        },
        McpProtocolPolicy::Pinned(McpProtocolVersion::V20251125),
        McpLifecycleScope::Core,
        None,
        limits(),
    )
    .unwrap()
}

async fn setup(path: &Path) -> (Arc<SqliteStore>, McpInstanceKey) {
    let store = Arc::new(SqliteStore::open(path).await.unwrap());
    let definition = definition(true);
    store
        .register_mcp_definition(&definition, 0, "register", limits())
        .await
        .unwrap();
    let key = McpInstanceKey::new(&definition, McpInstanceOwner::Core, 4096).unwrap();
    (store, key)
}

const READY: &str = r#"
    printf '%s' "$$" > "$PID_FILE"
    IFS= read -r request || exit 31
    printf '%s\n' '{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2025-11-25","capabilities":{},"serverInfo":{"name":"fixture","version":"1"}}}'
    while IFS= read -r request; do :; done
"#;

fn launch(key: McpInstanceKey, path: &Path, script: &str) -> StdioGenerationLaunch {
    StdioGenerationLaunch {
        host_binding_version: None,
        key,
        definition_version: 1,
        generation: McpGenerationId::from_ulid(ulid::Ulid::generate()),
        definition_limits: limits(),
        process: StdioProcessConfig {
            executable: "/bin/sh".into(),
            arguments: vec!["-c".into(), script.into()],
            working_directory: std::fs::File::open(path).unwrap().into(),
            environment: BTreeMap::from([(
                OsString::from("PID_FILE"),
                path.join("pid").into_os_string(),
            )]),
            max_frame_bytes: NonZeroUsize::new(512).unwrap(),
            shutdown_grace: Duration::ZERO,
        },
        // Fixtures have a bounded scheduling allowance, not a product default.
        startup_deadline: tokio::time::Instant::now() + Duration::from_secs(5),
    }
}

async fn pid(path: &Path) -> Pid {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(value) = std::fs::read_to_string(path.join("pid")) {
                if let Ok(value) = value.parse() {
                    return Pid::from_raw(value).unwrap();
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("fixture should write PID")
}

async fn stopped(store: &SqliteStore, key: &McpInstanceKey) -> McpInstanceRecord {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(record) = store.get_mcp_instance(key).await.unwrap() {
                if record.observed == McpObservedState::Stopped {
                    return record;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("worker should journal cleanup")
}

#[tokio::test]
async fn durable_claim_owns_one_process_and_stop_reaps_before_terminal_state() {
    let directory = tempfile::tempdir().unwrap();
    let (store, key) = setup(directory.path()).await;
    let mut owner =
        StdioGeneration::spawn(store.clone(), launch(key.clone(), directory.path(), READY));
    let ready = owner.wait_ready().await.unwrap();
    let process = pid(directory.path()).await;
    assert_eq!(ready.observed, McpObservedState::Ready);
    assert_eq!(
        ready.negotiated_protocol,
        Some(McpProtocolVersion::V20251125)
    );

    let mut duplicate = StdioGeneration::spawn(
        store.clone(),
        launch(key.clone(), directory.path(), "exit 99"),
    );
    assert_eq!(
        duplicate.wait_ready().await.err(),
        Some(StdioGenerationError::Existing)
    );
    assert!(test_kill_process(process).is_ok());
    assert_eq!(
        store
            .get_mcp_instance(&key)
            .await
            .unwrap()
            .unwrap()
            .generation,
        ready.generation
    );

    let terminal = owner.stop().await.unwrap();
    assert_eq!(terminal.observed, McpObservedState::Stopped);
    assert_eq!(terminal.desired, McpDesiredState::Stopped);
    assert_eq!(test_kill_process(process), Err(Errno::SRCH));
    let mut next = StdioGeneration::spawn(store.clone(), launch(key, directory.path(), READY));
    assert_ne!(
        next.wait_ready().await.unwrap().generation,
        ready.generation
    );
    next.stop().await.unwrap();
}

#[tokio::test]
async fn dropping_startup_handle_still_reaps_and_journals_stop() {
    let directory = tempfile::tempdir().unwrap();
    let (store, key) = setup(directory.path()).await;
    let owner = StdioGeneration::spawn(
        store.clone(),
        launch(
            key.clone(),
            directory.path(),
            "printf '%s' \"$$\" > \"$PID_FILE\"; exec /bin/sleep 60",
        ),
    );
    let process = pid(directory.path()).await;
    drop(owner);
    let terminal = stopped(store.as_ref(), &key).await;
    assert_eq!(terminal.desired, McpDesiredState::Stopped);
    assert_eq!(test_kill_process(process), Err(Errno::SRCH));
}

#[tokio::test]
async fn startup_timeout_reaps_before_failed_state() {
    let directory = tempfile::tempdir().unwrap();
    let (store, key) = setup(directory.path()).await;
    let mut inputs = launch(
        key.clone(),
        directory.path(),
        "printf '%s' \"$$\" > \"$PID_FILE\"; exec /bin/sleep 60",
    );
    // Enough for this local process to write its PID; intentionally shorter than
    // its sleep, solely to exercise timeout and cleanup.
    inputs.startup_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let mut owner = StdioGeneration::spawn(store.clone(), inputs);
    let process = pid(directory.path()).await;
    assert_eq!(
        owner.wait_ready().await.err(),
        Some(StdioGenerationError::StartupDeadline)
    );
    assert_eq!(test_kill_process(process), Err(Errno::SRCH));
    assert_eq!(
        store
            .get_mcp_instance(&key)
            .await
            .unwrap()
            .unwrap()
            .observed,
        McpObservedState::Failed
    );
}

#[tokio::test]
async fn definition_change_during_startup_rejects_ready_and_cleans_up() {
    let directory = tempfile::tempdir().unwrap();
    let (store, key) = setup(directory.path()).await;
    let script = READY.replace(
        "IFS= read -r request || exit 31",
        "while test ! -f gate; do /bin/sleep 0.01; done\nIFS= read -r request || exit 31",
    );
    let mut owner = StdioGeneration::spawn(
        store.clone(),
        launch(key.clone(), directory.path(), &script),
    );
    let process = pid(directory.path()).await;
    store
        .register_mcp_definition(&definition(false), 1, "disable", limits())
        .await
        .unwrap();
    std::fs::write(directory.path().join("gate"), b"").unwrap();
    assert!(matches!(
        owner.wait_ready().await.err(),
        Some(StdioGenerationError::Store(
            McpInstanceError::DefinitionChanged | McpInstanceError::Disabled
        ))
    ));
    assert_eq!(test_kill_process(process), Err(Errno::SRCH));
    assert_eq!(
        store
            .get_mcp_instance(&key)
            .await
            .unwrap()
            .unwrap()
            .observed,
        McpObservedState::Failed
    );
}

#[tokio::test]
async fn server_disconnect_is_cleaned_up_without_restarting() {
    let directory = tempfile::tempdir().unwrap();
    let (store, key) = setup(directory.path()).await;
    let script = READY.replace(
        "while IFS= read -r request; do :; done",
        "while test ! -f gate; do /bin/sleep 0.01; done",
    );
    let mut owner = StdioGeneration::spawn(
        store.clone(),
        launch(key.clone(), directory.path(), &script),
    );
    let ready = owner.wait_ready().await.unwrap();
    let process = pid(directory.path()).await;
    std::fs::write(directory.path().join("gate"), b"").unwrap();
    let terminal = stopped(store.as_ref(), &key).await;
    assert_eq!(terminal.generation, ready.generation);
    assert_eq!(terminal.desired, McpDesiredState::Running);
    assert_eq!(test_kill_process(process), Err(Errno::SRCH));
    owner.stop().await.unwrap();
}

#[tokio::test]
async fn failed_stop_journal_does_not_skip_cleanup_or_allow_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let (store, key) = setup(directory.path()).await;
    let mut owner =
        StdioGeneration::spawn(store.clone(), launch(key.clone(), directory.path(), READY));
    let ready = owner.wait_ready().await.unwrap();
    let process = pid(directory.path()).await;
    let mut connection = sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new().filename(directory.path().join("kiln.sqlite3")),
    )
    .await
    .unwrap();
    sqlx::query("CREATE TRIGGER fixture_stop_failure BEFORE INSERT ON mcp_instance_events WHEN NEW.reason = 'stop_requested' BEGIN SELECT RAISE(ABORT, 'fixture'); END")
        .execute(&mut connection).await.unwrap();
    assert!(matches!(
        owner.stop().await.err(),
        Some(StdioGenerationError::Store(_))
    ));
    assert_eq!(test_kill_process(process), Err(Errno::SRCH));
    let recorded = store.get_mcp_instance(&key).await.unwrap().unwrap();
    assert_eq!(recorded.generation, ready.generation);
    assert_eq!(recorded.observed, McpObservedState::Ready);
    // The stale ready snapshot is not proof of a live process or permission to
    // restart. This uncertain journal state requires recovery reconciliation.
    let mut replacement = StdioGeneration::spawn(store, launch(key, directory.path(), READY));
    assert_eq!(
        replacement.wait_ready().await.err(),
        Some(StdioGenerationError::Existing)
    );
}

#[tokio::test]
async fn registry_reuses_one_owner_and_rejects_changed_bindings_and_definitions() {
    let directory = tempfile::tempdir().unwrap();
    let (store, key) = setup(directory.path()).await;
    let registry = StdioRegistry::new(store.clone(), NonZeroUsize::new(1).unwrap());
    let (first, second) = tokio::join!(
        registry.ensure_ready(
            launch(key.clone(), directory.path(), READY),
            1.try_into().unwrap()
        ),
        registry.ensure_ready(
            launch(key.clone(), directory.path(), "exit 99"),
            1.try_into().unwrap()
        ),
    );
    assert_eq!(first.unwrap().generation, second.unwrap().generation);
    let process = pid(directory.path()).await;
    assert_eq!(
        registry
            .ensure_ready(
                launch(key.clone(), directory.path(), READY),
                2.try_into().unwrap()
            )
            .await
            .err(),
        Some(StdioRegistryError::BindingChanged)
    );
    store
        .register_mcp_definition(&definition(false), 1, "disable", limits())
        .await
        .unwrap();
    assert_eq!(
        registry
            .ensure_ready(
                launch(key.clone(), directory.path(), READY),
                1.try_into().unwrap()
            )
            .await
            .err(),
        Some(StdioRegistryError::Generation(StdioGenerationError::Store(
            McpInstanceError::DefinitionChanged
        )))
    );
    registry.stop(&key).await.unwrap().unwrap();
    assert_eq!(test_kill_process(process), Err(Errno::SRCH));
    let results = registry.shutdown().await;
    assert_eq!(results.len(), 1);
    assert!(results[0].is_ok());
    assert_eq!(
        registry
            .ensure_ready(launch(key, directory.path(), READY), 1.try_into().unwrap())
            .await
            .err(),
        Some(StdioRegistryError::Closed)
    );
}

#[tokio::test]
async fn cancelled_registry_waiter_preserves_owner_and_shutdown_can_be_awaited_again() {
    let directory = tempfile::tempdir().unwrap();
    let (store, key) = setup(directory.path()).await;
    let registry = Arc::new(StdioRegistry::new(
        store.clone(),
        NonZeroUsize::new(1).unwrap(),
    ));
    let mut inputs = launch(
        key.clone(),
        directory.path(),
        "printf '%s' \"$$\" > \"$PID_FILE\"; exec /bin/sleep 60",
    );
    // Grace keeps cleanup pending so cancellation of the first shutdown waiter
    // exercises the retained lifecycle worker, without a product timing default.
    inputs.process.shutdown_grace = Duration::from_secs(1);
    let waiter = tokio::spawn({
        let registry = registry.clone();
        async move { registry.ensure_ready(inputs, 1.try_into().unwrap()).await }
    });
    let process = pid(directory.path()).await;
    waiter.abort();
    assert!(waiter.await.err().unwrap().is_cancelled());
    assert!(test_kill_process(process).is_ok());
    assert!(
        tokio::time::timeout(Duration::from_millis(10), registry.shutdown())
            .await
            .is_err()
    );
    let results = registry.shutdown().await;
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].as_ref().unwrap().observed,
        McpObservedState::Stopped
    );
    assert_eq!(test_kill_process(process), Err(Errno::SRCH));
    assert_eq!(
        store
            .get_mcp_instance(&key)
            .await
            .unwrap()
            .unwrap()
            .observed,
        McpObservedState::Stopped
    );
}

#[tokio::test]
async fn registry_capacity_bounds_live_owners_and_finished_scope_releases_slot() {
    let directory = tempfile::tempdir().unwrap();
    let (store, key) = setup(directory.path()).await;
    let mut input = definition(true).server().clone();
    input.id = SharedConfigurationKey::parse("second", 64).unwrap();
    let second = McpServerDefinition::new(
        input,
        McpProtocolPolicy::Pinned(McpProtocolVersion::V20251125),
        McpLifecycleScope::Core,
        None,
        limits(),
    )
    .unwrap();
    store
        .register_mcp_definition(&second, 0, "second", limits())
        .await
        .unwrap();
    let second_key = McpInstanceKey::new(&second, McpInstanceOwner::Core, 4096).unwrap();
    let registry = StdioRegistry::new(store, NonZeroUsize::new(1).unwrap());
    let first = registry
        .ensure_ready(
            launch(key.clone(), directory.path(), READY),
            1.try_into().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        registry
            .ensure_ready(
                launch(second_key.clone(), directory.path(), READY),
                1.try_into().unwrap()
            )
            .await
            .err(),
        Some(StdioRegistryError::Capacity)
    );
    registry.stop(&key).await.unwrap();
    let second = registry
        .ensure_ready(
            launch(second_key, directory.path(), READY),
            1.try_into().unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(first.generation, second.generation);
    let results = registry.shutdown().await;
    assert_eq!(results.len(), 1);
    assert!(results[0].is_ok());
}

#[tokio::test]
async fn durable_host_revision_is_checked_before_process_spawn() {
    let directory = tempfile::tempdir().unwrap();
    let (store, key) = setup(directory.path()).await;
    let instance = KilnInstanceId::from_ulid(ulid::Ulid::generate());
    let bindings = McpHostBindings::new(
        key.clone(),
        McpHostBindingInput {
            instance_id: instance.clone(),
            definition_version: 1,
            runtime_binding: SharedConfigurationKey::parse("runtime", 64).unwrap(),
            executable: "/bin/sh".into(),
            arguments: BTreeMap::new(),
            environment: BTreeMap::new(),
        },
        limits(),
    )
    .unwrap();
    store
        .publish_mcp_host_bindings(&bindings, 0, limits())
        .await
        .unwrap();
    store
        .publish_mcp_host_bindings(&bindings, 1, limits())
        .await
        .unwrap();
    let mut stale = launch(key.clone(), directory.path(), READY);
    stale.host_binding_version = Some(McpHostBindingVersion {
        instance_id: instance.clone(),
        revision: 1.try_into().unwrap(),
    });
    let mut stale = StdioGeneration::spawn(store.clone(), stale);
    assert_eq!(
        stale.wait_ready().await.err(),
        Some(StdioGenerationError::Store(
            McpInstanceError::BindingChanged
        ))
    );
    assert!(!directory.path().join("pid").exists());
    assert!(store.get_mcp_instance(&key).await.unwrap().is_none());

    let mut current = launch(key.clone(), directory.path(), READY);
    current.host_binding_version = Some(McpHostBindingVersion {
        instance_id: instance,
        revision: 2.try_into().unwrap(),
    });
    let registry = StdioRegistry::new(store.clone(), NonZeroUsize::new(1).unwrap());
    let record = registry
        .ensure_ready(current, 2.try_into().unwrap())
        .await
        .unwrap();
    assert_eq!(record.host_binding_version.unwrap().revision.get(), 2);
    let process = pid(directory.path()).await;
    assert_eq!(
        store
            .publish_mcp_host_bindings(&bindings, 2, limits())
            .await
            .err(),
        Some(McpHostBindingError::ActiveGeneration)
    );
    registry.stop(&key).await.unwrap();
    assert_eq!(test_kill_process(process), Err(Errno::SRCH));
    let third = store
        .publish_mcp_host_bindings(&bindings, 2, limits())
        .await
        .unwrap();
    let retired = store
        .retire_mcp_host_bindings(&key, third.revision, limits())
        .await
        .unwrap();
    std::fs::remove_file(directory.path().join("pid")).unwrap();
    for version in [
        None,
        Some(McpHostBindingVersion {
            instance_id: bindings.instance_id().clone(),
            revision: retired.revision,
        }),
    ] {
        let mut request = launch(key.clone(), directory.path(), READY);
        request.host_binding_version = version;
        let mut owner = StdioGeneration::spawn(store.clone(), request);
        assert_eq!(
            owner.wait_ready().await.err(),
            Some(StdioGenerationError::Store(
                McpInstanceError::BindingChanged
            ))
        );
        assert!(!directory.path().join("pid").exists());
        assert_eq!(
            store
                .get_mcp_instance(&key)
                .await
                .unwrap()
                .unwrap()
                .generation,
            record.generation
        );
    }
}
