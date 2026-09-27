use kiln_core::*;
use sqlx::Connection;
use std::{collections::BTreeMap, num::NonZeroUsize};

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
fn definition(
    id: &str,
    scope: McpLifecycleScope,
    profile: Option<&str>,
    enabled: bool,
) -> McpServerDefinition {
    McpServerDefinition::new(
        SharedMcpServerInput {
            id: SharedConfigurationKey::parse(id, 64).unwrap(),
            enabled,
            transport: SharedMcpTransport::Stdio {
                runtime_binding: SharedConfigurationKey::parse("runtime", 64).unwrap(),
                arguments: vec![],
                environment: BTreeMap::new(),
            },
        },
        McpProtocolPolicy::Pinned(McpProtocolVersion::V20251125),
        scope,
        profile.map(|v| SharedConfigurationKey::parse(v, 64).unwrap()),
        limits(),
    )
    .unwrap()
}
fn generation() -> McpGenerationId {
    McpGenerationId::from_ulid(ulid::Ulid::generate())
}
fn acquired(claim: McpInstanceClaim) -> McpInstanceRecord {
    match claim {
        McpInstanceClaim::Acquired(record) => record,
        _ => panic!("expected newly acquired generation"),
    }
}
fn existing(claim: McpInstanceClaim) -> McpInstanceRecord {
    match claim {
        McpInstanceClaim::Existing(record) => record,
        _ => panic!("retry must not acquire a generation"),
    }
}
async fn connection(path: &std::path::Path) -> sqlx::SqliteConnection {
    sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new().filename(path.join("kiln.sqlite3")),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn concurrent_claims_and_transition_retries_never_reacquire_active_generations() {
    let data = tempfile::tempdir().unwrap();
    let store = kiln_infrastructure::SqliteStore::open(data.path())
        .await
        .unwrap();
    let other = kiln_infrastructure::SqliteStore::open(data.path())
        .await
        .unwrap();
    let def = definition("fixture", McpLifecycleScope::Core, None, true);
    store
        .register_mcp_definition(&def, 0, "register", limits())
        .await
        .unwrap();
    let key = McpInstanceKey::new(&def, McpInstanceOwner::Core, 4096).unwrap();
    let first_id = generation();
    let second_id = generation();
    let (a, b) = tokio::join!(
        store.claim_mcp_instance(&key, 1, &first_id, limits()),
        other.claim_mcp_instance(&key, 1, &second_id, limits())
    );
    let (started, duplicate) = match (a.unwrap(), b.unwrap()) {
        (McpInstanceClaim::Acquired(a), McpInstanceClaim::Existing(b))
        | (McpInstanceClaim::Existing(b), McpInstanceClaim::Acquired(a)) => (a, b),
        _ => panic!("exactly one start may be acquired"),
    };
    assert_eq!(started.generation, duplicate.generation);
    assert_eq!(
        store
            .transition_mcp_instance(
                &started,
                McpInstanceTransition::Ready(McpProtocolVersion::V20260728)
            )
            .await
            .err(),
        Some(McpInstanceError::InvalidTransition)
    );
    let ready = store
        .transition_mcp_instance(
            &started,
            McpInstanceTransition::Ready(McpProtocolVersion::V20251125),
        )
        .await
        .unwrap();
    let retry = store
        .transition_mcp_instance(
            &started,
            McpInstanceTransition::Ready(McpProtocolVersion::V20251125),
        )
        .await
        .unwrap();
    assert_eq!(retry.state_version, ready.state_version);
    assert_eq!(
        store
            .transition_mcp_instance(&started, McpInstanceTransition::RequestStop)
            .await
            .err(),
        Some(McpInstanceError::Conflict)
    );
    let stopping = store
        .transition_mcp_instance(&ready, McpInstanceTransition::RequestStop)
        .await
        .unwrap();
    assert_eq!(
        existing(
            store
                .claim_mcp_instance(&key, 1, &generation(), limits())
                .await
                .unwrap()
        )
        .observed,
        McpObservedState::Stopping
    );
    let stopped = store
        .transition_mcp_instance(&stopping, McpInstanceTransition::Stopped)
        .await
        .unwrap();
    assert_eq!(stopped.desired, McpDesiredState::Stopped);
    assert_eq!(
        store
            .claim_mcp_instance(&key, 1, &started.generation, limits())
            .await
            .err(),
        Some(McpInstanceError::GenerationReused)
    );
    let replacement = acquired(
        store
            .claim_mcp_instance(&key, 1, &generation(), limits())
            .await
            .unwrap(),
    );
    assert_ne!(replacement.generation, started.generation);
    // Historical retries return receipts, not the current generation or a new start.
    let historical = store
        .transition_mcp_instance(
            &started,
            McpInstanceTransition::Ready(McpProtocolVersion::V20251125),
        )
        .await
        .unwrap();
    assert_eq!(historical.generation, started.generation);
    assert_eq!(
        store
            .get_mcp_instance(&key)
            .await
            .unwrap()
            .unwrap()
            .generation,
        replacement.generation
    );
    let mut sql = connection(data.path()).await;
    let starts: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM mcp_instance_events WHERE reason = 'start'")
            .fetch_one(&mut sql)
            .await
            .unwrap();
    assert_eq!(starts, 2);
}

#[tokio::test]
async fn checkout_and_profile_keys_isolate_owners_and_recheck_current_definition() {
    let data = tempfile::tempdir().unwrap();
    let store = kiln_infrastructure::SqliteStore::open(data.path())
        .await
        .unwrap();
    let workspace = WorkspaceId::from_ulid(ulid::Ulid::generate());
    let root = WorkspaceRootId::from_ulid(ulid::Ulid::generate());
    let mut sql = connection(data.path()).await;
    sqlx::query("INSERT INTO workspaces (workspace_id,name) VALUES (?, 'fixture')")
        .bind(workspace.as_str())
        .execute(&mut sql)
        .await
        .unwrap();
    // These are metadata fixtures only; no path is opened and no process starts.
    sqlx::query("INSERT INTO workspace_roots (workspace_root_id,workspace_id,name,display_path,canonical_path,git_common_directory_path,position,state,filesystem_identity) VALUES (?, ?, 'root', '/fixture', '/fixture', '/fixture/.git', 0, 'available', 'unix:1:2')")
        .bind(root.as_str()).bind(workspace.as_str()).execute(&mut sql).await.unwrap();
    let checkout = |relative: &str, identity: &str| {
        WorkspaceCheckout::from_resolved_paths(
            workspace.clone(),
            root.clone(),
            relative,
            "/fixture",
            "/fixture/.git",
            FilesystemIdentity::new(identity).unwrap(),
        )
        .unwrap()
    };
    let def = definition(
        "checkout",
        McpLifecycleScope::WorkspaceCheckout,
        Some("profile-a"),
        true,
    );
    store
        .register_mcp_definition(&def, 0, "register", limits())
        .await
        .unwrap();
    assert!(McpInstanceKey::new(&def, McpInstanceOwner::Core, 4096).is_err());
    let key = McpInstanceKey::new(
        &def,
        McpInstanceOwner::WorkspaceCheckout(checkout("", "unix:1:2")),
        4096,
    )
    .unwrap();
    let same = McpInstanceKey::new(
        &def,
        McpInstanceOwner::WorkspaceCheckout(checkout("", "unix:1:2")),
        4096,
    )
    .unwrap();
    let subdir = McpInstanceKey::new(
        &def,
        McpInstanceOwner::WorkspaceCheckout(checkout("subdir", "unix:1:2")),
        4096,
    )
    .unwrap();
    let changed_fs = McpInstanceKey::new(
        &def,
        McpInstanceOwner::WorkspaceCheckout(checkout("", "unix:1:3")),
        4096,
    )
    .unwrap();
    let start = acquired(
        store
            .claim_mcp_instance(&key, 1, &generation(), limits())
            .await
            .unwrap(),
    );
    assert_eq!(
        existing(
            store
                .claim_mcp_instance(&same, 1, &generation(), limits())
                .await
                .unwrap()
        )
        .generation,
        start.generation
    );
    assert_ne!(
        acquired(
            store
                .claim_mcp_instance(&subdir, 1, &generation(), limits())
                .await
                .unwrap()
        )
        .generation,
        start.generation
    );
    assert_eq!(
        store
            .claim_mcp_instance(&changed_fs, 1, &generation(), limits())
            .await
            .err(),
        Some(McpInstanceError::OwnerNotFound)
    );
    let changed = definition(
        "checkout",
        McpLifecycleScope::WorkspaceCheckout,
        Some("profile-b"),
        true,
    );
    store
        .register_mcp_definition(&changed, 1, "profile-change", limits())
        .await
        .unwrap();
    assert_eq!(
        store
            .claim_mcp_instance(&key, 1, &generation(), limits())
            .await
            .err(),
        Some(McpInstanceError::DefinitionChanged)
    );
    assert_eq!(
        store
            .claim_mcp_instance(&key, 2, &generation(), limits())
            .await
            .err(),
        Some(McpInstanceError::OwnerMismatch)
    );
    assert_eq!(
        store
            .transition_mcp_instance(
                &start,
                McpInstanceTransition::Ready(McpProtocolVersion::V20251125)
            )
            .await
            .err(),
        Some(McpInstanceError::DefinitionChanged)
    );
    let other = McpInstanceKey::new(&changed, key.owner().clone(), 4096).unwrap();
    assert_ne!(
        acquired(
            store
                .claim_mcp_instance(&other, 2, &generation(), limits())
                .await
                .unwrap()
        )
        .generation,
        start.generation
    );
    let disabled = definition(
        "checkout",
        McpLifecycleScope::WorkspaceCheckout,
        Some("profile-b"),
        false,
    );
    store
        .register_mcp_definition(&disabled, 2, "disable", limits())
        .await
        .unwrap();
    assert_eq!(
        store
            .claim_mcp_instance(&other, 3, &generation(), limits())
            .await
            .err(),
        Some(McpInstanceError::Disabled)
    );
}

#[tokio::test]
async fn restart_records_uncertainty_and_requires_cleanup_before_new_generation() {
    let data = tempfile::tempdir().unwrap();
    let store = kiln_infrastructure::SqliteStore::open(data.path())
        .await
        .unwrap();
    let def = definition("restart", McpLifecycleScope::Core, None, true);
    store
        .register_mcp_definition(&def, 0, "register", limits())
        .await
        .unwrap();
    let key = McpInstanceKey::new(&def, McpInstanceOwner::Core, 4096).unwrap();
    let start = acquired(
        store
            .claim_mcp_instance(&key, 1, &generation(), limits())
            .await
            .unwrap(),
    );
    drop(store);
    let store = kiln_infrastructure::SqliteStore::open(data.path())
        .await
        .unwrap();
    assert_eq!(
        store
            .interrupt_mcp_instances_after_restart(NonZeroUsize::new(1).unwrap())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .interrupt_mcp_instances_after_restart(NonZeroUsize::new(1).unwrap())
            .await
            .unwrap(),
        0
    );
    let interrupted = existing(
        store
            .claim_mcp_instance(&key, 1, &generation(), limits())
            .await
            .unwrap(),
    );
    assert_eq!(interrupted.generation, start.generation);
    assert_eq!(
        (interrupted.desired, interrupted.observed),
        (McpDesiredState::Running, McpObservedState::Interrupted)
    );
    assert_eq!(
        store
            .transition_mcp_instance(
                &start,
                McpInstanceTransition::Ready(McpProtocolVersion::V20251125)
            )
            .await
            .err(),
        Some(McpInstanceError::Conflict)
    );
    // A trusted lifecycle adapter must verify actual cleanup before reporting Stopped.
    store
        .transition_mcp_instance(&interrupted, McpInstanceTransition::Stopped)
        .await
        .unwrap();
    let mut sql = connection(data.path()).await;
    sqlx::query("CREATE TRIGGER fixture_mcp_event_failure BEFORE INSERT ON mcp_instance_events BEGIN SELECT RAISE(ABORT, 'fixture rollback'); END").execute(&mut sql).await.unwrap();
    let next = generation();
    assert_eq!(
        store
            .claim_mcp_instance(&key, 1, &next, limits())
            .await
            .err(),
        Some(McpInstanceError::Unavailable)
    );
    assert_eq!(
        store
            .get_mcp_instance(&key)
            .await
            .unwrap()
            .unwrap()
            .generation,
        start.generation
    );
    sqlx::query("DROP TRIGGER fixture_mcp_event_failure")
        .execute(&mut sql)
        .await
        .unwrap();
    assert_eq!(
        acquired(
            store
                .claim_mcp_instance(&key, 1, &next, limits())
                .await
                .unwrap()
        )
        .generation,
        next
    );
    let interrupted_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM mcp_instance_events WHERE reason = 'daemon_restart'",
    )
    .fetch_one(&mut sql)
    .await
    .unwrap();
    assert_eq!(interrupted_events, 1);
}
