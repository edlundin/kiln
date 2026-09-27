use kiln_core::{
    McpDefinitionError, McpDefinitionLimits, McpDefinitionStore, McpLifecycleScope,
    McpProtocolPolicy, McpProtocolVersion, McpServerDefinition, SharedConfigurationKey,
    SharedMcpServerInput, SharedMcpTransport,
};
use sqlx::Connection;

fn limits() -> McpDefinitionLimits {
    // Resource bounds for this small fixture, not product defaults.
    McpDefinitionLimits {
        max_key_bytes: 64,
        max_metadata_bytes: 2048,
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
            transport: SharedMcpTransport::Https {
                endpoint: "https://mcp.example.test/".into(),
                credential_binding: None,
            },
        },
        McpProtocolPolicy::Auto,
        McpLifecycleScope::WorkspaceCheckout,
        None,
        limits(),
    )
    .unwrap()
}

#[test]
fn definitions_reject_drafts_and_unrecognized_or_noncanonical_metadata() {
    let definition = definition(true);
    let bytes = definition.metadata_json().as_bytes();
    assert!(McpServerDefinition::from_metadata_json(bytes, limits()).is_ok());
    assert!(McpProtocolVersion::parse("draft").is_err());
    for altered in [
        definition
            .metadata_json()
            .replace("\"local\"", "\"shared\""),
        definition
            .metadata_json()
            .replace("\"auto\"", "\"2099-01-01\""),
        definition
            .metadata_json()
            .replace("kiln_mediated_serial", "server_trusted"),
        definition
            .metadata_json()
            .replacen('{', "{\"extra\":true,", 1),
        definition
            .metadata_json()
            .replacen('{', "{\"source\":\"local\",", 1),
    ] {
        assert!(McpServerDefinition::from_metadata_json(altered.as_bytes(), limits()).is_err());
    }
}

#[tokio::test]
async fn registration_is_atomic_versioned_and_exact_retries_do_not_restore_old_state() {
    let data = tempfile::tempdir().unwrap();
    let store = kiln_infrastructure::SqliteStore::open(data.path())
        .await
        .unwrap();
    let initial = definition(true);
    assert!(
        store
            .get_mcp_definition(initial.id(), limits())
            .await
            .unwrap()
            .is_none()
    );
    let first = store
        .register_mcp_definition(&initial, 0, "first", limits())
        .await
        .unwrap();
    assert_eq!(first.version, 1);
    assert_eq!(
        store
            .register_mcp_definition(&initial, 0, "first", limits())
            .await
            .unwrap()
            .version,
        1
    );
    assert_eq!(
        store
            .register_mcp_definition(&initial, 0, "stale", limits())
            .await
            .err(),
        Some(McpDefinitionError::Conflict)
    );

    let disabled = definition(false);
    assert_eq!(
        store
            .register_mcp_definition(&disabled, 1, "disable", limits())
            .await
            .unwrap()
            .version,
        2
    );
    assert_eq!(
        store
            .register_mcp_definition(&disabled, 0, "first", limits())
            .await
            .err(),
        Some(McpDefinitionError::IdempotencyConflict)
    );
    assert_eq!(
        store
            .register_mcp_definition(&initial, 1, "first", limits())
            .await
            .err(),
        Some(McpDefinitionError::IdempotencyConflict)
    );
    drop(store);
    let store = kiln_infrastructure::SqliteStore::open(data.path())
        .await
        .unwrap();
    assert_eq!(
        store
            .register_mcp_definition(&initial, 0, "first", limits())
            .await
            .unwrap()
            .version,
        1
    );
    let current = store
        .get_mcp_definition(initial.id(), limits())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.version, 2);
    assert!(!current.definition.server().enabled);
    let mut small = limits();
    small.max_metadata_bytes = 1;
    assert_eq!(
        store.get_mcp_definition(initial.id(), small).await.err(),
        Some(McpDefinitionError::LimitExceeded)
    );

    let mut connection = sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new().filename(data.path().join("kiln.sqlite3")),
    )
    .await
    .unwrap();
    let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mcp_definition_events")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(events, 2);
    assert!(
        sqlx::query("UPDATE mcp_definition_versions SET metadata_json = '{}' WHERE version = 1")
            .execute(&mut connection)
            .await
            .is_err()
    );
    sqlx::query("CREATE TRIGGER fixture_reject_mcp_event BEFORE INSERT ON mcp_definition_events BEGIN SELECT RAISE(ABORT, 'fixture rollback'); END").execute(&mut connection).await.unwrap();
    assert_eq!(
        store
            .register_mcp_definition(&initial, 2, "re-enable", limits())
            .await
            .err(),
        Some(McpDefinitionError::Unavailable)
    );
    assert_eq!(
        store
            .get_mcp_definition(initial.id(), limits())
            .await
            .unwrap()
            .unwrap()
            .version,
        2
    );
    let versions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mcp_definition_versions")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    let commands: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mcp_definition_commands")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!((versions, commands), (2, 2));
}
