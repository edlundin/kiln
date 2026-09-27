use kiln_core::*;
use sqlx::Connection;
use std::{collections::BTreeMap, num::NonZeroUsize};
use ulid::Ulid;

fn limits() -> McpDefinitionLimits {
    McpDefinitionLimits {
        max_key_bytes: 64,
        max_metadata_bytes: 2048,
        max_arguments: 4,
        max_argument_bytes: 64,
        max_environment: 4,
        max_endpoint_bytes: 128,
    }
}
fn name(value: &str) -> SharedConfigurationKey {
    SharedConfigurationKey::parse(value, 64).unwrap()
}
fn definition(enabled: bool) -> McpServerDefinition {
    McpServerDefinition::new(
        SharedMcpServerInput {
            id: name("fixture"),
            enabled,
            transport: SharedMcpTransport::Stdio {
                runtime_binding: name("python"),
                arguments: vec![SharedMcpArgument::HostBinding(name("argument"))],
                environment: BTreeMap::from([("TOKEN".into(), name("token"))]),
            },
        },
        McpProtocolPolicy::Auto,
        McpLifecycleScope::Core,
        None,
        limits(),
    )
    .unwrap()
}
fn binding(
    instance: &KilnInstanceId,
    key: &McpInstanceKey,
    reference: &SecretRef,
    purpose: McpSecretPurpose,
    field: &str,
) -> McpSecretBinding {
    McpSecretBinding::new(
        instance.clone(),
        key.clone(),
        name(field),
        purpose,
        reference.clone(),
    )
}

#[tokio::test]
async fn reservation_receipts_survive_restart_and_deletion_without_reauthorizing_writes() {
    let data = tempfile::tempdir().unwrap();
    let store = kiln_infrastructure::SqliteStore::open(data.path())
        .await
        .unwrap();
    let definition = definition(true);
    store
        .register_mcp_definition(&definition, 0, "initial", limits())
        .await
        .unwrap();
    let key = McpInstanceKey::new(&definition, McpInstanceOwner::Core, 2048).unwrap();
    let instance = KilnInstanceId::from_ulid(Ulid::generate());
    let reference = SecretRef::from_ulid(Ulid::generate());
    let secret = binding(
        &instance,
        &key,
        &reference,
        McpSecretPurpose::Environment,
        "token",
    );
    assert!(matches!(
        store
            .reserve_mcp_secret(&secret, 1, limits())
            .await
            .unwrap(),
        McpSecretReservation::Fresh
    ));
    assert!(matches!(
        store
            .reserve_mcp_secret(&secret, 1, limits())
            .await
            .unwrap(),
        McpSecretReservation::Existing(McpSecretReservationState::Reserved)
    ));
    assert_eq!(
        store.finish_mcp_secret_deletion(&secret).await,
        Err(McpSecretJournalError::Conflict)
    );
    for other in [
        binding(
            &KilnInstanceId::from_ulid(Ulid::generate()),
            &key,
            &reference,
            McpSecretPurpose::Environment,
            "token",
        ),
        binding(
            &instance,
            &key,
            &reference,
            McpSecretPurpose::Argument,
            "argument",
        ),
    ] {
        assert_eq!(
            store.reserve_mcp_secret(&other, 1, limits()).await.err(),
            Some(McpSecretJournalError::Conflict)
        );
        assert_eq!(
            store.retire_mcp_secret_reservation(&other).await,
            Err(McpSecretJournalError::Conflict)
        );
    }
    let second = binding(
        &instance,
        &key,
        &SecretRef::from_ulid(Ulid::generate()),
        McpSecretPurpose::Argument,
        "argument",
    );
    store
        .reserve_mcp_secret(&second, 1, limits())
        .await
        .unwrap();
    drop(store);
    let store = kiln_infrastructure::SqliteStore::open(data.path())
        .await
        .unwrap();
    // Reconciliation remains possible after disabling/revising the definition.
    store
        .register_mcp_definition(&self::definition(false), 1, "disable", limits())
        .await
        .unwrap();
    assert!(matches!(
        store
            .reserve_mcp_secret(&secret, 1, limits())
            .await
            .unwrap(),
        McpSecretReservation::Existing(McpSecretReservationState::Reserved)
    ));
    let batch = NonZeroUsize::new(1).unwrap();
    for _ in 0..2 {
        let pending = store
            .pending_mcp_secret_reservations(&instance, &key, batch)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        let item = &pending[0].0;
        assert_eq!(
            store.retire_mcp_secret_reservation(item).await.unwrap(),
            McpSecretReservationState::Retired
        );
        store.finish_mcp_secret_deletion(item).await.unwrap();
        store.finish_mcp_secret_deletion(item).await.unwrap();
        assert_eq!(
            store.retire_mcp_secret_reservation(item).await.unwrap(),
            McpSecretReservationState::Deleted
        );
        assert!(matches!(
            store.reserve_mcp_secret(item, 1, limits()).await.unwrap(),
            McpSecretReservation::Existing(McpSecretReservationState::Deleted)
        ));
    }
    assert!(
        store
            .pending_mcp_secret_reservations(&instance, &key, batch)
            .await
            .unwrap()
            .is_empty()
    );
    let mut connection = sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new().filename(data.path().join("kiln.sqlite3")),
    )
    .await
    .unwrap();
    for statement in [
        "DELETE FROM mcp_secret_reservations",
        "UPDATE mcp_secret_reservations SET state = 'reserved'",
        "UPDATE mcp_secret_reservations SET binding_name = 'other'",
    ] {
        assert!(
            sqlx::query(statement)
                .execute(&mut connection)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn reservations_validate_definition_role_and_budget_before_creating_ownership() {
    let data = tempfile::tempdir().unwrap();
    let store = kiln_infrastructure::SqliteStore::open(data.path())
        .await
        .unwrap();
    let definition = definition(true);
    store
        .register_mcp_definition(&definition, 0, "initial", limits())
        .await
        .unwrap();
    let key = McpInstanceKey::new(&definition, McpInstanceOwner::Core, 2048).unwrap();
    let instance = KilnInstanceId::from_ulid(Ulid::generate());
    let reference = SecretRef::from_ulid(Ulid::generate());
    let wrong_role = binding(
        &instance,
        &key,
        &reference,
        McpSecretPurpose::Argument,
        "token",
    );
    assert_eq!(
        store
            .reserve_mcp_secret(&wrong_role, 1, limits())
            .await
            .err(),
        Some(McpSecretJournalError::InvalidBinding)
    );
    let secret = binding(
        &instance,
        &key,
        &reference,
        McpSecretPurpose::Environment,
        "token",
    );
    assert_eq!(
        store.reserve_mcp_secret(&secret, 2, limits()).await.err(),
        Some(McpSecretJournalError::DefinitionChanged)
    );
    let mut small = limits();
    small.max_metadata_bytes = 1;
    assert_eq!(
        store.reserve_mcp_secret(&secret, 1, small).await.err(),
        Some(McpSecretJournalError::LimitExceeded)
    );
    assert!(
        store
            .pending_mcp_secret_reservations(&instance, &key, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        store
            .reserve_mcp_secret(&secret, 1, limits())
            .await
            .unwrap(),
        McpSecretReservation::Fresh
    ));
}
