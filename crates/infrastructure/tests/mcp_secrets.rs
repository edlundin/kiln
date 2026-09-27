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

fn snapshot(
    instance: &KilnInstanceId,
    key: &McpInstanceKey,
    argument: &SecretRef,
    token: &SecretRef,
) -> McpHostBindings {
    McpHostBindings::new(
        key.clone(),
        McpHostBindingInput {
            instance_id: instance.clone(),
            definition_version: 1,
            runtime_binding: name("python"),
            executable: "/usr/bin/python3".into(),
            arguments: BTreeMap::from([(name("argument"), argument.clone())]),
            environment: BTreeMap::from([(name("token"), token.clone())]),
        },
        limits(),
    )
    .unwrap()
}

#[tokio::test]
async fn publication_and_generation_claims_fence_rotation_and_preserve_secret_ownership() {
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
    let argument = SecretRef::from_ulid(Ulid::generate());
    let token = SecretRef::from_ulid(Ulid::generate());
    let next_token = SecretRef::from_ulid(Ulid::generate());
    let first = snapshot(&instance, &key, &argument, &token);
    assert_eq!(
        store
            .publish_mcp_host_bindings(&first, 0, limits())
            .await
            .err(),
        Some(McpHostBindingError::InvalidBinding)
    );
    for (reference, purpose, name) in [
        (&argument, McpSecretPurpose::Argument, "argument"),
        (&token, McpSecretPurpose::Environment, "token"),
        (&next_token, McpSecretPurpose::Environment, "token"),
    ] {
        store
            .reserve_mcp_secret(
                &binding(&instance, &key, reference, purpose, name),
                1,
                limits(),
            )
            .await
            .unwrap();
    }
    let first_record = store
        .publish_mcp_host_bindings(&first, 0, limits())
        .await
        .unwrap();
    assert_eq!(first_record.revision.get(), 1);
    let old_secret = binding(
        &instance,
        &key,
        &token,
        McpSecretPurpose::Environment,
        "token",
    );
    assert_eq!(
        store.retire_mcp_secret_reservation(&old_secret).await,
        Err(McpSecretJournalError::Conflict)
    );
    let pending = store
        .pending_mcp_secret_reservations(&instance, &key, NonZeroUsize::new(4).unwrap())
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].0.secret_ref(), &next_token);
    let host = McpHostBindingVersion {
        instance_id: instance.clone(),
        revision: first_record.revision,
    };
    assert_eq!(
        store
            .claim_mcp_instance(
                &key,
                1,
                &McpGenerationId::from_ulid(Ulid::generate()),
                limits()
            )
            .await
            .err(),
        Some(McpInstanceError::BindingChanged)
    );
    let claim = store
        .claim_mcp_instance_with_host_bindings(
            &key,
            1,
            &McpGenerationId::from_ulid(Ulid::generate()),
            Some(&host),
            limits(),
        )
        .await
        .unwrap();
    let McpInstanceClaim::Acquired(generation) = claim else {
        panic!("fresh generation")
    };
    let second = snapshot(&instance, &key, &argument, &next_token);
    assert_eq!(
        store
            .publish_mcp_host_bindings(&second, 1, limits())
            .await
            .err(),
        Some(McpHostBindingError::ActiveGeneration)
    );
    let generation = store
        .transition_mcp_instance(&generation, McpInstanceTransition::ConnectionLost)
        .await
        .unwrap();
    assert_eq!(
        store
            .publish_mcp_host_bindings(&second, 1, limits())
            .await
            .err(),
        Some(McpHostBindingError::ActiveGeneration)
    );
    store
        .transition_mcp_instance(&generation, McpInstanceTransition::Stopped)
        .await
        .unwrap();
    let mut connection = sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new().filename(data.path().join("kiln.sqlite3")),
    )
    .await
    .unwrap();
    sqlx::query("CREATE TRIGGER fixture_reject_snapshot BEFORE INSERT ON mcp_host_binding_refs BEGIN SELECT RAISE(ABORT, 'fixture rollback'); END")
        .execute(&mut connection).await.unwrap();
    assert_eq!(
        store
            .publish_mcp_host_bindings(&second, 1, limits())
            .await
            .err(),
        Some(McpHostBindingError::Unavailable)
    );
    assert_eq!(
        store
            .get_mcp_host_bindings(&key, limits())
            .await
            .unwrap()
            .unwrap()
            .revision
            .get(),
        1
    );
    assert_eq!(
        store.retire_mcp_secret_reservation(&old_secret).await,
        Err(McpSecretJournalError::Conflict)
    );
    sqlx::query("DROP TRIGGER fixture_reject_snapshot")
        .execute(&mut connection)
        .await
        .unwrap();
    drop(connection);
    let second_record = store
        .publish_mcp_host_bindings(&second, 1, limits())
        .await
        .unwrap();
    assert_eq!(second_record.revision.get(), 2);
    // A read-before-rotation startup cannot claim its stale resolved credentials.
    assert_eq!(
        store
            .claim_mcp_instance_with_host_bindings(
                &key,
                1,
                &McpGenerationId::from_ulid(Ulid::generate()),
                Some(&host),
                limits()
            )
            .await
            .err(),
        Some(McpInstanceError::BindingChanged)
    );
    assert_eq!(
        store
            .publish_mcp_host_bindings(&first, 0, limits())
            .await
            .unwrap()
            .revision
            .get(),
        1
    );
    assert_eq!(
        store
            .get_mcp_host_bindings(&key, limits())
            .await
            .unwrap()
            .unwrap()
            .revision
            .get(),
        2
    );
    let pending = store
        .pending_mcp_secret_reservations(&instance, &key, NonZeroUsize::new(4).unwrap())
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].0.secret_ref(), &token);
    assert_eq!(pending[0].1, McpSecretReservationState::Retired);
    store.finish_mcp_secret_deletion(&old_secret).await.unwrap();
    assert_eq!(
        store
            .publish_mcp_host_bindings(&first, 2, limits())
            .await
            .err(),
        Some(McpHostBindingError::InvalidBinding)
    );
    drop(store);
    let store = kiln_infrastructure::SqliteStore::open(data.path())
        .await
        .unwrap();
    let current = store
        .get_mcp_host_bindings(&key, limits())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.bindings.metadata_json(), second.metadata_json());
    let host = McpHostBindingVersion {
        instance_id: instance,
        revision: current.revision,
    };
    let claim = store
        .claim_mcp_instance_with_host_bindings(
            &key,
            1,
            &McpGenerationId::from_ulid(Ulid::generate()),
            Some(&host),
            limits(),
        )
        .await
        .unwrap();
    assert!(matches!(claim, McpInstanceClaim::Acquired(_)));
    let mut small = limits();
    small.max_metadata_bytes = 1;
    assert_eq!(
        store.get_mcp_host_bindings(&key, small).await.err(),
        Some(McpHostBindingError::LimitExceeded)
    );
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
