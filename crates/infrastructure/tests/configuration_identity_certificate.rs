use sqlx::Connection;

#[tokio::test]
async fn public_identity_certificate_export_requires_current_active_identity_and_budget() {
    use kiln_core::{
        ConfigurationAuthority, ConfigurationGroupId, ConfigurationIdentityStatusStore,
        ConfigurationMasterIdentityId, ConfigurationRole, ConfigurationStateError,
        ConfigurationStateStore, KilnInstanceId, SecretRef,
    };
    let data = tempfile::tempdir().unwrap();
    let store = kiln_infrastructure::SqliteStore::open(data.path())
        .await
        .unwrap();
    let instance = KilnInstanceId::from_ulid(ulid::Ulid::generate());
    let group = ConfigurationGroupId::from_ulid(ulid::Ulid::generate());
    let identity = ConfigurationMasterIdentityId::from_ulid(ulid::Ulid::generate());
    let initial = store
        .initialize_configuration_instance(instance.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .get_configuration_identity_certificate(&identity, 64)
            .await
            .unwrap(),
        None
    );
    let master = store
        .change_configuration_role(
            &initial,
            ConfigurationRole::Master(ConfigurationAuthority::new(group.clone(), instance.clone())),
        )
        .await
        .unwrap();
    // This port exports stored public bytes, not TLS readiness. No vault entry is
    // created; distinct certificate bytes and references catch wrong-column reads.
    let certificate = b"public-ca-der";
    {
        let mut connection = sqlx::SqliteConnection::connect_with(
            &sqlx::sqlite::SqliteConnectOptions::new().filename(data.path().join("kiln.sqlite3")),
        )
        .await
        .unwrap();
        sqlx::query("INSERT INTO configuration_master_identities (identity_id, ca_ref, tls_ref, group_id, master_instance_id, reserved_state_version, server_name, not_before, leaf_not_after, ca_not_after, ca_der, tls_der, ca_key_hash, tls_key_hash, status) VALUES (?, ?, ?, ?, ?, ?, 'master.example.com', 1, 2, 3, ?, ?, ?, ?, 'active')")
            .bind(identity.as_str())
            .bind(SecretRef::from_ulid(ulid::Ulid::generate()).as_str())
            .bind(SecretRef::from_ulid(ulid::Ulid::generate()).as_str())
            .bind(group.as_str()).bind(instance.as_str()).bind(master.version() as i64)
            .bind(certificate.as_slice()).bind(b"different-public-leaf".as_slice())
            .bind("0".repeat(64)).bind("1".repeat(64))
            .execute(&mut connection).await.unwrap();
    }
    assert_eq!(
        store
            .get_configuration_identity_certificate(&identity, certificate.len())
            .await
            .unwrap(),
        Some(certificate.to_vec())
    );
    assert_eq!(
        store
            .get_configuration_identity_certificate(&identity, certificate.len() - 1)
            .await,
        Err(ConfigurationStateError::IntegrityViolation)
    );
    let other = ConfigurationMasterIdentityId::from_ulid(ulid::Ulid::generate());
    assert_eq!(
        store
            .get_configuration_identity_certificate(&other, 64)
            .await
            .unwrap(),
        None
    );
    {
        let mut connection = sqlx::SqliteConnection::connect_with(
            &sqlx::sqlite::SqliteConnectOptions::new().filename(data.path().join("kiln.sqlite3")),
        )
        .await
        .unwrap();
        sqlx::query(
            "UPDATE configuration_master_identities SET status = 'pending' WHERE identity_id = ?",
        )
        .bind(identity.as_str())
        .execute(&mut connection)
        .await
        .unwrap();
    }
    assert_eq!(
        store
            .get_configuration_identity_certificate(&identity, 64)
            .await
            .unwrap(),
        None
    );
    {
        let mut connection = sqlx::SqliteConnection::connect_with(
            &sqlx::sqlite::SqliteConnectOptions::new().filename(data.path().join("kiln.sqlite3")),
        )
        .await
        .unwrap();
        sqlx::query(
            "UPDATE configuration_master_identities SET status = 'active' WHERE identity_id = ?",
        )
        .bind(identity.as_str())
        .execute(&mut connection)
        .await
        .unwrap();
    }
    store
        .change_configuration_role(&master, ConfigurationRole::Unassigned)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_configuration_identity_certificate(&identity, 64)
            .await
            .unwrap(),
        None
    );
}
