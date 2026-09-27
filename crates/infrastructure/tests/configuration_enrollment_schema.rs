use sqlx::{Connection, Row};

#[tokio::test]
async fn upgrade_from_43_preserves_local_enrollment_and_creates_master_journal() {
    let data = tempfile::tempdir().unwrap();
    let mut connection = sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new()
            .filename(data.path().join("kiln.sqlite3"))
            .create_if_missing(true)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    let mut prefix = sqlx::migrate!("./migrations");
    prefix
        .migrations
        .to_mut()
        .retain(|migration| migration.version <= 43);
    prefix.run(&mut connection).await.unwrap();
    let attempt = format!("cra_{:032x}", ulid::Ulid::generate().0);
    let follower = format!("ins_{}", ulid::Ulid::generate());
    let master = format!("ins_{}", ulid::Ulid::generate());
    let group = format!("cfg_{}", ulid::Ulid::generate());
    let reference = format!("sec_{}", ulid::Ulid::generate());
    sqlx::query("INSERT INTO configuration_follower_enrollment_requests (attempt_id, follower_instance_id, expected_state_version, group_id, master_instance_id, server_name, ca_der, ca_fingerprint, secret_ref, credential_digest) VALUES (?, ?, 1, ?, ?, 'master.example.com', x'010203', ?, ?, ?)")
        .bind(&attempt).bind(&follower).bind(&group).bind(&master)
        .bind("0".repeat(64)).bind(&reference).bind("1".repeat(64))
        .execute(&mut connection).await.unwrap();
    sqlx::query("INSERT INTO configuration_follower_enrollment_lifecycle (attempt_id, follower_instance_id, phase) VALUES (?, ?, 'reserved')")
        .bind(&attempt).bind(&follower).execute(&mut connection).await.unwrap();
    let _store = kiln_infrastructure::SqliteStore::open(data.path())
        .await
        .unwrap();
    let row = sqlx::query("SELECT r.secret_ref, r.ca_der, l.phase, c.state FROM configuration_follower_enrollment_requests r JOIN configuration_follower_enrollment_lifecycle l USING (attempt_id) JOIN configuration_follower_enrollment_credentials c USING (attempt_id) WHERE r.attempt_id = ?")
        .bind(&attempt).fetch_one(&mut connection).await.unwrap();
    assert_eq!(row.get::<String, _>("secret_ref"), reference);
    assert_eq!(row.get::<Vec<u8>, _>("ca_der"), [1, 2, 3]);
    assert_eq!(row.get::<String, _>("phase"), "reserved");
    assert_eq!(row.get::<String, _>("state"), "pending");
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM configuration_master_enrollment_requests")
            .fetch_one(&mut connection)
            .await
            .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn fresh_database_has_distinct_master_and_follower_enrollment_journals() {
    let data = tempfile::tempdir().unwrap();
    let _store = kiln_infrastructure::SqliteStore::open(data.path())
        .await
        .unwrap();
    let mut connection = sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new().filename(data.path().join("kiln.sqlite3")),
    )
    .await
    .unwrap();
    let follower = sqlx::query("PRAGMA table_info(configuration_follower_enrollment_requests)")
        .fetch_all(&mut connection)
        .await
        .unwrap();
    let master = sqlx::query("PRAGMA table_info(configuration_master_enrollment_requests)")
        .fetch_all(&mut connection)
        .await
        .unwrap();
    assert!(
        follower
            .iter()
            .any(|row| row.get::<String, _>("name") == "secret_ref")
    );
    assert!(
        !follower
            .iter()
            .any(|row| row.get::<String, _>("name") == "request_id")
    );
    assert!(
        master
            .iter()
            .any(|row| row.get::<String, _>("name") == "request_id")
    );
    assert!(
        !master
            .iter()
            .any(|row| row.get::<String, _>("name") == "secret_ref")
    );
    let follower_owner: String =
        sqlx::query("PRAGMA foreign_key_list(configuration_follower_enrollment_lifecycle)")
            .fetch_one(&mut connection)
            .await
            .unwrap()
            .get("table");
    assert_eq!(follower_owner, "configuration_follower_enrollment_requests");
    let master_references =
        sqlx::query("PRAGMA foreign_key_list(configuration_follower_enrollment_request_lifecycle)")
            .fetch_all(&mut connection)
            .await
            .unwrap();
    assert!(
        master_references
            .iter()
            .any(|row| row.get::<String, _>("table") == "configuration_master_enrollment_requests")
    );
    let violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut connection)
        .await
        .unwrap();
    assert!(violations.is_empty());
}
