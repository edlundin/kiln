use sqlx::{Connection, SqliteConnection};

#[tokio::test]
async fn http_credential_upgrade_preserves_receipts_publication_and_guards() {
    let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    let migrations = sqlx::migrate!("./migrations");
    migrations.run_to(57, &mut connection).await.unwrap();
    sqlx::raw_sql(
        "INSERT INTO mcp_secret_reservations VALUES
        ('published', 'local', 'scope', 'token', 'environment', 1, 'reserved'),
        ('retired', 'local', 'scope', 'old', 'argument', 1, 'retired'),
        ('deleted', 'local', 'scope', 'gone', 'environment', 1, 'deleted');
        INSERT INTO mcp_host_binding_versions(instance_key, revision, metadata_json) VALUES ('scope', 1, '{}');
        INSERT INTO mcp_host_bindings VALUES ('scope', 1);
        INSERT INTO mcp_host_binding_refs VALUES ('scope', 'environment', 'token', 'published');",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    let before: Vec<(String, String, String, String, String, i64, String)> =
        sqlx::query_as("SELECT * FROM mcp_secret_reservations ORDER BY secret_ref")
            .fetch_all(&mut connection)
            .await
            .unwrap();
    migrations.run(&mut connection).await.unwrap();
    let after: Vec<(String, String, String, String, String, i64, String)> =
        sqlx::query_as("SELECT * FROM mcp_secret_reservations ORDER BY secret_ref")
            .fetch_all(&mut connection)
            .await
            .unwrap();
    assert_eq!(before, after);
    let refs: Vec<(String, String, String, String)> =
        sqlx::query_as("SELECT * FROM mcp_host_binding_refs")
            .fetch_all(&mut connection)
            .await
            .unwrap();
    assert_eq!(
        refs,
        [(
            "scope".into(),
            "environment".into(),
            "token".into(),
            "published".into()
        )]
    );
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&mut connection)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
            .fetch_one(&mut connection)
            .await
            .unwrap(),
        1
    );
    sqlx::raw_sql("INSERT INTO mcp_secret_reservations VALUES ('http', 'local', 'scope', 'auth', 'http_credential', 1, 'reserved');
        INSERT INTO mcp_host_binding_refs VALUES ('scope', 'http_credential', 'auth', 'http');")
        .execute(&mut connection).await.unwrap();
    for statement in [
        "UPDATE mcp_secret_reservations SET state = 'retired' WHERE secret_ref = 'published'",
        "UPDATE mcp_secret_reservations SET state = 'retired' WHERE secret_ref = 'http'",
        "UPDATE mcp_secret_reservations SET state = 'reserved' WHERE secret_ref = 'deleted'",
        "UPDATE mcp_secret_reservations SET purpose = 'http_credential' WHERE secret_ref = 'retired'",
        "DELETE FROM mcp_secret_reservations WHERE secret_ref = 'deleted'",
        "INSERT INTO mcp_secret_reservations VALUES ('bad', 'local', 'scope', 'auth', 'unknown', 1, 'reserved')",
        "INSERT INTO mcp_host_binding_refs VALUES ('scope', 'http_credential', 'missing', 'absent')",
    ] {
        assert!(
            sqlx::query(statement)
                .execute(&mut connection)
                .await
                .is_err(),
            "{statement}"
        );
    }
    sqlx::raw_sql(
        "DELETE FROM mcp_host_binding_refs WHERE secret_ref = 'http';
        UPDATE mcp_secret_reservations SET state = 'retired' WHERE secret_ref = 'http';
        UPDATE mcp_secret_reservations SET state = 'deleted' WHERE secret_ref = 'http';",
    )
    .execute(&mut connection)
    .await
    .unwrap();
}
