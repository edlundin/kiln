use sqlx::{Connection, SqliteConnection};

#[tokio::test]
async fn mcp_event_upgrade_preserves_referenced_history_and_cursor_high_water() {
    let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    let migrations = sqlx::migrate!("./migrations");
    migrations.run_to(56, &mut connection).await.unwrap();
    sqlx::raw_sql("INSERT INTO workspaces(workspace_id, name) VALUES('workspace', 'fixture');
        INSERT INTO sessions(session_id, workspace_id) VALUES('session', 'workspace');
        INSERT INTO runs(run_id, session_id, state) VALUES('run', 'session', 'completed');
        INSERT INTO session_events(event_id, session_id, event_type) VALUES('created', 'session', 'session.created');
        INSERT INTO session_events(event_id, session_id, event_type, run_id, run_state) VALUES('done', 'session', 'run.state_changed', 'run', 'completed');
        INSERT INTO messages(message_id, session_id, role, content, target_run_id, child_activity_run_id, child_activity_event_id)
            VALUES('message', 'session', 'user', 'reaction', 'run', 'run', 'done');
        INSERT INTO message_deliveries(message_id, run_id, delivery_mode, state, queued_cursor) VALUES('message', 'run', 'queued', 'delivered', 1);
        INSERT INTO context_manifests(context_manifest_id, session_id, run_id, content_hash, entry_count)
            VALUES('manifest', 'session', 'run', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1);
        INSERT INTO context_manifest_entries(context_manifest_id, position, entry_kind, provenance, source_run_id, source_event_id, message_id, content)
            VALUES('manifest', 0, 'child_activity', 'child_activity', 'run', 'done', 'message', 'snapshot');
        UPDATE sqlite_sequence SET seq=500 WHERE name='session_events';")
        .execute(&mut connection).await.unwrap();
    migrations.run(&mut connection).await.unwrap();
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
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA defer_foreign_keys")
            .fetch_one(&mut connection)
            .await
            .unwrap(),
        0
    );
    let history: Vec<(i64, String, String)> =
        sqlx::query_as("SELECT cursor,event_id,event_type FROM session_events ORDER BY cursor")
            .fetch_all(&mut connection)
            .await
            .unwrap();
    assert_eq!(
        history,
        [
            (1, "created".into(), "session.created".into()),
            (2, "done".into(), "run.state_changed".into())
        ]
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT child_activity_event_id FROM messages")
            .fetch_one(&mut connection)
            .await
            .unwrap(),
        "done"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT source_event_id FROM context_manifest_entries")
            .fetch_one(&mut connection)
            .await
            .unwrap(),
        "done"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT queued_cursor FROM message_deliveries")
            .fetch_one(&mut connection)
            .await
            .unwrap(),
        1
    );
    let cursor: i64 = sqlx::query_scalar("INSERT INTO session_events(event_id,session_id,event_type) VALUES('next','session','session.created') RETURNING cursor").fetch_one(&mut connection).await.unwrap();
    assert_eq!(cursor, 501);
}
