use sqlx::{Connection, SqliteConnection};

#[tokio::test]
async fn mcp_input_event_upgrade_preserves_invocation_links_and_input_history() {
    let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    let migrations = sqlx::migrate!("./migrations");
    migrations.run_to(59, &mut connection).await.unwrap();
    sqlx::raw_sql("INSERT INTO workspaces(workspace_id, name) VALUES('workspace', 'fixture');
        INSERT INTO sessions(session_id, workspace_id) VALUES('session', 'workspace');
        INSERT INTO runs(run_id, session_id, state) VALUES('run', 'session', 'completed');
        INSERT INTO tool_calls(tool_call_id, run_id, capability, state) VALUES('tool', 'run', 'mcp.call', 'completed');
        INSERT INTO mcp_definition_versions VALUES('fixture',1,'{}');
        INSERT INTO mcp_definitions VALUES('fixture',1);
        INSERT INTO mcp_instance_generations(generation_id,instance_key,definition_id,definition_version,state_version,desired,observed)
            VALUES('mcg_01ARZ3NDEKTSV4RRFFQ69G5FAV','{\"definition_id\":\"fixture\"}','fixture',1,1,'stopped','interrupted');
        INSERT INTO mcp_invocations VALUES('tool','mcg_01ARZ3NDEKTSV4RRFFQ69G5FAV','completed');
        INSERT INTO mcp_invocation_events(tool_call_id,state) VALUES('tool','completed');
        INSERT INTO session_events(event_id,session_id,event_type,run_id,tool_call_id,mcp_invocation_sequence)
            VALUES('old','session','mcp.invocation_state_changed','run','tool',1);
        INSERT INTO mcp_inputs VALUES('tool',1,'roots','resolved');
        INSERT INTO mcp_input_events(tool_call_id,ordinal,state) VALUES('tool',1,'required'),('tool',1,'resolved');
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
        sqlx::query_scalar::<_, i64>(
            "SELECT mcp_invocation_sequence FROM session_events WHERE event_id='old'"
        )
        .fetch_one(&mut connection)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM mcp_input_events")
            .fetch_one(&mut connection)
            .await
            .unwrap(),
        2
    );
    let cursor: i64 = sqlx::query_scalar("INSERT INTO session_events(event_id,session_id,event_type,run_id,tool_call_id,mcp_input_sequence)
        VALUES('input','session','mcp.input_state_changed','run','tool',1) RETURNING cursor").fetch_one(&mut connection).await.unwrap();
    assert_eq!(cursor, 501);
    assert!(
        sqlx::query("UPDATE session_events SET mcp_invocation_sequence=1 WHERE event_id='input'")
            .execute(&mut connection)
            .await
            .is_err()
    );
}

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
