use std::{path::Path, process::Command};

use kiln_core::{
    DETERMINISTIC_SUBPROCESS_CAPABILITY, EventCursor, EventId, Message, MessageId, MessageRole,
    Run, RunApplication, RunId, RunState, RunStore, Session, SessionEvent, SessionEventPayload,
    SessionId, SessionStore, StartRunDisposition, SubprocessOutput, ToolCall, ToolCallId,
    ToolCallResult, ToolCallState, Workspace, WorkspaceId, WorkspaceRoot, WorkspaceRootDiscovery,
    WorkspaceRootId, WorkspaceRootState, WorkspaceStore,
};
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
use tempfile::TempDir;

fn git_repository() -> (TempDir, std::path::PathBuf) {
    let directory = tempfile::tempdir().unwrap();
    let repository = directory.path().join("repository");
    std::fs::create_dir(&repository).unwrap();
    let output = Command::new("git")
        .args(["-C", repository.to_str().unwrap(), "init"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let output = Command::new("git")
        .args([
            "-C",
            repository.to_str().unwrap(),
            "-c",
            "user.name=Kiln tests",
            "-c",
            "user.email=kiln-tests@example.invalid",
            "-c",
            "commit.gpgSign=false",
            "commit",
            "--allow-empty",
            "--quiet",
            "-m",
            "initial",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    (directory, repository)
}

fn workspace(id: &str, roots: Vec<WorkspaceRoot>) -> Workspace {
    Workspace::new(
        WorkspaceId::parse(id).unwrap(),
        "Workspace".to_owned(),
        roots,
    )
    .unwrap()
}

fn root(id: &str, name: &str, position: usize, path: &str, common: &str) -> WorkspaceRoot {
    WorkspaceRoot::new(
        WorkspaceRootId::parse(id).unwrap(),
        name.to_owned(),
        path.to_owned(),
        path.to_owned(),
        common.to_owned(),
        position,
        WorkspaceRootState::Available,
    )
    .unwrap()
}

#[tokio::test]
async fn migrations_create_schema_and_round_trip_preserves_order_and_ids() {
    let data = tempfile::tempdir().unwrap();
    let store = super::SqliteStore::open(data.path()).await.unwrap();
    let value = workspace(
        "wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        vec![
            root(
                "wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
                "first",
                0,
                "/first",
                "/first/.git",
            ),
            root(
                "wrt_01ARZ3NDEKTSV4RRFFQ69G5FAW",
                "second",
                1,
                "/second",
                "/second/.git",
            ),
        ],
    );
    store.create_workspace(&value).await.unwrap();
    assert_eq!(store.get_workspace(value.id()).await.unwrap(), Some(value));
}

#[tokio::test]
async fn plain_folder_is_rejected() {
    let data = tempfile::tempdir().unwrap();
    let folder = data.path().join("plain");
    std::fs::create_dir(&folder).unwrap();
    let result = super::GitWorkspaceRootDiscovery.discover(&folder).await;
    assert_eq!(result, Err(kiln_core::RootDiscoveryError::NotGitRepository));
}

#[tokio::test]
async fn primary_and_linked_worktrees_share_common_directory() {
    let (_data, repository) = git_repository();
    let linked = repository.parent().unwrap().join("linked");
    let output = Command::new("git")
        .args([
            "-C",
            repository.to_str().unwrap(),
            "worktree",
            "add",
            "--detach",
            linked.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let discovery = super::GitWorkspaceRootDiscovery;
    let primary = discovery.discover(&repository).await.unwrap();
    let linked = discovery.discover(&linked).await.unwrap();
    assert_ne!(primary.canonical_path, linked.canonical_path);
    assert_eq!(
        primary.git_common_directory_path,
        linked.git_common_directory_path
    );
}

#[tokio::test]
async fn root_insert_failure_rolls_back_workspace_and_previous_roots() {
    let data = tempfile::tempdir().unwrap();
    let store = super::SqliteStore::open(data.path()).await.unwrap();
    let seed = workspace(
        "wsp_01ARZ3NDEKTSV4RRFFQ69G5FAW",
        vec![root(
            "wrt_01ARZ3NDEKTSV4RRFFQ69G5FAX",
            "existing",
            0,
            "/existing",
            "/existing/.git",
        )],
    );
    store.create_workspace(&seed).await.unwrap();
    let value = workspace(
        "wsp_01ARZ3NDEKTSV4RRFFQ69G5FAX",
        vec![
            root(
                "wrt_01ARZ3NDEKTSV4RRFFQ69G5FAY",
                "first",
                0,
                "/first",
                "/first/.git",
            ),
            root(
                "wrt_01ARZ3NDEKTSV4RRFFQ69G5FAX",
                "second",
                1,
                "/second",
                "/second/.git",
            ),
        ],
    );
    assert_eq!(
        store.create_workspace(&value).await,
        Err(kiln_core::StoreError::Unavailable)
    );
    assert_eq!(store.get_workspace(value.id()).await.unwrap(), None);
    assert_eq!(store.get_workspace(seed.id()).await.unwrap(), Some(seed));
}

#[tokio::test]
async fn malformed_persisted_id_is_rejected_on_read() {
    let data = tempfile::tempdir().unwrap();
    let store = super::SqliteStore::open(data.path()).await.unwrap();
    let workspace_id = WorkspaceId::parse("wsp_01ARZ3NDEKTSV4RRFFQ69G5FAZ").unwrap();
    {
        let mut connection = store.connection.lock().await;
        sqlx::query("INSERT INTO workspaces (workspace_id, name) VALUES (?, ?)")
            .bind(workspace_id.as_str())
            .bind("Corrupt")
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::query("INSERT INTO workspace_roots (workspace_root_id, workspace_id, name, display_path, canonical_path, git_common_directory_path, position, state) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            .bind("wrt_invalid")
            .bind(workspace_id.as_str())
            .bind("root")
            .bind("/root")
            .bind("/root")
            .bind("/root/.git")
            .bind(0_i64)
            .bind("available")
            .execute(&mut *connection)
            .await
            .unwrap();
    }
    assert_eq!(
        store.get_workspace(&workspace_id).await,
        Err(kiln_core::StoreError::Unavailable)
    );
}

#[tokio::test]
async fn nested_path_resolves_to_worktree_top_level() {
    let (_data, repository) = git_repository();
    let nested = repository.join("nested");
    std::fs::create_dir(&nested).unwrap();
    let result = super::GitWorkspaceRootDiscovery
        .discover(Path::new(&nested))
        .await
        .unwrap();
    assert_eq!(
        result.canonical_path,
        std::fs::canonicalize(repository).unwrap().to_str().unwrap()
    );
}

fn session(id: &str, workspace_id: &str) -> Session {
    Session::new(
        SessionId::parse(id).unwrap(),
        WorkspaceId::parse(workspace_id).unwrap(),
    )
}

fn message(id: &str, session_id: &str, content: &str) -> Message {
    Message::new(
        MessageId::parse(id).unwrap(),
        SessionId::parse(session_id).unwrap(),
        MessageRole::User,
        content.to_owned(),
    )
    .unwrap()
}

async fn seeded_store() -> (tempfile::TempDir, super::SqliteStore, WorkspaceId) {
    let data = tempfile::tempdir().unwrap();
    let store = super::SqliteStore::open(data.path()).await.unwrap();
    let value = workspace(
        "wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        vec![root(
            "wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "main",
            0,
            "/main",
            "/main/.git",
        )],
    );
    store.create_workspace(&value).await.unwrap();
    (data, store, value.id().clone())
}

#[tokio::test]
async fn session_migration_survives_reopen() {
    let (data, store, workspace_id) = seeded_store().await;
    let value = session("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV", workspace_id.as_str());
    let event = SessionEvent::session_created(
        EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
        value.id().clone(),
        workspace_id.clone(),
    );
    store.create_session(&value, &event).await.unwrap();
    drop(store);

    let reopened = super::SqliteStore::open(data.path()).await.unwrap();
    assert_eq!(
        reopened.get_session(value.id()).await.unwrap(),
        Some(value.clone())
    );
    assert_eq!(
        reopened
            .list_session_events(value.id(), EventCursor::zero())
            .await
            .unwrap()
            .events()
            .len(),
        1
    );
}

#[tokio::test]
async fn event_insert_failure_rolls_back_message_insert() {
    let (_data, store, workspace_id) = seeded_store().await;
    let value = session("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV", workspace_id.as_str());
    store
        .create_session(
            &value,
            &SessionEvent::session_created(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                value.id().clone(),
                workspace_id,
            ),
        )
        .await
        .unwrap();
    let first = message(
        "msg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        value.id().as_str(),
        "first",
    );
    store
        .append_message(
            &first,
            &SessionEvent::message_appended(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap(),
                first.clone(),
            ),
        )
        .await
        .unwrap();
    let second = message(
        "msg_01ARZ3NDEKTSV4RRFFQ69G5FAX",
        value.id().as_str(),
        "second",
    );
    assert_eq!(
        store
            .append_message(
                &second,
                &SessionEvent::message_appended(
                    EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap(),
                    second.clone(),
                ),
            )
            .await,
        Err(kiln_core::StoreError::Unavailable)
    );
    let page = store
        .list_session_events(value.id(), EventCursor::zero())
        .await
        .unwrap();
    assert_eq!(page.events().len(), 2);
    let mut connection = store.connection.lock().await;
    let message_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE message_id = ?")
            .bind(second.id().as_str())
            .fetch_one(&mut *connection)
            .await
            .unwrap();
    assert_eq!(message_count, 0);
}

#[tokio::test]
async fn event_insert_failure_rolls_back_session_insert() {
    let (_data, store, workspace_id) = seeded_store().await;
    let first = session("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV", workspace_id.as_str());
    let event_id = EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
    store
        .create_session(
            &first,
            &SessionEvent::session_created(
                event_id.clone(),
                first.id().clone(),
                workspace_id.clone(),
            ),
        )
        .await
        .unwrap();

    let second = session("ses_01ARZ3NDEKTSV4RRFFQ69G5FAW", workspace_id.as_str());
    assert_eq!(
        store
            .create_session(
                &second,
                &SessionEvent::session_created(event_id, second.id().clone(), workspace_id),
            )
            .await,
        Err(kiln_core::StoreError::Unavailable)
    );
    assert_eq!(store.get_session(second.id()).await.unwrap(), None);
    assert_eq!(
        store.current_event_cursor().await.unwrap(),
        Some(EventCursor::from_value(1))
    );
}

#[tokio::test]
async fn cursors_are_global_but_event_queries_are_session_filtered() {
    let (_data, store, workspace_id) = seeded_store().await;
    let first = session("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV", workspace_id.as_str());
    let second = session("ses_01ARZ3NDEKTSV4RRFFQ69G5FAW", workspace_id.as_str());
    store
        .create_session(
            &first,
            &SessionEvent::session_created(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                first.id().clone(),
                workspace_id.clone(),
            ),
        )
        .await
        .unwrap();
    store
        .create_session(
            &second,
            &SessionEvent::session_created(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap(),
                second.id().clone(),
                workspace_id,
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        store.current_event_cursor().await.unwrap(),
        Some(EventCursor::from_value(2))
    );

    let first_message = message(
        "msg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        first.id().as_str(),
        "hello",
    );
    store
        .append_message(
            &first_message,
            &SessionEvent::message_appended(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAX").unwrap(),
                first_message.clone(),
            ),
        )
        .await
        .unwrap();

    let first_page = store
        .list_session_events(first.id(), EventCursor::zero())
        .await
        .unwrap();
    assert_eq!(
        first_page
            .events()
            .iter()
            .map(|event| event.cursor().value())
            .collect::<Vec<_>>(),
        [1, 3]
    );
    assert_eq!(first_page.current_cursor(), EventCursor::from_value(3));
    assert_eq!(
        store
            .list_session_events(first.id(), EventCursor::from_value(1))
            .await
            .unwrap()
            .events()
            .len(),
        1
    );
    assert_eq!(
        store
            .list_session_events(second.id(), EventCursor::zero())
            .await
            .unwrap()
            .events()
            .iter()
            .map(|event| event.cursor().value())
            .collect::<Vec<_>>(),
        [2]
    );
}

#[tokio::test]
async fn malformed_persisted_event_is_rejected_on_read() {
    let (_data, store, workspace_id) = seeded_store().await;
    let value = session("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV", workspace_id.as_str());
    {
        let mut connection = store.connection.lock().await;
        sqlx::query("INSERT INTO sessions (session_id, workspace_id) VALUES (?, ?)")
            .bind(value.id().as_str())
            .bind(value.workspace_id().as_str())
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO session_events (event_id, session_id, event_type) VALUES (?, ?, ?)",
        )
        .bind("evt_invalid")
        .bind(value.id().as_str())
        .bind("session.created")
        .execute(&mut *connection)
        .await
        .unwrap();
    }
    assert_eq!(
        store
            .list_session_events(value.id(), EventCursor::zero())
            .await,
        Err(kiln_core::StoreError::Unavailable)
    );
}

async fn seeded_session() -> (tempfile::TempDir, super::SqliteStore, Session) {
    let (data, store, workspace_id) = seeded_store().await;
    let value = session("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV", workspace_id.as_str());
    store
        .create_session(
            &value,
            &SessionEvent::session_created(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                value.id().clone(),
                workspace_id,
            ),
        )
        .await
        .unwrap();
    (data, store, value)
}

#[tokio::test]
async fn run_migration_preserves_the_event_cursor_high_water_mark() {
    let data = tempfile::tempdir().unwrap();
    let database = data.path().join("migration.sqlite3");
    let options = SqliteConnectOptions::new()
        .filename(database)
        .create_if_missing(true)
        .foreign_keys(true);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::raw_sql(include_str!("../migrations/0001_workspaces.sql"))
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../migrations/0002_sessions.sql"))
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspaces (workspace_id, name) VALUES (?, 'Migration test')")
        .bind("wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO sessions (session_id, workspace_id) VALUES (?, ?)")
        .bind("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV")
        .bind("wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV")
        .execute(&mut connection)
        .await
        .unwrap();
    for event_id in [
        "evt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "evt_01ARZ3NDEKTSV4RRFFQ69G5FAW",
        "evt_01ARZ3NDEKTSV4RRFFQ69G5FAX",
    ] {
        sqlx::query(
            "INSERT INTO session_events (event_id, session_id, event_type) VALUES (?, ?, 'session.created')",
        )
        .bind(event_id)
        .bind("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV")
        .execute(&mut connection)
        .await
        .unwrap();
    }
    sqlx::query("DELETE FROM session_events WHERE cursor = 3")
        .execute(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT seq FROM sqlite_sequence WHERE name = 'session_events'",
        )
        .fetch_one(&mut connection)
        .await
        .unwrap(),
        3
    );

    sqlx::raw_sql(include_str!("../migrations/0003_runs.sql"))
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO session_events (event_id, session_id, event_type) VALUES (?, ?, 'session.created')",
    )
    .bind("evt_01ARZ3NDEKTSV4RRFFQ69G5FAY")
    .bind("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .execute(&mut connection)
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT cursor FROM session_events WHERE event_id = ?",)
            .bind("evt_01ARZ3NDEKTSV4RRFFQ69G5FAY")
            .fetch_one(&mut connection)
            .await
            .unwrap(),
        4
    );
}

#[tokio::test]
async fn run_creation_is_atomic_and_has_a_global_cursor_event() {
    let (_data, store, value) = seeded_session().await;
    let app = RunApplication::new(store, super::UlidIdGenerator);
    let mutation = app
        .start_root_run(value.id().clone(), "atomic-run".to_owned())
        .await
        .unwrap();
    assert_eq!(mutation.events.len(), 1);
    assert_eq!(mutation.events[0].cursor().value(), 2);
    assert_eq!(mutation.value.run().state(), RunState::Queued);
    assert!(matches!(
        mutation.events[0].payload(),
        SessionEventPayload::RunCreated {
            state: RunState::Queued,
            ..
        }
    ));
}

#[tokio::test]
async fn global_event_suffix_is_exclusive_and_ordered_across_sessions() {
    let (_data, store, workspace_id) = seeded_store().await;
    let first = session("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV", workspace_id.as_str());
    let second = session("ses_01ARZ3NDEKTSV4RRFFQ69G5FAW", workspace_id.as_str());
    store
        .create_session(
            &first,
            &SessionEvent::session_created(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                first.id().clone(),
                workspace_id.clone(),
            ),
        )
        .await
        .unwrap();
    store
        .create_session(
            &second,
            &SessionEvent::session_created(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap(),
                second.id().clone(),
                workspace_id,
            ),
        )
        .await
        .unwrap();
    let first_page = store.list_events_after(EventCursor::zero()).await.unwrap();
    assert_eq!(
        first_page
            .events()
            .iter()
            .map(|event| event.cursor().value())
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(first_page.current_cursor(), EventCursor::from_value(2));
    let suffix = store
        .list_events_after(EventCursor::from_value(1))
        .await
        .unwrap();
    assert_eq!(
        suffix
            .events()
            .iter()
            .map(|event| event.cursor().value())
            .collect::<Vec<_>>(),
        [2]
    );
    assert_eq!(suffix.current_cursor(), EventCursor::from_value(2));
}

#[tokio::test]
async fn idempotent_start_returns_original_run_before_and_after_terminal_completion() {
    let (data, store, value) = seeded_session().await;
    let app = RunApplication::new(store.clone(), super::UlidIdGenerator);
    let first = app
        .start_root_run(value.id().clone(), "same-key".to_owned())
        .await
        .unwrap();
    assert_eq!(first.disposition, StartRunDisposition::Created);
    let duplicate = app
        .start_root_run(value.id().clone(), "same-key".to_owned())
        .await
        .unwrap();
    assert_eq!(duplicate.disposition, StartRunDisposition::Duplicate);
    assert_eq!(duplicate.value, first.value);
    assert!(duplicate.events.is_empty());
    assert_eq!(
        store
            .list_session_events(value.id(), EventCursor::zero())
            .await
            .unwrap()
            .events()
            .iter()
            .filter(|event| matches!(event.payload(), SessionEventPayload::RunCreated { .. }))
            .count(),
        1
    );

    let run_id = first.value.run().run_id().clone();
    let running = app.begin_execution(run_id.clone()).await.unwrap();
    let tool_call_id = running.value.tool_calls()[0].tool_call_id().clone();
    app.begin_tool_call(tool_call_id.clone()).await.unwrap();
    app.finish_execution(
        run_id,
        tool_call_id,
        SubprocessOutput::success("out", "err", 0),
    )
    .await
    .unwrap();

    let reopened = super::SqliteStore::open(data.path()).await.unwrap();
    let reopened_app = RunApplication::new(reopened, super::UlidIdGenerator);
    let after_terminal = reopened_app
        .start_root_run(value.id().clone(), "same-key".to_owned())
        .await
        .unwrap();
    assert_eq!(after_terminal.disposition, StartRunDisposition::Duplicate);
    assert_eq!(
        after_terminal.value.run().run_id(),
        first.value.run().run_id()
    );
    assert_eq!(after_terminal.value.run().state(), RunState::Queued);
    assert!(after_terminal.value.tool_calls().is_empty());
}

#[tokio::test]
async fn failed_start_transaction_does_not_reserve_the_idempotency_key() {
    let (_data, store, session) = seeded_session().await;
    let rejected = Run::new(
        RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAY").unwrap(),
        session.id().clone(),
    );
    let duplicate_event_id = SessionEvent::run_created(
        EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
        &rejected,
    );

    assert_eq!(
        store
            .start_root_run(&rejected, &duplicate_event_id, "retryable-key")
            .await,
        Err(kiln_core::RunStoreError::Unavailable)
    );
    assert_eq!(store.get_run(rejected.run_id()).await.unwrap(), None);

    let accepted = Run::new(
        RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAZ").unwrap(),
        session.id().clone(),
    );
    let event = SessionEvent::run_created(
        EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAY").unwrap(),
        &accepted,
    );
    let mutation = store
        .start_root_run(&accepted, &event, "retryable-key")
        .await
        .unwrap();

    assert_eq!(mutation.disposition, StartRunDisposition::Created);
    assert_eq!(mutation.value.run().run_id(), accepted.run_id());
}

#[tokio::test]
async fn different_idempotency_key_keeps_active_root_conflict() {
    let (_data, store, value) = seeded_session().await;
    let app = RunApplication::new(store, super::UlidIdGenerator);
    app.start_root_run(value.id().clone(), "first-key".to_owned())
        .await
        .unwrap();
    assert_eq!(
        app.start_root_run(value.id().clone(), "second-key".to_owned())
            .await,
        Err(kiln_core::RunError::ActiveRootRunExists)
    );
}

#[tokio::test]
async fn only_one_active_root_run_is_allowed_per_session() {
    let (_data, store, value) = seeded_session().await;
    let app = RunApplication::new(store, super::UlidIdGenerator);
    app.start_root_run(value.id().clone(), "first-run".to_owned())
        .await
        .unwrap();
    assert_eq!(
        app.start_root_run(value.id().clone(), "second-run".to_owned())
            .await,
        Err(kiln_core::RunError::ActiveRootRunExists)
    );
}

#[tokio::test]
async fn run_store_rejects_state_machine_bypass_inputs() {
    let (_data, store, session) = seeded_session().await;
    let forged_id = RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAY").unwrap();
    let forged = Run::from_persisted(forged_id.clone(), session.id().clone(), RunState::Completed);
    let forged_event = SessionEvent::run_created(
        EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAY").unwrap(),
        &forged,
    );
    assert_eq!(
        store
            .start_root_run(&forged, &forged_event, "forged-run")
            .await,
        Err(kiln_core::RunStoreError::InvalidTransition)
    );
    assert_eq!(store.get_run(&forged_id).await.unwrap(), None);

    let app = RunApplication::new(store.clone(), super::UlidIdGenerator);
    let created = app
        .start_root_run(session.id().clone(), "valid-run".to_owned())
        .await
        .unwrap();
    let run_id = created.value.run().run_id().clone();
    let running_run = created.value.run().transition(RunState::Running).unwrap();
    let proposed_tool = ToolCall::new(
        ToolCallId::parse("tcl_01ARZ3NDEKTSV4RRFFQ69G5FAY").unwrap(),
        run_id.clone(),
        DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
    );
    let reversed_events = [
        SessionEvent::tool_call_requested(
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAZ").unwrap(),
            session.id().clone(),
            proposed_tool.clone(),
        ),
        SessionEvent::run_state_changed(
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB0").unwrap(),
            &running_run,
        ),
    ];
    assert_eq!(
        store
            .begin_execution(&running_run, &proposed_tool, &reversed_events)
            .await,
        Err(kiln_core::RunStoreError::InvalidTransition)
    );
    assert_eq!(
        store.get_run(&run_id).await.unwrap().unwrap().run().state(),
        RunState::Queued
    );

    let running = app.begin_execution(run_id.clone()).await.unwrap();
    let tool_call_id = running.value.tool_calls()[0].tool_call_id().clone();
    let requested_tool = running.value.tool_calls()[0].clone();
    let running_tool = requested_tool.transition(ToolCallState::Running).unwrap();
    let wrong_session_event = SessionEvent::tool_call_state_changed(
        EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB1").unwrap(),
        SessionId::parse("ses_01ARZ3NDEKTSV4RRFFQ69G5FAY").unwrap(),
        running_tool,
    );
    assert_eq!(
        store
            .begin_tool_call(&tool_call_id, &[wrong_session_event])
            .await,
        Err(kiln_core::RunStoreError::InvalidTransition)
    );

    let running_tool = app.begin_tool_call(tool_call_id.clone()).await.unwrap();
    let completed_run = running.value.run().transition(RunState::Completed).unwrap();
    let failed_result = ToolCallResult::new(
        ToolCallState::Failed,
        "out".to_owned(),
        "err".to_owned(),
        Some(7),
    )
    .unwrap();
    let failed_tool = running_tool.value.with_result(&failed_result).unwrap();
    let mismatched_events = [
        SessionEvent::tool_call_output(
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB2").unwrap(),
            session.id().clone(),
            run_id.clone(),
            tool_call_id.clone(),
            kiln_core::ToolOutputStream::Stdout,
            "out".to_owned(),
        ),
        SessionEvent::tool_call_output(
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB3").unwrap(),
            session.id().clone(),
            run_id.clone(),
            tool_call_id.clone(),
            kiln_core::ToolOutputStream::Stderr,
            "err".to_owned(),
        ),
        SessionEvent::tool_call_state_changed(
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB4").unwrap(),
            session.id().clone(),
            failed_tool.clone(),
        ),
        SessionEvent::run_state_changed(
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB5").unwrap(),
            &completed_run,
        ),
    ];
    assert_eq!(
        store
            .finish_execution(&completed_run, &failed_tool, &mismatched_events)
            .await,
        Err(kiln_core::RunStoreError::InvalidTransition)
    );
    let unchanged = store.get_run(&run_id).await.unwrap().unwrap();
    assert_eq!(unchanged.run().state(), RunState::Running);
    assert_eq!(unchanged.tool_calls()[0].state(), ToolCallState::Running);
}

#[tokio::test]
async fn run_transitions_persist_historical_events_and_results() {
    let (data, store, value) = seeded_session().await;
    let app = RunApplication::new(store.clone(), super::UlidIdGenerator);
    let created = app
        .start_root_run(value.id().clone(), "historical-run".to_owned())
        .await
        .unwrap();
    let run_id = created.value.run().run_id().clone();
    let running = app.begin_execution(run_id.clone()).await.unwrap();
    let tool_call_id = running.value.tool_calls()[0].tool_call_id().clone();
    assert_eq!(running.events.len(), 2);
    let tool_running = app.begin_tool_call(tool_call_id.clone()).await.unwrap();
    assert_eq!(tool_running.value.state(), ToolCallState::Running);
    let finished = app
        .finish_execution(
            run_id.clone(),
            tool_call_id,
            SubprocessOutput::success("out", "err", 0),
        )
        .await
        .unwrap();
    assert_eq!(finished.value.run().state(), RunState::Completed);
    assert_eq!(
        finished.value.tool_calls()[0].state(),
        ToolCallState::Completed
    );
    assert_eq!(finished.value.tool_calls()[0].stdout(), Some("out"));
    assert_eq!(finished.value.tool_calls()[0].stderr(), Some("err"));
    assert_eq!(finished.events.len(), 4);

    let history = store
        .list_session_events(value.id(), EventCursor::zero())
        .await
        .unwrap();
    assert_eq!(history.events().len(), 9);
    assert!(matches!(
        history.events()[1].payload(),
        SessionEventPayload::RunCreated {
            state: RunState::Queued,
            ..
        }
    ));
    assert!(matches!(
        history.events()[2].payload(),
        SessionEventPayload::RunStateChanged {
            state: RunState::Running,
            ..
        }
    ));
    assert!(matches!(
        history.events()[8].payload(),
        SessionEventPayload::RunStateChanged {
            state: RunState::Completed,
            ..
        }
    ));

    drop(app);
    drop(store);
    let reopened = super::SqliteStore::open(data.path()).await.unwrap();
    let retrieved = RunApplication::new(reopened, super::UlidIdGenerator)
        .get_run(run_id)
        .await
        .unwrap();
    assert_eq!(retrieved.run().state(), RunState::Completed);
}

#[tokio::test]
async fn failed_output_fails_both_tool_call_and_run() {
    let (_data, store, value) = seeded_session().await;
    let app = RunApplication::new(store, super::UlidIdGenerator);
    let created = app
        .start_root_run(value.id().clone(), "failed-run".to_owned())
        .await
        .unwrap();
    let run_id = created.value.run().run_id().clone();
    let running = app.begin_execution(run_id.clone()).await.unwrap();
    let tool_call_id = running.value.tool_calls()[0].tool_call_id().clone();
    app.begin_tool_call(tool_call_id.clone()).await.unwrap();
    let finished = app
        .finish_execution(
            run_id,
            tool_call_id,
            SubprocessOutput::failure("", "bad", Some(7)),
        )
        .await
        .unwrap();
    assert_eq!(finished.value.run().state(), RunState::Failed);
    assert_eq!(
        finished.value.tool_calls()[0].state(),
        ToolCallState::Failed
    );
    assert_eq!(finished.value.tool_calls()[0].exit_code(), Some(7));
}

#[tokio::test]
async fn malformed_persisted_run_is_rejected_on_read() {
    let (_data, store, value) = seeded_session().await;
    let mut connection = store.connection.lock().await;
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO runs (run_id, session_id, state) VALUES (?, ?, ?)")
        .bind("run_01ARZ3NDEKTSV4RRFFQ69G5FAV")
        .bind(value.id().as_str())
        .bind("broken")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);
    let run_id = RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
    assert_eq!(
        super::SqliteStore::open(_data.path())
            .await
            .unwrap()
            .get_run(&run_id)
            .await,
        Err(kiln_core::RunStoreError::Unavailable)
    );
}

#[tokio::test]
async fn malformed_persisted_run_event_is_rejected_on_read() {
    let (_data, store, value) = seeded_session().await;
    let mut connection = store.connection.lock().await;
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO runs (run_id, session_id, state) VALUES (?, ?, 'queued')")
        .bind("run_01ARZ3NDEKTSV4RRFFQ69G5FAV")
        .bind(value.id().as_str())
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO session_events (event_id, session_id, event_type, run_id, run_state) VALUES (?, ?, 'run.created', ?, 'broken')")
        .bind("evt_01ARZ3NDEKTSV4RRFFQ69G5FAW")
        .bind(value.id().as_str())
        .bind("run_01ARZ3NDEKTSV4RRFFQ69G5FAV")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);
    assert_eq!(
        store
            .list_session_events(value.id(), EventCursor::zero())
            .await,
        Err(kiln_core::StoreError::Unavailable)
    );
}
