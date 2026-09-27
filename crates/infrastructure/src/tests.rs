use std::{path::Path, process::Command};

use kiln_core::{
    ApprovalPolicy, AssignTask, ContextInstructionProvenance, ContextManifestApplication,
    ContextManifestEntryInput, ContextManifestError, ContextManifestId, ContextManifestStore,
    CreateContextManifest, CreateContextManifestDisposition, CreateTaskDisposition,
    DETERMINISTIC_SUBPROCESS_CAPABILITY, DiscoveredWorkspaceRoot, EventCursor, EventId,
    FilesystemIdentity, Message, MessageDeliveryMode, MessageDeliveryState, MessageId, MessageRole,
    RecordRunInputDelivery, RecordRunInputDisposition, Run, RunApplication, RunId, RunInputMode,
    RunState, RunStore, SendRunInput, SendRunInputDisposition, Session, SessionEvent,
    SessionEventPayload, SessionId, SessionStore, StartRunDisposition, SubprocessOutput, Task,
    TaskId, TaskMutationDisposition, TaskState, TaskStore, TaskStoreError, ToolCall, ToolCallId,
    ToolCallResult, ToolCallState, TransitionTask, UpdateTask, Workspace, WorkspaceId,
    WorkspacePathScope, WorkspaceRoot, WorkspaceRootDiscovery, WorkspaceRootId, WorkspaceRootState,
    WorkspaceStore,
};
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
use tempfile::TempDir;

#[tokio::test]
async fn native_tool_inspection_is_bounded_hash_checked_and_does_not_approve() {
    use kiln_core::*;
    use std::num::NonZeroUsize;
    let (_data, store, session) = seeded_session().await;
    let ids = super::UlidIdGenerator;
    let runs = RunApplication::new(store.clone(), ids);
    let run = runs
        .start_root_run(
            session.id().clone(),
            "inspection".into(),
            ApprovalPolicy::Ask,
            test_scope(),
        )
        .await
        .unwrap();
    let run_id = run.value.run().run_id().clone();
    let manifest = ContextManifestApplication::new(store.clone(), ids)
        .create_context_manifest(CreateContextManifest {
            run_id: run_id.clone(),
            entries: vec![ContextManifestEntryInput::Instruction {
                provenance: ContextInstructionProvenance::Runtime,
                content: "fixture".into(),
            }],
            idempotency_key: "inspection".into(),
        })
        .await
        .unwrap();
    let invocation = ModelInvocationApplication::new(store.clone(), ids)
        .create_model_invocation(CreateModelInvocation {
            run_id: run_id.clone(),
            context_manifest_id: manifest.value.context_manifest_id().clone(),
            context_manifest_hash: manifest.value.content_hash().clone(),
            provider_account_id: ProviderAccountId::from_ulid(ulid::Ulid::generate()),
            settings: ModelInvocationSettings::new(
                ProviderType::parse("fixture").unwrap(),
                ModelId::parse("fixture").unwrap(),
                GenerationSettings::new(None).unwrap(),
                ReasoningSettings::new(None).unwrap(),
            ),
            capabilities: ModelCapabilitySnapshot::new(
                "fixture",
                CapabilitySupport::Supported,
                CapabilitySupport::Unsupported,
                CapabilitySupport::Unsupported,
            )
            .unwrap(),
            purpose: ModelInvocationPurpose::Generation,
            retry_of: None,
            idempotency_key: "inspection".into(),
        })
        .await
        .unwrap()
        .value;
    let tool = WorkspaceFileReadTool::new(
        WorkspaceFileReadLimits {
            max_path_bytes: 128,
            max_file_bytes: 4096,
        },
        ModelToolCatalogLimits {
            max_tools: 1,
            max_definition_bytes: 4096,
            max_total_definition_bytes: 4096,
        },
    )
    .unwrap();
    store
        .attach_model_tool_catalog(invocation.invocation_id(), tool.catalog())
        .await
        .unwrap();
    let app = ProviderApplication::new(store.clone(), ids);
    assert!(matches!(
        app.claim(invocation.invocation_id().clone()).await.unwrap(),
        ProviderClaim::Applied { .. }
    ));
    let invocation = store
        .get_model_invocation(invocation.invocation_id())
        .await
        .unwrap()
        .unwrap();
    let requests = ModelToolRequestBatch::new(
        invocation.invocation_id().clone(),
        vec![ModelToolRequestInput {
            provider_call_id: "call-1".into(),
            name: "read_file".into(),
            arguments: serde_json::json!({"path":"private-file.txt"})
                .as_object()
                .unwrap()
                .clone(),
        }],
        ModelToolRequestLimits {
            max_requests: 1,
            max_provider_call_id_bytes: 64,
            max_name_bytes: 64,
            max_arguments_bytes: 128,
            max_total_arguments_bytes: 128,
        },
    )
    .unwrap();
    let usage = ProviderUsageUpdate::new(
        ProviderUsageMetadata {
            update_id: "inspection".into(),
            provider_account_id: invocation.provider_account_id().clone(),
            work_id: invocation.work_id().clone(),
            model_invocation_id: invocation.invocation_id().clone(),
            accounting: UsageAccounting::Cumulative,
            finality: UsageFinality::Final,
            completeness: UsageCompleteness::Unknown,
            observed_at_unix_ms: 1,
            request_id: None,
            resolved_model: None,
            service_tier: None,
            source: UsageSource::NativeProvider,
        },
        vec![],
    )
    .unwrap();
    app.record_tool_requests(&invocation, &requests, &usage)
        .await
        .unwrap();
    let batch = app
        .resolve_tool_requests(invocation.invocation_id().clone(), &tool)
        .await
        .unwrap();
    let adopted = app
        .adopt_tool_request(&batch.prepare_adoption(0, test_scope()).unwrap())
        .await
        .unwrap();
    let inspection = store
        .inspect_tool_call(&adopted.tool_call_id, NonZeroUsize::new(8192).unwrap())
        .await
        .unwrap();
    assert_eq!(inspection.run_id, run_id);
    assert_eq!(inspection.capability, WORKSPACE_FILE_READ_CAPABILITY);
    let source = inspection.source.unwrap();
    assert_eq!(
        source.request.arguments_json(),
        r#"{"path":"private-file.txt"}"#
    );
    assert_eq!(source.definition.revision(), "1");
    assert_eq!(
        store
            .inspect_tool_call(&adopted.tool_call_id, NonZeroUsize::new(1).unwrap())
            .await
            .err(),
        Some(ToolCallInspectionError::LimitExceeded)
    );
    let after = runs.get_run(run_id).await.unwrap();
    assert_eq!(
        after.tool_call(&adopted.tool_call_id).unwrap().state(),
        ToolCallState::AwaitingApproval
    );
    assert_eq!(after.approvals()[0].state(), ApprovalState::Pending);
    let mut connection = store.connection.lock().await;
    sqlx::query("UPDATE model_tool_requests SET arguments_json = '{\"path\":\"changed\"}' WHERE model_invocation_id = ?")
        .bind(invocation.invocation_id().as_str()).execute(&mut *connection).await.unwrap();
    drop(connection);
    assert_eq!(
        store
            .inspect_tool_call(&adopted.tool_call_id, NonZeroUsize::new(8192).unwrap())
            .await
            .err(),
        Some(ToolCallInspectionError::IntegrityViolation)
    );
}

#[test]
fn local_auth_credential_is_private_persistent_and_rejects_unsafe_files() {
    let data = tempfile::tempdir().unwrap();
    let first = super::LocalAuthCredential::open(data.path()).unwrap();
    let second = super::LocalAuthCredential::open(data.path()).unwrap();
    assert_eq!(first.token(), second.token());
    assert_eq!(first.path(), second.path());

    #[cfg(unix)]
    {
        use std::os::unix::fs::{PermissionsExt, symlink};

        assert_eq!(
            std::fs::metadata(first.path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let auth_directory = first.path().parent().unwrap();
        assert_eq!(
            std::fs::metadata(auth_directory)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );

        std::fs::set_permissions(first.path(), std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(super::LocalAuthCredential::open(data.path()).is_err());

        let symlink_data = tempfile::tempdir().unwrap();
        let credential = super::LocalAuthCredential::open(symlink_data.path()).unwrap();
        let credential_path = credential.path().to_owned();
        let external = symlink_data.path().join("external-token");
        std::fs::write(&external, [b'a'; 80]).unwrap();
        std::fs::set_permissions(&external, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::remove_file(&credential_path).unwrap();
        symlink(&external, &credential_path).unwrap();
        assert!(super::LocalAuthCredential::open(symlink_data.path()).is_err());
    }
}

#[cfg(unix)]
#[test]
fn subprocess_scope_accepts_in_root_symlinks_and_rejects_escape_symlinks() {
    use std::os::unix::fs::symlink;

    let workspace = tempfile::tempdir().unwrap();
    let workspace_root = std::fs::canonicalize(workspace.path()).unwrap();
    let inside = workspace.path().join("inside");
    std::fs::create_dir(&inside).unwrap();
    symlink(&inside, workspace.path().join("inside-link")).unwrap();
    let inside_request = kiln_core::SubprocessRequest::new(
        workspace_root.to_str().unwrap().to_owned(),
        super::workspace_root_filesystem_identity(&workspace_root).unwrap(),
        WorkspacePathScope::new(test_scope().workspace_root_id().clone(), "inside-link").unwrap(),
    )
    .unwrap();
    assert_eq!(super::validate_subprocess_request(&inside_request), Ok(()));

    let outside = tempfile::tempdir().unwrap();
    symlink(outside.path(), workspace.path().join("outside-link")).unwrap();
    let outside_request = kiln_core::SubprocessRequest::new(
        workspace_root.to_str().unwrap().to_owned(),
        super::workspace_root_filesystem_identity(&workspace_root).unwrap(),
        WorkspacePathScope::new(test_scope().workspace_root_id().clone(), "outside-link").unwrap(),
    )
    .unwrap();
    assert_eq!(
        super::validate_subprocess_request(&outside_request),
        Err(kiln_core::RunError::PathOutsideWorkspaceRoot)
    );
}

#[cfg(unix)]
#[test]
fn subprocess_scope_rejects_a_replaced_workspace_root() {
    let parent = tempfile::tempdir().unwrap();
    let workspace = parent.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let request = kiln_core::SubprocessRequest::new(
        workspace.to_str().unwrap().to_owned(),
        super::workspace_root_filesystem_identity(&workspace).unwrap(),
        WorkspacePathScope::new(test_scope().workspace_root_id().clone(), ".").unwrap(),
    )
    .unwrap();

    std::fs::rename(&workspace, parent.path().join("original-workspace")).unwrap();
    std::fs::create_dir(&workspace).unwrap();

    assert_eq!(
        super::validate_subprocess_request(&request),
        Err(kiln_core::RunError::PathOutsideWorkspaceRoot)
    );
}

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
        DiscoveredWorkspaceRoot {
            canonical_path: path.to_owned(),
            git_common_directory_path: common.to_owned(),
            filesystem_identity: FilesystemIdentity::new(format!("test:{path}"))
                .expect("test identity"),
        },
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

fn test_scope() -> WorkspacePathScope {
    WorkspacePathScope::new(
        WorkspaceRootId::parse("wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
        ".",
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
async fn tasks_are_idempotent_validate_lifecycle_and_survive_reopen() {
    let (data, store, workspace_id) = seeded_store().await;
    let first_session = session("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV", workspace_id.as_str());
    let second_session = session("ses_01ARZ3NDEKTSV4RRFFQ69G5FAW", workspace_id.as_str());
    for (value, event_id) in [
        (&first_session, "evt_01ARZ3NDEKTSV4RRFFQ69G5FAV"),
        (&second_session, "evt_01ARZ3NDEKTSV4RRFFQ69G5FAW"),
    ] {
        store
            .create_session(
                value,
                &SessionEvent::session_created(
                    EventId::parse(event_id).unwrap(),
                    value.id().clone(),
                    workspace_id.clone(),
                ),
            )
            .await
            .unwrap();
    }

    let parent = Task::new(
        TaskId::parse("tsk_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
        first_session.id().clone(),
        "parent".to_owned(),
        None,
        Vec::new(),
    )
    .unwrap();
    let created = store
        .create_task(
            &parent,
            &SessionEvent::task_created(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAX").unwrap(),
                parent.clone(),
            ),
            "parent-key",
        )
        .await
        .unwrap();
    assert_eq!(created.disposition, CreateTaskDisposition::Created);
    assert_eq!(created.events.len(), 1);

    let retry = Task::new(
        TaskId::parse("tsk_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap(),
        first_session.id().clone(),
        "parent".to_owned(),
        None,
        Vec::new(),
    )
    .unwrap();
    let duplicate = store
        .create_task(
            &retry,
            &SessionEvent::task_created(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAY").unwrap(),
                retry.clone(),
            ),
            "parent-key",
        )
        .await
        .unwrap();
    assert_eq!(duplicate.disposition, CreateTaskDisposition::Duplicate);
    assert_eq!(duplicate.value, parent);
    assert!(duplicate.events.is_empty());

    let conflict = Task::new(
        TaskId::parse("tsk_01ARZ3NDEKTSV4RRFFQ69G5FAX").unwrap(),
        first_session.id().clone(),
        "different".to_owned(),
        None,
        Vec::new(),
    )
    .unwrap();
    assert_eq!(
        store
            .create_task(
                &conflict,
                &SessionEvent::task_created(
                    EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAZ").unwrap(),
                    conflict.clone(),
                ),
                "parent-key",
            )
            .await,
        Err(TaskStoreError::IdempotencyConflict)
    );

    let outside = Task::new(
        TaskId::parse("tsk_01ARZ3NDEKTSV4RRFFQ69G5FAY").unwrap(),
        second_session.id().clone(),
        "outside".to_owned(),
        Some(parent.task_id().clone()),
        Vec::new(),
    )
    .unwrap();
    assert_eq!(
        store
            .create_task(
                &outside,
                &SessionEvent::task_created(
                    EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB0").unwrap(),
                    outside.clone(),
                ),
                "outside-key",
            )
            .await,
        Err(TaskStoreError::TaskLinkOutsideSession)
    );

    let child = Task::new(
        TaskId::parse("tsk_01ARZ3NDEKTSV4RRFFQ69G5FAZ").unwrap(),
        first_session.id().clone(),
        "child".to_owned(),
        Some(parent.task_id().clone()),
        vec![parent.task_id().clone()],
    )
    .unwrap();
    store
        .create_task(
            &child,
            &SessionEvent::task_created(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB1").unwrap(),
                child.clone(),
            ),
            "child-key",
        )
        .await
        .unwrap();

    assert_eq!(
        store
            .update_task(
                &UpdateTask {
                    task_id: parent.task_id().clone(),
                    objective: parent.objective().to_owned(),
                    dependency_task_ids: vec![child.task_id().clone()],
                    idempotency_key: "cycle-key".to_owned(),
                },
                [
                    EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB2").unwrap(),
                    EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB3").unwrap(),
                ],
            )
            .await,
        Err(TaskStoreError::Cycle)
    );

    let update = UpdateTask {
        task_id: child.task_id().clone(),
        objective: "updated child".to_owned(),
        dependency_task_ids: vec![parent.task_id().clone()],
        idempotency_key: "update-key".to_owned(),
    };
    assert_eq!(
        store
            .update_task(
                &update,
                [
                    EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                    EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB3").unwrap(),
                ],
            )
            .await,
        Err(TaskStoreError::Unavailable)
    );
    assert_eq!(
        store
            .get_task(child.task_id())
            .await
            .unwrap()
            .unwrap()
            .objective(),
        "child"
    );
    let updated = store
        .update_task(
            &update,
            [
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB4").unwrap(),
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB5").unwrap(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(updated.disposition, TaskMutationDisposition::Applied);
    assert_eq!(updated.value.objective(), "updated child");
    assert_eq!(updated.events.len(), 1);
    let duplicate = store
        .update_task(
            &update,
            [
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB6").unwrap(),
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB7").unwrap(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(duplicate.disposition, TaskMutationDisposition::Duplicate);
    assert!(duplicate.events.is_empty());
    assert_eq!(
        store
            .update_task(
                &UpdateTask {
                    objective: "conflict".to_owned(),
                    ..update.clone()
                },
                [
                    EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB8").unwrap(),
                    EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB9").unwrap(),
                ],
            )
            .await,
        Err(TaskStoreError::IdempotencyConflict)
    );

    assert_eq!(
        store
            .transition_task(
                &TransitionTask {
                    task_id: child.task_id().clone(),
                    state: TaskState::Ready,
                    idempotency_key: "not-ready-key".to_owned(),
                },
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBA").unwrap(),
            )
            .await,
        Err(TaskStoreError::InvalidTransition)
    );
    store
        .transition_task(
            &TransitionTask {
                task_id: parent.task_id().clone(),
                state: TaskState::Cancelled,
                idempotency_key: "cancel-parent-key".to_owned(),
            },
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBB").unwrap(),
        )
        .await
        .unwrap();
    store
        .transition_task(
            &TransitionTask {
                task_id: child.task_id().clone(),
                state: TaskState::Blocked,
                idempotency_key: "block-child-key".to_owned(),
            },
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBC").unwrap(),
        )
        .await
        .unwrap();

    let unblocked = store
        .update_task(
            &UpdateTask {
                task_id: child.task_id().clone(),
                objective: "updated child".to_owned(),
                dependency_task_ids: Vec::new(),
                idempotency_key: "unblock-child-key".to_owned(),
            },
            [
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBD").unwrap(),
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBE").unwrap(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(unblocked.value.state(), TaskState::Pending);
    assert_eq!(unblocked.events.len(), 2);
    assert!(matches!(
        unblocked.events[0].payload(),
        SessionEventPayload::TaskUpdated { .. }
    ));
    assert!(matches!(
        unblocked.events[1].payload(),
        SessionEventPayload::TaskStateChanged { .. }
    ));

    let transition = TransitionTask {
        task_id: child.task_id().clone(),
        state: TaskState::Ready,
        idempotency_key: "ready-child-key".to_owned(),
    };
    let ready = store
        .transition_task(
            &transition,
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBF").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ready.value.state(), TaskState::Ready);
    let duplicate = store
        .transition_task(
            &transition,
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBG").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(duplicate.disposition, TaskMutationDisposition::Duplicate);
    assert!(duplicate.events.is_empty());
    assert_eq!(
        store
            .transition_task(
                &TransitionTask {
                    state: TaskState::Cancelled,
                    ..transition.clone()
                },
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBH").unwrap(),
            )
            .await,
        Err(TaskStoreError::IdempotencyConflict)
    );
    assert_eq!(
        store
            .transition_task(
                &TransitionTask {
                    task_id: child.task_id().clone(),
                    state: TaskState::Completed,
                    idempotency_key: "invalid-terminal-key".to_owned(),
                },
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBJ").unwrap(),
            )
            .await,
        Err(TaskStoreError::InvalidTransition)
    );

    let run = Run::new(
        RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
        first_session.id().clone(),
        ApprovalPolicy::FullAccess,
        test_scope(),
    );
    store
        .start_root_run(
            &run,
            &[
                SessionEvent::run_created(
                    EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBK").unwrap(),
                    &run,
                ),
                SessionEvent::run_queued(
                    EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBN").unwrap(),
                    &run,
                ),
            ],
            "first-task-run",
        )
        .await
        .unwrap();
    let outside_run = Run::new(
        RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap(),
        second_session.id().clone(),
        ApprovalPolicy::FullAccess,
        test_scope(),
    );
    store
        .start_root_run(
            &outside_run,
            &[
                SessionEvent::run_created(
                    EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBM").unwrap(),
                    &outside_run,
                ),
                SessionEvent::run_queued(
                    EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBP").unwrap(),
                    &outside_run,
                ),
            ],
            "outside-task-run",
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .assign_task(
                &AssignTask {
                    task_id: child.task_id().clone(),
                    run_id: RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAX").unwrap(),
                    idempotency_key: "missing-run-key".to_owned(),
                },
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBN").unwrap(),
            )
            .await,
        Err(TaskStoreError::RunNotFound)
    );
    assert_eq!(
        store
            .assign_task(
                &AssignTask {
                    task_id: child.task_id().clone(),
                    run_id: outside_run.run_id().clone(),
                    idempotency_key: "outside-run-key".to_owned(),
                },
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBP").unwrap(),
            )
            .await,
        Err(TaskStoreError::TaskLinkOutsideSession)
    );
    let assignment = AssignTask {
        task_id: child.task_id().clone(),
        run_id: run.run_id().clone(),
        idempotency_key: "assign-run-key".to_owned(),
    };
    let assigned = store
        .assign_task(
            &assignment,
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBQ").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(assigned.disposition, TaskMutationDisposition::Applied);
    assert_eq!(assigned.value.assigned_run_id(), Some(run.run_id()));
    assert!(matches!(
        assigned.events[0].payload(),
        SessionEventPayload::TaskAssigned { .. }
    ));
    assert_eq!(
        store
            .get_run(run.run_id())
            .await
            .unwrap()
            .unwrap()
            .run()
            .task_id(),
        Some(child.task_id())
    );
    let other = Task::new(
        TaskId::parse("tsk_01ARZ3NDEKTSV4RRFFQ69G5FB0").unwrap(),
        first_session.id().clone(),
        "other".to_owned(),
        None,
        Vec::new(),
    )
    .unwrap();
    store
        .create_task(
            &other,
            &SessionEvent::task_created(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBW").unwrap(),
                other.clone(),
            ),
            "other-task-key",
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .assign_task(
                &AssignTask {
                    task_id: other.task_id().clone(),
                    run_id: run.run_id().clone(),
                    idempotency_key: "duplicate-run-key".to_owned(),
                },
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBX").unwrap(),
            )
            .await,
        Err(TaskStoreError::InvalidAssignment)
    );
    let duplicate = store
        .assign_task(
            &assignment,
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBR").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(duplicate.disposition, TaskMutationDisposition::Duplicate);
    assert!(duplicate.events.is_empty());
    assert_eq!(
        store
            .assign_task(
                &AssignTask {
                    run_id: outside_run.run_id().clone(),
                    ..assignment.clone()
                },
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBS").unwrap(),
            )
            .await,
        Err(TaskStoreError::IdempotencyConflict)
    );
    assert_eq!(
        store
            .transition_task(
                &TransitionTask {
                    task_id: child.task_id().clone(),
                    state: TaskState::Running,
                    idempotency_key: "run-before-claim-key".to_owned(),
                },
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBT").unwrap(),
            )
            .await,
        Err(TaskStoreError::InvalidTransition)
    );
    RunApplication::new(store.clone(), super::UlidIdGenerator)
        .begin_execution(run.run_id().clone())
        .await
        .unwrap();
    let running = store
        .transition_task(
            &TransitionTask {
                task_id: child.task_id().clone(),
                state: TaskState::Running,
                idempotency_key: "run-after-claim-key".to_owned(),
            },
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBV").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(running.value.state(), TaskState::Running);
    drop(store);

    let reopened = super::SqliteStore::open(data.path()).await.unwrap();
    let recovered = reopened.get_task(child.task_id()).await.unwrap().unwrap();
    assert_eq!(recovered.objective(), "updated child");
    assert_eq!(recovered.state(), TaskState::Running);
    assert!(recovered.dependency_task_ids().is_empty());
    assert_eq!(recovered.assigned_run_id(), Some(run.run_id()));
    let events = reopened
        .list_session_events(first_session.id(), EventCursor::zero())
        .await
        .unwrap();
    assert_eq!(
        events
            .events()
            .iter()
            .filter(|event| matches!(event.payload(), SessionEventPayload::TaskCreated { .. }))
            .count(),
        3
    );
    assert_eq!(
        events
            .events()
            .iter()
            .filter(|event| matches!(event.payload(), SessionEventPayload::TaskAssigned { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .events()
            .iter()
            .filter(|event| matches!(event.payload(), SessionEventPayload::TaskUpdated { .. }))
            .count(),
        2
    );
    assert_eq!(
        events
            .events()
            .iter()
            .filter(|event| matches!(
                event.payload(),
                SessionEventPayload::TaskStateChanged { .. }
            ))
            .count(),
        5
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
            first.id().as_str(),
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
                second.id().as_str(),
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
            first_message.id().as_str(),
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

    sqlx::raw_sql(include_str!("../migrations/0004_start_run_idempotency.sql"))
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO runs (run_id, session_id, state) VALUES (?, ?, 'completed')")
        .bind("run_01ARZ3NDEKTSV4RRFFQ69G5FAV")
        .bind("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO tool_calls (
            tool_call_id, run_id, capability, state, stdout, stderr, exit_code
         ) VALUES (?, ?, 'kiln.deterministic.subprocess', 'completed', '', '', 0)",
    )
    .bind("tcl_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .bind("run_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .execute(&mut connection)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO start_run_idempotencies (session_id, idempotency_key, run_id)
         VALUES (?, 'migration-key', ?)",
    )
    .bind("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .bind("run_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .execute(&mut connection)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO session_events (
            event_id, session_id, event_type, run_id, run_state
         ) VALUES (?, ?, 'run.created', ?, 'completed')",
    )
    .bind("evt_01ARZ3NDEKTSV4RRFFQ69G5FB0")
    .bind("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .bind("run_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .execute(&mut connection)
    .await
    .unwrap();
    sqlx::raw_sql(include_str!("../migrations/0005_run_cancellation.sql"))
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../migrations/0006_approval_scopes.sql"))
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
        5
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT run_id FROM start_run_idempotencies WHERE idempotency_key = 'migration-key'",
        )
        .fetch_one(&mut connection)
        .await
        .unwrap(),
        "run_01ARZ3NDEKTSV4RRFFQ69G5FAV"
    );
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&mut connection)
            .await
            .unwrap()
            .is_empty()
    );
    sqlx::query(
        "INSERT INTO session_events (event_id, session_id, event_type) VALUES (?, ?, 'session.created')",
    )
    .bind("evt_01ARZ3NDEKTSV4RRFFQ69G5FAZ")
    .bind("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .execute(&mut connection)
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT cursor FROM session_events WHERE event_id = ?")
            .bind("evt_01ARZ3NDEKTSV4RRFFQ69G5FAZ")
            .fetch_one(&mut connection)
            .await
            .unwrap(),
        6
    );
    for migration in [
        include_str!("../migrations/0007_artifacts.sql"),
        include_str!("../migrations/0008_tasks.sql"),
        include_str!("../migrations/0009_task_lifecycle.sql"),
        include_str!("../migrations/0010_task_assignment.sql"),
        include_str!("../migrations/0011_run_hierarchy.sql"),
        include_str!("../migrations/0012_run_input.sql"),
    ] {
        sqlx::raw_sql(migration)
            .execute(&mut connection)
            .await
            .unwrap();
    }
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT user_input_mode FROM runs WHERE run_id = 'run_01ARZ3NDEKTSV4RRFFQ69G5FAV'",
        )
        .fetch_one(&mut connection)
        .await
        .unwrap(),
        "interactive"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT user_input_mode FROM session_events WHERE event_id = 'evt_01ARZ3NDEKTSV4RRFFQ69G5FB0'",
        )
        .fetch_one(&mut connection)
        .await
        .unwrap(),
        "interactive"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT seq FROM sqlite_sequence WHERE name = 'session_events'",
        )
        .fetch_one(&mut connection)
        .await
        .unwrap(),
        6
    );
    sqlx::query(
        "INSERT INTO messages (message_id, session_id, role, content, target_run_id)
         VALUES (?, ?, 'user', 'queued before migration', ?)",
    )
    .bind("msg_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .bind("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .bind("run_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .execute(&mut connection)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO session_events (event_id, session_id, event_type, message_id)
         VALUES (?, ?, 'message.appended', ?)",
    )
    .bind("evt_01ARZ3NDEKTSV4RRFFQ69G5FBE")
    .bind("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .bind("msg_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .execute(&mut connection)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO session_events (event_id, session_id, event_type, message_id, run_id)
         VALUES (?, ?, 'run.input_queued', ?, ?)",
    )
    .bind("evt_01ARZ3NDEKTSV4RRFFQ69G5FBF")
    .bind("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .bind("msg_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .bind("run_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .execute(&mut connection)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO message_deliveries
            (message_id, run_id, delivery_mode, state, queued_cursor)
         VALUES (?, ?, 'queued', 'queued', ?)",
    )
    .bind("msg_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .bind("run_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .bind(8_i64)
    .execute(&mut connection)
    .await
    .unwrap();
    sqlx::raw_sql(include_str!("../migrations/0013_context_manifests.sql"))
        .execute(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM session_events")
            .fetch_one(&mut connection)
            .await
            .unwrap(),
        7
    );
    assert_eq!(
        sqlx::query_as::<_, (String, String)>(
            "SELECT session_id, event_type FROM session_events WHERE event_id = ?",
        )
        .bind("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV")
        .fetch_one(&mut connection)
        .await
        .unwrap(),
        (
            "ses_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            "session.created".to_owned(),
        )
    );
    assert_eq!(
        sqlx::query_as::<_, (String, String, i64)>(
            "SELECT delivery_mode, state, queued_cursor FROM message_deliveries
             WHERE message_id = ?",
        )
        .bind("msg_01ARZ3NDEKTSV4RRFFQ69G5FAV")
        .fetch_one(&mut connection)
        .await
        .unwrap(),
        ("queued".to_owned(), "queued".to_owned(), 8)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT seq FROM sqlite_sequence WHERE name = 'session_events'",
        )
        .fetch_one(&mut connection)
        .await
        .unwrap(),
        8
    );
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&mut connection)
            .await
            .unwrap()
            .is_empty()
    );
    sqlx::query(
        "INSERT INTO session_events (event_id, session_id, event_type) VALUES (?, ?, 'session.created')",
    )
    .bind("evt_01ARZ3NDEKTSV4RRFFQ69G5FBD")
    .bind("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .execute(&mut connection)
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT cursor FROM session_events WHERE event_id = ?")
            .bind("evt_01ARZ3NDEKTSV4RRFFQ69G5FBD")
            .fetch_one(&mut connection)
            .await
            .unwrap(),
        9
    );
}

#[tokio::test]
async fn approval_scope_migration_rejects_an_active_legacy_run() {
    let data = tempfile::tempdir().unwrap();
    let options = SqliteConnectOptions::new()
        .filename(data.path().join("migration.sqlite3"))
        .create_if_missing(true)
        .foreign_keys(true);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    for migration in [
        include_str!("../migrations/0001_workspaces.sql"),
        include_str!("../migrations/0002_sessions.sql"),
        include_str!("../migrations/0003_runs.sql"),
        include_str!("../migrations/0004_start_run_idempotency.sql"),
        include_str!("../migrations/0005_run_cancellation.sql"),
    ] {
        sqlx::raw_sql(migration)
            .execute(&mut connection)
            .await
            .unwrap();
    }
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
    sqlx::query("INSERT INTO runs (run_id, session_id, state) VALUES (?, ?, 'running')")
        .bind("run_01ARZ3NDEKTSV4RRFFQ69G5FAV")
        .bind("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV")
        .execute(&mut connection)
        .await
        .unwrap();

    assert!(
        sqlx::raw_sql(include_str!("../migrations/0006_approval_scopes.sql"))
            .execute(&mut connection)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn run_creation_is_atomic_and_has_a_global_cursor_event() {
    let (_data, store, value) = seeded_session().await;
    let app = RunApplication::new(store, super::UlidIdGenerator);
    let mutation = app
        .start_root_run(
            value.id().clone(),
            "atomic-run".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .unwrap();
    assert_eq!(mutation.events.len(), 2);
    assert_eq!(mutation.events[0].cursor().value(), 2);
    assert_eq!(mutation.value.run().state(), RunState::Queued);
    assert!(matches!(
        mutation.events[0].payload(),
        SessionEventPayload::RunCreated {
            state: RunState::Queued,
            ..
        }
    ));
    assert!(matches!(
        mutation.events[1].payload(),
        SessionEventPayload::RunQueued { .. }
    ));
    assert_eq!(mutation.value.run().parent_run_id(), None);
    assert_eq!(mutation.value.run().task_id(), None);
    assert_eq!(
        mutation.value.run().user_input_mode(),
        RunInputMode::Interactive
    );
}

#[tokio::test]
async fn child_runs_are_atomic_idempotent_and_recover_as_a_session_tree() {
    let (data, store, session) = seeded_session().await;
    let task = Task::new(
        TaskId::parse("tsk_01ARZ3NDEKTSV4RRFFQ69G5FCA").unwrap(),
        session.id().clone(),
        "delegated task".to_owned(),
        None,
        Vec::new(),
    )
    .unwrap();
    store
        .create_task(
            &task,
            &SessionEvent::task_created(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FCA").unwrap(),
                task.clone(),
            ),
            "child-task",
        )
        .await
        .unwrap();
    let app = RunApplication::new(store.clone(), super::UlidIdGenerator);
    let root = app
        .start_root_run(
            session.id().clone(),
            "tree-root".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .unwrap();
    let parent_run_id = root.value.run().run_id().clone();
    let first = app
        .start_child_run(
            parent_run_id.clone(),
            Some(task.task_id().clone()),
            RunInputMode::ReadOnly,
            "first-child".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .unwrap();
    assert_eq!(first.disposition, StartRunDisposition::Created);
    assert_eq!(first.events.len(), 4);
    assert!(matches!(
        first.events[0].payload(),
        SessionEventPayload::RunCreated {
            parent_run_id: Some(parent),
            task_id: Some(linked_task),
            user_input_mode: RunInputMode::ReadOnly,
            ..
        } if parent == &parent_run_id && linked_task == task.task_id()
    ));
    assert!(matches!(
        first.events[1].payload(),
        SessionEventPayload::RunQueued { .. }
    ));
    assert!(matches!(
        first.events[2].payload(),
        SessionEventPayload::RunChildAdded {
            parent_run_id: parent,
            child_run_id,
        } if parent == &parent_run_id && child_run_id == first.value.run().run_id()
    ));
    assert!(matches!(
        first.events[3].payload(),
        SessionEventPayload::TaskAssigned { task: assigned }
            if assigned.assigned_run_id() == Some(first.value.run().run_id())
    ));
    let second = app
        .start_child_run(
            parent_run_id.clone(),
            None,
            RunInputMode::Interactive,
            "second-child".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .unwrap();
    assert_eq!(second.disposition, StartRunDisposition::Created);
    assert_eq!(second.events.len(), 3);

    assert_eq!(
        app.start_child_run(
            RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FCB").unwrap(),
            None,
            RunInputMode::Interactive,
            "missing-parent".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await,
        Err(kiln_core::RunError::ParentRunNotFound)
    );
    assert_eq!(
        app.start_child_run(
            parent_run_id.clone(),
            Some(TaskId::parse("tsk_01ARZ3NDEKTSV4RRFFQ69G5FCB").unwrap()),
            RunInputMode::Interactive,
            "missing-task".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await,
        Err(kiln_core::RunError::TaskNotFound)
    );
    let outside_session = Session::new(
        SessionId::parse("ses_01ARZ3NDEKTSV4RRFFQ69G5FCB").unwrap(),
        session.workspace_id().clone(),
    );
    store
        .create_session(
            &outside_session,
            &SessionEvent::session_created(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FCB").unwrap(),
                outside_session.id().clone(),
                outside_session.workspace_id().clone(),
            ),
        )
        .await
        .unwrap();
    let outside_task = Task::new(
        TaskId::parse("tsk_01ARZ3NDEKTSV4RRFFQ69G5FCC").unwrap(),
        outside_session.id().clone(),
        "outside task".to_owned(),
        None,
        Vec::new(),
    )
    .unwrap();
    store
        .create_task(
            &outside_task,
            &SessionEvent::task_created(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FCC").unwrap(),
                outside_task.clone(),
            ),
            "outside-child-task",
        )
        .await
        .unwrap();
    assert_eq!(
        app.start_child_run(
            parent_run_id.clone(),
            Some(outside_task.task_id().clone()),
            RunInputMode::Interactive,
            "outside-task".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await,
        Err(kiln_core::RunError::TaskLinkOutsideSession)
    );

    let duplicate = app
        .start_child_run(
            parent_run_id.clone(),
            Some(task.task_id().clone()),
            RunInputMode::ReadOnly,
            "first-child".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .unwrap();
    assert_eq!(duplicate.disposition, StartRunDisposition::Duplicate);
    assert_eq!(duplicate.value, first.value);
    assert!(duplicate.events.is_empty());
    assert_eq!(
        app.start_child_run(
            parent_run_id.clone(),
            Some(task.task_id().clone()),
            RunInputMode::Interactive,
            "first-child".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await,
        Err(kiln_core::RunError::IdempotencyConflict)
    );
    assert_eq!(
        app.start_child_run(
            parent_run_id.clone(),
            Some(task.task_id().clone()),
            RunInputMode::Interactive,
            "assigned-task".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await,
        Err(kiln_core::RunError::InvalidTaskAssignment)
    );

    drop(app);
    drop(store);
    let reopened = super::SqliteStore::open(data.path()).await.unwrap();
    let recovered = RunApplication::new(reopened.clone(), super::UlidIdGenerator)
        .list_session_runs(session.id().clone())
        .await
        .unwrap();
    assert_eq!(recovered.len(), 3);
    assert_eq!(recovered[0].run().run_id(), &parent_run_id);
    assert_eq!(recovered[1].run(), first.value.run());
    assert_eq!(recovered[2].run(), second.value.run());

    let reopened_app = RunApplication::new(reopened, super::UlidIdGenerator);
    let cancelling = reopened_app
        .request_cancellation(parent_run_id.clone())
        .await
        .unwrap();
    assert_eq!(cancelling.value.run().state(), RunState::Cancelling);
    let terminal_duplicate = reopened_app
        .start_child_run(
            parent_run_id.clone(),
            Some(task.task_id().clone()),
            RunInputMode::ReadOnly,
            "first-child".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .unwrap();
    assert_eq!(
        terminal_duplicate.disposition,
        StartRunDisposition::Duplicate
    );
    assert_eq!(terminal_duplicate.value, first.value);
    assert!(terminal_duplicate.events.is_empty());
    assert_eq!(
        reopened_app
            .start_child_run(
                first.value.run().run_id().clone(),
                None,
                RunInputMode::Interactive,
                "cancelling-ancestor".to_owned(),
                ApprovalPolicy::FullAccess,
                test_scope(),
            )
            .await,
        Err(kiln_core::RunError::InvalidTransition)
    );

    drop(reopened_app);
    let reopened = super::SqliteStore::open(data.path()).await.unwrap();
    let reopened_app = RunApplication::new(reopened, super::UlidIdGenerator);
    assert_eq!(
        reopened_app
            .get_run(parent_run_id.clone())
            .await
            .unwrap()
            .run()
            .state(),
        RunState::Cancelling
    );
    reopened_app
        .request_cancellation(first.value.run().run_id().clone())
        .await
        .unwrap();
    reopened_app
        .request_cancellation(second.value.run().run_id().clone())
        .await
        .unwrap();
    let cancelled = reopened_app
        .request_cancellation(parent_run_id)
        .await
        .unwrap();
    assert_eq!(cancelled.value.run().state(), RunState::Cancelled);
}

#[tokio::test]
async fn targeted_run_input_is_atomic_idempotent_fifo_and_recovers() {
    let (data, store, session) = seeded_session().await;
    let app = RunApplication::new(store.clone(), super::UlidIdGenerator);
    let root = app
        .start_root_run(
            session.id().clone(),
            "input-root".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .unwrap();
    let run_id = root.value.run().run_id().clone();
    let read_only = app
        .start_child_run(
            run_id.clone(),
            None,
            RunInputMode::ReadOnly,
            "input-read-only-child".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .unwrap();

    let first_command = SendRunInput {
        attachments: Vec::new(),
        run_id: run_id.clone(),
        content: "first guidance".to_owned(),
        delivery_mode: MessageDeliveryMode::Queued,
        idempotency_key: "first-input".to_owned(),
    };
    let first = app.send_run_input(first_command.clone()).await.unwrap();
    assert_eq!(first.disposition, SendRunInputDisposition::Created);
    assert_eq!(first.value.state(), MessageDeliveryState::Queued);
    assert_eq!(first.value.message().target_run_id(), Some(&run_id));
    assert!(matches!(
        first.events[0].payload(),
        SessionEventPayload::MessageAppended { message }
            if message.id() == first.value.message().id()
    ));
    assert!(matches!(
        first.events[1].payload(),
        SessionEventPayload::RunInputQueued {
            run_id: event_run_id,
            message_id,
        } if event_run_id == &run_id && message_id == first.value.message().id()
    ));

    let second = app
        .send_run_input(SendRunInput {
            attachments: Vec::new(),
            run_id: run_id.clone(),
            content: "urgent guidance".to_owned(),
            delivery_mode: MessageDeliveryMode::Interrupt,
            idempotency_key: "second-input".to_owned(),
        })
        .await
        .unwrap();
    assert!(matches!(
        second.events[1].payload(),
        SessionEventPayload::RunInterruptRequested { .. }
    ));
    let third = app
        .send_run_input(SendRunInput {
            attachments: Vec::new(),
            run_id: run_id.clone(),
            content: "third guidance".to_owned(),
            delivery_mode: MessageDeliveryMode::Queued,
            idempotency_key: "third-input".to_owned(),
        })
        .await
        .unwrap();

    let duplicate = app.send_run_input(first_command.clone()).await.unwrap();
    assert_eq!(duplicate.disposition, SendRunInputDisposition::Duplicate);
    assert_eq!(duplicate.value, first.value);
    assert!(duplicate.events.is_empty());
    let mut conflicting = first_command.clone();
    conflicting.content = "different guidance".to_owned();
    assert_eq!(
        app.send_run_input(conflicting).await,
        Err(kiln_core::RunError::IdempotencyConflict)
    );
    assert_eq!(
        app.send_run_input(SendRunInput {
            attachments: Vec::new(),
            run_id: read_only.value.run().run_id().clone(),
            content: "not permitted".to_owned(),
            delivery_mode: MessageDeliveryMode::Queued,
            idempotency_key: "read-only-input".to_owned(),
        })
        .await,
        Err(kiln_core::RunError::RunInputReadOnly)
    );

    let forged_message = Message::new_targeted(
        MessageId::parse("msg_01ARZ3NDEKTSV4RRFFQ69G5FCE").unwrap(),
        session.id().clone(),
        MessageRole::User,
        "bypass".to_owned(),
        run_id.clone(),
    )
    .unwrap();
    assert_eq!(
        store
            .append_message(
                &forged_message,
                &SessionEvent::message_appended(
                    EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FCE").unwrap(),
                    forged_message.clone(),
                ),
                forged_message.id().as_str(),
            )
            .await,
        Err(kiln_core::StoreError::Unavailable)
    );

    assert_eq!(
        app.next_queued_run_input(run_id.clone()).await.unwrap(),
        Some(first.value.clone())
    );
    assert_eq!(
        app.record_run_input_delivery(RecordRunInputDelivery {
            message_id: second.value.message().id().clone(),
            state: MessageDeliveryState::Delivered,
        })
        .await,
        Err(kiln_core::RunError::MessageDeliveryOutOfOrder)
    );
    let delivered = app
        .record_run_input_delivery(RecordRunInputDelivery {
            message_id: first.value.message().id().clone(),
            state: MessageDeliveryState::Delivered,
        })
        .await
        .unwrap();
    assert_eq!(delivered.disposition, RecordRunInputDisposition::Applied);
    assert!(matches!(
        delivered.events[0].payload(),
        SessionEventPayload::RunInputDelivered { .. }
    ));
    let delivered_retry = app
        .record_run_input_delivery(RecordRunInputDelivery {
            message_id: first.value.message().id().clone(),
            state: MessageDeliveryState::Delivered,
        })
        .await
        .unwrap();
    assert_eq!(
        delivered_retry.disposition,
        RecordRunInputDisposition::Duplicate
    );
    assert!(delivered_retry.events.is_empty());
    assert_eq!(
        app.record_run_input_delivery(RecordRunInputDelivery {
            message_id: first.value.message().id().clone(),
            state: MessageDeliveryState::Failed,
        })
        .await,
        Err(kiln_core::RunError::InvalidMessageDelivery)
    );
    let failed = app
        .record_run_input_delivery(RecordRunInputDelivery {
            message_id: second.value.message().id().clone(),
            state: MessageDeliveryState::Failed,
        })
        .await
        .unwrap();
    assert_eq!(failed.value.state(), MessageDeliveryState::Failed);

    app.request_cancellation(read_only.value.run().run_id().clone())
        .await
        .unwrap();
    let terminal = app.request_cancellation(run_id.clone()).await.unwrap();
    assert!(matches!(
        terminal.events.last().map(|event| event.payload()),
        Some(SessionEventPayload::RunInputCancelled {
            run_id: event_run_id,
            message_id,
        }) if event_run_id == &run_id && message_id == third.value.message().id()
    ));
    assert_eq!(
        app.record_run_input_delivery(RecordRunInputDelivery {
            message_id: third.value.message().id().clone(),
            state: MessageDeliveryState::Delivered,
        })
        .await,
        Err(kiln_core::RunError::InvalidMessageDelivery)
    );
    let cancelled = app
        .record_run_input_delivery(RecordRunInputDelivery {
            message_id: third.value.message().id().clone(),
            state: MessageDeliveryState::Cancelled,
        })
        .await
        .unwrap();
    assert_eq!(cancelled.value.state(), MessageDeliveryState::Cancelled);
    assert_eq!(cancelled.disposition, RecordRunInputDisposition::Duplicate);
    assert!(cancelled.events.is_empty());
    let terminal_retry = app.send_run_input(first_command).await.unwrap();
    assert_eq!(
        terminal_retry.disposition,
        SendRunInputDisposition::Duplicate
    );
    assert_eq!(
        terminal_retry.value.state(),
        MessageDeliveryState::Delivered
    );
    assert_eq!(
        app.send_run_input(SendRunInput {
            attachments: Vec::new(),
            run_id: run_id.clone(),
            content: "late guidance".to_owned(),
            delivery_mode: MessageDeliveryMode::Queued,
            idempotency_key: "late-input".to_owned(),
        })
        .await,
        Err(kiln_core::RunError::RunNotAcceptingInput)
    );

    drop(app);
    drop(store);
    let reopened = super::SqliteStore::open(data.path()).await.unwrap();
    let reopened_app = RunApplication::new(reopened.clone(), super::UlidIdGenerator);
    assert_eq!(
        reopened_app
            .next_queued_run_input(run_id.clone())
            .await
            .unwrap(),
        None
    );
    let history = reopened
        .list_session_events(session.id(), EventCursor::zero())
        .await
        .unwrap();
    let kinds = history
        .events()
        .iter()
        .filter_map(|event| match event.payload() {
            SessionEventPayload::RunInputQueued { .. } => Some("queued"),
            SessionEventPayload::RunInterruptRequested { .. } => Some("interrupt"),
            SessionEventPayload::RunInputDelivered { .. } => Some("delivered"),
            SessionEventPayload::RunInputFailed { .. } => Some("failed"),
            SessionEventPayload::RunInputCancelled { .. } => Some("cancelled"),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        [
            "queued",
            "interrupt",
            "queued",
            "delivered",
            "failed",
            "cancelled"
        ]
    );
}

#[tokio::test]
async fn context_manifests_validate_sources_hash_atomically_and_survive_termination() {
    let (data, store, session_value) = seeded_session().await;
    let run_app = RunApplication::new(store.clone(), super::UlidIdGenerator);
    let manifest_app = ContextManifestApplication::new(store.clone(), super::UlidIdGenerator);
    let root = run_app
        .start_root_run(
            session_value.id().clone(),
            "manifest-root".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .unwrap();
    let run_id = root.value.run().run_id().clone();
    let child = run_app
        .start_child_run(
            run_id.clone(),
            None,
            RunInputMode::Interactive,
            "manifest-child".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .unwrap();
    let child_run_id = child.value.run().run_id().clone();

    let session_message = message(
        "msg_01ARZ3NDEKTSV4RRFFQ69G5FAW",
        session_value.id().as_str(),
        "session context",
    );
    store
        .append_message(
            &session_message,
            &SessionEvent::message_appended(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap(),
                session_message.clone(),
            ),
            session_message.id().as_str(),
        )
        .await
        .unwrap();
    let other_session = session(
        "ses_01ARZ3NDEKTSV4RRFFQ69G5FAW",
        session_value.workspace_id().as_str(),
    );
    store
        .create_session(
            &other_session,
            &SessionEvent::session_created(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAX").unwrap(),
                other_session.id().clone(),
                other_session.workspace_id().clone(),
            ),
        )
        .await
        .unwrap();
    let other_message = message(
        "msg_01ARZ3NDEKTSV4RRFFQ69G5FAX",
        other_session.id().as_str(),
        "other session",
    );
    store
        .append_message(
            &other_message,
            &SessionEvent::message_appended(
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAY").unwrap(),
                other_message.clone(),
            ),
            other_message.id().as_str(),
        )
        .await
        .unwrap();
    let targeted = run_app
        .send_run_input(SendRunInput {
            attachments: Vec::new(),
            run_id: run_id.clone(),
            content: "delivered guidance".to_owned(),
            delivery_mode: MessageDeliveryMode::Queued,
            idempotency_key: "manifest-input".to_owned(),
        })
        .await
        .unwrap();

    let one_message =
        |target_run_id: RunId, message_id: MessageId, key: &str| CreateContextManifest {
            run_id: target_run_id,
            entries: vec![ContextManifestEntryInput::Message { message_id }],
            idempotency_key: key.to_owned(),
        };
    assert_eq!(
        manifest_app
            .create_context_manifest(one_message(
                run_id.clone(),
                targeted.value.message().id().clone(),
                "queued-message",
            ))
            .await,
        Err(ContextManifestError::MessageDeliveryNotDelivered)
    );
    assert_eq!(
        manifest_app
            .create_context_manifest(one_message(
                child_run_id.clone(),
                targeted.value.message().id().clone(),
                "wrong-target",
            ))
            .await,
        Err(ContextManifestError::MessageTargetMismatch)
    );
    assert_eq!(
        manifest_app
            .create_context_manifest(one_message(
                run_id.clone(),
                other_message.id().clone(),
                "other-session",
            ))
            .await,
        Err(ContextManifestError::MessageOutsideSession)
    );
    assert_eq!(
        manifest_app
            .create_context_manifest(one_message(
                run_id.clone(),
                MessageId::parse("msg_01ARZ3NDEKTSV4RRFFQ69G5FAZ").unwrap(),
                "missing-message",
            ))
            .await,
        Err(ContextManifestError::MessageNotFound)
    );
    assert_eq!(
        manifest_app
            .create_context_manifest(CreateContextManifest {
                run_id: run_id.clone(),
                entries: vec![ContextManifestEntryInput::Instruction {
                    provenance: ContextInstructionProvenance::Runtime,
                    content: " \n ".to_owned(),
                }],
                idempotency_key: "blank-instruction".to_owned(),
            })
            .await,
        Err(ContextManifestError::InstructionContentRequired)
    );
    assert_eq!(
        manifest_app
            .create_context_manifest(CreateContextManifest {
                run_id: run_id.clone(),
                entries: vec![ContextManifestEntryInput::Instruction {
                    provenance: ContextInstructionProvenance::Workspace {
                        workspace_root_id:
                            WorkspaceRootId::parse("wrt_01ARZ3NDEKTSV4RRFFQ69G5FAW",).unwrap(),
                    },
                    content: "wrong workspace".to_owned(),
                }],
                idempotency_key: "wrong-workspace".to_owned(),
            })
            .await,
        Err(ContextManifestError::WorkspaceProvenanceMismatch)
    );
    assert_eq!(
        manifest_app
            .create_context_manifest(CreateContextManifest {
                run_id: run_id.clone(),
                entries: vec![
                    ContextManifestEntryInput::Message {
                        message_id: session_message.id().clone(),
                    },
                    ContextManifestEntryInput::Message {
                        message_id: session_message.id().clone(),
                    },
                ],
                idempotency_key: "duplicate-message".to_owned(),
            })
            .await,
        Err(ContextManifestError::DuplicateMessage)
    );

    run_app
        .record_run_input_delivery(RecordRunInputDelivery {
            message_id: targeted.value.message().id().clone(),
            state: MessageDeliveryState::Delivered,
        })
        .await
        .unwrap();
    let entries = vec![
        ContextManifestEntryInput::Instruction {
            provenance: ContextInstructionProvenance::Runtime,
            content: " \0é runtime instruction\n".to_owned(),
        },
        ContextManifestEntryInput::Instruction {
            provenance: ContextInstructionProvenance::Workspace {
                workspace_root_id: test_scope().workspace_root_id().clone(),
            },
            content: "workspace instruction".to_owned(),
        },
        ContextManifestEntryInput::Instruction {
            provenance: ContextInstructionProvenance::Run {
                run_id: run_id.clone(),
            },
            content: "run instruction".to_owned(),
        },
        ContextManifestEntryInput::Message {
            message_id: session_message.id().clone(),
        },
        ContextManifestEntryInput::Message {
            message_id: targeted.value.message().id().clone(),
        },
    ];
    let first_command = CreateContextManifest {
        run_id: run_id.clone(),
        entries: entries.clone(),
        idempotency_key: "manifest-first".to_owned(),
    };
    let first = manifest_app
        .create_context_manifest(first_command.clone())
        .await
        .unwrap();
    assert_eq!(first.disposition, CreateContextManifestDisposition::Created);
    assert_eq!(
        first.value.entries()[0].content(),
        " \0é runtime instruction\n"
    );
    assert!(matches!(
        first.events[0].payload(),
        SessionEventPayload::ContextManifestCreated {
            context_manifest_id,
            run_id: event_run_id,
            content_hash,
            entry_count: 5,
        } if context_manifest_id == first.value.context_manifest_id()
            && event_run_id == &run_id
            && content_hash == first.value.content_hash()
    ));
    assert_eq!(
        run_app.get_run(run_id.clone()).await.unwrap().run().state(),
        RunState::Queued
    );
    assert_eq!(
        store
            .get_context_manifest(first.value.context_manifest_id())
            .await
            .unwrap(),
        Some(first.value.clone())
    );
    let duplicate = manifest_app
        .create_context_manifest(first_command.clone())
        .await
        .unwrap();
    assert_eq!(
        duplicate.disposition,
        CreateContextManifestDisposition::Duplicate
    );
    assert_eq!(duplicate.value, first.value);
    assert!(duplicate.events.is_empty());
    let mut conflicting = first_command.clone();
    conflicting.entries.reverse();
    assert_eq!(
        manifest_app.create_context_manifest(conflicting).await,
        Err(ContextManifestError::IdempotencyConflict)
    );
    let equivalent = manifest_app
        .create_context_manifest(CreateContextManifest {
            run_id: run_id.clone(),
            entries: entries.clone(),
            idempotency_key: "manifest-equivalent".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(equivalent.value.content_hash(), first.value.content_hash());
    let mut reversed_entries = entries.clone();
    reversed_entries.reverse();
    let reversed = manifest_app
        .create_context_manifest(CreateContextManifest {
            run_id: run_id.clone(),
            entries: reversed_entries,
            idempotency_key: "manifest-reversed".to_owned(),
        })
        .await
        .unwrap();
    assert_ne!(reversed.value.content_hash(), first.value.content_hash());
    assert_eq!(
        manifest_app
            .list_context_manifests(run_id.clone())
            .await
            .unwrap()
            .iter()
            .map(|manifest| manifest.context_manifest_id())
            .collect::<Vec<_>>(),
        [
            first.value.context_manifest_id(),
            equivalent.value.context_manifest_id(),
            reversed.value.context_manifest_id(),
        ]
    );

    let cursor_before_failed_create = store.current_event_cursor().await.unwrap();
    let rollback_id = ContextManifestId::parse("cmf_01ARZ3NDEKTSV4RRFFQ69G5FB0").unwrap();
    assert_eq!(
        store
            .create_context_manifest(
                &CreateContextManifest {
                    run_id: run_id.clone(),
                    entries: entries.clone(),
                    idempotency_key: "manifest-rollback".to_owned(),
                },
                rollback_id.clone(),
                EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            )
            .await,
        Err(kiln_core::ContextManifestStoreError::Unavailable)
    );
    assert_eq!(
        store.current_event_cursor().await.unwrap(),
        cursor_before_failed_create
    );
    assert_eq!(
        store.get_context_manifest(&rollback_id).await.unwrap(),
        None
    );
    {
        let mut connection = store.connection.lock().await;
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM create_context_manifest_idempotencies
                 WHERE run_id = ? AND idempotency_key = 'manifest-rollback'",
            )
            .bind(run_id.as_str())
            .fetch_one(&mut *connection)
            .await
            .unwrap(),
            0
        );
    }

    run_app.request_cancellation(child_run_id).await.unwrap();
    run_app.request_cancellation(run_id.clone()).await.unwrap();
    let terminal_retry = manifest_app
        .create_context_manifest(first_command)
        .await
        .unwrap();
    assert_eq!(
        terminal_retry.disposition,
        CreateContextManifestDisposition::Duplicate
    );
    assert_eq!(terminal_retry.value, first.value);
    assert_eq!(
        manifest_app
            .create_context_manifest(CreateContextManifest {
                run_id: run_id.clone(),
                entries,
                idempotency_key: "manifest-after-terminal".to_owned(),
            })
            .await,
        Err(ContextManifestError::RunNotAcceptingWork)
    );

    drop(manifest_app);
    drop(run_app);
    drop(store);
    let reopened = super::SqliteStore::open(data.path()).await.unwrap();
    let reopened_app = ContextManifestApplication::new(reopened.clone(), super::UlidIdGenerator);
    assert_eq!(
        reopened_app
            .get_context_manifest(first.value.context_manifest_id().clone())
            .await
            .unwrap(),
        first.value
    );
    assert!(
        reopened
            .list_session_events(session_value.id(), EventCursor::zero())
            .await
            .unwrap()
            .events()
            .iter()
            .any(|event| matches!(
                event.payload(),
                SessionEventPayload::ContextManifestCreated { context_manifest_id, .. }
                    if context_manifest_id == first.value.context_manifest_id()
            ))
    );
    {
        let mut connection = reopened.connection.lock().await;
        sqlx::query("UPDATE context_manifests SET content_hash = ? WHERE context_manifest_id = ?")
            .bind("b".repeat(64))
            .bind(first.value.context_manifest_id().as_str())
            .execute(&mut *connection)
            .await
            .unwrap();
    }
    assert_eq!(
        reopened_app
            .get_context_manifest(first.value.context_manifest_id().clone())
            .await,
        Err(ContextManifestError::IntegrityViolation)
    );
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
        .start_root_run(
            value.id().clone(),
            "same-key".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .unwrap();
    assert_eq!(first.disposition, StartRunDisposition::Created);
    let duplicate = app
        .start_root_run(
            value.id().clone(),
            "same-key".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
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
        .start_root_run(
            value.id().clone(),
            "same-key".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .unwrap();
    assert_eq!(after_terminal.disposition, StartRunDisposition::Duplicate);
    assert_eq!(
        after_terminal.value.run().run_id(),
        first.value.run().run_id()
    );
    assert_eq!(after_terminal.value.run().state(), RunState::Completed);
    assert_eq!(after_terminal.value.tool_calls().len(), 1);
}

#[tokio::test]
async fn failed_start_transaction_does_not_reserve_the_idempotency_key() {
    let (_data, store, session) = seeded_session().await;
    let rejected = Run::new(
        RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAY").unwrap(),
        session.id().clone(),
        ApprovalPolicy::FullAccess,
        test_scope(),
    );
    let duplicate_event_id = [
        SessionEvent::run_created(
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            &rejected,
        ),
        SessionEvent::run_queued(
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBQ").unwrap(),
            &rejected,
        ),
    ];

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
        ApprovalPolicy::FullAccess,
        test_scope(),
    );
    let event = [
        SessionEvent::run_created(
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAY").unwrap(),
            &accepted,
        ),
        SessionEvent::run_queued(
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBR").unwrap(),
            &accepted,
        ),
    ];
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
    app.start_root_run(
        value.id().clone(),
        "first-key".to_owned(),
        ApprovalPolicy::FullAccess,
        test_scope(),
    )
    .await
    .unwrap();
    assert_eq!(
        app.start_root_run(
            value.id().clone(),
            "second-key".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await,
        Err(kiln_core::RunError::ActiveRootRunExists)
    );
}

#[tokio::test]
async fn only_one_active_root_run_is_allowed_per_session() {
    let (_data, store, value) = seeded_session().await;
    let app = RunApplication::new(store, super::UlidIdGenerator);
    app.start_root_run(
        value.id().clone(),
        "first-run".to_owned(),
        ApprovalPolicy::FullAccess,
        test_scope(),
    )
    .await
    .unwrap();
    assert_eq!(
        app.start_root_run(
            value.id().clone(),
            "second-run".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await,
        Err(kiln_core::RunError::ActiveRootRunExists)
    );
}

#[tokio::test]
async fn run_store_rejects_state_machine_bypass_inputs() {
    let (_data, store, session) = seeded_session().await;
    let forged_id = RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAY").unwrap();
    let forged = Run::from_persisted(
        forged_id.clone(),
        session.id().clone(),
        RunState::Completed,
        None,
        None,
    )
    .unwrap();
    let forged_event = [
        SessionEvent::run_created(
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAY").unwrap(),
            &forged,
        ),
        SessionEvent::run_queued(
            EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FBT").unwrap(),
            &forged,
        ),
    ];
    assert_eq!(
        store
            .start_root_run(&forged, &forged_event, "forged-run")
            .await,
        Err(kiln_core::RunStoreError::InvalidTransition)
    );
    assert_eq!(store.get_run(&forged_id).await.unwrap(), None);

    let app = RunApplication::new(store.clone(), super::UlidIdGenerator);
    let created = app
        .start_root_run(
            session.id().clone(),
            "valid-run".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .unwrap();
    let run_id = created.value.run().run_id().clone();
    let running_run = created.value.run().transition(RunState::Running).unwrap();
    let proposed_tool = ToolCall::new(
        ToolCallId::parse("tcl_01ARZ3NDEKTSV4RRFFQ69G5FAY").unwrap(),
        run_id.clone(),
        DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
        test_scope(),
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
            .begin_execution(&running_run, &proposed_tool, None, &reversed_events)
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
        .start_root_run(
            value.id().clone(),
            "historical-run".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .unwrap();
    let run_id = created.value.run().run_id().clone();
    let running = app.begin_execution(run_id.clone()).await.unwrap();
    let tool_call_id = running.value.tool_calls()[0].tool_call_id().clone();
    assert_eq!(running.events.len(), 3);
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
    assert_eq!(history.events().len(), 11);
    assert!(matches!(
        history.events()[1].payload(),
        SessionEventPayload::RunCreated {
            state: RunState::Queued,
            ..
        }
    ));
    assert!(matches!(
        history.events()[3].payload(),
        SessionEventPayload::RunStateChanged {
            state: RunState::Running,
            ..
        }
    ));
    assert!(matches!(
        history.events()[10].payload(),
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
async fn queued_cancellation_is_atomic_and_replayed_in_order() {
    let (_data, store, session) = seeded_session().await;
    let app = RunApplication::new(store.clone(), super::UlidIdGenerator);
    let created = app
        .start_root_run(
            session.id().clone(),
            "queued-cancel".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .unwrap();
    let run_id = created.value.run().run_id().clone();

    let cancelled = app.request_cancellation(run_id.clone()).await.unwrap();
    assert_eq!(cancelled.value.run().state(), RunState::Cancelled);
    assert_eq!(cancelled.events.len(), 2);
    assert!(matches!(
        cancelled.events[0].payload(),
        SessionEventPayload::RunCancellationRequested { .. }
    ));
    assert!(matches!(
        cancelled.events[1].payload(),
        SessionEventPayload::RunStateChanged {
            state: RunState::Cancelled,
            ..
        }
    ));
    assert_eq!(
        cancelled
            .events
            .iter()
            .map(|event| event.cursor().value())
            .collect::<Vec<_>>(),
        [4, 5]
    );

    let repeated = app.request_cancellation(run_id).await.unwrap();
    assert_eq!(repeated.value, cancelled.value);
    assert!(repeated.events.is_empty());
    let history = store
        .list_session_events(session.id(), EventCursor::zero())
        .await
        .unwrap();
    assert_eq!(history.events().len(), 5);
    assert!(matches!(
        history.events()[3].payload(),
        SessionEventPayload::RunCancellationRequested { .. }
    ));
    assert!(matches!(
        history.events()[4].payload(),
        SessionEventPayload::RunStateChanged {
            state: RunState::Cancelled,
            ..
        }
    ));
}

#[tokio::test]
async fn running_cancellation_finishes_with_output_and_event_order() {
    let (_data, store, session) = seeded_session().await;
    let app = RunApplication::new(store.clone(), super::UlidIdGenerator);
    let created = app
        .start_root_run(
            session.id().clone(),
            "running-cancel".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .unwrap();
    let run_id = created.value.run().run_id().clone();
    let running = app.begin_execution(run_id.clone()).await.unwrap();
    let tool_call_id = running.value.tool_calls()[0].tool_call_id().clone();
    let cancelling = app.request_cancellation(run_id.clone()).await.unwrap();
    assert_eq!(cancelling.value.run().state(), RunState::Cancelling);
    assert!(cancelling.value.tool_call(&tool_call_id).is_some());

    let cancelled = app
        .finish_cancellation(
            run_id,
            tool_call_id,
            SubprocessOutput::success("out", "err", 137),
        )
        .await
        .unwrap();
    assert_eq!(cancelled.value.run().state(), RunState::Cancelled);
    assert_eq!(
        cancelled.value.tool_calls()[0].state(),
        ToolCallState::Cancelled
    );
    assert_eq!(cancelled.events.len(), 4);
    assert!(matches!(
        cancelled.events[0].payload(),
        SessionEventPayload::ToolCallOutput { .. }
    ));
    assert!(matches!(
        cancelled.events[1].payload(),
        SessionEventPayload::ToolCallOutput { .. }
    ));
    assert!(matches!(
        cancelled.events[2].payload(),
        SessionEventPayload::ToolCallStateChanged {
            tool_call,
        } if tool_call.state() == ToolCallState::Cancelled
    ));
    assert!(matches!(
        cancelled.events[3].payload(),
        SessionEventPayload::RunStateChanged {
            state: RunState::Cancelled,
            ..
        }
    ));

    let history = store
        .list_session_events(session.id(), EventCursor::zero())
        .await
        .unwrap();
    assert!(matches!(
        history.events()[6].payload(),
        SessionEventPayload::RunCancellationRequested { .. }
    ));
    assert!(matches!(
        history.events()[11].payload(),
        SessionEventPayload::RunStateChanged {
            state: RunState::Cancelled,
            ..
        }
    ));
}

#[tokio::test]
async fn cancelling_run_keeps_active_root_unique_until_terminal() {
    let (_data, store, session) = seeded_session().await;
    let app = RunApplication::new(store, super::UlidIdGenerator);
    let created = app
        .start_root_run(
            session.id().clone(),
            "active-cancel".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .unwrap();
    let run_id = created.value.run().run_id().clone();
    app.begin_execution(run_id.clone()).await.unwrap();
    app.request_cancellation(run_id.clone()).await.unwrap();

    assert_eq!(
        app.start_root_run(
            session.id().clone(),
            "blocked-by-cancel".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await,
        Err(kiln_core::RunError::ActiveRootRunExists)
    );

    let tool_call_id = app.get_run(run_id.clone()).await.unwrap().tool_calls()[0]
        .tool_call_id()
        .clone();
    app.finish_cancellation(run_id, tool_call_id, SubprocessOutput::success("", "", 137))
        .await
        .unwrap();
    assert!(
        app.start_root_run(
            session.id().clone(),
            "after-cancel".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn failed_output_fails_both_tool_call_and_run() {
    let (_data, store, value) = seeded_session().await;
    let app = RunApplication::new(store, super::UlidIdGenerator);
    let created = app
        .start_root_run(
            value.id().clone(),
            "failed-run".to_owned(),
            ApprovalPolicy::FullAccess,
            test_scope(),
        )
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
