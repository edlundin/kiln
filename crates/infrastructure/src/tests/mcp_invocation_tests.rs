use super::*;
use kiln_core::*;
use std::{collections::BTreeMap, num::NonZeroUsize};

#[tokio::test]
async fn mcp_input_journal_keeps_dispatch_owned_and_interrupts_pending_input() {
    let (data, store, session) = seeded_session().await;
    let target = ready(&store, McpInstanceOwner::Core).await;
    let request = request(&store, &session, "input").await;
    let McpDispatchClaim::Acquired(permit) = claim_mcp_dispatch(&store, request, &target, limits())
        .await
        .unwrap()
    else {
        panic!()
    };
    let one = std::num::NonZeroU64::new(1).unwrap();
    let two = std::num::NonZeroU64::new(2).unwrap();
    let mut wake = store.subscribe_mcp_invocation_events();
    let McpInputMutation::Applied(first) = store
        .require_mcp_input(permit.record(), one, McpInputKind::Roots)
        .await
        .unwrap()
    else {
        panic!("fresh input must be applied")
    };
    assert_eq!(first.state, McpInputState::Required);
    assert!(wake.has_changed().unwrap());
    wake.borrow_and_update();
    assert_eq!(
        store
            .require_mcp_input(permit.record(), one, McpInputKind::Roots)
            .await
            .unwrap(),
        McpInputMutation::Existing(first.clone())
    );
    assert!(!wake.has_changed().unwrap());
    assert_eq!(
        store
            .require_mcp_input(permit.record(), one, McpInputKind::Sampling)
            .await
            .err(),
        Some(McpInvocationError::Conflict)
    );
    assert_eq!(
        store
            .require_mcp_input(permit.record(), two, McpInputKind::Sampling)
            .await
            .err(),
        Some(McpInvocationError::Busy)
    );
    assert!(matches!(
        store
            .begin_mcp_invocation(permit.request(), &target, limits())
            .await
            .unwrap(),
        McpInvocationMutation::Existing(_)
    ));
    let McpInputMutation::Applied(resolved) = store.resolve_mcp_input(&first).await.unwrap() else {
        panic!("fresh resolution must be applied")
    };
    assert_eq!(resolved.state, McpInputState::Resolved);
    assert_eq!(
        store.resolve_mcp_input(&first).await.unwrap(),
        McpInputMutation::Existing(resolved)
    );
    let McpInputMutation::Applied(second) = store
        .require_mcp_input(permit.record(), two, McpInputKind::Elicitation)
        .await
        .unwrap()
    else {
        panic!("next input must be applied")
    };
    store
        .interrupt_mcp_invocations(Some(&target.generation), NonZeroUsize::new(1).unwrap())
        .await
        .unwrap();
    assert_eq!(
        store.resolve_mcp_input(&second).await.err(),
        Some(McpInvocationError::Conflict)
    );
    let McpInputMutation::Existing(interrupted) = store
        .require_mcp_input(permit.record(), two, McpInputKind::Elicitation)
        .await
        .unwrap()
    else {
        panic!("interrupted retry is a receipt")
    };
    assert_eq!(interrupted.state, McpInputState::Interrupted);
    assert_eq!(
        interrupted.invocation.state,
        McpInvocationState::Interrupted
    );
    assert_eq!(
        store
            .require_mcp_input(
                permit.record(),
                std::num::NonZeroU64::new(3).unwrap(),
                McpInputKind::Roots
            )
            .await
            .err(),
        Some(McpInvocationError::Conflict)
    );
    let mut sql = store.connection.lock().await;
    let states: Vec<String> =
        sqlx::query_scalar("SELECT state FROM mcp_input_events ORDER BY sequence")
            .fetch_all(&mut *sql)
            .await
            .unwrap();
    assert_eq!(states, ["required", "resolved", "required", "interrupted"]);
    drop(sql);
    drop(store);
    let store = super::super::SqliteStore::open(data.path()).await.unwrap();
    let page = store
        .list_session_events(session.id(), EventCursor::zero())
        .await
        .unwrap();
    let inputs: Vec<_> = page
        .events()
        .iter()
        .filter_map(|event| {
            if let SessionEventPayload::McpInputStateChanged {
                run_id,
                tool_call_id,
                generation,
                ordinal,
                kind,
                state,
            } = event.payload()
            {
                assert_eq!(run_id, permit.request().tool_call().run_id());
                assert_eq!(tool_call_id, &permit.record().tool_call_id);
                assert_eq!(generation, &target.generation);
                Some((event.cursor(), ordinal.get(), *kind, *state))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        inputs
            .iter()
            .map(|(_, ordinal, kind, state)| (*ordinal, *kind, *state))
            .collect::<Vec<_>>(),
        [
            (1, McpInputKind::Roots, McpInputState::Required),
            (1, McpInputKind::Roots, McpInputState::Resolved),
            (2, McpInputKind::Elicitation, McpInputState::Required),
            (2, McpInputKind::Elicitation, McpInputState::Interrupted),
        ]
    );
    let replay = store
        .list_session_events(session.id(), inputs[2].0)
        .await
        .unwrap();
    assert_eq!(
        replay
            .events()
            .iter()
            .filter(|event| matches!(
                event.payload(),
                SessionEventPayload::McpInputStateChanged { .. }
            ))
            .count(),
        1
    );
    assert!(!format!("{inputs:?}").contains("private-payload"));
}

#[tokio::test]
async fn mcp_input_journal_rolls_back_and_rejects_lost_ownership() {
    let (_data, store, session) = seeded_session().await;
    let target = ready(&store, McpInstanceOwner::Core).await;
    let request = request(&store, &session, "input-rollback").await;
    let McpDispatchClaim::Acquired(permit) = claim_mcp_dispatch(&store, request, &target, limits())
        .await
        .unwrap()
    else {
        panic!()
    };
    let one = std::num::NonZeroU64::new(1).unwrap();
    assert_eq!(
        store
            .require_mcp_input(
                permit.record(),
                std::num::NonZeroU64::new(2).unwrap(),
                McpInputKind::Roots
            )
            .await
            .err(),
        Some(McpInvocationError::Conflict)
    );
    {
        let mut sql = store.connection.lock().await;
        sqlx::query("CREATE TRIGGER fixture_input_failure BEFORE INSERT ON session_events WHEN NEW.event_type = 'mcp.input_state_changed' BEGIN SELECT RAISE(ABORT, 'fixture rollback'); END").execute(&mut *sql).await.unwrap();
    }
    assert_eq!(
        store
            .require_mcp_input(permit.record(), one, McpInputKind::Roots)
            .await
            .err(),
        Some(McpInvocationError::Unavailable)
    );
    {
        let mut sql = store.connection.lock().await;
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mcp_inputs")
            .fetch_one(&mut *sql)
            .await
            .unwrap();
        assert_eq!(count, 0);
        sqlx::query("DROP TRIGGER fixture_input_failure")
            .execute(&mut *sql)
            .await
            .unwrap();
    }
    let McpInputMutation::Applied(input) = store
        .require_mcp_input(permit.record(), one, McpInputKind::Roots)
        .await
        .unwrap()
    else {
        panic!("rolled back attempt must not consume ordinal")
    };
    {
        let mut sql = store.connection.lock().await;
        sqlx::query("CREATE TRIGGER fixture_input_failure BEFORE INSERT ON mcp_input_events BEGIN SELECT RAISE(ABORT, 'fixture rollback'); END").execute(&mut *sql).await.unwrap();
    }
    assert_eq!(
        store.resolve_mcp_input(&input).await.err(),
        Some(McpInvocationError::Unavailable)
    );
    assert_eq!(
        store
            .finish_mcp_invocation(permit.record(), McpInvocationState::Interrupted)
            .await
            .err(),
        Some(McpInvocationError::Unavailable)
    );
    {
        let mut sql = store.connection.lock().await;
        let state: String = sqlx::query_scalar("SELECT state FROM mcp_invocations")
            .fetch_one(&mut *sql)
            .await
            .unwrap();
        assert_eq!(state, "dispatching");
        sqlx::query("DROP TRIGGER fixture_input_failure")
            .execute(&mut *sql)
            .await
            .unwrap();
    }
    store
        .transition_mcp_instance(&target, McpInstanceTransition::RequestStop)
        .await
        .unwrap();
    assert_eq!(
        store.resolve_mcp_input(&input).await.err(),
        Some(McpInvocationError::Conflict)
    );
    let mut wrong = permit.record().clone();
    wrong.generation = McpGenerationId::from_ulid(ulid::Ulid::generate());
    assert_eq!(
        store
            .require_mcp_input(&wrong, one, McpInputKind::Roots)
            .await
            .err(),
        Some(McpInvocationError::Conflict)
    );
    store
        .finish_mcp_invocation(permit.record(), McpInvocationState::Interrupted)
        .await
        .unwrap();
    assert_eq!(
        store
            .require_mcp_input(permit.record(), one, McpInputKind::Roots)
            .await
            .unwrap()
            .record()
            .state,
        McpInputState::Interrupted
    );
}

fn limits() -> McpDefinitionLimits {
    McpDefinitionLimits {
        max_key_bytes: 64,
        max_metadata_bytes: 4096,
        max_arguments: 4,
        max_argument_bytes: 128,
        max_environment: 4,
        max_endpoint_bytes: 128,
    }
}

#[cfg(unix)]
fn catalog_limits() -> kiln_mcp::McpCatalogLimits {
    // Fixture catalogue is one short page with one simple object schema.
    kiln_mcp::McpCatalogLimits {
        max_pages: NonZeroUsize::new(1).unwrap(),
        max_entries: NonZeroUsize::new(1).unwrap(),
        max_bytes: NonZeroUsize::new(1024).unwrap(),
        max_regex_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        max_regex_backtracks: NonZeroUsize::new(10_000).unwrap(),
    }
}

async fn request(
    store: &super::super::SqliteStore,
    session: &Session,
    key: &str,
) -> ModelToolExecutionRequest<McpCommand> {
    request_operation(
        store,
        session,
        key,
        serde_json::json!({"kind":"tool","name":"write","arguments":{"text":"private-payload"}}),
    )
    .await
}

async fn request_operation(
    store: &super::super::SqliteStore,
    session: &Session,
    key: &str,
    operation: serde_json::Value,
) -> ModelToolExecutionRequest<McpCommand> {
    request_native(
        store,
        session,
        key,
        "mcp_call",
        serde_json::json!({"server_id":"fixture","definition_version":1,"operation":operation}),
    )
    .await
}

async fn request_native(
    store: &super::super::SqliteStore,
    session: &Session,
    key: &str,
    name: &str,
    arguments: serde_json::Value,
) -> ModelToolExecutionRequest<McpCommand> {
    let ids = super::super::UlidIdGenerator;
    let runs = RunApplication::new(store.clone(), ids);
    let run = runs
        .start_root_run(
            session.id().clone(),
            key.into(),
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
            idempotency_key: key.into(),
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
            idempotency_key: key.into(),
        })
        .await
        .unwrap()
        .value;
    let tool = McpTools::new(
        NonZeroUsize::new(4096).unwrap(),
        ModelToolCatalogLimits {
            max_tools: 3,
            max_definition_bytes: 8192,
            max_total_definition_bytes: 8192,
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
            name: name.into(),
            arguments: arguments.as_object().unwrap().clone(),
        }],
        ModelToolRequestLimits {
            max_requests: 1,
            max_provider_call_id_bytes: 64,
            max_name_bytes: 64,
            max_arguments_bytes: 4096,
            max_total_arguments_bytes: 4096,
        },
    )
    .unwrap();
    let usage = ProviderUsageUpdate::new(
        ProviderUsageMetadata {
            update_id: key.into(),
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
    assert!(
        app.claim_tool_call(batch, 0, adopted.tool_call_id.clone())
            .await
            .is_err(),
        "pending approval cannot produce a claim"
    );
    let snapshot = runs.get_run(run_id).await.unwrap();
    runs.decide_approval(
        snapshot.approvals()[0].approval_id().clone(),
        ApprovalState::Approved,
        format!("{key}-approve"),
    )
    .await
    .unwrap();
    let batch = app
        .resolve_tool_requests(invocation.invocation_id().clone(), &tool)
        .await
        .unwrap();
    match app
        .claim_tool_call(batch, 0, adopted.tool_call_id)
        .await
        .unwrap()
    {
        ModelToolExecutionClaim::Applied { request, .. } => request,
        _ => panic!("fresh native claim required"),
    }
}

async fn register(
    store: &super::super::SqliteStore,
    owner: McpInstanceOwner,
    policy: McpProtocolPolicy,
) -> McpInstanceKey {
    let definition = McpServerDefinition::new(
        SharedMcpServerInput {
            id: SharedConfigurationKey::parse("fixture", 64).unwrap(),
            enabled: true,
            transport: SharedMcpTransport::Stdio {
                runtime_binding: SharedConfigurationKey::parse("runtime", 64).unwrap(),
                arguments: vec![],
                environment: BTreeMap::new(),
            },
        },
        policy,
        owner.scope(),
        None,
        limits(),
    )
    .unwrap();
    store
        .register_mcp_definition(&definition, 0, "definition", limits())
        .await
        .unwrap();
    McpInstanceKey::new(&definition, owner, 4096).unwrap()
}

async fn ready(store: &super::super::SqliteStore, owner: McpInstanceOwner) -> McpInstanceRecord {
    let key = register(store, owner, McpProtocolPolicy::Auto).await;
    let generation = McpGenerationId::from_ulid(ulid::Ulid::generate());
    let McpInstanceClaim::Acquired(record) = store
        .claim_mcp_instance(&key, 1, &generation, limits())
        .await
        .unwrap()
    else {
        panic!()
    };
    store
        .transition_mcp_instance(
            &record,
            McpInstanceTransition::Ready(McpProtocolVersion::V20260728),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn mcp_dispatch_requires_approved_claim_and_never_reacquires_after_interruption() {
    let (data, store, session) = seeded_session().await;
    let target = ready(&store, McpInstanceOwner::Core).await;
    let first = request(&store, &session, "first").await;
    let failed_wake = store.subscribe_mcp_invocation_events();
    let mut stale = target.clone();
    stale.state_version += 1;
    assert_eq!(
        store
            .begin_mcp_invocation(&first, &stale, limits())
            .await
            .err(),
        Some(McpInvocationError::GenerationChanged)
    );
    {
        let mut sql = store.connection.lock().await;
        sqlx::query("CREATE TRIGGER fixture_mcp_invocation_failure BEFORE INSERT ON session_events WHEN NEW.event_type = 'mcp.invocation_state_changed' BEGIN SELECT RAISE(ABORT, 'fixture rollback'); END")
            .execute(&mut *sql).await.unwrap();
    }
    assert_eq!(
        store
            .begin_mcp_invocation(&first, &target, limits())
            .await
            .err(),
        Some(McpInvocationError::Unavailable)
    );
    {
        let mut sql = store.connection.lock().await;
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mcp_invocations")
            .fetch_one(&mut *sql)
            .await
            .unwrap();
        assert_eq!(count, 0);
        let audit_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mcp_invocation_events")
            .fetch_one(&mut *sql)
            .await
            .unwrap();
        assert_eq!(audit_count, 0);
        assert!(!failed_wake.has_changed().unwrap());
        sqlx::query("DROP TRIGGER fixture_mcp_invocation_failure")
            .execute(&mut *sql)
            .await
            .unwrap();
    }
    let McpDispatchClaim::Acquired(permit) = claim_mcp_dispatch(&store, first, &target, limits())
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(matches!(
        store
            .begin_mcp_invocation(permit.request(), &target, limits())
            .await
            .unwrap(),
        McpInvocationMutation::Existing(_)
    ));
    let second_session =
        SessionApplication::new(store.clone(), store.clone(), super::super::UlidIdGenerator)
            .create_session(session.workspace_id().clone())
            .await
            .unwrap();
    let second = request(&store, &second_session, "second").await;
    assert_eq!(
        store
            .begin_mcp_invocation(&second, &target, limits())
            .await
            .err(),
        Some(McpInvocationError::Busy)
    );
    let original = permit.record().clone();
    let original_run = permit.request().tool_call().run_id().clone();
    let before_restart = store
        .list_session_events(session.id(), EventCursor::zero())
        .await
        .unwrap();
    let dispatched = before_restart
        .events()
        .iter()
        .find(|event| {
            matches!(
                event.payload(),
                SessionEventPayload::McpInvocationStateChanged { .. }
            )
        })
        .unwrap()
        .clone();
    drop(store);
    let store = super::super::SqliteStore::open(data.path()).await.unwrap();
    let mut wake = store.subscribe_mcp_invocation_events();
    store
        .interrupt_mcp_instances_after_restart(NonZeroUsize::new(1).unwrap())
        .await
        .unwrap();
    assert_eq!(
        store
            .interrupt_mcp_invocations(None, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .interrupt_mcp_invocations(None, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap(),
        0
    );
    assert!(matches!(
        store
            .begin_mcp_invocation(permit.request(), &target, limits())
            .await
            .unwrap(),
        McpInvocationMutation::Existing(McpInvocationRecord {
            state: McpInvocationState::Interrupted,
            ..
        })
    ));
    drop(permit);
    assert_eq!(
        store
            .finish_mcp_invocation(&original, McpInvocationState::Completed)
            .await
            .err(),
        Some(McpInvocationError::Conflict)
    );
    assert_eq!(
        store
            .begin_mcp_invocation(&second, &target, limits())
            .await
            .err(),
        Some(McpInvocationError::GenerationChanged)
    );
    let mut sql = store.connection.lock().await;
    let states: Vec<String> =
        sqlx::query_scalar("SELECT state FROM mcp_invocation_events ORDER BY sequence")
            .fetch_all(&mut *sql)
            .await
            .unwrap();
    assert_eq!(states, ["dispatching", "interrupted"]);
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mcp_invocations")
        .fetch_one(&mut *sql)
        .await
        .unwrap();
    assert_eq!(rows, 1);
    drop(sql);
    assert!(wake.has_changed().unwrap());
    wake.borrow_and_update();
    let replay = store
        .list_session_events(session.id(), EventCursor::zero())
        .await
        .unwrap();
    let receipts: Vec<_> = replay
        .events()
        .iter()
        .filter(|event| {
            matches!(
                event.payload(),
                SessionEventPayload::McpInvocationStateChanged { .. }
            )
        })
        .collect();
    assert_eq!(receipts.len(), 2);
    assert_eq!(*receipts[0], dispatched);
    let SessionEventPayload::McpInvocationStateChanged { run_id, invocation } =
        receipts[1].payload()
    else {
        unreachable!()
    };
    assert_eq!(run_id, &original_run);
    assert_eq!(invocation.state, McpInvocationState::Interrupted);
    assert_eq!(invocation.tool_call_id, original.tool_call_id);
    assert_eq!(invocation.generation, original.generation);
    assert!(receipts[1].cursor() > dispatched.cursor());
    assert!(!format!("{:?}", receipts).contains("private-payload"));
    assert_eq!(
        store
            .interrupt_mcp_invocations(None, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap(),
        0
    );
    assert!(!wake.has_changed().unwrap());
}

#[tokio::test]
async fn mcp_dispatch_rechecks_definition_and_journals_terminal_retry_once() {
    let (_data, store, session) = seeded_session().await;
    let mut wake = store.subscribe_mcp_invocation_events();
    let target = ready(&store, McpInstanceOwner::Core).await;
    let request = request(&store, &session, "finish").await;
    let McpDispatchClaim::Acquired(permit) = claim_mcp_dispatch(&store, request, &target, limits())
        .await
        .unwrap()
    else {
        panic!()
    };
    let first = store
        .finish_mcp_invocation(permit.record(), McpInvocationState::Completed)
        .await
        .unwrap();
    assert!(wake.has_changed().unwrap());
    wake.borrow_and_update();
    assert_eq!(
        store
            .finish_mcp_invocation(permit.record(), McpInvocationState::Completed)
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        store
            .finish_mcp_invocation(permit.record(), McpInvocationState::Failed)
            .await
            .err(),
        Some(McpInvocationError::Conflict)
    );
    assert!(matches!(
        store
            .begin_mcp_invocation(permit.request(), &target, limits())
            .await
            .unwrap(),
        McpInvocationMutation::Existing(_)
    ));
    assert!(!wake.has_changed().unwrap());
    let page = store
        .list_session_events(session.id(), EventCursor::zero())
        .await
        .unwrap();
    let states: Vec<_> = page
        .events()
        .iter()
        .filter_map(|event| match event.payload() {
            SessionEventPayload::McpInvocationStateChanged { run_id, invocation } => {
                assert_eq!(run_id, permit.request().tool_call().run_id());
                assert_eq!(invocation.tool_call_id, permit.record().tool_call_id);
                Some(invocation.state)
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        states,
        [
            McpInvocationState::Dispatching,
            McpInvocationState::Completed
        ]
    );
    let second_session =
        SessionApplication::new(store.clone(), store.clone(), super::super::UlidIdGenerator)
            .create_session(session.workspace_id().clone())
            .await
            .unwrap();
    let second = self::request(&store, &second_session, "changed").await;
    let definition = store
        .get_mcp_definition(target.key.definition_id(), limits())
        .await
        .unwrap()
        .unwrap();
    store
        .register_mcp_definition(&definition.definition, 1, "bump", limits())
        .await
        .unwrap();
    assert_eq!(
        store
            .begin_mcp_invocation(&second, &target, limits())
            .await
            .err(),
        Some(McpInvocationError::DefinitionChanged)
    );
    let mut sql = store.connection.lock().await;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mcp_invocation_events")
        .fetch_one(&mut *sql)
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn mcp_dispatch_rejects_another_sessions_generation() {
    let (_data, store, session) = seeded_session().await;
    let other =
        SessionApplication::new(store.clone(), store.clone(), super::super::UlidIdGenerator)
            .create_session(session.workspace_id().clone())
            .await
            .unwrap();
    let target = ready(&store, McpInstanceOwner::Session(other.id().clone())).await;
    let request = request(&store, &session, "scope").await;
    assert_eq!(
        store
            .begin_mcp_invocation(&request, &target, limits())
            .await
            .err(),
        Some(McpInvocationError::ScopeMismatch)
    );
    let mut sql = store.connection.lock().await;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mcp_invocations")
        .fetch_one(&mut *sql)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[cfg(unix)]
#[tokio::test]
async fn mcp_live_dispatch_sends_once_and_retires_uncertain_processes() {
    use kiln_mcp::{
        StdioCallError, StdioCallLimits, StdioGeneration, StdioGenerationLaunch, StdioProcessConfig,
    };
    use std::{sync::Arc, time::Duration};
    use tokio::{sync::oneshot, time::Instant};
    // Match the existing real-process fixtures' five-second scheduling allowance.
    let allowance = Duration::from_secs(5);
    let script = r#"
import json, os, sys, time
mode = sys.argv[1]
open('pid', 'w').write(str(os.getpid()))
for line in sys.stdin:
    request = json.loads(line)
    method = request.get('method')
    if method == 'initialize':
        result = {'protocolVersion':'2025-11-25','capabilities':{'tools':{}},'serverInfo':{'name':'fixture','version':'1'}}
    elif method == 'tools/list':
        with open('lists', 'a') as lists: lists.write('list\n')
        result = {'tools':[{'name':'write','inputSchema':{'type':'object','properties':{'text':{'type':'string'}},'required':['text'],'additionalProperties':False}}]}
        if mode == 'invalid_arguments': result['tools'][0]['inputSchema']['properties']['text']['type'] = 'integer'
        if mode == 'unknown_tool': result['tools'] = []
        if mode == 'catalog_limit': result['nextCursor'] = 'more'
        if mode in ('invalid_output', 'valid_output'): result['tools'][0]['outputSchema'] = {'type':'object','required':['text'],'properties':{'text':{'type':'string'}}}
    elif method == 'tools/call':
        with open('calls', 'a') as calls: calls.write(json.dumps(request['params'])+'\n')
        if mode == 'disconnect': sys.exit(0)
        if mode in ('cancel','deadline'): time.sleep(60)
        if mode == 'server_error':
            print(json.dumps({'jsonrpc':'2.0','id':request['id'],'error':{'code':-32602,'message':'fixture error'}}), flush=True)
            continue
        text = 'private-result' * (400 if mode == 'capture' else 1)
        result = {'content':[{'type':'text','text':text}]}
        if mode == 'invalid_output': result['structuredContent'] = {'text':42}
        if mode == 'valid_output': result['structuredContent'] = {'text':'valid'}
    else:
        continue
    print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':result}), flush=True)
"#;
    for mode in [
        "complete",
        "capture",
        "server_error",
        "invalid_arguments",
        "unknown_tool",
        "catalog_limit",
        "invalid_output",
        "valid_output",
        "oversize",
        "disconnect",
        "cancel",
        "deadline",
    ] {
        let (data, store, session) = seeded_session().await;
        let key = register(
            &store,
            McpInstanceOwner::Core,
            McpProtocolPolicy::Pinned(McpProtocolVersion::V20251125),
        )
        .await;
        let mut owner = StdioGeneration::spawn(
            Arc::new(store.clone()),
            StdioGenerationLaunch {
                host_binding_version: None,
                key,
                definition_version: 1,
                generation: McpGenerationId::from_ulid(ulid::Ulid::generate()),
                definition_limits: limits(),
                startup_deadline: Instant::now() + allowance,
                process: StdioProcessConfig {
                    executable: "/usr/bin/python3".into(),
                    arguments: vec!["-c".into(), script.into(), mode.into()],
                    working_directory: std::fs::File::open(data.path()).unwrap().into(),
                    environment: BTreeMap::new(),
                    max_frame_bytes: NonZeroUsize::new(8192).unwrap(),
                    shutdown_grace: Duration::ZERO,
                },
            },
        );
        let target = owner.wait_ready().await.unwrap();
        let request = request(&store, &session, mode).await;
        let McpDispatchClaim::Acquired(permit) =
            claim_mcp_dispatch(&store, request, &target, limits())
                .await
                .unwrap()
        else {
            panic!()
        };
        let record = permit.record().clone();
        let (cancel, cancelled) = oneshot::channel();
        let result = {
            let call = owner.dispatch(
                permit,
                StdioCallLimits {
                    catalog: catalog_limits(),
                    deadline: Instant::now() + allowance,
                    max_result_bytes: NonZeroUsize::new(if mode == "oversize" { 1 } else { 8192 })
                        .unwrap(),
                },
                cancelled,
            );
            let trigger = async {
                if mode == "cancel" {
                    tokio::time::timeout(allowance, async {
                        while !data.path().join("calls").exists() {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    cancel.send(()).unwrap();
                } else {
                    // Keep the cancellation sender alive until the call completes.
                    std::future::pending::<()>().await;
                    drop(cancel);
                }
            };
            tokio::pin!(call, trigger);
            tokio::select! {
                result = &mut call => result,
                _ = &mut trigger => call.as_mut().await,
            }
        };
        let expected = match mode {
            "complete" | "capture" | "valid_output" => {
                assert!(
                    String::from_utf8(result.unwrap().json)
                        .unwrap()
                        .contains("private-result")
                );
                McpInvocationState::Completed
            }
            "invalid_arguments" | "unknown_tool" | "catalog_limit" => {
                let error = match mode {
                    "invalid_arguments" => kiln_mcp::McpCatalogError::InvalidArguments,
                    "unknown_tool" => kiln_mcp::McpCatalogError::UnknownTool,
                    _ => kiln_mcp::McpCatalogError::LimitExceeded,
                };
                assert_eq!(result.err(), Some(StdioCallError::Catalog(error)));
                McpInvocationState::Failed
            }
            "invalid_output" => {
                assert_eq!(result.err(), Some(StdioCallError::InvalidOutput));
                McpInvocationState::Failed
            }
            "server_error" => {
                assert_eq!(result.err(), Some(StdioCallError::Server));
                McpInvocationState::Failed
            }
            "oversize" => {
                assert_eq!(result.err(), Some(StdioCallError::ResultTooLarge));
                McpInvocationState::Failed
            }
            _ => {
                assert_eq!(result.err(), Some(StdioCallError::Interrupted));
                McpInvocationState::Interrupted
            }
        };
        assert_eq!(
            std::fs::read_to_string(data.path().join("calls"))
                .unwrap_or_default()
                .lines()
                .count(),
            if matches!(mode, "invalid_arguments" | "unknown_tool" | "catalog_limit") {
                0
            } else {
                1
            },
            "{mode} must never replay or send rejected input"
        );
        if expected == McpInvocationState::Interrupted {
            assert_ne!(
                store
                    .get_mcp_instance(&target.key)
                    .await
                    .unwrap()
                    .unwrap()
                    .observed,
                McpObservedState::Ready,
                "retirement must precede releasing the interrupted invocation slot"
            );
        }
        if matches!(mode, "complete" | "capture") {
            let next_session = SessionApplication::new(
                store.clone(),
                store.clone(),
                super::super::UlidIdGenerator,
            )
            .create_session(session.workspace_id().clone())
            .await
            .unwrap();
            let next = self::request(&store, &next_session, "reuse").await;
            let McpDispatchClaim::Acquired(permit) =
                claim_mcp_dispatch(&store, next, &target, limits())
                    .await
                    .unwrap()
            else {
                panic!()
            };
            let tool_call_id = permit.record().tool_call_id.clone();
            let artifacts = super::super::FileArtifactStore::open(data.path()).unwrap();
            let (_cancel, cancelled) = oneshot::channel();
            let next = owner
                .dispatch_tool_call(
                    permit,
                    StdioCallLimits {
                        catalog: catalog_limits(),
                        deadline: Instant::now() + allowance,
                        max_result_bytes: NonZeroUsize::new(8192).unwrap(),
                    },
                    cancelled,
                    |bytes| std::future::ready(artifacts.store(&bytes, TOOL_OUTPUT_MEDIA_TYPE)),
                )
                .await
                .unwrap();
            assert_eq!(next.state(), ToolCallState::Completed);
            if mode == "capture" {
                assert!(next.stdout().is_none());
                let artifact = next.stdout_artifact().unwrap();
                assert!(artifact.size() > INLINE_TOOL_OUTPUT_LIMIT as u64);
                let stored = artifacts.read(artifact.content_hash()).unwrap().unwrap();
                assert!(
                    String::from_utf8(stored)
                        .unwrap()
                        .contains("private-result")
                );
            } else {
                assert!(next.stdout().unwrap().contains("private-result"));
            }
            let mutation = ProviderApplication::new(store.clone(), super::super::UlidIdGenerator)
                .finish_tool_call(&tool_call_id, &next)
                .await
                .unwrap();
            assert!(!mutation.events.is_empty());
            let (_, stored) = store.get_tool_call(&tool_call_id).await.unwrap().unwrap();
            assert_eq!(stored.state(), ToolCallState::Completed);
            assert_eq!(stored.stdout_artifact(), next.stdout_artifact());
            assert_eq!(
                std::fs::read_to_string(data.path().join("calls"))
                    .unwrap()
                    .lines()
                    .count(),
                2,
                "two distinct Runs reuse the same process for two distinct claims"
            );
        }
        owner.stop().await.unwrap();
        let pid = rustix::process::Pid::from_raw(
            std::fs::read_to_string(data.path().join("pid"))
                .unwrap()
                .parse()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            rustix::process::test_kill_process(pid),
            Err(rustix::io::Errno::SRCH)
        );
        let mut sql = store.connection.lock().await;
        let state: String =
            sqlx::query_scalar("SELECT state FROM mcp_invocations WHERE tool_call_id = ?")
                .bind(record.tool_call_id.as_str())
                .fetch_one(&mut *sql)
                .await
                .unwrap();
        assert_eq!(state, expected.as_str(), "{mode}");
    }
}

#[cfg(unix)]
#[tokio::test]
async fn mcp_broker_prepares_approved_scope_and_reuses_single_dispatch_owner() {
    use kiln_mcp::{
        StdioBrokerError, StdioBrokerLimits, StdioCallLimits, StdioRegistry, execute_stdio_call,
    };
    use std::{
        os::unix::fs::{MetadataExt, PermissionsExt},
        sync::Arc,
        time::Duration,
    };
    use tokio::{sync::oneshot, time::Instant};
    struct NoSecrets;
    impl McpSecretStore for NoSecrets {
        async fn put_at(
            &self,
            _: &McpSecretBinding,
            _: SecretValue,
        ) -> Result<(), SecretStoreError> {
            panic!("no vault writes")
        }
        async fn get(&self, _: &McpSecretBinding) -> Result<SecretValue, SecretStoreError> {
            panic!("no credentials configured")
        }
        async fn delete(&self, _: &McpSecretBinding) -> Result<(), SecretStoreError> {
            panic!("no vault deletion")
        }
    }
    let (data, store, session) = seeded_session().await;
    let path = std::fs::canonicalize(data.path()).unwrap();
    let stat = std::fs::metadata(&path).unwrap();
    let identity = FilesystemIdentity::new(format!("unix:{}:{}", stat.dev(), stat.ino())).unwrap();
    {
        let mut sql = store.connection.lock().await;
        sqlx::query("UPDATE workspace_roots SET canonical_path = ?, git_common_directory_path = ?, filesystem_identity = ? WHERE workspace_root_id = ?")
            .bind(path.to_str().unwrap()).bind(path.join(".git").to_str().unwrap())
            .bind(identity.as_str()).bind(test_scope().workspace_root_id().as_str())
            .execute(&mut *sql).await.unwrap();
    }
    let directory = WorkspaceCheckout::from_resolved_paths(
        session.workspace_id().clone(),
        test_scope().workspace_root_id().clone(),
        "",
        path.to_str().unwrap(),
        path.join(".git").to_str().unwrap(),
        identity,
    )
    .unwrap();
    let key = register(
        &store,
        McpInstanceOwner::Core,
        McpProtocolPolicy::Pinned(McpProtocolVersion::V20251125),
    )
    .await;
    let initial = request(&store, &session, "broker-initial").await;
    assert_eq!(
        store.inspect_mcp_launch(&initial, limits()).await.err(),
        Some(McpInvocationError::NotFound)
    );
    let executable = path.join("server");
    std::fs::write(&executable, r#"#!/usr/bin/python3
import json, os, sys
with open('starts', 'a') as starts: starts.write(str(os.getpid())+'\n')
for line in sys.stdin:
    request = json.loads(line)
    if request.get('method') == 'initialize':
        result = {'protocolVersion':'2025-11-25','capabilities':{'tools':{}},'serverInfo':{'name':'fixture','version':'1'}}
    elif request.get('method') == 'tools/list':
        result = {'tools':[{'name':'write','inputSchema':{'type':'object','properties':{'text':{'type':'string'}},'required':['text'],'additionalProperties':False}}]}
    elif request.get('method') == 'tools/call':
        with open('calls', 'a') as calls: calls.write(request['params']['name']+'\n')
        result = {'content':[{'type':'text','text':'broker-result'}]}
    else: continue
    print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':result}), flush=True)
"#).unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let instance = KilnInstanceId::from_ulid(ulid::Ulid::generate());
    let bindings = McpHostBindings::new(
        key.clone(),
        McpHostBindingInput {
            instance_id: instance.clone(),
            definition_version: 1,
            runtime_binding: SharedConfigurationKey::parse("runtime", 64).unwrap(),
            executable: executable.to_str().unwrap().into(),
            working_directory: Some((&directory).into()),
            arguments: BTreeMap::new(),
            environment: BTreeMap::new(),
        },
        limits(),
    )
    .unwrap();
    store
        .publish_mcp_host_bindings(&bindings, 0, limits())
        .await
        .unwrap();
    // Metadata published for an uninitialized/different host is not launch authority.
    assert_eq!(
        store.inspect_mcp_launch(&initial, limits()).await.err(),
        Some(McpInvocationError::DefinitionChanged)
    );
    store
        .initialize_configuration_instance(instance)
        .await
        .unwrap();
    assert_eq!(
        store
            .inspect_mcp_launch(&initial, limits())
            .await
            .unwrap()
            .directory,
        directory
    );
    let registry = StdioRegistry::new(Arc::new(store.clone()), NonZeroUsize::new(1).unwrap());
    let budgets = || StdioBrokerLimits {
        generation: McpGenerationId::from_ulid(ulid::Ulid::generate()),
        definition: limits(),
        max_resolved_bytes: NonZeroUsize::new(4096).unwrap(),
        max_frame_bytes: NonZeroUsize::new(1024).unwrap(),
        shutdown_grace: Duration::ZERO,
        startup_deadline: Instant::now() + Duration::from_secs(5),
        call: StdioCallLimits {
            catalog: catalog_limits(),
            deadline: Instant::now() + Duration::from_secs(5),
            max_result_bytes: NonZeroUsize::new(1024).unwrap(),
        },
    };
    let artifacts = super::super::FileArtifactStore::open(data.path()).unwrap();
    let next_session =
        SessionApplication::new(store.clone(), store.clone(), super::super::UlidIdGenerator)
            .create_session(session.workspace_id().clone())
            .await
            .unwrap();
    let second = request(&store, &next_session, "broker-reuse").await;
    for request in [initial, second] {
        let tool_id = request.tool_call().tool_call_id().clone();
        let (_cancel, cancelled) = oneshot::channel();
        let result = execute_stdio_call(
            &registry,
            request,
            &NoSecrets,
            budgets(),
            cancelled,
            |checkout| std::future::ready(super::super::pin_mcp_working_directory(&checkout)),
            |bytes| std::future::ready(artifacts.store(&bytes, TOOL_OUTPUT_MEDIA_TYPE)),
        )
        .await
        .unwrap();
        assert_eq!(result.state(), ToolCallState::Completed);
        assert!(result.stdout().unwrap().contains("broker-result"));
        ProviderApplication::new(store.clone(), super::super::UlidIdGenerator)
            .finish_tool_call(&tool_id, &result)
            .await
            .unwrap();
    }
    assert_eq!(
        std::fs::read_to_string(path.join("starts"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert_eq!(
        std::fs::read_to_string(path.join("calls"))
            .unwrap()
            .lines()
            .count(),
        2
    );
    let cancel_session =
        SessionApplication::new(store.clone(), store.clone(), super::super::UlidIdGenerator)
            .create_session(session.workspace_id().clone())
            .await
            .unwrap();
    let cancelled_request = request(&store, &cancel_session, "cancel-during-preparation").await;
    let run_id = cancelled_request.tool_call().run_id().clone();
    let (_cancel, cancelled) = oneshot::channel();
    let result = execute_stdio_call(
        &registry,
        cancelled_request,
        &NoSecrets,
        budgets(),
        cancelled,
        |checkout| {
            let store = store.clone();
            async move {
                RunApplication::new(store, super::super::UlidIdGenerator)
                    .request_cancellation(run_id)
                    .await
                    .unwrap();
                super::super::pin_mcp_working_directory(&checkout)
            }
        },
        |bytes| std::future::ready(artifacts.store(&bytes, TOOL_OUTPUT_MEDIA_TYPE)),
    )
    .await;
    assert_eq!(
        result.err(),
        Some(StdioBrokerError::Preparation(
            McpInvocationError::InvalidRequest
        ))
    );
    let mismatch_session =
        SessionApplication::new(store.clone(), store.clone(), super::super::UlidIdGenerator)
            .create_session(session.workspace_id().clone())
            .await
            .unwrap();
    let mismatch = request(&store, &mismatch_session, "mismatched-directory").await;
    let target = store.get_mcp_instance(&key).await.unwrap().unwrap();
    let mut unbound = target.clone();
    unbound.host_binding_version = None;
    assert_eq!(
        store
            .begin_mcp_invocation(&mismatch, &unbound, limits())
            .await
            .err(),
        Some(McpInvocationError::GenerationChanged)
    );
    {
        let mut sql = store.connection.lock().await;
        sqlx::query("UPDATE workspace_roots SET filesystem_identity = 'replaced' WHERE workspace_root_id = ?")
            .bind(test_scope().workspace_root_id().as_str()).execute(&mut *sql).await.unwrap();
    }
    assert_eq!(
        store
            .begin_mcp_invocation(&mismatch, &target, limits())
            .await
            .err(),
        Some(McpInvocationError::ScopeMismatch)
    );
    let (_cancel, cancelled) = oneshot::channel();
    let result = execute_stdio_call(
        &registry,
        mismatch,
        &NoSecrets,
        budgets(),
        cancelled,
        |_| async { panic!("preflight must reject before filesystem access") },
        |bytes| std::future::ready(artifacts.store(&bytes, TOOL_OUTPUT_MEDIA_TYPE)),
    )
    .await;
    assert_eq!(
        result.err(),
        Some(StdioBrokerError::Preparation(
            McpInvocationError::ScopeMismatch
        ))
    );
    assert_eq!(
        std::fs::read_to_string(path.join("calls"))
            .unwrap()
            .lines()
            .count(),
        2
    );
    assert!(registry.shutdown().await.into_iter().all(|r| r.is_ok()));
}

#[cfg(unix)]
#[tokio::test]
async fn mcp_prompt_and_resource_dispatch_validate_without_inventing_a_resource_allowlist() {
    use kiln_mcp::{
        McpCatalogError, StdioCallError, StdioCallLimits, StdioGeneration, StdioGenerationLaunch,
        StdioProcessConfig,
    };
    use std::{sync::Arc, time::Duration};
    use tokio::{sync::oneshot, time::Instant};
    let script = r#"
import json, sys
mode = sys.argv[1]
for line in sys.stdin:
    request = json.loads(line)
    method = request.get('method')
    if method == 'initialize':
        caps = {} if mode == 'unsupported' else {'resources':{},'prompts':{}}
        result = {'protocolVersion':'2025-11-25','capabilities':caps,'serverInfo':{'name':'fixture','version':'1'}}
    elif method == 'prompts/list':
        with open('lists', 'a') as log: log.write('list\n')
        prompt = {'name':'review','arguments':[{'name':'language','required':True}]}
        if mode in ('pagination','changed') and not request.get('params', {}).get('cursor'):
            if mode == 'changed': print(json.dumps({'jsonrpc':'2.0','method':'notifications/prompts/list_changed'}), flush=True)
            result = {'prompts':[], 'nextCursor':'second'}
        elif mode == 'duplicate': result = {'prompts':[prompt,prompt]}
        else: result = {'prompts':[prompt]}
    elif method == 'prompts/get':
        with open('operations', 'a') as log: log.write(json.dumps(request['params'])+'\n')
        result = {'messages':[{'role':'user','content':{'type':'text','text':'untrusted prompt content'}}]}
    elif method == 'resources/read':
        with open('operations', 'a') as log: log.write(json.dumps(request['params'])+'\n')
        uri = 'relative/bad' if mode == 'bad_output' else request['params']['uri']
        result = {'contents':[{'uri':uri,'text':'server resource content'}]}
    elif method == 'resources/list':
        raise AssertionError('unlisted resource links are valid; no allowlist probe')
    else: continue
    print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':result}), flush=True)
"#;
    for (mode, operation, expected) in [
        (
            "prompt",
            serde_json::json!({"kind":"prompt","name":"review","arguments":{"language":"en"}}),
            None,
        ),
        (
            "pagination",
            serde_json::json!({"kind":"prompt","name":"review","arguments":{"language":"en"}}),
            None,
        ),
        (
            "changed",
            serde_json::json!({"kind":"prompt","name":"review","arguments":{"language":"en"}}),
            Some(StdioCallError::Catalog(McpCatalogError::CatalogChanged)),
        ),
        (
            "missing",
            serde_json::json!({"kind":"prompt","name":"review","arguments":{}}),
            Some(StdioCallError::Catalog(McpCatalogError::InvalidArguments)),
        ),
        (
            "extra",
            serde_json::json!({"kind":"prompt","name":"review","arguments":{"language":"en","extra":"x"}}),
            Some(StdioCallError::Catalog(McpCatalogError::InvalidArguments)),
        ),
        (
            "unknown",
            serde_json::json!({"kind":"prompt","name":"unknown","arguments":{}}),
            Some(StdioCallError::Catalog(McpCatalogError::UnknownPrompt)),
        ),
        (
            "duplicate",
            serde_json::json!({"kind":"prompt","name":"review","arguments":{"language":"en"}}),
            Some(StdioCallError::Catalog(McpCatalogError::InvalidCatalog)),
        ),
        (
            "resource",
            serde_json::json!({"kind":"resource","uri":"notes://host/a%20b?x=1#part"}),
            None,
        ),
        (
            "file_uri",
            serde_json::json!({"kind":"resource","uri":"file:///not-a-local-read"}),
            None,
        ),
        (
            "invalid_uri",
            serde_json::json!({"kind":"resource","uri":"notes://host/%zz"}),
            Some(StdioCallError::Catalog(McpCatalogError::InvalidUri)),
        ),
        (
            "unsupported",
            serde_json::json!({"kind":"resource","uri":"notes://host/document"}),
            Some(StdioCallError::Catalog(McpCatalogError::Unsupported)),
        ),
        (
            "bad_output",
            serde_json::json!({"kind":"resource","uri":"notes://host/document"}),
            Some(StdioCallError::InvalidOutput),
        ),
    ] {
        let (data, store, session) = seeded_session().await;
        let key = register(
            &store,
            McpInstanceOwner::Core,
            McpProtocolPolicy::Pinned(McpProtocolVersion::V20251125),
        )
        .await;
        let mut owner = StdioGeneration::spawn(
            Arc::new(store.clone()),
            StdioGenerationLaunch {
                key,
                definition_version: 1,
                host_binding_version: None,
                generation: McpGenerationId::from_ulid(ulid::Ulid::generate()),
                definition_limits: limits(),
                startup_deadline: Instant::now() + Duration::from_secs(5),
                process: StdioProcessConfig {
                    executable: "/usr/bin/python3".into(),
                    arguments: vec!["-c".into(), script.into(), mode.into()],
                    working_directory: std::fs::File::open(data.path()).unwrap().into(),
                    environment: BTreeMap::new(),
                    max_frame_bytes: NonZeroUsize::new(4096).unwrap(),
                    shutdown_grace: Duration::ZERO,
                },
            },
        );
        let target = owner.wait_ready().await.unwrap();
        let request = request_operation(&store, &session, mode, operation.clone()).await;
        let McpDispatchClaim::Acquired(permit) =
            claim_mcp_dispatch(&store, request, &target, limits())
                .await
                .unwrap()
        else {
            panic!()
        };
        let (_cancel, cancelled) = oneshot::channel();
        let mut catalog = catalog_limits();
        catalog.max_pages = NonZeroUsize::new(2).unwrap();
        catalog.max_entries = NonZeroUsize::new(2).unwrap();
        let result = owner
            .dispatch(
                permit,
                StdioCallLimits {
                    deadline: Instant::now() + Duration::from_secs(5),
                    max_result_bytes: NonZeroUsize::new(4096).unwrap(),
                    catalog,
                },
                cancelled,
            )
            .await;
        let sent = expected.is_none() || expected == Some(StdioCallError::InvalidOutput);
        match expected {
            Some(error) => assert_eq!(result.err(), Some(error), "{mode}"),
            None => {
                let text = String::from_utf8(result.unwrap().json).unwrap();
                assert!(text.contains(if operation["kind"] == "prompt" {
                    "untrusted prompt content"
                } else {
                    "server resource content"
                }));
            }
        }
        let wire = std::fs::read_to_string(data.path().join("operations")).unwrap_or_default();
        assert_eq!(wire.lines().count(), usize::from(sent), "{mode}");
        if sent && operation["kind"] == "resource" {
            let params: serde_json::Value = serde_json::from_str(wire.trim()).unwrap();
            assert_eq!(params["uri"], operation["uri"]);
        }
        let lists = std::fs::read_to_string(data.path().join("lists"))
            .unwrap_or_default()
            .lines()
            .count();
        assert_eq!(
            lists,
            if operation["kind"] == "resource" {
                0
            } else if matches!(mode, "pagination" | "changed") {
                2
            } else {
                1
            }
        );
        owner.stop().await.unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn mcp_discovery_uses_distinct_approved_claims_and_compact_receipt_backed_results() {
    use kiln_mcp::{StdioCallLimits, StdioGeneration, StdioGenerationLaunch, StdioProcessConfig};
    use std::{sync::Arc, time::Duration};
    use tokio::{sync::oneshot, time::Instant};
    let script = r#"
import json, sys
tools_lists = 0
for line in sys.stdin:
    request = json.loads(line)
    method = request.get('method')
    if method == 'initialize':
        result = {'protocolVersion':'2025-11-25','capabilities':{'tools':{},'resources':{},'prompts':{}},'serverInfo':{'name':'fixture','version':'1'}}
    elif method == 'notifications/initialized': continue
    else:
        with open('discovery', 'a') as log: log.write(method+'\n')
        if method == 'tools/list':
            tools_lists += 1
            if tools_lists == 5: print(json.dumps({'jsonrpc':'2.0','method':'notifications/tools/list_changed'}), flush=True)
            second = bool(request.get('params',{}).get('cursor'))
            result = {'tools':[{'name':'alpha' if second else 'zeta','description':'Write a note','inputSchema':{'type':'object','properties':{'text':{'type':'string'}},'required':['text']}}]}
            if not second: result['nextCursor'] = 'next'
        elif method == 'resources/list': result = {'resources':[{'name':'note','uri':'file:///must-not-open','description':'Read a note'}]}
        elif method == 'prompts/list': result = {'prompts':[{'name':'review','arguments':[{'name':'language','required':True}]}]}
        elif method == 'resources/templates/list': result = {'resourceTemplates':[{'name':'notes','uriTemplate':'notes:///{+path}'}]}
        else: raise AssertionError('discovery must not execute '+method)
    print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':result}), flush=True)
"#;
    let (data, store, session) = seeded_session().await;
    let key = register(
        &store,
        McpInstanceOwner::Core,
        McpProtocolPolicy::Pinned(McpProtocolVersion::V20251125),
    )
    .await;
    let mut owner = StdioGeneration::spawn(
        Arc::new(store.clone()),
        StdioGenerationLaunch {
            key,
            definition_version: 1,
            host_binding_version: None,
            generation: McpGenerationId::from_ulid(ulid::Ulid::generate()),
            definition_limits: limits(),
            startup_deadline: Instant::now() + Duration::from_secs(5),
            process: StdioProcessConfig {
                executable: "/usr/bin/python3".into(),
                arguments: vec!["-c".into(), script.into()],
                working_directory: std::fs::File::open(data.path()).unwrap().into(),
                environment: BTreeMap::new(),
                max_frame_bytes: NonZeroUsize::new(4096).unwrap(),
                shutdown_grace: Duration::ZERO,
            },
        },
    );
    let target = owner.wait_ready().await.unwrap();
    let mut catalog = catalog_limits();
    catalog.max_pages = NonZeroUsize::new(2).unwrap();
    catalog.max_entries = NonZeroUsize::new(2).unwrap();
    let cases = [
        (
            "mcp_search",
            serde_json::json!({"kind":"tool","query":"WRITE","offset":0,"limit":1}),
        ),
        (
            "mcp_search",
            serde_json::json!({"kind":"tool","query":"WRITE","offset":1,"limit":1}),
        ),
        (
            "mcp_describe",
            serde_json::json!({"kind":"tool","identifier":"zeta"}),
        ),
        (
            "mcp_search",
            serde_json::json!({"kind":"resource","query":"","offset":0,"limit":1}),
        ),
        (
            "mcp_describe",
            serde_json::json!({"kind":"resource","identifier":"file:///must-not-open"}),
        ),
        (
            "mcp_search",
            serde_json::json!({"kind":"prompt","query":"","offset":0,"limit":1}),
        ),
        (
            "mcp_describe",
            serde_json::json!({"kind":"prompt","identifier":"review"}),
        ),
        (
            "mcp_search",
            serde_json::json!({"kind":"resource_template","query":"","offset":0,"limit":1}),
        ),
        (
            "mcp_describe",
            serde_json::json!({"kind":"resource_template","identifier":"notes:///{+path}"}),
        ),
        (
            "mcp_describe",
            serde_json::json!({"kind":"tool","identifier":"absent"}),
        ),
        (
            "mcp_search",
            serde_json::json!({"kind":"tool","query":"","offset":0,"limit":1}),
        ),
        (
            "mcp_describe",
            serde_json::json!({"kind":"tool","identifier":"zeta"}),
        ),
    ];
    let mut snapshot = String::new();
    for (index, (name, mut arguments)) in cases.into_iter().enumerate() {
        if matches!(index, 1 | 2 | 11) {
            arguments["snapshot"] = snapshot.clone().into();
        }
        let session =
            SessionApplication::new(store.clone(), store.clone(), super::super::UlidIdGenerator)
                .create_session(session.workspace_id().clone())
                .await
                .unwrap();
        arguments["server_id"] = "fixture".into();
        arguments["definition_version"] = 1.into();
        let request = request_native(
            &store,
            &session,
            &format!("discovery-{index}"),
            name,
            arguments,
        )
        .await;
        assert_eq!(
            request.tool_call().capability(),
            if name == "mcp_search" {
                MCP_SEARCH_CAPABILITY
            } else {
                MCP_DESCRIBE_CAPABILITY
            }
        );
        let tool_call_id = request.tool_call().tool_call_id().clone();
        let McpDispatchClaim::Acquired(permit) =
            claim_mcp_dispatch(&store, request, &target, limits())
                .await
                .unwrap()
        else {
            panic!()
        };
        let (_cancel, cancelled) = oneshot::channel();
        let result = owner
            .dispatch_tool_call(
                permit,
                StdioCallLimits {
                    deadline: Instant::now() + Duration::from_secs(5),
                    max_result_bytes: NonZeroUsize::new(4096).unwrap(),
                    catalog,
                },
                cancelled,
                |_| std::future::ready(Err::<Artifact, ()>(())),
            )
            .await
            .unwrap();
        assert_eq!(
            result.state(),
            if index >= 9 {
                ToolCallState::Failed
            } else {
                ToolCallState::Completed
            }
        );
        if index < 9 {
            let value: serde_json::Value = serde_json::from_str(result.stdout().unwrap()).unwrap();
            if index == 0 {
                snapshot = value["catalog_snapshot"].as_str().unwrap().to_owned();
            }
            if matches!(index, 1 | 2) {
                assert_eq!(value["catalog_snapshot"], snapshot);
            }
            assert_eq!(value["generation"], target.generation.as_str());
            assert_eq!(value["server_id"], "fixture");
            assert_eq!(value["protocol_version"], "2025-11-25");
            if name == "mcp_search" {
                assert!(!result.stdout().unwrap().contains("inputSchema"));
                assert_eq!(value["result"]["entries"].as_array().unwrap().len(), 1);
            }
            match index {
                0 => {
                    assert_eq!(value["result"]["entries"][0]["identifier"], "alpha");
                    assert_eq!(value["result"]["next_offset"], 1);
                }
                1 => {
                    assert_eq!(value["result"]["entries"][0]["identifier"], "zeta");
                    assert!(value["result"]["next_offset"].is_null());
                }
                2 => assert_eq!(value["result"]["inputSchema"]["required"][0], "text"),
                4 => assert_eq!(value["result"]["uri"], "file:///must-not-open"),
                6 => assert_eq!(value["result"]["arguments"][0]["name"], "language"),
                8 => assert_eq!(value["result"]["uriTemplate"], "notes:///{+path}"),
                _ => {}
            }
        }
        if index == 10 {
            assert!(
                result
                    .stderr()
                    .unwrap()
                    .contains("catalogue is no longer valid")
            );
        }
        if index == 11 {
            assert!(result.stderr().unwrap().contains("snapshot is unavailable"));
        }

        ProviderApplication::new(store.clone(), super::super::UlidIdGenerator)
            .finish_tool_call(&tool_call_id, &result)
            .await
            .unwrap();
        let (_, stored) = store.get_tool_call(&tool_call_id).await.unwrap().unwrap();
        assert_eq!(stored.state(), result.state());
    }
    // A valid parsed discovery request cannot claim a durable source relabelled
    // as a call, even though it still names the same server and version.
    let request = request_native(&store, &session, "wrong-source", "mcp_search", serde_json::json!({"server_id":"fixture","definition_version":1,"kind":"tool","query":"","offset":0,"limit":1})).await;
    {
        let mut sql = store.connection.lock().await;
        sqlx::query(
            "UPDATE model_tool_requests SET name = 'mcp_call' WHERE model_invocation_id = ?",
        )
        .bind(request.invocation_id().as_str())
        .execute(&mut *sql)
        .await
        .unwrap();
    }
    assert_eq!(
        store
            .begin_mcp_invocation(&request, &target, limits())
            .await
            .err(),
        Some(McpInvocationError::InvalidRequest)
    );
    owner.stop().await.unwrap();
    let wire = std::fs::read_to_string(data.path().join("discovery")).unwrap();
    assert_eq!(wire.lines().filter(|line| *line == "tools/list").count(), 6);
    assert_eq!(
        wire.lines().count(),
        12,
        "snapshot page/describe reuse metadata; fresh requests list and never execute"
    );
}
