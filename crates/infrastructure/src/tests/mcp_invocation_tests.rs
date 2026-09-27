use super::*;
use kiln_core::*;
use std::{collections::BTreeMap, num::NonZeroUsize};

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

async fn request(
    store: &super::super::SqliteStore,
    session: &Session,
    key: &str,
) -> ModelToolExecutionRequest<McpCallCommand> {
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
    let tool = McpCallTool::new(
        NonZeroUsize::new(4096).unwrap(),
        ModelToolCatalogLimits {
            max_tools: 1,
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
            name: "mcp_call".into(),
            arguments: serde_json::json!({"server_id":"fixture","definition_version":1,
            "operation":{"kind":"tool","name":"write","arguments":{"text":"private-payload"}}})
            .as_object()
            .unwrap()
            .clone(),
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
        sqlx::query("CREATE TRIGGER fixture_mcp_invocation_failure BEFORE INSERT ON mcp_invocation_events BEGIN SELECT RAISE(ABORT, 'fixture rollback'); END")
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
    drop(store);
    let store = super::super::SqliteStore::open(data.path()).await.unwrap();
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
}

#[tokio::test]
async fn mcp_dispatch_rechecks_definition_and_journals_terminal_retry_once() {
    let (_data, store, session) = seeded_session().await;
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
    elif method == 'tools/call':
        with open('calls', 'a') as calls: calls.write(json.dumps(request['params'])+'\n')
        if mode == 'disconnect': sys.exit(0)
        if mode in ('cancel','deadline'): time.sleep(60)
        if mode == 'server_error':
            print(json.dumps({'jsonrpc':'2.0','id':request['id'],'error':{'code':-32602,'message':'fixture error'}}), flush=True)
            continue
        result = {'content':[{'type':'text','text':'private-result'}]}
    else:
        continue
    print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':result}), flush=True)
"#;
    for mode in [
        "complete",
        "server_error",
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
                    max_frame_bytes: NonZeroUsize::new(4096).unwrap(),
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
                    deadline: Instant::now() + allowance,
                    max_result_bytes: NonZeroUsize::new(if mode == "oversize" { 1 } else { 4096 })
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
            "complete" => {
                assert!(
                    String::from_utf8(result.unwrap().json)
                        .unwrap()
                        .contains("private-result")
                );
                McpInvocationState::Completed
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
                .unwrap()
                .lines()
                .count(),
            1,
            "{mode} must never replay"
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
        if mode == "complete" {
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
            let (_cancel, cancelled) = oneshot::channel();
            let next = owner
                .dispatch(
                    permit,
                    StdioCallLimits {
                        deadline: Instant::now() + allowance,
                        max_result_bytes: NonZeroUsize::new(4096).unwrap(),
                    },
                    cancelled,
                )
                .await
                .unwrap();
            assert!(!next.is_error);
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
