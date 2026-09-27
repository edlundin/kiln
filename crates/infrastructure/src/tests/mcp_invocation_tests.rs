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
        text = 'private-result' * (400 if mode == 'capture' else 1)
        result = {'content':[{'type':'text','text':text}]}
    else:
        continue
    print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':result}), flush=True)
"#;
    for mode in [
        "complete",
        "capture",
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
            "complete" | "capture" => {
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
