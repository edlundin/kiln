use std::{
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

use kiln_core::{McpDefinitionLimits, McpGenerationId, McpTools, ModelToolCatalogLimits};
use kiln_infrastructure::{OsMcpSecretStore, pin_mcp_working_directory};
use kiln_mcp::{
    McpCatalogLimits, StdioBrokerError, StdioBrokerLimits, StdioCallLimits, execute_stdio_call,
};
use serde::Deserialize;
use tokio::time::Instant;

use super::*;

/// All allowances are host inputs, not defaults or model-selected values.
#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeMcpLimits {
    pub max_request_bytes: NonZeroUsize,
    pub max_definition_key_bytes: NonZeroUsize,
    pub max_definition_bytes: NonZeroUsize,
    pub max_arguments: NonZeroUsize,
    pub max_argument_bytes: NonZeroUsize,
    pub max_environment: NonZeroUsize,
    pub max_endpoint_bytes: NonZeroUsize,
    pub max_resolved_bytes: NonZeroUsize,
    pub max_frame_bytes: NonZeroUsize,
    pub max_result_bytes: NonZeroUsize,
    pub max_catalog_pages: NonZeroUsize,
    pub max_catalog_entries: NonZeroUsize,
    pub max_catalog_bytes: NonZeroUsize,
    pub max_regex_bytes: NonZeroUsize,
    pub max_regex_backtracks: NonZeroUsize,
    pub startup_timeout_ms: NonZeroU64,
    pub call_timeout_ms: NonZeroU64,
    pub shutdown_grace_ms: u64,
}

pub(crate) struct NativeMcp {
    pub tools: McpTools,
    limits: NativeMcpLimits,
    vault: OsMcpSecretStore,
}

impl NativeMcp {
    pub(crate) fn new(limits: NativeMcpLimits) -> Result<Self, &'static str> {
        let invalid = "invalid native MCP limits";
        limits.broker_limits().map_err(|_| invalid)?;
        let tools = McpTools::new(
            limits.max_request_bytes,
            ModelToolCatalogLimits {
                // Only three Kiln-owned fixed schemas and numeric host allowances.
                max_tools: 3,
                max_definition_bytes: usize::MAX,
                max_total_definition_bytes: usize::MAX,
            },
        )
        .map_err(|_| invalid)?;
        Ok(Self {
            tools,
            limits,
            vault: OsMcpSecretStore::open_default(),
        })
    }
}

impl NativeMcpLimits {
    fn broker_limits(self) -> Result<StdioBrokerLimits, RunError> {
        let definition = McpDefinitionLimits {
            max_key_bytes: self.max_definition_key_bytes.get(),
            max_metadata_bytes: self.max_definition_bytes.get(),
            max_arguments: self.max_arguments.get(),
            max_argument_bytes: self.max_argument_bytes.get(),
            max_environment: self.max_environment.get(),
            max_endpoint_bytes: self.max_endpoint_bytes.get(),
        };
        definition
            .validate()
            .map_err(|_| RunError::InvalidTransition)?;
        let now = Instant::now();
        let startup_deadline = now
            .checked_add(Duration::from_millis(self.startup_timeout_ms.get()))
            .ok_or(RunError::InvalidTransition)?;
        let deadline = now
            .checked_add(Duration::from_millis(self.call_timeout_ms.get()))
            .ok_or(RunError::InvalidTransition)?;
        let shutdown_grace = Duration::from_millis(self.shutdown_grace_ms);
        now.checked_add(shutdown_grace)
            .ok_or(RunError::InvalidTransition)?;
        Ok(StdioBrokerLimits {
            generation: McpGenerationId::from_ulid(ulid::Ulid::generate()),
            definition,
            max_resolved_bytes: self.max_resolved_bytes,
            max_frame_bytes: self.max_frame_bytes,
            startup_deadline,
            shutdown_grace,
            call: StdioCallLimits {
                deadline,
                max_result_bytes: self.max_result_bytes,
                catalog: McpCatalogLimits {
                    max_pages: self.max_catalog_pages,
                    max_entries: self.max_catalog_entries,
                    max_bytes: self.max_catalog_bytes,
                    max_regex_bytes: self.max_regex_bytes,
                    max_regex_backtracks: self.max_regex_backtracks,
                },
            },
        })
    }
}

pub(crate) fn configured_native_mcp() -> Result<Option<NativeMcp>, &'static str> {
    match std::env::var("KILN_NATIVE_MCP_LIMITS") {
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err("KILN_NATIVE_MCP_LIMITS must be valid UTF-8 JSON"),
        Ok(json) => {
            let limits = serde_json::from_str(&json).map_err(|_| "KILN_NATIVE_MCP_LIMITS requires all explicit native MCP limits and no unknown fields")?;
            NativeMcp::new(limits).map(Some)
        }
    }
}

impl RunService {
    pub(crate) fn with_native_mcp(mut self, mcp: Option<NativeMcp>) -> Result<Self, &'static str> {
        if mcp.is_some() && self.mcp_registry.is_none() {
            return Err("native MCP tools require the configured MCP lifecycle registry");
        }
        self.native_mcp = mcp.map(Arc::new);
        Ok(self)
    }

    pub(super) async fn execute_mcp_native(
        &self,
        request: kiln_core::ModelToolExecutionRequest<kiln_core::McpCommand>,
        cancellation: &mut oneshot::Receiver<()>,
    ) -> Result<kiln_core::ToolCallResult, RunError> {
        let mcp = self
            .native_mcp
            .as_ref()
            .ok_or(RunError::InvalidTransition)?;
        let registry = self
            .mcp_registry
            .as_ref()
            .ok_or(RunError::InvalidTransition)?;
        let (cancel, cancelled) = oneshot::channel();
        let operation = execute_stdio_call(
            registry,
            request,
            &mcp.vault,
            mcp.limits.broker_limits()?,
            cancelled,
            |directory| async move {
                tokio::task::spawn_blocking(move || pin_mcp_working_directory(&directory))
                    .await
                    .map_err(|_| RunError::RunStoreUnavailable)?
            },
            |bytes| {
                let artifacts = self.artifacts.clone();
                async move {
                    tokio::task::spawn_blocking(move || {
                        artifacts.store(&bytes, TOOL_OUTPUT_MEDIA_TYPE)
                    })
                    .await
                    .map_err(|_| ())?
                    .map_err(|_| ())
                }
            },
        );
        tokio::pin!(operation);
        let result = tokio::select! {
            biased;
            _ = cancellation => { let _ = cancel.send(()); operation.await },
            result = &mut operation => result,
        };
        match result {
            Ok(result) => Ok(result),
            Err(StdioBrokerError::CancelledBeforeDispatch) => {
                kiln_core::ToolCallResult::cancelled(empty_output())
            }
            // These errors may have lost a dispatch/journal receipt. Never
            // manufacture completion or authorize replay from missing evidence.
            Err(StdioBrokerError::DispatchClaim(_) | StdioBrokerError::Completion(_)) => {
                Err(RunError::RunStoreUnavailable)
            }
            Err(_) => kiln_core::ToolCallResult::new(
                kiln_core::ToolCallState::Failed,
                String::new(),
                "MCP preparation failed before operation dispatch. The request was not retried."
                    .into(),
                None,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kiln_core::*;
    use kiln_infrastructure::DeterministicOutcome;
    use serde_json::json;
    use std::{
        collections::BTreeMap,
        os::unix::fs::{MetadataExt, PermissionsExt},
    };

    fn limits_json() -> serde_json::Value {
        // Exact small fixture workloads plus the same five-second scheduling
        // allowance as existing real-process tests; these are not defaults.
        json!({"max_request_bytes":4096,"max_definition_key_bytes":64,"max_definition_bytes":4096,
            "max_arguments":4,"max_argument_bytes":128,"max_environment":4,"max_endpoint_bytes":128,
            "max_resolved_bytes":4096,"max_frame_bytes":4096,"max_result_bytes":4096,
            "max_catalog_pages":1,"max_catalog_entries":1,"max_catalog_bytes":1024,
            "max_regex_bytes":1048576,"max_regex_backtracks":10000,
            "startup_timeout_ms":5000,"call_timeout_ms":5000,"shutdown_grace_ms":0})
    }

    #[test]
    fn native_mcp_configuration_has_no_implicit_allowances() {
        assert!(NativeMcp::new(serde_json::from_value(limits_json()).unwrap()).is_ok());
        for field in limits_json().as_object().unwrap().keys() {
            let mut value = limits_json();
            value.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<NativeMcpLimits>(value).is_err(),
                "{field}"
            );
            if field != "shutdown_grace_ms" {
                let mut value = limits_json();
                value[field] = 0.into();
                assert!(
                    serde_json::from_value::<NativeMcpLimits>(value).is_err(),
                    "{field}"
                );
            }
        }
        let mut value = limits_json();
        value["parallel_safe"] = true.into();
        assert!(serde_json::from_value::<NativeMcpLimits>(value).is_err());
    }

    #[tokio::test]
    async fn mixed_native_batch_waits_for_approval_completes_and_never_replays() {
        let data = tempfile::tempdir().unwrap();
        let path = std::fs::canonicalize(data.path()).unwrap();
        let stat = std::fs::metadata(&path).unwrap();
        let identity =
            FilesystemIdentity::new(format!("unix:{}:{}", stat.dev(), stat.ino())).unwrap();
        let root_id = WorkspaceRootId::from_ulid(ulid::Ulid::generate());
        let workspace = Workspace::new(
            WorkspaceId::from_ulid(ulid::Ulid::generate()),
            "fixture".into(),
            vec![
                WorkspaceRoot::new(
                    root_id.clone(),
                    "main".into(),
                    path.to_str().unwrap().into(),
                    DiscoveredWorkspaceRoot {
                        canonical_path: path.to_str().unwrap().into(),
                        git_common_directory_path: path.join(".git").to_str().unwrap().into(),
                        filesystem_identity: identity.clone(),
                    },
                    0,
                    WorkspaceRootState::Available,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let store = SqliteStore::open(data.path()).await.unwrap();
        store.create_workspace(&workspace).await.unwrap();
        let session = SessionApplication::new(store.clone(), store.clone(), UlidIdGenerator)
            .create_session(workspace.id().clone())
            .await
            .unwrap();
        let scope = WorkspacePathScope::new(root_id.clone(), "").unwrap();
        let directory = WorkspaceCheckout::from_resolved_paths(
            workspace.id().clone(),
            root_id,
            "",
            path.to_str().unwrap(),
            path.join(".git").to_str().unwrap(),
            identity,
        )
        .unwrap();
        let executable = path.join("server");
        std::fs::write(path.join("note.txt"), "local file output").unwrap();
        std::fs::write(&executable,r#"#!/usr/bin/python3
import json, sys
with open('starts','a') as log: log.write('start\n')
for line in sys.stdin:
    request = json.loads(line)
    method = request.get('method')
    if method == 'initialize': result = {'protocolVersion':'2025-11-25','capabilities':{'tools':{}},'serverInfo':{'name':'fixture','version':'1'}}
    elif method == 'tools/list':
        with open('lists','a') as log: log.write('list\n')
        result = {'tools':[{'name':'write','inputSchema':{'type':'object'}}]}
    elif method == 'tools/call':
        with open('calls','a') as log: log.write('call\n')
        result = {'content':[{'type':'text','text':'remote output'}]}
    else: continue
    print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':result}),flush=True)
"#).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config: NativeMcpLimits = serde_json::from_value(limits_json()).unwrap();
        let definitions = config.broker_limits().unwrap().definition;
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
            McpProtocolPolicy::Pinned(McpProtocolVersion::V20251125),
            McpLifecycleScope::Core,
            None,
            definitions,
        )
        .unwrap();
        store
            .register_mcp_definition(&definition, 0, "fixture", definitions)
            .await
            .unwrap();
        let key = McpInstanceKey::new(&definition, McpInstanceOwner::Core, 4096).unwrap();
        let instance = KilnInstanceId::from_ulid(ulid::Ulid::generate());
        store
            .initialize_configuration_instance(instance.clone())
            .await
            .unwrap();
        let bindings = McpHostBindings::new(
            key,
            McpHostBindingInput {
                instance_id: instance,
                definition_version: 1,
                runtime_binding: SharedConfigurationKey::parse("runtime", 64).unwrap(),
                executable: executable.to_str().unwrap().into(),
                working_directory: Some((&directory).into()),
                arguments: BTreeMap::new(),
                environment: BTreeMap::new(),
            },
            definitions,
        )
        .unwrap();
        store
            .publish_mcp_host_bindings(&bindings, 0, definitions)
            .await
            .unwrap();
        let registry = Arc::new(kiln_mcp::StdioRegistry::new(
            Arc::new(store.clone()),
            NonZeroUsize::new(1).unwrap(),
        ));
        let runs = RunApplication::new(store.clone(), UlidIdGenerator);
        let service = RunService::new(
            runs,
            DeterministicSubprocessExecutor::new(DeterministicOutcome::Success),
            EventBroadcaster::default(),
            store.clone(),
            FileArtifactStore::open(data.path()).unwrap(),
        )
        .with_native_file_read(Some(
            WorkspaceFileReadTool::new(
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
            .unwrap(),
        ))
        .with_mcp_registry(Some(registry))
        .with_native_mcp(Some(NativeMcp::new(config).unwrap()))
        .unwrap();
        assert!(
            service
                .clone()
                .with_mcp_registry(None)
                .with_native_mcp(Some(NativeMcp::new(config).unwrap()))
                .is_err()
        );
        let run = service
            .runs
            .start_root_run(
                session.id().clone(),
                "mixed".into(),
                ApprovalPolicy::Ask,
                scope,
            )
            .await
            .unwrap();
        let run_id = run.value.run().run_id().clone();
        let manifest = ContextManifestApplication::new(store.clone(), UlidIdGenerator)
            .create_context_manifest(CreateContextManifest {
                run_id: run_id.clone(),
                entries: vec![ContextManifestEntryInput::Instruction {
                    provenance: ContextInstructionProvenance::Runtime,
                    content: "fixture".into(),
                }],
                idempotency_key: "mixed".into(),
            })
            .await
            .unwrap();
        let invocation = ModelInvocationApplication::new(store.clone(), UlidIdGenerator)
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
                idempotency_key: "mixed".into(),
            })
            .await
            .unwrap()
            .value;
        let tools = service.native_tools().unwrap();
        assert_eq!(
            tools
                .catalog()
                .definitions()
                .iter()
                .map(|d| d.name())
                .collect::<Vec<_>>(),
            ["read_file", "mcp_call", "mcp_search", "mcp_describe"]
        );
        store
            .attach_model_tool_catalog(invocation.invocation_id(), tools.catalog())
            .await
            .unwrap();
        let app = ProviderApplication::new(store.clone(), UlidIdGenerator);
        assert!(matches!(
            app.claim(invocation.invocation_id().clone()).await.unwrap(),
            ProviderClaim::Applied { .. }
        ));
        let invocation = store
            .get_model_invocation(invocation.invocation_id())
            .await
            .unwrap()
            .unwrap();
        let requests = ModelToolRequestBatch::new(invocation.invocation_id().clone(),[
            ("read_file",json!({"path":"note.txt"})),
            ("mcp_search",json!({"server_id":"fixture","definition_version":1,"kind":"tool","query":"","offset":0,"limit":1})),
            ("mcp_describe",json!({"server_id":"fixture","definition_version":1,"kind":"tool","identifier":"write"})),
            ("mcp_call",json!({"server_id":"fixture","definition_version":1,"operation":{"kind":"tool","name":"write","arguments":{}}})),
        ].into_iter().enumerate().map(|(i,(name,args))|ModelToolRequestInput { provider_call_id:format!("call-{i}"),name:name.into(),arguments:args.as_object().unwrap().clone() }).collect(),ModelToolRequestLimits {
            max_requests:4,max_provider_call_id_bytes:64,max_name_bytes:64,max_arguments_bytes:4096,max_total_arguments_bytes:16384
        }).unwrap();
        let usage = ProviderUsageUpdate::new(
            ProviderUsageMetadata {
                update_id: "mixed".into(),
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
        let (_cancel, mut cancelled) = oneshot::channel();
        let mut wake = service.events.subscribe_wake();
        let approve = async {
            for index in 0..4 {
                loop {
                    let snapshot = service.runs.get_run(run_id.clone()).await.unwrap();
                    if let Some(approval) = snapshot
                        .approvals()
                        .iter()
                        .find(|a| a.state() == ApprovalState::Pending)
                    {
                        if index < 2 {
                            assert!(
                                !path.join("starts").exists(),
                                "no MCP launch before approval"
                            );
                        }
                        let mutation = service
                            .runs
                            .decide_approval(
                                approval.approval_id().clone(),
                                ApprovalState::Approved,
                                format!("approve-{index}"),
                            )
                            .await
                            .unwrap();
                        service.events.publish(mutation.events);
                        service.active.changed.notify_waiters();
                        break;
                    }
                    wake.recv().await.unwrap();
                }
            }
        };
        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            let (outcome, ()) = tokio::join!(
                service.execute_native_tools(&invocation, &mut cancelled),
                approve
            );
            outcome.unwrap()
        })
        .await
        .unwrap();
        let super::super::native_tools::NativeToolBatchOutcome::Completed(ids) = outcome else {
            panic!("batch failed")
        };
        assert_eq!(ids.len(), 4);
        for (index, id) in ids.iter().enumerate() {
            let (_, tool) = store.get_tool_call(id).await.unwrap().unwrap();
            assert_eq!(tool.state(), ToolCallState::Completed);
            let output = tool.stdout().unwrap();
            assert!(output.contains(
                [
                    "local file output",
                    "total_matches",
                    "inputSchema",
                    "remote output"
                ][index]
            ));
        }
        assert!(matches!(
            service
                .execute_native_tools(&invocation, &mut cancelled)
                .await
                .unwrap(),
            super::super::native_tools::NativeToolBatchOutcome::Completed(_)
        ));
        service.shutdown().await.unwrap();
        assert_eq!(
            std::fs::read_to_string(path.join("starts"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert_eq!(
            std::fs::read_to_string(path.join("lists"))
                .unwrap()
                .lines()
                .count(),
            3
        );
        assert_eq!(
            std::fs::read_to_string(path.join("calls"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
}
