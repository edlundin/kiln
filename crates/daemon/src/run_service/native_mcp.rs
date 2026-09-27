use std::{
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

use kiln_core::{McpDefinitionLimits, McpGenerationId, McpTools, ModelToolCatalogLimits};
use kiln_infrastructure::{OsMcpSecretStore, pin_mcp_working_directory};
use kiln_mcp::{
    McpCatalogLimits, StdioBrokerError, StdioBrokerLimits, StdioCallLimits, execute_mcp_call,
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
    #[serde(default)]
    pub max_input_requests: Option<NonZeroUsize>,
    #[serde(default)]
    pub http: Option<NativeMcpHttpLimits>,
}

/// Every HTTP allowance is explicit; omitting the object disables HTTP startup.
#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeMcpHttpLimits {
    pub max_request_bytes: NonZeroUsize,
    pub max_response_bytes: NonZeroUsize,
    pub max_stream_bytes: NonZeroUsize,
    pub max_event_bytes: NonZeroUsize,
    pub max_header_bytes: NonZeroUsize,
    pub request_timeout_ms: NonZeroU64,
    pub channel_capacity: NonZeroUsize,
    pub max_exchanges: NonZeroUsize,
    pub max_catalog_lifetime_bytes: NonZeroUsize,
    pub legacy_resume_delay_ms: Option<NonZeroU64>,
}
impl NativeMcpHttpLimits {
    fn broker_limits(self) -> Result<kiln_mcp::HttpBrokerLimits, RunError> {
        let request_timeout = Duration::from_millis(self.request_timeout_ms.get());
        let legacy_resume_delay = self
            .legacy_resume_delay_ms
            .map(|ms| Duration::from_millis(ms.get()));
        let now = Instant::now();
        now.checked_add(request_timeout)
            .ok_or(RunError::InvalidTransition)?;
        if let Some(delay) = legacy_resume_delay {
            now.checked_add(delay).ok_or(RunError::InvalidTransition)?;
        }
        Ok(kiln_mcp::HttpBrokerLimits {
            io: kiln_mcp::McpHttpLimits {
                max_request_bytes: self.max_request_bytes,
                max_response_bytes: self.max_response_bytes,
                max_stream_bytes: self.max_stream_bytes,
                max_event_bytes: self.max_event_bytes,
                max_header_bytes: self.max_header_bytes,
                request_timeout,
            },
            channel_capacity: self.channel_capacity,
            max_exchanges: self.max_exchanges,
            max_catalog_lifetime_bytes: self.max_catalog_lifetime_bytes,
            legacy_resume_delay,
        })
    }
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
        limits
            .http
            .map(NativeMcpHttpLimits::broker_limits)
            .transpose()
            .map_err(|_| invalid)?;
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
                max_input_requests: self.max_input_requests,
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
        let operation = execute_mcp_call(
            registry,
            request,
            &mcp.vault,
            mcp.limits.broker_limits()?,
            mcp.limits
                .http
                .map(NativeMcpHttpLimits::broker_limits)
                .transpose()?,
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
        let mut roots = limits_json();
        roots["max_input_requests"] = json!(0);
        assert!(serde_json::from_value::<NativeMcpLimits>(roots.clone()).is_err());
        roots["max_input_requests"] = json!(1);
        assert_eq!(
            serde_json::from_value::<NativeMcpLimits>(roots)
                .unwrap()
                .max_input_requests
                .unwrap()
                .get(),
            1
        );
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

    fn http_limits_json() -> serde_json::Value {
        json!({"max_request_bytes":16384,"max_response_bytes":16384,"max_stream_bytes":16384,"max_event_bytes":16384,"max_header_bytes":4096,"request_timeout_ms":5000,"channel_capacity":1,"max_exchanges":16,"max_catalog_lifetime_bytes":16384,"legacy_resume_delay_ms":null})
    }

    #[test]
    fn http_configuration_requires_explicit_positive_allowances() {
        assert!(
            serde_json::from_value::<NativeMcpLimits>(limits_json())
                .unwrap()
                .http
                .is_none()
        );
        let mut value = limits_json();
        value["http"] = http_limits_json();
        assert!(NativeMcp::new(serde_json::from_value(value.clone()).unwrap()).is_ok());
        for field in http_limits_json()
            .as_object()
            .unwrap()
            .keys()
            .filter(|field| field.as_str() != "legacy_resume_delay_ms")
        {
            let mut missing = value.clone();
            missing["http"].as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<NativeMcpLimits>(missing).is_err());
            let mut zero = value.clone();
            zero["http"][field] = json!(0);
            assert!(serde_json::from_value::<NativeMcpLimits>(zero).is_err());
        }
        value["http"]["unexpected"] = json!(1);
        assert!(serde_json::from_value::<NativeMcpLimits>(value).is_err());
    }

    struct HttpFixture {
        endpoint: String,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for HttpFixture {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    impl HttpFixture {
        async fn new(path: std::path::PathBuf, protocol: McpProtocolVersion) -> Self {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
            let task = tokio::spawn(async move {
                let mut pending_call: Option<serde_json::Value> = None;
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut headers = Vec::new();
                    let mut byte = [0];
                    while !headers.ends_with(b"\r\n\r\n") {
                        socket.read_exact(&mut byte).await.unwrap();
                        headers.push(byte[0]);
                        assert!(headers.len() < 8192);
                    }
                    let headers = String::from_utf8(headers).unwrap();
                    let method = headers.split_whitespace().next().unwrap();
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(|n| n.parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    assert!(length < 16384);
                    let mut bytes = vec![0; length];
                    socket.read_exact(&mut bytes).await.unwrap();
                    let mut status = "200 OK";
                    let mut extra = "";
                    let response = if method == "GET" {
                        status = "405 Method Not Allowed";
                        String::new()
                    } else if method == "DELETE" {
                        String::new()
                    } else {
                        let request: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                        let method = request["method"].as_str().unwrap();
                        let header = |name: &str| {
                            headers.lines().find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case(name).then(|| value.trim())
                            })
                        };
                        let assert_identity = |identity: &serde_json::Value| {
                            assert_eq!(identity["name"], "kiln");
                            assert_eq!(identity["version"], env!("CARGO_PKG_VERSION"));
                        };
                        if method == "initialize" {
                            assert_identity(&request["params"]["clientInfo"]);
                            assert_eq!(
                                request["params"]["capabilities"],
                                json!({"roots":{},"elicitation":{"form":{"schemaValidation":true}}})
                            );
                        }
                        if protocol == McpProtocolVersion::V20260728 {
                            assert_eq!(header("mcp-protocol-version"), Some("2026-07-28"));
                            assert_eq!(header("mcp-method"), Some(method));
                            assert_eq!(header("mcp-session-id"), None);
                            if request.get("id").is_some() {
                                let meta = &request["params"]["_meta"];
                                assert_eq!(
                                    meta["io.modelcontextprotocol/protocolVersion"],
                                    "2026-07-28"
                                );
                                assert_identity(&meta["io.modelcontextprotocol/clientInfo"]);
                                assert_eq!(
                                    meta["io.modelcontextprotocol/clientCapabilities"],
                                    json!({"roots":{},"elicitation":{"form":{"schemaValidation":true}}})
                                );
                            }
                            if method == "tools/call" {
                                assert_eq!(header("mcp-name"), Some("write"));
                            }
                        }
                        let result = match method {
                            "server/discover" => {
                                use std::io::Write;
                                writeln!(
                                    std::fs::OpenOptions::new()
                                        .create(true)
                                        .append(true)
                                        .open(path.join("starts"))
                                        .unwrap(),
                                    "start"
                                )
                                .unwrap();
                                json!({"resultType":"complete","supportedVersions":["2026-07-28"],"capabilities":{"tools":{}},"ttlMs":0,"cacheScope":"private"})
                            }
                            "initialize" => {
                                use std::io::Write;
                                writeln!(
                                    std::fs::OpenOptions::new()
                                        .create(true)
                                        .append(true)
                                        .open(path.join("starts"))
                                        .unwrap(),
                                    "start"
                                )
                                .unwrap();
                                extra = "Mcp-Session-Id: fixture\r\n";
                                json!({"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"fixture","version":"1"}})
                            }
                            "tools/list" => {
                                use std::io::Write;
                                writeln!(
                                    std::fs::OpenOptions::new()
                                        .create(true)
                                        .append(true)
                                        .open(path.join("lists"))
                                        .unwrap(),
                                    "list"
                                )
                                .unwrap();
                                json!({"tools":[{"name":"write","inputSchema":{"type":"object"}}]})
                            }
                            "tools/call"
                                if protocol == McpProtocolVersion::V20260728
                                    && request["params"]["inputResponses"].is_null() =>
                            {
                                assert!(pending_call.is_none());
                                pending_call = Some(request.clone());
                                json!({"resultType":"input_required","inputRequests":{"root-request":{"method":"roots/list"},"form-request":{"method":"elicitation/create","params":{"mode":"form","message":"Confirm fixture action","requestedSchema":{"type":"object","properties":{"proceed":{"type":"boolean"}},"required":["proceed"]}}}},"requestState":"opaque-state"})
                            }
                            "tools/call" => {
                                if protocol == McpProtocolVersion::V20260728 {
                                    let original = pending_call.take().unwrap();
                                    assert_ne!(request["id"], original["id"]);
                                    assert_eq!(
                                        request["params"]["arguments"],
                                        original["params"]["arguments"]
                                    );
                                    assert_eq!(request["params"]["requestState"], "opaque-state");
                                    assert_eq!(
                                        request["params"]["inputResponses"]["form-request"],
                                        json!({"action":"accept","content":{"proceed":true}})
                                    );
                                    assert_eq!(
                                        request["params"]["inputResponses"]["root-request"],
                                        json!({"roots":[{"uri":format!("file://{}/",path.display())}]})
                                    );
                                }
                                use std::io::Write;
                                writeln!(
                                    std::fs::OpenOptions::new()
                                        .create(true)
                                        .append(true)
                                        .open(path.join("calls"))
                                        .unwrap(),
                                    "call"
                                )
                                .unwrap();
                                json!({"content":[{"type":"text","text":"remote output".repeat(500)}]})
                            }
                            "notifications/initialized" | "notifications/cancelled" => {
                                status = "202 Accepted";
                                serde_json::Value::Null
                            }
                            other => panic!("unexpected method {other}"),
                        };
                        if request.get("id").is_some() {
                            json!({"jsonrpc":"2.0","id":request["id"],"result":result}).to_string()
                        } else {
                            String::new()
                        }
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{extra}\r\n{response}",
                        response.len()
                    );
                    socket.write_all(response.as_bytes()).await.unwrap();
                    socket.shutdown().await.unwrap();
                }
            });
            Self { endpoint, task }
        }
    }

    #[tokio::test]
    async fn mixed_native_batch_waits_for_approval_completes_and_never_replays() {
        mixed_native_batch(None).await;
    }

    #[tokio::test]
    async fn http_native_batches_use_approval_artifacts_paging_and_never_replay() {
        mixed_native_batch(Some(McpProtocolVersion::V20260728)).await;
        mixed_native_batch(Some(McpProtocolVersion::V20251125)).await;
    }

    async fn mixed_native_batch(http_protocol: Option<McpProtocolVersion>) {
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
        let http_fixture = if let Some(protocol) = http_protocol {
            Some(HttpFixture::new(path.clone(), protocol).await)
        } else {
            None
        };
        let executable = path.join("server");
        std::fs::write(path.join("note.txt"), "local file output").unwrap();
        std::fs::write(&executable,r#"#!/usr/bin/python3
import json, sys, pathlib
with open('starts','a') as log: log.write('start\n')
for line in sys.stdin:
    request = json.loads(line)
    method = request.get('method')
    if method == 'initialize': result = {'protocolVersion':'2025-11-25','capabilities':{'tools':{}},'serverInfo':{'name':'fixture','version':'1'}}
    elif method == 'tools/list':
        with open('lists','a') as log: log.write('list\n')
        result = {'tools':[{'name':'write','inputSchema':{'type':'object'}}]}
    elif method == 'tools/call':
        print(json.dumps({'jsonrpc':'2.0','id':'root-request','method':'roots/list'}),flush=True)
        roots = json.loads(sys.stdin.readline())
        assert roots['id'] == 'root-request'
        assert roots['result'] == {'roots':[{'uri':pathlib.Path.cwd().as_uri() + '/'}]}
        print(json.dumps({'jsonrpc':'2.0','id':'form-request','method':'elicitation/create','params':{'mode':'form','message':'Confirm fixture action','requestedSchema':{'type':'object','properties':{'proceed':{'type':'boolean'}},'required':['proceed']}}}),flush=True)
        form = json.loads(sys.stdin.readline())
        assert form['id'] == 'form-request'
        assert form['result'] == {'action':'accept','content':{'proceed':True}}
        with open('calls','a') as log: log.write('call\n')
        result = {'content':[{'type':'text','text':'remote output' * 500}]}
    else: continue
    print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':result}),flush=True)
"#).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut config: NativeMcpLimits = serde_json::from_value(limits_json()).unwrap();
        // This fixture's repeated output exercises artifact paging after completion.
        config.max_frame_bytes = NonZeroUsize::new(16384).unwrap();
        config.max_result_bytes = NonZeroUsize::new(16384).unwrap();
        // The fixture asks for one roots and one form response per invocation.
        config.max_input_requests = NonZeroUsize::new(2);
        if http_protocol.is_some() {
            config.http = Some(serde_json::from_value(http_limits_json()).unwrap());
        }
        let definitions = config.broker_limits().unwrap().definition;
        let definition = McpServerDefinition::new(
            SharedMcpServerInput {
                id: SharedConfigurationKey::parse("fixture", 64).unwrap(),
                enabled: true,
                transport: if http_protocol.is_some() {
                    SharedMcpTransport::HostEndpoint {
                        endpoint_binding: SharedConfigurationKey::parse("local", 64).unwrap(),
                    }
                } else {
                    SharedMcpTransport::Stdio {
                        runtime_binding: SharedConfigurationKey::parse("runtime", 64).unwrap(),
                        arguments: vec![],
                        environment: BTreeMap::new(),
                    }
                },
            },
            McpProtocolPolicy::Pinned(http_protocol.unwrap_or(McpProtocolVersion::V20251125)),
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
        let bindings = if let Some(fixture) = &http_fixture {
            McpHostBindings::new_http(
                key,
                McpHttpHostBindingInput {
                    instance_id: instance,
                    definition_version: 1,
                    working_directory: Some((&directory).into()),
                    endpoint: fixture.endpoint.clone(),
                    endpoint_binding: Some(SharedConfigurationKey::parse("local", 64).unwrap()),
                    credential: None,
                },
                definitions,
            )
            .unwrap()
        } else {
            McpHostBindings::new(
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
            .unwrap()
        };
        store
            .publish_mcp_host_bindings(&bindings, 0, definitions)
            .await
            .unwrap();
        let form_limits = kiln_mcp::McpElicitationValidationLimits {
            form: McpElicitationFormLimits {
                max_message_bytes: NonZeroUsize::new(128).unwrap(),
                max_schema_bytes: NonZeroUsize::new(1024).unwrap(),
            },
            max_response_bytes: NonZeroUsize::new(256).unwrap(),
        };
        // The fixture supplies its explicit decision below. Production daemon
        // construction remains default-off until authenticated endpoints exist.
        let registry = Arc::new(kiln_mcp::StdioRegistry::new_with_elicitation(
            Arc::new(store.clone()),
            NonZeroUsize::new(1).unwrap(),
            form_limits,
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
        .with_native_output_page(Some(
            ToolOutputPageTool::new(
                ToolOutputPageLimits {
                    max_request_bytes: 1024,
                    max_artifact_bytes: 16384,
                    max_page_bytes: 640,
                },
                ModelToolCatalogLimits {
                    max_tools: 1,
                    max_definition_bytes: 4096,
                    max_total_definition_bytes: 4096,
                },
            )
            .unwrap(),
        ))
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
            [
                "read_file",
                "mcp_call",
                "mcp_search",
                "mcp_describe",
                "read_tool_output"
            ]
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
        let mut input_changes = store.subscribe_mcp_input_changes();
        let interact = async {
            if http_protocol == Some(McpProtocolVersion::V20251125) {
                return;
            }
            loop {
                input_changes.borrow_and_update();
                let events = store
                    .list_session_events(session.id(), EventCursor::zero())
                    .await
                    .unwrap();
                let pending = events
                    .events()
                    .iter()
                    .find_map(|event| match event.payload() {
                        SessionEventPayload::McpInputStateChanged {
                            tool_call_id,
                            generation,
                            ordinal,
                            kind: McpInputKind::Elicitation,
                            state: McpInputState::Required,
                            ..
                        } => Some(McpInputRecord {
                            invocation: McpInvocationRecord {
                                tool_call_id: tool_call_id.clone(),
                                generation: generation.clone(),
                                state: McpInvocationState::Dispatching,
                            },
                            ordinal: *ordinal,
                            kind: McpInputKind::Elicitation,
                            state: McpInputState::Required,
                        }),
                        _ => None,
                    });
                if let Some(input) = pending {
                    assert_eq!(
                        store.mcp_input_interaction_run(&input).await.unwrap(),
                        run_id
                    );
                    let form = store
                        .get_mcp_elicitation_form(&input, &run_id, form_limits.form)
                        .await
                        .unwrap();
                    assert_eq!(form.form.message(), "Confirm fixture action");
                    let decision = McpElicitationDecision::from_json(
                        r#"{"action":"accept","content":{"proceed":true}}"#,
                        form_limits.max_response_bytes,
                    )
                    .unwrap();
                    assert_eq!(
                        kiln_mcp::decide_elicitation_form(
                            &store,
                            &input,
                            &run_id,
                            &decision,
                            form_limits
                        )
                        .await
                        .unwrap(),
                        McpElicitationDecisionMutation::Applied
                    );
                    return;
                }
                input_changes.changed().await.unwrap();
            }
        };
        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            let (outcome, (), ()) = tokio::join!(
                service.execute_native_tools(&invocation, &mut cancelled),
                approve,
                interact,
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
            let output = if let Some(artifact) = tool.stdout_artifact() {
                String::from_utf8(
                    service
                        .artifacts
                        .read(artifact.content_hash())
                        .unwrap()
                        .unwrap(),
                )
                .unwrap()
            } else {
                tool.stdout().unwrap().to_owned()
            };
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
        let page_manifest = ContextManifestApplication::new(store.clone(), UlidIdGenerator)
            .create_context_manifest(CreateContextManifest {
                run_id: run_id.clone(),
                entries: vec![ContextManifestEntryInput::ToolExchange {
                    tool_call_id: ids[3].clone(),
                }],
                idempotency_key: "page-context".into(),
            })
            .await
            .unwrap()
            .value;
        let page_invocation = ModelInvocationApplication::new(store.clone(), UlidIdGenerator)
            .create_model_invocation(CreateModelInvocation {
                run_id: run_id.clone(),
                context_manifest_id: page_manifest.context_manifest_id().clone(),
                context_manifest_hash: page_manifest.content_hash().clone(),
                provider_account_id: invocation.provider_account_id().clone(),
                settings: invocation.settings().clone(),
                capabilities: invocation.capabilities().clone(),
                purpose: ModelInvocationPurpose::Generation,
                retry_of: None,
                idempotency_key: "page".into(),
            })
            .await
            .unwrap()
            .value;
        store
            .attach_model_tool_catalog(page_invocation.invocation_id(), tools.catalog())
            .await
            .unwrap();
        let ProviderClaim::Applied {
            request: page_request,
            ..
        } = app
            .claim(page_invocation.invocation_id().clone())
            .await
            .unwrap()
        else {
            panic!("page claim was not fresh")
        };
        let context = page_request
            .assemble_context(
                &kiln_infrastructure::StoredProviderContextReader::new(
                    store.clone(),
                    service.artifacts.clone(),
                ),
                ProviderContextLimits {
                    max_text_bytes: 16384,
                    max_attachment_bytes: 1,
                    max_total_attachment_bytes: 1,
                    max_continuation_bytes: 1,
                    max_total_continuation_bytes: 1,
                },
            )
            .await
            .unwrap();
        let ProviderContextEntry::ToolExchange {
            exchange,
            stdout_artifact,
            stderr_artifact,
        } = &context.entries()[0]
        else {
            panic!("missing exchange")
        };
        assert!(stdout_artifact.is_none() && stderr_artifact.is_none());
        assert!(exchange.tool_call().stdout_artifact().unwrap().size() > 4096);
        let page_invocation = store
            .get_model_invocation(page_invocation.invocation_id())
            .await
            .unwrap()
            .unwrap();
        let requests = ModelToolRequestBatch::new(
            page_invocation.invocation_id().clone(),
            vec![ModelToolRequestInput {
                provider_call_id: "page".into(),
                name: "read_tool_output".into(),
                arguments:
                    json!({"tool_call_id":ids[3].as_str(),"stream":"stdout","offset":0,"limit":640})
                        .as_object()
                        .unwrap()
                        .clone(),
            }],
            ModelToolRequestLimits {
                max_requests: 1,
                max_provider_call_id_bytes: 64,
                max_name_bytes: 64,
                max_arguments_bytes: 1024,
                max_total_arguments_bytes: 1024,
            },
        )
        .unwrap();
        let usage = ProviderUsageUpdate::new(
            ProviderUsageMetadata {
                update_id: "page".into(),
                provider_account_id: page_invocation.provider_account_id().clone(),
                work_id: page_invocation.work_id().clone(),
                model_invocation_id: page_invocation.invocation_id().clone(),
                accounting: UsageAccounting::Cumulative,
                finality: UsageFinality::Final,
                completeness: UsageCompleteness::Unknown,
                observed_at_unix_ms: 2,
                request_id: None,
                resolved_model: None,
                service_tier: None,
                source: UsageSource::NativeProvider,
            },
            vec![],
        )
        .unwrap();
        app.record_tool_requests(&page_invocation, &requests, &usage)
            .await
            .unwrap();
        let approve_page = async {
            loop {
                let snapshot = service.runs.get_run(run_id.clone()).await.unwrap();
                if let Some(approval) = snapshot
                    .approvals()
                    .iter()
                    .find(|a| a.state() == ApprovalState::Pending)
                {
                    let tool = snapshot.tool_call(approval.tool_call_id()).unwrap();
                    assert_eq!(tool.capability(), TOOL_OUTPUT_PAGE_CAPABILITY);
                    assert!(tool.stdout().is_none());
                    let mutation = service
                        .runs
                        .decide_approval(
                            approval.approval_id().clone(),
                            ApprovalState::Approved,
                            "approve-page".into(),
                        )
                        .await
                        .unwrap();
                    service.events.publish(mutation.events);
                    service.active.changed.notify_waiters();
                    break;
                }
                wake.recv().await.unwrap();
            }
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                service.execute_native_tools(&page_invocation, &mut cancelled),
                approve_page
            )
        })
        .await
        .unwrap();
        let super::super::native_tools::NativeToolBatchOutcome::Completed(page_ids) =
            result.unwrap()
        else {
            panic!("page batch failed")
        };
        let (_, page) = store.get_tool_call(&page_ids[0]).await.unwrap().unwrap();
        assert_eq!(page.state(), ToolCallState::Completed);
        assert!(page.stdout_artifact().is_none());
        let value: serde_json::Value = serde_json::from_str(page.stdout().unwrap()).unwrap();
        assert!(value["text"].as_str().unwrap().contains("remote output"));
        assert_eq!(value["next_offset"], 640);
        assert_eq!(value["eof"], false);
        assert!(matches!(
            service
                .execute_native_tools(&page_invocation, &mut cancelled)
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
        let page = store
            .list_session_events(session.id(), EventCursor::zero())
            .await
            .unwrap();
        let inputs: Vec<_> = page
            .events()
            .iter()
            .filter_map(|event| match event.payload() {
                SessionEventPayload::McpInputStateChanged {
                    tool_call_id,
                    state,
                    ..
                } => Some((tool_call_id.clone(), *state)),
                _ => None,
            })
            .collect();
        if http_protocol != Some(McpProtocolVersion::V20251125) {
            assert_eq!(
                inputs,
                [
                    (ids[3].clone(), McpInputState::Required),
                    (ids[3].clone(), McpInputState::Resolved),
                    (ids[3].clone(), McpInputState::Required),
                    (ids[3].clone(), McpInputState::Resolved)
                ]
            );
        } else {
            assert!(inputs.is_empty());
        }
    }
}
