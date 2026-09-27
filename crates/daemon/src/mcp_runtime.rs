use std::{num::NonZeroUsize, sync::Arc};

use kiln_core::{McpInstanceStore, McpInvocationStore};
use kiln_infrastructure::SqliteStore;
use kiln_mcp::StdioRegistry;

/// Internal opt-in until launch administration and durable tool dispatch exist.
/// Startup holds the daemon store lock before this function is called.
pub(crate) async fn configured_registry(
    store: &SqliteStore,
) -> Result<Option<Arc<StdioRegistry<SqliteStore>>>, &'static str> {
    let capacity = std::env::var("KILN_MCP_MAX_INSTANCES");
    let recovery_batch = std::env::var("KILN_MCP_RECOVERY_BATCH_SIZE");
    if matches!(capacity, Err(std::env::VarError::NotPresent))
        && matches!(recovery_batch, Err(std::env::VarError::NotPresent))
    {
        return Ok(None);
    }
    let invalid = "MCP runtime requires positive instance capacity and recovery batch size";
    let capacity: NonZeroUsize = capacity
        .map_err(|_| invalid)?
        .parse()
        .map_err(|_| invalid)?;
    let recovery_batch: NonZeroUsize = recovery_batch
        .map_err(|_| invalid)?
        .parse()
        .map_err(|_| invalid)?;
    recover(store, recovery_batch).await?;
    Ok(Some(Arc::new(StdioRegistry::new(
        Arc::new(store.clone()),
        capacity,
    ))))
}

async fn recover(store: &SqliteStore, batch_size: NonZeroUsize) -> Result<(), &'static str> {
    // Recovery records uncertainty only. Without evidence of orphan cleanup,
    // interrupted generations remain blocked against replacement.
    loop {
        let count = store
            .interrupt_mcp_instances_after_restart(batch_size)
            .await
            .map_err(|_| "cannot record interrupted MCP generations")?;
        if count < batch_size.get() {
            break;
        }
    }
    loop {
        let count = store
            .interrupt_mcp_invocations(None, batch_size)
            .await
            .map_err(|_| "cannot record interrupted MCP invocations")?;
        if count < batch_size.get() {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kiln_core::*;
    use std::{collections::BTreeMap, time::Duration};

    fn limits() -> McpDefinitionLimits {
        McpDefinitionLimits {
            max_key_bytes: 64,
            max_metadata_bytes: 4096,
            max_arguments: 4,
            max_argument_bytes: 64,
            max_environment: 4,
            max_endpoint_bytes: 128,
        }
    }

    async fn register(store: &SqliteStore, name: &str) -> McpInstanceKey {
        let definition = McpServerDefinition::new(
            SharedMcpServerInput {
                id: SharedConfigurationKey::parse(name, 64).unwrap(),
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
            limits(),
        )
        .unwrap();
        store
            .register_mcp_definition(&definition, 0, name, limits())
            .await
            .unwrap();
        McpInstanceKey::new(&definition, McpInstanceOwner::Core, 4096).unwrap()
    }

    #[tokio::test]
    async fn restart_batches_leave_all_uncertain_generations_blocked() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path()).await.unwrap();
        let first = register(&store, "first").await;
        let second = register(&store, "second").await;
        for key in [&first, &second] {
            assert!(matches!(
                store
                    .claim_mcp_instance(
                        key,
                        1,
                        &McpGenerationId::from_ulid(ulid::Ulid::generate()),
                        limits()
                    )
                    .await
                    .unwrap(),
                McpInstanceClaim::Acquired(_)
            ));
        }
        recover(&store, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap();
        recover(&store, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap();
        for key in [&first, &second] {
            let record = store.get_mcp_instance(key).await.unwrap().unwrap();
            assert_eq!(record.observed, McpObservedState::Interrupted);
            assert_eq!(record.state_version, 2);
            assert!(matches!(
                store
                    .claim_mcp_instance(
                        key,
                        1,
                        &McpGenerationId::from_ulid(ulid::Ulid::generate()),
                        limits()
                    )
                    .await
                    .unwrap(),
                McpInstanceClaim::Existing(_)
            ));
        }
    }

    #[tokio::test]
    async fn run_service_shutdown_drains_its_mcp_registry() {
        use kiln_infrastructure::{
            DeterministicOutcome, DeterministicSubprocessExecutor, FileArtifactStore,
            UlidIdGenerator,
        };
        use kiln_mcp::{StdioGenerationLaunch, StdioProcessConfig};
        use rustix::{
            io::Errno,
            process::{Pid, test_kill_process},
        };

        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path()).await.unwrap();
        let key = register(&store, "fixture").await;
        let registry = Arc::new(StdioRegistry::new(
            Arc::new(store.clone()),
            NonZeroUsize::new(1).unwrap(),
        ));
        registry.ensure_ready(StdioGenerationLaunch {
            host_binding_version: None,
            key: key.clone(), definition_version: 1,
            generation: McpGenerationId::from_ulid(ulid::Ulid::generate()), definition_limits: limits(),
            // The fixture has a bounded scheduling allowance, not a product default.
            startup_deadline: tokio::time::Instant::now() + Duration::from_secs(5),
            process: StdioProcessConfig {
                executable: "/bin/sh".into(),
                arguments: vec!["-c".into(), r#"
                    printf '%s' "$$" > pid
                    IFS= read -r request || exit 31
                    printf '%s\n' '{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2025-11-25","capabilities":{},"serverInfo":{"name":"fixture","version":"1"}}}'
                    while IFS= read -r request; do :; done
                "#.into()],
                working_directory: std::fs::File::open(directory.path()).unwrap().into(),
                environment: BTreeMap::new(), max_frame_bytes: NonZeroUsize::new(512).unwrap(),
                shutdown_grace: Duration::ZERO,
            },
        }, 1.try_into().unwrap()).await.unwrap();
        let pid = Pid::from_raw(
            std::fs::read_to_string(directory.path().join("pid"))
                .unwrap()
                .parse()
                .unwrap(),
        )
        .unwrap();
        let service = crate::run_service::RunService::new(
            RunApplication::new(store.clone(), UlidIdGenerator),
            DeterministicSubprocessExecutor::new(DeterministicOutcome::Success),
            kiln_server::EventBroadcaster::default(),
            store.clone(),
            FileArtifactStore::open(directory.path()).unwrap(),
        )
        .with_mcp_registry(Some(registry));
        service.shutdown().await.unwrap();
        assert_eq!(test_kill_process(pid), Err(Errno::SRCH));
        assert_eq!(
            store
                .get_mcp_instance(&key)
                .await
                .unwrap()
                .unwrap()
                .observed,
            McpObservedState::Stopped
        );
    }
}
