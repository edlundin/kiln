#![cfg(unix)]

use kiln_core::*;
use kiln_mcp::*;
use rmcp::{model::ServerJsonRpcMessage, transport::Transport};
use std::{collections::BTreeMap, ffi::OsString, num::NonZeroUsize, path::Path, time::Duration};

fn key(value: &str) -> SharedConfigurationKey {
    SharedConfigurationKey::parse(value, 64).unwrap()
}
fn limits() -> McpDefinitionLimits {
    McpDefinitionLimits {
        max_key_bytes: 64,
        max_metadata_bytes: 4096,
        max_arguments: 8,
        max_argument_bytes: 1024,
        max_environment: 4,
        max_endpoint_bytes: 128,
    }
}

fn fixture(path: &Path) -> (McpDefinitionRecord, StdioHostBindings, StdioLaunchResources) {
    let definition = McpServerDefinition::new(SharedMcpServerInput {
        id: key("fixture"), enabled: true,
        transport: SharedMcpTransport::Stdio {
            runtime_binding: key("shell"),
            arguments: vec![SharedMcpArgument::Literal("-c".into()), SharedMcpArgument::Literal(r#"
                test -z "${HOME+x}" || exit 31
                test "$BINDING" = 'private fixture' || exit 32
                test "$1" = '$(touch surprise)' || exit 33
                test -f authorized || exit 34
                printf '%s\n' '{"jsonrpc":"2.0","id":0,"error":{"code":-32601,"message":"resolved"}}'
                while IFS= read -r request; do :; done
            "#.into()), SharedMcpArgument::Literal("fixture".into()), SharedMcpArgument::HostBinding(key("argument"))],
            environment: BTreeMap::from([("BINDING".into(), key("environment"))]),
        },
    }, McpProtocolPolicy::Pinned(McpProtocolVersion::V20251125), McpLifecycleScope::Core,
        Some(key("profile")), limits()).unwrap();
    let bindings = StdioHostBindings {
        key: McpInstanceKey::new(&definition, McpInstanceOwner::Core, 4096).unwrap(),
        definition_version: 1,
        revision: 7.try_into().unwrap(),
        runtime_binding: key("shell"),
        executable: "/bin/sh".into(),
        arguments: BTreeMap::from([(key("argument"), OsString::from("$(touch surprise)"))]),
        environment: BTreeMap::from([(key("environment"), OsString::from("private fixture"))]),
    };
    let resources = StdioLaunchResources {
        generation: McpGenerationId::from_ulid(ulid::Ulid::generate()),
        working_directory: std::fs::File::open(path).unwrap().into(),
        definition_limits: limits(),
        max_resolved_bytes: NonZeroUsize::new(4096).unwrap(),
        max_frame_bytes: NonZeroUsize::new(512).unwrap(),
        shutdown_grace: Duration::ZERO,
        startup_deadline: tokio::time::Instant::now() + Duration::from_secs(5),
    };
    (
        McpDefinitionRecord {
            definition,
            version: 1,
        },
        bindings,
        resources,
    )
}

#[tokio::test]
async fn resolved_values_are_literal_explicit_and_pinned_to_the_supplied_directory() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("authorized"), b"").unwrap();
    let (definition, bindings, resources) = fixture(directory.path());
    let resolved = resolve_stdio_launch(&definition, bindings, resources).unwrap();
    assert_eq!(resolved.binding_revision.get(), 7);
    assert_eq!(resolved.launch.definition_version, 1);
    let mut process = StdioProcess::spawn(resolved.launch.process).unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(5), process.receive())
        .await
        .unwrap();
    let Some(ServerJsonRpcMessage::Error(reply)) = reply else {
        panic!("fixture launch checks failed");
    };
    assert_eq!(reply.error.message.as_ref(), "resolved");
    process.close().await.unwrap();
    assert!(!directory.path().join("surprise").exists());
}

#[test]
fn bindings_do_not_cross_versions_profiles_or_argument_environment_roles() {
    let directory = tempfile::tempdir().unwrap();
    let (definition, mut bindings, resources) = fixture(directory.path());
    bindings.definition_version = 2;
    assert_eq!(
        resolve_stdio_launch(&definition, bindings, resources).err(),
        Some(StdioBindingError::DefinitionChanged)
    );
    let (definition, mut bindings, resources) = fixture(directory.path());
    let other = McpServerDefinition::new(
        definition.definition.server().clone(),
        definition.definition.protocol(),
        McpLifecycleScope::Core,
        Some(key("other-profile")),
        limits(),
    )
    .unwrap();
    bindings.key = McpInstanceKey::new(&other, McpInstanceOwner::Core, 4096).unwrap();
    assert_eq!(
        resolve_stdio_launch(&definition, bindings, resources).err(),
        Some(StdioBindingError::ScopeMismatch)
    );
    let (definition, mut bindings, resources) = fixture(directory.path());
    bindings.runtime_binding = key("other-runtime");
    assert_eq!(
        resolve_stdio_launch(&definition, bindings, resources).err(),
        Some(StdioBindingError::RuntimeMismatch)
    );
    let (definition, mut bindings, resources) = fixture(directory.path());
    bindings.environment.insert(
        key("argument"),
        bindings.arguments.remove(&key("argument")).unwrap(),
    );
    assert_eq!(
        resolve_stdio_launch(&definition, bindings, resources).err(),
        Some(StdioBindingError::MissingArgument)
    );
    let (definition, mut bindings, resources) = fixture(directory.path());
    bindings.arguments.insert(
        key("environment"),
        bindings.environment.remove(&key("environment")).unwrap(),
    );
    assert_eq!(
        resolve_stdio_launch(&definition, bindings, resources).err(),
        Some(StdioBindingError::MissingEnvironment)
    );
}

#[test]
fn output_budget_counts_terminators_and_rejects_nul_values() {
    let directory = tempfile::tempdir().unwrap();
    let (definition, bindings, resources) = fixture(directory.path());
    let resolved = resolve_stdio_launch(&definition, bindings, resources).unwrap();
    let config = resolved.launch.process;
    let exact = config.executable.as_os_str().as_encoded_bytes().len()
        + 1
        + config
            .arguments
            .iter()
            .map(|v| v.as_encoded_bytes().len() + 1)
            .sum::<usize>()
        + config
            .environment
            .iter()
            .map(|(k, v)| k.as_encoded_bytes().len() + v.as_encoded_bytes().len() + 2)
            .sum::<usize>();
    let (definition, bindings, mut resources) = fixture(directory.path());
    resources.max_resolved_bytes = NonZeroUsize::new(exact).unwrap();
    assert!(resolve_stdio_launch(&definition, bindings, resources).is_ok());
    let (definition, bindings, mut resources) = fixture(directory.path());
    resources.max_resolved_bytes = NonZeroUsize::new(exact - 1).unwrap();
    assert_eq!(
        resolve_stdio_launch(&definition, bindings, resources).err(),
        Some(StdioBindingError::LimitExceeded)
    );
    let (definition, mut bindings, resources) = fixture(directory.path());
    bindings
        .environment
        .insert(key("environment"), OsString::from("a\0b"));
    assert_eq!(
        resolve_stdio_launch(&definition, bindings, resources).err(),
        Some(StdioBindingError::InvalidValue)
    );
}
