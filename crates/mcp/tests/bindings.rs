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

struct FixtureVault {
    instance: KilnInstanceId,
    key: String,
    reference: SecretRef,
    reads: std::sync::Mutex<Vec<McpSecretPurpose>>,
}
impl McpSecretStore for FixtureVault {
    async fn put_at(&self, _: &McpSecretBinding, _: SecretValue) -> Result<(), SecretStoreError> {
        panic!("resolution must never write credentials")
    }
    async fn delete(&self, _: &McpSecretBinding) -> Result<(), SecretStoreError> {
        panic!("resolution must never delete credentials")
    }
    async fn get(&self, binding: &McpSecretBinding) -> Result<SecretValue, SecretStoreError> {
        assert_eq!(binding.instance_id(), &self.instance);
        assert_eq!(binding.key().canonical_json(), self.key);
        self.reads.lock().unwrap().push(binding.purpose());
        if binding.secret_ref() != &self.reference {
            return Err(SecretStoreError::NotFound);
        }
        let value = match (binding.purpose(), binding.name().as_str()) {
            (McpSecretPurpose::Argument, "argument") => "$(touch surprise)",
            (McpSecretPurpose::Environment, "environment") => "private fixture",
            _ => return Err(SecretStoreError::NotFound),
        };
        Ok(SecretValue::new(value.as_bytes()).unwrap())
    }
}

fn references(bindings: StdioHostBindings) -> (StdioHostBindingReferences, FixtureVault) {
    let instance = KilnInstanceId::from_ulid(ulid::Ulid::generate());
    // Reusing this synthetic reference in both roles must still produce two
    // different lookups; no real OS vault is accessed by this test.
    let reference = SecretRef::from_ulid(ulid::Ulid::generate());
    let vault = FixtureVault {
        instance: instance.clone(),
        key: bindings.key.canonical_json().into(),
        reference: reference.clone(),
        reads: Default::default(),
    };
    (
        StdioHostBindingReferences {
            instance_id: instance,
            key: bindings.key,
            definition_version: bindings.definition_version,
            revision: bindings.revision,
            runtime_binding: bindings.runtime_binding,
            executable: bindings.executable,
            arguments: BTreeMap::from([
                (key("argument"), reference.clone()),
                (key("unused"), reference.clone()),
            ]),
            environment: BTreeMap::from([(key("environment"), reference)]),
        },
        vault,
    )
}

#[tokio::test]
async fn vault_resolution_is_scoped_role_specific_and_reads_only_needed_references() {
    let directory = tempfile::tempdir().unwrap();
    let (definition, bindings, resources) = fixture(directory.path());
    let (refs, vault) = references(bindings);
    let resolved = resolve_stdio_launch_from_vault(&definition, refs, resources, &vault)
        .await
        .unwrap();
    assert_eq!(
        resolved.launch.process.arguments.last().unwrap(),
        "$(touch surprise)"
    );
    assert_eq!(
        resolved.launch.process.environment[&OsString::from("BINDING")],
        "private fixture"
    );
    assert_eq!(
        *vault.reads.lock().unwrap(),
        [McpSecretPurpose::Argument, McpSecretPurpose::Environment]
    );
    for mode in ["version", "missing", "budget", "wrong-reference"] {
        let (definition, bindings, mut resources) = fixture(directory.path());
        let (mut refs, vault) = references(bindings);
        let expected = match mode {
            "version" => {
                refs.definition_version += 1;
                StdioBindingError::DefinitionChanged
            }
            "missing" => {
                refs.environment.clear();
                StdioBindingError::MissingEnvironment
            }
            "budget" => {
                resources.max_resolved_bytes = NonZeroUsize::new(1).unwrap();
                StdioBindingError::LimitExceeded
            }
            _ => {
                refs.arguments.insert(
                    key("argument"),
                    SecretRef::from_ulid(ulid::Ulid::generate()),
                );
                StdioBindingError::Secret(SecretStoreError::NotFound)
            }
        };
        assert_eq!(
            resolve_stdio_launch_from_vault(&definition, refs, resources, &vault)
                .await
                .err(),
            Some(expected)
        );
        if mode == "wrong-reference" {
            assert_eq!(
                *vault.reads.lock().unwrap(),
                [McpSecretPurpose::Argument],
                "a missing secret must not fall back or continue to other reads"
            );
        } else {
            assert!(
                vault.reads.lock().unwrap().is_empty(),
                "{mode} must fail before vault access"
            );
        }
    }
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
