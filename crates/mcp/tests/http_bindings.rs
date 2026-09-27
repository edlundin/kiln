use kiln_core::*;
use kiln_mcp::*;
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::time::Instant;

fn name(s: &str) -> SharedConfigurationKey {
    SharedConfigurationKey::parse(s, 64).unwrap()
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
fn resources() -> HttpLaunchResources {
    let n = NonZeroUsize::new(4096).unwrap();
    HttpLaunchResources {
        generation: McpGenerationId::from_ulid(ulid::Ulid::generate()),
        definition_limits: limits(),
        max_resolved_bytes: n,
        io: McpHttpLimits {
            max_request_bytes: n,
            max_response_bytes: n,
            max_stream_bytes: n,
            max_event_bytes: n,
            max_header_bytes: n,
            request_timeout: Duration::from_secs(1),
        },
        channel_capacity: NonZeroUsize::new(1).unwrap(),
        max_exchanges: n,
        max_catalog_lifetime_bytes: n,
        legacy_resume_delay: None,
        startup_deadline: Instant::now() + Duration::from_secs(5),
    }
}
struct Fixture {
    definition: McpDefinitionRecord,
    record: McpHostBindingRecord,
    instance: KilnInstanceId,
    key: McpInstanceKey,
    reference: SecretRef,
}
impl Fixture {
    fn new(policy: McpProtocolPolicy, host: bool) -> Self {
        let definition = McpServerDefinition::new(
            SharedMcpServerInput {
                id: name("fixture"),
                enabled: true,
                transport: if host {
                    SharedMcpTransport::HostEndpoint {
                        endpoint_binding: name("local"),
                    }
                } else {
                    SharedMcpTransport::Https {
                        endpoint: "https://example.com/mcp".into(),
                        credential_binding: Some(name("token")),
                    }
                },
            },
            policy,
            McpLifecycleScope::Core,
            Some(name("profile")),
            limits(),
        )
        .unwrap();
        let key = McpInstanceKey::new(&definition, McpInstanceOwner::Core, 4096).unwrap();
        let instance = KilnInstanceId::from_ulid(ulid::Ulid::generate());
        let reference = SecretRef::from_ulid(ulid::Ulid::generate());
        let bindings = McpHostBindings::new_http(
            key.clone(),
            McpHttpHostBindingInput {
                instance_id: instance.clone(),
                definition_version: 7,
                working_directory: None,
                endpoint: if host {
                    "http://127.0.0.1/mcp"
                } else {
                    "https://example.com/mcp"
                }
                .into(),
                endpoint_binding: host.then(|| name("local")),
                credential: (!host).then(|| (name("token"), reference.clone())),
            },
            limits(),
        )
        .unwrap();
        Self {
            definition: McpDefinitionRecord {
                definition,
                version: 7,
            },
            record: McpHostBindingRecord {
                bindings,
                revision: 9.try_into().unwrap(),
                retired: false,
            },
            instance,
            key,
            reference,
        }
    }
    fn authorization(&self) -> HttpLaunchAuthorization<'_> {
        HttpLaunchAuthorization {
            instance_id: &self.instance,
            key: &self.key,
            directory: None,
        }
    }
    fn vault(&self, value: &[u8]) -> Vault {
        Vault {
            instance: self.instance.clone(),
            key: self.key.canonical_json().into(),
            reference: self.reference.clone(),
            value: value.to_vec(),
            reads: AtomicUsize::new(0),
            wait: false,
        }
    }
}
struct Vault {
    instance: KilnInstanceId,
    key: String,
    reference: SecretRef,
    value: Vec<u8>,
    reads: AtomicUsize,
    wait: bool,
}
impl McpSecretStore for Vault {
    async fn put_at(&self, _: &McpSecretBinding, _: SecretValue) -> Result<(), SecretStoreError> {
        panic!("no writes")
    }
    async fn delete(&self, _: &McpSecretBinding) -> Result<(), SecretStoreError> {
        panic!("no deletes")
    }
    async fn get(&self, binding: &McpSecretBinding) -> Result<SecretValue, SecretStoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        assert_eq!(binding.instance_id(), &self.instance);
        assert_eq!(binding.key().canonical_json(), self.key);
        assert_eq!(binding.secret_ref(), &self.reference);
        assert_eq!(binding.name().as_str(), "token");
        assert_eq!(binding.purpose(), McpSecretPurpose::HttpCredential);
        if self.wait {
            std::future::pending::<()>().await;
        }
        SecretValue::new(self.value.as_slice()).map_err(|_| SecretStoreError::InvalidSecret)
    }
}

#[tokio::test]
async fn resolution_pins_revision_and_protocol_without_ambient_headers() {
    for policy in [
        McpProtocolPolicy::Auto,
        McpProtocolPolicy::Pinned(McpProtocolVersion::V20251125),
        McpProtocolPolicy::Pinned(McpProtocolVersion::V20260728),
    ] {
        let f = Fixture::new(policy, false);
        let vault = f.vault(b"opaque-token==");
        let result = resolve_persisted_http_launch(
            &f.definition,
            f.record.clone(),
            f.authorization(),
            resources(),
            &vault,
        )
        .await
        .unwrap();
        assert_eq!(result.host_binding_version.instance_id, f.instance);
        assert_eq!(result.host_binding_version.revision.get(), 9);
        assert_eq!(result.definition_version, 7);
        assert_eq!(result.key.canonical_json(), f.key.canonical_json());
        assert_eq!(result.policy, policy);
        assert_eq!(
            result.config.protocol,
            match policy {
                McpProtocolPolicy::Auto => McpProtocolVersion::V20260728,
                McpProtocolPolicy::Pinned(v) => v,
            }
        );
        assert_eq!(
            result.config.bearer_token.as_deref(),
            Some("opaque-token==")
        );
        assert!(result.config.headers.is_empty());
        assert_eq!(vault.reads.load(Ordering::SeqCst), 1);
    }
    let f = Fixture::new(McpProtocolPolicy::Auto, true);
    let vault = f.vault(b"unused");
    let result = resolve_persisted_http_launch(
        &f.definition,
        f.record.clone(),
        f.authorization(),
        resources(),
        &vault,
    )
    .await
    .unwrap();
    assert!(result.config.bearer_token.is_none());
    assert_eq!(vault.reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn identity_version_policy_and_limits_fail_before_vault_access() {
    let mut f = Fixture::new(McpProtocolPolicy::Auto, false);
    let vault = f.vault(b"unused");
    let other = KilnInstanceId::from_ulid(ulid::Ulid::generate());
    assert_eq!(
        resolve_persisted_http_launch(
            &f.definition,
            f.record.clone(),
            HttpLaunchAuthorization {
                instance_id: &other,
                ..f.authorization()
            },
            resources(),
            &vault
        )
        .await
        .err(),
        Some(HttpBindingError::ScopeMismatch)
    );
    for (endpoint, credential, profile, expected) in [
        (
            "https://other.example.com/mcp",
            Some(name("token")),
            Some(name("profile")),
            HttpBindingError::InvalidBinding,
        ),
        (
            "https://example.com/mcp",
            Some(name("other")),
            Some(name("profile")),
            HttpBindingError::InvalidBinding,
        ),
        (
            "https://example.com/mcp",
            Some(name("token")),
            Some(name("other")),
            HttpBindingError::ScopeMismatch,
        ),
    ] {
        let changed = McpDefinitionRecord {
            definition: McpServerDefinition::new(
                SharedMcpServerInput {
                    id: name("fixture"),
                    enabled: true,
                    transport: SharedMcpTransport::Https {
                        endpoint: endpoint.into(),
                        credential_binding: credential,
                    },
                },
                McpProtocolPolicy::Auto,
                McpLifecycleScope::Core,
                profile,
                limits(),
            )
            .unwrap(),
            version: 7,
        };
        assert_eq!(
            resolve_persisted_http_launch(
                &changed,
                f.record.clone(),
                f.authorization(),
                resources(),
                &vault
            )
            .await
            .err(),
            Some(expected)
        );
    }
    f.definition.version = 8;
    assert_eq!(
        resolve_persisted_http_launch(
            &f.definition,
            f.record.clone(),
            f.authorization(),
            resources(),
            &vault
        )
        .await
        .err(),
        Some(HttpBindingError::DefinitionChanged)
    );
    f.definition.version = 7;
    let mut retired = f.record.clone();
    retired.retired = true;
    assert_eq!(
        resolve_persisted_http_launch(
            &f.definition,
            retired,
            f.authorization(),
            resources(),
            &vault
        )
        .await
        .err(),
        Some(HttpBindingError::Disabled)
    );
    let mut small = resources();
    small.max_resolved_bytes = NonZeroUsize::new(1).unwrap();
    assert_eq!(
        resolve_persisted_http_launch(
            &f.definition,
            f.record.clone(),
            f.authorization(),
            small,
            &vault
        )
        .await
        .err(),
        Some(HttpBindingError::LimitExceeded)
    );
    let mut small = resources();
    small.definition_limits.max_metadata_bytes = 1;
    assert!(
        resolve_persisted_http_launch(
            &f.definition,
            f.record.clone(),
            f.authorization(),
            small,
            &vault
        )
        .await
        .is_err()
    );
    assert_eq!(vault.reads.load(Ordering::SeqCst), 0);
    let f = Fixture::new(
        McpProtocolPolicy::Pinned(McpProtocolVersion::V20241105),
        false,
    );
    assert_eq!(
        resolve_persisted_http_launch(
            &f.definition,
            f.record.clone(),
            f.authorization(),
            resources(),
            &vault
        )
        .await
        .err(),
        Some(HttpBindingError::UnsupportedTransport)
    );
    let f = Fixture::new(
        McpProtocolPolicy::Pinned(McpProtocolVersion::V20260728),
        false,
    );
    let mut resume = resources();
    resume.legacy_resume_delay = Some(Duration::from_secs(1));
    assert_eq!(
        resolve_persisted_http_launch(
            &f.definition,
            f.record.clone(),
            f.authorization(),
            resume,
            &vault
        )
        .await
        .err(),
        Some(HttpBindingError::InvalidLimits)
    );
    assert_eq!(vault.reads.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn credential_envelope_retention_and_resolution_deadline_are_enforced() {
    let f = Fixture::new(McpProtocolPolicy::Auto, false);
    for value in [
        b"has space".as_slice(),
        b"a=b",
        b"===",
        b"nonascii-\xc3\xa9",
    ] {
        let vault = f.vault(value);
        assert_eq!(
            resolve_persisted_http_launch(
                &f.definition,
                f.record.clone(),
                f.authorization(),
                resources(),
                &vault
            )
            .await
            .err(),
            Some(HttpBindingError::InvalidCredential)
        );
    }
    let vault = f.vault(b"valid-token");
    let mut small = resources();
    small.max_resolved_bytes = NonZeroUsize::new("https://example.com/mcp".len()).unwrap();
    assert_eq!(
        resolve_persisted_http_launch(
            &f.definition,
            f.record.clone(),
            f.authorization(),
            small,
            &vault
        )
        .await
        .err(),
        Some(HttpBindingError::LimitExceeded)
    );
    let mut small = resources();
    small.io.max_header_bytes = NonZeroUsize::new(10).unwrap();
    assert_eq!(
        resolve_persisted_http_launch(
            &f.definition,
            f.record.clone(),
            f.authorization(),
            small,
            &vault
        )
        .await
        .err(),
        Some(HttpBindingError::LimitExceeded)
    );
    let mut vault = f.vault(b"valid-token");
    vault.wait = true;
    let mut timed = resources();
    timed.startup_deadline = Instant::now() + Duration::from_secs(1);
    assert_eq!(
        resolve_persisted_http_launch(
            &f.definition,
            f.record.clone(),
            f.authorization(),
            timed,
            &vault
        )
        .await
        .err(),
        Some(HttpBindingError::Deadline)
    );
    assert_eq!(vault.reads.load(Ordering::SeqCst), 1);
}
