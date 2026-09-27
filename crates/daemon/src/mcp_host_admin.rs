//! Offline host metadata and MCP-vault administration under exclusive store ownership.

use kiln_core::*;
use kiln_infrastructure::{DaemonStoreLock, OsMcpSecretStore, SqliteStore};
use std::{
    collections::BTreeMap,
    io::{IsTerminal, Read},
    num::NonZeroUsize,
    process::ExitCode,
};

const USAGE: &str = "Usage: kilnd mcp-host-admin --max-bytes N --stdin
Stop kilnd first. Pipe canonical JSON containing key and action.
Actions: inspect, pending, import_secret, publish, publish_http, retire, reconcile.
The positive byte budget bounds input and each stored snapshot plus its key.
Import uses a fresh reference; ambiguous imports are never repeated at that reference.
Reconcile deletes unpublished imports as well as retired values; publish wanted imports first.
These commands grant no process permission and never launch a server.";

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    key: serde_json::Value,
    action: Action,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    Inspect,
    Pending {
        batch_size: NonZeroUsize,
    },
    ImportSecret {
        definition_version: u64,
        name: String,
        purpose: String,
        value: String,
    },
    Publish {
        expected_revision: u64,
        definition_version: u64,
        runtime_binding: String,
        executable: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        working_directory: Option<McpHostWorkingDirectory>,
        arguments: BTreeMap<String, String>,
        environment: BTreeMap<String, String>,
    },
    PublishHttp {
        expected_revision: u64,
        definition_version: u64,
        endpoint: String,
        endpoint_binding: Option<String>,
        credential: Option<(String, String)>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        working_directory: Option<McpHostWorkingDirectory>,
    },
    Retire {
        expected_revision: std::num::NonZeroU64,
    },
    Reconcile {
        batch_size: NonZeroUsize,
    },
}

pub(crate) async fn run() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(2).collect();
    if args == ["--help"] {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    match run_locked(&args).await {
        Ok(value) => {
            println!("{value}");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("kilnd: {message}");
            ExitCode::FAILURE
        }
    }
}

async fn run_locked(args: &[String]) -> Result<String, &'static str> {
    let mut budget = None;
    let mut stdin = false;
    let mut flags = args.iter();
    while let Some(flag) = flags.next() {
        match flag.as_str() {
            "--max-bytes" if budget.is_none() => {
                budget = Some(
                    flags
                        .next()
                        .ok_or(USAGE)?
                        .parse::<NonZeroUsize>()
                        .map_err(|_| USAGE)?,
                )
            }
            "--stdin" if !stdin => stdin = true,
            _ => return Err(USAGE),
        }
    }
    let budget = budget.ok_or(USAGE)?.get();
    if !stdin {
        return Err(USAGE);
    }
    if std::io::stdin().is_terminal() {
        return Err("pipe canonical host administration JSON to stdin");
    }
    let read_limit = budget
        .checked_add(3)
        .and_then(|n| u64::try_from(n).ok())
        .ok_or(USAGE)?;
    let mut bytes = Vec::new();
    std::io::stdin()
        .lock()
        .take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read host administration JSON")?;
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    let request = parse(&bytes, budget)?;
    let _lock = DaemonStoreLock::open_default()
        .map_err(|_| "cannot lock data directory; stop kilnd before MCP host administration")?;
    let store = SqliteStore::open_default()
        .await
        .map_err(|_| "cannot open host binding store")?;
    execute(&store, &OsMcpSecretStore::open_default(), request, budget)
        .await
        .map(|v| v.to_string())
}

fn parse(bytes: &[u8], budget: usize) -> Result<Request, &'static str> {
    if budget == 0 || bytes.len() > budget {
        return Err("host administration input exceeds byte budget");
    }
    let request: Request =
        serde_json::from_slice(bytes).map_err(|_| "invalid host administration input")?;
    let mut value =
        serde_json::to_value(&request).map_err(|_| "invalid host administration input")?;
    value.sort_all_objects();
    let canonical = serde_json::to_vec(&value).map_err(|_| "invalid host administration input")?;
    if canonical != bytes {
        return Err(
            "host administration JSON must be canonical, with no duplicate or unknown fields",
        );
    }
    Ok(request)
}

fn limits(budget: usize) -> McpDefinitionLimits {
    McpDefinitionLimits {
        max_key_bytes: budget,
        max_metadata_bytes: budget,
        max_arguments: budget,
        max_argument_bytes: budget,
        max_environment: budget,
        max_endpoint_bytes: budget,
    }
}

async fn execute<V: McpSecretStore>(
    store: &SqliteStore,
    vault: &V,
    request: Request,
    budget: usize,
) -> Result<serde_json::Value, &'static str> {
    let limits = limits(budget);
    let key_bytes = serde_json::to_vec(&request.key).map_err(|_| "invalid scoped key")?;
    let key = McpInstanceKey::from_canonical_json(&key_bytes, budget)
        .map_err(|_| "invalid scoped key")?;
    let instance = store
        .initialize_configuration_instance(KilnInstanceId::from_ulid(ulid::Ulid::generate()))
        .await
        .map_err(|_| "cannot initialize local instance identity")?
        .instance_id()
        .clone();
    match request.action {
        Action::Inspect => {
            let current = store
                .get_mcp_host_bindings(&key, limits)
                .await
                .map_err(binding_error)?;
            let snapshot = current.map(|r| Ok::<_, &'static str>(serde_json::json!({"revision":r.revision.get(), "retired":r.retired,
                "bindings":serde_json::from_str::<serde_json::Value>(r.bindings.metadata_json()).map_err(|_| "invalid stored snapshot")?}))).transpose()?;
            Ok(serde_json::json!({"instance_id":instance.as_str(), "snapshot":snapshot}))
        }
        Action::Pending { batch_size } => {
            let pending = store
                .pending_mcp_secret_reservations(&instance, &key, batch_size)
                .await
                .map_err(journal_error)?;
            Ok(
                serde_json::json!({"pending":pending.into_iter().map(|(b,s)| serde_json::json!({
                "name":b.name().as_str(), "purpose":b.purpose().as_str(), "secret_ref":b.secret_ref().as_str(),
                "state":match s { McpSecretReservationState::Reserved => "reserved", McpSecretReservationState::Retired => "retired", McpSecretReservationState::Deleted => "deleted" }
            })).collect::<Vec<_>>()}),
            )
        }
        Action::ImportSecret {
            definition_version,
            name,
            purpose,
            value,
        } => {
            let purpose = match purpose.as_str() {
                "argument" => McpSecretPurpose::Argument,
                "environment" => McpSecretPurpose::Environment,
                "http_credential" => McpSecretPurpose::HttpCredential,
                _ => return Err("invalid secret purpose"),
            };
            let secret =
                SecretValue::new(value.as_bytes()).map_err(|_| "invalid secret envelope")?;
            let reference = SecretRef::from_ulid(ulid::Ulid::generate());
            let binding = McpSecretBinding::new(
                instance,
                key,
                SharedConfigurationKey::parse(name, budget).map_err(|_| "invalid binding name")?,
                purpose,
                reference.clone(),
            );
            if !matches!(
                store
                    .reserve_mcp_secret(&binding, definition_version, limits)
                    .await
                    .map_err(journal_error)?,
                McpSecretReservation::Fresh
            ) {
                return Err("secret reference already reserved; no write was performed");
            }
            // Never delete on an ambiguous write result. The durable reservation
            // remains visible to explicit reconciliation under this store lock.
            vault.put_at(&binding, secret).await.map_err(
                |_| "MCP vault write failed; pending reservation retained for reconciliation",
            )?;
            Ok(serde_json::json!({"secret_ref":reference.as_str()}))
        }
        Action::Publish {
            expected_revision,
            definition_version,
            runtime_binding,
            executable,
            working_directory,
            arguments,
            environment,
        } => {
            let refs = |map: BTreeMap<String, String>| {
                map.into_iter()
                    .map(|(name, reference)| {
                        Ok((
                            SharedConfigurationKey::parse(name, budget)
                                .map_err(|_| "invalid binding name")?,
                            SecretRef::parse(reference).map_err(|_| "invalid secret reference")?,
                        ))
                    })
                    .collect::<Result<BTreeMap<_, _>, &'static str>>()
            };
            let bindings = McpHostBindings::new(
                key,
                McpHostBindingInput {
                    instance_id: instance,
                    definition_version,
                    runtime_binding: SharedConfigurationKey::parse(runtime_binding, budget)
                        .map_err(|_| "invalid runtime binding")?,
                    executable,
                    working_directory,
                    arguments: refs(arguments)?,
                    environment: refs(environment)?,
                },
                limits,
            )
            .map_err(binding_error)?;
            publish(store, vault, &bindings, expected_revision, limits).await
        }
        Action::PublishHttp {
            expected_revision,
            definition_version,
            endpoint,
            endpoint_binding,
            credential,
            working_directory,
        } => {
            let bindings = McpHostBindings::new_http(
                key,
                McpHttpHostBindingInput {
                    instance_id: instance,
                    definition_version,
                    endpoint,
                    endpoint_binding: endpoint_binding
                        .map(|name| {
                            SharedConfigurationKey::parse(name, budget)
                                .map_err(|_| "invalid endpoint binding")
                        })
                        .transpose()?,
                    credential: credential
                        .map(|(name, reference)| {
                            Ok::<_, &'static str>((
                                SharedConfigurationKey::parse(name, budget)
                                    .map_err(|_| "invalid credential binding")?,
                                SecretRef::parse(reference)
                                    .map_err(|_| "invalid secret reference")?,
                            ))
                        })
                        .transpose()?,
                    working_directory,
                },
                limits,
            )
            .map_err(binding_error)?;
            publish(store, vault, &bindings, expected_revision, limits).await
        }

        Action::Retire { expected_revision } => {
            let record = store
                .retire_mcp_host_bindings(&key, expected_revision, limits)
                .await
                .map_err(binding_error)?;
            Ok(serde_json::json!({"retired_revision":record.revision.get()}))
        }
        Action::Reconcile { batch_size } => {
            let pending = store
                .pending_mcp_secret_reservations(&instance, &key, batch_size)
                .await
                .map_err(journal_error)?;
            let count = pending.len();
            for (binding, _) in pending {
                let state = store
                    .retire_mcp_secret_reservation(&binding)
                    .await
                    .map_err(journal_error)?;
                if state != McpSecretReservationState::Deleted {
                    match vault.delete(&binding).await {
                        Ok(()) | Err(SecretStoreError::NotFound) => {}
                        Err(_) => {
                            return Err(
                                "MCP vault deletion failed; retired reference retained for retry",
                            );
                        }
                    }
                    store
                        .finish_mcp_secret_deletion(&binding)
                        .await
                        .map_err(journal_error)?;
                }
            }
            Ok(serde_json::json!({"reconciled":count}))
        }
    }
}

async fn publish<V: McpSecretStore>(
    store: &SqliteStore,
    vault: &V,
    bindings: &McpHostBindings,
    expected_revision: u64,
    limits: McpDefinitionLimits,
) -> Result<serde_json::Value, &'static str> {
    if let Some(receipt) = store
        .inspect_mcp_host_binding_publication(bindings, expected_revision, limits)
        .await
        .map_err(binding_error)?
    {
        return Ok(serde_json::json!({"registered_revision":receipt.revision.get()}));
    }
    let identities = bindings
        .references()
        .map(|(purpose, name, reference)| {
            McpSecretBinding::new(
                bindings.instance_id().clone(),
                bindings.key().clone(),
                name.clone(),
                purpose,
                reference.clone(),
            )
        })
        .collect::<Vec<_>>();
    // Preflight checked all identities. Exclusive store ownership spans
    // vault reads and publication; the store revalidates before commit.
    for identity in &identities {
        vault
            .get(identity)
            .await
            .map_err(|_| "snapshot secret is unavailable in the MCP vault")?;
    }
    let record = store
        .publish_mcp_host_bindings(bindings, expected_revision, limits)
        .await
        .map_err(binding_error)?;
    Ok(serde_json::json!({"registered_revision":record.revision.get()}))
}

fn binding_error(error: McpHostBindingError) -> &'static str {
    match error {
        McpHostBindingError::ActiveGeneration => {
            "stop and reap the scoped generation before changing host bindings"
        }
        McpHostBindingError::Conflict => {
            "host revision changed or receipt conflicts; inspect before updating"
        }
        McpHostBindingError::LimitExceeded => "host snapshot exceeds supplied byte budget",
        McpHostBindingError::DefinitionChanged => "MCP definition version changed",
        McpHostBindingError::NotFound => "host snapshot not found",
        McpHostBindingError::InvalidRequest | McpHostBindingError::InvalidBinding => {
            "invalid host snapshot or secret bindings"
        }
        McpHostBindingError::IntegrityViolation => "stored host snapshot could not be verified",
        McpHostBindingError::Unavailable => {
            "host store unavailable; inspect or retry the same revision and input"
        }
    }
}
fn journal_error(_: McpSecretJournalError) -> &'static str {
    "secret reservation unavailable, invalid or conflicting"
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };

    #[derive(Default)]
    struct Vault {
        values: Mutex<BTreeMap<String, Vec<u8>>>,
        calls: Mutex<Vec<&'static str>>,
        fail_write: AtomicBool,
        fail_delete: AtomicBool,
    }
    impl McpSecretStore for Vault {
        async fn put_at(
            &self,
            binding: &McpSecretBinding,
            value: SecretValue,
        ) -> Result<(), SecretStoreError> {
            self.calls.lock().unwrap().push("put");
            self.values.lock().unwrap().insert(
                binding.secret_ref().as_str().into(),
                value.as_bytes().to_vec(),
            );
            if self.fail_write.load(Ordering::SeqCst) {
                Err(SecretStoreError::Unavailable)
            } else {
                Ok(())
            }
        }
        async fn get(&self, binding: &McpSecretBinding) -> Result<SecretValue, SecretStoreError> {
            self.calls.lock().unwrap().push("get");
            SecretValue::new(
                self.values
                    .lock()
                    .unwrap()
                    .get(binding.secret_ref().as_str())
                    .ok_or(SecretStoreError::NotFound)?
                    .clone(),
            )
            .map_err(|_| SecretStoreError::InvalidSecret)
        }
        async fn delete(&self, binding: &McpSecretBinding) -> Result<(), SecretStoreError> {
            self.calls.lock().unwrap().push("delete");
            if self.fail_delete.load(Ordering::SeqCst) {
                return Err(SecretStoreError::Unavailable);
            }
            self.values
                .lock()
                .unwrap()
                .remove(binding.secret_ref().as_str());
            Ok(())
        }
    }
    fn request(key: &McpInstanceKey, action: serde_json::Value) -> Request {
        let mut value = serde_json::json!({"key":serde_json::from_str::<serde_json::Value>(key.canonical_json()).unwrap(), "action":action});
        value.sort_all_objects();
        parse(value.to_string().as_bytes(), 4096).unwrap()
    }

    #[tokio::test]
    async fn http_publication_preflights_before_vault_and_retries_without_reads() {
        let data = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(data.path()).await.unwrap();
        let name = |s| SharedConfigurationKey::parse(s, 64).unwrap();
        let definition = McpServerDefinition::new(
            SharedMcpServerInput {
                id: name("http"),
                enabled: true,
                transport: SharedMcpTransport::Https {
                    endpoint: "https://example.com/mcp".into(),
                    credential_binding: Some(name("token")),
                },
            },
            McpProtocolPolicy::Auto,
            McpLifecycleScope::Core,
            None,
            limits(4096),
        )
        .unwrap();
        store
            .register_mcp_definition(&definition, 0, "http", limits(4096))
            .await
            .unwrap();
        let key = McpInstanceKey::new(&definition, McpInstanceOwner::Core, 4096).unwrap();
        let vault = Vault::default();
        let output = execute(&store, &vault, request(&key, serde_json::json!({"operation":"import_secret", "definition_version":1, "name":"token", "purpose":"http_credential", "value":"private fixture"})), 4096).await.unwrap();
        let publish = serde_json::json!({"operation":"publish_http", "expected_revision":0, "definition_version":1, "endpoint":"https://example.com/mcp", "endpoint_binding":null, "credential":["token",output["secret_ref"]]});
        let mut invalid = publish.clone();
        invalid["endpoint"] = "https://other.example.com/mcp".into();
        assert!(
            execute(&store, &vault, request(&key, invalid), 4096)
                .await
                .is_err()
        );
        assert_eq!(*vault.calls.lock().unwrap(), ["put"]);
        for _ in 0..2 {
            assert_eq!(
                execute(&store, &vault, request(&key, publish.clone()), 4096)
                    .await
                    .unwrap()["registered_revision"],
                1
            );
        }
        assert_eq!(*vault.calls.lock().unwrap(), ["put", "get"]);
        execute(
            &store,
            &vault,
            request(
                &key,
                serde_json::json!({"operation":"retire", "expected_revision":1}),
            ),
            4096,
        )
        .await
        .unwrap();
        assert_eq!(
            execute(
                &store,
                &vault,
                request(
                    &key,
                    serde_json::json!({"operation":"reconcile", "batch_size":1})
                ),
                4096
            )
            .await
            .unwrap()["reconciled"],
            1
        );
        let calls = vault.calls.lock().unwrap().len();
        assert_eq!(
            execute(&store, &vault, request(&key, publish), 4096)
                .await
                .unwrap()["registered_revision"],
            1
        );
        assert_eq!(vault.calls.lock().unwrap().len(), calls);
    }

    #[tokio::test]
    async fn host_admin_publication_retries_and_ambiguous_vault_cleanup() {
        let data = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(data.path()).await.unwrap();
        let name = |s| SharedConfigurationKey::parse(s, 64).unwrap();
        let definition = McpServerDefinition::new(
            SharedMcpServerInput {
                id: name("fixture"),
                enabled: true,
                transport: SharedMcpTransport::Stdio {
                    runtime_binding: name("runtime"),
                    arguments: vec![],
                    environment: BTreeMap::from([("TOKEN".into(), name("token"))]),
                },
            },
            McpProtocolPolicy::Auto,
            McpLifecycleScope::Core,
            None,
            limits(4096),
        )
        .unwrap();
        store
            .register_mcp_definition(&definition, 0, "fixture", limits(4096))
            .await
            .unwrap();
        let key = McpInstanceKey::new(&definition, McpInstanceOwner::Core, 4096).unwrap();
        let vault = Vault::default();
        let import = serde_json::json!({"operation":"import_secret", "definition_version":1, "name":"token", "purpose":"environment", "value":"private fixture"});
        let output = execute(&store, &vault, request(&key, import.clone()), 4096)
            .await
            .unwrap();
        assert!(!output.to_string().contains("private fixture"));
        let reference = output["secret_ref"].as_str().unwrap();
        let publish = serde_json::json!({"operation":"publish", "expected_revision":0, "definition_version":1,
            "runtime_binding":"runtime", "executable":"/bin/sh", "arguments":{}, "environment":{"token":reference}});
        let mut invalid = publish.clone();
        invalid["runtime_binding"] = "wrong".into();
        assert!(
            execute(&store, &vault, request(&key, invalid), 4096)
                .await
                .is_err()
        );
        assert_eq!(*vault.calls.lock().unwrap(), ["put"]);
        let mut unknown_directory = publish.clone();
        unknown_directory["working_directory"] = serde_json::json!({
            "workspace_id": WorkspaceId::from_ulid(ulid::Ulid::generate()).as_str(),
            "workspace_root_id": WorkspaceRootId::from_ulid(ulid::Ulid::generate()).as_str(),
            "relative_directory": "",
            "root_path": "/fixture",
            "git_common_directory_path": "/fixture/.git",
            "filesystem_identity": "unix:1:2",
        });
        assert!(
            execute(&store, &vault, request(&key, unknown_directory), 4096)
                .await
                .is_err()
        );
        assert_eq!(*vault.calls.lock().unwrap(), ["put"]);
        assert_eq!(
            execute(&store, &vault, request(&key, publish.clone()), 4096)
                .await
                .unwrap()["registered_revision"],
            1
        );
        assert_eq!(
            execute(&store, &vault, request(&key, publish.clone()), 4096)
                .await
                .unwrap()["registered_revision"],
            1
        );
        assert_eq!(*vault.calls.lock().unwrap(), ["put", "get"]);
        let pending = serde_json::json!({"operation":"pending", "batch_size":1});
        assert!(
            execute(&store, &vault, request(&key, pending.clone()), 4096)
                .await
                .unwrap()["pending"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        execute(
            &store,
            &vault,
            request(
                &key,
                serde_json::json!({"operation":"retire", "expected_revision":1}),
            ),
            4096,
        )
        .await
        .unwrap();
        let reconcile = serde_json::json!({"operation":"reconcile", "batch_size":1});
        vault.fail_delete.store(true, Ordering::SeqCst);
        assert!(
            execute(&store, &vault, request(&key, reconcile.clone()), 4096)
                .await
                .is_err()
        );
        assert_eq!(
            execute(&store, &vault, request(&key, pending.clone()), 4096)
                .await
                .unwrap()["pending"][0]["state"],
            "retired"
        );
        vault.fail_delete.store(false, Ordering::SeqCst);
        assert_eq!(
            execute(&store, &vault, request(&key, reconcile.clone()), 4096)
                .await
                .unwrap()["reconciled"],
            1
        );
        // Old publication receipt remains accessible after vault deletion, with
        // no read of retired credentials and no restoration of the snapshot.
        let calls = vault.calls.lock().unwrap().len();
        assert_eq!(
            execute(&store, &vault, request(&key, publish), 4096)
                .await
                .unwrap()["registered_revision"],
            1
        );
        assert_eq!(vault.calls.lock().unwrap().len(), calls);
        assert!(
            execute(
                &store,
                &vault,
                request(&key, serde_json::json!({"operation":"inspect"})),
                4096
            )
            .await
            .unwrap()["snapshot"]["retired"]
                .as_bool()
                .unwrap()
        );
        vault.fail_write.store(true, Ordering::SeqCst);
        assert!(
            execute(&store, &vault, request(&key, import), 4096)
                .await
                .is_err()
        );
        assert_eq!(
            execute(&store, &vault, request(&key, pending), 4096)
                .await
                .unwrap()["pending"][0]["state"],
            "reserved"
        );
        assert_eq!(
            execute(&store, &vault, request(&key, reconcile), 4096)
                .await
                .unwrap()["reconciled"],
            1
        );
        assert!(vault.values.lock().unwrap().is_empty());
    }

    #[test]
    fn host_admin_parser_rejects_noncanonical_or_ambiguous_inputs() {
        let valid = br#"{"action":{"operation":"inspect"},"key":{"auth_profile":null,"definition_id":"fixture","owner":{"kind":"core"}}}"#;
        assert!(parse(valid, valid.len()).is_ok());
        assert!(parse(valid, valid.len() - 1).is_err());
        for invalid in [
            String::from_utf8(valid.to_vec())
                .unwrap()
                .replace("\"kind\":\"core\"", "\"kind\":\"core\",\"kind\":\"core\""),
            String::from_utf8(valid.to_vec()).unwrap().replace(
                "\"operation\":\"inspect\"",
                "\"operation\":\"inspect\",\"extra\":true",
            ),
            format!(" {}", String::from_utf8(valid.to_vec()).unwrap()),
        ] {
            assert!(parse(invalid.as_bytes(), 4096).is_err());
        }
    }
}
