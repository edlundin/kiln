use kiln_core::{
    ConfigurationSecretBinding, ConfigurationSecretPurpose, ConfigurationSecretStore,
    McpSecretBinding, McpSecretStore, ProviderAccountId, ProviderType, SecretRef, SecretStore,
    SecretStoreError, SecretValue,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
#[cfg(target_os = "linux")]
use tokio::{io::AsyncWriteExt, process::Command};

#[cfg(target_os = "macos")]
use tokio::task::spawn_blocking;

const DEFAULT_SERVICE: &str = "dev.kiln.provider-account";
const CONFIGURATION_SERVICE: &str = "dev.kiln.configuration-sync";
const MCP_SERVICE: &str = "dev.kiln.mcp";

#[cfg(test)]
mod mcp_namespace_tests {
    use super::*;
    use kiln_core::*;
    use std::collections::{BTreeMap, HashSet};

    #[test]
    fn mcp_vault_identity_isolates_every_lookup_dimension_and_service() {
        let instance = KilnInstanceId::from_ulid(ulid::Ulid::generate());
        let secret_ref = SecretRef::from_ulid(ulid::Ulid::generate());
        let name = SharedConfigurationKey::parse("private-binding", 64).unwrap();
        let session = SessionId::from_ulid(ulid::Ulid::generate());
        let make_key = |server: &str, profile: &str, session: SessionId| {
            let definition = McpServerDefinition::new(
                SharedMcpServerInput {
                    id: SharedConfigurationKey::parse(server, 64).unwrap(),
                    enabled: true,
                    transport: SharedMcpTransport::Stdio {
                        runtime_binding: name.clone(),
                        arguments: vec![],
                        environment: BTreeMap::new(),
                    },
                },
                McpProtocolPolicy::Auto,
                McpLifecycleScope::Session,
                Some(SharedConfigurationKey::parse(profile, 64).unwrap()),
                McpDefinitionLimits {
                    max_key_bytes: 64,
                    max_metadata_bytes: 4096,
                    max_arguments: 1,
                    max_argument_bytes: 64,
                    max_environment: 1,
                    max_endpoint_bytes: 128,
                },
            )
            .unwrap();
            McpInstanceKey::new(&definition, McpInstanceOwner::Session(session), 4096).unwrap()
        };
        let key = make_key("server", "profile", session.clone());
        let bindings = [
            McpSecretBinding::new(
                instance.clone(),
                key.clone(),
                name.clone(),
                McpSecretPurpose::Argument,
                secret_ref.clone(),
            ),
            McpSecretBinding::new(
                KilnInstanceId::from_ulid(ulid::Ulid::generate()),
                key.clone(),
                name.clone(),
                McpSecretPurpose::Argument,
                secret_ref.clone(),
            ),
            McpSecretBinding::new(
                instance.clone(),
                make_key("other-server", "profile", session.clone()),
                name.clone(),
                McpSecretPurpose::Argument,
                secret_ref.clone(),
            ),
            McpSecretBinding::new(
                instance.clone(),
                make_key("server", "other-profile", session.clone()),
                name.clone(),
                McpSecretPurpose::Argument,
                secret_ref.clone(),
            ),
            McpSecretBinding::new(
                instance.clone(),
                make_key(
                    "server",
                    "profile",
                    SessionId::from_ulid(ulid::Ulid::generate()),
                ),
                name.clone(),
                McpSecretPurpose::Argument,
                secret_ref.clone(),
            ),
            McpSecretBinding::new(
                instance.clone(),
                key.clone(),
                SharedConfigurationKey::parse("other-binding", 64).unwrap(),
                McpSecretPurpose::Argument,
                secret_ref.clone(),
            ),
            McpSecretBinding::new(
                instance.clone(),
                key.clone(),
                name.clone(),
                McpSecretPurpose::Environment,
                secret_ref.clone(),
            ),
            McpSecretBinding::new(
                instance,
                key,
                name,
                McpSecretPurpose::Argument,
                SecretRef::from_ulid(ulid::Ulid::generate()),
            ),
        ];
        let keys = bindings
            .iter()
            .map(OsMcpSecretStore::key)
            .collect::<HashSet<_>>();
        assert_eq!(keys.len(), bindings.len());
        assert_eq!(
            OsMcpSecretStore::key(&bindings[0]),
            OsMcpSecretStore::key(&bindings[0].clone())
        );
        assert!(
            keys.iter()
                .all(|key| key.len() == 64 && key.bytes().all(|b| b.is_ascii_hexdigit()))
        );
        assert_ne!(MCP_SERVICE, DEFAULT_SERVICE);
        assert_ne!(MCP_SERVICE, CONFIGURATION_SERVICE);
        assert_eq!(OsMcpSecretStore::open_default().0.service, MCP_SERVICE);
    }
}

/// Host-local MCP values. Provider and configuration-sync credentials cannot be
/// addressed through this service. Construction performs no vault operation.
#[derive(Clone)]
pub struct OsMcpSecretStore(OsSecretStore);
impl OsMcpSecretStore {
    pub fn open_default() -> Self {
        Self(OsSecretStore {
            service: MCP_SERVICE.to_owned(),
            entry_locks: Arc::new(Mutex::new(HashMap::new())),
        })
    }
    fn key(binding: &McpSecretBinding) -> String {
        use sha2::{Digest, Sha256};
        // Length framing avoids delimiter ambiguity. Hashing also keeps scoped
        // checkout paths and binding names out of OS-vault account metadata.
        let mut hash = Sha256::new();
        hash.update(b"kiln.mcp-vault.v1");
        for field in [
            binding.instance_id().as_str(),
            binding.key().canonical_json(),
            binding.name().as_str(),
            binding.purpose().as_str(),
            binding.secret_ref().as_str(),
        ] {
            hash.update((field.len() as u64).to_be_bytes());
            hash.update(field.as_bytes());
        }
        let mut encoded = String::with_capacity(64);
        for byte in hash.finalize() {
            encoded.push(b"0123456789abcdef"[(byte >> 4) as usize] as char);
            encoded.push(b"0123456789abcdef"[(byte & 0x0f) as usize] as char);
        }
        encoded
    }
}
impl Default for OsMcpSecretStore {
    fn default() -> Self {
        Self::open_default()
    }
}
impl McpSecretStore for OsMcpSecretStore {
    async fn put_at(
        &self,
        binding: &McpSecretBinding,
        value: SecretValue,
    ) -> Result<(), SecretStoreError> {
        self.0.put_key(Self::key(binding), value).await
    }
    async fn get(&self, binding: &McpSecretBinding) -> Result<SecretValue, SecretStoreError> {
        SecretValue::new(self.0.read_value(&Self::key(binding)).await?)
            .map_err(|_| SecretStoreError::InvalidSecret)
    }
    async fn delete(&self, binding: &McpSecretBinding) -> Result<(), SecretStoreError> {
        self.0.delete_key(Self::key(binding)).await
    }
}

/// Host-local configuration credentials in a distinct OS vault service. Clones
/// share per-entry write/delete locks; construction performs no vault operation.
#[derive(Clone)]
pub struct OsConfigurationSecretStore(OsSecretStore);

impl OsConfigurationSecretStore {
    pub fn open_default() -> Self {
        Self(OsSecretStore {
            service: CONFIGURATION_SERVICE.to_owned(),
            entry_locks: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn key(binding: &ConfigurationSecretBinding) -> String {
        let purpose = match binding.purpose() {
            ConfigurationSecretPurpose::MasterCertificateAuthority => "master-ca",
            ConfigurationSecretPurpose::MasterTlsIdentity => "master-tls",
            ConfigurationSecretPurpose::FollowerReadCredential => "follower-read",
        };
        format!(
            "{}:{}:{}:{purpose}:{}",
            binding.instance_id().as_str(),
            binding.authority().group_id().as_str(),
            binding.authority().master_id().as_str(),
            binding.secret_ref().as_str()
        )
    }
}

impl Default for OsConfigurationSecretStore {
    fn default() -> Self {
        Self::open_default()
    }
}

impl ConfigurationSecretStore for OsConfigurationSecretStore {
    async fn put_at(
        &self,
        binding: &ConfigurationSecretBinding,
        value: SecretValue,
    ) -> Result<(), SecretStoreError> {
        self.0.put_key(Self::key(binding), value).await
    }

    async fn get(
        &self,
        binding: &ConfigurationSecretBinding,
    ) -> Result<SecretValue, SecretStoreError> {
        let bytes = self.0.read_value(&Self::key(binding)).await?;
        SecretValue::new(bytes).map_err(|_| SecretStoreError::InvalidSecret)
    }

    async fn delete(&self, binding: &ConfigurationSecretBinding) -> Result<(), SecretStoreError> {
        self.0.delete_key(Self::key(binding)).await
    }
}

/// OS-backed storage for provider credentials. The database receives only the
/// generated SecretRef; the secret value is passed directly to the native
/// Keychain API on macOS or over stdin to Secret Service on Linux.
#[derive(Clone)]
pub struct OsSecretStore {
    service: String,
    entry_locks: Arc<Mutex<HashMap<String, Weak<AsyncMutex<()>>>>>,
}

impl std::fmt::Debug for OsSecretStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OsSecretStore")
            .field("service", &self.service)
            .finish_non_exhaustive()
    }
}

impl OsSecretStore {
    pub fn open_default() -> Self {
        Self {
            service: DEFAULT_SERVICE.to_owned(),
            entry_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn key(
        &self,
        provider_type: &ProviderType,
        account_id: &ProviderAccountId,
        secret_ref: &SecretRef,
    ) -> String {
        format!(
            "{}:{}:{}",
            provider_type.as_str(),
            account_id.as_str(),
            secret_ref.as_str()
        )
    }

    async fn entry_lock(&self, key: &str) -> OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self
                .entry_locks
                .lock()
                .expect("vault entry locks are not poisoned");
            locks.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = locks.get(key).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(AsyncMutex::new(()));
                locks.insert(key.to_owned(), Arc::downgrade(&lock));
                lock
            }
        };
        lock.lock_owned().await
    }
}

impl Default for OsSecretStore {
    fn default() -> Self {
        Self::open_default()
    }
}

impl SecretStore for OsSecretStore {
    async fn put_at(
        &self,
        provider_type: &ProviderType,
        account_id: &ProviderAccountId,
        secret_ref: &SecretRef,
        value: SecretValue,
    ) -> Result<(), SecretStoreError> {
        let key = self.key(provider_type, account_id, secret_ref);
        self.put_key(key, value).await
    }

    async fn get(
        &self,
        provider_type: &ProviderType,
        account_id: &ProviderAccountId,
        secret_ref: &SecretRef,
    ) -> Result<SecretValue, SecretStoreError> {
        let key = self.key(provider_type, account_id, secret_ref);
        let bytes = self.read_value(&key).await?;
        SecretValue::new(bytes).map_err(|_| SecretStoreError::InvalidSecret)
    }

    async fn delete(
        &self,
        provider_type: &ProviderType,
        account_id: &ProviderAccountId,
        secret_ref: &SecretRef,
    ) -> Result<(), SecretStoreError> {
        self.delete_key(self.key(provider_type, account_id, secret_ref))
            .await
    }
}

impl OsSecretStore {
    async fn put_key(&self, key: String, value: SecretValue) -> Result<(), SecretStoreError> {
        let guard = self.entry_lock(&key).await;
        let store = self.clone();
        // Keep the lock until the OS effect finishes even if the caller drops
        // its future. Journal recovery must not delete before a late write.
        tokio::spawn(async move {
            let _guard = guard;
            #[cfg(target_os = "macos")]
            {
                store.store_keychain(&key, value).await
            }
            #[cfg(not(target_os = "macos"))]
            {
                store.store_value(&key, value.as_bytes()).await
            }
        })
        .await
        .map_err(|_| SecretStoreError::Unavailable)?
    }

    async fn delete_key(&self, key: String) -> Result<(), SecretStoreError> {
        let guard = self.entry_lock(&key).await;
        let store = self.clone();
        tokio::spawn(async move {
            let _guard = guard;
            store.delete_value(&key).await
        })
        .await
        .map_err(|_| SecretStoreError::Unavailable)?
    }
}

impl OsSecretStore {
    #[cfg(target_os = "macos")]
    async fn store_keychain(&self, key: &str, value: SecretValue) -> Result<(), SecretStoreError> {
        let service = self.service.clone();
        let key = key.to_owned();
        spawn_blocking(move || {
            security_framework::passwords::set_generic_password(&service, &key, value.as_bytes())
                .map_err(map_keychain_error)
        })
        .await
        .map_err(|_| SecretStoreError::Unavailable)?
    }

    #[cfg(target_os = "linux")]
    async fn store_value(&self, key: &str, value: &[u8]) -> Result<(), SecretStoreError> {
        #[cfg(target_os = "linux")]
        {
            let output = run_command(
                "secret-tool",
                &[
                    "store",
                    "--label",
                    &self.service,
                    "service",
                    &self.service,
                    "account",
                    key,
                ],
                Some(value),
            )
            .await?;
            return command_result(output, false);
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (key, value);
            Err(SecretStoreError::Unsupported)
        }
    }

    async fn read_value(&self, key: &str) -> Result<Vec<u8>, SecretStoreError> {
        #[cfg(target_os = "macos")]
        {
            let service = self.service.clone();
            let key = key.to_owned();
            return spawn_blocking(move || {
                security_framework::passwords::get_generic_password(&service, &key)
                    .map_err(map_keychain_error)
            })
            .await
            .map_err(|_| SecretStoreError::Unavailable)?;
        }
        #[cfg(target_os = "linux")]
        {
            let output = run_command(
                "secret-tool",
                &["lookup", "service", &self.service, "account", key],
                None,
            )
            .await?;
            return command_output(output, true);
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            let _ = key;
            Err(SecretStoreError::Unsupported)
        }
    }

    async fn delete_value(&self, key: &str) -> Result<(), SecretStoreError> {
        #[cfg(target_os = "macos")]
        {
            let service = self.service.clone();
            let key = key.to_owned();
            return spawn_blocking(move || {
                security_framework::passwords::delete_generic_password(&service, &key)
                    .map_err(map_keychain_error)
            })
            .await
            .map_err(|_| SecretStoreError::Unavailable)?;
        }
        #[cfg(target_os = "linux")]
        {
            let output = run_command(
                "secret-tool",
                &["clear", "service", &self.service, "account", key],
                None,
            )
            .await?;
            return command_result(output, true);
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            let _ = key;
            Err(SecretStoreError::Unsupported)
        }
    }
}

#[cfg(target_os = "macos")]
fn map_keychain_error(error: security_framework::base::Error) -> SecretStoreError {
    if error.code() == -25300 {
        SecretStoreError::NotFound
    } else {
        SecretStoreError::Unavailable
    }
}

#[cfg(target_os = "linux")]
async fn run_command(
    program: &str,
    args: &[&str],
    input: Option<&[u8]>,
) -> Result<std::process::Output, SecretStoreError> {
    let mut command = Command::new(program);
    command
        .args(args)
        .kill_on_drop(true)
        .stdin(if input.is_some() {
            std::process::Stdio::piped()
        } else {
            std::process::Stdio::null()
        })
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    command.env_clear();
    for variable in [
        "DBUS_SESSION_BUS_ADDRESS",
        "DISPLAY",
        "HOME",
        "WAYLAND_DISPLAY",
        "XDG_RUNTIME_DIR",
    ] {
        if let Some(value) = std::env::var_os(variable) {
            command.env(variable, value);
        }
    }
    let mut child = command.spawn().map_err(|error| match error.kind() {
        std::io::ErrorKind::NotFound => SecretStoreError::Unsupported,
        _ => SecretStoreError::Unavailable,
    })?;
    if let Some(input) = input {
        let mut stdin = child.stdin.take().ok_or(SecretStoreError::Unavailable)?;
        stdin
            .write_all(input)
            .await
            .map_err(|_| SecretStoreError::Unavailable)?;
        stdin
            .write_all(b"\n")
            .await
            .map_err(|_| SecretStoreError::Unavailable)?;
        stdin
            .shutdown()
            .await
            .map_err(|_| SecretStoreError::Unavailable)?;
    }
    child
        .wait_with_output()
        .await
        .map_err(|_| SecretStoreError::Unavailable)
}

#[cfg(target_os = "linux")]
fn command_result(
    output: std::process::Output,
    missing_is_not_found: bool,
) -> Result<(), SecretStoreError> {
    if output.status.success() {
        return Ok(());
    }
    if missing_is_not_found && output_indicates_missing(&output) {
        return Err(SecretStoreError::NotFound);
    }
    Err(SecretStoreError::Unavailable)
}

#[cfg(target_os = "linux")]
fn command_output(
    output: std::process::Output,
    missing_is_not_found: bool,
) -> Result<Vec<u8>, SecretStoreError> {
    if !output.status.success() {
        if missing_is_not_found && output_indicates_missing(&output) {
            return Err(SecretStoreError::NotFound);
        }
        return Err(SecretStoreError::Unavailable);
    }
    let mut value = output.stdout;
    if value.last() == Some(&b'\n') {
        value.pop();
        if value.last() == Some(&b'\r') {
            value.pop();
        }
    }
    Ok(value)
}

#[cfg(target_os = "linux")]
fn output_indicates_missing(output: &std::process::Output) -> bool {
    let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
    stderr.contains("not found")
        || stderr.contains("could not be found")
        || stderr.contains("no such item")
}
