use kiln_core::{
    ProviderAccountId, ProviderType, SecretRef, SecretStore, SecretStoreError, SecretValue,
};
#[cfg(target_os = "linux")]
use tokio::{io::AsyncWriteExt, process::Command};
use ulid::Ulid;

#[cfg(target_os = "macos")]
use tokio::task::spawn_blocking;

const DEFAULT_SERVICE: &str = "dev.kiln.provider-account";

/// OS-backed storage for provider credentials. The database receives only the
/// generated SecretRef; the secret value is passed directly to the native
/// Keychain API on macOS or over stdin to Secret Service on Linux.
#[derive(Debug, Clone)]
pub struct OsSecretStore {
    service: String,
}

impl OsSecretStore {
    pub fn open_default() -> Self {
        Self {
            service: DEFAULT_SERVICE.to_owned(),
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
}

impl Default for OsSecretStore {
    fn default() -> Self {
        Self::open_default()
    }
}

impl SecretStore for OsSecretStore {
    async fn put(
        &self,
        provider_type: &ProviderType,
        account_id: &ProviderAccountId,
        value: SecretValue,
    ) -> Result<SecretRef, SecretStoreError> {
        let secret_ref = SecretRef::from_ulid(Ulid::generate());
        let key = self.key(provider_type, account_id, &secret_ref);
        #[cfg(target_os = "macos")]
        let result = self.store_keychain(&key, value).await;
        #[cfg(not(target_os = "macos"))]
        let result = {
            let result = self.store_value(&key, value.as_bytes()).await;
            drop(value);
            result
        };
        result.map(|()| secret_ref)
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
        let key = self.key(provider_type, account_id, secret_ref);
        self.delete_value(&key).await
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
