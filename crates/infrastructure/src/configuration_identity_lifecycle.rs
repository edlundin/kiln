//! Managed identity persistence and OS-vault effects. Local authentication and
//! explicit setup approval belong to the caller; this adapter opens no listener.

use super::{
    ConfigurationCertificateValidity, ConfigurationIdentityError, ConfigurationPrivateKey,
    OsConfigurationSecretStore, SqliteStore, configuration_identity::valid_server_name,
    configuration_sync, decode_configuration_private_key, generate_configuration_identity,
    hash_bytes,
};
use kiln_core::{
    ConfigurationAuthority, ConfigurationGroupId, ConfigurationInstanceState,
    ConfigurationMasterIdentityId, ConfigurationRole, ConfigurationSecretBinding,
    ConfigurationSecretPurpose, ConfigurationSecretStore, ContentHash, KilnInstanceId, SecretRef,
    SecretStoreError,
};
use sqlx::{Connection, Row, SqliteConnection};

impl kiln_core::ConfigurationIdentityStatusStore for SqliteStore {
    async fn get_configuration_identity_status(
        &self,
    ) -> Result<kiln_core::ConfigurationMasterIdentityStatus, kiln_core::ConfigurationStateError>
    {
        use kiln_core::{
            ConfigurationMasterIdentityPhase as Phase, ConfigurationStateError as StateError,
        };
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| StateError::Unavailable)?;
        let state = configuration_sync::load(&mut transaction)
            .await?
            .ok_or(StateError::Uninitialized)?;
        let identity = if let ConfigurationRole::Master(authority) = state.role() {
            let reference: Option<String> = sqlx::query_scalar("SELECT ca_ref FROM configuration_master_identities WHERE group_id = ? AND status != 'retired'")
                .bind(authority.group_id().as_str()).fetch_optional(&mut *transaction).await.map_err(|_| StateError::Unavailable)?;
            if let Some(reference) = reference {
                let reference =
                    SecretRef::parse(reference).map_err(|_| StateError::IntegrityViolation)?;
                let stored = load(&mut transaction, &reference)
                    .await
                    .map_err(|error| match error {
                        Error::Unavailable => StateError::Unavailable,
                        _ => StateError::IntegrityViolation,
                    })?
                    .ok_or(StateError::IntegrityViolation)?;
                let record = stored.record;
                if !is_master(&state, &record.request) {
                    return Err(StateError::IntegrityViolation);
                }
                Some(kiln_core::ConfigurationMasterIdentitySummary {
                    identity_id: record.identity_id.clone(),
                    phase: match record.status {
                        ConfigurationIdentityStatus::Pending => Phase::Pending,
                        ConfigurationIdentityStatus::Active => Phase::Active,
                        ConfigurationIdentityStatus::Retired => {
                            return Err(StateError::IntegrityViolation);
                        }
                    },
                    server_name: record.request.server_name,
                    certificate_authority_fingerprint: record.certificate_authority_fingerprint,
                    not_before_unix_seconds: record.request.validity.not_before,
                    leaf_not_after_unix_seconds: record.request.validity.leaf_not_after,
                    ca_not_after_unix_seconds: record.request.validity.ca_not_after,
                })
            } else {
                None
            }
        } else {
            None
        };
        transaction
            .commit()
            .await
            .map_err(|_| StateError::Unavailable)?;
        Ok(kiln_core::ConfigurationMasterIdentityStatus { state, identity })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationIdentityLifecycleError {
    InvalidRequest,
    Conflict,
    ReferenceConflict,
    Retired,
    RecoveryRequired,
    IntegrityViolation,
    Identity(ConfigurationIdentityError),
    Vault(SecretStoreError),
    Unavailable,
}
use ConfigurationIdentityLifecycleError as Error;

/// Application-facing command adapter with an explicit wall clock. Constructing
/// it performs no vault effects. Retain command ownership while awaiting it.
#[derive(Clone)]
pub struct ConfigurationIdentityCommands {
    provisioner: ConfigurationIdentityProvisioner,
    clock: std::sync::Arc<dyn Fn() -> i64 + Send + Sync>,
}
impl ConfigurationIdentityCommands {
    pub fn new(
        provisioner: ConfigurationIdentityProvisioner,
        clock: impl Fn() -> i64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            provisioner,
            clock: std::sync::Arc::new(clock),
        }
    }
}
impl kiln_core::ConfigurationIdentityAdministration for ConfigurationIdentityCommands {
    async fn configure_master_identity(
        &self,
        command: kiln_core::ConfigureMasterIdentity,
        idempotency_key: String,
    ) -> Result<
        kiln_core::ConfigurationIdentitySetupReceipt,
        kiln_core::ConfigurationIdentityCommandError,
    > {
        use kiln_core::ConfigurationIdentityCommandError as CommandError;
        if idempotency_key.is_empty()
            || !idempotency_key.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(CommandError::InvalidRequest);
        }
        let owner = self.clone();
        tokio::spawn(async move {
            let _guard = owner
                .provisioner
                .store
                .configuration_identity_operations
                .lock()
                .await;
            let existing = {
                let mut connection = owner.provisioner.store.connection.lock().await;
                let reference: Option<String> = sqlx::query_scalar(
                    "SELECT ca_ref FROM configuration_master_identities WHERE request_key = ?",
                )
                .bind(&idempotency_key)
                .fetch_optional(&mut *connection)
                .await
                .map_err(|_| CommandError::Unavailable)?;
                if let Some(reference) = reference {
                    let reference =
                        SecretRef::parse(reference).map_err(|_| CommandError::Unavailable)?;
                    Some(
                        load(&mut connection, &reference)
                            .await
                            .map_err(command_error)?
                            .ok_or(CommandError::Unavailable)?,
                    )
                } else {
                    None
                }
            };
            let request = if let Some(stored) = existing {
                let request = stored.record.request;
                if request.ca_binding.instance_id() != &command.expected_instance_id
                    || request.ca_binding.authority().group_id() != &command.expected_group_id
                    || request.expected_version != command.expected_state_version
                    || request.server_name != command.server_name
                    || request.validity != command.validity
                {
                    return Err(CommandError::IdempotencyConflict);
                }
                request
            } else {
                ConfigurationIdentityRequest::from_parts(
                    ConfigurationAuthority::new(
                        command.expected_group_id,
                        command.expected_instance_id,
                    ),
                    command.expected_state_version,
                    SecretRef::from_ulid(ulid::Ulid::generate()),
                    SecretRef::from_ulid(ulid::Ulid::generate()),
                    command.server_name,
                    command.validity,
                )
                .map_err(command_error)?
            };
            let record = owner
                .provisioner
                .provision_owned(request, &*owner.clock, Some(&idempotency_key))
                .await
                .map_err(command_error)?;
            Ok(kiln_core::ConfigurationIdentitySetupReceipt {
                instance_id: record.request.ca_binding.instance_id().clone(),
                group_id: record.request.ca_binding.authority().group_id().clone(),
                reserved_state_version: record.request.expected_version,
                identity_id: record.identity_id,
            })
        })
        .await
        .map_err(|_| CommandError::Unavailable)?
    }

    async fn retire_master_identity(
        &self,
        expected_instance_id: KilnInstanceId,
        setup_idempotency_key: String,
    ) -> Result<(), kiln_core::ConfigurationIdentityCommandError> {
        use kiln_core::ConfigurationIdentityCommandError as CommandError;
        if setup_idempotency_key.is_empty()
            || !setup_idempotency_key
                .bytes()
                .all(|byte| byte.is_ascii_graphic())
        {
            return Err(CommandError::InvalidRequest);
        }
        let identity_id = {
            let mut connection = self.provisioner.store.connection.lock().await;
            let reference: Option<String> = sqlx::query_scalar(
                "SELECT ca_ref FROM configuration_master_identities WHERE request_key = ?",
            )
            .bind(&setup_idempotency_key)
            .fetch_optional(&mut *connection)
            .await
            .map_err(|_| CommandError::Unavailable)?;
            SecretRef::parse(reference.ok_or(CommandError::Conflict)?)
                .map_err(|_| CommandError::Unavailable)?
        };
        let record = self
            .provisioner
            .get(&identity_id)
            .await
            .map_err(command_error)?
            .ok_or(CommandError::Conflict)?;
        if record.request.ca_binding.instance_id() != &expected_instance_id {
            return Err(CommandError::Conflict);
        }
        self.provisioner
            .retire_and_cleanup(identity_id)
            .await
            .map_err(command_error)
    }

    async fn retire_master_identity_by_id(
        &self,
        expected_instance_id: KilnInstanceId,
        identity_id: ConfigurationMasterIdentityId,
    ) -> Result<(), kiln_core::ConfigurationIdentityCommandError> {
        self.provisioner
            .retire_and_cleanup_by_identity_id(identity_id, expected_instance_id)
            .await
            .map_err(command_error)
    }
}

fn command_error(error: Error) -> kiln_core::ConfigurationIdentityCommandError {
    use kiln_core::ConfigurationIdentityCommandError as CommandError;
    match error {
        Error::InvalidRequest
        | Error::Identity(
            ConfigurationIdentityError::InvalidBinding
            | ConfigurationIdentityError::InvalidServerName
            | ConfigurationIdentityError::InvalidValidity,
        ) => CommandError::InvalidRequest,
        Error::Conflict | Error::ReferenceConflict | Error::Retired => CommandError::Conflict,
        Error::RecoveryRequired => CommandError::RecoveryRequired,
        _ => CommandError::Unavailable,
    }
}

/// Immutable setup command. Keep this exact request for an ambiguous retry.
/// The CA reference is its durable idempotency identity; both refs must be fresh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationIdentityRequest {
    ca_binding: ConfigurationSecretBinding,
    tls_binding: ConfigurationSecretBinding,
    expected_version: u64,
    server_name: String,
    validity: ConfigurationCertificateValidity,
}
impl ConfigurationIdentityRequest {
    pub fn new(
        expected: &ConfigurationInstanceState,
        ca_ref: SecretRef,
        tls_ref: SecretRef,
        server_name: String,
        validity: ConfigurationCertificateValidity,
    ) -> Result<Self, Error> {
        let ConfigurationRole::Master(authority) = expected.role() else {
            return Err(Error::InvalidRequest);
        };
        Self::from_parts(
            authority.clone(),
            expected.version(),
            ca_ref,
            tls_ref,
            server_name,
            validity,
        )
    }

    fn from_parts(
        authority: ConfigurationAuthority,
        expected_version: u64,
        ca_ref: SecretRef,
        tls_ref: SecretRef,
        server_name: String,
        validity: ConfigurationCertificateValidity,
    ) -> Result<Self, Error> {
        if ca_ref == tls_ref
            || expected_version == 0
            || expected_version > i64::MAX as u64
            || !valid_server_name(&server_name)
            || validity.leaf_not_after <= validity.not_before
            || validity.ca_not_after < validity.leaf_not_after
            || [
                validity.not_before,
                validity.leaf_not_after,
                validity.ca_not_after,
            ]
            .iter()
            .any(|value| time::OffsetDateTime::from_unix_timestamp(*value).is_err())
        {
            return Err(Error::InvalidRequest);
        }
        let binding = |purpose, secret_ref| {
            ConfigurationSecretBinding::new(
                authority.master_id().clone(),
                authority.clone(),
                purpose,
                secret_ref,
            )
            .map_err(|_| Error::InvalidRequest)
        };
        Ok(Self {
            ca_binding: binding(
                ConfigurationSecretPurpose::MasterCertificateAuthority,
                ca_ref,
            )?,
            tls_binding: binding(ConfigurationSecretPurpose::MasterTlsIdentity, tls_ref)?,
            expected_version,
            server_name,
            validity,
        })
    }
    pub fn ca_binding(&self) -> &ConfigurationSecretBinding {
        &self.ca_binding
    }
    pub fn tls_binding(&self) -> &ConfigurationSecretBinding {
        &self.tls_binding
    }
    pub fn server_name(&self) -> &str {
        &self.server_name
    }
    pub fn validity(&self) -> ConfigurationCertificateValidity {
        self.validity
    }
    pub fn expected_version(&self) -> u64 {
        self.expected_version
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationIdentityStatus {
    Pending,
    Active,
    Retired,
}

/// Public metadata only. Active is a durable setup state, not a reusable proof
/// of current authority, certificate validity or permission to serve remotely.
#[derive(Debug, Clone)]
pub struct ConfigurationIdentityRecord {
    pub identity_id: ConfigurationMasterIdentityId,
    pub request: ConfigurationIdentityRequest,
    pub status: ConfigurationIdentityStatus,
    pub certificate_authority_der: Vec<u8>,
    pub server_certificate_der: Vec<u8>,
    pub certificate_authority_fingerprint: ContentHash,
}

/// Private serving material acquired for one explicit active master identity.
/// Its state is the exact current state checked both before and after reading
/// the TLS key from the OS vault. This is an acquisition snapshot, not a lease:
/// callers must drop derived TLS configuration when authority, identity status
/// or certificate validity changes; active connections also need draining.
pub struct ConfigurationTlsIdentity {
    state: ConfigurationInstanceState,
    authority: ConfigurationAuthority,
    identity_id: ConfigurationMasterIdentityId,
    server_name: String,
    certificate_chain_der: Vec<Vec<u8>>,
    leaf_not_after_unix_seconds: i64,
    private_key: ConfigurationPrivateKey,
}

impl ConfigurationTlsIdentity {
    pub fn state(&self) -> &ConfigurationInstanceState {
        &self.state
    }

    pub fn authority(&self) -> &ConfigurationAuthority {
        &self.authority
    }

    pub fn identity_id(&self) -> &ConfigurationMasterIdentityId {
        &self.identity_id
    }

    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    /// Leaf certificate first, followed by the public self-signed CA.
    pub fn certificate_chain_der(&self) -> &[Vec<u8>] {
        &self.certificate_chain_der
    }

    pub fn leaf_not_after_unix_seconds(&self) -> i64 {
        self.leaf_not_after_unix_seconds
    }

    /// Borrow the zeroizing TLS private-key DER without copying it into a
    /// caller-owned `Vec`.
    pub fn private_key_der(&self) -> &[u8] {
        self.private_key.expose_der()
    }
}

struct StoredIdentity {
    record: ConfigurationIdentityRecord,
    ca_key_hash: ContentHash,
    tls_key_hash: ContentHash,
}

/// One daemon's managed-identity owner. Clone this owner/store and vault handles;
/// do not concurrently open independent stores for the same database. The daemon
/// store lock remains the cross-process ownership boundary. All configuration
/// key writes must go through this owner, not the low-level vault adapter.
#[derive(Clone)]
pub struct ConfigurationIdentityProvisioner {
    store: SqliteStore,
    vault: OsConfigurationSecretStore,
}
impl ConfigurationIdentityProvisioner {
    pub fn new(store: SqliteStore, vault: OsConfigurationSecretStore) -> Self {
        Self { store, vault }
    }

    /// Generate once, reserve metadata, write/read back both envelopes, then
    /// activate under current master authority. Exact retries never rewrite keys.
    /// An incomplete interrupted attempt is retired and needs fresh references.
    /// Unavailable vault/storage errors preserve pending work for another retry.
    /// Clock/validity policy is supplied by the authenticated application. The
    /// clock is sampled after queueing and again after vault work, at activation.
    pub async fn provision(
        &self,
        request: ConfigurationIdentityRequest,
        clock: impl Fn() -> i64 + Send + Sync + 'static,
    ) -> Result<ConfigurationIdentityRecord, Error> {
        let owner = self.clone();
        // A dropped HTTP caller must not release ownership while a vault effect
        // can still finish. The task owns the lock through readback/activation.
        tokio::spawn(async move {
            let _guard = owner.store.configuration_identity_operations.lock().await;
            owner.provision_owned(request, &clock, None).await
        })
        .await
        .map_err(|_| Error::Unavailable)?
    }

    /// Read public journal metadata for local administration/recovery. No secret
    /// lookup and no assertion that an active record still authorizes serving.
    pub async fn get(
        &self,
        ca_ref: &SecretRef,
    ) -> Result<Option<ConfigurationIdentityRecord>, Error> {
        let mut connection = self.store.connection.lock().await;
        Ok(load(&mut connection, ca_ref)
            .await?
            .map(|stored| stored.record))
    }

    /// Acquire the active TLS identity for exactly the caller's current master
    /// state and public identity ID. The vault read is serialized with identity
    /// setup/retirement; authority, active status, metadata and time are checked
    /// again after the vault returns. A successful result is a point-in-time
    /// snapshot, not a serving lease or proof of future readiness.
    pub async fn acquire_active_tls_identity(
        &self,
        expected: &ConfigurationInstanceState,
        identity_id: &ConfigurationMasterIdentityId,
        clock: impl Fn() -> i64 + Send + Sync + 'static,
    ) -> Result<ConfigurationTlsIdentity, Error> {
        let owner = self.clone();
        let expected = expected.clone();
        let identity_id = identity_id.clone();
        // As with provision/retirement, a dropped caller cannot abandon an OS
        // vault read before its raw result is wrapped and zeroized.
        tokio::spawn(async move {
            let _guard = owner.store.configuration_identity_operations.lock().await;
            owner
                .acquire_active_tls_identity_owned(&expected, &identity_id, &clock)
                .await
        })
        .await
        .map_err(|_| Error::Unavailable)?
    }

    async fn acquire_active_tls_identity_owned(
        &self,
        expected: &ConfigurationInstanceState,
        identity_id: &ConfigurationMasterIdentityId,
        clock: &(impl Fn() -> i64 + Sync + ?Sized),
    ) -> Result<ConfigurationTlsIdentity, Error> {
        let initial = {
            let mut connection = self.store.connection.lock().await;
            let mut transaction = connection.begin().await.map_err(|_| Error::Unavailable)?;
            let current = configuration_sync::load(&mut transaction)
                .await
                .map_err(|_| Error::Unavailable)?
                .ok_or(Error::Conflict)?;
            if current != *expected {
                return Err(Error::Conflict);
            }
            let stored = load_by_identity_id(&mut transaction, identity_id)
                .await?
                .ok_or(Error::Conflict)?;
            require_active_identity(&current, identity_id, &stored)?;
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            stored
        };

        let now = clock();
        require_identity_time(&initial.record.request, now)?;

        let binding = initial.record.request.tls_binding();
        let secret = self.vault.get(binding).await.map_err(|error| match error {
            SecretStoreError::NotFound | SecretStoreError::InvalidSecret => Error::RecoveryRequired,
            error => Error::Vault(error),
        })?;
        if hash_bytes(secret.as_bytes()) != initial.tls_key_hash {
            return Err(Error::RecoveryRequired);
        }
        let private_key = decode_configuration_private_key(binding, &secret)
            .map_err(|_| Error::IntegrityViolation)?;
        drop(secret);

        let final_state = {
            let mut connection = self.store.connection.lock().await;
            let mut transaction = connection.begin().await.map_err(|_| Error::Unavailable)?;
            let current = configuration_sync::load(&mut transaction)
                .await
                .map_err(|_| Error::Unavailable)?
                .ok_or(Error::Conflict)?;
            if current != *expected {
                return Err(Error::Conflict);
            }
            let current_identity = load_by_identity_id(&mut transaction, identity_id)
                .await?
                .ok_or(Error::Conflict)?;
            require_active_identity(&current, identity_id, &current_identity)?;
            if !same_identity_metadata(&initial, &current_identity) {
                return Err(Error::IntegrityViolation);
            }
            transaction.commit().await.map_err(|_| Error::Unavailable)?;
            current
        };

        // Sample after all queued/database/vault work, as close to handing the
        // material to the caller as possible.
        require_identity_time(&initial.record.request, clock())?;
        let authority = match final_state.role() {
            ConfigurationRole::Master(authority) => authority.clone(),
            _ => return Err(Error::Conflict),
        };
        Ok(ConfigurationTlsIdentity {
            state: final_state,
            authority,
            identity_id: initial.record.identity_id,
            server_name: initial.record.request.server_name,
            certificate_chain_der: vec![
                initial.record.server_certificate_der,
                initial.record.certificate_authority_der,
            ],
            leaf_not_after_unix_seconds: initial.record.request.validity.leaf_not_after,
            private_key,
        })
    }

    /// Bounded journal enumeration for restart recovery, including tombstones
    /// whose vault deletions may need retrying. Resume after the last returned ref.
    pub async fn list_references(
        &self,
        after: Option<&SecretRef>,
        limit: std::num::NonZeroU32,
    ) -> Result<Vec<SecretRef>, Error> {
        let mut connection = self.store.connection.lock().await;
        sqlx::query_scalar::<_, String>("SELECT ca_ref FROM configuration_master_identities WHERE (? IS NULL OR ca_ref > ?) ORDER BY ca_ref LIMIT ?")
            .bind(after.map(SecretRef::as_str)).bind(after.map(SecretRef::as_str))
            .bind(i64::from(limit.get())).fetch_all(&mut *connection).await.map_err(|_| Error::Unavailable)?
            .into_iter().map(|value| SecretRef::parse(value).map_err(|_| Error::IntegrityViolation)).collect()
    }

    /// Retire before deleting either key. Exact retries reattempt both deletions;
    /// the tombstone is never removed, even when cleanup succeeds. Also usable
    /// after a role change, since this can only reduce old identity access.
    pub async fn retire_and_cleanup(&self, ca_ref: SecretRef) -> Result<(), Error> {
        let owner = self.clone();
        tokio::spawn(async move {
            let _guard = owner.store.configuration_identity_operations.lock().await;
            owner.retire_and_cleanup_owned(ca_ref).await
        })
        .await
        .map_err(|_| Error::Unavailable)?
    }

    /// Retire exactly the identity selected by its public ID. The owner check,
    /// tombstone write, and vault cleanup share the same serialized operation.
    /// Historical identities remain cleanable after their master role is left.
    pub async fn retire_and_cleanup_by_identity_id(
        &self,
        identity_id: ConfigurationMasterIdentityId,
        expected_instance_id: KilnInstanceId,
    ) -> Result<(), Error> {
        let owner = self.clone();
        tokio::spawn(async move {
            let _guard = owner.store.configuration_identity_operations.lock().await;
            let ca_ref = {
                let mut connection = owner.store.connection.lock().await;
                let reference: Option<String> = sqlx::query_scalar(
                    "SELECT ca_ref FROM configuration_master_identities WHERE identity_id = ? AND master_instance_id = ?",
                )
                .bind(identity_id.as_str())
                .bind(expected_instance_id.as_str())
                .fetch_optional(&mut *connection)
                .await
                .map_err(|_| Error::Unavailable)?;
                let ca_ref = SecretRef::parse(reference.ok_or(Error::InvalidRequest)?)
                    .map_err(|_| Error::IntegrityViolation)?;
                let stored = load(&mut connection, &ca_ref)
                    .await?
                    .ok_or(Error::IntegrityViolation)?;
                if stored.record.identity_id != identity_id
                    || stored.record.request.ca_binding.instance_id() != &expected_instance_id
                {
                    return Err(Error::IntegrityViolation);
                }
                ca_ref
            };
            owner.retire_and_cleanup_owned(ca_ref).await
        })
        .await
        .map_err(|_| Error::Unavailable)?
    }

    async fn retire_and_cleanup_owned(&self, ca_ref: SecretRef) -> Result<(), Error> {
        let stored = {
            let mut connection = self.store.connection.lock().await;
            let stored = load(&mut connection, &ca_ref)
                .await?
                .ok_or(Error::InvalidRequest)?;
            retire(&mut connection, &ca_ref).await?;
            stored
        };
        // Try both slots even if the first delete fails. Tombstone retries
        // are deliberate: never forget an uncertain OS deletion.
        let ca = self.vault.delete(stored.record.request.ca_binding()).await;
        let tls = self.vault.delete(stored.record.request.tls_binding()).await;
        for result in [ca, tls] {
            match result {
                Ok(()) | Err(SecretStoreError::NotFound) => {}
                Err(error) => return Err(Error::Vault(error)),
            }
        }
        Ok(())
    }

    async fn provision_owned(
        &self,
        request: ConfigurationIdentityRequest,
        clock: &(impl Fn() -> i64 + Sync + ?Sized),
        request_key: Option<&str>,
    ) -> Result<ConfigurationIdentityRecord, Error> {
        let ca_ref = request.ca_binding.secret_ref();
        let mut existing = {
            let mut connection = self.store.connection.lock().await;
            load(&mut connection, ca_ref).await?
        };
        if let Some(stored) = &existing {
            if stored.record.request != request {
                return Err(Error::ReferenceConflict);
            }
            if stored.record.status == ConfigurationIdentityStatus::Retired {
                return Err(Error::Retired);
            }
        }
        let now = clock();
        if now < request.validity.not_before || now >= request.validity.leaf_not_after {
            return Err(Error::Identity(ConfigurationIdentityError::InvalidValidity));
        }
        if existing.is_none() {
            // Generation has no external effects. Persist every reference and
            // public/key-hash binding atomically before entering the vault.
            let generated = generate_configuration_identity(
                &request.ca_binding,
                &request.tls_binding,
                &request.server_name,
                request.validity,
                now,
            )
            .map_err(Error::Identity)?;
            let stored = StoredIdentity {
                ca_key_hash: hash_bytes(generated.certificate_authority_key.as_bytes()),
                tls_key_hash: hash_bytes(generated.server_private_key.as_bytes()),
                record: ConfigurationIdentityRecord {
                    identity_id: ConfigurationMasterIdentityId::from_ulid(ulid::Ulid::generate()),
                    request: request.clone(),
                    status: ConfigurationIdentityStatus::Pending,
                    certificate_authority_der: generated.certificate_authority_der,
                    server_certificate_der: generated.server_certificate_der,
                    certificate_authority_fingerprint: generated.certificate_authority_fingerprint,
                },
            };
            self.reserve(&stored, request_key).await?;
            self.vault
                .put_at(&request.ca_binding, generated.certificate_authority_key)
                .await
                .map_err(Error::Vault)?;
            self.vault
                .put_at(&request.tls_binding, generated.server_private_key)
                .await
                .map_err(Error::Vault)?;
            existing = Some(stored);
        }
        let stored = existing.ok_or(Error::IntegrityViolation)?;
        // Recheck before resolving secrets as well as in the activation commit.
        // Normal publications advance state version but keep this reservation;
        // leaving the role retires it in the same transaction as the role write.
        self.require_live(&request).await?;
        for (binding, expected_hash) in [
            (&request.ca_binding, &stored.ca_key_hash),
            (&request.tls_binding, &stored.tls_key_hash),
        ] {
            match self.vault.get(binding).await {
                Ok(secret) if hash_bytes(secret.as_bytes()) == *expected_hash => {
                    if decode_configuration_private_key(binding, &secret).is_err() {
                        self.retire_incomplete(ca_ref).await?;
                        return Err(Error::IntegrityViolation);
                    }
                }
                Ok(_) | Err(SecretStoreError::NotFound | SecretStoreError::InvalidSecret) => {
                    self.retire_incomplete(ca_ref).await?;
                    return Err(Error::RecoveryRequired);
                }
                Err(error) => return Err(Error::Vault(error)),
            }
        }
        let mut connection = self.store.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        require_live(&mut transaction, &request).await?;
        let now = clock();
        if now < request.validity.not_before || now >= request.validity.leaf_not_after {
            return Err(Error::Identity(ConfigurationIdentityError::InvalidValidity));
        }
        sqlx::query("UPDATE configuration_master_identities SET status = 'active' WHERE ca_ref = ? AND status = 'pending'")
            .bind(ca_ref.as_str()).execute(&mut *transaction).await.map_err(|_| Error::Unavailable)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        let mut record = stored.record;
        record.status = ConfigurationIdentityStatus::Active;
        Ok(record)
    }

    async fn require_live(&self, request: &ConfigurationIdentityRequest) -> Result<(), Error> {
        let mut connection = self.store.connection.lock().await;
        let mut transaction = connection.begin().await.map_err(|_| Error::Unavailable)?;
        require_live(&mut transaction, request).await?;
        transaction.commit().await.map_err(|_| Error::Unavailable)
    }

    async fn retire_incomplete(&self, ca_ref: &SecretRef) -> Result<(), Error> {
        let mut connection = self.store.connection.lock().await;
        retire(&mut connection, ca_ref).await
    }

    async fn reserve(
        &self,
        stored: &StoredIdentity,
        request_key: Option<&str>,
    ) -> Result<(), Error> {
        let request = &stored.record.request;
        let mut connection = self.store.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| Error::Unavailable)?;
        let current = configuration_sync::load(&mut transaction)
            .await
            .map_err(|_| Error::Unavailable)?
            .ok_or(Error::Conflict)?;
        if current.version() != request.expected_version || !is_master(&current, request) {
            return Err(Error::Conflict);
        }
        // Cross-purpose collisions must be rejected too; column UNIQUE alone
        // would allow a historical TLS ref to become a future CA ref.
        let collision: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM configuration_master_identities WHERE ca_ref IN (?, ?) OR tls_ref IN (?, ?)")
            .bind(request.ca_binding.secret_ref().as_str()).bind(request.tls_binding.secret_ref().as_str())
            .bind(request.ca_binding.secret_ref().as_str()).bind(request.tls_binding.secret_ref().as_str())
            .fetch_one(&mut *transaction).await.map_err(|_| Error::Unavailable)?;
        if collision != 0 {
            return Err(Error::ReferenceConflict);
        }
        let occupied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM configuration_master_identities WHERE group_id = ? AND status != 'retired'")
            .bind(request.ca_binding.authority().group_id().as_str()).fetch_one(&mut *transaction).await.map_err(|_| Error::Unavailable)?;
        if occupied != 0 {
            return Err(Error::Conflict);
        }
        sqlx::query("INSERT INTO configuration_master_identities (ca_ref, tls_ref, group_id, master_instance_id, reserved_state_version, server_name, not_before, leaf_not_after, ca_not_after, ca_der, tls_der, ca_key_hash, tls_key_hash, status, request_key, identity_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)")
            .bind(request.ca_binding.secret_ref().as_str()).bind(request.tls_binding.secret_ref().as_str())
            .bind(request.ca_binding.authority().group_id().as_str()).bind(request.ca_binding.instance_id().as_str())
            .bind(request.expected_version as i64).bind(&request.server_name)
            .bind(request.validity.not_before).bind(request.validity.leaf_not_after).bind(request.validity.ca_not_after)
            .bind(&stored.record.certificate_authority_der).bind(&stored.record.server_certificate_der)
            .bind(stored.ca_key_hash.as_str()).bind(stored.tls_key_hash.as_str())
            .bind(request_key)
            .bind(stored.record.identity_id.as_str())
            .execute(&mut *transaction).await.map_err(|_| Error::Unavailable)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)
    }
}

fn is_master(current: &ConfigurationInstanceState, request: &ConfigurationIdentityRequest) -> bool {
    current.instance_id() == request.ca_binding.instance_id()
        && matches!(current.role(), ConfigurationRole::Master(authority) if authority == request.ca_binding.authority())
}

async fn load_by_identity_id(
    connection: &mut SqliteConnection,
    identity_id: &ConfigurationMasterIdentityId,
) -> Result<Option<StoredIdentity>, Error> {
    let reference: Option<String> = sqlx::query_scalar(
        "SELECT ca_ref FROM configuration_master_identities WHERE identity_id = ?",
    )
    .bind(identity_id.as_str())
    .fetch_optional(&mut *connection)
    .await
    .map_err(|_| Error::Unavailable)?;
    let Some(reference) = reference else {
        return Ok(None);
    };
    let reference = SecretRef::parse(reference).map_err(|_| Error::IntegrityViolation)?;
    load(connection, &reference).await
}

fn require_active_identity(
    current: &ConfigurationInstanceState,
    identity_id: &ConfigurationMasterIdentityId,
    stored: &StoredIdentity,
) -> Result<(), Error> {
    if stored.record.identity_id != *identity_id {
        return Err(Error::IntegrityViolation);
    }
    if !is_master(current, &stored.record.request) {
        return Err(Error::Conflict);
    }
    match stored.record.status {
        ConfigurationIdentityStatus::Active => Ok(()),
        ConfigurationIdentityStatus::Pending => Err(Error::Conflict),
        ConfigurationIdentityStatus::Retired => Err(Error::Retired),
    }
}

fn require_identity_time(request: &ConfigurationIdentityRequest, now: i64) -> Result<(), Error> {
    let validity = request.validity();
    if now < validity.not_before || now >= validity.leaf_not_after || now >= validity.ca_not_after {
        return Err(Error::Identity(ConfigurationIdentityError::InvalidValidity));
    }
    Ok(())
}

fn same_identity_metadata(left: &StoredIdentity, right: &StoredIdentity) -> bool {
    left.record.identity_id == right.record.identity_id
        && left.record.request == right.record.request
        && left.record.status == right.record.status
        && left.record.certificate_authority_der == right.record.certificate_authority_der
        && left.record.server_certificate_der == right.record.server_certificate_der
        && left.record.certificate_authority_fingerprint
            == right.record.certificate_authority_fingerprint
        && left.ca_key_hash == right.ca_key_hash
        && left.tls_key_hash == right.tls_key_hash
}

async fn require_live(
    connection: &mut SqliteConnection,
    request: &ConfigurationIdentityRequest,
) -> Result<(), Error> {
    let current = configuration_sync::load(connection)
        .await
        .map_err(|_| Error::Unavailable)?
        .ok_or(Error::Conflict)?;
    if !is_master(&current, request) {
        return Err(Error::Conflict);
    }
    let stored = load(connection, request.ca_binding.secret_ref())
        .await?
        .ok_or(Error::IntegrityViolation)?;
    if stored.record.request != *request {
        return Err(Error::ReferenceConflict);
    }
    if stored.record.status == ConfigurationIdentityStatus::Retired {
        return Err(Error::Retired);
    }
    Ok(())
}
async fn retire(connection: &mut SqliteConnection, ca_ref: &SecretRef) -> Result<(), Error> {
    sqlx::query("UPDATE configuration_master_identities SET status = 'retired' WHERE ca_ref = ?")
        .bind(ca_ref.as_str())
        .execute(connection)
        .await
        .map_err(|_| Error::Unavailable)?;
    Ok(())
}
async fn load(
    connection: &mut SqliteConnection,
    ca_ref: &SecretRef,
) -> Result<Option<StoredIdentity>, Error> {
    let row = sqlx::query("SELECT i.*, a.master_instance_id AS authority_master FROM configuration_master_identities i JOIN configuration_authorities a ON a.group_id = i.group_id WHERE i.ca_ref = ?")
        .bind(ca_ref.as_str()).fetch_optional(connection).await.map_err(|_| Error::Unavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let string = |key| {
        row.try_get::<String, _>(key)
            .map_err(|_| Error::IntegrityViolation)
    };
    let integer = |key| {
        row.try_get::<i64, _>(key)
            .map_err(|_| Error::IntegrityViolation)
    };
    let master = KilnInstanceId::parse(string("master_instance_id")?)
        .map_err(|_| Error::IntegrityViolation)?;
    if master.as_str() != string("authority_master")? {
        return Err(Error::IntegrityViolation);
    }
    let authority = ConfigurationAuthority::new(
        ConfigurationGroupId::parse(string("group_id")?).map_err(|_| Error::IntegrityViolation)?,
        master,
    );
    let request = ConfigurationIdentityRequest::from_parts(
        authority,
        u64::try_from(integer("reserved_state_version")?).map_err(|_| Error::IntegrityViolation)?,
        ca_ref.clone(),
        SecretRef::parse(string("tls_ref")?).map_err(|_| Error::IntegrityViolation)?,
        string("server_name")?,
        ConfigurationCertificateValidity {
            not_before: integer("not_before")?,
            leaf_not_after: integer("leaf_not_after")?,
            ca_not_after: integer("ca_not_after")?,
        },
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let status = match string("status")?.as_str() {
        "pending" => ConfigurationIdentityStatus::Pending,
        "active" => ConfigurationIdentityStatus::Active,
        "retired" => ConfigurationIdentityStatus::Retired,
        _ => return Err(Error::IntegrityViolation),
    };
    let ca_der: Vec<u8> = row
        .try_get("ca_der")
        .map_err(|_| Error::IntegrityViolation)?;
    let tls_der: Vec<u8> = row
        .try_get("tls_der")
        .map_err(|_| Error::IntegrityViolation)?;
    if ca_der.is_empty() || tls_der.is_empty() {
        return Err(Error::IntegrityViolation);
    }
    Ok(Some(StoredIdentity {
        ca_key_hash: ContentHash::parse(string("ca_key_hash")?)
            .map_err(|_| Error::IntegrityViolation)?,
        tls_key_hash: ContentHash::parse(string("tls_key_hash")?)
            .map_err(|_| Error::IntegrityViolation)?,
        record: ConfigurationIdentityRecord {
            identity_id: ConfigurationMasterIdentityId::parse(string("identity_id")?)
                .map_err(|_| Error::IntegrityViolation)?,
            request,
            status,
            certificate_authority_fingerprint: hash_bytes(&ca_der),
            certificate_authority_der: ca_der,
            server_certificate_der: tls_der,
        },
    }))
}
