//! MCP host values use their own vault identity, never provider credentials.

use crate::{
    KilnInstanceId, McpInstanceKey, SecretRef, SecretStoreError, SecretValue,
    SharedConfigurationKey,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpSecretPurpose {
    Argument,
    Environment,
    /// Authentication material for a declared HTTP credential binding.
    HttpCredential,
}
impl McpSecretPurpose {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Argument => "argument",
            Self::Environment => "environment",
            Self::HttpCredential => "http_credential",
        }
    }
}

/// Lookup identity only. The caller must authorize the exact local instance,
/// scoped server/auth profile and binding before resolution. No Debug: the
/// scoped key can contain private host paths. Definition versions are checked
/// by the host snapshot; rotating a value requires a fresh SecretRef.
#[derive(Clone)]
pub struct McpSecretBinding {
    instance_id: KilnInstanceId,
    key: McpInstanceKey,
    name: SharedConfigurationKey,
    purpose: McpSecretPurpose,
    secret_ref: SecretRef,
}
impl McpSecretBinding {
    pub fn new(
        instance_id: KilnInstanceId,
        key: McpInstanceKey,
        name: SharedConfigurationKey,
        purpose: McpSecretPurpose,
        secret_ref: SecretRef,
    ) -> Self {
        Self {
            instance_id,
            key,
            name,
            purpose,
            secret_ref,
        }
    }
    pub fn instance_id(&self) -> &KilnInstanceId {
        &self.instance_id
    }
    pub fn key(&self) -> &McpInstanceKey {
        &self.key
    }
    pub fn name(&self) -> &SharedConfigurationKey {
        &self.name
    }
    pub fn purpose(&self) -> McpSecretPurpose {
        self.purpose
    }
    pub fn secret_ref(&self) -> &SecretRef {
        &self.secret_ref
    }
}

/// Distinct from both provider and configuration-sync vault namespaces. Values
/// obey SecretValue's existing single-line, nonempty, bounded envelope.
pub trait McpSecretStore: Send + Sync {
    /// Write only a fresh reference reserved in durable host metadata. Deletion
    /// must serialize with OS writes that outlive a cancelled caller.
    fn put_at(
        &self,
        binding: &McpSecretBinding,
        value: SecretValue,
    ) -> impl Future<Output = Result<(), SecretStoreError>> + Send;
    fn get(
        &self,
        binding: &McpSecretBinding,
    ) -> impl Future<Output = Result<SecretValue, SecretStoreError>> + Send;
    fn delete(
        &self,
        binding: &McpSecretBinding,
    ) -> impl Future<Output = Result<(), SecretStoreError>> + Send;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpSecretReservationState {
    Reserved,
    Retired,
    Deleted,
}

/// Only Fresh permits the caller to write a vault value. An existing receipt,
/// including Reserved after an ambiguous commit, must never repeat that write.
pub enum McpSecretReservation {
    Fresh,
    Existing(McpSecretReservationState),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpSecretJournalError {
    InvalidRequest,
    LimitExceeded,
    DefinitionChanged,
    InvalidBinding,
    Conflict,
    NotFound,
    IntegrityViolation,
    Unavailable,
}

/// Host-local write ownership and cleanup receipts; never stores secret bytes.
/// Snapshot publication is owned by McpHostBindingStore. Published references
/// are excluded from pending cleanup and cannot be retired through this port.
/// Callers must serialize vault writes, retirement and deletion for a binding;
/// restart reconciliation requires exclusive daemon ownership. A cancelled OS
/// write must settle before its retirement is acknowledged as deleted.
pub trait McpSecretJournal: Send + Sync {
    fn reserve_mcp_secret(
        &self,
        binding: &McpSecretBinding,
        expected_definition_version: u64,
        limits: crate::McpDefinitionLimits,
    ) -> impl Future<Output = Result<McpSecretReservation, McpSecretJournalError>> + Send;

    /// Retires an unpublished reservation. Exact retries preserve Deleted.
    fn retire_mcp_secret_reservation(
        &self,
        binding: &McpSecretBinding,
    ) -> impl Future<Output = Result<McpSecretReservationState, McpSecretJournalError>> + Send;

    /// Call only after an idempotent vault delete has completed successfully.
    /// Retains a tombstone so this reference can never acquire a new writer.
    fn finish_mcp_secret_deletion(
        &self,
        binding: &McpSecretBinding,
    ) -> impl Future<Output = Result<(), McpSecretJournalError>> + Send;

    /// Bounded reconciliation for one already-authorized scope. Deleted
    /// tombstones are omitted. Retire/finish each returned item before taking
    /// another batch; this is not a cursor over concurrently changing writers.
    fn pending_mcp_secret_reservations(
        &self,
        instance_id: &KilnInstanceId,
        key: &McpInstanceKey,
        batch_size: std::num::NonZeroUsize,
    ) -> impl Future<
        Output = Result<Vec<(McpSecretBinding, McpSecretReservationState)>, McpSecretJournalError>,
    > + Send;
}
