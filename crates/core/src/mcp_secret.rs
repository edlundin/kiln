//! MCP host values use their own vault identity, never provider credentials.

use crate::{
    KilnInstanceId, McpInstanceKey, SecretRef, SecretStoreError, SecretValue,
    SharedConfigurationKey,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpSecretPurpose {
    Argument,
    Environment,
}
impl McpSecretPurpose {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Argument => "argument",
            Self::Environment => "environment",
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
