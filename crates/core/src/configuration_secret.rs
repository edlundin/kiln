//! Host-local vault references, excluded from shared configuration snapshots.

use crate::{ConfigurationAuthority, KilnInstanceId, SecretRef, SecretStoreError, SecretValue};
use std::future::Future;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationSecretPurpose {
    MasterCertificateAuthority,
    MasterTlsIdentity,
    FollowerReadCredential,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidConfigurationSecretBinding;

/// Immutable vault lookup identity, not proof of current enrollment or authority.
/// The application must recheck durable role/version before resolving a secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationSecretBinding {
    instance_id: KilnInstanceId,
    authority: ConfigurationAuthority,
    purpose: ConfigurationSecretPurpose,
    secret_ref: SecretRef,
}
impl ConfigurationSecretBinding {
    pub fn new(
        instance_id: KilnInstanceId,
        authority: ConfigurationAuthority,
        purpose: ConfigurationSecretPurpose,
        secret_ref: SecretRef,
    ) -> Result<Self, InvalidConfigurationSecretBinding> {
        let master_secret = matches!(
            purpose,
            ConfigurationSecretPurpose::MasterCertificateAuthority
                | ConfigurationSecretPurpose::MasterTlsIdentity
        );
        if master_secret != (authority.master_id() == &instance_id) {
            return Err(InvalidConfigurationSecretBinding);
        }
        Ok(Self {
            instance_id,
            authority,
            purpose,
            secret_ref,
        })
    }
    pub fn instance_id(&self) -> &KilnInstanceId {
        &self.instance_id
    }
    pub fn authority(&self) -> &ConfigurationAuthority {
        &self.authority
    }
    pub fn purpose(&self) -> ConfigurationSecretPurpose {
        self.purpose
    }
    pub fn secret_ref(&self) -> &SecretRef {
        &self.secret_ref
    }
}

/// A separate vault namespace from provider credentials and local API auth.
/// Values are adapter-owned, redacted, bounded single-line envelopes; binary
/// TLS material must be encoded by its adapter before entering this port.
pub trait ConfigurationSecretStore: Send + Sync {
    /// Write only a fresh reference already reserved in durable metadata. Caller
    /// cancellation may outlive the OS write; cleanup must serialize with it.
    fn put_at(
        &self,
        binding: &ConfigurationSecretBinding,
        value: SecretValue,
    ) -> impl Future<Output = Result<(), SecretStoreError>> + Send;
    fn get(
        &self,
        binding: &ConfigurationSecretBinding,
    ) -> impl Future<Output = Result<SecretValue, SecretStoreError>> + Send;
    fn delete(
        &self,
        binding: &ConfigurationSecretBinding,
    ) -> impl Future<Output = Result<(), SecretStoreError>> + Send;
}
