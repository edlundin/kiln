//! Explicit, host-local certificate generation and vault key envelopes. No vault
//! writes, trust installation, socket binding or enrollment occur here.

use kiln_core::{ConfigurationSecretBinding, ConfigurationSecretPurpose, ContentHash, SecretValue};
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose,
};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use time::OffsetDateTime;
use zeroize::{Zeroize, Zeroizing};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationIdentityError {
    InvalidBinding,
    InvalidServerName,
    InvalidValidity,
    InvalidSecret,
    Unavailable,
}

pub use kiln_core::ConfigurationCertificateValidity;

/// Public certificate bytes and separate redacted vault envelopes. A generated
/// identity is not active and does not establish trust on any other instance.
pub struct GeneratedConfigurationIdentity {
    pub certificate_authority_der: Vec<u8>,
    pub server_certificate_der: Vec<u8>,
    pub certificate_authority_fingerprint: ContentHash,
    pub certificate_authority_key: SecretValue,
    pub server_private_key: SecretValue,
}

/// Explicit decoding boundary for transport/signing adapters. No Debug, Clone
/// or serialization implementation; the owned DER buffer is zeroized on drop.
pub struct ConfigurationPrivateKey(Zeroizing<Vec<u8>>);
impl ConfigurationPrivateKey {
    pub fn expose_der(&self) -> &[u8] {
        &self.0
    }
}

/// Generate fresh independent P-256 CA and leaf keys. Bindings must already name
/// distinct fresh master CA/TLS references in one authority. The caller must
/// durably reserve references and public metadata before any vault write, and
/// supplies the clock and validity policy; this function persists nothing.
pub fn generate_configuration_identity(
    ca_binding: &ConfigurationSecretBinding,
    tls_binding: &ConfigurationSecretBinding,
    server_name: &str,
    validity: ConfigurationCertificateValidity,
    now_unix_seconds: i64,
) -> Result<GeneratedConfigurationIdentity, ConfigurationIdentityError> {
    use ConfigurationIdentityError as Error;
    if ca_binding.purpose() != ConfigurationSecretPurpose::MasterCertificateAuthority
        || tls_binding.purpose() != ConfigurationSecretPurpose::MasterTlsIdentity
        || ca_binding.authority() != tls_binding.authority()
        || ca_binding.instance_id() != tls_binding.instance_id()
        || ca_binding.secret_ref() == tls_binding.secret_ref()
    {
        return Err(Error::InvalidBinding);
    }
    if !valid_server_name(server_name) {
        return Err(Error::InvalidServerName);
    }
    if validity.not_before > now_unix_seconds
        || validity.leaf_not_after <= now_unix_seconds
        || validity.ca_not_after < validity.leaf_not_after
    {
        return Err(Error::InvalidValidity);
    }
    let not_before = OffsetDateTime::from_unix_timestamp(validity.not_before)
        .map_err(|_| Error::InvalidValidity)?;
    let leaf_not_after = OffsetDateTime::from_unix_timestamp(validity.leaf_not_after)
        .map_err(|_| Error::InvalidValidity)?;
    let ca_not_after = OffsetDateTime::from_unix_timestamp(validity.ca_not_after)
        .map_err(|_| Error::InvalidValidity)?;
    let mut ca = CertificateParams::default();
    ca.distinguished_name = DistinguishedName::new();
    ca.distinguished_name.push(
        DnType::CommonName,
        format!("Kiln {} CA", ca_binding.authority().group_id().as_str()),
    );
    ca.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    ca.not_before = not_before;
    ca.not_after = ca_not_after;
    let ca_key = Zeroizing::new(
        KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).map_err(|_| Error::Unavailable)?,
    );
    let ca_certificate = ca.self_signed(&*ca_key).map_err(|_| Error::Unavailable)?;
    let issuer = Issuer::new(ca, &*ca_key);

    let mut leaf = CertificateParams::new(vec![server_name.to_owned()])
        .map_err(|_| Error::InvalidServerName)?;
    leaf.distinguished_name = DistinguishedName::new();
    leaf.distinguished_name.push(
        DnType::CommonName,
        format!("Kiln {}", tls_binding.instance_id().as_str()),
    );
    leaf.is_ca = IsCa::ExplicitNoCa;
    leaf.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    leaf.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    leaf.use_authority_key_identifier_extension = true;
    leaf.not_before = not_before;
    leaf.not_after = leaf_not_after;
    let leaf_key = Zeroizing::new(
        KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).map_err(|_| Error::Unavailable)?,
    );
    let leaf_certificate = leaf
        .signed_by(&*leaf_key, &issuer)
        .map_err(|_| Error::Unavailable)?;
    let certificate_authority_der = ca_certificate.der().to_vec();
    let certificate_authority_fingerprint = super::hash_bytes(&certificate_authority_der);
    Ok(GeneratedConfigurationIdentity {
        certificate_authority_der,
        certificate_authority_fingerprint,
        server_certificate_der: leaf_certificate.der().to_vec(),
        certificate_authority_key: encode_key(ca_binding, ca_key.serialized_der())?,
        server_private_key: encode_key(tls_binding, leaf_key.serialized_der())?,
    })
}

#[derive(Serialize)]
struct KeyEnvelope<'a> {
    schema_version: u32,
    instance_id: &'a str,
    group_id: &'a str,
    master_instance_id: &'a str,
    purpose: &'a str,
    secret_ref: &'a str,
    private_key_der: &'a [u8],
}

fn encode_key(
    binding: &ConfigurationSecretBinding,
    key: &[u8],
) -> Result<SecretValue, ConfigurationIdentityError> {
    let envelope = KeyEnvelope {
        schema_version: 1,
        instance_id: binding.instance_id().as_str(),
        group_id: binding.authority().group_id().as_str(),
        master_instance_id: binding.authority().master_id().as_str(),
        purpose: key_purpose(binding)?,
        secret_ref: binding.secret_ref().as_str(),
        private_key_der: key,
    };
    let bytes =
        serde_json::to_vec(&envelope).map_err(|_| ConfigurationIdentityError::Unavailable)?;
    SecretValue::new(bytes).map_err(|_| ConfigurationIdentityError::InvalidSecret)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredKeyEnvelope {
    schema_version: u32,
    instance_id: String,
    group_id: String,
    master_instance_id: String,
    purpose: String,
    secret_ref: String,
    private_key_der: Zeroizing<Vec<u8>>,
}

/// Decode only for the exact authority, purpose and reserved reference. It checks
/// DER key usability; matching a public leaf certificate remains the TLS adapter's
/// responsibility. Current enrollment/version authorization belongs to the caller.
pub fn decode_configuration_private_key(
    binding: &ConfigurationSecretBinding,
    secret: &SecretValue,
) -> Result<ConfigurationPrivateKey, ConfigurationIdentityError> {
    use ConfigurationIdentityError as Error;
    let purpose = key_purpose(binding)?;
    let envelope: StoredKeyEnvelope =
        serde_json::from_slice(secret.as_bytes()).map_err(|_| Error::InvalidSecret)?;
    if envelope.schema_version != 1
        || envelope.instance_id != binding.instance_id().as_str()
        || envelope.group_id != binding.authority().group_id().as_str()
        || envelope.master_instance_id != binding.authority().master_id().as_str()
        || envelope.purpose != purpose
        || envelope.secret_ref != binding.secret_ref().as_str()
    {
        return Err(Error::InvalidSecret);
    }
    let mut key =
        KeyPair::try_from(envelope.private_key_der.as_slice()).map_err(|_| Error::InvalidSecret)?;
    key.zeroize();
    Ok(ConfigurationPrivateKey(envelope.private_key_der))
}

fn key_purpose(
    binding: &ConfigurationSecretBinding,
) -> Result<&'static str, ConfigurationIdentityError> {
    match binding.purpose() {
        ConfigurationSecretPurpose::MasterCertificateAuthority => Ok("master-ca"),
        ConfigurationSecretPurpose::MasterTlsIdentity => Ok("master-tls"),
        ConfigurationSecretPurpose::FollowerReadCredential => {
            Err(ConfigurationIdentityError::InvalidBinding)
        }
    }
}

pub(super) fn valid_server_name(name: &str) -> bool {
    if let Ok(address) = name.parse::<IpAddr>() {
        return address.to_string() == name;
    }
    // RFC DNS limits. Require a canonical lowercase exact name, never a wildcard
    // or a URI/path/port. IDNs must already be converted to ASCII by enrollment.
    !name.is_empty()
        && name.len() <= 253
        && name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
}
