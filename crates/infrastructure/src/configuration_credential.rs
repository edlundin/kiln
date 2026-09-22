use kiln_core::ConfigurationCredentialDigest;

const PREFIX: &[u8; 6] = b"kcfg1_";
const LENGTH: usize = PREFIX.len() + super::AUTH_TOKEN_LENGTH;

/// Dedicated read-only synchronization bearer. Deliberately neither Debug,
/// Clone nor serializable. Private delivery/storage belongs to enrollment.
pub struct ConfigurationReadCredential([u8; LENGTH]);

impl ConfigurationReadCredential {
    /// Uses the same CSPRNG construction as local authentication, with fresh
    /// independent randomness. Never reads or derives from the local API bearer.
    pub fn generate() -> Self {
        let mut bytes = [0; LENGTH];
        bytes[..PREFIX.len()].copy_from_slice(PREFIX);
        bytes[PREFIX.len()..].copy_from_slice(&super::generated_auth_token());
        Self(bytes)
    }

    pub fn parse(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != LENGTH
            || !bytes.starts_with(PREFIX)
            || !bytes[PREFIX.len()..]
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
        {
            return None;
        }
        Some(Self(bytes.try_into().ok()?))
    }

    pub fn digest(&self) -> ConfigurationCredentialDigest {
        ConfigurationCredentialDigest::from_sha256(super::hash_bytes(&self.0))
    }

    /// Secret material: use only for private enrollment delivery or a sensitive
    /// Authorization header. Never put this value in URLs, logs or events.
    pub fn expose_secret(&self) -> &[u8] {
        &self.0
    }
}
