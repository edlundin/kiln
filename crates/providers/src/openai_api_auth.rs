use std::fmt;

use kiln_core::{ProviderAccountId, SecretValue};
use serde::{Deserialize, Serialize};

pub const OPENAI_API_PROVIDER_TYPE: &str = "openai_api";
const CREDENTIAL_VERSION: u32 = 1;

/// A syntactically valid API key. This does not establish provider acceptance,
/// billing status, model access, or entitlement. It has no refresh-token path.
pub struct OpenAiApiKey(SecretValue);

impl fmt::Debug for OpenAiApiKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OpenAiApiKey(<redacted>)")
    }
}

impl OpenAiApiKey {
    pub fn new(secret: SecretValue) -> Result<Self, OpenAiApiCredentialError> {
        // Require a nonempty visible ASCII header value without assuming a
        // vendor-specific key prefix or length beyond the core secret ceiling.
        if secret
            .as_bytes()
            .iter()
            .any(|byte| !byte.is_ascii_graphic())
        {
            return Err(OpenAiApiCredentialError::InvalidKey);
        }
        Ok(Self(secret))
    }

    pub fn into_credential(
        self,
        account_id: &ProviderAccountId,
    ) -> Result<SecretValue, OpenAiApiCredentialError> {
        let api_key = std::str::from_utf8(self.0.as_bytes())
            .map_err(|_| OpenAiApiCredentialError::InvalidKey)?;
        let bytes = serde_json::to_vec(&CredentialEnvelopeRef {
            version: CREDENTIAL_VERSION,
            provider_type: OPENAI_API_PROVIDER_TYPE,
            provider_account_id: account_id.as_str(),
            api_key,
        })
        .map_err(|_| OpenAiApiCredentialError::InvalidEnvelope)?;
        SecretValue::new(bytes).map_err(|_| OpenAiApiCredentialError::InvalidEnvelope)
    }

    pub fn from_credential(
        account_id: &ProviderAccountId,
        credential: SecretValue,
    ) -> Result<Self, OpenAiApiCredentialError> {
        let envelope: CredentialEnvelope = serde_json::from_slice(credential.as_bytes())
            .map_err(|_| OpenAiApiCredentialError::InvalidEnvelope)?;
        if envelope.version != CREDENTIAL_VERSION
            || envelope.provider_type != OPENAI_API_PROVIDER_TYPE
        {
            return Err(OpenAiApiCredentialError::InvalidEnvelope);
        }
        if envelope.provider_account_id != account_id.as_str() {
            return Err(OpenAiApiCredentialError::AccountMismatch);
        }
        Self::new(
            SecretValue::new(envelope.api_key.into_bytes())
                .map_err(|_| OpenAiApiCredentialError::InvalidKey)?,
        )
    }

    /// Consumed only inside the provider's authenticated transport boundary.
    pub fn into_secret(self) -> SecretValue {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAiApiCredentialError {
    InvalidKey,
    InvalidEnvelope,
    AccountMismatch,
}

#[derive(Serialize)]
struct CredentialEnvelopeRef<'a> {
    version: u32,
    provider_type: &'a str,
    provider_account_id: &'a str,
    api_key: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialEnvelope {
    version: u32,
    provider_type: String,
    provider_account_id: String,
    api_key: String,
}
