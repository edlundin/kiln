use std::{fmt, time::Duration};

use kiln_core::{
    ProviderAccountId, ProviderCredentialRefreshError, ProviderCredentialRefresher, ProviderType,
    SecretValue,
};
use reqwest::{Client, StatusCode, redirect::Policy};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const OPENAI_CODEX_SUBSCRIPTION_PROVIDER_TYPE: &str = "openai_codex_subscription";

const CREDENTIAL_VERSION: u32 = 1;
const OAUTH_TOKEN_ENDPOINT: &str = "https://auth.openai.com/oauth/token";
const OAUTH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const MAX_SECRET_BYTES: usize = 1024 * 1024;
const MAX_ACCOUNT_ID_BYTES: usize = 1024;
const MAX_TOKEN_BYTES: usize = 256 * 1024;

/// Operational bounds for one refresh request. These are Kiln safety defaults,
/// not limits imposed by the OAuth service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodexSubscriptionRefreshLimits {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub max_response_bytes: usize,
}

impl Default for CodexSubscriptionRefreshLimits {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            max_response_bytes: MAX_SECRET_BYTES,
        }
    }
}

/// A provider-bound HTTPS refresher for ChatGPT subscription credentials.
///
/// The adapter is bound to one Kiln provider account when it is constructed.
/// Credential bytes only enter and leave through [`SecretValue`].
pub struct CodexSubscriptionCredentialRefresher {
    account_id: ProviderAccountId,
    provider_type: ProviderType,
    client: Client,
    max_response_bytes: usize,
}

impl fmt::Debug for CodexSubscriptionCredentialRefresher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexSubscriptionCredentialRefresher")
            .field("account_id", &self.account_id)
            .field("provider_type", &self.provider_type)
            .finish_non_exhaustive()
    }
}

impl CodexSubscriptionCredentialRefresher {
    pub fn new(account_id: ProviderAccountId) -> Result<Self, CodexSubscriptionCredentialError> {
        Self::with_limits(account_id, CodexSubscriptionRefreshLimits::default())
    }

    pub fn with_limits(
        account_id: ProviderAccountId,
        limits: CodexSubscriptionRefreshLimits,
    ) -> Result<Self, CodexSubscriptionCredentialError> {
        if limits.connect_timeout.is_zero()
            || limits.request_timeout.is_zero()
            || limits.max_response_bytes == 0
            || limits.max_response_bytes > MAX_SECRET_BYTES
        {
            return Err(CodexSubscriptionCredentialError::InvalidTransportLimits);
        }
        let provider_type = ProviderType::parse(OPENAI_CODEX_SUBSCRIPTION_PROVIDER_TYPE)
            .map_err(|_| CodexSubscriptionCredentialError::InvalidProviderBinding)?;
        let client = Client::builder()
            .redirect(Policy::none())
            .connect_timeout(limits.connect_timeout)
            .timeout(limits.request_timeout)
            .build()
            .map_err(|_| CodexSubscriptionCredentialError::TransportUnavailable)?;
        Ok(Self {
            account_id,
            provider_type,
            client,
            max_response_bytes: limits.max_response_bytes,
        })
    }

    async fn refresh_credential(
        &self,
        current: SecretValue,
    ) -> Result<SecretValue, ProviderCredentialRefreshError> {
        let mut credential = CredentialEnvelope::decode(current.as_bytes())
            .map_err(|_| ProviderCredentialRefreshError::Transient)?;
        let response = self
            .client
            .post(OAUTH_TOKEN_ENDPOINT)
            .json(&RefreshRequest {
                grant_type: "refresh_token",
                client_id: OAUTH_CLIENT_ID,
                refresh_token: &credential.refresh_token,
            })
            .send()
            .await
            .map_err(|_| ProviderCredentialRefreshError::Transient)?;
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED {
            return Err(ProviderCredentialRefreshError::Permanent);
        }
        let body = read_bounded_body(response, self.max_response_bytes).await?;

        if status.is_success() {
            let refreshed: RefreshResponse = serde_json::from_slice(&body)
                .map_err(|_| ProviderCredentialRefreshError::Transient)?;
            credential.merge(refreshed)?;
            return credential
                .encode()
                .map_err(|_| ProviderCredentialRefreshError::Transient);
        }

        let error_code = serde_json::from_slice::<RefreshErrorResponse>(&body)
            .ok()
            .and_then(|response| response.error)
            .filter(|code| !code.is_empty());
        if status == StatusCode::BAD_REQUEST
            && error_code
                .as_deref()
                .is_some_and(|code| code.eq_ignore_ascii_case("invalid_grant"))
            || error_code.as_deref().is_some_and(is_permanent_error_code)
        {
            Err(ProviderCredentialRefreshError::Permanent)
        } else {
            Err(ProviderCredentialRefreshError::Transient)
        }
    }
}

impl ProviderCredentialRefresher for CodexSubscriptionCredentialRefresher {
    async fn refresh(
        &self,
        provider_type: &ProviderType,
        account_id: &ProviderAccountId,
        current: SecretValue,
    ) -> Result<SecretValue, ProviderCredentialRefreshError> {
        if provider_type != &self.provider_type || account_id != &self.account_id {
            return Err(ProviderCredentialRefreshError::Transient);
        }
        self.refresh_credential(current).await
    }
}

/// Creates the versioned vault payload produced by a future login flow.
///
/// The ChatGPT account claim is read as trusted identity metadata from the JWT
/// payload. This validates account binding but does not verify the JWT signature.
pub fn encode_codex_subscription_credential(
    chatgpt_account_id: impl Into<String>,
    id_token: impl Into<String>,
    access_token: impl Into<String>,
    refresh_token: impl Into<String>,
) -> Result<SecretValue, CodexSubscriptionCredentialError> {
    CredentialEnvelope {
        version: CREDENTIAL_VERSION,
        chatgpt_account_id: chatgpt_account_id.into(),
        id_token: id_token.into(),
        access_token: access_token.into(),
        refresh_token: refresh_token.into(),
    }
    .validated()?
    .encode()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexSubscriptionCredentialError {
    InvalidProviderBinding,
    InvalidEnvelope,
    InvalidAccountBinding,
    InvalidTransportLimits,
    TransportUnavailable,
}

#[derive(Serialize, Deserialize)]
struct CredentialEnvelope {
    version: u32,
    chatgpt_account_id: String,
    id_token: String,
    access_token: String,
    refresh_token: String,
}

impl CredentialEnvelope {
    fn decode(bytes: &[u8]) -> Result<Self, CodexSubscriptionCredentialError> {
        serde_json::from_slice::<Self>(bytes)
            .map_err(|_| CodexSubscriptionCredentialError::InvalidEnvelope)?
            .validated()
    }

    fn validated(self) -> Result<Self, CodexSubscriptionCredentialError> {
        if self.version != CREDENTIAL_VERSION
            || !valid_account_id(&self.chatgpt_account_id)
            || !valid_token(&self.id_token)
            || !valid_token(&self.access_token)
            || !valid_token(&self.refresh_token)
        {
            return Err(CodexSubscriptionCredentialError::InvalidEnvelope);
        }
        validate_id_token_account(&self.id_token, &self.chatgpt_account_id)?;
        Ok(self)
    }

    fn merge(&mut self, refreshed: RefreshResponse) -> Result<(), ProviderCredentialRefreshError> {
        match refreshed.id_token {
            TokenUpdate::Missing => {}
            TokenUpdate::Invalid => return Err(ProviderCredentialRefreshError::Permanent),
            TokenUpdate::Present(id_token) => {
                if !valid_token(&id_token)
                    || validate_id_token_account(&id_token, &self.chatgpt_account_id).is_err()
                {
                    return Err(ProviderCredentialRefreshError::Permanent);
                }
                self.id_token = id_token;
            }
        }
        match refreshed.access_token {
            TokenUpdate::Missing => {}
            TokenUpdate::Invalid => return Err(ProviderCredentialRefreshError::Transient),
            TokenUpdate::Present(access_token) => {
                if !valid_token(&access_token) {
                    return Err(ProviderCredentialRefreshError::Transient);
                }
                self.access_token = access_token;
            }
        }
        match refreshed.refresh_token {
            TokenUpdate::Missing => {}
            TokenUpdate::Invalid => return Err(ProviderCredentialRefreshError::Transient),
            TokenUpdate::Present(refresh_token) => {
                if !valid_token(&refresh_token) {
                    return Err(ProviderCredentialRefreshError::Transient);
                }
                self.refresh_token = refresh_token;
            }
        }
        Ok(())
    }

    fn encode(self) -> Result<SecretValue, CodexSubscriptionCredentialError> {
        let bytes = serde_json::to_vec(&self)
            .map_err(|_| CodexSubscriptionCredentialError::InvalidEnvelope)?;
        SecretValue::new(bytes).map_err(|_| CodexSubscriptionCredentialError::InvalidEnvelope)
    }
}

#[derive(Serialize)]
struct RefreshRequest<'a> {
    grant_type: &'static str,
    client_id: &'static str,
    refresh_token: &'a str,
}

#[derive(Deserialize)]
struct RefreshResponse {
    #[serde(default)]
    id_token: TokenUpdate,
    #[serde(default)]
    access_token: TokenUpdate,
    #[serde(default)]
    refresh_token: TokenUpdate,
}

#[derive(Default)]
enum TokenUpdate {
    #[default]
    Missing,
    Invalid,
    Present(String),
}

impl<'de> Deserialize<'de> for TokenUpdate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(match Option::<String>::deserialize(deserializer)? {
            Some(value) => Self::Present(value),
            None => Self::Invalid,
        })
    }
}

#[derive(Deserialize)]
struct RefreshErrorResponse {
    error: Option<String>,
}

async fn read_bounded_body(
    mut response: reqwest::Response,
    max_response_bytes: usize,
) -> Result<Vec<u8>, ProviderCredentialRefreshError> {
    if response
        .content_length()
        .is_some_and(|length| length > max_response_bytes as u64)
    {
        return Err(ProviderCredentialRefreshError::Transient);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| ProviderCredentialRefreshError::Transient)?
    {
        if body.len().saturating_add(chunk.len()) > max_response_bytes {
            return Err(ProviderCredentialRefreshError::Transient);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn valid_account_id(value: &str) -> bool {
    !value.is_empty()
        && value.trim() == value
        && value.len() <= MAX_ACCOUNT_ID_BYTES
        && !value.chars().any(char::is_control)
}

fn valid_token(value: &str) -> bool {
    !value.is_empty()
        && value.trim() == value
        && value.len() <= MAX_TOKEN_BYTES
        && !value.chars().any(char::is_control)
}

fn is_permanent_error_code(code: &str) -> bool {
    [
        "refresh_token_expired",
        "refresh_token_reused",
        "refresh_token_invalidated",
    ]
    .iter()
    .any(|expected| code.eq_ignore_ascii_case(expected))
}

fn validate_id_token_account(
    token: &str,
    expected_account_id: &str,
) -> Result<(), CodexSubscriptionCredentialError> {
    let account_id = extract_id_token_account(token)?;
    if account_id != expected_account_id {
        return Err(CodexSubscriptionCredentialError::InvalidAccountBinding);
    }
    Ok(())
}

pub(crate) fn extract_id_token_account(
    token: &str,
) -> Result<String, CodexSubscriptionCredentialError> {
    let mut parts = token.split('.');
    let (header, payload, signature) =
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(header), Some(payload), Some(signature), None)
                if !header.is_empty() && !payload.is_empty() && !signature.is_empty() =>
            {
                (header, payload, signature)
            }
            _ => return Err(CodexSubscriptionCredentialError::InvalidEnvelope),
        };
    let _ = (header, signature);
    let payload = decode_base64_url(payload)?;
    let claims: Value = serde_json::from_slice(&payload)
        .map_err(|_| CodexSubscriptionCredentialError::InvalidEnvelope)?;
    claims
        .get("https://api.openai.com/auth")
        .and_then(|auth| auth.get("chatgpt_account_id"))
        .and_then(Value::as_str)
        .filter(|account_id| valid_account_id(account_id))
        .ok_or(CodexSubscriptionCredentialError::InvalidAccountBinding)
        .map(str::to_owned)
}

fn decode_base64_url(value: &str) -> Result<Vec<u8>, CodexSubscriptionCredentialError> {
    let mut output = Vec::with_capacity(value.len().saturating_mul(3) / 4);
    let mut accumulator = 0_u32;
    let mut bits = 0_u8;
    for byte in value.bytes() {
        let digit = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return Err(CodexSubscriptionCredentialError::InvalidEnvelope),
        };
        accumulator = (accumulator << 6) | u32::from(digit);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            output.push((accumulator >> bits) as u8);
            accumulator &= (1_u32 << bits).saturating_sub(1);
        }
    }
    if bits >= 6 || accumulator != 0 {
        return Err(CodexSubscriptionCredentialError::InvalidEnvelope);
    }
    Ok(output)
}
