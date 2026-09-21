use std::{fmt, future::Future, time::Duration};

use reqwest::{Client, StatusCode, redirect::Policy};
use serde::{Deserialize, Deserializer, Serialize, de};
use tokio::{sync::watch, time::Instant};

use crate::{
    CodexSubscriptionCredentialError, CodexSubscriptionRefreshLimits,
    encode_codex_subscription_credential, extract_id_token_account,
};
use kiln_core::SecretValue;

const ISSUER: &str = "https://auth.openai.com";
const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const DEVICE_LOGIN_LIFETIME: Duration = Duration::from_secs(15 * 60);

pub struct CodexDeviceLoginClient {
    client: Client,
    max_response_bytes: usize,
}

impl fmt::Debug for CodexDeviceLoginClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexDeviceLoginClient")
            .finish_non_exhaustive()
    }
}

impl CodexDeviceLoginClient {
    pub fn new() -> Result<Self, CodexDeviceLoginError> {
        Self::with_limits(CodexSubscriptionRefreshLimits::default())
    }

    pub fn with_limits(
        limits: CodexSubscriptionRefreshLimits,
    ) -> Result<Self, CodexDeviceLoginError> {
        if limits.connect_timeout.is_zero()
            || limits.request_timeout.is_zero()
            || limits.max_response_bytes == 0
            || limits.max_response_bytes > 1024 * 1024
        {
            return Err(CodexDeviceLoginError::InvalidTransportLimits);
        }
        let client = Client::builder()
            .redirect(Policy::none())
            .connect_timeout(limits.connect_timeout)
            .timeout(limits.request_timeout)
            .build()
            .map_err(|_| CodexDeviceLoginError::Transport)?;
        Ok(Self {
            client,
            max_response_bytes: limits.max_response_bytes,
        })
    }

    pub async fn begin(&self) -> Result<CodexDeviceAuthorization, CodexDeviceLoginError> {
        let response = self
            .client
            .post(format!("{ISSUER}/api/accounts/deviceauth/usercode"))
            .json(&UserCodeRequest {
                client_id: CLIENT_ID,
            })
            .send()
            .await
            .map_err(|_| CodexDeviceLoginError::Transport)?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(CodexDeviceLoginError::Unavailable);
        }
        if !response.status().is_success() {
            return Err(CodexDeviceLoginError::Rejected);
        }
        let body = read_body(response, self.max_response_bytes).await?;
        let response: UserCodeResponse =
            serde_json::from_slice(&body).map_err(|_| CodexDeviceLoginError::InvalidResponse)?;
        if !valid_field(&response.device_auth_id)
            || !valid_field(&response.user_code)
            || response.interval == 0
            || response.interval > DEVICE_LOGIN_LIFETIME.as_secs()
        {
            return Err(CodexDeviceLoginError::InvalidResponse);
        }
        Ok(CodexDeviceAuthorization {
            verification_url: format!("{ISSUER}/codex/device"),
            user_code: response.user_code,
            state: DeviceAuthorizationState {
                device_auth_id: response.device_auth_id,
                interval: Duration::from_secs(response.interval),
                deadline: Instant::now() + DEVICE_LOGIN_LIFETIME,
            },
        })
    }

    pub async fn complete(
        &self,
        authorization: CodexDeviceAuthorization,
        mut cancellation: watch::Receiver<bool>,
    ) -> Result<CodexDeviceLoginResult, CodexDeviceLoginError> {
        let state = authorization.state;
        let authorization_code = loop {
            let response = until_deadline(
                state.deadline,
                &mut cancellation,
                self.client
                    .post(format!("{ISSUER}/api/accounts/deviceauth/token"))
                    .json(&TokenPollRequest {
                        device_auth_id: &state.device_auth_id,
                        user_code: &authorization.user_code,
                    })
                    .send(),
            )
            .await?
            .map_err(|_| CodexDeviceLoginError::Transport)?;
            let status = response.status();
            if status.is_success() {
                let body = until_deadline(
                    state.deadline,
                    &mut cancellation,
                    read_body(response, self.max_response_bytes),
                )
                .await??;
                break serde_json::from_slice::<AuthorizationCodeResponse>(&body)
                    .map_err(|_| CodexDeviceLoginError::InvalidResponse)?;
            }
            if status != StatusCode::FORBIDDEN && status != StatusCode::NOT_FOUND {
                return Err(CodexDeviceLoginError::Rejected);
            }
            until_deadline(
                state.deadline,
                &mut cancellation,
                tokio::time::sleep(state.interval),
            )
            .await?;
        };
        if !valid_field(&authorization_code.authorization_code)
            || !valid_field(&authorization_code.code_challenge)
            || !valid_field(&authorization_code.code_verifier)
        {
            return Err(CodexDeviceLoginError::InvalidResponse);
        }
        let response = until_deadline(
            state.deadline,
            &mut cancellation,
            self.client
                .post(format!("{ISSUER}/oauth/token"))
                .form(&TokenExchangeRequest {
                    grant_type: "authorization_code",
                    client_id: CLIENT_ID,
                    code: &authorization_code.authorization_code,
                    redirect_uri: &format!("{ISSUER}/deviceauth/callback"),
                    code_verifier: &authorization_code.code_verifier,
                })
                .send(),
        )
        .await?
        .map_err(|_| CodexDeviceLoginError::Transport)?;
        if !response.status().is_success() {
            return Err(CodexDeviceLoginError::Rejected);
        }
        let body = until_deadline(
            state.deadline,
            &mut cancellation,
            read_body(response, self.max_response_bytes),
        )
        .await??;
        let tokens: TokenExchangeResponse =
            serde_json::from_slice(&body).map_err(|_| CodexDeviceLoginError::InvalidResponse)?;
        let account_id = extract_id_token_account(&tokens.id_token)
            .map_err(|_| CodexDeviceLoginError::InvalidAccountBinding)?;
        let secret = encode_codex_subscription_credential(
            account_id.clone(),
            tokens.id_token,
            tokens.access_token,
            tokens.refresh_token,
        )
        .map_err(map_credential_error)?;
        Ok(CodexDeviceLoginResult {
            chatgpt_account_id: account_id,
            secret,
        })
    }
}

pub struct CodexDeviceAuthorization {
    verification_url: String,
    user_code: String,
    state: DeviceAuthorizationState,
}

impl CodexDeviceAuthorization {
    pub fn verification_url(&self) -> &str {
        &self.verification_url
    }
    pub fn user_code(&self) -> &str {
        &self.user_code
    }
}

struct DeviceAuthorizationState {
    device_auth_id: String,
    interval: Duration,
    deadline: Instant,
}

pub struct CodexDeviceLoginResult {
    chatgpt_account_id: String,
    secret: SecretValue,
}

impl CodexDeviceLoginResult {
    pub fn chatgpt_account_id(&self) -> &str {
        &self.chatgpt_account_id
    }
    pub fn into_secret(self) -> SecretValue {
        self.secret
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexDeviceLoginError {
    Cancelled,
    Expired,
    Unavailable,
    Rejected,
    Transport,
    InvalidResponse,
    InvalidAccountBinding,
    InvalidTransportLimits,
}

#[derive(Serialize)]
struct UserCodeRequest<'a> {
    client_id: &'a str,
}

#[derive(Deserialize)]
struct UserCodeResponse {
    device_auth_id: String,
    #[serde(alias = "usercode")]
    user_code: String,
    #[serde(deserialize_with = "deserialize_interval")]
    interval: u64,
}

fn deserialize_interval<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer)?
        .trim()
        .parse()
        .map_err(de::Error::custom)
}

#[derive(Serialize)]
struct TokenPollRequest<'a> {
    device_auth_id: &'a str,
    user_code: &'a str,
}

#[derive(Deserialize)]
struct AuthorizationCodeResponse {
    authorization_code: String,
    code_challenge: String,
    code_verifier: String,
}

#[derive(Serialize)]
struct TokenExchangeRequest<'a> {
    grant_type: &'static str,
    client_id: &'static str,
    code: &'a str,
    redirect_uri: &'a str,
    code_verifier: &'a str,
}

#[derive(Deserialize)]
struct TokenExchangeResponse {
    id_token: String,
    access_token: String,
    refresh_token: String,
}

async fn until_deadline<F, T>(
    deadline: Instant,
    cancellation: &mut watch::Receiver<bool>,
    future: F,
) -> Result<T, CodexDeviceLoginError>
where
    F: Future<Output = T>,
{
    if *cancellation.borrow() {
        return Err(CodexDeviceLoginError::Cancelled);
    }
    tokio::select! {
        _ = cancellation.changed() => Err(CodexDeviceLoginError::Cancelled),
        result = tokio::time::timeout_at(deadline, future) => result.map_err(|_| CodexDeviceLoginError::Expired),
    }
}

async fn read_body(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, CodexDeviceLoginError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(CodexDeviceLoginError::InvalidResponse);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| CodexDeviceLoginError::Transport)?
    {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(CodexDeviceLoginError::InvalidResponse);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn valid_field(value: &str) -> bool {
    !value.is_empty()
        && value.trim() == value
        && value.len() <= 256 * 1024
        && !value.chars().any(char::is_control)
}

fn map_credential_error(error: CodexSubscriptionCredentialError) -> CodexDeviceLoginError {
    match error {
        CodexSubscriptionCredentialError::InvalidAccountBinding => {
            CodexDeviceLoginError::InvalidAccountBinding
        }
        _ => CodexDeviceLoginError::InvalidResponse,
    }
}
