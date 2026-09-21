//! Native ChatGPT PKCE. The verifier and callback code remain in memory until
//! the attempt is consumed. Only the authorization URL leaves this adapter.

use std::{fmt, net::Ipv4Addr, time::Duration};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::Url;
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    time::Instant,
};

use crate::device_code_auth::{CLIENT_ID, ISSUER, until_deadline};
use crate::{CodexDeviceLoginClient, CodexDeviceLoginError, CodexDeviceLoginResult};

// Matches Kiln's device-login window. A listener cannot survive its attempt.
const LOGIN_LIFETIME: Duration = Duration::from_secs(15 * 60);
// A loopback redirect has no body. Bound untrusted headers and slow local peers;
// oversized requests are rejected rather than allocating without a ceiling.
const MAX_CALLBACK_BYTES: usize = 16 * 1024;
const CALLBACK_IO_TIMEOUT: Duration = Duration::from_secs(5);

pub struct CodexBrowserLoginClient {
    transport: CodexDeviceLoginClient,
}

impl fmt::Debug for CodexBrowserLoginClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexBrowserLoginClient")
            .finish_non_exhaustive()
    }
}

impl CodexBrowserLoginClient {
    pub fn new() -> Result<Self, CodexBrowserLoginError> {
        Ok(Self {
            transport: CodexDeviceLoginClient::new().map_err(CodexBrowserLoginError::Login)?,
        })
    }

    pub async fn begin(&self) -> Result<CodexBrowserAuthorization, CodexBrowserLoginError> {
        // These are registered upstream redirect ports. Never cancel another
        // process occupying one, and never bind the callback on a public address.
        let listener = match TcpListener::bind((Ipv4Addr::LOCALHOST, 1455)).await {
            Ok(listener) => listener,
            Err(_) => TcpListener::bind((Ipv4Addr::LOCALHOST, 1457))
                .await
                .map_err(|_| CodexBrowserLoginError::CallbackUnavailable)?,
        };
        let port = listener
            .local_addr()
            .map_err(|_| CodexBrowserLoginError::CallbackUnavailable)?
            .port();
        let redirect_uri = format!("http://localhost:{port}/auth/callback");
        let verifier = random_value::<64>()?;
        let state = random_value::<32>()?;
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let mut url = Url::parse(&format!("{ISSUER}/oauth/authorize"))
            .map_err(|_| CodexBrowserLoginError::Login(CodexDeviceLoginError::InvalidResponse))?;
        url.query_pairs_mut().extend_pairs([
            ("response_type", "code"),
            ("client_id", CLIENT_ID),
            ("redirect_uri", redirect_uri.as_str()),
            ("scope", "openid profile email offline_access"),
            ("code_challenge", challenge.as_str()),
            ("code_challenge_method", "S256"),
            ("state", state.as_str()),
            ("id_token_add_organizations", "true"),
            ("codex_cli_simplified_flow", "true"),
            ("originator", "kiln"),
        ]);
        Ok(CodexBrowserAuthorization {
            authorization_url: url.into(),
            redirect_uri,
            verifier,
            state,
            listener,
            deadline: Instant::now() + LOGIN_LIFETIME,
        })
    }

    pub async fn complete(
        &self,
        authorization: CodexBrowserAuthorization,
        mut cancellation: watch::Receiver<bool>,
    ) -> Result<CodexDeviceLoginResult, CodexBrowserLoginError> {
        let code = until_deadline(
            authorization.deadline,
            &mut cancellation,
            authorization.receive_code(),
        )
        .await
        .map_err(CodexBrowserLoginError::Login)??;
        // Stop accepting callbacks before the one-shot code exchange. Transport
        // errors never retry a potentially consumed authorization code.
        drop(authorization.listener);
        self.transport
            .exchange_code(
                &code,
                &authorization.redirect_uri,
                &authorization.verifier,
                authorization.deadline,
                &mut cancellation,
            )
            .await
            .map_err(CodexBrowserLoginError::Login)
    }
}

pub struct CodexBrowserAuthorization {
    authorization_url: String,
    redirect_uri: String,
    verifier: String,
    state: String,
    listener: TcpListener,
    deadline: Instant,
}

impl CodexBrowserAuthorization {
    pub fn authorization_url(&self) -> &str {
        &self.authorization_url
    }

    async fn receive_code(&self) -> Result<String, CodexBrowserLoginError> {
        loop {
            let (mut stream, _) = self
                .listener
                .accept()
                .await
                .map_err(|_| CodexBrowserLoginError::CallbackUnavailable)?;
            let outcome =
                tokio::time::timeout(CALLBACK_IO_TIMEOUT, read_callback(&mut stream, self)).await;
            let (status, message, result) = match outcome {
                Ok(Some(Callback::Code(code))) => (
                    "200 OK",
                    "Authorization received. Return to Kiln to check sign-in status.",
                    Some(Ok(code)),
                ),
                Ok(Some(Callback::Rejected)) => (
                    "200 OK",
                    "Sign-in was declined. Return to Kiln to try again.",
                    Some(Err(CodexBrowserLoginError::Login(
                        CodexDeviceLoginError::Rejected,
                    ))),
                ),
                _ => (
                    "400 Bad Request",
                    "Invalid sign-in callback. You can return to Kiln and try again.",
                    None,
                ),
            };
            // No reflection of provider text, codes, state, or arbitrary URLs.
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nContent-Security-Policy: default-src 'none'; frame-ancestors 'none'\r\nConnection: close\r\n\r\n{message}",
                message.len()
            );
            let _ =
                tokio::time::timeout(CALLBACK_IO_TIMEOUT, stream.write_all(response.as_bytes()))
                    .await;
            if let Some(result) = result {
                return result;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexBrowserLoginError {
    CallbackUnavailable,
    RandomUnavailable,
    Login(CodexDeviceLoginError),
}

fn random_value<const N: usize>() -> Result<String, CodexBrowserLoginError> {
    let mut bytes = [0; N];
    getrandom::fill(&mut bytes).map_err(|_| CodexBrowserLoginError::RandomUnavailable)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

enum Callback {
    Code(String),
    Rejected,
}

async fn read_callback(
    stream: &mut TcpStream,
    authorization: &CodexBrowserAuthorization,
) -> Option<Callback> {
    let mut bytes = Vec::new();
    let mut chunk = [0; 1024];
    loop {
        let count = stream.read(&mut chunk).await.ok()?;
        if count == 0 || bytes.len() + count > MAX_CALLBACK_BYTES {
            return None;
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    let request = std::str::from_utf8(&bytes).ok()?;
    let (head, body) = request.split_once("\r\n\r\n")?;
    if !body.is_empty() {
        return None;
    }
    let mut lines = head.split("\r\n");
    let mut line = lines.next()?.split(' ');
    if line.next()? != "GET" {
        return None;
    }
    let target = line.next()?;
    if !matches!(line.next()?, "HTTP/1.1" | "HTTP/1.0")
        || line.next().is_some()
        || !target.starts_with("/auth/callback?")
        || target.contains('#')
    {
        return None;
    }
    let expected_host = format!(
        "localhost:{}",
        authorization.listener.local_addr().ok()?.port()
    );
    let mut host = None;
    for header in lines {
        let (name, value) = header.split_once(':')?;
        if name.eq_ignore_ascii_case("host") {
            if host.replace(value.trim()).is_some() {
                return None;
            }
        }
        if name.eq_ignore_ascii_case("transfer-encoding")
            || (name.eq_ignore_ascii_case("content-length") && value.trim() != "0")
        {
            return None;
        }
    }
    if host != Some(expected_host.as_str()) {
        return None;
    }
    let url = Url::parse(&format!("http://{expected_host}{target}")).ok()?;
    if url.path() != "/auth/callback" {
        return None;
    }
    let (mut state, mut code, mut error) = (None, None, None);
    for (key, value) in url.query_pairs() {
        let field = match key.as_ref() {
            "state" => &mut state,
            "code" => &mut code,
            "error" => &mut error,
            _ => continue,
        };
        if field.replace(value.into_owned()).is_some() {
            return None;
        }
    }
    // Validate even error callbacks before consuming the attempt. Duplicated or
    // unsolicited parameters never choose a code or terminate another login.
    if state.as_deref() != Some(authorization.state.as_str()) {
        return None;
    }
    match (code, error) {
        (Some(code), None) if !code.is_empty() && !code.chars().any(char::is_control) => {
            Some(Callback::Code(code))
        }
        (None, Some(error)) if !error.is_empty() => Some(Callback::Rejected),
        _ => None,
    }
}
