//! Typed HTTP and WebSocket client for the public Kiln daemon protocol.

mod configuration_sync;
pub use configuration_sync::{
    ConfigurationMasterPin, ConfigurationSyncClient, ConfigurationSyncError,
    ConfigurationSyncTimeouts,
};

use std::fmt;
use std::net::SocketAddr;

use futures_util::{SinkExt, StreamExt};
use kiln_protocol::{
    APPEND_MESSAGE_OPERATION_ID, ARTIFACT_PATH, ARTIFACT_SESSION_HEADER, ARTIFACTS_PATH,
    AppendMessageRequest, ApprovalDecisionRequest, ArtifactResponse,
    CANCEL_PROVIDER_ACCOUNT_LOGIN_OPERATION_ID, CANCEL_RUN_OPERATION_ID,
    CREATE_PROVIDER_ACCOUNT_OPERATION_ID, CREATE_SESSION_OPERATION_ID,
    CREATE_WORKSPACE_OPERATION_ID, ClientIdentity, CreateProviderAccountRequest,
    CreateWorkspaceRequest, DECIDE_APPROVAL_OPERATION_ID, EVENTS_WEBSOCKET_PATH,
    GET_ARTIFACT_OPERATION_ID, GET_PROVIDER_ACCOUNT_LOGIN_OPERATION_ID,
    GET_PROVIDER_ACCOUNT_OPERATION_ID, GET_RUN_OPERATION_ID, GET_SESSION_CHANGE_DIFF_OPERATION_ID,
    GET_SESSION_OPERATION_ID, GET_WORKSPACE_OPERATION_ID, IDEMPOTENCY_KEY_HEADER,
    LIST_PROVIDER_ACCOUNTS_OPERATION_ID, LIST_SESSION_CHANGES_OPERATION_ID,
    LIST_SESSION_EVENTS_OPERATION_ID, LIST_SESSION_RUNS_OPERATION_ID, LIST_SESSIONS_OPERATION_ID,
    LIST_USAGE_OPERATION_ID, LIST_WORKSPACES_OPERATION_ID, ListProviderAccountsResponse,
    ListSessionsResponse, ListWorkspacesResponse, MessageDeliveryResponse, MessageResponse,
    NEGOTIATE_OPERATION_ID, NEGOTIATE_PATH, NegotiateRequest, NegotiateResponse, PROTOCOL_VERSION,
    PROVIDER_ACCOUNT_LOGIN_ATTEMPT_PATH, PROVIDER_ACCOUNT_LOGIN_PATH, PROVIDER_ACCOUNT_PATH,
    PROVIDER_ACCOUNTS_PATH, ProblemDetails, ProviderAccountLoginResponse, ProviderAccountResponse,
    REACT_TO_RUN_ACTIVITY_OPERATION_ID, RUN_CANCEL_PATH, RUN_CHILDREN_PATH, RUN_INPUT_PATH,
    RUN_PATH, RUN_REACTIONS_PATH, ReactToRunActivityRequest, RunResponse,
    SEND_RUN_INPUT_OPERATION_ID, SESSION_CHANGE_DIFF_PATH, SESSION_CHANGES_PATH,
    SESSION_EVENTS_PATH, SESSION_MESSAGES_PATH, SESSION_PATH, SESSION_RUNS_PATH,
    START_CHILD_RUN_OPERATION_ID, START_PROVIDER_ACCOUNT_LOGIN_OPERATION_ID,
    START_RUN_OPERATION_ID, SessionChangeDiffResponse, SessionChangesResponse,
    SessionEventsResponse, SessionResponse, SessionRunsResponse, StartChildRunRequest,
    StartProviderAccountLoginResponse, StartRunRequest, TOOL_CALL_APPROVAL_PATH,
    UPLOAD_ARTIFACT_OPERATION_ID, USAGE_PATH, UsageLedgerResponse, WEBSOCKET_CAPABILITY,
    WORKSPACE_PATH, WORKSPACE_SESSIONS_PATH, WORKSPACES_PATH, WebSocketFrame, WorkspaceResponse,
};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio_tungstenite::tungstenite::{
    Message, client::IntoClientRequest, error::Error as WebSocketError,
    http::header::SEC_WEBSOCKET_PROTOCOL,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

/// Errors from local validation, transport, protocol decoding, or the Kiln API.
#[derive(Debug, Error)]
pub enum Error {
    #[error("the Kiln daemon address must be a loopback socket address with a nonzero port")]
    InvalidAddress,
    #[error("the bearer token is empty or cannot be sent safely in an HTTP header")]
    InvalidBearerToken,
    #[error("the idempotency key is empty or cannot be sent safely in an HTTP header")]
    InvalidIdempotencyKey,
    #[error("{name} must be a nonempty URL path segment other than `.` or `..`")]
    InvalidPathSegment { name: &'static str },
    #[error("HTTP transport failed during {operation}")]
    HttpTransport {
        operation: &'static str,
        #[source]
        source: reqwest::Error,
    },
    #[error("WebSocket transport failed")]
    WebSocketTransport(#[source] WebSocketError),
    #[error("the Kiln API returned HTTP {status}: {}", .problem.code)]
    Api {
        status: u16,
        problem: ProblemDetails,
    },
    #[error("the Kiln API returned an undocumented HTTP {status} response")]
    UnexpectedResponse { status: u16 },
    #[error("the Kiln API response for {operation} was not valid protocol JSON")]
    Decode {
        operation: &'static str,
        #[source]
        source: serde_json::Error,
    },
    #[error("the daemon selected protocol version {selected}; this client requires {expected}")]
    ProtocolVersion {
        selected: String,
        expected: &'static str,
    },
    #[error("the daemon did not negotiate the {0} capability")]
    MissingCapability(&'static str),
    #[error("the WebSocket server did not select the negotiated subprotocol")]
    WebSocketSubprotocol,
    #[error("the WebSocket sent an unsupported {kind} message")]
    UnexpectedWebSocketMessage { kind: &'static str },
    #[error("artifact preview limit must be greater than zero")]
    InvalidArtifactPreviewLimit,
    #[error("configuration snapshot response exceeds the transfer budget")]
    ConfigurationSnapshotTooLarge,
}

/// A downloaded immutable artifact and its public response metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactDownload {
    pub bytes: Vec<u8>,
    pub media_type: Option<String>,
    pub content_length: Option<u64>,
    pub etag: Option<String>,
}

/// A bounded artifact response for safe in-app previews.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactPreviewDownload {
    pub bytes: Vec<u8>,
    pub media_type: Option<String>,
    pub content_length: Option<u64>,
    pub etag: Option<String>,
    pub truncated: bool,
}

/// Client for one explicitly selected local Kiln daemon.
#[derive(Clone)]
pub struct Client {
    address: SocketAddr,
    websocket_protocols: tokio_tungstenite::tungstenite::http::HeaderValue,
    http: reqwest::Client,
}

impl fmt::Debug for Client {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Client")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Creates a client pinned to a loopback daemon address.
    pub fn new(address: SocketAddr, bearer_token: impl AsRef<str>) -> Result<Self, Error> {
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err(Error::InvalidAddress);
        }

        let bearer_token = bearer_token.as_ref();
        if bearer_token.is_empty() || !bearer_token.bytes().all(is_http_token_byte) {
            return Err(Error::InvalidBearerToken);
        }

        let mut authorization = HeaderValue::from_str(&format!("Bearer {bearer_token}"))
            .map_err(|_| Error::InvalidBearerToken)?;
        authorization.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, authorization);

        let mut websocket_protocols = tokio_tungstenite::tungstenite::http::HeaderValue::from_str(
            &format!("{WEBSOCKET_CAPABILITY}, kiln.auth.{bearer_token}"),
        )
        .map_err(|_| Error::InvalidBearerToken)?;
        websocket_protocols.set_sensitive(true);

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(|source| Error::HttpTransport {
                operation: "build_client",
                source,
            })?;

        Ok(Self {
            address,
            websocket_protocols,
            http,
        })
    }

    /// Negotiates the single protocol version and event capability supported by this crate.
    pub async fn negotiate(&self, client: ClientIdentity) -> Result<NegotiateResponse, Error> {
        let request = NegotiateRequest {
            min_version: PROTOCOL_VERSION.to_owned(),
            max_version: PROTOCOL_VERSION.to_owned(),
            client,
            requested_capabilities: vec![WEBSOCKET_CAPABILITY.to_owned()],
        };
        let response: NegotiateResponse = self
            .send_json(
                NEGOTIATE_OPERATION_ID,
                self.http.post(self.http_url(NEGOTIATE_PATH)).json(&request),
            )
            .await?;
        self.validate_negotiated_version(&response)?;
        Ok(response)
    }

    pub async fn create_workspace(
        &self,
        request: &CreateWorkspaceRequest,
    ) -> Result<WorkspaceResponse, Error> {
        self.send_json(
            CREATE_WORKSPACE_OPERATION_ID,
            self.http.post(self.http_url(WORKSPACES_PATH)).json(request),
        )
        .await
    }

    pub async fn list_workspaces(&self) -> Result<ListWorkspacesResponse, Error> {
        self.send_json(
            LIST_WORKSPACES_OPERATION_ID,
            self.http.get(self.http_url(WORKSPACES_PATH)),
        )
        .await
    }

    pub async fn get_workspace(&self, workspace_id: &str) -> Result<WorkspaceResponse, Error> {
        let path = path_with_segment(
            WORKSPACE_PATH,
            "{workspace_id}",
            "workspace_id",
            workspace_id,
        )?;
        self.send_json(
            GET_WORKSPACE_OPERATION_ID,
            self.http.get(self.http_url(&path)),
        )
        .await
    }

    pub async fn create_provider_account(
        &self,
        idempotency_key: &str,
        request: &CreateProviderAccountRequest,
    ) -> Result<ProviderAccountResponse, Error> {
        self.send_json(
            CREATE_PROVIDER_ACCOUNT_OPERATION_ID,
            with_idempotency_key(
                self.http.post(self.http_url(PROVIDER_ACCOUNTS_PATH)),
                idempotency_key,
            )?
            .json(request),
        )
        .await
    }

    pub async fn get_configuration_sync_status(
        &self,
    ) -> Result<kiln_protocol::ConfigurationSyncStatusResponse, Error> {
        self.send_json(
            kiln_protocol::GET_CONFIGURATION_SYNC_STATUS_OPERATION_ID,
            self.http
                .get(self.http_url(kiln_protocol::CONFIGURATION_SYNC_STATUS_PATH)),
        )
        .await
    }

    /// Read the current authority and its managed identity setup metadata.
    /// This does not read private keys or establish remote transport readiness.
    pub async fn get_configuration_identity_status(
        &self,
    ) -> Result<kiln_protocol::ConfigurationIdentityStatusResponse, Error> {
        self.send_json(
            kiln_protocol::GET_CONFIGURATION_IDENTITY_STATUS_OPERATION_ID,
            self.http
                .get(self.http_url(kiln_protocol::CONFIGURATION_IDENTITY_STATUS_PATH)),
        )
        .await
    }

    /// Explicitly create a managed master identity. Preserve the exact key and
    /// request for ambiguous retries, and reload identity status after success.
    pub async fn configure_master_identity(
        &self,
        idempotency_key: &str,
        request: &kiln_protocol::ConfigureMasterIdentityRequest,
    ) -> Result<kiln_protocol::ConfigurationIdentitySetupResponse, Error> {
        // The same key later travels in JSON for cleanup. Exclude HTTP optional
        // whitespace so transport normalization cannot change its identity.
        if idempotency_key.is_empty()
            || !idempotency_key.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(Error::InvalidIdempotencyKey);
        }
        self.send_json(
            kiln_protocol::CONFIGURE_MASTER_IDENTITY_OPERATION_ID,
            with_idempotency_key(
                self.http
                    .post(self.http_url(kiln_protocol::CONFIGURATION_IDENTITY_STATUS_PATH)),
                idempotency_key,
            )?
            .json(request),
        )
        .await
    }

    /// Permanently retire the exact identity and delete its vault keys. Retrying
    /// this same request is safe after an uncertain result or cleanup failure.
    pub async fn retire_master_identity(
        &self,
        request: &kiln_protocol::RetireMasterIdentityRequest,
    ) -> Result<(), Error> {
        let operation = kiln_protocol::RETIRE_MASTER_IDENTITY_OPERATION_ID;
        let response = self
            .http
            .post(self.http_url(kiln_protocol::CONFIGURATION_IDENTITY_RETIRE_PATH))
            .json(request)
            .send()
            .await
            .map_err(|source| Error::HttpTransport { operation, source })?;
        let response = successful_response(response).await?;
        if response.status() != reqwest::StatusCode::NO_CONTENT {
            return Err(Error::UnexpectedResponse {
                status: response.status().as_u16(),
            });
        }
        Ok(())
    }

    /// Export the verified stored bundle. Both success and error bodies are
    /// bounded while streaming; an oversized response is never truncated into JSON.
    pub async fn get_configuration_snapshot(
        &self,
    ) -> Result<kiln_protocol::ConfigurationSnapshotResponse, Error> {
        let operation = kiln_protocol::GET_CONFIGURATION_SNAPSHOT_OPERATION_ID;
        let cap = kiln_protocol::CONFIGURATION_PUBLICATION_MAX_BYTES;
        let response = self
            .http
            .get(self.http_url(kiln_protocol::CONFIGURATION_SNAPSHOT_PATH))
            .send()
            .await
            .map_err(|source| Error::HttpTransport { operation, source })?;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|bytes| bytes > cap as u64)
        {
            return Err(Error::ConfigurationSnapshotTooLarge);
        }
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|source| Error::HttpTransport { operation, source })?;
            if chunk.len() > cap.saturating_sub(bytes.len()) {
                return Err(Error::ConfigurationSnapshotTooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            return match serde_json::from_slice(&bytes) {
                Ok(problem) => Err(Error::Api {
                    status: status.as_u16(),
                    problem,
                }),
                Err(_) => Err(Error::UnexpectedResponse {
                    status: status.as_u16(),
                }),
            };
        }
        serde_json::from_slice(&bytes).map_err(|source| Error::Decode { operation, source })
    }

    /// Publish a complete explicitly prepared bundle. Exact retries return the
    /// original receipt; reload status to learn the current stored revision.
    pub async fn publish_configuration_snapshot(
        &self,
        idempotency_key: &str,
        request: &kiln_protocol::PublishConfigurationSnapshotRequest,
    ) -> Result<kiln_protocol::ConfigurationPublicationResponse, Error> {
        self.send_json(
            kiln_protocol::PUBLISH_CONFIGURATION_OPERATION_ID,
            with_idempotency_key(
                self.http
                    .post(self.http_url(kiln_protocol::CONFIGURATION_PUBLICATIONS_PATH)),
                idempotency_key,
            )?
            .json(request),
        )
        .await
    }

    /// Exact retries return the original designation receipt, not current status.
    pub async fn designate_configuration_master(
        &self,
        idempotency_key: &str,
        request: &kiln_protocol::DesignateConfigurationMasterRequest,
    ) -> Result<kiln_protocol::ConfigurationMasterDesignationResponse, Error> {
        self.send_json(
            kiln_protocol::DESIGNATE_CONFIGURATION_MASTER_OPERATION_ID,
            with_idempotency_key(
                self.http
                    .post(self.http_url(kiln_protocol::CONFIGURATION_MASTER_PATH)),
                idempotency_key,
            )?
            .json(request),
        )
        .await
    }

    pub async fn list_provider_accounts(&self) -> Result<ListProviderAccountsResponse, Error> {
        self.send_json(
            LIST_PROVIDER_ACCOUNTS_OPERATION_ID,
            self.http.get(self.http_url(PROVIDER_ACCOUNTS_PATH)),
        )
        .await
    }

    pub async fn get_provider_account(
        &self,
        provider_account_id: &str,
    ) -> Result<ProviderAccountResponse, Error> {
        let path = path_with_segment(
            PROVIDER_ACCOUNT_PATH,
            "{provider_account_id}",
            "provider_account_id",
            provider_account_id,
        )?;
        self.send_json(
            GET_PROVIDER_ACCOUNT_OPERATION_ID,
            self.http.get(self.http_url(&path)),
        )
        .await
    }

    /// Disconnects the addressed account locally. The caller decides whether
    /// to retry; a later request also disconnects a newly reconnected account.
    pub async fn disconnect_provider_account(
        &self,
        provider_account_id: &str,
    ) -> Result<ProviderAccountResponse, Error> {
        let path = path_with_segment(
            kiln_protocol::PROVIDER_ACCOUNT_DISCONNECT_PATH,
            "{provider_account_id}",
            "provider_account_id",
            provider_account_id,
        )?;
        self.send_json(
            kiln_protocol::DISCONNECT_PROVIDER_ACCOUNT_OPERATION_ID,
            self.http.post(self.http_url(&path)),
        )
        .await
    }

    pub async fn start_provider_account_login(
        &self,
        provider_account_id: &str,
    ) -> Result<StartProviderAccountLoginResponse, Error> {
        let path = path_with_segment(
            PROVIDER_ACCOUNT_LOGIN_PATH,
            "{provider_account_id}",
            "provider_account_id",
            provider_account_id,
        )?;
        self.send_json(
            START_PROVIDER_ACCOUNT_LOGIN_OPERATION_ID,
            self.http.post(self.http_url(&path)),
        )
        .await
    }

    pub async fn start_provider_account_browser_login(
        &self,
        provider_account_id: &str,
    ) -> Result<kiln_protocol::StartProviderAccountBrowserLoginResponse, Error> {
        let path = path_with_segment(
            kiln_protocol::PROVIDER_ACCOUNT_BROWSER_LOGIN_PATH,
            "{provider_account_id}",
            "provider_account_id",
            provider_account_id,
        )?;
        self.send_json(
            kiln_protocol::START_PROVIDER_ACCOUNT_BROWSER_LOGIN_OPERATION_ID,
            self.http.post(self.http_url(&path)),
        )
        .await
    }

    pub async fn get_provider_account_login(
        &self,
        provider_account_id: &str,
        attempt_id: &str,
    ) -> Result<ProviderAccountLoginResponse, Error> {
        let path = path_with_segments(
            PROVIDER_ACCOUNT_LOGIN_ATTEMPT_PATH,
            &[
                (
                    "{provider_account_id}",
                    "provider_account_id",
                    provider_account_id,
                ),
                ("{attempt_id}", "attempt_id", attempt_id),
            ],
        )?;
        self.send_json(
            GET_PROVIDER_ACCOUNT_LOGIN_OPERATION_ID,
            self.http.get(self.http_url(&path)),
        )
        .await
    }

    pub async fn cancel_provider_account_login(
        &self,
        provider_account_id: &str,
        attempt_id: &str,
    ) -> Result<ProviderAccountLoginResponse, Error> {
        let path = path_with_segments(
            PROVIDER_ACCOUNT_LOGIN_ATTEMPT_PATH,
            &[
                (
                    "{provider_account_id}",
                    "provider_account_id",
                    provider_account_id,
                ),
                ("{attempt_id}", "attempt_id", attempt_id),
            ],
        )?;
        self.send_json(
            CANCEL_PROVIDER_ACCOUNT_LOGIN_OPERATION_ID,
            self.http.post(self.http_url(&path)),
        )
        .await
    }

    pub async fn create_session(&self, workspace_id: &str) -> Result<SessionResponse, Error> {
        let path = path_with_segment(
            WORKSPACE_SESSIONS_PATH,
            "{workspace_id}",
            "workspace_id",
            workspace_id,
        )?;
        self.send_json(
            CREATE_SESSION_OPERATION_ID,
            self.http.post(self.http_url(&path)),
        )
        .await
    }

    pub async fn list_sessions(&self, workspace_id: &str) -> Result<ListSessionsResponse, Error> {
        let path = path_with_segment(
            WORKSPACE_SESSIONS_PATH,
            "{workspace_id}",
            "workspace_id",
            workspace_id,
        )?;
        self.send_json(
            LIST_SESSIONS_OPERATION_ID,
            self.http.get(self.http_url(&path)),
        )
        .await
    }

    pub async fn get_session(&self, session_id: &str) -> Result<SessionResponse, Error> {
        let path = path_with_segment(SESSION_PATH, "{session_id}", "session_id", session_id)?;
        self.send_json(
            GET_SESSION_OPERATION_ID,
            self.http.get(self.http_url(&path)),
        )
        .await
    }

    pub async fn list_session_changes(
        &self,
        session_id: &str,
    ) -> Result<SessionChangesResponse, Error> {
        let path = path_with_segment(
            SESSION_CHANGES_PATH,
            "{session_id}",
            "session_id",
            session_id,
        )?;
        self.send_json(
            LIST_SESSION_CHANGES_OPERATION_ID,
            self.http.get(self.http_url(&path)),
        )
        .await
    }

    pub async fn get_session_change_diff(
        &self,
        session_id: &str,
        path: &str,
    ) -> Result<SessionChangeDiffResponse, Error> {
        let route = path_with_segment(
            SESSION_CHANGE_DIFF_PATH,
            "{session_id}",
            "session_id",
            session_id,
        )?;
        let mut url = self.http_url(&route);
        url.query_pairs_mut().append_pair("path", path);
        self.send_json(GET_SESSION_CHANGE_DIFF_OPERATION_ID, self.http.get(url))
            .await
    }

    pub async fn append_message(
        &self,
        session_id: &str,
        idempotency_key: &str,
        request: &AppendMessageRequest,
    ) -> Result<MessageResponse, Error> {
        let path = path_with_segment(
            SESSION_MESSAGES_PATH,
            "{session_id}",
            "session_id",
            session_id,
        )?;
        self.send_json(
            APPEND_MESSAGE_OPERATION_ID,
            with_idempotency_key(self.http.post(self.http_url(&path)), idempotency_key)?
                .json(request),
        )
        .await
    }

    pub async fn start_run(
        &self,
        session_id: &str,
        idempotency_key: &str,
        request: &StartRunRequest,
    ) -> Result<RunResponse, Error> {
        let path = path_with_segment(SESSION_RUNS_PATH, "{session_id}", "session_id", session_id)?;
        self.send_json(
            START_RUN_OPERATION_ID,
            with_idempotency_key(self.http.post(self.http_url(&path)), idempotency_key)?
                .json(request),
        )
        .await
    }

    pub async fn start_child_run(
        &self,
        parent_run_id: &str,
        idempotency_key: &str,
        request: &StartChildRunRequest,
    ) -> Result<RunResponse, Error> {
        let path = path_with_segment(
            RUN_CHILDREN_PATH,
            "{parent_run_id}",
            "parent_run_id",
            parent_run_id,
        )?;
        self.send_json(
            START_CHILD_RUN_OPERATION_ID,
            with_idempotency_key(self.http.post(self.http_url(&path)), idempotency_key)?
                .json(request),
        )
        .await
    }

    pub async fn list_session_runs(&self, session_id: &str) -> Result<SessionRunsResponse, Error> {
        let path = path_with_segment(SESSION_RUNS_PATH, "{session_id}", "session_id", session_id)?;
        self.send_json(
            LIST_SESSION_RUNS_OPERATION_ID,
            self.http.get(self.http_url(&path)),
        )
        .await
    }

    pub async fn get_run(&self, run_id: &str) -> Result<RunResponse, Error> {
        let path = path_with_segment(RUN_PATH, "{run_id}", "run_id", run_id)?;
        self.send_json(GET_RUN_OPERATION_ID, self.http.get(self.http_url(&path)))
            .await
    }

    pub async fn send_run_input(
        &self,
        run_id: &str,
        idempotency_key: &str,
        request: &kiln_protocol::SendRunInputRequest,
    ) -> Result<MessageDeliveryResponse, Error> {
        let path = path_with_segment(RUN_INPUT_PATH, "{run_id}", "run_id", run_id)?;
        self.send_json(
            SEND_RUN_INPUT_OPERATION_ID,
            with_idempotency_key(self.http.post(self.http_url(&path)), idempotency_key)?
                .json(request),
        )
        .await
    }

    pub async fn react_to_run_activity(
        &self,
        run_id: &str,
        idempotency_key: &str,
        request: &ReactToRunActivityRequest,
    ) -> Result<MessageDeliveryResponse, Error> {
        let path = path_with_segment(RUN_REACTIONS_PATH, "{run_id}", "run_id", run_id)?;
        self.send_json(
            REACT_TO_RUN_ACTIVITY_OPERATION_ID,
            with_idempotency_key(self.http.post(self.http_url(&path)), idempotency_key)?
                .json(request),
        )
        .await
    }

    pub async fn cancel_run(&self, run_id: &str) -> Result<RunResponse, Error> {
        let path = path_with_segment(RUN_CANCEL_PATH, "{run_id}", "run_id", run_id)?;
        self.send_json(
            CANCEL_RUN_OPERATION_ID,
            self.http.post(self.http_url(&path)),
        )
        .await
    }

    pub async fn decide_approval(
        &self,
        tool_call_id: &str,
        idempotency_key: &str,
        request: &ApprovalDecisionRequest,
    ) -> Result<RunResponse, Error> {
        let path = path_with_segment(
            TOOL_CALL_APPROVAL_PATH,
            "{tool_call_id}",
            "tool_call_id",
            tool_call_id,
        )?;
        self.send_json(
            DECIDE_APPROVAL_OPERATION_ID,
            with_idempotency_key(self.http.post(self.http_url(&path)), idempotency_key)?
                .json(request),
        )
        .await
    }

    pub async fn list_session_events(
        &self,
        session_id: &str,
        after: Option<&str>,
    ) -> Result<SessionEventsResponse, Error> {
        let path = path_with_segment(
            SESSION_EVENTS_PATH,
            "{session_id}",
            "session_id",
            session_id,
        )?;
        let mut url = self.http_url(&path);
        if let Some(after) = after {
            url.query_pairs_mut().append_pair("after", after);
        }
        self.send_json(LIST_SESSION_EVENTS_OPERATION_ID, self.http.get(url))
            .await
    }

    pub async fn list_usage(
        &self,
        after: Option<&str>,
        limit: Option<u64>,
    ) -> Result<UsageLedgerResponse, Error> {
        let mut url = self.http_url(USAGE_PATH);
        if let Some(after) = after {
            url.query_pairs_mut().append_pair("after", after);
        }
        if let Some(limit) = limit {
            url.query_pairs_mut()
                .append_pair("limit", &limit.to_string());
        }
        self.send_json(LIST_USAGE_OPERATION_ID, self.http.get(url))
            .await
    }

    pub async fn upload_artifact(
        &self,
        session_id: &str,
        bytes: Vec<u8>,
        media_type: &str,
    ) -> Result<ArtifactResponse, Error> {
        let request = self
            .http
            .post(self.http_url(ARTIFACTS_PATH))
            .header(ARTIFACT_SESSION_HEADER, session_id)
            .header(reqwest::header::CONTENT_TYPE, media_type)
            .body(bytes);
        self.send_json(UPLOAD_ARTIFACT_OPERATION_ID, request).await
    }

    pub async fn get_artifact(&self, content_hash: &str) -> Result<ArtifactDownload, Error> {
        let path = path_with_segment(
            ARTIFACT_PATH,
            "{content_hash}",
            "content_hash",
            content_hash,
        )?;
        let response = self
            .http
            .get(self.http_url(&path))
            .send()
            .await
            .map_err(|source| Error::HttpTransport {
                operation: GET_ARTIFACT_OPERATION_ID,
                source,
            })?;
        let response = successful_response(response).await?;
        let headers = response.headers();
        let media_type = header_text(headers, reqwest::header::CONTENT_TYPE);
        let content_length = headers
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
        let etag = header_text(headers, reqwest::header::ETAG);
        let bytes = response
            .bytes()
            .await
            .map_err(|source| Error::HttpTransport {
                operation: GET_ARTIFACT_OPERATION_ID,
                source,
            })?
            .to_vec();
        Ok(ArtifactDownload {
            bytes,
            media_type,
            content_length,
            etag,
        })
    }

    /// Fetches at most `max_bytes` for a bounded in-app preview.
    pub async fn get_artifact_preview(
        &self,
        content_hash: &str,
        max_bytes: usize,
    ) -> Result<ArtifactPreviewDownload, Error> {
        if max_bytes == 0 {
            return Err(Error::InvalidArtifactPreviewLimit);
        }

        let path = path_with_segment(
            ARTIFACT_PATH,
            "{content_hash}",
            "content_hash",
            content_hash,
        )?;
        let response = self
            .http
            .get(self.http_url(&path))
            .send()
            .await
            .map_err(|source| Error::HttpTransport {
                operation: GET_ARTIFACT_OPERATION_ID,
                source,
            })?;
        let response = successful_response(response).await?;
        let headers = response.headers();
        let media_type = header_text(headers, reqwest::header::CONTENT_TYPE);
        let content_length = headers
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
        let etag = header_text(headers, reqwest::header::ETAG);
        let declared_truncated = content_length.is_some_and(|length| {
            usize::try_from(length).map_or(true, |length| length > max_bytes)
        });
        let capacity = content_length
            .and_then(|length| usize::try_from(length).ok())
            .map_or(max_bytes, |length| length.min(max_bytes));
        let mut bytes = Vec::with_capacity(capacity);
        let mut stream = response.bytes_stream();
        let mut truncated = declared_truncated;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|source| Error::HttpTransport {
                operation: GET_ARTIFACT_OPERATION_ID,
                source,
            })?;
            let remaining = max_bytes.saturating_sub(bytes.len());
            if chunk.len() > remaining {
                bytes.extend_from_slice(&chunk[..remaining]);
                truncated = true;
                break;
            }
            bytes.extend_from_slice(&chunk);
            if bytes.len() == max_bytes && declared_truncated {
                break;
            }
        }

        Ok(ArtifactPreviewDownload {
            bytes,
            media_type,
            content_length,
            etag,
            truncated,
        })
    }

    /// Opens the negotiated event stream. `None` requests live events only.
    pub async fn subscribe_events(
        &self,
        negotiated: &NegotiateResponse,
        after: Option<&str>,
    ) -> Result<EventStream, Error> {
        self.validate_negotiated_version(negotiated)?;
        if !negotiated
            .selected_capabilities
            .iter()
            .any(|capability| capability == WEBSOCKET_CAPABILITY)
        {
            return Err(Error::MissingCapability(WEBSOCKET_CAPABILITY));
        }

        let mut endpoint = self.http_url(EVENTS_WEBSOCKET_PATH);
        endpoint
            .set_scheme("ws")
            .expect("the canonical loopback HTTP URL accepts the ws scheme");
        {
            let mut query = endpoint.query_pairs_mut();
            query
                .append_pair("version", &negotiated.selected_version)
                .append_pair("capability", WEBSOCKET_CAPABILITY);
            if let Some(after) = after {
                query.append_pair("after", after);
            }
        }

        let mut request = endpoint
            .as_str()
            .into_client_request()
            .map_err(Error::WebSocketTransport)?;
        request
            .headers_mut()
            .insert(SEC_WEBSOCKET_PROTOCOL, self.websocket_protocols.clone());

        let (socket, response) = connect_async(request).await.map_err(map_websocket_error)?;
        if response
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok())
            != Some(WEBSOCKET_CAPABILITY)
        {
            return Err(Error::WebSocketSubprotocol);
        }
        Ok(EventStream { socket })
    }

    fn validate_negotiated_version(&self, response: &NegotiateResponse) -> Result<(), Error> {
        if response.selected_version != PROTOCOL_VERSION {
            return Err(Error::ProtocolVersion {
                selected: response.selected_version.clone(),
                expected: PROTOCOL_VERSION,
            });
        }
        Ok(())
    }

    async fn send_json<T>(
        &self,
        operation: &'static str,
        request: reqwest::RequestBuilder,
    ) -> Result<T, Error>
    where
        T: DeserializeOwned,
    {
        let response = request
            .send()
            .await
            .map_err(|source| Error::HttpTransport { operation, source })?;
        let response = successful_response(response).await?;
        let bytes = response
            .bytes()
            .await
            .map_err(|source| Error::HttpTransport { operation, source })?;
        serde_json::from_slice(&bytes).map_err(|source| Error::Decode { operation, source })
    }

    fn http_url(&self, path: &str) -> reqwest::Url {
        let mut url = reqwest::Url::parse(&format!("http://{}/", self.address))
            .expect("a loopback socket address is a valid URL authority");
        url.set_path(path);
        url
    }
}

/// A connected event stream. Reconnection and cursor persistence belong to the caller.
pub struct EventStream {
    socket: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
}

impl fmt::Debug for EventStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventStream")
            .finish_non_exhaustive()
    }
}

impl EventStream {
    /// Returns each canonical frame unchanged, including acknowledgements and protocol errors.
    pub async fn next_frame(&mut self) -> Result<Option<WebSocketFrame>, Error> {
        loop {
            let Some(message) = self.socket.next().await else {
                return Ok(None);
            };
            match message.map_err(Error::WebSocketTransport)? {
                Message::Text(text) => {
                    return serde_json::from_str(text.as_ref())
                        .map(Some)
                        .map_err(|source| Error::Decode {
                            operation: "event_stream",
                            source,
                        });
                }
                Message::Ping(payload) => self
                    .socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(Error::WebSocketTransport)?,
                Message::Pong(_) => {}
                Message::Close(_) => return Ok(None),
                Message::Binary(_) => {
                    return Err(Error::UnexpectedWebSocketMessage { kind: "binary" });
                }
                Message::Frame(_) => {
                    return Err(Error::UnexpectedWebSocketMessage { kind: "raw frame" });
                }
            }
        }
    }

    pub async fn ping(&mut self, payload: Vec<u8>) -> Result<(), Error> {
        self.socket
            .send(Message::Ping(payload.into()))
            .await
            .map_err(Error::WebSocketTransport)
    }

    pub async fn close(mut self) -> Result<(), Error> {
        self.socket
            .close(None)
            .await
            .map_err(Error::WebSocketTransport)
    }
}

async fn successful_response(response: reqwest::Response) -> Result<reqwest::Response, Error> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status().as_u16();
    let bytes = response
        .bytes()
        .await
        .map_err(|source| Error::HttpTransport {
            operation: "read_error_response",
            source,
        })?;
    match serde_json::from_slice(&bytes) {
        Ok(problem) => Err(Error::Api { status, problem }),
        Err(_) => Err(Error::UnexpectedResponse { status }),
    }
}

fn map_websocket_error(error: WebSocketError) -> Error {
    match error {
        WebSocketError::Http(response) => {
            let status = response.status().as_u16();
            if let Some(body) = response.body()
                && let Ok(problem) = serde_json::from_slice(body)
            {
                return Error::Api { status, problem };
            }
            Error::UnexpectedResponse { status }
        }
        error => Error::WebSocketTransport(error),
    }
}

fn with_idempotency_key(
    request: reqwest::RequestBuilder,
    idempotency_key: &str,
) -> Result<reqwest::RequestBuilder, Error> {
    if idempotency_key.is_empty() {
        return Err(Error::InvalidIdempotencyKey);
    }
    let value = HeaderValue::from_str(idempotency_key).map_err(|_| Error::InvalidIdempotencyKey)?;
    Ok(request.header(IDEMPOTENCY_KEY_HEADER, value))
}

fn path_with_segment(
    template: &str,
    placeholder: &str,
    name: &'static str,
    value: &str,
) -> Result<String, Error> {
    path_with_segments(template, &[(placeholder, name, value)])
}

fn path_with_segments(
    template: &str,
    replacements: &[(&str, &'static str, &str)],
) -> Result<String, Error> {
    let mut url =
        reqwest::Url::parse("http://127.0.0.1/").expect("the canonical URL base is valid");
    let mut segments = url
        .path_segments_mut()
        .expect("the canonical HTTP URL supports path segments");
    segments.pop_if_empty();
    for template_segment in template.split('/').filter(|segment| !segment.is_empty()) {
        if let Some((_, name, value)) = replacements
            .iter()
            .find(|(placeholder, _, _)| *placeholder == template_segment)
        {
            if value.is_empty() || *value == "." || *value == ".." {
                return Err(Error::InvalidPathSegment { name });
            }
            segments.push(value);
        } else {
            segments.push(template_segment);
        }
    }
    drop(segments);
    Ok(url.path().to_owned())
}

fn is_http_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn header_text(headers: &HeaderMap, name: reqwest::header::HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}
