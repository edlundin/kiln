//! HTTP and WebSocket transport for the current Kiln protocol slice.

use std::{future::Future, net::SocketAddr, pin::Pin, sync::Arc};

use axum::{
    Json, Router,
    body::Bytes,
    body::{Body, to_bytes},
    extract::{
        FromRequest, Path, Query, Request, State, WebSocketUpgrade,
        rejection::QueryRejection,
        ws::{self, rejection::WebSocketUpgradeRejection},
    },
    http::{
        HeaderName, HeaderValue, StatusCode,
        header::{
            AUTHORIZATION, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE, ETAG,
            HOST, ORIGIN, SEC_WEBSOCKET_PROTOCOL,
        },
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use kiln_core::{
    AppendMessage, Artifact, AssignTask, ChildActivityReference as CoreChildActivityReference,
    ContentHash, CreateTask, CreateWorkspace, DEFAULT_USAGE_PAGE_LIMIT, EventCursor,
    MAX_USAGE_PAGE_LIMIT, Message, MessageDelivery, MessageDeliveryMode as CoreMessageDeliveryMode,
    MessageDeliveryState as CoreMessageDeliveryState, MessageRole as CoreMessageRole,
    ModelInvocationId, ProviderAccount, ProviderAccountError, ProviderAccountId, ProviderType,
    ReactToRunActivity, RunError, RunId, RunInputMode as CoreRunInputMode, RunSnapshot,
    RunState as CoreRunState, SendRunInput, Session, SessionError, SessionEventPage,
    SessionEventPayload, SessionId, SessionOperations, StoreMetadata, StoredSessionEvent, Task,
    TaskError, TaskId, TaskOperations, TaskState as CoreTaskState, ToolCall,
    ToolCallState as CoreToolCallState, ToolOutputStream as CoreToolOutputStream, TransitionTask,
    UpdateTask, UsageLedgerPage, UsageQueryError, WorkspaceChangePath, WorkspaceError, WorkspaceId,
    WorkspaceOperations,
};
use kiln_protocol::{
    ARTIFACT_PATH, ARTIFACT_SESSION_HEADER, ARTIFACTS_PATH, AppendMessageRequest, ApprovalDecision,
    ApprovalDecisionRequest, ApprovalPolicy as ProtocolApprovalPolicy, ApprovalResponse,
    ApprovalState as ProtocolApprovalState, ArtifactResponse, AssignTaskRequest,
    ChangedFileResponse, ChildActivityReference, CreateProviderAccountRequest, CreateTaskRequest,
    CreateWorkspaceRequest, EVENTS_WEBSOCKET_PATH, IDEMPOTENCY_KEY_HEADER,
    ListProviderAccountsResponse, ListSessionsResponse, ListWorkspacesResponse,
    MAX_ARTIFACT_UPLOAD_BYTES, MessageDeliveryMode, MessageDeliveryResponse, MessageDeliveryState,
    MessageResponse, MessageRole, NEGOTIATE_PATH, NegotiateRequest, NegotiateResponse,
    PROTOCOL_VERSION, PROVIDER_ACCOUNT_LOGIN_ATTEMPT_PATH, PROVIDER_ACCOUNT_LOGIN_PATH,
    PROVIDER_ACCOUNT_PATH, PROVIDER_ACCOUNTS_PATH, ProblemDetails, ProviderAccountLoginResponse,
    ProviderAccountLoginState, ProviderAccountResponse, RUN_CANCEL_PATH, RUN_CHILDREN_PATH,
    RUN_INPUT_PATH, RUN_PATH, RUN_REACTIONS_PATH, ReactToRunActivityRequest, RunInputMode,
    RunResponse, RunState, SESSION_CHANGE_DIFF_PATH, SESSION_CHANGES_PATH, SESSION_EVENTS_PATH,
    SESSION_MESSAGES_PATH, SESSION_PATH, SESSION_RUNS_PATH, SESSION_TASKS_PATH,
    SendRunInputRequest, SessionChangeDiffContent, SessionChangeDiffResponse,
    SessionChangeDiffUnavailableReason, SessionChangesResponse, SessionEventDataResponse,
    SessionEventResponse, SessionEventsResponse, SessionResponse, SessionRunsResponse,
    StartChildRunRequest, StartProviderAccountLoginResponse, StartRunRequest, StoreIdentity,
    TASK_ASSIGNMENT_PATH, TASK_PATH, TASK_TRANSITION_PATH, TOOL_CALL_APPROVAL_PATH, TaskResponse,
    TaskState, ToolCallResponse, ToolCallState, ToolOutputStream, TransitionTaskRequest,
    USAGE_PATH, UpdateTaskRequest, UsageAccounting, UsageCompleteness, UsageFinality,
    UsageLedgerEntryResponse, UsageLedgerResponse, UsageQuantityRelation, UsageQuantityResponse,
    UsageSource, WEBSOCKET_CAPABILITY, WORKSPACE_PATH, WORKSPACE_SESSIONS_PATH, WORKSPACES_PATH,
    WebSocketFrame, WorkspaceResponse, WorkspaceRootResponse, WorkspaceScopeResponse, error_code,
};
use semver::Version;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

pub trait RunOperations: Send + Sync {
    fn start_run(
        &self,
        session_id: SessionId,
        idempotency_key: String,
        approval_policy: kiln_core::ApprovalPolicy,
        requested_scope: kiln_core::WorkspacePathScope,
    ) -> impl Future<Output = Result<kiln_core::StartRunMutation, RunError>> + Send;

    fn start_child_run(
        &self,
        parent_run_id: RunId,
        task_id: Option<TaskId>,
        user_input_mode: CoreRunInputMode,
        idempotency_key: String,
        approval_policy: kiln_core::ApprovalPolicy,
        requested_scope: kiln_core::WorkspacePathScope,
    ) -> impl Future<Output = Result<kiln_core::StartRunMutation, RunError>> + Send;

    fn list_session_runs(
        &self,
        session_id: SessionId,
    ) -> impl Future<Output = Result<Vec<RunSnapshot>, RunError>> + Send;

    fn get_run(&self, run_id: RunId) -> impl Future<Output = Result<RunSnapshot, RunError>> + Send;

    fn send_run_input(
        &self,
        command: SendRunInput,
    ) -> impl Future<Output = Result<kiln_core::SendRunInputMutation, RunError>> + Send;
    fn react_to_run_activity(
        &self,
        command: ReactToRunActivity,
    ) -> impl Future<Output = Result<kiln_core::SendRunInputMutation, RunError>> + Send;

    fn cancel_run(
        &self,
        run_id: RunId,
    ) -> impl Future<Output = Result<RunSnapshot, RunError>> + Send;

    fn decide_approval(
        &self,
        tool_call_id: kiln_core::ToolCallId,
        decision: kiln_core::ApprovalState,
        idempotency_key: String,
    ) -> impl Future<Output = Result<kiln_core::ApprovalDecisionMutation, RunError>> + Send;
}

#[derive(Debug, Clone)]
pub struct ProviderAccountCreateCommand {
    pub provider_type: ProviderType,
    pub label: String,
    pub workspace_ids: Vec<WorkspaceId>,
    pub idempotency_key: String,
}

#[derive(Debug, Clone)]
pub struct ProviderAccountLoginStart {
    pub attempt_id: String,
    pub verification_url: String,
    pub user_code: String,
    pub account: ProviderAccount,
}

#[derive(Debug, Clone)]
pub struct ProviderAccountLoginStatus {
    pub attempt_id: String,
    pub state: ProviderAccountLoginState,
    pub account: ProviderAccount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ProviderAccountOperationError {
    #[error("the provider account request is invalid")]
    InvalidRequest,
    #[error("the provider account was not found")]
    AccountNotFound,
    #[error("the provider account workspace association is invalid")]
    WorkspaceAssociationMismatch,
    #[error("the provider account limit was reached")]
    ProviderAccountLimitReached,
    #[error("the provider account idempotency key was reused with a different request")]
    IdempotencyConflict,
    #[error("the provider account store is unavailable")]
    StoreUnavailable,
    #[error("the provider account credential store is unavailable")]
    CredentialStoreUnavailable,
    #[error("provider account credential cleanup must be retried")]
    CleanupRequired,
    #[error("the provider account is in an invalid state")]
    InvalidState,
    #[error("the provider account login attempt was not found")]
    AttemptNotFound,
    #[error("the provider account login provider is unavailable")]
    LoginUnavailable,
    #[error("the provider account login failed")]
    LoginFailed,
    #[error("the provider account login was cancelled")]
    Cancelled,
}

impl From<ProviderAccountError> for ProviderAccountOperationError {
    fn from(error: ProviderAccountError) -> Self {
        match error {
            ProviderAccountError::AccountNotFound => Self::AccountNotFound,
            ProviderAccountError::WorkspaceAssociationMismatch => {
                Self::WorkspaceAssociationMismatch
            }
            ProviderAccountError::ProviderAccountLimitReached => Self::ProviderAccountLimitReached,
            ProviderAccountError::IdempotencyConflict => Self::IdempotencyConflict,
            ProviderAccountError::StoreUnavailable => Self::StoreUnavailable,
            ProviderAccountError::CredentialCleanupRequired { .. } => Self::CleanupRequired,
            ProviderAccountError::CredentialStore(_)
            | ProviderAccountError::CredentialStoreRequired => Self::CredentialStoreUnavailable,
            ProviderAccountError::ProviderTypeMismatch
            | ProviderAccountError::InvalidLabel
            | ProviderAccountError::InvalidSubject
            | ProviderAccountError::InvalidSecretRef
            | ProviderAccountError::InvalidMetadata
            | ProviderAccountError::InvalidTimestamp
            | ProviderAccountError::SecretRefRequired
            | ProviderAccountError::SecretRefForbidden
            | ProviderAccountError::InvalidTransition
            | ProviderAccountError::AccountNotConnected
            | ProviderAccountError::IntegrityViolation
            | ProviderAccountError::CredentialVersionConflict
            | ProviderAccountError::CredentialRefresh(_) => Self::InvalidRequest,
        }
    }
}

pub trait ProviderAccountOperations: Send + Sync {
    fn disconnect_provider_account(
        &self,
        _account_id: ProviderAccountId,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ProviderAccount, ProviderAccountOperationError>> + Send + '_,
        >,
    > {
        Box::pin(async { Err(ProviderAccountOperationError::StoreUnavailable) })
    }

    fn create_provider_account(
        &self,
        command: ProviderAccountCreateCommand,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ProviderAccount, ProviderAccountOperationError>> + Send + '_,
        >,
    >;

    fn list_provider_accounts(
        &self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ProviderAccount>, ProviderAccountOperationError>>
                + Send
                + '_,
        >,
    >;

    fn get_provider_account(
        &self,
        account_id: ProviderAccountId,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ProviderAccount, ProviderAccountOperationError>> + Send + '_,
        >,
    >;

    fn start_provider_account_login(
        &self,
        account_id: ProviderAccountId,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ProviderAccountLoginStart, ProviderAccountOperationError>>
                + Send
                + '_,
        >,
    >;

    fn get_provider_account_login(
        &self,
        account_id: ProviderAccountId,
        attempt_id: String,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ProviderAccountLoginStatus, ProviderAccountOperationError>>
                + Send
                + '_,
        >,
    >;

    fn cancel_provider_account_login(
        &self,
        account_id: ProviderAccountId,
        attempt_id: String,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ProviderAccountLoginStatus, ProviderAccountOperationError>>
                + Send
                + '_,
        >,
    >;
}

pub trait UsageOperations: Send + Sync {
    fn list_usage_ledger(
        &self,
        after: Option<kiln_core::ModelInvocationId>,
        limit: u64,
    ) -> Pin<Box<dyn Future<Output = Result<UsageLedgerPage, UsageQueryError>> + Send + '_>>;
}

impl<S, I> UsageOperations for kiln_core::UsageApplication<S, I>
where
    S: kiln_core::UsageStore + 'static,
    I: kiln_core::UsageIdGenerator + 'static,
{
    fn list_usage_ledger(
        &self,
        after: Option<kiln_core::ModelInvocationId>,
        limit: u64,
    ) -> Pin<Box<dyn Future<Output = Result<UsageLedgerPage, UsageQueryError>> + Send + '_>> {
        Box::pin(kiln_core::UsageApplication::list_usage_ledger(
            self, after, limit,
        ))
    }
}

struct UnavailableUsageOperations;

impl UsageOperations for UnavailableUsageOperations {
    fn list_usage_ledger(
        &self,
        after: Option<kiln_core::ModelInvocationId>,
        limit: u64,
    ) -> Pin<Box<dyn Future<Output = Result<UsageLedgerPage, UsageQueryError>> + Send + '_>> {
        let _ = (after, limit);
        Box::pin(async { Err(UsageQueryError::Unavailable) })
    }
}

struct UnavailableProviderAccountOperations;

impl ProviderAccountOperations for UnavailableProviderAccountOperations {
    fn create_provider_account(
        &self,
        _command: ProviderAccountCreateCommand,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ProviderAccount, ProviderAccountOperationError>> + Send + '_,
        >,
    > {
        Box::pin(async { Err(ProviderAccountOperationError::StoreUnavailable) })
    }

    fn list_provider_accounts(
        &self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ProviderAccount>, ProviderAccountOperationError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async { Err(ProviderAccountOperationError::StoreUnavailable) })
    }

    fn get_provider_account(
        &self,
        _account_id: ProviderAccountId,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ProviderAccount, ProviderAccountOperationError>> + Send + '_,
        >,
    > {
        Box::pin(async { Err(ProviderAccountOperationError::StoreUnavailable) })
    }

    fn start_provider_account_login(
        &self,
        _account_id: ProviderAccountId,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ProviderAccountLoginStart, ProviderAccountOperationError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async { Err(ProviderAccountOperationError::LoginUnavailable) })
    }

    fn get_provider_account_login(
        &self,
        _account_id: ProviderAccountId,
        _attempt_id: String,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ProviderAccountLoginStatus, ProviderAccountOperationError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async { Err(ProviderAccountOperationError::LoginUnavailable) })
    }

    fn cancel_provider_account_login(
        &self,
        _account_id: ProviderAccountId,
        _attempt_id: String,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ProviderAccountLoginStatus, ProviderAccountOperationError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async { Err(ProviderAccountOperationError::LoginUnavailable) })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ArtifactFetchError {
    #[error("artifact does not exist")]
    NotFound,
    #[error("artifact store is unavailable")]
    Unavailable,
}

#[derive(Debug)]
pub struct ArtifactDownload {
    artifact: Artifact,
    bytes: Vec<u8>,
}

impl ArtifactDownload {
    pub fn new(artifact: Artifact, bytes: Vec<u8>) -> Result<Self, ArtifactFetchError> {
        if u64::try_from(bytes.len()).ok() != Some(artifact.size()) {
            return Err(ArtifactFetchError::Unavailable);
        }
        Ok(Self { artifact, bytes })
    }
}

pub trait ArtifactOperations: Send + Sync {
    fn get_artifact(
        &self,
        content_hash: ContentHash,
    ) -> impl Future<Output = Result<ArtifactDownload, ArtifactFetchError>> + Send;
    fn upload_artifact(
        &self,
        session_id: SessionId,
        bytes: Vec<u8>,
        media_type: String,
    ) -> impl Future<Output = Result<Artifact, ArtifactUploadError>> + Send;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ArtifactUploadError {
    #[error("artifact upload is too large")]
    TooLarge,
    #[error("artifact media type is invalid")]
    InvalidMediaType,
    #[error("artifact store is unavailable")]
    Unavailable,
}

const WEBSOCKET_AUTH_PREFIX: &str = "kiln.auth.";

#[derive(Clone)]
pub struct AuthToken([u8; 80]);

impl AuthToken {
    pub fn from_bytes(bytes: [u8; 80]) -> Self {
        Self(bytes)
    }

    fn matches(&self, candidate: &[u8]) -> bool {
        constant_time_equal(&self.0, candidate)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecyclePhase {
    Running,
    Quiescing,
}

#[derive(Debug)]
struct LifecycleState {
    phase: LifecyclePhase,
    in_flight_commands: usize,
}

#[derive(Debug)]
struct LifecycleInner {
    state: std::sync::Mutex<LifecycleState>,
    changed: tokio::sync::Notify,
}

#[derive(Debug, Clone)]
pub struct LifecycleCoordinator {
    inner: Arc<LifecycleInner>,
}

impl Default for LifecycleCoordinator {
    fn default() -> Self {
        Self {
            inner: Arc::new(LifecycleInner {
                state: std::sync::Mutex::new(LifecycleState {
                    phase: LifecyclePhase::Running,
                    in_flight_commands: 0,
                }),
                changed: tokio::sync::Notify::new(),
            }),
        }
    }
}

impl LifecycleCoordinator {
    pub fn new() -> Self {
        Self::default()
    }

    fn begin_command(&self) -> Result<CommandPermit, PublicError> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("lifecycle state lock is not poisoned");
        if state.phase == LifecyclePhase::Quiescing {
            return Err(PublicError::DaemonShuttingDown);
        }
        state.in_flight_commands += 1;
        Ok(CommandPermit {
            lifecycle: self.clone(),
        })
    }

    pub fn request_shutdown(&self) -> bool {
        let changed = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("lifecycle state lock is not poisoned");
            if state.phase == LifecyclePhase::Quiescing {
                false
            } else {
                state.phase = LifecyclePhase::Quiescing;
                true
            }
        };
        if changed {
            self.inner.changed.notify_waiters();
        }
        changed
    }

    pub async fn wait_for_shutdown_request(&self) {
        loop {
            let changed = self.inner.changed.notified();
            if self
                .inner
                .state
                .lock()
                .expect("lifecycle state lock is not poisoned")
                .phase
                == LifecyclePhase::Quiescing
            {
                return;
            }
            changed.await;
        }
    }

    pub async fn wait_for_commands(&self) {
        loop {
            let changed = self.inner.changed.notified();
            if self
                .inner
                .state
                .lock()
                .expect("lifecycle state lock is not poisoned")
                .in_flight_commands
                == 0
            {
                return;
            }
            changed.await;
        }
    }
}

struct CommandPermit {
    lifecycle: LifecycleCoordinator,
}

impl Drop for CommandPermit {
    fn drop(&mut self) {
        let became_idle = {
            let mut state = self
                .lifecycle
                .inner
                .state
                .lock()
                .expect("lifecycle state lock is not poisoned");
            state.in_flight_commands -= 1;
            state.in_flight_commands == 0
        };
        if became_idle {
            self.lifecycle.inner.changed.notify_waiters();
        }
    }
}

#[derive(Clone, Default)]
pub struct EventBroadcaster {
    wake_subscribers: Arc<std::sync::Mutex<Vec<tokio::sync::mpsc::UnboundedSender<()>>>>,
}

impl EventBroadcaster {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe_wake(&self) -> tokio::sync::mpsc::UnboundedReceiver<()> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        self.wake_subscribers
            .lock()
            .expect("event broadcaster wake lock is not poisoned")
            .push(sender);
        receiver
    }

    pub fn wake(&self) {
        let mut subscribers = self
            .wake_subscribers
            .lock()
            .expect("event broadcaster wake lock is not poisoned");
        subscribers.retain(|subscriber| subscriber.send(()).is_ok());
    }

    pub fn publish(&self, events: impl IntoIterator<Item = StoredSessionEvent>) {
        if events.into_iter().next().is_some() {
            self.wake();
        }
    }
}

pub struct AppState<W, S, R> {
    store: StoreMetadata,
    event_websocket_endpoint: String,
    bound_authority: String,
    http_origin: String,
    auth_token: AuthToken,
    workspace_operations: Arc<W>,
    session_operations: Arc<S>,
    run_operations: Arc<R>,
    usage_operations: Arc<dyn UsageOperations>,
    provider_account_operations: Arc<dyn ProviderAccountOperations>,
    event_broadcaster: EventBroadcaster,
    lifecycle: LifecycleCoordinator,
}

impl<W, S, R> Clone for AppState<W, S, R> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            event_websocket_endpoint: self.event_websocket_endpoint.clone(),
            bound_authority: self.bound_authority.clone(),
            http_origin: self.http_origin.clone(),
            auth_token: self.auth_token.clone(),
            workspace_operations: Arc::clone(&self.workspace_operations),
            session_operations: Arc::clone(&self.session_operations),
            run_operations: Arc::clone(&self.run_operations),
            usage_operations: Arc::clone(&self.usage_operations),
            provider_account_operations: Arc::clone(&self.provider_account_operations),
            event_broadcaster: self.event_broadcaster.clone(),
            lifecycle: self.lifecycle.clone(),
        }
    }
}

impl<W, S, R> AppState<W, S, R> {
    pub fn with_operations(
        store: StoreMetadata,
        bound_addr: SocketAddr,
        workspace_operations: W,
        session_operations: S,
        run_operations: R,
        event_broadcaster: EventBroadcaster,
        auth_token: AuthToken,
    ) -> Self {
        Self::with_usage_operations(
            store,
            bound_addr,
            workspace_operations,
            session_operations,
            run_operations,
            UnavailableUsageOperations,
            event_broadcaster,
            auth_token,
        )
    }

    pub fn with_usage_operations<U>(
        store: StoreMetadata,
        bound_addr: SocketAddr,
        workspace_operations: W,
        session_operations: S,
        run_operations: R,
        usage_operations: U,
        event_broadcaster: EventBroadcaster,
        auth_token: AuthToken,
    ) -> Self
    where
        U: UsageOperations + 'static,
    {
        let bound_authority = bound_addr.to_string();
        Self {
            store,
            event_websocket_endpoint: format!("ws://{bound_addr}{EVENTS_WEBSOCKET_PATH}"),
            http_origin: format!("http://{bound_authority}"),
            bound_authority,
            auth_token,
            workspace_operations: Arc::new(workspace_operations),
            session_operations: Arc::new(session_operations),
            run_operations: Arc::new(run_operations),
            usage_operations: Arc::new(usage_operations),
            provider_account_operations: Arc::new(UnavailableProviderAccountOperations),
            event_broadcaster,
            lifecycle: LifecycleCoordinator::new(),
        }
    }

    pub fn with_provider_account_operations<U>(
        store: StoreMetadata,
        bound_addr: SocketAddr,
        workspace_operations: W,
        session_operations: S,
        run_operations: R,
        usage_operations: U,
        provider_account_operations: Arc<dyn ProviderAccountOperations>,
        event_broadcaster: EventBroadcaster,
        auth_token: AuthToken,
    ) -> Self
    where
        U: UsageOperations + 'static,
    {
        let bound_authority = bound_addr.to_string();
        Self {
            store,
            event_websocket_endpoint: format!("ws://{bound_addr}{EVENTS_WEBSOCKET_PATH}"),
            http_origin: format!("http://{bound_authority}"),
            bound_authority,
            auth_token,
            workspace_operations: Arc::new(workspace_operations),
            session_operations: Arc::new(session_operations),
            run_operations: Arc::new(run_operations),
            usage_operations: Arc::new(usage_operations),
            provider_account_operations,
            event_broadcaster,
            lifecycle: LifecycleCoordinator::new(),
        }
    }

    pub fn lifecycle(&self) -> LifecycleCoordinator {
        self.lifecycle.clone()
    }
}

pub fn router<W, S, R>(state: AppState<W, S, R>) -> Router
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + TaskOperations + 'static,
    R: RunOperations + ArtifactOperations + 'static,
{
    Router::new()
        .route(NEGOTIATE_PATH, post(negotiate))
        .route(WORKSPACES_PATH, get(list_workspaces).post(create_workspace))
        .route(WORKSPACE_PATH, get(get_workspace))
        .route(
            PROVIDER_ACCOUNTS_PATH,
            get(list_provider_accounts).post(create_provider_account),
        )
        .route(PROVIDER_ACCOUNT_PATH, get(get_provider_account))
        .route(
            kiln_protocol::PROVIDER_ACCOUNT_DISCONNECT_PATH,
            post(disconnect_provider_account),
        )
        .route(
            PROVIDER_ACCOUNT_LOGIN_PATH,
            post(start_provider_account_login),
        )
        .route(
            PROVIDER_ACCOUNT_LOGIN_ATTEMPT_PATH,
            get(get_provider_account_login).post(cancel_provider_account_login),
        )
        .route(
            WORKSPACE_SESSIONS_PATH,
            get(list_sessions).post(create_session),
        )
        .route(SESSION_PATH, get(get_session))
        .route(SESSION_MESSAGES_PATH, post(append_message))
        .route(SESSION_TASKS_PATH, post(create_task))
        .route(TASK_PATH, get(get_task).patch(update_task))
        .route(TASK_ASSIGNMENT_PATH, post(assign_task))
        .route(TASK_TRANSITION_PATH, post(transition_task))
        .route(SESSION_EVENTS_PATH, get(list_session_events))
        .route(SESSION_CHANGES_PATH, get(list_session_changes))
        .route(SESSION_CHANGE_DIFF_PATH, get(get_session_change_diff))
        .route(USAGE_PATH, get(list_usage_ledger))
        .route(SESSION_RUNS_PATH, post(start_run).get(list_session_runs))
        .route(RUN_CHILDREN_PATH, post(start_child_run))
        .route(RUN_PATH, get(get_run))
        .route(RUN_INPUT_PATH, post(send_run_input))
        .route(RUN_REACTIONS_PATH, post(react_to_run_activity))
        .route(RUN_CANCEL_PATH, post(cancel_run))
        .route(TOOL_CALL_APPROVAL_PATH, post(decide_approval))
        .route(ARTIFACTS_PATH, post(upload_artifact))
        .route(ARTIFACT_PATH, get(get_artifact))
        .route(EVENTS_WEBSOCKET_PATH, get(events))
        .method_not_allowed_fallback(method_not_allowed)
        .fallback(not_found)
        .layer(middleware::from_fn_with_state(state.clone(), authenticate))
        .with_state(state)
}

async fn authenticate<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    request: Request,
    next: Next,
) -> Response
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + TaskOperations + 'static,
    R: RunOperations + ArtifactOperations + 'static,
{
    if let Err(error) = validate_authority(&request, &state.bound_authority, &state.http_origin) {
        return error.into_response();
    }
    if let Err(error) = authenticate_request(&request, &state.auth_token) {
        return error.into_response();
    }
    next.run(request).await
}

fn validate_authority(
    request: &Request,
    bound_authority: &str,
    http_origin: &str,
) -> Result<(), PublicError> {
    if request
        .headers()
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        != Some(bound_authority)
    {
        return Err(PublicError::InvalidHost);
    }
    let origins = request.headers().get_all(ORIGIN);
    if origins.iter().count() > 1
        || origins
            .iter()
            .next()
            .is_some_and(|origin| origin.to_str().ok() != Some(http_origin))
    {
        return Err(PublicError::InvalidOrigin);
    }
    Ok(())
}

fn authenticate_request(request: &Request, token: &AuthToken) -> Result<(), PublicError> {
    let has_authentication_input = request.headers().get(AUTHORIZATION).is_some()
        || request.headers().get(SEC_WEBSOCKET_PROTOCOL).is_some();
    let authorization_valid = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|candidate| token.matches(candidate.as_bytes()));
    if authorization_valid {
        return Ok(());
    }
    let offered = request.headers().get_all(SEC_WEBSOCKET_PROTOCOL);
    let mut websocket = false;
    let mut auth = false;
    for protocol in offered.iter().filter_map(|value| value.to_str().ok()) {
        for protocol in protocol.split(',').map(str::trim) {
            websocket |= protocol == WEBSOCKET_CAPABILITY;
            if let Some(candidate) = protocol.strip_prefix(WEBSOCKET_AUTH_PREFIX) {
                auth |= token.matches(candidate.as_bytes());
            }
        }
    }
    if websocket && auth {
        Ok(())
    } else if has_authentication_input {
        Err(PublicError::InvalidAuthentication)
    } else {
        Err(PublicError::AuthenticationRequired)
    }
}

fn constant_time_equal(expected: &[u8], actual: &[u8]) -> bool {
    let mut difference = expected.len() ^ actual.len();
    for index in 0..expected.len().max(actual.len()) {
        difference |= usize::from(expected.get(index).copied().unwrap_or_default())
            ^ usize::from(actual.get(index).copied().unwrap_or_default());
    }
    difference == 0
}

pub async fn serve<W, S, R>(
    listener: tokio::net::TcpListener,
    state: AppState<W, S, R>,
) -> Result<(), std::io::Error>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + TaskOperations + 'static,
    R: RunOperations + ArtifactOperations + 'static,
{
    axum::serve(listener, router(state)).await
}

pub async fn serve_with_shutdown<W, S, R>(
    listener: tokio::net::TcpListener,
    state: AppState<W, S, R>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), std::io::Error>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + TaskOperations + 'static,
    R: RunOperations + ArtifactOperations + 'static,
{
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown)
        .await
}

async fn create_workspace<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    StrictJson(request): StrictJson<CreateWorkspaceRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let workspace = state
        .workspace_operations
        .create_workspace(CreateWorkspace {
            name: request.name,
            roots: request
                .roots
                .into_iter()
                .map(|root| kiln_core::WorkspaceRootInput {
                    name: root.name,
                    path: root.path,
                })
                .collect(),
        })
        .await
        .map_err(PublicError::from)?;
    Ok((StatusCode::CREATED, Json(workspace_response(&workspace))))
}

async fn list_workspaces<W, S, R>(
    State(state): State<AppState<W, S, R>>,
) -> Result<Json<ListWorkspacesResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let workspaces = state
        .workspace_operations
        .list_workspaces()
        .await
        .map_err(PublicError::from)?;
    Ok(Json(ListWorkspacesResponse {
        workspaces: workspaces.iter().map(workspace_response).collect(),
    }))
}

async fn get_workspace<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(workspace_id): Path<String>,
) -> Result<Json<WorkspaceResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let workspace = state
        .workspace_operations
        .get_workspace(WorkspaceId::parse(workspace_id).map_err(|_| PublicError::InvalidRequest)?)
        .await
        .map_err(PublicError::from)?;
    Ok(Json(workspace_response(&workspace)))
}

async fn create_provider_account<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    headers: axum::http::HeaderMap,
    StrictJson(request): StrictJson<CreateProviderAccountRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let idempotency_key = required_idempotency_key(&headers)?;
    let provider_type = ProviderType::parse(request.provider_type)
        .map_err(|_| PublicError::ProviderAccount(ProviderAccountOperationError::InvalidRequest))?;
    let workspace_ids = request
        .workspace_ids
        .into_iter()
        .map(WorkspaceId::parse)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| PublicError::ProviderAccount(ProviderAccountOperationError::InvalidRequest))?;
    let account = state
        .provider_account_operations
        .create_provider_account(ProviderAccountCreateCommand {
            provider_type,
            label: request.label,
            workspace_ids,
            idempotency_key,
        })
        .await
        .map_err(PublicError::from)?;
    Ok((
        StatusCode::CREATED,
        Json(provider_account_response(&account)),
    ))
}

async fn list_provider_accounts<W, S, R>(
    State(state): State<AppState<W, S, R>>,
) -> Result<Json<ListProviderAccountsResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let accounts = state
        .provider_account_operations
        .list_provider_accounts()
        .await
        .map_err(PublicError::from)?;
    Ok(Json(ListProviderAccountsResponse {
        provider_accounts: accounts.iter().map(provider_account_response).collect(),
    }))
}

async fn get_provider_account<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(provider_account_id): Path<String>,
) -> Result<Json<ProviderAccountResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let account_id = ProviderAccountId::parse(provider_account_id)
        .map_err(|_| PublicError::ProviderAccount(ProviderAccountOperationError::InvalidRequest))?;
    let account = state
        .provider_account_operations
        .get_provider_account(account_id)
        .await
        .map_err(PublicError::from)?;
    Ok(Json(provider_account_response(&account)))
}

async fn disconnect_provider_account<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(provider_account_id): Path<String>,
) -> Result<Json<ProviderAccountResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let account_id = ProviderAccountId::parse(provider_account_id)
        .map_err(|_| PublicError::ProviderAccount(ProviderAccountOperationError::InvalidRequest))?;
    let account = state
        .provider_account_operations
        .disconnect_provider_account(account_id)
        .await
        .map_err(PublicError::from)?;
    Ok(Json(provider_account_response(&account)))
}

async fn start_provider_account_login<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(provider_account_id): Path<String>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let account_id = ProviderAccountId::parse(provider_account_id)
        .map_err(|_| PublicError::ProviderAccount(ProviderAccountOperationError::InvalidRequest))?;
    let login = state
        .provider_account_operations
        .start_provider_account_login(account_id)
        .await
        .map_err(PublicError::from)?;
    Ok((
        StatusCode::CREATED,
        Json(StartProviderAccountLoginResponse {
            attempt_id: login.attempt_id,
            verification_url: login.verification_url,
            user_code: login.user_code,
            account: provider_account_response(&login.account),
        }),
    ))
}

async fn get_provider_account_login<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path((provider_account_id, attempt_id)): Path<(String, String)>,
) -> Result<Json<ProviderAccountLoginResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let account_id = ProviderAccountId::parse(provider_account_id)
        .map_err(|_| PublicError::ProviderAccount(ProviderAccountOperationError::InvalidRequest))?;
    let login = state
        .provider_account_operations
        .get_provider_account_login(account_id, attempt_id)
        .await
        .map_err(PublicError::from)?;
    Ok(Json(provider_account_login_response(&login)))
}

async fn cancel_provider_account_login<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path((provider_account_id, attempt_id)): Path<(String, String)>,
) -> Result<Json<ProviderAccountLoginResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let account_id = ProviderAccountId::parse(provider_account_id)
        .map_err(|_| PublicError::ProviderAccount(ProviderAccountOperationError::InvalidRequest))?;
    let login = state
        .provider_account_operations
        .cancel_provider_account_login(account_id, attempt_id)
        .await
        .map_err(PublicError::from)?;
    Ok(Json(provider_account_login_response(&login)))
}

fn workspace_response(workspace: &kiln_core::Workspace) -> WorkspaceResponse {
    WorkspaceResponse {
        workspace_id: workspace.id().as_str().to_owned(),
        name: workspace.name().to_owned(),
        roots: workspace
            .roots()
            .iter()
            .map(|root| WorkspaceRootResponse {
                workspace_root_id: root.id().as_str().to_owned(),
                name: root.name().to_owned(),
                display_path: root.display_path().to_owned(),
                canonical_path: root.canonical_path().to_owned(),
                git_common_directory_path: root.git_common_directory_path().to_owned(),
                position: root.position(),
                state: root.state().as_str().to_owned(),
            })
            .collect(),
    }
}

fn provider_account_response(account: &ProviderAccount) -> ProviderAccountResponse {
    ProviderAccountResponse {
        provider_account_id: account.id().as_str().to_owned(),
        provider_type: account.provider_type().as_str().to_owned(),
        label: account.label().to_owned(),
        state: account.state().as_str().to_owned(),
        created_at_unix_ms: account.created_at_unix_ms(),
        updated_at_unix_ms: account.updated_at_unix_ms(),
        last_used_at_unix_ms: account.last_used_at_unix_ms(),
        capabilities_refreshed_at_unix_ms: account.capabilities_refreshed_at_unix_ms(),
    }
}

fn provider_account_login_response(
    login: &ProviderAccountLoginStatus,
) -> ProviderAccountLoginResponse {
    ProviderAccountLoginResponse {
        attempt_id: login.attempt_id.clone(),
        state: login.state,
        account: provider_account_response(&login.account),
    }
}

async fn create_session<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(workspace_id): Path<String>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let workspace_id = WorkspaceId::parse(workspace_id).map_err(|_| PublicError::InvalidRequest)?;
    let session = state
        .session_operations
        .create_session(workspace_id)
        .await
        .map_err(PublicError::from)?;
    state.event_broadcaster.wake();
    Ok((StatusCode::CREATED, Json(session_response(&session))))
}

async fn list_sessions<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(workspace_id): Path<String>,
) -> Result<Json<ListSessionsResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let workspace_id = WorkspaceId::parse(workspace_id).map_err(|_| PublicError::InvalidRequest)?;
    let sessions = state
        .session_operations
        .list_sessions(workspace_id)
        .await
        .map_err(PublicError::from)?;
    Ok(Json(ListSessionsResponse {
        sessions: sessions.iter().map(session_response).collect(),
    }))
}

async fn get_session<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let session_id = SessionId::parse(session_id).map_err(|_| PublicError::InvalidRequest)?;
    let session = state
        .session_operations
        .get_session(session_id)
        .await
        .map_err(PublicError::from)?;
    Ok(Json(session_response(&session)))
}

async fn append_message<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(session_id): Path<String>,
    headers: axum::http::HeaderMap,
    StrictJson(request): StrictJson<AppendMessageRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let session_id = SessionId::parse(session_id).map_err(|_| PublicError::InvalidRequest)?;
    let message = state
        .session_operations
        .append_message(AppendMessage {
            session_id,
            content: request.content,
            attachments: parse_attachments(request.attachments)?,
            idempotency_key: required_idempotency_key(&headers)?,
        })
        .await
        .map_err(PublicError::from)?;
    state.event_broadcaster.wake();
    Ok((StatusCode::CREATED, Json(message_response(&message))))
}

async fn create_task<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(session_id): Path<String>,
    headers: axum::http::HeaderMap,
    StrictJson(request): StrictJson<CreateTaskRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + TaskOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let session_id = SessionId::parse(session_id).map_err(|_| PublicError::InvalidRequest)?;
    let parent_task_id = request
        .parent_task_id
        .map(TaskId::parse)
        .transpose()
        .map_err(|_| PublicError::InvalidRequest)?;
    let dependency_task_ids = request
        .dependency_task_ids
        .into_iter()
        .map(TaskId::parse)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| PublicError::InvalidRequest)?;
    let mutation = state
        .session_operations
        .create_task(CreateTask {
            session_id,
            objective: request.objective,
            parent_task_id,
            dependency_task_ids,
            idempotency_key: required_idempotency_key(&headers)?,
        })
        .await
        .map_err(PublicError::from)?;
    state.event_broadcaster.publish(mutation.events);
    Ok((StatusCode::CREATED, Json(task_response(&mutation.value))))
}

async fn get_task<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(task_id): Path<String>,
) -> Result<Json<TaskResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + TaskOperations + 'static,
    R: RunOperations + 'static,
{
    let task_id = TaskId::parse(task_id).map_err(|_| PublicError::InvalidRequest)?;
    let task = state
        .session_operations
        .get_task(task_id)
        .await
        .map_err(PublicError::from)?;
    Ok(Json(task_response(&task)))
}

async fn update_task<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(task_id): Path<String>,
    headers: axum::http::HeaderMap,
    StrictJson(request): StrictJson<UpdateTaskRequest>,
) -> Result<Json<TaskResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + TaskOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let task_id = TaskId::parse(task_id).map_err(|_| PublicError::InvalidRequest)?;
    let dependency_task_ids = request
        .dependency_task_ids
        .into_iter()
        .map(TaskId::parse)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| PublicError::InvalidRequest)?;
    let mutation = state
        .session_operations
        .update_task(UpdateTask {
            task_id,
            objective: request.objective,
            dependency_task_ids,
            idempotency_key: required_idempotency_key(&headers)?,
        })
        .await
        .map_err(PublicError::from)?;
    state.event_broadcaster.publish(mutation.events);
    Ok(Json(task_response(&mutation.value)))
}

async fn transition_task<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(task_id): Path<String>,
    headers: axum::http::HeaderMap,
    StrictJson(request): StrictJson<TransitionTaskRequest>,
) -> Result<Json<TaskResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + TaskOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let mutation = state
        .session_operations
        .transition_task(TransitionTask {
            task_id: TaskId::parse(task_id).map_err(|_| PublicError::InvalidRequest)?,
            state: task_state_request(request.state),
            idempotency_key: required_idempotency_key(&headers)?,
        })
        .await
        .map_err(PublicError::from)?;
    state.event_broadcaster.publish(mutation.events);
    Ok(Json(task_response(&mutation.value)))
}

async fn assign_task<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(task_id): Path<String>,
    headers: axum::http::HeaderMap,
    StrictJson(request): StrictJson<AssignTaskRequest>,
) -> Result<Json<TaskResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + TaskOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let mutation = state
        .session_operations
        .assign_task(AssignTask {
            task_id: TaskId::parse(task_id).map_err(|_| PublicError::InvalidRequest)?,
            run_id: RunId::parse(request.run_id).map_err(|_| PublicError::InvalidRequest)?,
            idempotency_key: required_idempotency_key(&headers)?,
        })
        .await
        .map_err(PublicError::from)?;
    state.event_broadcaster.publish(mutation.events);
    Ok(Json(task_response(&mutation.value)))
}

fn required_idempotency_key(headers: &axum::http::HeaderMap) -> Result<String, PublicError> {
    let value = headers
        .get(IDEMPOTENCY_KEY_HEADER)
        .ok_or(PublicError::MissingIdempotencyKey)?
        .to_str()
        .map_err(|_| PublicError::InvalidIdempotencyKey)?;
    if value.is_empty() {
        return Err(PublicError::InvalidIdempotencyKey);
    }
    Ok(value.to_owned())
}

async fn start_run<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(session_id): Path<String>,
    headers: axum::http::HeaderMap,
    StrictJson(request): StrictJson<StartRunRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let session_id = SessionId::parse(session_id).map_err(|_| PublicError::InvalidRequest)?;
    let idempotency_key = required_idempotency_key(&headers)?;
    let approval_policy = match request.approval_policy {
        ProtocolApprovalPolicy::Ask => kiln_core::ApprovalPolicy::Ask,
        ProtocolApprovalPolicy::ReadOnly => kiln_core::ApprovalPolicy::ReadOnly,
        ProtocolApprovalPolicy::FullAccess => kiln_core::ApprovalPolicy::FullAccess,
    };
    let root = kiln_core::WorkspaceRootId::parse(request.workspace_root_id)
        .map_err(|_| PublicError::InvalidRequest)?;
    let requested_scope = kiln_core::WorkspacePathScope::new(root, request.relative_directory)
        .map_err(|_| PublicError::Run(RunError::PathOutsideWorkspaceRoot))?;
    let run = state
        .run_operations
        .start_run(
            session_id,
            idempotency_key,
            approval_policy,
            requested_scope,
        )
        .await
        .map_err(PublicError::from)?;
    Ok((StatusCode::ACCEPTED, Json(run_response(&run.value))))
}

async fn start_child_run<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(parent_run_id): Path<String>,
    headers: axum::http::HeaderMap,
    StrictJson(request): StrictJson<StartChildRunRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let task_id = request
        .task_id
        .map(TaskId::parse)
        .transpose()
        .map_err(|_| PublicError::InvalidRequest)?;
    let user_input_mode = match request.user_input_mode {
        RunInputMode::Interactive => CoreRunInputMode::Interactive,
        RunInputMode::ReadOnly => CoreRunInputMode::ReadOnly,
    };
    let approval_policy = match request.approval_policy {
        ProtocolApprovalPolicy::Ask => kiln_core::ApprovalPolicy::Ask,
        ProtocolApprovalPolicy::ReadOnly => kiln_core::ApprovalPolicy::ReadOnly,
        ProtocolApprovalPolicy::FullAccess => kiln_core::ApprovalPolicy::FullAccess,
    };
    let root = kiln_core::WorkspaceRootId::parse(request.workspace_root_id)
        .map_err(|_| PublicError::InvalidRequest)?;
    let requested_scope = kiln_core::WorkspacePathScope::new(root, request.relative_directory)
        .map_err(|_| PublicError::Run(RunError::PathOutsideWorkspaceRoot))?;
    let run = state
        .run_operations
        .start_child_run(
            RunId::parse(parent_run_id).map_err(|_| PublicError::InvalidRequest)?,
            task_id,
            user_input_mode,
            required_idempotency_key(&headers)?,
            approval_policy,
            requested_scope,
        )
        .await
        .map_err(PublicError::from)?;
    Ok((StatusCode::ACCEPTED, Json(run_response(&run.value))))
}

async fn send_run_input<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(run_id): Path<String>,
    headers: axum::http::HeaderMap,
    StrictJson(request): StrictJson<SendRunInputRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let delivery = state
        .run_operations
        .send_run_input(SendRunInput {
            run_id: RunId::parse(run_id).map_err(|_| PublicError::InvalidRequest)?,
            content: request.content,
            attachments: parse_attachments(request.attachments)?,
            delivery_mode: match request.delivery_mode {
                MessageDeliveryMode::Queued => CoreMessageDeliveryMode::Queued,
                MessageDeliveryMode::Interrupt => CoreMessageDeliveryMode::Interrupt,
            },
            idempotency_key: required_idempotency_key(&headers)?,
        })
        .await
        .map_err(PublicError::from)?;
    Ok((
        StatusCode::CREATED,
        Json(message_delivery_response(&delivery.value)),
    ))
}

async fn react_to_run_activity<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(run_id): Path<String>,
    headers: axum::http::HeaderMap,
    StrictJson(request): StrictJson<ReactToRunActivityRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let delivery = state
        .run_operations
        .react_to_run_activity(ReactToRunActivity {
            run_id: RunId::parse(run_id).map_err(|_| PublicError::InvalidRequest)?,
            content: request.content,
            attachments: parse_attachments(request.attachments)?,
            child_activity: CoreChildActivityReference {
                run_id: RunId::parse(request.child_activity.run_id)
                    .map_err(|_| PublicError::InvalidRequest)?,
                event_id: kiln_core::EventId::parse(request.child_activity.event_id)
                    .map_err(|_| PublicError::InvalidRequest)?,
            },
            idempotency_key: required_idempotency_key(&headers)?,
        })
        .await
        .map_err(PublicError::from)?;
    Ok((
        StatusCode::CREATED,
        Json(message_delivery_response(&delivery.value)),
    ))
}

async fn list_session_runs<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionRunsResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let runs = state
        .run_operations
        .list_session_runs(SessionId::parse(session_id).map_err(|_| PublicError::InvalidRequest)?)
        .await
        .map_err(PublicError::from)?;
    Ok(Json(SessionRunsResponse {
        runs: runs.iter().map(run_response).collect(),
    }))
}

async fn decide_approval<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(tool_call_id): Path<String>,
    headers: axum::http::HeaderMap,
    StrictJson(request): StrictJson<ApprovalDecisionRequest>,
) -> Result<Json<RunResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let tool_call_id =
        kiln_core::ToolCallId::parse(tool_call_id).map_err(|_| PublicError::InvalidRequest)?;
    let idempotency_key = required_idempotency_key(&headers)?;
    let decision = match request.decision {
        ApprovalDecision::Approved => kiln_core::ApprovalState::Approved,
        ApprovalDecision::Rejected => kiln_core::ApprovalState::Rejected,
    };
    let approval = state
        .run_operations
        .decide_approval(tool_call_id, decision, idempotency_key)
        .await
        .map_err(PublicError::from)?;
    Ok(Json(run_response(&approval.value)))
}

async fn get_run<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(run_id): Path<String>,
) -> Result<Json<RunResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let run_id = RunId::parse(run_id).map_err(|_| PublicError::InvalidRequest)?;
    let run = state
        .run_operations
        .get_run(run_id)
        .await
        .map_err(PublicError::from)?;
    Ok(Json(run_response(&run)))
}

async fn get_artifact<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(content_hash): Path<String>,
) -> Result<Response, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + ArtifactOperations + 'static,
{
    let content_hash =
        ContentHash::parse(content_hash).map_err(|_| PublicError::InvalidContentHash)?;
    let download = state
        .run_operations
        .get_artifact(content_hash)
        .await
        .map_err(PublicError::from)?;
    let mut response = download.bytes.into_response();
    let headers = response.headers_mut();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(download.artifact.media_type())
            .expect("validated artifact media type is a valid header value"),
    );
    headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&download.artifact.size().to_string())
            .expect("artifact size is a valid header value"),
    );
    headers.insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment; filename=\"artifact\""),
    );
    headers.insert(
        ETAG,
        HeaderValue::from_str(&format!(
            "\"{}\"",
            download.artifact.content_hash().as_str()
        ))
        .expect("content hash is a valid ETag"),
    );
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=31536000, immutable"),
    );
    headers.insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static("sandbox; default-src 'none'"),
    );
    headers.insert(
        HeaderName::from_static("cross-origin-resource-policy"),
        HeaderValue::from_static("same-origin"),
    );
    Ok(response)
}

async fn upload_artifact<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    headers: axum::http::HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + ArtifactOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let session_id = headers
        .get(ARTIFACT_SESSION_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or(PublicError::InvalidRequest)
        .and_then(|value| SessionId::parse(value).map_err(|_| PublicError::InvalidRequest))?;
    if headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > MAX_ARTIFACT_UPLOAD_BYTES as u64)
    {
        return Err(PublicError::ArtifactUpload(ArtifactUploadError::TooLarge));
    }
    let bytes = to_bytes(body, MAX_ARTIFACT_UPLOAD_BYTES)
        .await
        .map_err(|_| PublicError::ArtifactUpload(ArtifactUploadError::TooLarge))?
        .to_vec();
    let media_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_owned();
    let artifact = state
        .run_operations
        .upload_artifact(session_id, bytes, media_type)
        .await
        .map_err(PublicError::from)?;
    Ok((StatusCode::CREATED, Json(artifact_response(&artifact))))
}

async fn cancel_run<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(run_id): Path<String>,
) -> Result<Json<RunResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let command = state.lifecycle.begin_command()?;
    let run_id = RunId::parse(run_id).map_err(|_| PublicError::InvalidRequest)?;
    drop(command);
    let run = state
        .run_operations
        .cancel_run(run_id)
        .await
        .map_err(PublicError::from)?;
    Ok(Json(run_response(&run)))
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionEventsQuery {
    after: Option<String>,
}

async fn list_session_events<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(session_id): Path<String>,
    query: Result<Query<SessionEventsQuery>, QueryRejection>,
) -> Result<Json<SessionEventsResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let session_id = SessionId::parse(session_id).map_err(|_| PublicError::InvalidRequest)?;
    let Query(query) = query.map_err(|_| PublicError::InvalidRequest)?;
    let after = match query.after {
        Some(value) => EventCursor::parse(&value).map_err(|_| PublicError::InvalidEventCursor)?,
        None => EventCursor::zero(),
    };
    let page = state
        .session_operations
        .list_session_events(session_id, after)
        .await
        .map_err(PublicError::from)?;
    Ok(Json(session_events_response(&page)))
}

async fn list_session_changes<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionChangesResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let session_id = SessionId::parse(session_id).map_err(|_| PublicError::InvalidRequest)?;
    let session = state
        .session_operations
        .get_session(session_id)
        .await
        .map_err(PublicError::from)?;
    let checkout = session
        .checkout()
        .cloned()
        .ok_or(PublicError::Session(SessionError::WorkspaceRootNotFound))?;
    let summary = state
        .workspace_operations
        .summarize_changes(checkout)
        .await
        .map_err(PublicError::from)?;
    Ok(Json(session_changes_response(&summary)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionChangeDiffQuery {
    path: String,
}

async fn get_session_change_diff<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(session_id): Path<String>,
    query: Result<Query<SessionChangeDiffQuery>, QueryRejection>,
) -> Result<Json<SessionChangeDiffResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let session_id = SessionId::parse(session_id).map_err(|_| PublicError::InvalidRequest)?;
    let Query(query) = query.map_err(|_| PublicError::InvalidRequest)?;
    let path = WorkspaceChangePath::parse(query.path)
        .map_err(|_| PublicError::Workspace(WorkspaceError::PathOutsideWorkspaceRoot))?;
    let session = state
        .session_operations
        .get_session(session_id)
        .await
        .map_err(PublicError::from)?;
    let checkout = session
        .checkout()
        .cloned()
        .ok_or(PublicError::Session(SessionError::WorkspaceRootNotFound))?;
    let diff = state
        .workspace_operations
        .diff_change(checkout, path)
        .await
        .map_err(PublicError::from)?;
    Ok(Json(session_change_diff_response(&diff)))
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct UsageLedgerQuery {
    after: Option<String>,
    limit: Option<String>,
}

async fn list_usage_ledger<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    query: Result<Query<UsageLedgerQuery>, QueryRejection>,
) -> Result<Json<UsageLedgerResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let Query(query) = query.map_err(|_| PublicError::InvalidRequest)?;
    let after = query
        .after
        .map(ModelInvocationId::parse)
        .transpose()
        .map_err(|_| PublicError::InvalidUsageCursor)?;
    let limit = query
        .limit
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| PublicError::InvalidUsageLimit)
        })
        .transpose()?
        .unwrap_or(DEFAULT_USAGE_PAGE_LIMIT);
    if !(1..=MAX_USAGE_PAGE_LIMIT).contains(&limit) {
        return Err(PublicError::InvalidUsageLimit);
    }
    let page = state
        .usage_operations
        .list_usage_ledger(after, limit)
        .await
        .map_err(PublicError::from)?;
    Ok(Json(usage_ledger_response(&page)))
}

fn session_response(session: &Session) -> SessionResponse {
    SessionResponse {
        session_id: session.id().as_str().to_owned(),
        workspace_id: session.workspace_id().as_str().to_owned(),
    }
}

fn message_response(message: &Message) -> MessageResponse {
    MessageResponse {
        message_id: message.id().as_str().to_owned(),
        session_id: message.session_id().as_str().to_owned(),
        role: match message.role() {
            CoreMessageRole::User => MessageRole::User,
            CoreMessageRole::Assistant => MessageRole::Assistant,
        },
        content: message.content().to_owned(),
        attachments: message
            .attachments()
            .iter()
            .map(artifact_response)
            .collect(),
        status: match message.status() {
            kiln_core::MessageStatus::Complete => kiln_protocol::MessageStatus::Complete,
            kiln_core::MessageStatus::Incomplete => kiln_protocol::MessageStatus::Incomplete,
        },
        origin_run_id: message
            .origin()
            .map(|origin| origin.run_id.as_str().to_owned()),
        model_invocation_id: message
            .origin()
            .map(|origin| origin.model_invocation_id.as_str().to_owned()),
        target_run_id: message
            .target_run_id()
            .map(|run_id| run_id.as_str().to_owned()),
        child_activity: message
            .child_activity()
            .map(|reference| ChildActivityReference {
                run_id: reference.run_id.as_str().to_owned(),
                event_id: reference.event_id.as_str().to_owned(),
            }),
    }
}

fn message_delivery_response(delivery: &MessageDelivery) -> MessageDeliveryResponse {
    MessageDeliveryResponse {
        message: message_response(delivery.message()),
        delivery_mode: match delivery.mode() {
            CoreMessageDeliveryMode::Queued => MessageDeliveryMode::Queued,
            CoreMessageDeliveryMode::Interrupt => MessageDeliveryMode::Interrupt,
        },
        state: match delivery.state() {
            CoreMessageDeliveryState::Queued => MessageDeliveryState::Queued,
            CoreMessageDeliveryState::Delivered => MessageDeliveryState::Delivered,
            CoreMessageDeliveryState::Failed => MessageDeliveryState::Failed,
            CoreMessageDeliveryState::Cancelled => MessageDeliveryState::Cancelled,
        },
    }
}

fn task_response(task: &Task) -> TaskResponse {
    TaskResponse {
        task_id: task.task_id().as_str().to_owned(),
        session_id: task.session_id().as_str().to_owned(),
        objective: task.objective().to_owned(),
        state: task_state_response(task.state()),
        parent_task_id: task
            .parent_task_id()
            .map(|task_id| task_id.as_str().to_owned()),
        dependency_task_ids: task
            .dependency_task_ids()
            .iter()
            .map(|task_id| task_id.as_str().to_owned())
            .collect(),
        assigned_run_id: task
            .assigned_run_id()
            .map(|run_id| run_id.as_str().to_owned()),
    }
}

fn task_state_response(state: CoreTaskState) -> TaskState {
    match state {
        CoreTaskState::Pending => TaskState::Pending,
        CoreTaskState::Ready => TaskState::Ready,
        CoreTaskState::Running => TaskState::Running,
        CoreTaskState::Blocked => TaskState::Blocked,
        CoreTaskState::Completed => TaskState::Completed,
        CoreTaskState::Failed => TaskState::Failed,
        CoreTaskState::Cancelled => TaskState::Cancelled,
    }
}

fn task_state_request(state: TaskState) -> CoreTaskState {
    match state {
        TaskState::Pending => CoreTaskState::Pending,
        TaskState::Ready => CoreTaskState::Ready,
        TaskState::Running => CoreTaskState::Running,
        TaskState::Blocked => CoreTaskState::Blocked,
        TaskState::Completed => CoreTaskState::Completed,
        TaskState::Failed => CoreTaskState::Failed,
        TaskState::Cancelled => CoreTaskState::Cancelled,
    }
}

fn run_response(snapshot: &RunSnapshot) -> RunResponse {
    RunResponse {
        run_id: snapshot.run().run_id().as_str().to_owned(),
        session_id: snapshot.run().session_id().as_str().to_owned(),
        parent_run_id: snapshot
            .run()
            .parent_run_id()
            .map(|run_id| run_id.as_str().to_owned()),
        task_id: snapshot
            .run()
            .task_id()
            .map(|task_id| task_id.as_str().to_owned()),
        user_input_mode: match snapshot.run().user_input_mode() {
            CoreRunInputMode::Interactive => RunInputMode::Interactive,
            CoreRunInputMode::ReadOnly => RunInputMode::ReadOnly,
        },
        state: run_state_response(snapshot.run().state()),
        approval_policy: snapshot.run().approval_policy().map(|policy| match policy {
            kiln_core::ApprovalPolicy::Ask => ProtocolApprovalPolicy::Ask,
            kiln_core::ApprovalPolicy::ReadOnly => ProtocolApprovalPolicy::ReadOnly,
            kiln_core::ApprovalPolicy::FullAccess => ProtocolApprovalPolicy::FullAccess,
        }),
        requested_scope: snapshot.run().requested_scope().map(scope_response),
        tool_calls: snapshot
            .tool_calls()
            .iter()
            .map(tool_call_response)
            .collect(),
        approvals: snapshot.approvals().iter().map(approval_response).collect(),
    }
}

fn scope_response(scope: &kiln_core::WorkspacePathScope) -> WorkspaceScopeResponse {
    WorkspaceScopeResponse {
        workspace_root_id: scope.workspace_root_id().as_str().to_owned(),
        relative_directory: scope.relative_directory().to_owned(),
    }
}

fn session_changes_response(summary: &kiln_core::WorkspaceChangeSummary) -> SessionChangesResponse {
    SessionChangesResponse {
        workspace_root_id: summary.checkout().workspace_root_id().as_str().to_owned(),
        relative_directory: summary.checkout().relative_directory().to_owned(),
        files: summary
            .files()
            .iter()
            .map(|file| ChangedFileResponse {
                path: file.path().to_owned(),
                kind: file.kind().as_str().to_owned(),
                additions: file.additions(),
                deletions: file.deletions(),
            })
            .collect(),
    }
}

fn session_change_diff_response(
    diff: &kiln_core::WorkspaceChangeDiff,
) -> SessionChangeDiffResponse {
    let content = match diff.content() {
        kiln_core::WorkspaceChangeDiffContent::Text { patch, truncated } => {
            SessionChangeDiffContent::Ready {
                patch: patch.clone(),
                truncated: *truncated,
            }
        }
        kiln_core::WorkspaceChangeDiffContent::Unavailable(reason) => {
            SessionChangeDiffContent::Unavailable {
                reason: match reason {
                    kiln_core::WorkspaceChangeDiffUnavailableReason::Untracked => {
                        SessionChangeDiffUnavailableReason::Untracked
                    }
                    kiln_core::WorkspaceChangeDiffUnavailableReason::Binary => {
                        SessionChangeDiffUnavailableReason::Binary
                    }
                    kiln_core::WorkspaceChangeDiffUnavailableReason::Conflicted => {
                        SessionChangeDiffUnavailableReason::Conflicted
                    }
                    kiln_core::WorkspaceChangeDiffUnavailableReason::Renamed => {
                        SessionChangeDiffUnavailableReason::Renamed
                    }
                    kiln_core::WorkspaceChangeDiffUnavailableReason::UnsupportedFileType => {
                        SessionChangeDiffUnavailableReason::UnsupportedFileType
                    }
                    kiln_core::WorkspaceChangeDiffUnavailableReason::UnsupportedEncoding => {
                        SessionChangeDiffUnavailableReason::UnsupportedEncoding
                    }
                },
            }
        }
    };
    SessionChangeDiffResponse {
        workspace_root_id: diff.checkout().workspace_root_id().as_str().to_owned(),
        relative_directory: diff.checkout().relative_directory().to_owned(),
        path: diff.file().path().to_owned(),
        kind: diff.file().kind().as_str().to_owned(),
        content,
    }
}

fn usage_ledger_response(page: &UsageLedgerPage) -> UsageLedgerResponse {
    UsageLedgerResponse {
        entries: page
            .observations
            .iter()
            .map(usage_ledger_entry_response)
            .collect(),
        next_cursor: page
            .next_cursor
            .as_ref()
            .map(|cursor| cursor.as_str().to_owned()),
    }
}

fn usage_ledger_entry_response(
    observation: &kiln_core::UsageObservation,
) -> UsageLedgerEntryResponse {
    let metadata = observation.update.metadata();
    UsageLedgerEntryResponse {
        usage_observation_id: observation.observation_id.as_str().to_owned(),
        model_invocation_id: observation.model_invocation_id.as_str().to_owned(),
        work_id: observation.work_id.as_str().to_owned(),
        run_id: observation.run_id.as_str().to_owned(),
        session_id: observation.session_id.as_str().to_owned(),
        workspace_id: observation.workspace_id.as_str().to_owned(),
        provider_account_id: observation.provider_account_id.as_str().to_owned(),
        requested_model: observation.requested_model.as_str().to_owned(),
        revision: observation.revision,
        supersedes_usage_observation_id: observation
            .supersedes
            .as_ref()
            .map(|id| id.as_str().to_owned()),
        update_id: metadata.update_id.clone(),
        accounting: match metadata.accounting {
            kiln_core::UsageAccounting::Delta => UsageAccounting::Delta,
            kiln_core::UsageAccounting::Cumulative => UsageAccounting::Cumulative,
        },
        finality: match metadata.finality {
            kiln_core::UsageFinality::Partial => UsageFinality::Partial,
            kiln_core::UsageFinality::Final => UsageFinality::Final,
            kiln_core::UsageFinality::Correction => UsageFinality::Correction,
        },
        completeness: match observation.completeness {
            kiln_core::UsageCompleteness::Complete => UsageCompleteness::Complete,
            kiln_core::UsageCompleteness::Partial => UsageCompleteness::Partial,
            kiln_core::UsageCompleteness::Unknown => UsageCompleteness::Unknown,
        },
        observed_at_unix_ms: metadata.observed_at_unix_ms,
        request_id: metadata.request_id.clone(),
        resolved_model: metadata.resolved_model.clone(),
        service_tier: metadata.service_tier.clone(),
        source: match metadata.source {
            kiln_core::UsageSource::NativeProvider => UsageSource::NativeProvider,
        },
        quantities: observation
            .quantities
            .iter()
            .map(|quantity| UsageQuantityResponse {
                dimension: quantity.dimension().to_owned(),
                unit: quantity.unit().to_owned(),
                amount: quantity.amount(),
                relation: match quantity.relation() {
                    kiln_core::QuantityRelation::Additive => UsageQuantityRelation::Additive,
                    kiln_core::QuantityRelation::Subset { .. } => UsageQuantityRelation::Subset,
                    kiln_core::QuantityRelation::Informational => {
                        UsageQuantityRelation::Informational
                    }
                },
                subset_of: match quantity.relation() {
                    kiln_core::QuantityRelation::Subset { of } => Some(of.clone()),
                    kiln_core::QuantityRelation::Additive
                    | kiln_core::QuantityRelation::Informational => None,
                },
            })
            .collect(),
        is_terminal: observation.is_terminal,
    }
}

fn approval_response(approval: &kiln_core::Approval) -> ApprovalResponse {
    ApprovalResponse {
        approval_id: approval.approval_id().as_str().to_owned(),
        run_id: approval.run_id().as_str().to_owned(),
        tool_call_id: approval.tool_call_id().as_str().to_owned(),
        requested_scope: scope_response(approval.scope()),
        state: match approval.state() {
            kiln_core::ApprovalState::Pending => ProtocolApprovalState::Pending,
            kiln_core::ApprovalState::Approved => ProtocolApprovalState::Approved,
            kiln_core::ApprovalState::Rejected => ProtocolApprovalState::Rejected,
        },
    }
}

fn tool_call_response(tool_call: &ToolCall) -> ToolCallResponse {
    ToolCallResponse {
        tool_call_id: tool_call.tool_call_id().as_str().to_owned(),
        run_id: tool_call.run_id().as_str().to_owned(),
        capability: tool_call.capability().to_owned(),
        state: tool_call_state_response(tool_call.state()),
        requested_scope: tool_call.requested_scope().map(scope_response),
        effective_scope: tool_call.effective_scope().map(scope_response),
        stdout: tool_call.stdout().map(str::to_owned),
        stderr: tool_call.stderr().map(str::to_owned),
        stdout_artifact: tool_call.stdout_artifact().map(artifact_response),
        stderr_artifact: tool_call.stderr_artifact().map(artifact_response),
        exit_code: tool_call.exit_code(),
    }
}

fn artifact_response(artifact: &Artifact) -> ArtifactResponse {
    ArtifactResponse {
        content_hash: artifact.content_hash().as_str().to_owned(),
        media_type: artifact.media_type().to_owned(),
        size: artifact.size().to_string(),
    }
}

fn parse_attachments(values: Vec<ArtifactResponse>) -> Result<Vec<Artifact>, PublicError> {
    values
        .into_iter()
        .map(|value| {
            let content_hash =
                ContentHash::parse(value.content_hash).map_err(|_| PublicError::InvalidRequest)?;
            let size = value
                .size
                .parse::<u64>()
                .map_err(|_| PublicError::InvalidRequest)?;
            Artifact::new(content_hash, value.media_type, size)
                .map_err(|_| PublicError::InvalidRequest)
        })
        .collect()
}

fn run_state_response(state: CoreRunState) -> RunState {
    match state {
        CoreRunState::Queued => RunState::Queued,
        CoreRunState::Running => RunState::Running,
        CoreRunState::WaitingForApproval => RunState::WaitingForApproval,
        CoreRunState::Cancelling => RunState::Cancelling,
        CoreRunState::Completed => RunState::Completed,
        CoreRunState::Failed => RunState::Failed,
        CoreRunState::Cancelled => RunState::Cancelled,
    }
}

fn tool_call_state_response(state: CoreToolCallState) -> ToolCallState {
    match state {
        CoreToolCallState::Requested => ToolCallState::Requested,
        CoreToolCallState::AwaitingApproval => ToolCallState::AwaitingApproval,
        CoreToolCallState::Ready => ToolCallState::Ready,
        CoreToolCallState::Running => ToolCallState::Running,
        CoreToolCallState::Completed => ToolCallState::Completed,
        CoreToolCallState::Failed => ToolCallState::Failed,
        CoreToolCallState::Cancelled => ToolCallState::Cancelled,
        CoreToolCallState::Denied => ToolCallState::Denied,
    }
}

fn tool_output_stream_response(stream: CoreToolOutputStream) -> ToolOutputStream {
    match stream {
        CoreToolOutputStream::Stdout => ToolOutputStream::Stdout,
        CoreToolOutputStream::Stderr => ToolOutputStream::Stderr,
    }
}

fn model_invocation_event_response(
    invocation: &kiln_core::ModelInvocation,
) -> kiln_protocol::ModelInvocationEventResponse {
    use kiln_core::{ModelInvocationOutcome as Outcome, ModelInvocationState as State};
    use kiln_protocol::ModelInvocationStatus as Status;

    let status = match (invocation.state(), invocation.outcome()) {
        (State::Pending, None) => Status::Pending,
        (State::InFlight, None) => Status::InFlight,
        (State::Completed, Some(Outcome::Completed { completion_kind })) => Status::Completed {
            completion_kind: match completion_kind {
                kiln_core::ModelInvocationCompletionKind::AssistantOutput => {
                    kiln_protocol::ModelInvocationCompletionKind::AssistantOutput
                }
                kiln_core::ModelInvocationCompletionKind::ToolRequests => {
                    kiln_protocol::ModelInvocationCompletionKind::ToolRequests
                }
            },
        },
        (State::Failed, Some(Outcome::Failed { reason })) => Status::Failed {
            reason: match reason {
                kiln_core::ModelInvocationFailureReason::ProviderError => {
                    kiln_protocol::ModelInvocationFailureReason::ProviderError
                }
                kiln_core::ModelInvocationFailureReason::InvalidRequest => {
                    kiln_protocol::ModelInvocationFailureReason::InvalidRequest
                }
                kiln_core::ModelInvocationFailureReason::Unknown => {
                    kiln_protocol::ModelInvocationFailureReason::Unknown
                }
            },
        },
        (State::Cancelled, Some(Outcome::Cancelled)) => Status::Cancelled,
        (State::Interrupted, Some(Outcome::Interrupted)) => Status::Interrupted,
        _ => unreachable!("ModelInvocation validates its state and outcome"),
    };
    kiln_protocol::ModelInvocationEventResponse {
        model_invocation_id: invocation.invocation_id().as_str().to_owned(),
        work_id: invocation.work_id().as_str().to_owned(),
        run_id: invocation.run_id().as_str().to_owned(),
        context_manifest_id: invocation.context_manifest_id().as_str().to_owned(),
        context_manifest_hash: invocation.context_manifest_hash().as_str().to_owned(),
        provider_account_id: invocation.provider_account_id().as_str().to_owned(),
        provider: invocation.settings().provider().as_str().to_owned(),
        model: invocation.settings().model().as_str().to_owned(),
        purpose: match invocation.purpose() {
            kiln_core::ModelInvocationPurpose::Generation => {
                kiln_protocol::ModelInvocationPurpose::Generation
            }
            kiln_core::ModelInvocationPurpose::Compaction => {
                kiln_protocol::ModelInvocationPurpose::Compaction
            }
        },
        retry_of: invocation.retry_of().map(|id| id.as_str().to_owned()),
        status,
    }
}

fn session_event_response(event: &StoredSessionEvent) -> SessionEventResponse {
    let event_data = match event.payload() {
        SessionEventPayload::ContextManifestCreated {
            context_manifest_id,
            run_id,
            content_hash,
            entry_count,
        } => SessionEventDataResponse::ContextManifestCreated(
            kiln_protocol::ContextManifestCreatedResponse {
                context_manifest_id: context_manifest_id.as_str().to_owned(),
                run_id: run_id.as_str().to_owned(),
                content_hash: content_hash.as_str().to_owned(),
                entry_count: *entry_count,
            },
        ),
        SessionEventPayload::ModelOutputRecorded { chunk } => {
            SessionEventDataResponse::ModelOutputRecorded(
                kiln_protocol::ModelOutputRecordedResponse {
                    output_chunk_id: chunk.output_chunk_id.as_str().to_owned(),
                    model_invocation_id: chunk.model_invocation_id.as_str().to_owned(),
                    run_id: chunk.run_id.as_str().to_owned(),
                    position: chunk.position,
                    stream: match chunk.stream {
                        kiln_core::ModelOutputStream::AssistantText => {
                            kiln_protocol::ModelOutputStream::AssistantText
                        }
                        kiln_core::ModelOutputStream::ReasoningSummary => {
                            kiln_protocol::ModelOutputStream::ReasoningSummary
                        }
                    },
                    content: chunk.content.clone(),
                },
            )
        }
        SessionEventPayload::UsageObserved { observation } => {
            SessionEventDataResponse::UsageObserved(kiln_protocol::UsageObservedResponse {
                usage_observation_id: observation.observation_id.as_str().to_owned(),
                model_invocation_id: observation.model_invocation_id.as_str().to_owned(),
                work_id: observation.work_id.as_str().to_owned(),
                run_id: observation.run_id.as_str().to_owned(),
                provider_account_id: observation.provider_account_id.as_str().to_owned(),
                revision: observation.revision,
                supersedes_usage_observation_id: observation
                    .supersedes
                    .as_ref()
                    .map(|id| id.as_str().to_owned()),
                completeness: match observation.completeness {
                    kiln_core::UsageCompleteness::Complete => {
                        kiln_protocol::UsageCompleteness::Complete
                    }
                    kiln_core::UsageCompleteness::Partial => {
                        kiln_protocol::UsageCompleteness::Partial
                    }
                    kiln_core::UsageCompleteness::Unknown => {
                        kiln_protocol::UsageCompleteness::Unknown
                    }
                },
                is_terminal: observation.is_terminal,
            })
        }
        SessionEventPayload::ModelInvocationCreated { invocation } => {
            SessionEventDataResponse::ModelInvocationCreated(model_invocation_event_response(
                invocation,
            ))
        }
        SessionEventPayload::ModelInvocationStateChanged { invocation } => {
            SessionEventDataResponse::ModelInvocationStateChanged(model_invocation_event_response(
                invocation,
            ))
        }
        SessionEventPayload::SessionCreated { workspace_id } => {
            SessionEventDataResponse::SessionCreated {
                workspace_id: workspace_id.as_str().to_owned(),
            }
        }
        SessionEventPayload::MessageAppended { message } => {
            SessionEventDataResponse::MessageAppended {
                message: message_response(message),
            }
        }
        SessionEventPayload::TaskCreated { task } => SessionEventDataResponse::TaskCreated {
            task: task_response(task),
        },
        SessionEventPayload::TaskUpdated { task } => SessionEventDataResponse::TaskUpdated {
            task: task_response(task),
        },
        SessionEventPayload::TaskAssigned { task } => SessionEventDataResponse::TaskAssigned {
            task: task_response(task),
        },
        SessionEventPayload::TaskStateChanged { task } => {
            SessionEventDataResponse::TaskStateChanged {
                task: task_response(task),
            }
        }
        SessionEventPayload::RunCreated {
            run_id,
            state,
            parent_run_id,
            task_id,
            user_input_mode,
            approval_policy,
            requested_scope,
        } => SessionEventDataResponse::RunCreated {
            run_id: run_id.as_str().to_owned(),
            state: run_state_response(*state),
            parent_run_id: parent_run_id
                .as_ref()
                .map(|run_id| run_id.as_str().to_owned()),
            task_id: task_id.as_ref().map(|task_id| task_id.as_str().to_owned()),
            user_input_mode: match user_input_mode {
                CoreRunInputMode::Interactive => RunInputMode::Interactive,
                CoreRunInputMode::ReadOnly => RunInputMode::ReadOnly,
            },
            approval_policy: approval_policy.map(|policy| match policy {
                kiln_core::ApprovalPolicy::Ask => ProtocolApprovalPolicy::Ask,
                kiln_core::ApprovalPolicy::ReadOnly => ProtocolApprovalPolicy::ReadOnly,
                kiln_core::ApprovalPolicy::FullAccess => ProtocolApprovalPolicy::FullAccess,
            }),
            requested_scope: requested_scope.as_ref().map(scope_response),
        },
        SessionEventPayload::RunQueued { run_id } => SessionEventDataResponse::RunQueued {
            run_id: run_id.as_str().to_owned(),
        },
        SessionEventPayload::RunChildAdded {
            parent_run_id,
            child_run_id,
        } => SessionEventDataResponse::RunChildAdded {
            parent_run_id: parent_run_id.as_str().to_owned(),
            child_run_id: child_run_id.as_str().to_owned(),
        },
        SessionEventPayload::RunInputQueued { run_id, message_id } => {
            SessionEventDataResponse::RunInputQueued {
                run_id: run_id.as_str().to_owned(),
                message_id: message_id.as_str().to_owned(),
            }
        }
        SessionEventPayload::RunInterruptRequested { run_id, message_id } => {
            SessionEventDataResponse::RunInterruptRequested {
                run_id: run_id.as_str().to_owned(),
                message_id: message_id.as_str().to_owned(),
            }
        }
        SessionEventPayload::RunInputDelivered { run_id, message_id } => {
            SessionEventDataResponse::RunInputDelivered {
                run_id: run_id.as_str().to_owned(),
                message_id: message_id.as_str().to_owned(),
            }
        }
        SessionEventPayload::RunInputFailed { run_id, message_id } => {
            SessionEventDataResponse::RunInputFailed {
                run_id: run_id.as_str().to_owned(),
                message_id: message_id.as_str().to_owned(),
            }
        }
        SessionEventPayload::RunInputCancelled { run_id, message_id } => {
            SessionEventDataResponse::RunInputCancelled {
                run_id: run_id.as_str().to_owned(),
                message_id: message_id.as_str().to_owned(),
            }
        }
        SessionEventPayload::RunStateChanged { run_id, state } => {
            SessionEventDataResponse::RunStateChanged {
                run_id: run_id.as_str().to_owned(),
                state: run_state_response(*state),
            }
        }
        SessionEventPayload::RunCancellationRequested { run_id } => {
            SessionEventDataResponse::RunCancellationRequested {
                run_id: run_id.as_str().to_owned(),
            }
        }
        SessionEventPayload::ToolCallRequested { tool_call } => {
            SessionEventDataResponse::ToolCallRequested {
                tool_call: tool_call_response(tool_call),
            }
        }
        SessionEventPayload::ApprovalRequested { approval } => {
            SessionEventDataResponse::ApprovalRequested {
                approval: approval_response(approval),
            }
        }
        SessionEventPayload::ApprovalDecided { approval } => {
            SessionEventDataResponse::ApprovalDecided {
                approval: approval_response(approval),
            }
        }
        SessionEventPayload::ToolCallDenied { tool_call } => {
            SessionEventDataResponse::ToolCallDenied {
                tool_call: tool_call_response(tool_call),
            }
        }
        SessionEventPayload::ToolCallStateChanged { tool_call } => {
            SessionEventDataResponse::ToolCallStateChanged {
                tool_call: tool_call_response(tool_call),
            }
        }
        SessionEventPayload::ToolCallOutput {
            run_id,
            tool_call_id,
            stream,
            content,
        } => SessionEventDataResponse::ToolCallOutput {
            run_id: run_id.as_str().to_owned(),
            tool_call_id: tool_call_id.as_str().to_owned(),
            stream: tool_output_stream_response(*stream),
            content: content.clone(),
        },
        SessionEventPayload::ArtifactRegistered {
            run_id,
            tool_call_id,
            stream,
            artifact,
        } => SessionEventDataResponse::ArtifactRegistered {
            run_id: run_id.as_str().to_owned(),
            tool_call_id: tool_call_id.as_str().to_owned(),
            stream: tool_output_stream_response(*stream),
            artifact: artifact_response(artifact),
        },
    };
    SessionEventResponse {
        event_id: event.event_id().as_str().to_owned(),
        cursor: event.cursor().to_string(),
        session_id: event.session_id().as_str().to_owned(),
        event: event_data,
    }
}

fn session_events_response(page: &SessionEventPage) -> SessionEventsResponse {
    SessionEventsResponse {
        events: page.events().iter().map(session_event_response).collect(),
        current_event_cursor: page.current_cursor().to_string(),
    }
}

async fn negotiate<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    StrictJson(request): StrictJson<NegotiateRequest>,
) -> Result<Json<NegotiateResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let selected_version = select_version(&request)?;
    let supported_capabilities = vec![WEBSOCKET_CAPABILITY.to_owned()];
    let selected_capabilities = request
        .requested_capabilities
        .iter()
        .filter(|capability| capability.as_str() == WEBSOCKET_CAPABILITY)
        .cloned()
        .collect();
    let current_event_cursor = state
        .session_operations
        .current_event_cursor()
        .await
        .map_err(PublicError::from)?
        .map(|cursor| cursor.to_string());

    Ok(Json(NegotiateResponse {
        selected_version,
        supported_capabilities,
        selected_capabilities,
        store_identity: StoreIdentity {
            id: state.store.id,
            name: state.store.name,
        },
        current_event_cursor,
        event_websocket_endpoint: state.event_websocket_endpoint,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventQuery {
    version: Option<String>,
    capability: Option<String>,
    after: Option<String>,
}

async fn events<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    query: Result<Query<EventQuery>, QueryRejection>,
    websocket: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Result<Response, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let Query(query) = query.map_err(|_| PublicError::InvalidRequest)?;
    let version = query.version.ok_or(PublicError::MissingVersion)?;
    if version != PROTOCOL_VERSION {
        return Err(PublicError::UnsupportedVersion);
    }
    if query.capability.as_deref() != Some(WEBSOCKET_CAPABILITY) {
        return Err(PublicError::MissingCapability);
    }
    let websocket = websocket
        .map_err(|_| PublicError::WebSocketUpgradeRequired)?
        .protocols([WEBSOCKET_CAPABILITY]);
    let after = query
        .after
        .as_deref()
        .map(EventCursor::parse)
        .transpose()
        .map_err(|_| PublicError::InvalidEventCursor)?;
    let wake_receiver = state.event_broadcaster.subscribe_wake();
    let (acknowledged_cursor, initial_page) = match after {
        Some(after) => {
            let page = state
                .session_operations
                .list_events_after(after)
                .await
                .map_err(PublicError::from)?;
            if after > page.current_cursor() {
                return Err(PublicError::InvalidEventCursor);
            }
            (page.current_cursor(), Some(page))
        }
        None => (
            state
                .session_operations
                .current_event_cursor()
                .await
                .map_err(PublicError::from)?
                .unwrap_or_else(EventCursor::zero),
            None,
        ),
    };

    Ok(websocket
        .on_upgrade(move |socket| {
            event_socket(
                socket,
                acknowledged_cursor,
                initial_page,
                state.session_operations,
                wake_receiver,
            )
        })
        .into_response())
}

async fn event_socket<S>(
    mut socket: ws::WebSocket,
    acknowledged_cursor: EventCursor,
    initial_page: Option<SessionEventPage>,
    operations: Arc<S>,
    mut wake_receiver: tokio::sync::mpsc::UnboundedReceiver<()>,
) where
    S: SessionOperations + 'static,
{
    if send_frame(
        &mut socket,
        WebSocketFrame::Ack {
            version: PROTOCOL_VERSION.to_owned(),
            capability: WEBSOCKET_CAPABILITY.to_owned(),
            current_event_cursor: (acknowledged_cursor != EventCursor::zero())
                .then(|| acknowledged_cursor.to_string()),
        },
    )
    .await
    .is_err()
    {
        return;
    }

    let mut last_cursor = acknowledged_cursor;
    if let Some(page) = initial_page {
        for event in page.events() {
            if send_frame(
                &mut socket,
                WebSocketFrame::Event {
                    event: session_event_response(event),
                },
            )
            .await
            .is_err()
            {
                return;
            }
        }
        last_cursor = page.current_cursor();
    }

    loop {
        tokio::select! {
            wake = wake_receiver.recv() => {
                let Some(()) = wake else { return };
                let page = match operations.list_events_after(last_cursor).await {
                    Ok(page) => page,
                    Err(_) => return,
                };
                for event in page.events() {
                    if send_frame(&mut socket, WebSocketFrame::Event { event: session_event_response(event) }).await.is_err() {
                        return;
                    }
                }
                last_cursor = page.current_cursor();
            }
            message = socket.recv() => {
                match message {
                    Some(Ok(ws::Message::Ping(payload))) => {
                        if socket.send(ws::Message::Pong(payload)).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(ws::Message::Pong(_))) => {}
                    Some(Ok(ws::Message::Close(_))) | None | Some(Err(_)) => return,
                    Some(Ok(ws::Message::Text(_) | ws::Message::Binary(_))) => {
                        if send_frame(
                            &mut socket,
                            WebSocketFrame::Error {
                                code: error_code::UNSUPPORTED_INPUT.to_owned(),
                                message: "The event WebSocket accepts control frames only.".to_owned(),
                            },
                        )
                        .await
                        .is_err()
                        {
                            return;
                        }
                    }
                }
            }
        }
    }
}

async fn send_frame(socket: &mut ws::WebSocket, frame: WebSocketFrame) -> Result<(), axum::Error> {
    let payload = serde_json::to_string(&frame).expect("public frame is serializable");
    socket.send(ws::Message::Text(payload.into())).await
}

fn select_version(request: &NegotiateRequest) -> Result<String, PublicError> {
    let min = Version::parse(&request.min_version).map_err(|_| PublicError::InvalidVersion)?;
    let max = Version::parse(&request.max_version).map_err(|_| PublicError::InvalidVersion)?;
    let supported = Version::parse(PROTOCOL_VERSION).expect("constant protocol version is valid");
    if min > max || min > supported || max < supported {
        return Err(PublicError::UnsupportedVersion);
    }
    Ok(PROTOCOL_VERSION.to_owned())
}

async fn not_found() -> PublicError {
    PublicError::NotFound
}

async fn method_not_allowed() -> PublicError {
    PublicError::MethodNotAllowed
}

struct StrictJson<T>(T);

impl<S, T> FromRequest<S> for StrictJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(request, state)
            .await
            .map_err(|_| PublicError::InvalidJson.into_response())?;
        serde_json::from_slice(&bytes)
            .map(StrictJson)
            .map_err(|error| {
                if error.is_syntax() || error.is_eof() {
                    PublicError::InvalidJson.into_response()
                } else {
                    PublicError::InvalidRequest.into_response()
                }
            })
    }
}

#[derive(Debug, Error)]
enum PublicError {
    #[error("authentication is required")]
    AuthenticationRequired,
    #[error("authentication is invalid")]
    InvalidAuthentication,
    #[error("request Host is invalid")]
    InvalidHost,
    #[error("request Origin is invalid")]
    InvalidOrigin,
    #[error("request body is not valid JSON")]
    InvalidJson,
    #[error("request fields are invalid")]
    InvalidRequest,
    #[error("event cursor is invalid")]
    InvalidEventCursor,
    #[error("usage cursor is invalid")]
    InvalidUsageCursor,
    #[error("usage page limit is invalid")]
    InvalidUsageLimit,
    #[error("content hash is invalid")]
    InvalidContentHash,
    #[error("Idempotency-Key header is required")]
    MissingIdempotencyKey,
    #[error("Idempotency-Key header is invalid")]
    InvalidIdempotencyKey,
    #[error("protocol version is not supported")]
    UnsupportedVersion,
    #[error("protocol version is invalid")]
    InvalidVersion,
    #[error("the WebSocket capability is required")]
    MissingCapability,
    #[error("the WebSocket version is required")]
    MissingVersion,
    #[error("a WebSocket upgrade is required")]
    WebSocketUpgradeRequired,
    #[error("method is not allowed for this route")]
    MethodNotAllowed,
    #[error("route does not exist")]
    NotFound,
    #[error("workspace operation failed")]
    Workspace(WorkspaceError),
    #[error("session operation failed")]
    Session(SessionError),
    #[error("task operation failed")]
    Task(TaskError),
    #[error("run operation failed")]
    Run(RunError),
    #[error("artifact operation failed")]
    Artifact(ArtifactFetchError),
    #[error("artifact upload failed")]
    ArtifactUpload(ArtifactUploadError),
    #[error("usage query failed")]
    Usage(UsageQueryError),
    #[error("provider account operation failed")]
    ProviderAccount(ProviderAccountOperationError),
    #[error("daemon is shutting down")]
    DaemonShuttingDown,
}

impl From<WorkspaceError> for PublicError {
    fn from(error: WorkspaceError) -> Self {
        Self::Workspace(error)
    }
}

impl From<SessionError> for PublicError {
    fn from(error: SessionError) -> Self {
        Self::Session(error)
    }
}

impl From<TaskError> for PublicError {
    fn from(error: TaskError) -> Self {
        Self::Task(error)
    }
}

impl From<RunError> for PublicError {
    fn from(error: RunError) -> Self {
        Self::Run(error)
    }
}

impl From<ArtifactFetchError> for PublicError {
    fn from(error: ArtifactFetchError) -> Self {
        Self::Artifact(error)
    }
}

impl From<ArtifactUploadError> for PublicError {
    fn from(error: ArtifactUploadError) -> Self {
        Self::ArtifactUpload(error)
    }
}

impl From<UsageQueryError> for PublicError {
    fn from(error: UsageQueryError) -> Self {
        Self::Usage(error)
    }
}

impl From<ProviderAccountOperationError> for PublicError {
    fn from(error: ProviderAccountOperationError) -> Self {
        Self::ProviderAccount(error)
    }
}

impl PublicError {
    fn problem(&self) -> ProblemDetails {
        let (status, code, title) = match self {
            Self::AuthenticationRequired => (
                StatusCode::UNAUTHORIZED,
                error_code::AUTHENTICATION_REQUIRED,
                "Authentication required",
            ),
            Self::InvalidAuthentication => (
                StatusCode::UNAUTHORIZED,
                error_code::INVALID_AUTHENTICATION,
                "Invalid authentication",
            ),
            Self::InvalidHost => (
                StatusCode::BAD_REQUEST,
                error_code::INVALID_HOST,
                "Invalid Host",
            ),
            Self::InvalidOrigin => (
                StatusCode::BAD_REQUEST,
                error_code::INVALID_ORIGIN,
                "Invalid Origin",
            ),
            Self::InvalidJson => (
                StatusCode::BAD_REQUEST,
                error_code::INVALID_JSON,
                "Invalid request",
            ),
            Self::InvalidRequest => (
                StatusCode::BAD_REQUEST,
                error_code::INVALID_REQUEST,
                "Invalid request",
            ),
            Self::InvalidEventCursor => (
                StatusCode::BAD_REQUEST,
                error_code::INVALID_EVENT_CURSOR,
                "Invalid Event cursor",
            ),
            Self::InvalidUsageCursor => (
                StatusCode::BAD_REQUEST,
                error_code::INVALID_USAGE_CURSOR,
                "Invalid Usage cursor",
            ),
            Self::InvalidUsageLimit => (
                StatusCode::BAD_REQUEST,
                error_code::INVALID_USAGE_LIMIT,
                "Invalid Usage page limit",
            ),
            Self::InvalidContentHash => (
                StatusCode::BAD_REQUEST,
                error_code::INVALID_CONTENT_HASH,
                "Invalid content hash",
            ),
            Self::MissingIdempotencyKey => (
                StatusCode::BAD_REQUEST,
                error_code::IDEMPOTENCY_KEY_REQUIRED,
                "Missing Idempotency-Key",
            ),
            Self::InvalidIdempotencyKey => (
                StatusCode::BAD_REQUEST,
                error_code::INVALID_IDEMPOTENCY_KEY,
                "Invalid Idempotency-Key",
            ),
            Self::InvalidVersion => (
                StatusCode::BAD_REQUEST,
                error_code::INVALID_VERSION,
                "Invalid request",
            ),
            Self::UnsupportedVersion => (
                StatusCode::BAD_REQUEST,
                error_code::UNSUPPORTED_VERSION,
                "Unsupported protocol",
            ),
            Self::MissingCapability => (
                StatusCode::BAD_REQUEST,
                error_code::MISSING_CAPABILITY,
                "Missing capability",
            ),
            Self::MissingVersion => (
                StatusCode::BAD_REQUEST,
                error_code::MISSING_VERSION,
                "Missing protocol version",
            ),
            Self::WebSocketUpgradeRequired => (
                StatusCode::UPGRADE_REQUIRED,
                error_code::WEBSOCKET_UPGRADE_REQUIRED,
                "WebSocket upgrade required",
            ),
            Self::MethodNotAllowed => (
                StatusCode::METHOD_NOT_ALLOWED,
                error_code::METHOD_NOT_ALLOWED,
                "Method not allowed",
            ),
            Self::NotFound => (StatusCode::NOT_FOUND, error_code::NOT_FOUND, "Not found"),
            Self::DaemonShuttingDown => (
                StatusCode::SERVICE_UNAVAILABLE,
                error_code::DAEMON_SHUTTING_DOWN,
                "Daemon shutting down",
            ),
            Self::Workspace(error) => match error {
                WorkspaceError::WorkspaceNameRequired => (
                    StatusCode::BAD_REQUEST,
                    error_code::WORKSPACE_NAME_REQUIRED,
                    "Invalid workspace",
                ),
                WorkspaceError::WorkspaceRootRequired => (
                    StatusCode::BAD_REQUEST,
                    error_code::WORKSPACE_ROOT_REQUIRED,
                    "Invalid workspace",
                ),
                WorkspaceError::WorkspaceRootNameRequired => (
                    StatusCode::BAD_REQUEST,
                    error_code::WORKSPACE_ROOT_NAME_REQUIRED,
                    "Invalid workspace",
                ),
                WorkspaceError::WorkspaceRootNameConflict => (
                    StatusCode::CONFLICT,
                    error_code::WORKSPACE_ROOT_NAME_CONFLICT,
                    "Workspace root conflict",
                ),
                WorkspaceError::WorkspaceRootIdentityConflict
                | WorkspaceError::WorkspaceRootOrderInvalid => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_code::WORKSPACE_STORE_UNAVAILABLE,
                    "Workspace store unavailable",
                ),
                WorkspaceError::WorkspaceRootMissing => (
                    StatusCode::BAD_REQUEST,
                    error_code::WORKSPACE_ROOT_MISSING,
                    "Invalid workspace root",
                ),
                WorkspaceError::WorkspaceRootNotDirectory => (
                    StatusCode::BAD_REQUEST,
                    error_code::WORKSPACE_ROOT_NOT_DIRECTORY,
                    "Invalid workspace root",
                ),
                WorkspaceError::WorkspaceRootNotGitRepository => (
                    StatusCode::BAD_REQUEST,
                    error_code::WORKSPACE_ROOT_NOT_GIT_REPOSITORY,
                    "Invalid workspace root",
                ),
                WorkspaceError::WorkspaceRootDuplicate => (
                    StatusCode::CONFLICT,
                    error_code::WORKSPACE_ROOT_DUPLICATE,
                    "Workspace root conflict",
                ),
                WorkspaceError::WorkspaceRootNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::WORKSPACE_ROOT_NOT_FOUND,
                    "Workspace root not found",
                ),
                WorkspaceError::PathOutsideWorkspaceRoot => (
                    StatusCode::BAD_REQUEST,
                    error_code::PATH_OUTSIDE_WORKSPACE_ROOT,
                    "Path is outside the workspace root",
                ),
                WorkspaceError::WorkspaceNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::WORKSPACE_NOT_FOUND,
                    "Workspace not found",
                ),
                WorkspaceError::GitUnavailable => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    error_code::GIT_UNAVAILABLE,
                    "Git unavailable",
                ),
                WorkspaceError::WorkspaceStoreUnavailable => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_code::WORKSPACE_STORE_UNAVAILABLE,
                    "Workspace store unavailable",
                ),
                WorkspaceError::ChangeNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::CHANGE_NOT_FOUND,
                    "Workspace change not found",
                ),
            },
            Self::Session(error) => match error {
                SessionError::WorkspaceNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::WORKSPACE_NOT_FOUND,
                    "Workspace not found",
                ),
                SessionError::WorkspaceRootNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::WORKSPACE_ROOT_NOT_FOUND,
                    "Workspace checkout is unavailable",
                ),
                SessionError::SessionNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::SESSION_NOT_FOUND,
                    "Session not found",
                ),
                SessionError::MessageContentRequired => (
                    StatusCode::BAD_REQUEST,
                    error_code::MESSAGE_CONTENT_REQUIRED,
                    "Invalid Message",
                ),
                SessionError::IdempotencyKeyRequired => (
                    StatusCode::BAD_REQUEST,
                    error_code::IDEMPOTENCY_KEY_REQUIRED,
                    "Missing Idempotency-Key",
                ),
                SessionError::IdempotencyConflict => (
                    StatusCode::CONFLICT,
                    error_code::IDEMPOTENCY_CONFLICT,
                    "Idempotency conflict",
                ),
                SessionError::InvalidMessageOrigin => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_code::SESSION_STORE_UNAVAILABLE,
                    "Invalid Message origin",
                ),
                SessionError::WorkspaceStoreUnavailable => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_code::WORKSPACE_STORE_UNAVAILABLE,
                    "Workspace store unavailable",
                ),
                SessionError::SessionStoreUnavailable => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_code::SESSION_STORE_UNAVAILABLE,
                    "Session store unavailable",
                ),
            },
            Self::Task(error) => match error {
                TaskError::SessionNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::SESSION_NOT_FOUND,
                    "Session not found",
                ),
                TaskError::TaskNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::TASK_NOT_FOUND,
                    "Task not found",
                ),
                TaskError::ObjectiveRequired => (
                    StatusCode::BAD_REQUEST,
                    error_code::TASK_OBJECTIVE_REQUIRED,
                    "Invalid Task",
                ),
                TaskError::ParentTaskNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::PARENT_TASK_NOT_FOUND,
                    "Parent Task not found",
                ),
                TaskError::DependencyTaskNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::DEPENDENCY_TASK_NOT_FOUND,
                    "Dependency Task not found",
                ),
                TaskError::RunNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::RUN_NOT_FOUND,
                    "Run not found",
                ),
                TaskError::TaskLinkOutsideSession => (
                    StatusCode::BAD_REQUEST,
                    error_code::TASK_LINK_OUTSIDE_SESSION,
                    "Task link outside Session",
                ),
                TaskError::DuplicateDependency => (
                    StatusCode::BAD_REQUEST,
                    error_code::DUPLICATE_TASK_DEPENDENCY,
                    "Duplicate Task dependency",
                ),
                TaskError::Cycle => (
                    StatusCode::BAD_REQUEST,
                    error_code::TASK_CYCLE,
                    "Task cycle",
                ),
                TaskError::InvalidTransition => (
                    StatusCode::CONFLICT,
                    error_code::INVALID_TASK_TRANSITION,
                    "Invalid Task transition",
                ),
                TaskError::InvalidAssignment => (
                    StatusCode::CONFLICT,
                    error_code::INVALID_TASK_ASSIGNMENT,
                    "Invalid Task assignment",
                ),
                TaskError::IdempotencyKeyRequired => (
                    StatusCode::BAD_REQUEST,
                    error_code::IDEMPOTENCY_KEY_REQUIRED,
                    "Missing Idempotency-Key",
                ),
                TaskError::IdempotencyConflict => (
                    StatusCode::CONFLICT,
                    error_code::IDEMPOTENCY_CONFLICT,
                    "Idempotency conflict",
                ),
                TaskError::TaskStoreUnavailable => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_code::TASK_STORE_UNAVAILABLE,
                    "Task store unavailable",
                ),
            },
            Self::Run(error) => match error {
                RunError::SessionNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::SESSION_NOT_FOUND,
                    "Session not found",
                ),
                RunError::WorkspaceRootNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::WORKSPACE_ROOT_NOT_FOUND,
                    "Workspace root not found",
                ),
                RunError::PathOutsideWorkspaceRoot => (
                    StatusCode::BAD_REQUEST,
                    error_code::PATH_OUTSIDE_WORKSPACE_ROOT,
                    "Path outside workspace root",
                ),
                RunError::RunNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::RUN_NOT_FOUND,
                    "Run not found",
                ),
                RunError::ParentRunNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::PARENT_RUN_NOT_FOUND,
                    "Parent run not found",
                ),
                RunError::ParentRunTerminal => (
                    StatusCode::CONFLICT,
                    error_code::PARENT_RUN_TERMINAL,
                    "Parent run is terminal",
                ),
                RunError::TaskNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::TASK_NOT_FOUND,
                    "Task not found",
                ),
                RunError::TaskLinkOutsideSession => (
                    StatusCode::CONFLICT,
                    error_code::TASK_LINK_OUTSIDE_SESSION,
                    "Task link outside Session",
                ),
                RunError::InvalidTaskAssignment => (
                    StatusCode::CONFLICT,
                    error_code::INVALID_TASK_ASSIGNMENT,
                    "Invalid Task assignment",
                ),
                RunError::ActiveRootRunExists => (
                    StatusCode::CONFLICT,
                    error_code::ACTIVE_ROOT_RUN_EXISTS,
                    "Active root run exists",
                ),
                RunError::IdempotencyKeyRequired => (
                    StatusCode::BAD_REQUEST,
                    error_code::IDEMPOTENCY_KEY_REQUIRED,
                    "Missing Idempotency-Key",
                ),
                RunError::InvalidTransition => (
                    StatusCode::CONFLICT,
                    error_code::INVALID_RUN_STATE,
                    "Invalid run state",
                ),
                RunError::InputContentRequired => (
                    StatusCode::BAD_REQUEST,
                    error_code::MESSAGE_CONTENT_REQUIRED,
                    "Message content required",
                ),
                RunError::RunInputReadOnly => (
                    StatusCode::CONFLICT,
                    error_code::RUN_INPUT_READ_ONLY,
                    "Run input is read-only",
                ),
                RunError::RunNotAcceptingInput => (
                    StatusCode::CONFLICT,
                    error_code::RUN_NOT_ACCEPTING_INPUT,
                    "Run is not accepting input",
                ),
                RunError::InvalidChildActivity => (
                    StatusCode::BAD_REQUEST,
                    error_code::INVALID_CHILD_ACTIVITY,
                    "Invalid child activity",
                ),
                RunError::MessageDeliveryNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::MESSAGE_DELIVERY_NOT_FOUND,
                    "Message delivery not found",
                ),
                RunError::InvalidMessageDelivery => (
                    StatusCode::CONFLICT,
                    error_code::INVALID_MESSAGE_DELIVERY,
                    "Invalid Message delivery",
                ),
                RunError::MessageDeliveryOutOfOrder => (
                    StatusCode::CONFLICT,
                    error_code::MESSAGE_DELIVERY_OUT_OF_ORDER,
                    "Message delivery out of order",
                ),
                RunError::CancellationFailed => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_code::RUN_CANCELLATION_FAILED,
                    "Run cancellation failed",
                ),
                RunError::ApprovalNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::APPROVAL_NOT_FOUND,
                    "Approval not found",
                ),
                RunError::ApprovalAlreadyDecided => (
                    StatusCode::CONFLICT,
                    error_code::APPROVAL_ALREADY_DECIDED,
                    "Approval already decided",
                ),
                RunError::IdempotencyConflict => (
                    StatusCode::CONFLICT,
                    error_code::IDEMPOTENCY_CONFLICT,
                    "Idempotency conflict",
                ),
                RunError::RunStoreUnavailable => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_code::RUN_STORE_UNAVAILABLE,
                    "Run store unavailable",
                ),
            },
            Self::Artifact(error) => match error {
                ArtifactFetchError::NotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::ARTIFACT_NOT_FOUND,
                    "Artifact not found",
                ),
                ArtifactFetchError::Unavailable => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_code::ARTIFACT_STORE_UNAVAILABLE,
                    "Artifact store unavailable",
                ),
            },
            Self::ArtifactUpload(error) => match error {
                ArtifactUploadError::TooLarge => (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    error_code::INVALID_REQUEST,
                    "Artifact upload is too large",
                ),
                ArtifactUploadError::InvalidMediaType => (
                    StatusCode::BAD_REQUEST,
                    error_code::INVALID_REQUEST,
                    "Artifact media type is invalid",
                ),
                ArtifactUploadError::Unavailable => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_code::ARTIFACT_STORE_UNAVAILABLE,
                    "Artifact store unavailable",
                ),
            },
            Self::Usage(error) => match error {
                UsageQueryError::InvalidLimit => (
                    StatusCode::BAD_REQUEST,
                    error_code::INVALID_USAGE_LIMIT,
                    "Invalid Usage page limit",
                ),
                UsageQueryError::IntegrityViolation => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_code::USAGE_INTEGRITY_VIOLATION,
                    "Usage store integrity violation",
                ),
                UsageQueryError::Unavailable => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_code::USAGE_STORE_UNAVAILABLE,
                    "Usage store unavailable",
                ),
            },
            Self::ProviderAccount(error) => match error {
                ProviderAccountOperationError::InvalidRequest => (
                    StatusCode::BAD_REQUEST,
                    error_code::PROVIDER_ACCOUNT_INVALID,
                    "Invalid provider account",
                ),
                ProviderAccountOperationError::AccountNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::PROVIDER_ACCOUNT_NOT_FOUND,
                    "Provider account not found",
                ),
                ProviderAccountOperationError::WorkspaceAssociationMismatch => (
                    StatusCode::BAD_REQUEST,
                    error_code::PROVIDER_ACCOUNT_WORKSPACE_ASSOCIATION_INVALID,
                    "Invalid provider account workspace association",
                ),
                ProviderAccountOperationError::ProviderAccountLimitReached => (
                    StatusCode::CONFLICT,
                    error_code::PROVIDER_ACCOUNT_LIMIT_REACHED,
                    "Provider account limit reached",
                ),
                ProviderAccountOperationError::IdempotencyConflict => (
                    StatusCode::CONFLICT,
                    error_code::IDEMPOTENCY_CONFLICT,
                    "Idempotency key was reused with a different provider account request",
                ),
                ProviderAccountOperationError::StoreUnavailable => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_code::PROVIDER_ACCOUNT_STORE_UNAVAILABLE,
                    "Provider account store unavailable",
                ),
                ProviderAccountOperationError::CredentialStoreUnavailable => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_code::PROVIDER_ACCOUNT_STORE_UNAVAILABLE,
                    "Provider account credential store unavailable",
                ),
                ProviderAccountOperationError::CleanupRequired => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    error_code::PROVIDER_ACCOUNT_CLEANUP_REQUIRED,
                    "Credential cleanup failed; retry disconnect",
                ),
                ProviderAccountOperationError::InvalidState => (
                    StatusCode::CONFLICT,
                    error_code::PROVIDER_ACCOUNT_INVALID_STATE,
                    "Provider account is in an invalid state",
                ),
                ProviderAccountOperationError::AttemptNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::PROVIDER_ACCOUNT_LOGIN_NOT_FOUND,
                    "Provider account login attempt not found",
                ),
                ProviderAccountOperationError::LoginUnavailable => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    error_code::PROVIDER_ACCOUNT_LOGIN_UNAVAILABLE,
                    "Provider account login unavailable",
                ),
                ProviderAccountOperationError::LoginFailed => (
                    StatusCode::BAD_GATEWAY,
                    error_code::PROVIDER_ACCOUNT_LOGIN_FAILED,
                    "Provider account login failed",
                ),
                ProviderAccountOperationError::Cancelled => (
                    StatusCode::CONFLICT,
                    error_code::PROVIDER_ACCOUNT_INVALID_STATE,
                    "Provider account login cancelled",
                ),
            },
        };
        ProblemDetails {
            type_uri: "about:blank".to_owned(),
            title: title.to_owned(),
            status: status.as_u16(),
            code: code.to_owned(),
            detail: self.to_string(),
        }
    }
}

impl IntoResponse for PublicError {
    fn into_response(self) -> Response {
        let problem = self.problem();
        let status = StatusCode::from_u16(problem.status).expect("valid problem status");
        let mut response = (status, Json(problem)).into_response();
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );
        if matches!(status, StatusCode::UNAUTHORIZED) {
            response.headers_mut().insert(
                axum::http::header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer"),
            );
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kiln_core::{
        ApprovalPolicy, EventId, MessageRole as CoreMessageRole, PersistedToolCall, Run,
        SessionEventPayload, ToolCallId, WorkspaceId, WorkspacePathScope, WorkspaceRootId,
    };

    const SESSION_ID: &str = "ses_01ARZ3NDEKTSV4RRFFQ69G5FAY";
    const RUN_ID: &str = "run_01ARZ3NDEKTSV4RRFFQ69G5FAY";
    const TOOL_CALL_ID: &str = "tcl_01ARZ3NDEKTSV4RRFFQ69G5FAY";
    const EVENT_ID: &str = "evt_01ARZ3NDEKTSV4RRFFQ69G5FAY";
    const WORKSPACE_ID: &str = "wsp_01ARZ3NDEKTSV4RRFFQ69G5FAY";
    const MESSAGE_ID: &str = "msg_01ARZ3NDEKTSV4RRFFQ69G5FAY";

    fn session_id() -> SessionId {
        SessionId::parse(SESSION_ID).unwrap()
    }

    fn run_id() -> RunId {
        RunId::parse(RUN_ID).unwrap()
    }

    fn tool_call_id() -> ToolCallId {
        ToolCallId::parse(TOOL_CALL_ID).unwrap()
    }

    fn path_scope() -> WorkspacePathScope {
        WorkspacePathScope::new(
            WorkspaceRootId::parse("wrt_01ARZ3NDEKTSV4RRFFQ69G5FAY").unwrap(),
            ".",
        )
        .unwrap()
    }

    fn stored_event(cursor: u64, payload: SessionEventPayload) -> StoredSessionEvent {
        StoredSessionEvent::from_parts(
            EventId::parse(EVENT_ID).unwrap(),
            session_id(),
            EventCursor::from_value(cursor),
            payload,
        )
        .unwrap()
    }

    #[test]
    fn run_snapshot_maps_to_protocol_response() {
        let run = Run::from_persisted(
            run_id(),
            session_id(),
            CoreRunState::Running,
            Some(ApprovalPolicy::FullAccess),
            Some(path_scope()),
        )
        .unwrap();
        let tool_call = ToolCall::from_persisted(PersistedToolCall {
            tool_call_id: tool_call_id(),
            run_id: run_id(),
            capability: "kiln.deterministic.subprocess".to_owned(),
            requested_scope: Some(path_scope()),
            effective_scope: Some(path_scope()),
            state: CoreToolCallState::Completed,
            stdout: Some("stdout".to_owned()),
            stderr: Some("stderr".to_owned()),
            stdout_artifact: None,
            stderr_artifact: None,
            exit_code: Some(0),
        })
        .unwrap();
        let response = run_response(&RunSnapshot::new(run, vec![tool_call]));

        assert_eq!(response.run_id, RUN_ID);
        assert_eq!(response.session_id, SESSION_ID);
        assert_eq!(response.state, RunState::Running);
        assert_eq!(response.tool_calls.len(), 1);
        assert_eq!(response.tool_calls[0].tool_call_id, TOOL_CALL_ID);
        assert_eq!(response.tool_calls[0].state, ToolCallState::Completed);
        assert_eq!(response.tool_calls[0].stdout.as_deref(), Some("stdout"));
        assert_eq!(response.tool_calls[0].stderr.as_deref(), Some("stderr"));
        assert_eq!(response.tool_calls[0].exit_code, Some(0));
    }

    #[test]
    fn every_core_event_payload_maps_to_protocol_event() {
        let tool_call = ToolCall::new(
            tool_call_id(),
            run_id(),
            "kiln.deterministic.subprocess".to_owned(),
            path_scope(),
        );
        let message = Message::new(
            kiln_core::MessageId::parse(MESSAGE_ID).unwrap(),
            session_id(),
            CoreMessageRole::User,
            "hello".to_owned(),
        )
        .unwrap();
        let payloads = vec![
            SessionEventPayload::SessionCreated {
                workspace_id: WorkspaceId::parse(WORKSPACE_ID).unwrap(),
            },
            SessionEventPayload::MessageAppended { message },
            SessionEventPayload::RunCreated {
                run_id: run_id(),
                state: CoreRunState::Queued,
                parent_run_id: None,
                task_id: None,
                user_input_mode: CoreRunInputMode::Interactive,
                approval_policy: Some(ApprovalPolicy::FullAccess),
                requested_scope: Some(path_scope()),
            },
            SessionEventPayload::RunInputQueued {
                run_id: run_id(),
                message_id: kiln_core::MessageId::parse(MESSAGE_ID).unwrap(),
            },
            SessionEventPayload::RunInterruptRequested {
                run_id: run_id(),
                message_id: kiln_core::MessageId::parse(MESSAGE_ID).unwrap(),
            },
            SessionEventPayload::RunInputDelivered {
                run_id: run_id(),
                message_id: kiln_core::MessageId::parse(MESSAGE_ID).unwrap(),
            },
            SessionEventPayload::RunInputFailed {
                run_id: run_id(),
                message_id: kiln_core::MessageId::parse(MESSAGE_ID).unwrap(),
            },
            SessionEventPayload::RunInputCancelled {
                run_id: run_id(),
                message_id: kiln_core::MessageId::parse(MESSAGE_ID).unwrap(),
            },
            SessionEventPayload::RunStateChanged {
                run_id: run_id(),
                state: CoreRunState::Running,
            },
            SessionEventPayload::RunCancellationRequested { run_id: run_id() },
            SessionEventPayload::ToolCallRequested {
                tool_call: tool_call.clone(),
            },
            SessionEventPayload::ToolCallStateChanged { tool_call },
            SessionEventPayload::ToolCallOutput {
                run_id: run_id(),
                tool_call_id: tool_call_id(),
                stream: CoreToolOutputStream::Stdout,
                content: "output".to_owned(),
            },
            SessionEventPayload::ArtifactRegistered {
                run_id: run_id(),
                tool_call_id: tool_call_id(),
                stream: CoreToolOutputStream::Stdout,
                artifact: Artifact::new(
                    ContentHash::parse("a".repeat(64)).unwrap(),
                    "text/plain; charset=utf-8",
                    7,
                )
                .unwrap(),
            },
            SessionEventPayload::ContextManifestCreated {
                context_manifest_id: kiln_core::ContextManifestId::parse(
                    "cmf_01ARZ3NDEKTSV4RRFFQ69G5FAV",
                )
                .unwrap(),
                run_id: run_id(),
                content_hash: ContentHash::parse("a".repeat(64)).unwrap(),
                entry_count: 3,
            },
        ];

        let events: Vec<_> = payloads
            .into_iter()
            .enumerate()
            .map(|(index, payload)| {
                session_event_response(&stored_event(index as u64 + 1, payload))
            })
            .collect();

        assert!(matches!(
            events[0].event,
            SessionEventDataResponse::SessionCreated { .. }
        ));
        assert!(matches!(
            events[1].event,
            SessionEventDataResponse::MessageAppended { .. }
        ));
        assert!(matches!(
            events[2].event,
            SessionEventDataResponse::RunCreated { .. }
        ));
        assert!(matches!(
            events[3].event,
            SessionEventDataResponse::RunInputQueued { .. }
        ));
        assert!(matches!(
            events[4].event,
            SessionEventDataResponse::RunInterruptRequested { .. }
        ));
        assert!(matches!(
            events[5].event,
            SessionEventDataResponse::RunInputDelivered { .. }
        ));
        assert!(matches!(
            events[6].event,
            SessionEventDataResponse::RunInputFailed { .. }
        ));
        assert!(matches!(
            events[7].event,
            SessionEventDataResponse::RunInputCancelled { .. }
        ));
        assert!(matches!(
            events[8].event,
            SessionEventDataResponse::RunStateChanged { .. }
        ));
        assert!(matches!(
            events[9].event,
            SessionEventDataResponse::RunCancellationRequested { .. }
        ));
        assert!(matches!(
            events[10].event,
            SessionEventDataResponse::ToolCallRequested { .. }
        ));
        assert!(matches!(
            events[11].event,
            SessionEventDataResponse::ToolCallStateChanged { .. }
        ));
        assert!(matches!(
            events[12].event,
            SessionEventDataResponse::ToolCallOutput { .. }
        ));
        assert!(matches!(
            events[13].event,
            SessionEventDataResponse::ArtifactRegistered { .. }
        ));
        assert_eq!(
            serde_json::to_value(&events[14].event).unwrap(),
            serde_json::json!({
                "type": "context.manifest_created",
                "context_manifest_id": "cmf_01ARZ3NDEKTSV4RRFFQ69G5FAV",
                "run_id": RUN_ID,
                "content_hash": "a".repeat(64),
                "entry_count": 3,
            })
        );
    }

    #[test]
    fn run_errors_map_to_stable_problem_details() {
        let cases = [
            (
                RunError::SessionNotFound,
                StatusCode::NOT_FOUND,
                error_code::SESSION_NOT_FOUND,
            ),
            (
                RunError::RunNotFound,
                StatusCode::NOT_FOUND,
                error_code::RUN_NOT_FOUND,
            ),
            (
                RunError::ActiveRootRunExists,
                StatusCode::CONFLICT,
                error_code::ACTIVE_ROOT_RUN_EXISTS,
            ),
            (
                RunError::InvalidTransition,
                StatusCode::CONFLICT,
                error_code::INVALID_RUN_STATE,
            ),
            (
                RunError::InputContentRequired,
                StatusCode::BAD_REQUEST,
                error_code::MESSAGE_CONTENT_REQUIRED,
            ),
            (
                RunError::RunInputReadOnly,
                StatusCode::CONFLICT,
                error_code::RUN_INPUT_READ_ONLY,
            ),
            (
                RunError::RunNotAcceptingInput,
                StatusCode::CONFLICT,
                error_code::RUN_NOT_ACCEPTING_INPUT,
            ),
            (
                RunError::MessageDeliveryNotFound,
                StatusCode::NOT_FOUND,
                error_code::MESSAGE_DELIVERY_NOT_FOUND,
            ),
            (
                RunError::InvalidMessageDelivery,
                StatusCode::CONFLICT,
                error_code::INVALID_MESSAGE_DELIVERY,
            ),
            (
                RunError::MessageDeliveryOutOfOrder,
                StatusCode::CONFLICT,
                error_code::MESSAGE_DELIVERY_OUT_OF_ORDER,
            ),
            (
                RunError::CancellationFailed,
                StatusCode::INTERNAL_SERVER_ERROR,
                error_code::RUN_CANCELLATION_FAILED,
            ),
            (
                RunError::RunStoreUnavailable,
                StatusCode::INTERNAL_SERVER_ERROR,
                error_code::RUN_STORE_UNAVAILABLE,
            ),
        ];

        for (error, status, code) in cases {
            let problem = PublicError::from(error).problem();
            assert_eq!(problem.status, status.as_u16());
            assert_eq!(problem.code, code);
        }
    }

    #[test]
    fn broadcaster_wakes_only_when_events_were_committed() {
        let broadcaster = EventBroadcaster::new();
        let mut receiver = broadcaster.subscribe_wake();

        broadcaster.publish(Vec::new());
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));

        broadcaster.publish(vec![stored_event(
            1,
            SessionEventPayload::RunCreated {
                run_id: run_id(),
                state: CoreRunState::Queued,
                parent_run_id: None,
                task_id: None,
                user_input_mode: CoreRunInputMode::Interactive,
                approval_policy: Some(ApprovalPolicy::FullAccess),
                requested_scope: Some(path_scope()),
            },
        )]);
        assert_eq!(receiver.try_recv(), Ok(()));
    }

    #[tokio::test]
    async fn lifecycle_rejects_new_commands_and_drains_accepted_commands() {
        let lifecycle = LifecycleCoordinator::new();
        let accepted = lifecycle.begin_command().expect("command is accepted");

        assert!(lifecycle.request_shutdown());
        assert!(!lifecycle.request_shutdown());
        assert!(matches!(
            lifecycle.begin_command(),
            Err(PublicError::DaemonShuttingDown)
        ));
        let problem = PublicError::DaemonShuttingDown.problem();
        assert_eq!(problem.status, StatusCode::SERVICE_UNAVAILABLE.as_u16());
        assert_eq!(problem.code, error_code::DAEMON_SHUTTING_DOWN);
        lifecycle.wait_for_shutdown_request().await;

        let mut draining = Box::pin(lifecycle.wait_for_commands());
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(matches!(
            draining.as_mut().poll(&mut context),
            std::task::Poll::Pending
        ));
        drop(accepted);
        draining.await;
    }
}
