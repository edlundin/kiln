//! HTTP and WebSocket transport for the current Kiln protocol slice.

use std::{future::Future, net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{
        FromRequest, Path, Query, Request, State, WebSocketUpgrade,
        rejection::QueryRejection,
        ws::{self, rejection::WebSocketUpgradeRejection},
    },
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use kiln_core::{
    AppendMessage, CreateWorkspace, EventCursor, Message, MessageRole as CoreMessageRole, RunError,
    RunId, RunSnapshot, RunState as CoreRunState, Session, SessionError, SessionEventPage,
    SessionEventPayload, SessionId, SessionOperations, StoreMetadata, StoredSessionEvent, ToolCall,
    ToolCallState as CoreToolCallState, ToolOutputStream as CoreToolOutputStream, WorkspaceError,
    WorkspaceId, WorkspaceOperations,
};
use kiln_protocol::{
    AppendMessageRequest, CreateWorkspaceRequest, EVENTS_WEBSOCKET_PATH, IDEMPOTENCY_KEY_HEADER,
    MessageResponse, MessageRole, NEGOTIATE_PATH, NegotiateRequest, NegotiateResponse,
    PROTOCOL_VERSION, ProblemDetails, RUN_PATH, RunResponse, RunState, SESSION_EVENTS_PATH,
    SESSION_MESSAGES_PATH, SESSION_PATH, SESSION_RUNS_PATH, SessionEventDataResponse,
    SessionEventResponse, SessionEventsResponse, SessionResponse, StoreIdentity, ToolCallResponse,
    ToolCallState, ToolOutputStream, WEBSOCKET_CAPABILITY, WORKSPACE_PATH, WORKSPACE_SESSIONS_PATH,
    WORKSPACES_PATH, WebSocketFrame, WorkspaceResponse, WorkspaceRootResponse, error_code,
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
    ) -> impl Future<Output = Result<kiln_core::StartRunMutation, RunError>> + Send;

    fn get_run(&self, run_id: RunId) -> impl Future<Output = Result<RunSnapshot, RunError>> + Send;
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
    workspace_operations: Arc<W>,
    session_operations: Arc<S>,
    run_operations: Arc<R>,
    event_broadcaster: EventBroadcaster,
}

impl<W, S, R> Clone for AppState<W, S, R> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            event_websocket_endpoint: self.event_websocket_endpoint.clone(),
            workspace_operations: Arc::clone(&self.workspace_operations),
            session_operations: Arc::clone(&self.session_operations),
            run_operations: Arc::clone(&self.run_operations),
            event_broadcaster: self.event_broadcaster.clone(),
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
    ) -> Self {
        Self {
            store,
            event_websocket_endpoint: format!("ws://{bound_addr}{EVENTS_WEBSOCKET_PATH}"),
            workspace_operations: Arc::new(workspace_operations),
            session_operations: Arc::new(session_operations),
            run_operations: Arc::new(run_operations),
            event_broadcaster,
        }
    }
}

pub fn router<W, S, R>(state: AppState<W, S, R>) -> Router
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    Router::new()
        .route(NEGOTIATE_PATH, post(negotiate))
        .route(WORKSPACES_PATH, post(create_workspace))
        .route(WORKSPACE_PATH, get(get_workspace))
        .route(WORKSPACE_SESSIONS_PATH, post(create_session))
        .route(SESSION_PATH, get(get_session))
        .route(SESSION_MESSAGES_PATH, post(append_message))
        .route(SESSION_EVENTS_PATH, get(list_session_events))
        .route(SESSION_RUNS_PATH, post(start_run))
        .route(RUN_PATH, get(get_run))
        .route(EVENTS_WEBSOCKET_PATH, get(events))
        .method_not_allowed_fallback(method_not_allowed)
        .fallback(not_found)
        .with_state(state)
}

pub async fn serve<W, S, R>(
    listener: tokio::net::TcpListener,
    state: AppState<W, S, R>,
) -> Result<(), std::io::Error>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    axum::serve(listener, router(state)).await
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

async fn create_session<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(workspace_id): Path<String>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let workspace_id = WorkspaceId::parse(workspace_id).map_err(|_| PublicError::InvalidRequest)?;
    let session = state
        .session_operations
        .create_session(workspace_id)
        .await
        .map_err(PublicError::from)?;
    state.event_broadcaster.wake();
    Ok((StatusCode::CREATED, Json(session_response(&session))))
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
    StrictJson(request): StrictJson<AppendMessageRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let session_id = SessionId::parse(session_id).map_err(|_| PublicError::InvalidRequest)?;
    let message = state
        .session_operations
        .append_message(AppendMessage {
            session_id,
            content: request.content,
        })
        .await
        .map_err(PublicError::from)?;
    state.event_broadcaster.wake();
    Ok((StatusCode::CREATED, Json(message_response(&message))))
}

async fn start_run<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(session_id): Path<String>,
    headers: axum::http::HeaderMap,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let session_id = SessionId::parse(session_id).map_err(|_| PublicError::InvalidRequest)?;
    let idempotency_key = headers
        .get(IDEMPOTENCY_KEY_HEADER)
        .ok_or(PublicError::MissingIdempotencyKey)?
        .to_str()
        .map_err(|_| PublicError::InvalidIdempotencyKey)?;
    if idempotency_key.is_empty() {
        return Err(PublicError::InvalidIdempotencyKey);
    }
    let run = state
        .run_operations
        .start_run(session_id, idempotency_key.to_owned())
        .await
        .map_err(PublicError::from)?;
    Ok((StatusCode::ACCEPTED, Json(run_response(&run.value))))
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
        },
        content: message.content().to_owned(),
    }
}

fn run_response(snapshot: &RunSnapshot) -> RunResponse {
    RunResponse {
        run_id: snapshot.run().run_id().as_str().to_owned(),
        session_id: snapshot.run().session_id().as_str().to_owned(),
        state: run_state_response(snapshot.run().state()),
        tool_calls: snapshot
            .tool_calls()
            .iter()
            .map(tool_call_response)
            .collect(),
    }
}

fn tool_call_response(tool_call: &ToolCall) -> ToolCallResponse {
    ToolCallResponse {
        tool_call_id: tool_call.tool_call_id().as_str().to_owned(),
        run_id: tool_call.run_id().as_str().to_owned(),
        capability: tool_call.capability().to_owned(),
        state: tool_call_state_response(tool_call.state()),
        stdout: tool_call.stdout().map(str::to_owned),
        stderr: tool_call.stderr().map(str::to_owned),
        exit_code: tool_call.exit_code(),
    }
}

fn run_state_response(state: CoreRunState) -> RunState {
    match state {
        CoreRunState::Queued => RunState::Queued,
        CoreRunState::Running => RunState::Running,
        CoreRunState::Completed => RunState::Completed,
        CoreRunState::Failed => RunState::Failed,
    }
}

fn tool_call_state_response(state: CoreToolCallState) -> ToolCallState {
    match state {
        CoreToolCallState::Requested => ToolCallState::Requested,
        CoreToolCallState::Running => ToolCallState::Running,
        CoreToolCallState::Completed => ToolCallState::Completed,
        CoreToolCallState::Failed => ToolCallState::Failed,
    }
}

fn tool_output_stream_response(stream: CoreToolOutputStream) -> ToolOutputStream {
    match stream {
        CoreToolOutputStream::Stdout => ToolOutputStream::Stdout,
        CoreToolOutputStream::Stderr => ToolOutputStream::Stderr,
    }
}

fn session_event_response(event: &StoredSessionEvent) -> SessionEventResponse {
    let event_data = match event.payload() {
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
        SessionEventPayload::RunCreated { run_id, state } => SessionEventDataResponse::RunCreated {
            run_id: run_id.as_str().to_owned(),
            state: run_state_response(*state),
        },
        SessionEventPayload::RunStateChanged { run_id, state } => {
            SessionEventDataResponse::RunStateChanged {
                run_id: run_id.as_str().to_owned(),
                state: run_state_response(*state),
            }
        }
        SessionEventPayload::ToolCallRequested { tool_call } => {
            SessionEventDataResponse::ToolCallRequested {
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
    let websocket = websocket.map_err(|_| PublicError::WebSocketUpgradeRequired)?;
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
    #[error("request body is not valid JSON")]
    InvalidJson,
    #[error("request fields are invalid")]
    InvalidRequest,
    #[error("event cursor is invalid")]
    InvalidEventCursor,
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
    #[error("run operation failed")]
    Run(RunError),
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

impl From<RunError> for PublicError {
    fn from(error: RunError) -> Self {
        Self::Run(error)
    }
}

impl PublicError {
    fn problem(&self) -> ProblemDetails {
        let (status, code, title) = match self {
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
            },
            Self::Session(error) => match error {
                SessionError::WorkspaceNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::WORKSPACE_NOT_FOUND,
                    "Workspace not found",
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
            Self::Run(error) => match error {
                RunError::SessionNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::SESSION_NOT_FOUND,
                    "Session not found",
                ),
                RunError::RunNotFound => (
                    StatusCode::NOT_FOUND,
                    error_code::RUN_NOT_FOUND,
                    "Run not found",
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
                RunError::RunStoreUnavailable => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_code::RUN_STORE_UNAVAILABLE,
                    "Run store unavailable",
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
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kiln_core::{
        EventId, MessageRole as CoreMessageRole, Run, SessionEventPayload, ToolCallId, WorkspaceId,
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
        let run = Run::from_persisted(run_id(), session_id(), CoreRunState::Running);
        let tool_call = ToolCall::from_persisted(
            tool_call_id(),
            run_id(),
            "kiln.deterministic.subprocess".to_owned(),
            CoreToolCallState::Completed,
            Some("stdout".to_owned()),
            Some("stderr".to_owned()),
            Some(0),
        )
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
            },
            SessionEventPayload::RunStateChanged {
                run_id: run_id(),
                state: CoreRunState::Running,
            },
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
            SessionEventDataResponse::RunStateChanged { .. }
        ));
        assert!(matches!(
            events[4].event,
            SessionEventDataResponse::ToolCallRequested { .. }
        ));
        assert!(matches!(
            events[5].event,
            SessionEventDataResponse::ToolCallStateChanged { .. }
        ));
        assert!(matches!(
            events[6].event,
            SessionEventDataResponse::ToolCallOutput { .. }
        ));
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
            },
        )]);
        assert_eq!(receiver.try_recv(), Ok(()));
    }
}
