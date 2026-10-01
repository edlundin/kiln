//! Native desktop connection orchestration for Kiln's public protocol.
//!
//! Runs currently use the deterministic executor fixture. The desktop must label
//! that executor explicitly; this connection is not a production model account.

use std::{
    fs::File,
    io::{self, Read},
    net::SocketAddr,
    path::Path,
};

use kiln_client::Client;
use kiln_protocol::{
    AppendMessageRequest, ApprovalPolicy, ClientIdentity, CreateWorkspaceRequest,
    MAX_ARTIFACT_UPLOAD_BYTES, MessageDeliveryMode, NegotiateResponse, SendRunInputRequest,
    SessionEventsResponse, SessionResponse, SessionRunsResponse, StartRunRequest,
    WorkspaceResponse, WorkspaceRootRequest,
};

#[derive(Clone)]
pub struct ConnectionConfig {
    pub address: String,
    pub token_file: String,
    pub repository_path: String,
    /// An empty value creates a workspace and session for `repository_path`.
    pub session_id: String,
}

#[derive(Clone)]
pub struct DaemonConnection {
    pub client: Client,
    pub negotiated: NegotiateResponse,
    pub workspaces: Vec<WorkspaceResponse>,
}

pub struct Connected {
    pub daemon: DaemonConnection,
    pub client: Client,
    pub negotiated: NegotiateResponse,
    pub session: SessionResponse,
    pub workspace: WorkspaceResponse,
    pub initial_runs: SessionRunsResponse,
    pub initial_events: SessionEventsResponse,
}

#[expect(
    clippy::large_enum_variant,
    reason = "The connection transition owns its negotiated session state until the UI installs it."
)]
pub enum ConnectionResult {
    Browse(DaemonConnection),
    Session(Connected),
}

#[derive(Clone)]
pub struct Submission {
    pub session_id: String,
    pub content: String,
    pub attachments: Vec<kiln_protocol::ArtifactResponse>,
    pub uploads: Vec<AttachmentUpload>,
    pub idempotency_key: String,
    pub active_run_id: Option<String>,
    pub child_activity: Option<kiln_protocol::ChildActivityReference>,
    pub message_appended: bool,
    pub append_uncertain: bool,
    pub append_retry_ready: bool,
}

#[derive(Clone)]
pub struct AttachmentUpload {
    pub path: Option<String>,
    pub bytes: Option<Vec<u8>>,
    pub media_type: String,
}

const ATTACHMENT_READ_CHUNK_SIZE: usize = 64 * 1024;

fn attachment_too_large() -> io::Error {
    io::Error::new(
        io::ErrorKind::FileTooLarge,
        format!(
            "attachment exceeds the {} byte upload limit",
            MAX_ARTIFACT_UPLOAD_BYTES
        ),
    )
}

fn invalid_attachment_source() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "attachment source is not a regular file",
    )
}

fn read_attachment_file(path: &Path) -> io::Result<Vec<u8>> {
    let link_metadata = std::fs::symlink_metadata(path)?;
    if !link_metadata.file_type().is_file() {
        return Err(invalid_attachment_source());
    }

    // Do not follow a symlink substituted after the picker or metadata check.
    // NONBLOCK also lets the descriptor check reject a substituted FIFO.
    let descriptor = rustix::fs::open(
        path,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?;
    let mut file = File::from(descriptor);
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(invalid_attachment_source());
    }
    if metadata.len() > MAX_ARTIFACT_UPLOAD_BYTES as u64 {
        return Err(attachment_too_large());
    }

    let capacity = usize::try_from(metadata.len())
        .unwrap_or(MAX_ARTIFACT_UPLOAD_BYTES)
        .min(MAX_ARTIFACT_UPLOAD_BYTES);
    let mut bytes = Vec::with_capacity(capacity);
    let mut chunk = [0_u8; ATTACHMENT_READ_CHUNK_SIZE];
    loop {
        let read = file.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        if bytes.len().saturating_add(read) > MAX_ARTIFACT_UPLOAD_BYTES {
            return Err(attachment_too_large());
        }
        bytes.extend_from_slice(&chunk[..read]);
    }

    // The size can change after the initial metadata check. Keep the bound in
    // force even when a file is replaced or grows while it is being read.
    if bytes.len() > MAX_ARTIFACT_UPLOAD_BYTES
        || file.metadata()?.len() > MAX_ARTIFACT_UPLOAD_BYTES as u64
    {
        return Err(attachment_too_large());
    }
    Ok(bytes)
}

fn read_attachment_bytes(bytes: &[u8]) -> io::Result<Vec<u8>> {
    if bytes.len() > MAX_ARTIFACT_UPLOAD_BYTES {
        return Err(attachment_too_large());
    }
    Ok(bytes.to_vec())
}

pub async fn connect(config: ConnectionConfig) -> Result<ConnectionResult, String> {
    let mut daemon = connect_daemon(&config).await?;
    if config.session_id.is_empty() && config.repository_path.is_empty() {
        return Ok(ConnectionResult::Browse(daemon));
    }

    let session_id = if config.session_id.is_empty() {
        let (_, session) =
            create_workspace_session(&daemon.client, &config.repository_path).await?;
        daemon.workspaces = daemon
            .client
            .list_workspaces()
            .await
            .map_err(|error| error_message("workspace list", &error))?
            .workspaces;
        session.session_id
    } else {
        config.session_id
    };
    let connected = open_session(&daemon, &session_id).await?;
    Ok(ConnectionResult::Session(connected))
}

pub async fn connect_daemon(config: &ConnectionConfig) -> Result<DaemonConnection, String> {
    let address = config
        .address
        .parse::<SocketAddr>()
        .map_err(|_| "invalid daemon address".to_owned())?;
    let token = std::fs::read_to_string(&config.token_file)
        .map_err(|_| "could not read daemon credential".to_owned())?;
    let client =
        Client::new(address, token).map_err(|error| error_message("connection setup", &error))?;
    let negotiated = client
        .negotiate(ClientIdentity {
            name: "kiln-desktop".to_owned(),
            build: env!("CARGO_PKG_VERSION").to_owned(),
        })
        .await
        .map_err(|error| error_message("protocol negotiation", &error))?;

    let workspaces = client
        .list_workspaces()
        .await
        .map_err(|error| error_message("workspace list", &error))?
        .workspaces;
    Ok(DaemonConnection {
        client,
        negotiated,
        workspaces,
    })
}

pub async fn open_session(
    daemon: &DaemonConnection,
    session_id: &str,
) -> Result<Connected, String> {
    let session = daemon
        .client
        .get_session(session_id)
        .await
        .map_err(|error| error_message("session lookup", &error))?;
    let workspace = daemon
        .client
        .get_workspace(&session.workspace_id)
        .await
        .map_err(|error| error_message("workspace lookup", &error))?;

    if workspace.roots.is_empty() {
        return Err("workspace has no repository root".to_owned());
    }

    let initial_runs = daemon
        .client
        .list_session_runs(&session.session_id)
        .await
        .map_err(|error| error_message("initial Run load", &error))?;
    let initial_events = daemon
        .client
        .list_session_events(&session.session_id, Some("0"))
        .await
        .map_err(|error| error_message("initial event load", &error))?;

    Ok(Connected {
        daemon: daemon.clone(),
        client: daemon.client.clone(),
        negotiated: daemon.negotiated.clone(),
        session,
        workspace,
        initial_runs,
        initial_events,
    })
}

pub async fn list_sessions(
    daemon: &DaemonConnection,
    workspace_id: &str,
) -> Result<Vec<SessionResponse>, String> {
    daemon
        .client
        .list_sessions(workspace_id)
        .await
        .map(|response| response.sessions)
        .map_err(|error| error_message("session list", &error))
}

pub async fn create_session(
    daemon: &DaemonConnection,
    workspace_id: &str,
) -> Result<Connected, String> {
    let session = daemon
        .client
        .create_session(workspace_id)
        .await
        .map_err(|error| error_message("session creation", &error))?;
    open_session(daemon, &session.session_id).await
}

async fn create_workspace_session(
    client: &Client,
    repository_path: &str,
) -> Result<(WorkspaceResponse, SessionResponse), String> {
    let repository_name = Path::new(repository_path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| "repository path has no name".to_owned())?;
    let workspace = client
        .create_workspace(&CreateWorkspaceRequest {
            name: repository_name.to_owned(),
            roots: vec![WorkspaceRootRequest {
                name: repository_name.to_owned(),
                path: repository_path.to_owned(),
            }],
        })
        .await
        .map_err(|error| error_message("workspace creation", &error))?;
    let session = client
        .create_session(&workspace.workspace_id)
        .await
        .map_err(|error| error_message("session creation", &error))?;
    Ok((workspace, session))
}

pub async fn submit(
    client: &Client,
    session_id: &str,
    root_id: &str,
    submission: &mut Submission,
) -> Result<(), String> {
    if !submission.uploads.is_empty() {
        let pending_uploads = std::mem::take(&mut submission.uploads);
        for (index, upload) in pending_uploads.iter().cloned().enumerate() {
            let source = match (upload.path.as_deref(), upload.bytes.as_deref()) {
                (Some(path), None) => read_attachment_file(Path::new(path)),
                (None, Some(bytes)) => read_attachment_bytes(bytes),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "attachment upload has no unambiguous source",
                )),
            };
            let bytes = match source {
                Ok(bytes) => bytes,
                Err(error) => {
                    submission
                        .uploads
                        .extend(pending_uploads[index..].iter().cloned());
                    return Err(format!("Read attachment: {error}"));
                }
            };
            match client
                .upload_artifact(session_id, bytes, &upload.media_type)
                .await
            {
                Ok(artifact) => submission.attachments.push(artifact),
                Err(error) => {
                    submission
                        .uploads
                        .extend(pending_uploads[index..].iter().cloned());
                    return Err(error_message("Upload attachment", &error));
                }
            }
        }
    }
    if let Some(run_id) = submission.active_run_id.as_deref() {
        if let Some(reference) = &submission.child_activity {
            client
                .react_to_run_activity(
                    run_id,
                    &submission.idempotency_key,
                    &kiln_protocol::ReactToRunActivityRequest {
                        content: submission.content.clone(),
                        attachments: submission.attachments.clone(),
                        child_activity: reference.clone(),
                    },
                )
                .await
                .map_err(|error| error_message("child activity reaction", &error))?;
            return Ok(());
        }
        client
            .send_run_input(
                run_id,
                &submission.idempotency_key,
                &SendRunInputRequest {
                    content: submission.content.clone(),
                    attachments: submission.attachments.clone(),
                    delivery_mode: MessageDeliveryMode::Queued,
                },
            )
            .await
            .map_err(|error| error_message("run input", &error))?;
        return Ok(());
    }

    if submission.child_activity.is_some() {
        return Err("a child reaction requires its original root Run".to_owned());
    }
    if !submission.message_appended {
        let append_result = client
            .append_message(
                session_id,
                &submission.idempotency_key,
                &AppendMessageRequest {
                    content: submission.content.clone(),
                    attachments: submission.attachments.clone(),
                },
            )
            .await;
        match append_result {
            Ok(_) => {
                submission.message_appended = true;
                submission.append_uncertain = false;
            }
            Err(error) if append_result_is_uncertain(&error) => {
                submission.append_uncertain = true;
                return Err(
                    "message submission status is uncertain; reconnect and review history before retry"
                        .to_owned(),
                );
            }
            Err(error) => return Err(error_message("message submission", &error)),
        }
    }
    client
        .start_run(
            session_id,
            &submission.idempotency_key,
            &StartRunRequest {
                approval_policy: ApprovalPolicy::Ask,
                workspace_root_id: root_id.to_owned(),
                relative_directory: ".".to_owned(),
            },
        )
        .await
        .map_err(|error| error_message("run start", &error))?;
    Ok(())
}

fn append_result_is_uncertain(error: &kiln_client::Error) -> bool {
    matches!(
        error,
        kiln_client::Error::HttpTransport { .. } | kiln_client::Error::Decode { .. }
    )
}

pub fn error_message(operation: &str, error: &kiln_client::Error) -> String {
    match error {
        kiln_client::Error::Api { problem, .. } => {
            format!("{operation} failed ({})", problem.code)
        }
        _ => format!("{operation} failed"),
    }
}
