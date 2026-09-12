//! Native desktop connection orchestration for Kiln's public protocol.
//!
//! Runs currently use the deterministic executor fixture. The desktop must label
//! that executor explicitly; this connection is not a production model account.

use std::{net::SocketAddr, path::Path};

use kiln_client::Client;
use kiln_protocol::{
    AppendMessageRequest, ApprovalPolicy, ClientIdentity, CreateWorkspaceRequest,
    MessageDeliveryMode, NegotiateResponse, SendRunInputRequest, SessionEventsResponse,
    SessionResponse, SessionRunsResponse, StartRunRequest, WorkspaceResponse, WorkspaceRootRequest,
};

#[derive(Clone)]
pub struct ConnectionConfig {
    pub address: String,
    pub token_file: String,
    pub repository_path: String,
    /// An empty value creates a workspace and session for `repository_path`.
    pub session_id: String,
}

pub struct Connected {
    pub client: Client,
    pub negotiated: NegotiateResponse,
    pub session: SessionResponse,
    pub workspace: WorkspaceResponse,
    pub initial_runs: SessionRunsResponse,
    pub initial_events: SessionEventsResponse,
}

#[derive(Clone)]
pub struct Submission {
    pub content: String,
    pub idempotency_key: String,
    pub active_run_id: Option<String>,
    pub child_activity: Option<kiln_protocol::ChildActivityReference>,
    pub message_appended: bool,
    pub append_uncertain: bool,
}

pub async fn connect(config: ConnectionConfig) -> Result<Connected, String> {
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

    let (workspace, session) = if config.session_id.is_empty() {
        create_workspace_session(&client, &config.repository_path).await?
    } else {
        let session = client
            .get_session(&config.session_id)
            .await
            .map_err(|error| error_message("session lookup", &error))?;
        let workspace = client
            .get_workspace(&session.workspace_id)
            .await
            .map_err(|error| error_message("workspace lookup", &error))?;
        (workspace, session)
    };

    if workspace.roots.first().is_none() {
        return Err("workspace has no repository root".to_owned());
    }

    let initial_runs = client
        .list_session_runs(&session.session_id)
        .await
        .map_err(|error| error_message("initial Run load", &error))?;
    let initial_events = client
        .list_session_events(&session.session_id, Some("0"))
        .await
        .map_err(|error| error_message("initial event load", &error))?;

    Ok(Connected {
        client,
        negotiated,
        session,
        workspace,
        initial_runs,
        initial_events,
    })
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
    if let Some(run_id) = submission.active_run_id.as_deref() {
        if let Some(reference) = &submission.child_activity {
            client
                .react_to_run_activity(
                    run_id,
                    &submission.idempotency_key,
                    &kiln_protocol::ReactToRunActivityRequest {
                        content: submission.content.clone(),
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
                &AppendMessageRequest {
                    content: submission.content.clone(),
                },
            )
            .await;
        match append_result {
            Ok(_) => submission.message_appended = true,
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
