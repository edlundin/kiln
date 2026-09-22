use super::{AppState, PublicError, RunOperations, SessionOperations, WorkspaceOperations};
use axum::{Json, extract::State, http::header, response::IntoResponse};
use kiln_core::{
    ConfigurationIdentityStatusStore, ConfigurationMasterIdentityPhase,
    ConfigurationMasterIdentityStatus, ConfigurationRole, ConfigurationStateError,
};
use kiln_protocol::{
    ConfigurationIdentityPhase, ConfigurationIdentityStatusResponse,
    ConfigurationIdentitySummaryResponse, ConfigurationSyncRole,
};
use std::{future::Future, pin::Pin};

pub(super) trait ConfigurationIdentityCommandOperations: Send + Sync {
    fn configure(
        &self,
        request: kiln_core::ConfigureMasterIdentity,
        key: String,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        kiln_core::ConfigurationIdentitySetupReceipt,
                        kiln_core::ConfigurationIdentityCommandError,
                    >,
                > + Send
                + '_,
        >,
    >;
    fn retire(
        &self,
        instance: kiln_core::KilnInstanceId,
        setup_key: String,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<(), kiln_core::ConfigurationIdentityCommandError>>
                + Send
                + '_,
        >,
    >;
    fn retire_by_id(
        &self,
        instance: kiln_core::KilnInstanceId,
        identity_id: kiln_core::ConfigurationMasterIdentityId,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<(), kiln_core::ConfigurationIdentityCommandError>>
                + Send
                + '_,
        >,
    >;
}
pub(super) struct ConfigurationIdentityCommandAdapter<T>(pub T);
impl<T: kiln_core::ConfigurationIdentityAdministration> ConfigurationIdentityCommandOperations
    for ConfigurationIdentityCommandAdapter<T>
{
    fn configure(
        &self,
        request: kiln_core::ConfigureMasterIdentity,
        key: String,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        kiln_core::ConfigurationIdentitySetupReceipt,
                        kiln_core::ConfigurationIdentityCommandError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(self.0.configure_master_identity(request, key))
    }
    fn retire(
        &self,
        instance: kiln_core::KilnInstanceId,
        setup_key: String,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<(), kiln_core::ConfigurationIdentityCommandError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(self.0.retire_master_identity(instance, setup_key))
    }
    fn retire_by_id(
        &self,
        instance: kiln_core::KilnInstanceId,
        identity_id: kiln_core::ConfigurationMasterIdentityId,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<(), kiln_core::ConfigurationIdentityCommandError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(self.0.retire_master_identity_by_id(instance, identity_id))
    }
}

pub(super) async fn configure<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    headers: axum::http::HeaderMap,
    super::StrictJson(request): super::StrictJson<kiln_protocol::ConfigureMasterIdentityRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let permit = state.lifecycle.begin_command()?;
    let key = super::required_idempotency_key(&headers)?;
    let invalid = || {
        PublicError::ConfigurationIdentity(
            kiln_core::ConfigurationIdentityCommandError::InvalidRequest,
        )
    };
    let request = kiln_core::ConfigureMasterIdentity {
        expected_instance_id: kiln_core::KilnInstanceId::parse(request.expected_instance_id)
            .map_err(|_| invalid())?,
        expected_group_id: kiln_core::ConfigurationGroupId::parse(request.expected_group_id)
            .map_err(|_| invalid())?,
        expected_state_version: request.expected_state_version,
        server_name: request.server_name,
        validity: kiln_core::ConfigurationCertificateValidity {
            not_before: request.not_before_unix_seconds,
            leaf_not_after: request.leaf_not_after_unix_seconds,
            ca_not_after: request.ca_not_after_unix_seconds,
        },
    };
    let operations = state
        .configuration_identity_command_operations
        .clone()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    // Keep the daemon command permit until the owned vault operation finishes,
    // even if the client disconnects and drops this handler's JoinHandle.
    let receipt = tokio::spawn(async move {
        let _permit = permit;
        operations.configure(request, key).await
    })
    .await
    .map_err(|_| PublicError::ConfigurationSyncUnavailable)?
    .map_err(PublicError::ConfigurationIdentity)?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(kiln_protocol::ConfigurationIdentitySetupResponse {
            instance_id: receipt.instance_id.as_str().to_owned(),
            group_id: receipt.group_id.as_str().to_owned(),
            reserved_state_version: receipt.reserved_state_version,
            identity_id: receipt.identity_id.as_str().to_owned(),
        }),
    ))
}

pub(super) async fn retire<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    super::StrictJson(request): super::StrictJson<kiln_protocol::RetireMasterIdentityRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let permit = state.lifecycle.begin_command()?;
    let invalid = || {
        PublicError::ConfigurationIdentity(
            kiln_core::ConfigurationIdentityCommandError::InvalidRequest,
        )
    };
    let instance =
        kiln_core::KilnInstanceId::parse(request.expected_instance_id).map_err(|_| invalid())?;
    let setup_key = request.setup_idempotency_key;
    if setup_key.is_empty() {
        return Err(invalid());
    }
    let operations = state
        .configuration_identity_command_operations
        .clone()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    tokio::spawn(async move {
        let _permit = permit;
        operations.retire(instance, setup_key).await
    })
    .await
    .map_err(|_| PublicError::ConfigurationSyncUnavailable)?
    .map_err(PublicError::ConfigurationIdentity)?;
    Ok((
        axum::http::StatusCode::NO_CONTENT,
        [(header::CACHE_CONTROL, "no-store")],
    ))
}

pub(super) async fn retire_by_id<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    super::StrictJson(request): super::StrictJson<kiln_protocol::RetireMasterIdentityByIdRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let permit = state.lifecycle.begin_command()?;
    let invalid = || {
        PublicError::ConfigurationIdentity(
            kiln_core::ConfigurationIdentityCommandError::InvalidRequest,
        )
    };
    let instance =
        kiln_core::KilnInstanceId::parse(request.expected_instance_id).map_err(|_| invalid())?;
    let identity_id = kiln_core::ConfigurationMasterIdentityId::parse(request.identity_id)
        .map_err(|_| invalid())?;
    let operations = state
        .configuration_identity_command_operations
        .clone()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    tokio::spawn(async move {
        let _permit = permit;
        operations.retire_by_id(instance, identity_id).await
    })
    .await
    .map_err(|_| PublicError::ConfigurationSyncUnavailable)?
    .map_err(PublicError::ConfigurationIdentity)?;
    Ok((
        axum::http::StatusCode::NO_CONTENT,
        [(header::CACHE_CONTROL, "no-store")],
    ))
}

pub(super) trait ConfigurationIdentityStatusOperations: Send + Sync {
    fn status(
        &self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ConfigurationMasterIdentityStatus, ConfigurationStateError>>
                + Send
                + '_,
        >,
    >;
}
pub(super) struct ConfigurationIdentityStatusAdapter<T>(pub T);
impl<T: ConfigurationIdentityStatusStore> ConfigurationIdentityStatusOperations
    for ConfigurationIdentityStatusAdapter<T>
{
    fn status(
        &self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ConfigurationMasterIdentityStatus, ConfigurationStateError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(self.0.get_configuration_identity_status())
    }
}

pub(super) async fn get_status<W, S, R>(
    State(state): State<AppState<W, S, R>>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let operations = state
        .configuration_identity_status_operations
        .as_ref()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    let status = operations
        .status()
        .await
        .map_err(|_| PublicError::ConfigurationSyncUnavailable)?;
    let authority = status.state.role().authority();
    let response = ConfigurationIdentityStatusResponse {
        instance_id: status.state.instance_id().as_str().to_owned(),
        state_version: status.state.version(),
        role: match status.state.role() {
            ConfigurationRole::Unassigned => ConfigurationSyncRole::Unassigned,
            ConfigurationRole::Master(_) => ConfigurationSyncRole::Master,
            ConfigurationRole::Follower(_) => ConfigurationSyncRole::Follower,
        },
        group_id: authority.map(|value| value.group_id().as_str().to_owned()),
        master_instance_id: authority.map(|value| value.master_id().as_str().to_owned()),
        identity: status
            .identity
            .map(|identity| ConfigurationIdentitySummaryResponse {
                identity_id: identity.identity_id.as_str().to_owned(),
                phase: match identity.phase {
                    ConfigurationMasterIdentityPhase::Pending => {
                        ConfigurationIdentityPhase::Pending
                    }
                    ConfigurationMasterIdentityPhase::Active => ConfigurationIdentityPhase::Active,
                },
                server_name: identity.server_name,
                certificate_authority_fingerprint: identity
                    .certificate_authority_fingerprint
                    .as_str()
                    .to_owned(),
                not_before_unix_seconds: identity.not_before_unix_seconds,
                leaf_not_after_unix_seconds: identity.leaf_not_after_unix_seconds,
                ca_not_after_unix_seconds: identity.ca_not_after_unix_seconds,
            }),
    };
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(response)))
}
