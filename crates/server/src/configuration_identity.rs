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
