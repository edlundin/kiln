use super::{AppState, PublicError, RunOperations, SessionOperations, WorkspaceOperations};
use axum::{Json, extract::State};
use kiln_core::{
    ConfigurationRevision, ConfigurationRole, ConfigurationSnapshotError,
    ConfigurationSnapshotStore, ConfigurationSyncStatus,
};
use kiln_protocol::{
    ConfigurationRevisionResponse, ConfigurationSyncRole, ConfigurationSyncStatusResponse,
    ConfigurationSyncTransportState,
};
use std::{future::Future, pin::Pin};

pub(super) trait ConfigurationStatusOperations: Send + Sync {
    fn status(
        &self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ConfigurationSyncStatus, ConfigurationSnapshotError>>
                + Send
                + '_,
        >,
    >;
}
pub(super) struct ConfigurationStatusAdapter<T>(pub T);
impl<T: ConfigurationSnapshotStore> ConfigurationStatusOperations
    for ConfigurationStatusAdapter<T>
{
    fn status(
        &self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ConfigurationSyncStatus, ConfigurationSnapshotError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(self.0.get_configuration_sync_status())
    }
}

pub(super) async fn get_status<W, S, R>(
    State(state): State<AppState<W, S, R>>,
) -> Result<Json<ConfigurationSyncStatusResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let operations = state
        .configuration_status_operations
        .as_ref()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    let status = operations
        .status()
        .await
        .map_err(|_| PublicError::ConfigurationSyncUnavailable)?;
    let authority = status.state.role().authority();
    Ok(Json(ConfigurationSyncStatusResponse {
        instance_id: status.state.instance_id().as_str().to_owned(),
        state_version: status.state.version(),
        role: match status.state.role() {
            ConfigurationRole::Unassigned => ConfigurationSyncRole::Unassigned,
            ConfigurationRole::Master(_) => ConfigurationSyncRole::Master,
            ConfigurationRole::Follower(_) => ConfigurationSyncRole::Follower,
        },
        group_id: authority.map(|value| value.group_id().as_str().to_owned()),
        master_instance_id: authority.map(|value| value.master_id().as_str().to_owned()),
        applied_revision: status.applied_revision.as_ref().map(revision_response),
        observed_revision: status.state.observed().map(revision_response),
        // No transport/enrollment implementation exists yet. Matching durable
        // revisions cannot establish currentness or a live master connection.
        transport: ConfigurationSyncTransportState::Unconfigured,
    }))
}
fn revision_response(revision: &ConfigurationRevision) -> ConfigurationRevisionResponse {
    ConfigurationRevisionResponse {
        revision: revision.number(),
        schema_version: revision.schema_version(),
        content_hash: revision.content_hash().as_str().to_owned(),
    }
}
