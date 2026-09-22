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

pub(super) trait ConfigurationAdministrationOperations: Send + Sync {
    fn designate(
        &self,
        instance: kiln_core::KilnInstanceId,
        version: u64,
        key: String,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        kiln_core::ConfigurationInstanceState,
                        kiln_core::ConfigurationMasterError,
                    >,
                > + Send
                + '_,
        >,
    >;
}

pub(super) struct ConfigurationAdministrationAdapter<T, I>(pub T, pub I);

impl<T: kiln_core::ConfigurationAdministrationStore, I: kiln_core::ConfigurationGroupIdGenerator>
    ConfigurationAdministrationOperations for ConfigurationAdministrationAdapter<T, I>
{
    fn designate(
        &self,
        instance: kiln_core::KilnInstanceId,
        version: u64,
        key: String,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        kiln_core::ConfigurationInstanceState,
                        kiln_core::ConfigurationMasterError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.0
                .designate_configuration_master(
                    &instance,
                    version,
                    &key,
                    self.1.next_configuration_group_id(),
                )
                .await
        })
    }
}

pub(super) async fn designate_master<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    headers: axum::http::HeaderMap,
    super::StrictJson(request): super::StrictJson<
        kiln_protocol::DesignateConfigurationMasterRequest,
    >,
) -> Result<Json<kiln_protocol::ConfigurationMasterDesignationResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let key = super::required_idempotency_key(&headers)?;
    let instance =
        kiln_core::KilnInstanceId::parse(request.expected_instance_id).map_err(|_| {
            PublicError::ConfigurationMaster(kiln_core::ConfigurationMasterError::InvalidRequest)
        })?;
    let operations = state
        .configuration_administration_operations
        .as_ref()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    let result = operations
        .designate(instance, request.expected_state_version, key)
        .await
        .map_err(PublicError::ConfigurationMaster)?;
    let ConfigurationRole::Master(authority) = result.role() else {
        return Err(PublicError::ConfigurationSyncUnavailable);
    };
    Ok(Json(
        kiln_protocol::ConfigurationMasterDesignationResponse {
            instance_id: result.instance_id().as_str().to_owned(),
            state_version: result.version(),
            group_id: authority.group_id().as_str().to_owned(),
        },
    ))
}

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
