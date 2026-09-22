use super::{
    AppState, PublicError, RunOperations, SessionOperations, StrictJson, WorkspaceOperations,
};
use axum::{Json, extract::State, http::HeaderMap};
use kiln_core::{
    ConfigurationGroupId, ConfigurationPublicationStore, ConfigurationSnapshotError as Error,
    ConfigurationSnapshotMutation, ContentHash, GlobalSkillId, KilnInstanceId,
    SharedConfigurationLimits, SharedConfigurationSnapshot, SharedSkillFileInput,
    SharedSkillLimits, SharedSkillPackage, SharedSkillPackageInput,
};
use kiln_protocol::{
    CONFIGURATION_PUBLICATION_MAX_BYTES, ConfigurationPublicationResponse,
    ConfigurationRevisionResponse, PublishConfigurationSnapshotRequest, SharedConfigurationBundle,
};
use std::{future::Future, pin::Pin};

pub(super) trait ConfigurationPublicationOperations: Send + Sync {
    fn publish(
        &self,
        instance: KilnInstanceId,
        group: ConfigurationGroupId,
        version: u64,
        key: String,
        snapshot: SharedConfigurationSnapshot,
    ) -> Pin<Box<dyn Future<Output = Result<ConfigurationSnapshotMutation, Error>> + Send + '_>>;
}

pub(super) struct ConfigurationPublicationAdapter<T>(pub T);
impl<T: ConfigurationPublicationStore> ConfigurationPublicationOperations
    for ConfigurationPublicationAdapter<T>
{
    fn publish(
        &self,
        instance: KilnInstanceId,
        group: ConfigurationGroupId,
        version: u64,
        key: String,
        snapshot: SharedConfigurationSnapshot,
    ) -> Pin<Box<dyn Future<Output = Result<ConfigurationSnapshotMutation, Error>> + Send + '_>>
    {
        Box::pin(async move {
            self.0
                .publish_configuration_snapshot_idempotent(
                    &instance, &group, version, &key, &snapshot,
                )
                .await
        })
    }
}

pub(super) async fn publish<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    headers: HeaderMap,
    StrictJson(request): StrictJson<PublishConfigurationSnapshotRequest>,
) -> Result<Json<ConfigurationPublicationResponse>, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let _command = state.lifecycle.begin_command()?;
    let key = super::required_idempotency_key(&headers)?;
    let operations = state
        .configuration_publication_operations
        .as_ref()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    let invalid = || PublicError::ConfigurationPublication(Error::InvalidRequest);
    let instance = KilnInstanceId::parse(request.expected_instance_id).map_err(|_| invalid())?;
    let group = ConfigurationGroupId::parse(request.expected_group_id).map_err(|_| invalid())?;
    if request.expected_state_version == 0 || request.expected_state_version >= i64::MAX as u64 {
        return Err(invalid());
    }
    let snapshot =
        decode_bundle(request.snapshot).map_err(PublicError::ConfigurationPublication)?;
    let result = operations
        .publish(
            instance,
            group,
            request.expected_state_version,
            key,
            snapshot,
        )
        .await
        .map_err(PublicError::ConfigurationPublication)?;
    Ok(Json(ConfigurationPublicationResponse {
        instance_id: result.state.instance_id().as_str().to_owned(),
        group_id: result.revision.authority().group_id().as_str().to_owned(),
        state_version: result.state.version(),
        revision: ConfigurationRevisionResponse {
            revision: result.revision.number(),
            schema_version: result.revision.schema_version(),
            content_hash: result.revision.content_hash().as_str().to_owned(),
        },
    }))
}

fn decode_bundle(bundle: SharedConfigurationBundle) -> Result<SharedConfigurationSnapshot, Error> {
    // The whole serialized request is capped before deserialization, using the
    // existing Axum 2 MiB JSON ceiling. Each field/count must fit within that
    // budget as well; these are transport bounds, not recommended catalog sizes.
    // JSON integer arrays preserve arbitrary file bytes without an archive parser.
    let cap = CONFIGURATION_PUBLICATION_MAX_BYTES;
    let configuration_limits = SharedConfigurationLimits {
        max_key_bytes: cap,
        max_metadata_bytes: cap,
        max_mcp_servers: cap,
        max_mcp_arguments: cap,
        max_mcp_argument_bytes: cap,
        max_mcp_environment: cap,
        max_endpoint_bytes: cap,
        max_skills: cap,
        max_total_skill_files: cap,
        max_total_skill_bytes: cap,
    };
    let skill_limits = SharedSkillLimits {
        max_identifier_bytes: cap,
        max_version_bytes: cap,
        max_dependencies: cap,
        max_files: cap,
        max_path_bytes: cap,
        max_file_bytes: cap,
        max_total_file_bytes: cap,
    };
    if bundle.metadata_json.len() > cap || bundle.skills.len() > cap {
        return Err(Error::LimitExceeded);
    }
    let skills = bundle
        .skills
        .into_iter()
        .map(|skill| {
            let id = GlobalSkillId::parse(skill.id, cap).map_err(|_| Error::InvalidSnapshot)?;
            let dependencies = skill
                .dependencies
                .into_iter()
                .map(|id| GlobalSkillId::parse(id, cap).map_err(|_| Error::InvalidSnapshot))
                .collect::<Result<_, _>>()?;
            let files = skill
                .files
                .into_iter()
                .map(|file| {
                    Ok(SharedSkillFileInput {
                        path: file.path,
                        content: file.content,
                        expected_hash: ContentHash::parse(file.content_hash)
                            .map_err(|_| Error::InvalidSnapshot)?,
                    })
                })
                .collect::<Result<_, Error>>()?;
            SharedSkillPackage::validate(
                SharedSkillPackageInput {
                    id,
                    version: skill.version,
                    enabled: skill.enabled,
                    dependencies,
                    files,
                },
                skill_limits,
            )
            .map_err(|_| Error::InvalidSnapshot)
        })
        .collect::<Result<Vec<_>, _>>()?;
    SharedConfigurationSnapshot::from_metadata_json(
        bundle.metadata_json.as_bytes(),
        skills,
        configuration_limits,
    )
    .map_err(|_| Error::InvalidSnapshot)
}
