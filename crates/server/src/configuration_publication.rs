use super::{
    AppState, PublicError, RunOperations, SessionOperations, StrictJson, WorkspaceOperations,
};
use axum::{Json, extract::State, http::HeaderMap, response::IntoResponse};
use kiln_core::{
    ConfigurationGroupId, ConfigurationPublicationStore, ConfigurationSnapshotError as Error,
    ConfigurationSnapshotMutation, ConfigurationSnapshotReadLimits, ConfigurationSnapshotStore,
    ContentHash, GlobalSkillId, KilnInstanceId, SharedConfigurationLimits,
    SharedConfigurationSnapshot, SharedSkillFileInput, SharedSkillLimits, SharedSkillPackage,
    SharedSkillPackageInput, StoredConfigurationSnapshot,
};
use kiln_protocol::{
    CONFIGURATION_PUBLICATION_MAX_BYTES, ConfigurationPublicationResponse,
    ConfigurationRevisionResponse, ConfigurationSnapshotResponse,
    PublishConfigurationSnapshotRequest, SharedConfigurationBundle, SharedSkillFileBundle,
    SharedSkillPackageBundle,
};
use std::{future::Future, pin::Pin};

pub(super) trait ConfigurationPublicationOperations: Send + Sync {
    fn snapshot(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<StoredConfigurationSnapshot>, Error>> + Send + '_>>;
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
impl<T: ConfigurationPublicationStore + ConfigurationSnapshotStore>
    ConfigurationPublicationOperations for ConfigurationPublicationAdapter<T>
{
    fn snapshot(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<StoredConfigurationSnapshot>, Error>> + Send + '_>>
    {
        Box::pin(self.0.get_configuration_snapshot(bundle_limits()))
    }
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

pub(super) async fn get_snapshot<W, S, R>(
    State(state): State<AppState<W, S, R>>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let operations = state
        .configuration_publication_operations
        .as_ref()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    let stored = operations
        .snapshot()
        .await
        .map_err(|error| match error {
            Error::LimitExceeded => PublicError::ConfigurationSnapshotTooLarge,
            _ => PublicError::ConfigurationSyncUnavailable,
        })?
        .ok_or(PublicError::ConfigurationSnapshotNotFound)?;
    let response = ConfigurationSnapshotResponse {
        instance_id: stored.state.instance_id().as_str().to_owned(),
        group_id: stored.revision.authority().group_id().as_str().to_owned(),
        master_instance_id: stored.revision.authority().master_id().as_str().to_owned(),
        state_version: stored.state.version(),
        revision: ConfigurationRevisionResponse {
            revision: stored.revision.number(),
            schema_version: stored.revision.schema_version(),
            content_hash: stored.revision.content_hash().as_str().to_owned(),
        },
        snapshot: SharedConfigurationBundle {
            metadata_json: stored.snapshot.metadata_json().to_owned(),
            skills: stored
                .snapshot
                .skills()
                .iter()
                .map(|skill| SharedSkillPackageBundle {
                    id: skill.id().as_str().to_owned(),
                    version: skill.version().to_owned(),
                    enabled: skill.enabled(),
                    dependencies: skill
                        .dependencies()
                        .iter()
                        .map(|id| id.as_str().to_owned())
                        .collect(),
                    files: skill
                        .files()
                        .iter()
                        .map(|file| SharedSkillFileBundle {
                            path: file.path().to_owned(),
                            content: file.content().to_vec(),
                            content_hash: file.content_hash().as_str().to_owned(),
                        })
                        .collect(),
                })
                .collect(),
        },
    };
    // Bound encoded JSON (including byte-array expansion) before sending any body.
    let mut output = BoundedJson {
        bytes: Vec::new(),
        exceeded: false,
    };
    if serde_json::to_writer(&mut output, &response).is_err() {
        return Err(if output.exceeded {
            PublicError::ConfigurationSnapshotTooLarge
        } else {
            PublicError::ConfigurationSyncUnavailable
        });
    }
    Ok((
        [
            (axum::http::header::CONTENT_TYPE, "application/json"),
            (axum::http::header::CACHE_CONTROL, "no-store"),
        ],
        output.bytes,
    ))
}

struct BoundedJson {
    bytes: Vec<u8>,
    exceeded: bool,
}
impl std::io::Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > CONFIGURATION_PUBLICATION_MAX_BYTES.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "configuration snapshot exceeds transfer budget",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn bundle_limits() -> ConfigurationSnapshotReadLimits {
    // Reuse the existing Axum 2 MiB JSON ceiling. Imports cap the serialized body;
    // exports preflight aggregate stored metadata/data and cap encoded output.
    // These are transfer budgets, not recommended catalog sizes or memory bounds.
    let cap = CONFIGURATION_PUBLICATION_MAX_BYTES;
    let configuration = SharedConfigurationLimits {
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
    let skill = SharedSkillLimits {
        max_identifier_bytes: cap,
        max_version_bytes: cap,
        max_dependencies: cap,
        max_files: cap,
        max_path_bytes: cap,
        max_file_bytes: cap,
        max_total_file_bytes: cap,
    };
    ConfigurationSnapshotReadLimits {
        configuration,
        skill,
        max_total_metadata_bytes: cap,
    }
}

fn decode_bundle(bundle: SharedConfigurationBundle) -> Result<SharedConfigurationSnapshot, Error> {
    let cap = CONFIGURATION_PUBLICATION_MAX_BYTES;
    let limits = bundle_limits();
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
                limits.skill,
            )
            .map_err(|_| Error::InvalidSnapshot)
        })
        .collect::<Result<Vec<_>, _>>()?;
    SharedConfigurationSnapshot::from_metadata_json(
        bundle.metadata_json.as_bytes(),
        skills,
        limits.configuration,
    )
    .map_err(|_| Error::InvalidSnapshot)
}
