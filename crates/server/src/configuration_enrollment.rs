use super::{AppState, PublicError, RunOperations, SessionOperations, WorkspaceOperations};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::header,
    response::IntoResponse,
};
use kiln_core::{
    ConfigurationAuthority, ConfigurationFollowerEnrollmentChoice,
    ConfigurationFollowerEnrollmentError, ConfigurationFollowerEnrollmentMetadata,
    ConfigurationFollowerEnrollmentPhase, ConfigurationGroupId, ConfigurationReadGrantAttemptId,
    KilnInstanceId,
};
use kiln_protocol::{
    ConfigurationFollowerEnrollmentListResponse,
    ConfigurationFollowerEnrollmentPhase as ProtocolPhase, ConfigurationFollowerEnrollmentResponse,
    PrepareConfigurationFollowerEnrollmentRequest, RetireConfigurationFollowerEnrollmentRequest,
};
use serde::Deserialize;
use std::{future::Future, pin::Pin};

pub(super) trait ConfigurationFollowerEnrollmentOperations: Send + Sync {
    fn prepare(
        &self,
        attempt_id: ConfigurationReadGrantAttemptId,
        choice: ConfigurationFollowerEnrollmentChoice,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        ConfigurationFollowerEnrollmentMetadata,
                        ConfigurationFollowerEnrollmentError,
                    >,
                > + Send
                + '_,
        >,
    >;

    fn list(
        &self,
        after: Option<ConfigurationReadGrantAttemptId>,
        limit: usize,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Vec<ConfigurationFollowerEnrollmentMetadata>,
                        ConfigurationFollowerEnrollmentError,
                    >,
                > + Send
                + '_,
        >,
    >;

    fn get(
        &self,
        attempt_id: ConfigurationReadGrantAttemptId,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Option<ConfigurationFollowerEnrollmentMetadata>,
                        ConfigurationFollowerEnrollmentError,
                    >,
                > + Send
                + '_,
        >,
    >;

    fn retire(
        &self,
        expected_instance_id: KilnInstanceId,
        attempt_id: ConfigurationReadGrantAttemptId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ConfigurationFollowerEnrollmentError>> + Send + '_>>;
}

pub(super) struct ConfigurationFollowerEnrollmentAdapter<T>(pub T);

impl<T: kiln_core::ConfigurationFollowerEnrollmentAdministration>
    ConfigurationFollowerEnrollmentOperations for ConfigurationFollowerEnrollmentAdapter<T>
{
    fn prepare(
        &self,
        attempt_id: ConfigurationReadGrantAttemptId,
        choice: ConfigurationFollowerEnrollmentChoice,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        ConfigurationFollowerEnrollmentMetadata,
                        ConfigurationFollowerEnrollmentError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(
            self.0
                .prepare_configuration_follower_enrollment(attempt_id, choice),
        )
    }

    fn list(
        &self,
        after: Option<ConfigurationReadGrantAttemptId>,
        limit: usize,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Vec<ConfigurationFollowerEnrollmentMetadata>,
                        ConfigurationFollowerEnrollmentError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.0
                .list_configuration_follower_enrollments(after.as_ref(), limit)
                .await
        })
    }

    fn get(
        &self,
        attempt_id: ConfigurationReadGrantAttemptId,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Option<ConfigurationFollowerEnrollmentMetadata>,
                        ConfigurationFollowerEnrollmentError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.0
                .get_configuration_follower_enrollment(&attempt_id)
                .await
        })
    }

    fn retire(
        &self,
        expected_instance_id: KilnInstanceId,
        attempt_id: ConfigurationReadGrantAttemptId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ConfigurationFollowerEnrollmentError>> + Send + '_>>
    {
        Box::pin(
            self.0
                .retire_configuration_follower_enrollment(expected_instance_id, attempt_id),
        )
    }
}

#[derive(Deserialize)]
pub(super) struct ListQuery {
    after: Option<String>,
    limit: Option<usize>,
}

pub(super) async fn prepare<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    super::StrictJson(request): super::StrictJson<PrepareConfigurationFollowerEnrollmentRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let permit = state.lifecycle.begin_command()?;
    let attempt_id = ConfigurationReadGrantAttemptId::parse(request.attempt_id)
        .map_err(|_| enrollment_invalid())?;
    let expected_instance_id =
        KilnInstanceId::parse(request.expected_instance_id).map_err(|_| enrollment_invalid())?;
    let group_id =
        ConfigurationGroupId::parse(request.group_id).map_err(|_| enrollment_invalid())?;
    let master_instance_id =
        KilnInstanceId::parse(request.master_instance_id).map_err(|_| enrollment_invalid())?;
    if request.certificate_authority_der.is_empty()
        || request.certificate_authority_der.len()
            > kiln_protocol::CONFIGURATION_FOLLOWER_ENROLLMENT_MAX_BYTES
    {
        return Err(enrollment_invalid());
    }
    let choice = ConfigurationFollowerEnrollmentChoice {
        expected_instance_id,
        expected_state_version: request.expected_state_version,
        authority: ConfigurationAuthority::new(group_id, master_instance_id),
        server_name: request.server_name,
        certificate_authority_der: request.certificate_authority_der,
    };
    let operations = state
        .configuration_follower_enrollment_operations
        .clone()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    // Keep lifecycle ownership until the vault-backed reservation finishes even
    // if this request handler is cancelled.
    let metadata = tokio::spawn(async move {
        let _permit = permit;
        operations.prepare(attempt_id, choice).await
    })
    .await
    .map_err(|_| PublicError::ConfigurationSyncUnavailable)?
    .map_err(PublicError::ConfigurationFollowerEnrollment)?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(enrollment_response(&metadata)),
    ))
}

pub(super) async fn list<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Query(query): Query<ListQuery>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let limit = query.limit.unwrap_or(50);
    if !(1..=100).contains(&limit) {
        return Err(enrollment_invalid());
    }
    let after = query
        .after
        .map(ConfigurationReadGrantAttemptId::parse)
        .transpose()
        .map_err(|_| enrollment_invalid())?;
    let operations = state
        .configuration_follower_enrollment_operations
        .as_ref()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    let mut enrollments = operations
        .list(after, limit + 1)
        .await
        .map_err(PublicError::ConfigurationFollowerEnrollment)?;
    let has_more = enrollments.len() > limit;
    enrollments.truncate(limit);
    let next_cursor = has_more
        .then(|| {
            enrollments
                .last()
                .map(|entry| entry.attempt_id.as_str().to_owned())
        })
        .flatten();
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(ConfigurationFollowerEnrollmentListResponse {
            enrollments: enrollments.iter().map(enrollment_response).collect(),
            next_cursor,
        }),
    ))
}

pub(super) async fn get<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(attempt_id): Path<String>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let attempt_id =
        ConfigurationReadGrantAttemptId::parse(attempt_id).map_err(|_| enrollment_invalid())?;
    let operations = state
        .configuration_follower_enrollment_operations
        .as_ref()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    let metadata = operations
        .get(attempt_id)
        .await
        .map_err(PublicError::ConfigurationFollowerEnrollment)?
        .ok_or(PublicError::ConfigurationFollowerEnrollmentNotFound)?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(enrollment_response(&metadata)),
    ))
}

pub(super) async fn retire<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(attempt_id): Path<String>,
    super::StrictJson(request): super::StrictJson<RetireConfigurationFollowerEnrollmentRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let permit = state.lifecycle.begin_command()?;
    let attempt_id =
        ConfigurationReadGrantAttemptId::parse(attempt_id).map_err(|_| enrollment_invalid())?;
    let expected_instance_id =
        KilnInstanceId::parse(request.expected_instance_id).map_err(|_| enrollment_invalid())?;
    let operations = state
        .configuration_follower_enrollment_operations
        .clone()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    tokio::spawn(async move {
        let _permit = permit;
        operations.retire(expected_instance_id, attempt_id).await
    })
    .await
    .map_err(|_| PublicError::ConfigurationSyncUnavailable)?
    .map_err(PublicError::ConfigurationFollowerEnrollment)?;
    Ok((
        axum::http::StatusCode::NO_CONTENT,
        [(header::CACHE_CONTROL, "no-store")],
    ))
}

fn enrollment_invalid() -> PublicError {
    PublicError::ConfigurationFollowerEnrollment(
        ConfigurationFollowerEnrollmentError::InvalidRequest,
    )
}

fn enrollment_response(
    metadata: &ConfigurationFollowerEnrollmentMetadata,
) -> ConfigurationFollowerEnrollmentResponse {
    ConfigurationFollowerEnrollmentResponse {
        attempt_id: metadata.attempt_id.as_str().to_owned(),
        follower_instance_id: metadata.follower_instance_id.as_str().to_owned(),
        expected_state_version: metadata.expected_state_version,
        group_id: metadata.authority.group_id().as_str().to_owned(),
        master_instance_id: metadata.authority.master_id().as_str().to_owned(),
        server_name: metadata.server_name.clone(),
        certificate_authority_fingerprint: metadata
            .certificate_authority_fingerprint
            .as_str()
            .to_owned(),
        phase: match metadata.phase {
            ConfigurationFollowerEnrollmentPhase::Reserved => ProtocolPhase::Reserved,
            ConfigurationFollowerEnrollmentPhase::Prepared => ProtocolPhase::Prepared,
            ConfigurationFollowerEnrollmentPhase::Retired => ProtocolPhase::Retired,
        },
    }
}
