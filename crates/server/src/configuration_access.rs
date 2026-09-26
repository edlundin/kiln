use super::{AppState, PublicError, RunOperations, SessionOperations, WorkspaceOperations};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::header,
    response::IntoResponse,
};
use kiln_core::{
    ConfigurationAccessError, ConfigurationAccessStore, ConfigurationAuthority,
    ConfigurationFollowerEnrollmentRequest, ConfigurationFollowerEnrollmentRequestConfirmation,
    ConfigurationFollowerEnrollmentRequestId, ConfigurationFollowerEnrollmentRequestPhase,
    ConfigurationGroupId, ConfigurationInstanceState, ConfigurationReadGrant,
    ConfigurationReadGrantAttemptId, ConfigurationReadGrantId, ConfigurationReadGrantSummary,
    ConfigurationStateError, ConfigurationStateStore, ContentHash, KilnInstanceId,
};
use kiln_protocol::{
    ConfigurationFollowerEnrollmentDecisionRequest,
    ConfigurationFollowerEnrollmentRequestListResponse,
    ConfigurationFollowerEnrollmentRequestPhase as ProtocolRequestPhase,
    ConfigurationFollowerEnrollmentRequestResponse, ConfigurationReadGrantListResponse,
    ConfigurationReadGrantResponse, RevokeConfigurationReadGrantRequest,
};
use serde::Deserialize;
use std::{future::Future, pin::Pin};

pub(super) trait ConfigurationAccessOperations: Send + Sync {
    fn get(
        &self,
        grant_id: ConfigurationReadGrantId,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ConfigurationReadGrant>, ConfigurationAccessError>>
                + Send
                + '_,
        >,
    >;

    fn get_by_attempt(
        &self,
        attempt_id: ConfigurationReadGrantAttemptId,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ConfigurationReadGrant>, ConfigurationAccessError>>
                + Send
                + '_,
        >,
    >;

    fn list(
        &self,
        after: Option<ConfigurationReadGrantId>,
        limit: usize,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ConfigurationReadGrant>, ConfigurationAccessError>>
                + Send
                + '_,
        >,
    >;

    fn revoke(
        &self,
        instance_id: KilnInstanceId,
        state_version: u64,
        grant_id: ConfigurationReadGrantId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ConfigurationAccessError>> + Send + '_>>;

    fn get_enrollment_request(
        &self,
        request_id: ConfigurationFollowerEnrollmentRequestId,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Option<ConfigurationFollowerEnrollmentRequest>,
                        ConfigurationAccessError,
                    >,
                > + Send
                + '_,
        >,
    >;

    fn list_enrollment_requests(
        &self,
        after: Option<ConfigurationFollowerEnrollmentRequestId>,
        limit: usize,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Vec<ConfigurationFollowerEnrollmentRequest>,
                        ConfigurationAccessError,
                    >,
                > + Send
                + '_,
        >,
    >;

    fn approve_enrollment_request(
        &self,
        instance_id: KilnInstanceId,
        state_version: u64,
        confirmation: ConfigurationFollowerEnrollmentRequestConfirmation,
        grant_id: ConfigurationReadGrantId,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        ConfigurationFollowerEnrollmentRequest,
                        ConfigurationAccessError,
                    >,
                > + Send
                + '_,
        >,
    >;

    fn reject_enrollment_request(
        &self,
        instance_id: KilnInstanceId,
        state_version: u64,
        confirmation: ConfigurationFollowerEnrollmentRequestConfirmation,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        ConfigurationFollowerEnrollmentRequest,
                        ConfigurationAccessError,
                    >,
                > + Send
                + '_,
        >,
    >;
}

pub(super) struct ConfigurationAccessAdapter<T>(pub T);

impl<T> ConfigurationAccessOperations for ConfigurationAccessAdapter<T>
where
    T: ConfigurationAccessStore + ConfigurationStateStore,
{
    fn get(
        &self,
        grant_id: ConfigurationReadGrantId,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ConfigurationReadGrant>, ConfigurationAccessError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move { self.0.get_configuration_read_grant(&grant_id).await })
    }

    fn get_by_attempt(
        &self,
        attempt_id: ConfigurationReadGrantAttemptId,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ConfigurationReadGrant>, ConfigurationAccessError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.0
                .get_configuration_read_grant_by_attempt(&attempt_id)
                .await
        })
    }

    fn list(
        &self,
        after: Option<ConfigurationReadGrantId>,
        limit: usize,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ConfigurationReadGrant>, ConfigurationAccessError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.0
                .list_configuration_read_grants(after.as_ref(), limit)
                .await
        })
    }

    fn revoke(
        &self,
        instance_id: KilnInstanceId,
        state_version: u64,
        grant_id: ConfigurationReadGrantId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ConfigurationAccessError>> + Send + '_>> {
        Box::pin(async move {
            let current = self
                .0
                .get_configuration_instance()
                .await
                .map_err(ConfigurationAccessError::State)?
                .ok_or(ConfigurationAccessError::State(
                    ConfigurationStateError::Uninitialized,
                ))?;
            if current.instance_id() != &instance_id || current.version() != state_version {
                return Err(ConfigurationAccessError::Conflict);
            }
            self.0
                .revoke_configuration_reader(&current, &grant_id)
                .await
        })
    }

    fn get_enrollment_request(
        &self,
        request_id: ConfigurationFollowerEnrollmentRequestId,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Option<ConfigurationFollowerEnrollmentRequest>,
                        ConfigurationAccessError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.0
                .get_configuration_follower_enrollment_request(&request_id)
                .await
        })
    }

    fn list_enrollment_requests(
        &self,
        after: Option<ConfigurationFollowerEnrollmentRequestId>,
        limit: usize,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Vec<ConfigurationFollowerEnrollmentRequest>,
                        ConfigurationAccessError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.0
                .list_configuration_follower_enrollment_requests(after.as_ref(), limit)
                .await
        })
    }

    fn approve_enrollment_request(
        &self,
        instance_id: KilnInstanceId,
        state_version: u64,
        confirmation: ConfigurationFollowerEnrollmentRequestConfirmation,
        grant_id: ConfigurationReadGrantId,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        ConfigurationFollowerEnrollmentRequest,
                        ConfigurationAccessError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let expected = expected_master_state(&self.0, &instance_id, state_version).await?;
            self.0
                .approve_configuration_follower_enrollment_request(
                    &expected,
                    &confirmation,
                    &grant_id,
                )
                .await
        })
    }

    fn reject_enrollment_request(
        &self,
        instance_id: KilnInstanceId,
        state_version: u64,
        confirmation: ConfigurationFollowerEnrollmentRequestConfirmation,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        ConfigurationFollowerEnrollmentRequest,
                        ConfigurationAccessError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let expected = expected_master_state(&self.0, &instance_id, state_version).await?;
            self.0
                .reject_configuration_follower_enrollment_request(&expected, &confirmation)
                .await
        })
    }
}

async fn expected_master_state<T: ConfigurationStateStore>(
    store: &T,
    instance_id: &KilnInstanceId,
    state_version: u64,
) -> Result<ConfigurationInstanceState, ConfigurationAccessError> {
    let current = store
        .get_configuration_instance()
        .await
        .map_err(ConfigurationAccessError::State)?
        .ok_or(ConfigurationAccessError::State(
            ConfigurationStateError::Uninitialized,
        ))?;
    if current.instance_id() != instance_id || current.version() != state_version {
        return Err(ConfigurationAccessError::Conflict);
    }
    Ok(current)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GrantListQuery {
    limit: Option<usize>,
    after: Option<String>,
}

pub(super) async fn list<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Query(query): Query<GrantListQuery>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let limit = query.limit.unwrap_or(50);
    if !(1..=100).contains(&limit) {
        return Err(PublicError::ConfigurationReadGrant(
            ConfigurationAccessError::InvalidRequest,
        ));
    }
    let after = query
        .after
        .map(ConfigurationReadGrantId::parse)
        .transpose()
        .map_err(|_| {
            PublicError::ConfigurationReadGrant(ConfigurationAccessError::InvalidRequest)
        })?;
    let operations = state
        .configuration_access_operations
        .as_ref()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    let mut grants = operations
        .list(after, limit + 1)
        .await
        .map_err(PublicError::ConfigurationReadGrant)?;
    let has_more = grants.len() > limit;
    grants.truncate(limit);
    let next_cursor = has_more
        .then(|| {
            grants
                .last()
                .map(|grant| grant.grant_id.as_str().to_owned())
        })
        .flatten();
    let response = ConfigurationReadGrantListResponse {
        grants: grants.iter().map(grant_response).collect(),
        next_cursor,
    };
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(response)))
}

pub(super) async fn get<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(grant_id): Path<String>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let grant_id = ConfigurationReadGrantId::parse(grant_id).map_err(|_| {
        PublicError::ConfigurationReadGrant(ConfigurationAccessError::InvalidRequest)
    })?;
    let operations = state
        .configuration_access_operations
        .as_ref()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    let grant = operations
        .get(grant_id)
        .await
        .map_err(PublicError::ConfigurationReadGrant)?
        .ok_or(PublicError::ConfigurationReadGrantNotFound)?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(grant_response(&grant)),
    ))
}

pub(super) async fn get_by_attempt<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(attempt_id): Path<String>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let attempt_id = ConfigurationReadGrantAttemptId::parse(attempt_id).map_err(|_| {
        PublicError::ConfigurationReadGrant(ConfigurationAccessError::InvalidRequest)
    })?;
    let operations = state
        .configuration_access_operations
        .as_ref()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    let grant = operations
        .get_by_attempt(attempt_id)
        .await
        .map_err(PublicError::ConfigurationReadGrant)?
        .ok_or(PublicError::ConfigurationReadGrantNotFound)?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(grant_response(&grant)),
    ))
}

pub(super) async fn revoke<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(grant_id): Path<String>,
    super::StrictJson(request): super::StrictJson<RevokeConfigurationReadGrantRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let permit = state.lifecycle.begin_command()?;
    let grant_id = ConfigurationReadGrantId::parse(grant_id).map_err(|_| {
        PublicError::ConfigurationReadGrant(ConfigurationAccessError::InvalidRequest)
    })?;
    let instance_id = KilnInstanceId::parse(request.expected_instance_id).map_err(|_| {
        PublicError::ConfigurationReadGrant(ConfigurationAccessError::InvalidRequest)
    })?;
    let operations = state
        .configuration_access_operations
        .clone()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    tokio::spawn(async move {
        let _permit = permit;
        operations
            .revoke(instance_id, request.expected_state_version, grant_id)
            .await
    })
    .await
    .map_err(|_| PublicError::ConfigurationSyncUnavailable)?
    .map_err(PublicError::ConfigurationReadGrant)?;
    Ok((
        axum::http::StatusCode::NO_CONTENT,
        [(header::CACHE_CONTROL, "no-store")],
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EnrollmentRequestListQuery {
    limit: Option<usize>,
    after: Option<String>,
}

pub(super) async fn list_enrollment_requests<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Query(query): Query<EnrollmentRequestListQuery>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let limit = query
        .limit
        .unwrap_or(kiln_protocol::CONFIGURATION_FOLLOWER_ENROLLMENT_REQUEST_DEFAULT_PAGE_SIZE);
    if !(1..=kiln_protocol::CONFIGURATION_FOLLOWER_ENROLLMENT_REQUEST_MAX_PAGE_SIZE)
        .contains(&limit)
    {
        return Err(PublicError::ConfigurationFollowerEnrollmentRequest(
            ConfigurationAccessError::InvalidRequest,
        ));
    }
    let after = query
        .after
        .map(ConfigurationFollowerEnrollmentRequestId::parse)
        .transpose()
        .map_err(|_| {
            PublicError::ConfigurationFollowerEnrollmentRequest(
                ConfigurationAccessError::InvalidRequest,
            )
        })?;
    let operations = state
        .configuration_access_operations
        .as_ref()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    let mut requests = operations
        .list_enrollment_requests(after, limit + 1)
        .await
        .map_err(PublicError::ConfigurationFollowerEnrollmentRequest)?;
    let has_more = requests.len() > limit;
    requests.truncate(limit);
    let next_cursor = has_more
        .then(|| {
            requests
                .last()
                .map(|request| request.request_id.as_str().to_owned())
        })
        .flatten();
    let response = ConfigurationFollowerEnrollmentRequestListResponse {
        requests: requests.iter().map(enrollment_request_response).collect(),
        next_cursor,
    };
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(response)))
}

pub(super) async fn get_enrollment_request<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(request_id): Path<String>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let request_id = parse_request_id(&request_id)?;
    let operations = state
        .configuration_access_operations
        .as_ref()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    let request = operations
        .get_enrollment_request(request_id)
        .await
        .map_err(PublicError::ConfigurationFollowerEnrollmentRequest)?
        .ok_or(PublicError::ConfigurationFollowerEnrollmentRequestNotFound)?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(enrollment_request_response(&request)),
    ))
}

pub(super) async fn approve_enrollment_request<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(request_id): Path<String>,
    super::StrictJson(body): super::StrictJson<ConfigurationFollowerEnrollmentDecisionRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let permit = state.lifecycle.begin_command()?;
    let request_id = parse_request_id(&request_id)?;
    let confirmation = parse_confirmation(request_id, &body)?;
    let expected_instance_id = parse_expected_instance_id(&body.expected_instance_id)?;
    let expected_state_version = body.expected_state_version;
    let operations = state
        .configuration_access_operations
        .clone()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    let grant_id = ConfigurationReadGrantId::from_ulid(ulid::Ulid::generate());
    let request = tokio::spawn(async move {
        let _permit = permit;
        operations
            .approve_enrollment_request(
                expected_instance_id,
                expected_state_version,
                confirmation,
                grant_id,
            )
            .await
    })
    .await
    .map_err(|_| PublicError::ConfigurationSyncUnavailable)?
    .map_err(PublicError::ConfigurationFollowerEnrollmentRequest)?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(enrollment_request_response(&request)),
    ))
}

pub(super) async fn reject_enrollment_request<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    Path(request_id): Path<String>,
    super::StrictJson(body): super::StrictJson<ConfigurationFollowerEnrollmentDecisionRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: WorkspaceOperations + 'static,
    S: SessionOperations + 'static,
    R: RunOperations + 'static,
{
    let permit = state.lifecycle.begin_command()?;
    let request_id = parse_request_id(&request_id)?;
    let confirmation = parse_confirmation(request_id, &body)?;
    let expected_instance_id = parse_expected_instance_id(&body.expected_instance_id)?;
    let expected_state_version = body.expected_state_version;
    let operations = state
        .configuration_access_operations
        .clone()
        .ok_or(PublicError::ConfigurationSyncUnavailable)?;
    let request = tokio::spawn(async move {
        let _permit = permit;
        operations
            .reject_enrollment_request(expected_instance_id, expected_state_version, confirmation)
            .await
    })
    .await
    .map_err(|_| PublicError::ConfigurationSyncUnavailable)?
    .map_err(PublicError::ConfigurationFollowerEnrollmentRequest)?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(enrollment_request_response(&request)),
    ))
}

fn parse_request_id(value: &str) -> Result<ConfigurationFollowerEnrollmentRequestId, PublicError> {
    ConfigurationFollowerEnrollmentRequestId::parse(value.to_owned()).map_err(|_| {
        PublicError::ConfigurationFollowerEnrollmentRequest(
            ConfigurationAccessError::InvalidRequest,
        )
    })
}

fn parse_expected_instance_id(value: &str) -> Result<KilnInstanceId, PublicError> {
    KilnInstanceId::parse(value.to_owned()).map_err(|_| {
        PublicError::ConfigurationFollowerEnrollmentRequest(
            ConfigurationAccessError::InvalidRequest,
        )
    })
}

fn parse_confirmation(
    path_request_id: ConfigurationFollowerEnrollmentRequestId,
    body: &ConfigurationFollowerEnrollmentDecisionRequest,
) -> Result<ConfigurationFollowerEnrollmentRequestConfirmation, PublicError> {
    let invalid = || {
        PublicError::ConfigurationFollowerEnrollmentRequest(
            ConfigurationAccessError::InvalidRequest,
        )
    };
    let request_id = ConfigurationFollowerEnrollmentRequestId::parse(body.request_id.clone())
        .map_err(|_| invalid())?;
    if request_id != path_request_id {
        return Err(invalid());
    }
    let attempt_id =
        ConfigurationReadGrantAttemptId::parse(body.attempt_id.clone()).map_err(|_| invalid())?;
    let follower_id = KilnInstanceId::parse(body.follower_id.clone()).map_err(|_| invalid())?;
    let group_id = ConfigurationGroupId::parse(body.group_id.clone()).map_err(|_| invalid())?;
    let master_instance_id =
        KilnInstanceId::parse(body.master_instance_id.clone()).map_err(|_| invalid())?;
    let master_ca_fingerprint =
        ContentHash::parse(body.master_ca_fingerprint.clone()).map_err(|_| invalid())?;
    let credential_fingerprint =
        ContentHash::parse(body.credential_fingerprint.clone()).map_err(|_| invalid())?;
    Ok(ConfigurationFollowerEnrollmentRequestConfirmation {
        request_id,
        attempt_id,
        follower_id,
        follower_state_version: body.follower_state_version,
        authority: ConfigurationAuthority::new(group_id, master_instance_id),
        server_name: body.server_name.clone(),
        master_ca_fingerprint,
        received_master_state_version: body.received_master_state_version,
        credential_fingerprint,
    })
}

pub(crate) fn enrollment_request_response(
    request: &ConfigurationFollowerEnrollmentRequest,
) -> ConfigurationFollowerEnrollmentRequestResponse {
    ConfigurationFollowerEnrollmentRequestResponse {
        request_id: request.request_id.as_str().to_owned(),
        attempt_id: request.attempt_id.as_str().to_owned(),
        follower_id: request.follower_id.as_str().to_owned(),
        follower_state_version: request.follower_state_version,
        group_id: request.authority.group_id().as_str().to_owned(),
        master_instance_id: request.authority.master_id().as_str().to_owned(),
        server_name: request.server_name.clone(),
        master_ca_fingerprint: request.master_ca_fingerprint.as_str().to_owned(),
        credential_fingerprint: request.credential_fingerprint.as_str().to_owned(),
        received_master_state_version: request.received_master_state_version,
        phase: match request.phase {
            ConfigurationFollowerEnrollmentRequestPhase::Pending => ProtocolRequestPhase::Pending,
            ConfigurationFollowerEnrollmentRequestPhase::Approved => ProtocolRequestPhase::Approved,
            ConfigurationFollowerEnrollmentRequestPhase::Rejected => ProtocolRequestPhase::Rejected,
        },
        grant: request.grant.as_ref().map(grant_summary_response),
    }
}

fn grant_response(grant: &ConfigurationReadGrant) -> ConfigurationReadGrantResponse {
    let grant = ConfigurationReadGrantSummary::from(grant);
    grant_summary_response(&grant)
}

fn grant_summary_response(grant: &ConfigurationReadGrantSummary) -> ConfigurationReadGrantResponse {
    ConfigurationReadGrantResponse {
        grant_id: grant.grant_id.as_str().to_owned(),
        issuance_attempt_id: grant
            .issuance_attempt_id
            .as_ref()
            .map(|attempt| attempt.as_str().to_owned()),
        group_id: grant.authority.group_id().as_str().to_owned(),
        master_instance_id: grant.authority.master_id().as_str().to_owned(),
        follower_instance_id: grant.follower_id.as_str().to_owned(),
        issued_state_version: grant.issued_state_version,
        revoked: grant.revoked,
    }
}
