use super::{AppState, PublicError, RunOperations, SessionOperations, WorkspaceOperations};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::header,
    response::IntoResponse,
};
use kiln_core::{
    ConfigurationAccessError, ConfigurationAccessStore, ConfigurationReadGrant,
    ConfigurationReadGrantAttemptId, ConfigurationReadGrantId, ConfigurationReadGrantSummary,
    ConfigurationStateError, ConfigurationStateStore, KilnInstanceId,
};
use kiln_protocol::{
    ConfigurationReadGrantListResponse, ConfigurationReadGrantResponse,
    RevokeConfigurationReadGrantRequest,
};
use serde::Deserialize;
use std::{future::Future, pin::Pin};

pub(super) trait ConfigurationReadGrantOperations: Send + Sync {
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
}

pub(super) struct ConfigurationReadGrantAdapter<T>(pub T);

impl<T> ConfigurationReadGrantOperations for ConfigurationReadGrantAdapter<T>
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
        .configuration_read_grant_operations
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
        .configuration_read_grant_operations
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
        .configuration_read_grant_operations
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
        .configuration_read_grant_operations
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

fn grant_response(grant: &ConfigurationReadGrant) -> ConfigurationReadGrantResponse {
    let grant = ConfigurationReadGrantSummary::from(grant);
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
