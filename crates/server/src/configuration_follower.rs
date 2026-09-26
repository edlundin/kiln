//! Isolated follower read service. Daemon startup does not mount this router.

use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header, uri::Authority},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use kiln_core::{
    ConfigurationAccessError, ConfigurationAccessStore, ConfigurationAuthority,
    ConfigurationCredentialDigest, ConfigurationFollowerEnrollmentRequestId,
    ConfigurationFollowerEnrollmentRequestSubmission, ConfigurationFollowerServingIdentity,
    ConfigurationGroupId, ConfigurationReadGrantAttemptId, ConfigurationSnapshotError, ContentHash,
    KilnInstanceId,
};
use kiln_protocol::{
    CONFIGURATION_FOLLOWER_ENROLLMENT_REQUESTS_PATH,
    CONFIGURATION_FOLLOWER_ENROLLMENT_SUBMISSION_MAX_BYTES,
    SubmitConfigurationFollowerEnrollmentRequest,
};
use std::sync::Arc;
use std::{net::IpAddr, num::NonZeroU32};

use super::LifecycleCoordinator;

#[derive(Debug, thiserror::Error)]
#[error("configuration follower service requires an explicit valid Host authority")]
pub struct InvalidConfigurationFollowerHost;

/// Only the restricted router can enter the follower TLS serving boundary.
/// There is intentionally no conversion from an arbitrary administrative router.
pub struct ConfigurationFollowerRouter(pub(super) Router);

struct FollowerState<T, F> {
    store: Arc<T>,
    credential_digest: F,
    expected_host: HeaderValue,
    serving_identity: ConfigurationFollowerServingIdentity,
    max_retained_requests_per_authority: NonZeroU32,
    lifecycle: LifecycleCoordinator,
}

/// Build a separate read-only service, never merge it into the local admin router.
/// The owning daemon must serve it only over authenticated HTTPS for the enrolled
/// master certificate, with connection/request limits and shutdown management.
/// This function binds no socket and provisions no certificate or enrollment.
///
/// `credential_digest` is the trusted adapter's strict parser and one-way hasher
/// for ConfigurationReadCredential. It must never accept a digest as a bearer or
/// derive from the unrestricted local API token. No credential is stored here.
/// The caller also supplies the exact active serving identity and a nonzero cap
/// for all retained request rows under its authority.
pub fn configuration_follower_router<T, F>(
    store: T,
    credential_digest: F,
    expected_host: &str,
    serving_identity: ConfigurationFollowerServingIdentity,
    max_retained_requests_per_authority: NonZeroU32,
    lifecycle: LifecycleCoordinator,
) -> Result<ConfigurationFollowerRouter, InvalidConfigurationFollowerHost>
where
    T: ConfigurationAccessStore + 'static,
    F: Fn(&[u8]) -> Option<ConfigurationCredentialDigest> + Send + Sync + 'static,
{
    let expected_host =
        configuration_follower_host_authority(expected_host, &serving_identity.server_name)?;
    let expected_host =
        HeaderValue::from_str(&expected_host).map_err(|_| InvalidConfigurationFollowerHost)?;
    let state = Arc::new(FollowerState {
        store: Arc::new(store),
        credential_digest,
        expected_host,
        serving_identity,
        max_retained_requests_per_authority,
        lifecycle,
    });
    Ok(ConfigurationFollowerRouter(
        Router::new()
            .route(
                kiln_protocol::CONFIGURATION_SNAPSHOT_PATH,
                get(snapshot::<T, F>),
            )
            .route(
                CONFIGURATION_FOLLOWER_ENROLLMENT_REQUESTS_PATH,
                post(submit_enrollment_request::<T, F>),
            )
            .fallback(|| async { StatusCode::NOT_FOUND })
            .layer(middleware::from_fn(no_store))
            .with_state(state),
    ))
}

async fn submit_enrollment_request<T, F>(
    State(state): State<Arc<FollowerState<T, F>>>,
    request: Request,
) -> Response
where
    T: ConfigurationAccessStore + 'static,
    F: Fn(&[u8]) -> Option<ConfigurationCredentialDigest> + Send + Sync + 'static,
{
    use super::PublicError;

    if request.method() != Method::POST {
        return PublicError::MethodNotAllowed.into_response();
    }
    let headers = request.headers();
    if !matches_expected_host(headers, &state)
        || headers.contains_key(header::ORIGIN)
        || headers.contains_key(header::SEC_WEBSOCKET_PROTOCOL)
        || headers.contains_key(header::AUTHORIZATION)
        || request.uri().query().is_some()
    {
        return PublicError::InvalidRequest.into_response();
    }
    if !matches!(
        single_header(headers, header::CONTENT_TYPE.as_str()).and_then(|value| value.to_str().ok()),
        Some("application/json")
    ) {
        return PublicError::InvalidJson.into_response();
    }
    if headers.get_all(header::CONTENT_LENGTH).iter().count() > 1 {
        return PublicError::InvalidJson.into_response();
    }
    if let Some(length) = single_header(headers, header::CONTENT_LENGTH.as_str()) {
        if length
            .to_str()
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .is_none_or(|length| length > CONFIGURATION_FOLLOWER_ENROLLMENT_SUBMISSION_MAX_BYTES)
        {
            return PublicError::InvalidJson.into_response();
        }
    }

    let bytes = match to_bytes(
        request.into_body(),
        CONFIGURATION_FOLLOWER_ENROLLMENT_SUBMISSION_MAX_BYTES,
    )
    .await
    {
        Ok(bytes) => bytes,
        Err(_) => return PublicError::InvalidJson.into_response(),
    };
    let submission: SubmitConfigurationFollowerEnrollmentRequest =
        match serde_json::from_slice(&bytes) {
            Ok(submission) => submission,
            Err(error) if error.is_syntax() || error.is_eof() => {
                return PublicError::InvalidJson.into_response();
            }
            Err(_) => return PublicError::InvalidRequest.into_response(),
        };

    let submission = match parse_submission(submission) {
        Ok(submission) => submission,
        Err(response) => return response,
    };
    let attempt_id = submission.attempt_id.clone();
    let request_id = ConfigurationFollowerEnrollmentRequestId::from_ulid(ulid::Ulid::generate());
    let command = match state.lifecycle.begin_command() {
        Ok(command) => command,
        Err(error) => return error.into_response(),
    };
    let store = Arc::clone(&state.store);
    let serving_identity = state.serving_identity.clone();
    let max_retained_requests_per_authority = state.max_retained_requests_per_authority;
    let receipt = match tokio::spawn(async move {
        // Own the shared daemon command permit until the accepted SQLite
        // transaction finishes, even if this HTTP connection is aborted.
        let _command = command;
        store
            .submit_configuration_follower_enrollment_request(
                &serving_identity,
                max_retained_requests_per_authority,
                &submission,
                &request_id,
            )
            .await
    })
    .await
    {
        Ok(Ok(receipt)) => receipt,
        Ok(Err(error)) => {
            return PublicError::ConfigurationFollowerEnrollmentRequest(error).into_response();
        }
        Err(_) => return PublicError::ConfigurationSyncUnavailable.into_response(),
    };
    if receipt.attempt_id == attempt_id {
        axum::Json(super::configuration_access::enrollment_request_response(
            &receipt,
        ))
        .into_response()
    } else {
        PublicError::ConfigurationFollowerEnrollmentRequest(
            ConfigurationAccessError::IntegrityViolation,
        )
        .into_response()
    }
}

/// Validate and canonicalize an externally configured HTTPS authority against
/// the active certificate name. An omitted port means HTTPS port 443.
pub fn configuration_follower_host_authority(
    expected_host: &str,
    server_name: &str,
) -> Result<String, InvalidConfigurationFollowerHost> {
    let authority: Authority = expected_host
        .parse()
        .map_err(|_| InvalidConfigurationFollowerHost)?;
    let raw_host = authority
        .host()
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or_else(|| authority.host());
    if raw_host.is_empty()
        || expected_host.contains('@')
        || (authority.as_str() != authority.host()
            && authority.port_u16().is_none_or(|port| port == 0))
    {
        return Err(InvalidConfigurationFollowerHost);
    }
    let host = match raw_host.parse::<IpAddr>() {
        Ok(address) => address.to_string(),
        Err(_) => raw_host.to_ascii_lowercase(),
    };
    if host != server_name {
        return Err(InvalidConfigurationFollowerHost);
    }
    let host = if host.contains(':') {
        format!("[{host}]")
    } else {
        host
    };
    let port = authority.port_u16().unwrap_or(443);
    if port == 0 {
        return Err(InvalidConfigurationFollowerHost);
    }
    Ok(format!("{host}:{port}"))
}

fn parse_submission(
    request: SubmitConfigurationFollowerEnrollmentRequest,
) -> Result<ConfigurationFollowerEnrollmentRequestSubmission, Response> {
    use super::PublicError;

    let attempt_id = ConfigurationReadGrantAttemptId::parse(request.attempt_id)
        .map_err(|_| PublicError::InvalidRequest.into_response())?;
    let follower_id = KilnInstanceId::parse(request.follower_id)
        .map_err(|_| PublicError::InvalidRequest.into_response())?;
    let group_id = ConfigurationGroupId::parse(request.group_id)
        .map_err(|_| PublicError::InvalidRequest.into_response())?;
    let master_id = KilnInstanceId::parse(request.master_instance_id)
        .map_err(|_| PublicError::InvalidRequest.into_response())?;
    let master_ca_fingerprint = ContentHash::parse(request.master_ca_fingerprint)
        .map_err(|_| PublicError::InvalidRequest.into_response())?;
    let credential_digest = ContentHash::parse(request.credential_digest)
        .map_err(|_| PublicError::InvalidRequest.into_response())?;
    if request.follower_state_version == 0 || request.follower_state_version > i64::MAX as u64 {
        return Err(PublicError::InvalidRequest.into_response());
    }
    Ok(ConfigurationFollowerEnrollmentRequestSubmission {
        attempt_id,
        follower_id,
        follower_state_version: request.follower_state_version,
        authority: ConfigurationAuthority::new(group_id, master_id),
        server_name: request.server_name,
        master_ca_fingerprint,
        credential_digest: ConfigurationCredentialDigest::from_sha256(credential_digest),
    })
}

async fn snapshot<T, F>(
    State(state): State<Arc<FollowerState<T, F>>>,
    request: Request,
) -> Result<Response, StatusCode>
where
    T: ConfigurationAccessStore + 'static,
    F: Fn(&[u8]) -> Option<ConfigurationCredentialDigest> + Send + Sync + 'static,
{
    // Axum otherwise allows HEAD through a GET handler. Avoid performing a
    // snapshot acquisition for any method other than the declared read command.
    if request.method() != Method::GET {
        return Err(StatusCode::METHOD_NOT_ALLOWED);
    }
    let headers = request.headers();
    if !matches_expected_host(headers, &state)
        || headers.contains_key(header::ORIGIN)
        || headers.contains_key(header::SEC_WEBSOCKET_PROTOCOL)
    {
        return Err(StatusCode::FORBIDDEN);
    }
    if request.uri().query().is_some() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let authorization =
        single_header(headers, header::AUTHORIZATION.as_str()).ok_or(StatusCode::UNAUTHORIZED)?;
    let credential = authorization
        .as_bytes()
        .strip_prefix(b"Bearer ")
        .ok_or(StatusCode::UNAUTHORIZED)?;
    // Exclude the local API bearer and hash-as-bearer formats before calling the
    // adapter. The adapter performs full syntax validation and SHA-256 hashing.
    if credential.len() != 86 || !credential.starts_with(b"kcfg1_") {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let digest = (state.credential_digest)(credential).ok_or(StatusCode::UNAUTHORIZED)?;
    let master = KilnInstanceId::parse(identity_header(headers, "kiln-configuration-master")?)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let group = ConfigurationGroupId::parse(identity_header(headers, "kiln-configuration-group")?)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let follower = KilnInstanceId::parse(identity_header(headers, "kiln-configuration-follower")?)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    if master == follower {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let authority = ConfigurationAuthority::new(group, master);
    if authority != state.serving_identity.authority {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let stored = state
        .store
        .read_configuration_for_follower(
            &state.serving_identity,
            &follower,
            &digest,
            super::configuration_publication::bundle_limits(),
        )
        .await
        .map_err(|error| match error {
            ConfigurationAccessError::Denied => StatusCode::UNAUTHORIZED,
            ConfigurationAccessError::Snapshot(ConfigurationSnapshotError::LimitExceeded) => {
                StatusCode::PAYLOAD_TOO_LARGE
            }
            _ => StatusCode::SERVICE_UNAVAILABLE,
        })?
        .ok_or(StatusCode::NOT_FOUND)?;
    super::configuration_publication::encode_snapshot(stored)
        .map(IntoResponse::into_response)
        .map_err(|error| match error {
            super::PublicError::ConfigurationSnapshotTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            _ => StatusCode::SERVICE_UNAVAILABLE,
        })
}

fn single_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a HeaderValue> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    Some(value)
}

fn matches_expected_host<T, F>(headers: &HeaderMap, state: &FollowerState<T, F>) -> bool {
    let Some(value) =
        single_header(headers, header::HOST.as_str()).and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    configuration_follower_host_authority(value, &state.serving_identity.server_name)
        .ok()
        .is_some_and(|canonical| canonical.as_bytes() == state.expected_host.as_bytes())
}

fn identity_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, StatusCode> {
    single_header(headers, name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.len() == 30)
        .ok_or(StatusCode::UNAUTHORIZED)
}

async fn no_store(request: Request, next: middleware::Next) -> Response {
    let path = request.uri().path().to_owned();
    let mut response = next.run(request).await;
    if response.status() == StatusCode::UNAUTHORIZED {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer realm=\"kiln-configuration\""),
        );
    }
    if response.status() == StatusCode::METHOD_NOT_ALLOWED {
        let allowed = if path == kiln_protocol::CONFIGURATION_SNAPSHOT_PATH {
            Some("GET")
        } else if path == CONFIGURATION_FOLLOWER_ENROLLMENT_REQUESTS_PATH {
            Some("POST")
        } else {
            None
        };
        if let Some(allowed) = allowed {
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static(allowed));
        }
    }
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
