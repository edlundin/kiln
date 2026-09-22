//! Isolated follower read service. Daemon startup does not mount this router.

use axum::{
    Router,
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header, uri::Authority},
    middleware,
    response::{IntoResponse, Response},
    routing::get,
};
use kiln_core::{
    ConfigurationAccessError, ConfigurationAccessStore, ConfigurationAuthority,
    ConfigurationCredentialDigest, ConfigurationGroupId, ConfigurationSnapshotError,
    KilnInstanceId,
};
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
#[error("configuration follower service requires an explicit valid Host authority")]
pub struct InvalidConfigurationFollowerHost;

struct FollowerState<T, F> {
    store: T,
    credential_digest: F,
    expected_host: HeaderValue,
}

/// Build a separate read-only service, never merge it into the local admin router.
/// The owning daemon must serve it only over authenticated HTTPS for the enrolled
/// master certificate, with connection/request limits and shutdown management.
/// This function binds no socket and provisions no certificate or enrollment.
///
/// `credential_digest` is the trusted adapter's strict parser and one-way hasher
/// for ConfigurationReadCredential. It must never accept a digest as a bearer or
/// derive from the unrestricted local API token. No credential is stored here.
pub fn configuration_follower_router<T, F>(
    store: T,
    credential_digest: F,
    expected_host: &str,
) -> Result<Router, InvalidConfigurationFollowerHost>
where
    T: ConfigurationAccessStore + 'static,
    F: Fn(&[u8]) -> Option<ConfigurationCredentialDigest> + Send + Sync + 'static,
{
    let authority: Authority = expected_host
        .parse()
        .map_err(|_| InvalidConfigurationFollowerHost)?;
    if authority.host().is_empty()
        || expected_host.contains('@')
        || (authority.as_str() != authority.host()
            && authority.port_u16().is_none_or(|port| port == 0))
    {
        return Err(InvalidConfigurationFollowerHost);
    }
    let expected_host =
        HeaderValue::from_str(expected_host).map_err(|_| InvalidConfigurationFollowerHost)?;
    let state = Arc::new(FollowerState {
        store,
        credential_digest,
        expected_host,
    });
    Ok(Router::new()
        .route(
            kiln_protocol::CONFIGURATION_SNAPSHOT_PATH,
            get(snapshot::<T, F>),
        )
        .fallback(|| async { StatusCode::NOT_FOUND })
        .layer(middleware::map_response(no_store))
        .with_state(state))
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
    if single_header(headers, header::HOST.as_str()) != Some(&state.expected_host)
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
    let stored = state
        .store
        .read_configuration_for_follower(
            &authority,
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

fn identity_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, StatusCode> {
    single_header(headers, name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.len() == 30)
        .ok_or(StatusCode::UNAUTHORIZED)
}

async fn no_store(mut response: Response) -> Response {
    if response.status() == StatusCode::UNAUTHORIZED {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer realm=\"kiln-configuration\""),
        );
    }
    if response.status() == StatusCode::METHOD_NOT_ALLOWED {
        response
            .headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("GET"));
    }
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
