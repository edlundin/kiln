//! Restricted HTTPS transport for an explicitly enrolled configuration follower.
//! No daemon listener or enrollment operation is enabled by this client.

use kiln_protocol::{
    CONFIGURATION_PUBLICATION_MAX_BYTES, CONFIGURATION_SNAPSHOT_PATH, ConfigurationSnapshotResponse,
};
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue};
use std::time::Duration;

/// Administrative enrollment must bind all these values together before any
/// credential is sent. A downloaded snapshot cannot supply or replace the pin.
pub struct ConfigurationMasterPin {
    /// HTTPS origin only: no credentials, path prefix, query or fragment.
    pub origin: String,
    /// One DER-encoded trust anchor dedicated to this master's TLS identity.
    /// The hostname and chain are still verified; system roots are excluded.
    pub certificate_authority_der: Vec<u8>,
    pub master_instance_id: String,
    pub group_id: String,
    pub follower_instance_id: String,
}

/// Deployment-specific budgets; no latency assumption is embedded in the client.
pub struct ConfigurationSyncTimeouts {
    pub connect: Duration,
    /// Covers connection, response headers and the complete body.
    pub request: Duration,
}

/// Content-free failures: no remote diagnostic, URL or credential is retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConfigurationSyncError {
    #[error("configuration master pin is invalid")]
    InvalidPin,
    #[error("configuration synchronization credential is invalid")]
    InvalidCredential,
    #[error("configuration synchronization deadlines are invalid")]
    InvalidTimeouts,
    #[error("configuration synchronization transport failed")]
    Transport,
    #[error("configuration snapshot exceeds the transfer budget")]
    TooLarge,
    #[error("configuration master returned HTTP {status}")]
    HttpStatus { status: u16 },
    #[error("configuration master returned an invalid response")]
    InvalidResponse,
    #[error("configuration snapshot does not match the enrolled master and group")]
    AuthorityMismatch,
}

/// Can only fetch configuration snapshots. It cannot call the general local API,
/// change authority, publish, or discover credentials. Deliberately not Debug or
/// Clone; each instance keeps one immutable peer/credential binding.
pub struct ConfigurationSyncClient {
    http: reqwest::Client,
    snapshot_url: reqwest::Url,
    master_instance_id: String,
    group_id: String,
}

impl ConfigurationSyncClient {
    /// Constructs transport only; performs no DNS, network or filesystem work.
    /// The caller must obtain the pin and follower-specific bearer through an
    /// authenticated enrollment flow. Supplying arbitrary public CA roots here
    /// would broaden the enrolled identity and is not an enrollment mechanism.
    pub fn new(
        pin: ConfigurationMasterPin,
        credential: &[u8],
        timeouts: ConfigurationSyncTimeouts,
    ) -> Result<Self, ConfigurationSyncError> {
        use ConfigurationSyncError as Error;
        if !valid_id(&pin.master_instance_id, "ins_")
            || !valid_id(&pin.follower_instance_id, "ins_")
            || !valid_id(&pin.group_id, "cfg_")
            || pin.master_instance_id == pin.follower_instance_id
            || pin
                .origin
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            return Err(Error::InvalidPin);
        }
        let mut snapshot_url = reqwest::Url::parse(&pin.origin).map_err(|_| Error::InvalidPin)?;
        if snapshot_url.scheme() != "https"
            || snapshot_url.host().is_none()
            || snapshot_url.port_or_known_default() == Some(0)
            || !snapshot_url.username().is_empty()
            || snapshot_url.password().is_some()
            || snapshot_url.path() != "/"
            || snapshot_url.query().is_some()
            || snapshot_url.fragment().is_some()
        {
            return Err(Error::InvalidPin);
        }
        snapshot_url.set_path(CONFIGURATION_SNAPSHOT_PATH);
        // Independent of the unrestricted local bearer format. Keep this wire
        // grammar aligned with infrastructure::ConfigurationReadCredential.
        if credential.len() != 86
            || !credential.starts_with(b"kcfg1_")
            || !credential[6..]
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
        {
            return Err(Error::InvalidCredential);
        }
        if timeouts.connect.is_zero()
            || timeouts.request.is_zero()
            || timeouts.connect > timeouts.request
            || std::time::Instant::now()
                .checked_add(timeouts.request)
                .is_none()
        {
            return Err(Error::InvalidTimeouts);
        }
        let certificate = reqwest::Certificate::from_der(&pin.certificate_authority_der)
            .map_err(|_| Error::InvalidPin)?;
        let mut bearer = b"Bearer ".to_vec();
        bearer.extend_from_slice(credential);
        let mut authorization =
            HeaderValue::from_bytes(&bearer).map_err(|_| Error::InvalidCredential)?;
        authorization.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, authorization);
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        for (name, value) in [
            ("kiln-configuration-master", &pin.master_instance_id),
            ("kiln-configuration-group", &pin.group_id),
            ("kiln-configuration-follower", &pin.follower_instance_id),
        ] {
            headers.insert(
                name,
                HeaderValue::from_str(value).map_err(|_| Error::InvalidPin)?,
            );
        }
        let http = reqwest::Client::builder()
            .tls_backend_rustls()
            .tls_certs_only([certificate])
            .min_tls_version(reqwest::tls::Version::TLS_1_2)
            .tls_sslkeylogfile(false)
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(timeouts.connect)
            .timeout(timeouts.request)
            .default_headers(headers)
            .build()
            .map_err(|_| Error::InvalidPin)?;
        Ok(Self {
            http,
            snapshot_url,
            master_instance_id: pin.master_instance_id,
            group_id: pin.group_id,
        })
    }

    /// Returns a candidate from the pinned HTTPS authority, never active data.
    /// The caller must validate canonical content, hashes, schema and monotonic
    /// revision rules and fence enrollment changes before atomic application.
    /// Dropping this future abandons the local request; there is no automatic retry.
    pub async fn get_snapshot(
        &self,
    ) -> Result<ConfigurationSnapshotResponse, ConfigurationSyncError> {
        use ConfigurationSyncError as Error;
        let mut response = self
            .http
            .get(self.snapshot_url.clone())
            .send()
            .await
            .map_err(|_| Error::Transport)?;
        let status = response.status();
        let cap = CONFIGURATION_PUBLICATION_MAX_BYTES;
        if response
            .content_length()
            .is_some_and(|length| length > cap as u64)
        {
            return Err(Error::TooLarge);
        }
        // Reject errors without buffering or exposing server-controlled bodies.
        if status != reqwest::StatusCode::OK {
            return Err(Error::HttpStatus {
                status: status.as_u16(),
            });
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| Error::Transport)? {
            if chunk.len() > cap.saturating_sub(bytes.len()) {
                return Err(Error::TooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        let snapshot: ConfigurationSnapshotResponse =
            serde_json::from_slice(&bytes).map_err(|_| Error::InvalidResponse)?;
        if snapshot.instance_id != self.master_instance_id
            || snapshot.master_instance_id != self.master_instance_id
            || snapshot.group_id != self.group_id
        {
            return Err(Error::AuthorityMismatch);
        }
        if snapshot.state_version == 0
            || snapshot.state_version > i64::MAX as u64
            || snapshot.revision.revision == 0
            || snapshot.revision.revision > i64::MAX as u64
            || snapshot.revision.schema_version == 0
            || snapshot.revision.content_hash.len() != 64
            || !snapshot
                .revision
                .content_hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(Error::InvalidResponse);
        }
        Ok(snapshot)
    }
}

// Canonical ULID grammar, matching core IDs without importing domain types into
// the protocol client. The leading digit bounds the value to 128 bits.
fn valid_id(value: &str, prefix: &str) -> bool {
    value.strip_prefix(prefix).is_some_and(|suffix| {
        suffix.len() == 26
            && matches!(suffix.as_bytes()[0], b'0'..=b'7')
            && suffix
                .bytes()
                .all(|byte| b"0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(&byte))
    })
}
