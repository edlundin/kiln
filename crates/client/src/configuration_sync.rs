//! Restricted HTTPS transport for an explicitly pinned configuration follower.
//! No daemon listener or automatic enrollment orchestration is enabled here.

use kiln_protocol::{
    CONFIGURATION_FOLLOWER_ENROLLMENT_RECEIPT_MAX_BYTES,
    CONFIGURATION_FOLLOWER_ENROLLMENT_REQUESTS_PATH, CONFIGURATION_PUBLICATION_MAX_BYTES,
    CONFIGURATION_SNAPSHOT_PATH, ConfigurationFollowerEnrollmentRequestPhase,
    ConfigurationFollowerEnrollmentRequestResponse, ConfigurationSnapshotResponse,
    SubmitConfigurationFollowerEnrollmentRequest,
};
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderValue};
use sha2::{Digest as _, Sha256};
use std::time::Duration;

/// Administrative enrollment must bind all these values together before any
/// credential is sent. A downloaded snapshot cannot supply or replace the pin.
pub struct ConfigurationMasterPin {
    /// HTTPS origin only: no credentials, path prefix, query or fragment.
    pub origin: String,
    /// Canonical DNS/IP name in the approved master identity and TLS SAN.
    pub server_name: String,
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
    #[error("configuration follower enrollment request is invalid")]
    InvalidEnrollmentRequest,
    #[error("configuration synchronization deadlines are invalid")]
    InvalidTimeouts,
    #[error("configuration synchronization transport failed")]
    Transport,
    #[error("configuration synchronization response exceeds the transfer budget")]
    TooLarge,
    #[error("configuration master returned HTTP {status}")]
    HttpStatus { status: u16 },
    #[error("configuration master returned an invalid response")]
    InvalidResponse,
    #[error("configuration snapshot does not match the enrolled master and group")]
    AuthorityMismatch,
}

/// Can only fetch snapshots and submit the exact credential digest for the
/// pinned follower enrollment attempt. The bearer is sent only on snapshot GET,
/// never on intake POST. It cannot call the general local API, change authority,
/// publish, or discover credentials. Deliberately not Debug or Clone; each
/// instance keeps one immutable peer/credential binding.
pub struct ConfigurationSyncClient {
    http: reqwest::Client,
    snapshot_url: reqwest::Url,
    enrollment_request_url: reqwest::Url,
    authorization: HeaderValue,
    master_instance_id: String,
    group_id: String,
    follower_instance_id: String,
    server_name: String,
    master_ca_fingerprint: String,
    credential_digest: String,
}

impl ConfigurationSyncClient {
    /// Validates the caller-selected HTTPS origin before daemon startup without
    /// resolving DNS or changing the enrollment's separately stored pin.
    pub fn validate_origin_syntax(origin: &str) -> Result<(), ConfigurationSyncError> {
        parse_origin(origin).map(|_| ())
    }

    /// Checks the caller-selected origin against the enrolled canonical TLS
    /// name without resolving DNS or changing any enrollment data.
    pub fn validate_origin_for_server_name(
        origin: &str,
        server_name: &str,
    ) -> Result<(), ConfigurationSyncError> {
        parse_origin_for_server_name(origin, server_name).map(|_| ())
    }

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
        {
            return Err(Error::InvalidPin);
        }
        let base_url = parse_origin_for_server_name(&pin.origin, &pin.server_name)?;
        let mut snapshot_url = base_url.clone();
        snapshot_url.set_path(CONFIGURATION_SNAPSHOT_PATH);
        let mut enrollment_request_url = base_url;
        enrollment_request_url.set_path(CONFIGURATION_FOLLOWER_ENROLLMENT_REQUESTS_PATH);
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
        let credential_digest = lower_hex(&Sha256::digest(credential));
        let master_ca_fingerprint = lower_hex(&Sha256::digest(&pin.certificate_authority_der));
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
            .build()
            .map_err(|_| Error::InvalidPin)?;
        Ok(Self {
            http,
            snapshot_url,
            enrollment_request_url,
            authorization,
            master_instance_id: pin.master_instance_id,
            group_id: pin.group_id,
            follower_instance_id: pin.follower_instance_id,
            server_name: pin.server_name,
            master_ca_fingerprint,
            credential_digest,
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
            .header(ACCEPT, "application/json")
            .header(AUTHORIZATION, self.authorization.clone())
            .header("kiln-configuration-master", &self.master_instance_id)
            .header("kiln-configuration-group", &self.group_id)
            .header("kiln-configuration-follower", &self.follower_instance_id)
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

    /// Submit the immutable digest-only enrollment claim through the pinned
    /// HTTPS channel. The bearer is never attached to this POST. Repeating the
    /// same attempt ID and local state version recovers the current master
    /// receipt; the server validates the exact pin before returning it.
    pub async fn submit_enrollment_request(
        &self,
        attempt_id: &str,
        follower_state_version: u64,
    ) -> Result<ConfigurationFollowerEnrollmentRequestResponse, ConfigurationSyncError> {
        use ConfigurationSyncError as Error;
        if !valid_attempt_id(attempt_id)
            || follower_state_version == 0
            || follower_state_version > i64::MAX as u64
        {
            return Err(Error::InvalidEnrollmentRequest);
        }
        let request = SubmitConfigurationFollowerEnrollmentRequest {
            attempt_id: attempt_id.to_owned(),
            follower_id: self.follower_instance_id.clone(),
            follower_state_version,
            group_id: self.group_id.clone(),
            master_instance_id: self.master_instance_id.clone(),
            server_name: self.server_name.clone(),
            master_ca_fingerprint: self.master_ca_fingerprint.clone(),
            credential_digest: self.credential_digest.clone(),
        };
        let mut response = self
            .http
            .post(self.enrollment_request_url.clone())
            .header(ACCEPT, "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|_| Error::Transport)?;
        let status = response.status();
        let cap = CONFIGURATION_FOLLOWER_ENROLLMENT_RECEIPT_MAX_BYTES;
        if response
            .content_length()
            .is_some_and(|length| length > cap as u64)
        {
            return Err(Error::TooLarge);
        }
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
        let receipt: ConfigurationFollowerEnrollmentRequestResponse =
            serde_json::from_slice(&bytes).map_err(|_| Error::InvalidResponse)?;
        if !valid_enrollment_receipt(&receipt, &request, &self.master_ca_fingerprint) {
            return Err(Error::InvalidResponse);
        }
        Ok(receipt)
    }
}

fn parse_origin(origin: &str) -> Result<reqwest::Url, ConfigurationSyncError> {
    use ConfigurationSyncError as Error;
    if origin
        .bytes()
        .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return Err(Error::InvalidPin);
    }
    let url = reqwest::Url::parse(origin).map_err(|_| Error::InvalidPin)?;
    if url.scheme() != "https"
        || url.host().is_none()
        || url.port_or_known_default() == Some(0)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::InvalidPin);
    }
    Ok(url)
}

fn parse_origin_for_server_name(
    origin: &str,
    server_name: &str,
) -> Result<reqwest::Url, ConfigurationSyncError> {
    let url = parse_origin(origin)?;
    if !valid_server_name(server_name) || !url_host_matches(&url, server_name) {
        return Err(ConfigurationSyncError::InvalidPin);
    }
    Ok(url)
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

fn valid_attempt_id(value: &str) -> bool {
    value.strip_prefix("cra_").is_some_and(|suffix| {
        suffix.len() == 32
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn valid_server_name(value: &str) -> bool {
    use std::net::IpAddr;
    if let Ok(address) = value.parse::<IpAddr>() {
        return address.to_string() == value;
    }
    !value.is_empty()
        && value.len() <= 253
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
}

fn url_host_matches(url: &reqwest::Url, server_name: &str) -> bool {
    url.host_str().is_some_and(|host| {
        let host = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host);
        host.eq_ignore_ascii_case(server_name)
    })
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(HEX[(byte >> 4) as usize] as char);
        value.push(HEX[(byte & 0x0f) as usize] as char);
    }
    value
}

fn valid_enrollment_receipt(
    receipt: &ConfigurationFollowerEnrollmentRequestResponse,
    request: &SubmitConfigurationFollowerEnrollmentRequest,
    master_ca_fingerprint: &str,
) -> bool {
    let valid_confirmation_fingerprint = |value: &str| {
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    };
    if !valid_attempt_id(&receipt.attempt_id)
        || !receipt.request_id.starts_with("cfr_")
        || receipt.request_id.len() != 36
        || !receipt.request_id[4..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || receipt.attempt_id != request.attempt_id
        || receipt.follower_id != request.follower_id
        || receipt.follower_state_version != request.follower_state_version
        || receipt.group_id != request.group_id
        || receipt.master_instance_id != request.master_instance_id
        || receipt.server_name != request.server_name
        || receipt.master_ca_fingerprint != master_ca_fingerprint
        || !valid_confirmation_fingerprint(&receipt.credential_fingerprint)
        || receipt.received_master_state_version == 0
        || receipt.received_master_state_version > i64::MAX as u64
    {
        return false;
    }
    if receipt.credential_fingerprint != enrollment_confirmation_fingerprint(receipt, request) {
        return false;
    }
    match (receipt.phase, receipt.grant.as_ref()) {
        (ConfigurationFollowerEnrollmentRequestPhase::Approved, Some(grant)) => {
            grant.grant_id.starts_with("crg_")
                && grant.grant_id.len() == 36
                && grant.grant_id[4..]
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                && grant.issuance_attempt_id.as_deref() == Some(request.attempt_id.as_str())
                && grant.group_id == request.group_id
                && grant.master_instance_id == request.master_instance_id
                && grant.follower_instance_id == request.follower_id
                && grant.issued_state_version > 0
                && grant.issued_state_version <= i64::MAX as u64
        }
        (
            ConfigurationFollowerEnrollmentRequestPhase::Pending
            | ConfigurationFollowerEnrollmentRequestPhase::Rejected,
            None,
        ) => true,
        _ => false,
    }
}

fn enrollment_confirmation_fingerprint(
    receipt: &ConfigurationFollowerEnrollmentRequestResponse,
    request: &SubmitConfigurationFollowerEnrollmentRequest,
) -> String {
    // Keep the framing aligned with infrastructure's durable confirmation hash.
    let mut hash = Sha256::new();
    hash.update(b"kiln configuration follower enrollment confirmation v1\0");
    for value in [
        receipt.request_id.as_str(),
        request.attempt_id.as_str(),
        request.follower_id.as_str(),
        request.group_id.as_str(),
        request.master_instance_id.as_str(),
        request.server_name.as_str(),
        request.master_ca_fingerprint.as_str(),
        request.credential_digest.as_str(),
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hash.update(request.follower_state_version.to_be_bytes());
    hash.update(receipt.received_master_state_version.to_be_bytes());
    lower_hex(&hash.finalize())
}
