//! Opt-in TLS serving for the restricted follower router. No socket binding,
//! certificate discovery or daemon activation occurs in this module.

use super::ConfigurationFollowerRouter;
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use std::{future::Future, num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{net::TcpListener, task::JoinSet, time::timeout};
use tokio_rustls::{TlsAcceptor, rustls};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConfigurationTlsError {
    #[error("configuration TLS identity is invalid")]
    InvalidIdentity,
    #[error("configuration TLS resource limits are invalid")]
    InvalidLimits,
    #[error("configuration TLS listener failed")]
    Listener,
    #[error("configuration TLS connection task failed")]
    ConnectionTask,
}

/// Caller-selected capacity and network budgets. These do not bound CPU time or
/// total process memory; validated snapshot/encoded-response limits also apply.
#[derive(Clone, Copy)]
pub struct ConfigurationTlsLimits {
    /// Accepted sockets, including TLS handshakes and responses, share this cap.
    pub max_connections: NonZeroUsize,
    /// Hyper HTTP/1's connection-buffer budget; its documented minimum is 8192.
    pub max_http_buffer_bytes: usize,
    pub handshake_timeout: Duration,
    /// Covers the single HTTP request from headers through response completion.
    pub request_timeout: Duration,
    /// Allow existing connections to finish, then cancel remaining local work.
    pub shutdown_timeout: Duration,
}

/// Immutable certificate/key configuration, deliberately not Debug or Clone.
/// Provisioning and private key-file ownership belong to local administration.
pub struct ConfigurationFollowerTls(TlsAcceptor);

impl ConfigurationFollowerTls {
    /// Accepts an explicit DER chain (leaf first) and DER private key. Rustls
    /// validates that the key is usable and matches the leaf certificate.
    /// Reads no files and creates no listeners. Enrollment must separately bind
    /// the certificate authority and hostname to the Kiln master/group identity.
    pub fn from_der(
        certificate_chain: Vec<Vec<u8>>,
        private_key: Vec<u8>,
    ) -> Result<Self, ConfigurationTlsError> {
        let certificates = certificate_chain
            .into_iter()
            .map(rustls::pki_types::CertificateDer::from)
            .collect();
        let key = rustls::pki_types::PrivateKeyDer::try_from(private_key)
            .map_err(|_| ConfigurationTlsError::InvalidIdentity)?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .map_err(|_| ConfigurationTlsError::InvalidIdentity)?
            // Follower identity is authenticated by its restricted read bearer,
            // not by a client certificate. The master proves TLS key possession.
            .with_no_client_auth()
            .with_single_cert(certificates, key)
            .map_err(|_| ConfigurationTlsError::InvalidIdentity)?;
        Ok(Self::from_config(config))
    }

    /// Borrow private DER so the caller need not copy its zeroizing buffer.
    /// The caller retains its key owner through this call. Ring owns
    /// the parsed signing key afterward; no borrowed bytes escape into TLS.
    /// As with `from_der`, current authority, lifetime and chain trust are the
    /// composition owner's responsibility. This checks leaf/key consistency.
    pub fn from_borrowed_der(
        certificate_chain: Vec<Vec<u8>>,
        private_key: &[u8],
    ) -> Result<Self, ConfigurationTlsError> {
        let key = rustls::pki_types::PrivateKeyDer::try_from(private_key)
            .map_err(|_| ConfigurationTlsError::InvalidIdentity)?;
        let signing_key = rustls::crypto::ring::sign::any_supported_type(&key)
            .map_err(|_| ConfigurationTlsError::InvalidIdentity)?;
        let certificates = certificate_chain
            .into_iter()
            .map(rustls::pki_types::CertificateDer::from)
            .collect();
        let certified_key = rustls::sign::CertifiedKey::new(certificates, signing_key);
        certified_key
            .keys_match()
            .map_err(|_| ConfigurationTlsError::InvalidIdentity)?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .map_err(|_| ConfigurationTlsError::InvalidIdentity)?
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(rustls::sign::SingleCertAndKey::from(
                certified_key,
            )));
        Ok(Self::from_config(config))
    }

    fn from_config(mut config: rustls::ServerConfig) -> Self {
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        config.send_tls13_tickets = 0;
        // Fresh config retains rustls defaults: no key log and no early data.
        Self(TlsAcceptor::from(Arc::new(config)))
    }
}

/// Serve only a constructed follower router over TLS. The daemon does not call
/// this function until explicit enrollment/certificate composition is available.
/// One HTTP/1 request per connection avoids unbounded keep-alive or HTTP/2 streams.
/// Dropping the serving future aborts its owned connection tasks.
pub async fn serve_configuration_followers<F>(
    listener: TcpListener,
    router: ConfigurationFollowerRouter,
    tls: ConfigurationFollowerTls,
    limits: ConfigurationTlsLimits,
    shutdown: F,
) -> Result<(), ConfigurationTlsError>
where
    F: Future<Output = ()> + Send,
{
    if limits.max_http_buffer_bytes < 8192
        || [
            limits.handshake_timeout,
            limits.request_timeout,
            limits.shutdown_timeout,
        ]
        .into_iter()
        .any(|duration| {
            duration.is_zero() || std::time::Instant::now().checked_add(duration).is_none()
        })
    {
        return Err(ConfigurationTlsError::InvalidLimits);
    }
    let mut connections = JoinSet::new();
    let mut shutdown = std::pin::pin!(shutdown);
    let outcome = loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break Ok(()),
            completed = connections.join_next(), if !connections.is_empty() => {
                if completed.is_some_and(|result| result.is_err()) {
                    break Err(ConfigurationTlsError::ConnectionTask);
                }
            }
            accepted = listener.accept(), if connections.len() < limits.max_connections.get() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(_) => break Err(ConfigurationTlsError::Listener),
                };
                let acceptor = tls.0.clone();
                let service = TowerToHyperService::new(router.0.clone());
                connections.spawn(async move {
                    let Ok(Ok(stream)) = timeout(limits.handshake_timeout, acceptor.accept(stream)).await else {
                        return;
                    };
                    let mut http = hyper::server::conn::http1::Builder::new();
                    http.keep_alive(false)
                        // The enclosing deadline covers headers and the entire
                        // response, replacing Hyper's independent header timer.
                        .header_read_timeout(None)
                        .max_buf_size(limits.max_http_buffer_bytes);
                    let connection = http.serve_connection(TokioIo::new(stream), service);
                    // Peer disconnects, malformed HTTP and deadline expiry close
                    // this socket only. Remote diagnostics are never exposed.
                    let _ = timeout(limits.request_timeout, connection).await;
                });
            }
        }
    };
    // Stop admission before draining any existing TLS/HTTP work.
    drop(listener);
    let mut task_failed = false;
    if timeout(limits.shutdown_timeout, async {
        while let Some(result) = connections.join_next().await {
            task_failed |= result.is_err();
        }
    })
    .await
    .is_err()
    {
        connections.abort_all();
        while let Some(result) = connections.join_next().await {
            task_failed |= result.is_err_and(|error| !error.is_cancelled());
        }
    }
    if task_failed {
        return Err(ConfigurationTlsError::ConnectionTask);
    }
    outcome
}
