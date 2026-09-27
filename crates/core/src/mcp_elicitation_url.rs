//! Private URL elicitation data. Validation is never consent or navigation authority.

use std::num::NonZeroUsize;

use crate::McpInvocationError;

#[derive(Debug, Clone, Copy)]
pub struct McpElicitationUrlLimits {
    pub max_message_bytes: NonZeroUsize,
    pub max_url_bytes: NonZeroUsize,
    pub max_legacy_id_bytes: NonZeroUsize,
}

/// Host policy, separate from an untrusted server's request. Plain HTTP is only
/// available for explicitly enabled loopback development, as with MCP transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpElicitationUrlPolicy {
    HttpsOnly,
    AllowLoopbackHttp,
}

/// The modern protocol has no legacy elicitation ID. Do not synthesize one or
/// confuse either identity with the Kiln input ordinal or a JSON-RPC request ID.
#[derive(Clone, PartialEq, Eq)]
pub enum McpElicitationUrlContext {
    Legacy { elicitation_id: String },
    Stateless,
}
impl std::fmt::Debug for McpElicitationUrlContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Legacy { .. } => f.write_str("Legacy { .. }"),
            Self::Stateless => f.write_str("Stateless"),
        }
    }
}

/// Untrusted message and exact server URL, retained only for private consent UI.
/// The URL may violate the server's no-secrets rule; never include it in public
/// Events, logs, model context, previews, or automatic metadata fetches.
#[derive(Clone, PartialEq, Eq)]
pub struct McpElicitationUrl {
    message: String,
    url: String,
    destination: url::Url,
    context: McpElicitationUrlContext,
}

impl std::fmt::Debug for McpElicitationUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpElicitationUrl").finish_non_exhaustive()
    }
}

impl McpElicitationUrl {
    pub fn new(
        message: String,
        url: String,
        context: McpElicitationUrlContext,
        limits: McpElicitationUrlLimits,
        policy: McpElicitationUrlPolicy,
    ) -> Result<Self, McpInvocationError> {
        validate_sizes(&message, &url, &context, limits)?;
        // Browsers normalize these characters when selecting an authority. Do
        // not present an apparently different destination or silently trim it.
        if url
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control() || byte == b'\\')
        {
            return Err(McpInvocationError::InvalidRequest);
        }
        let destination = url::Url::parse(&url).map_err(|_| McpInvocationError::InvalidRequest)?;
        validate_destination(&destination, policy)?;
        if destination.as_str().len() > limits.max_url_bytes.get() {
            return Err(McpInvocationError::InvalidRequest);
        }
        Ok(Self {
            message,
            url,
            destination,
            context,
        })
    }

    pub fn validate(
        &self,
        limits: McpElicitationUrlLimits,
        policy: McpElicitationUrlPolicy,
    ) -> Result<(), McpInvocationError> {
        validate_sizes(&self.message, &self.url, &self.context, limits)?;
        if self.destination.as_str().len() > limits.max_url_bytes.get() {
            return Err(McpInvocationError::InvalidRequest);
        }
        validate_destination(&self.destination, policy)
    }

    pub fn message(&self) -> &str {
        &self.message
    }
    /// Show the full, exact URL before consent. Do not turn other message text
    /// into links. The OS browser handoff must be initiated by the user alone.
    pub fn url(&self) -> &str {
        &self.url
    }
    /// Highlight this parsed ASCII host alongside the full URL to distinguish
    /// the actual destination from path/query text and Unicode lookalikes.
    pub fn host(&self) -> &str {
        self.destination.host_str().expect("validated host")
    }
    pub fn origin(&self) -> String {
        self.destination.origin().ascii_serialization()
    }
    pub fn has_punycode_host(&self) -> bool {
        self.host()
            .split('.')
            .any(|label| label.starts_with("xn--"))
    }
    pub fn context(&self) -> &McpElicitationUrlContext {
        &self.context
    }
}

fn validate_sizes(
    message: &str,
    url: &str,
    context: &McpElicitationUrlContext,
    limits: McpElicitationUrlLimits,
) -> Result<(), McpInvocationError> {
    if message.len() > limits.max_message_bytes.get() || url.len() > limits.max_url_bytes.get() {
        return Err(McpInvocationError::InvalidRequest);
    }
    if let McpElicitationUrlContext::Legacy { elicitation_id } = context {
        if elicitation_id.is_empty() || elicitation_id.len() > limits.max_legacy_id_bytes.get() {
            return Err(McpInvocationError::InvalidRequest);
        }
    }
    Ok(())
}

fn validate_destination(
    destination: &url::Url,
    policy: McpElicitationUrlPolicy,
) -> Result<(), McpInvocationError> {
    let loopback = match destination.host() {
        Some(url::Host::Domain("localhost")) => true,
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        _ => false,
    };
    if !destination.has_host()
        || !destination.username().is_empty()
        || destination.password().is_some()
        || !(destination.scheme() == "https"
            || destination.scheme() == "http"
                && loopback
                && policy == McpElicitationUrlPolicy::AllowLoopbackHttp)
    {
        return Err(McpInvocationError::InvalidRequest);
    }
    Ok(())
}

/// Consent to out-of-band navigation is not confirmation that the external
/// interaction completed. URL decisions never contain credentials or form data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpElicitationUrlDecision {
    Accept,
    Decline,
    Cancel,
}
impl McpElicitationUrlDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Decline => "decline",
            Self::Cancel => "cancel",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> McpElicitationUrlLimits {
        McpElicitationUrlLimits {
            max_message_bytes: NonZeroUsize::new(64).unwrap(),
            max_url_bytes: NonZeroUsize::new(256).unwrap(),
            max_legacy_id_bytes: NonZeroUsize::new(32).unwrap(),
        }
    }
    fn request(
        url: &str,
        policy: McpElicitationUrlPolicy,
    ) -> Result<McpElicitationUrl, McpInvocationError> {
        McpElicitationUrl::new(
            "Private message".into(),
            url.into(),
            McpElicitationUrlContext::Stateless,
            limits(),
            policy,
        )
    }

    #[test]
    fn url_consent_boundary_preserves_exact_text_and_rejects_ambiguous_destinations() {
        let raw = "https://例え.example/continue?state=private#step";
        let value = request(raw, McpElicitationUrlPolicy::HttpsOnly).unwrap();
        assert_eq!(value.url(), raw);
        assert_eq!(value.host(), "xn--r8jz45g.example");
        assert!(value.has_punycode_host());
        assert_eq!(value.origin(), "https://xn--r8jz45g.example");
        assert_eq!(value.context(), &McpElicitationUrlContext::Stateless);
        let debug = format!("{value:?}");
        for private in ["Private", "continue", "state", "example"] {
            assert!(!debug.contains(private));
        }
        for invalid in [
            "javascript:alert(1)",
            "file:///etc/passwd",
            "data:text/plain,test",
            "https://user:secret@example.com/",
            "https://user@example.com/",
            " https://example.com/",
            "https://example.com/with space",
            "https://example.com\\@other.example/",
            "https://example.com/\n",
            "http://example.com/",
            "http://localhost.example/",
            "//example.com/",
        ] {
            assert!(
                request(invalid, McpElicitationUrlPolicy::AllowLoopbackHttp).is_err(),
                "{invalid}"
            );
        }
        for local in [
            "http://localhost:3000/",
            "http://127.0.0.1/",
            "http://[::1]/",
        ] {
            assert!(request(local, McpElicitationUrlPolicy::HttpsOnly).is_err());
            let value = request(local, McpElicitationUrlPolicy::AllowLoopbackHttp).unwrap();
            assert!(
                value
                    .validate(limits(), McpElicitationUrlPolicy::HttpsOnly)
                    .is_err()
            );
        }
    }

    #[test]
    fn url_budgets_cover_original_and_normalized_text_and_legacy_identity() {
        let legacy = McpElicitationUrlContext::Legacy {
            elicitation_id: "private-id".into(),
        };
        let value = McpElicitationUrl::new(
            "message".into(),
            "https://example.com/".into(),
            legacy.clone(),
            limits(),
            McpElicitationUrlPolicy::HttpsOnly,
        )
        .unwrap();
        assert_eq!(value.context(), &legacy);
        assert!(!format!("{legacy:?}").contains("private-id"));
        let exact = McpElicitationUrlLimits {
            max_message_bytes: NonZeroUsize::new(7).unwrap(),
            max_url_bytes: NonZeroUsize::new(20).unwrap(),
            max_legacy_id_bytes: NonZeroUsize::new(10).unwrap(),
        };
        assert!(
            value
                .validate(exact, McpElicitationUrlPolicy::HttpsOnly)
                .is_ok()
        );
        for smaller in [
            McpElicitationUrlLimits {
                max_message_bytes: NonZeroUsize::new(6).unwrap(),
                ..exact
            },
            McpElicitationUrlLimits {
                max_url_bytes: NonZeroUsize::new(18).unwrap(),
                ..exact
            },
            McpElicitationUrlLimits {
                max_legacy_id_bytes: NonZeroUsize::new(9).unwrap(),
                ..exact
            },
        ] {
            assert!(
                value
                    .validate(smaller, McpElicitationUrlPolicy::HttpsOnly)
                    .is_err()
            );
        }
        let raw = "https://example.com/é";
        let cap = McpElicitationUrlLimits {
            max_url_bytes: NonZeroUsize::new(raw.len()).unwrap(),
            ..limits()
        };
        assert!(
            McpElicitationUrl::new(
                "message".into(),
                raw.into(),
                McpElicitationUrlContext::Stateless,
                cap,
                McpElicitationUrlPolicy::HttpsOnly
            )
            .is_err()
        );
        assert!(
            McpElicitationUrl::new(
                "message".into(),
                "https://example.com/".into(),
                McpElicitationUrlContext::Legacy {
                    elicitation_id: String::new()
                },
                limits(),
                McpElicitationUrlPolicy::HttpsOnly
            )
            .is_err()
        );
    }
}
