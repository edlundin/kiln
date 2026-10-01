//! Identity validation for the documented direct ChatGPT plan-usage flow.

use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    jwk::{JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse},
};
use serde::Deserialize;

const ISSUER: &str = "https://auth.openai.com";
const JWKS_URI: &str = "https://auth.openai.com/.well-known/jwks.json";
// Reuse the existing OAuth payload and field bounds. These are local allocation
// safeguards, not provider limits or token-lifetime assumptions.
const MAX_DOCUMENT_BYTES: usize = 1024 * 1024;
const MAX_TOKEN_BYTES: usize = 256 * 1024;
const MAX_IDENTITY_BYTES: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatGptIdentityError {
    InvalidDiscovery,
    InvalidKeys,
    InvalidIdToken,
}

/// Constructed only after signature and transaction-bound claim validation.
/// It carries identity, never the ID token or any other credential.
pub struct VerifiedChatGptIdentity {
    issuer: String,
    client_id: String,
    subject: String,
}

impl VerifiedChatGptIdentity {
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }
}

/// The caller must acquire discovery and keys over the pinned issuer's HTTPS
/// boundary. This pure verifier performs no network or credential-store I/O.
pub struct ChatGptIdentityVerifier {
    keys: JwkSet,
}

impl ChatGptIdentityVerifier {
    pub fn from_documents(
        discovery_json: &[u8],
        jwks_json: &[u8],
    ) -> Result<Self, ChatGptIdentityError> {
        if discovery_json.len() > MAX_DOCUMENT_BYTES {
            return Err(ChatGptIdentityError::InvalidDiscovery);
        }
        let discovery: Discovery = serde_json::from_slice(discovery_json)
            .map_err(|_| ChatGptIdentityError::InvalidDiscovery)?;
        if discovery.issuer != ISSUER || discovery.jwks_uri != JWKS_URI {
            return Err(ChatGptIdentityError::InvalidDiscovery);
        }
        if jwks_json.len() > MAX_DOCUMENT_BYTES {
            return Err(ChatGptIdentityError::InvalidKeys);
        }
        let keys: JwkSet =
            serde_json::from_slice(jwks_json).map_err(|_| ChatGptIdentityError::InvalidKeys)?;
        if keys.keys.is_empty() {
            return Err(ChatGptIdentityError::InvalidKeys);
        }
        Ok(Self { keys })
    }

    pub fn verify(
        &self,
        id_token: &str,
        issued_client_id: &str,
        expected_nonce: &str,
    ) -> Result<VerifiedChatGptIdentity, ChatGptIdentityError> {
        use ChatGptIdentityError::InvalidIdToken;
        if id_token.len() > MAX_TOKEN_BYTES
            || !valid_identity_field(issued_client_id)
            || issued_client_id == "dynamic_agent_client"
            || !valid_identity_field(expected_nonce)
        {
            return Err(InvalidIdToken);
        }
        let header = decode_header(id_token).map_err(|_| InvalidIdToken)?;
        if header.alg != Algorithm::RS256 {
            return Err(InvalidIdToken);
        }
        let kid = header.kid.ok_or(InvalidIdToken)?;
        let mut matching_keys = self
            .keys
            .keys
            .iter()
            .filter(|key| key.common.key_id.as_deref() == Some(kid.as_str()));
        let key = matching_keys.next().ok_or(InvalidIdToken)?;
        if matching_keys.next().is_some() {
            return Err(InvalidIdToken);
        }
        if key
            .common
            .key_algorithm
            .is_some_and(|algorithm| algorithm != KeyAlgorithm::RS256)
            || key
                .common
                .public_key_use
                .as_ref()
                .is_some_and(|usage| *usage != PublicKeyUse::Signature)
            || key
                .common
                .key_operations
                .as_ref()
                .is_some_and(|operations| !operations.contains(&KeyOperations::Verify))
        {
            return Err(InvalidIdToken);
        }
        let key = DecodingKey::from_jwk(key).map_err(|_| InvalidIdToken)?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[ISSUER]);
        validation.set_audience(&[issued_client_id]);
        validation.set_required_spec_claims(&["iss", "aud", "sub", "exp"]);
        validation.leeway = 0;
        // The library compares exp < now. With integer-second claims, rejecting
        // expiring tokens within one second also rejects exp == now, as OIDC
        // requires, without adding a token-freshness policy.
        validation.reject_tokens_expiring_in_less_than = 1;
        let claims = decode::<IdentityClaims>(id_token, &key, &validation)
            .map_err(|_| InvalidIdToken)?
            .claims;
        if claims.nonce != expected_nonce || !valid_identity_field(&claims.sub) {
            return Err(InvalidIdToken);
        }
        Ok(VerifiedChatGptIdentity {
            issuer: ISSUER.to_owned(),
            client_id: issued_client_id.to_owned(),
            subject: claims.sub,
        })
    }
}

#[derive(Deserialize)]
struct Discovery {
    issuer: String,
    jwks_uri: String,
}

#[derive(Clone, Deserialize)]
struct IdentityClaims {
    sub: String,
    nonce: String,
    // The JWT library's required-claim set does not handle iat. OIDC requires
    // its presence and numeric type; it is not a separate freshness window.
    #[serde(rename = "iat")]
    _issued_at: u64,
}

fn valid_identity_field(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTITY_BYTES
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde_json::{Value, json};

    use super::*;

    const DISCOVERY: &[u8] = br#"{"issuer":"https://auth.openai.com","jwks_uri":"https://auth.openai.com/.well-known/jwks.json"}"#;
    const JWKS: &[u8] = include_bytes!("fixtures/chatgpt-id-token-test-jwks.json");
    // Generated solely for these never-issued fixtures with openssl genpkey
    // -algorithm RSA -outform DER -pkeyopt rsa_keygen_bits:2048, then
    // openssl rsa -inform DER -traditional -outform DER (PKCS#1).
    const KEY: &[u8] = include_bytes!("fixtures/chatgpt-id-token-test-key.der");

    fn claims() -> Value {
        json!({
            "iss": ISSUER,
            "aud": "oaiapp_kiln_fixture",
            "sub": "fixture-subject",
            "exp": 4_102_444_800_u64,
            "iat": 1,
            "nonce": "fixture-transaction-nonce",
        })
    }

    fn signed(claims: &Value) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("kiln-test-only".to_owned());
        encode(&header, claims, &EncodingKey::from_rsa_der(KEY)).unwrap()
    }

    #[test]
    fn signature_and_transaction_claims_bind_the_identity() {
        let verifier = ChatGptIdentityVerifier::from_documents(DISCOVERY, JWKS).unwrap();
        let token = signed(&claims());
        let identity = verifier
            .verify(&token, "oaiapp_kiln_fixture", "fixture-transaction-nonce")
            .unwrap();
        assert_eq!(identity.issuer(), ISSUER);
        assert_eq!(identity.client_id(), "oaiapp_kiln_fixture");
        assert_eq!(identity.subject(), "fixture-subject");
        for (field, value) in [
            ("iss", json!("https://other.example")),
            ("aud", json!("oaiapp_other_account")),
            ("nonce", json!("other-transaction")),
            ("exp", json!(1)),
            ("exp", json!(jsonwebtoken::get_current_timestamp())),
            ("sub", json!("")),
        ] {
            let mut invalid = claims();
            invalid[field] = value;
            assert!(
                matches!(
                    verifier.verify(
                        &signed(&invalid),
                        "oaiapp_kiln_fixture",
                        "fixture-transaction-nonce"
                    ),
                    Err(ChatGptIdentityError::InvalidIdToken)
                ),
                "{field}"
            );
        }
        assert!(
            verifier
                .verify(&token, "dynamic_agent_client", "fixture-transaction-nonce")
                .is_err()
        );
        for field in ["iss", "aud", "sub", "exp", "iat", "nonce"] {
            let mut invalid = claims();
            invalid.as_object_mut().unwrap().remove(field);
            assert!(
                verifier
                    .verify(
                        &signed(&invalid),
                        "oaiapp_kiln_fixture",
                        "fixture-transaction-nonce"
                    )
                    .is_err(),
                "missing {field}"
            );
        }
    }

    #[test]
    fn altered_signature_and_algorithm_confusion_are_rejected() {
        let verifier = ChatGptIdentityVerifier::from_documents(DISCOVERY, JWKS).unwrap();
        let token = signed(&claims());
        let mut parts: Vec<_> = token.split('.').map(str::to_owned).collect();
        let mut signature = URL_SAFE_NO_PAD.decode(&parts[2]).unwrap();
        signature[0] ^= 1;
        parts[2] = URL_SAFE_NO_PAD.encode(signature);
        assert!(
            verifier
                .verify(
                    &parts.join("."),
                    "oaiapp_kiln_fixture",
                    "fixture-transaction-nonce"
                )
                .is_err()
        );
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some("kiln-test-only".to_owned());
        let forged = encode(&header, &claims(), &EncodingKey::from_secret(JWKS)).unwrap();
        assert!(
            verifier
                .verify(&forged, "oaiapp_kiln_fixture", "fixture-transaction-nonce")
                .is_err()
        );
    }

    #[test]
    fn discovery_and_key_purpose_are_not_taken_from_the_token() {
        let invalid_discovery =
            br#"{"issuer":"https://auth.openai.com","jwks_uri":"https://other.example/keys"}"#;
        assert!(matches!(
            ChatGptIdentityVerifier::from_documents(invalid_discovery, JWKS),
            Err(ChatGptIdentityError::InvalidDiscovery)
        ));
        let token = signed(&claims());
        for (field, value) in [
            ("kid", json!("unknown-key")),
            ("use", json!("enc")),
            ("alg", json!("RS512")),
            ("key_ops", json!(["encrypt"])),
        ] {
            let mut keys: Value = serde_json::from_slice(JWKS).unwrap();
            keys["keys"][0][field] = value;
            let verifier = ChatGptIdentityVerifier::from_documents(
                DISCOVERY,
                &serde_json::to_vec(&keys).unwrap(),
            )
            .unwrap();
            assert!(
                verifier
                    .verify(&token, "oaiapp_kiln_fixture", "fixture-transaction-nonce")
                    .is_err(),
                "{field}"
            );
        }
        let mut keys: Value = serde_json::from_slice(JWKS).unwrap();
        let duplicate = keys["keys"][0].clone();
        keys["keys"].as_array_mut().unwrap().push(duplicate);
        let verifier =
            ChatGptIdentityVerifier::from_documents(DISCOVERY, &serde_json::to_vec(&keys).unwrap())
                .unwrap();
        assert!(
            verifier
                .verify(&token, "oaiapp_kiln_fixture", "fixture-transaction-nonce")
                .is_err()
        );
    }
}
