//! Private mediation receipts. These never authorize replay or contain MCP bodies.

use std::num::NonZeroU64;

use crate::{McpInvocationError, McpInvocationRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpInputKind {
    Roots,
    Sampling,
    Elicitation,
}
impl McpInputKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Roots => "roots",
            Self::Sampling => "sampling",
            Self::Elicitation => "elicitation",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpInputState {
    Required,
    Resolved,
    Interrupted,
}
impl McpInputState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Required => "required",
            Self::Resolved => "resolved",
            Self::Interrupted => "interrupted",
        }
    }
}

/// An ordinal belongs to one invocation, across all legacy requests/MRTR rounds.
/// Resolution records completed mediation, not permission to retry the operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpInputRecord {
    pub invocation: McpInvocationRecord,
    pub ordinal: NonZeroU64,
    pub kind: McpInputKind,
    pub state: McpInputState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpInputMutation {
    Applied(McpInputRecord),
    Existing(McpInputRecord),
}

impl McpInputMutation {
    pub fn record(&self) -> &McpInputRecord {
        match self {
            Self::Applied(record) | Self::Existing(record) => record,
        }
    }
}

pub trait McpInputStore: Send + Sync {
    /// Identify the live interaction owner without creating or approving input.
    /// Interactive Runs own their input; read-only children route to their
    /// interactive root in the same Session. This snapshot is not authority to
    /// publish a prompt or execute sampling; writers must revalidate ownership.
    fn mcp_input_interaction_run(
        &self,
        expected: &McpInputRecord,
    ) -> impl Future<Output = Result<crate::RunId, McpInvocationError>> + Send;

    /// Resolve the one directory already approved for this live roots input.
    /// Includes the approved relative scope, never the broader workspace root.
    /// A path is metadata, not filesystem access or authority to send a response;
    /// resolution must still be journaled against the live claim before sending.
    fn mcp_input_root(
        &self,
        expected: &McpInputRecord,
        limits: crate::McpDefinitionLimits,
    ) -> impl Future<Output = Result<std::path::PathBuf, McpInvocationError>> + Send;

    /// Record the next input before invoking any provider or interaction path.
    /// Only one input can remain pending per invocation; exact retries return a
    /// receipt, including after interruption, and never grant execution authority.
    fn require_mcp_input(
        &self,
        invocation: &McpInvocationRecord,
        ordinal: NonZeroU64,
        kind: McpInputKind,
    ) -> impl Future<Output = Result<McpInputMutation, McpInvocationError>> + Send;

    /// Journal mediation before sending its response. The same live invocation
    /// must still own dispatch. Sampling/elicitation also require live same-Session
    /// interaction ancestry in the resolution transaction. This does not approve
    /// provider or user interaction. Restart/termination interrupts pending inputs.
    fn resolve_mcp_input(
        &self,
        expected: &McpInputRecord,
    ) -> impl Future<Output = Result<McpInputMutation, McpInvocationError>> + Send;
}

/// Host-selected storage budgets for normalized form fields, not wire frames.
#[derive(Debug, Clone, Copy)]
pub struct McpElicitationFormLimits {
    pub max_message_bytes: std::num::NonZeroUsize,
    pub max_schema_bytes: std::num::NonZeroUsize,
}

/// Private, untrusted form data. This container checks size and object shape;
/// the protocol adapter must validate the supported schema before presenting it.
/// Its fields exclude wire IDs, requestState and transport/credential metadata.
/// The untrusted message and schema may themselves contain sensitive content.
#[derive(Clone, PartialEq, Eq)]
pub struct McpElicitationForm {
    message: String,
    schema_json: String,
}

impl std::fmt::Debug for McpElicitationForm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpElicitationForm").finish_non_exhaustive()
    }
}

impl McpElicitationForm {
    pub fn new(
        message: String,
        schema_json: &str,
        limits: McpElicitationFormLimits,
    ) -> Result<Self, McpInvocationError> {
        if message.len() > limits.max_message_bytes.get()
            || schema_json.len() > limits.max_schema_bytes.get()
        {
            return Err(McpInvocationError::InvalidRequest);
        }
        let schema: serde_json::Value =
            serde_json::from_str(schema_json).map_err(|_| McpInvocationError::InvalidRequest)?;
        if !schema.is_object() || schema.get("type").and_then(|v| v.as_str()) != Some("object") {
            return Err(McpInvocationError::InvalidRequest);
        }
        let form = Self {
            message,
            schema_json: serde_json::to_string(&schema)
                .map_err(|_| McpInvocationError::InvalidRequest)?,
        };
        form.validate(limits)?;
        Ok(form)
    }

    pub fn validate(&self, limits: McpElicitationFormLimits) -> Result<(), McpInvocationError> {
        if self.message.len() > limits.max_message_bytes.get()
            || self.schema_json.len() > limits.max_schema_bytes.get()
        {
            return Err(McpInvocationError::InvalidRequest);
        }
        Ok(())
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn schema_json(&self) -> &str {
        &self.schema_json
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpElicitationFormRecord {
    pub input: McpInputRecord,
    pub interaction_run: crate::RunId,
    pub form: McpElicitationForm,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpElicitationFormMutation {
    Applied(McpElicitationFormRecord),
    Existing(McpElicitationFormRecord),
}

/// Private interaction storage, separate from tool approval and public Events.
/// Neither a record nor an Existing receipt grants response or provider authority.
pub trait McpElicitationFormStore: Send + Sync {
    /// Atomically create the pending input, its immutable form and live owner,
    /// and the metadata-only input-required Event. Exact pending duplicates are
    /// receipts; changed forms, missing forms and inactive ownership reject.
    fn require_mcp_elicitation_form(
        &self,
        invocation: &McpInvocationRecord,
        ordinal: NonZeroU64,
        form: &McpElicitationForm,
        limits: McpElicitationFormLimits,
    ) -> impl Future<Output = Result<McpElicitationFormMutation, McpInvocationError>> + Send;

    /// Inspect only through the recorded interactive owner while input and the
    /// entire source ancestry remain live. No generic Session/Event projection
    /// includes the body. The caller must authenticate access to the target Run.
    fn get_mcp_elicitation_form(
        &self,
        expected: &McpInputRecord,
        interaction_run: &crate::RunId,
        limits: McpElicitationFormLimits,
    ) -> impl Future<Output = Result<McpElicitationFormRecord, McpInvocationError>> + Send;
}

/// A normalized private user decision, not transport metadata or send authority.
/// Construction checks the response envelope and size, not the form's schema.
#[derive(Clone, PartialEq, Eq)]
pub struct McpElicitationDecision {
    json: String,
}

impl std::fmt::Debug for McpElicitationDecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpElicitationDecision")
            .finish_non_exhaustive()
    }
}

impl McpElicitationDecision {
    pub fn from_json(
        json: &str,
        max_bytes: std::num::NonZeroUsize,
    ) -> Result<Self, McpInvocationError> {
        if json.len() > max_bytes.get() {
            return Err(McpInvocationError::InvalidRequest);
        }
        let value: serde_json::Value =
            serde_json::from_str(json).map_err(|_| McpInvocationError::InvalidRequest)?;
        let fields = value
            .as_object()
            .ok_or(McpInvocationError::InvalidRequest)?;
        match fields.get("action").and_then(|action| action.as_str()) {
            Some("accept")
                if fields.len() == 2 && fields.get("content").is_some_and(|v| v.is_object()) => {}
            Some("decline" | "cancel") if fields.len() == 1 => {}
            _ => return Err(McpInvocationError::InvalidRequest),
        }
        let decision = Self {
            json: serde_json::to_string(&value).map_err(|_| McpInvocationError::InvalidRequest)?,
        };
        decision.validate(max_bytes)?;
        Ok(decision)
    }

    pub fn validate(&self, max_bytes: std::num::NonZeroUsize) -> Result<(), McpInvocationError> {
        if self.json.len() > max_bytes.get() {
            return Err(McpInvocationError::InvalidRequest);
        }
        Ok(())
    }

    pub fn as_json(&self) -> &str {
        &self.json
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpElicitationDecisionMutation {
    Applied,
    Existing,
}

pub trait McpElicitationDecisionStore: McpElicitationFormStore {
    /// The caller authenticates the user and validates the decision against this
    /// exact immutable form. The write revalidates form, owner, pending input and
    /// live ancestry atomically. First decision wins; identical pending retries
    /// are receipts. No input resolution or response send occurs here.
    fn decide_mcp_elicitation_form(
        &self,
        expected: &McpElicitationFormRecord,
        decision: &McpElicitationDecision,
        form_limits: McpElicitationFormLimits,
        max_response_bytes: std::num::NonZeroUsize,
    ) -> impl Future<Output = Result<McpElicitationDecisionMutation, McpInvocationError>> + Send;

    /// Read a private decision only while this exact form/input/owner remains
    /// live and pending. Terminal input cannot yield a response for replay.
    fn get_mcp_elicitation_decision(
        &self,
        expected: &McpElicitationFormRecord,
        form_limits: McpElicitationFormLimits,
        max_response_bytes: std::num::NonZeroUsize,
    ) -> impl Future<Output = Result<Option<McpElicitationDecision>, McpInvocationError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_decisions_require_explicit_bounded_action_and_redact_content() {
        let cap = std::num::NonZeroUsize::new(128).unwrap();
        let decision = McpElicitationDecision::from_json(
            r#"{ "content": {"answer":"private"}, "action": "accept" }"#,
            cap,
        )
        .unwrap();
        assert_eq!(
            decision.as_json(),
            r#"{"action":"accept","content":{"answer":"private"}}"#
        );
        assert!(!format!("{decision:?}").contains("private"));
        let exact = std::num::NonZeroUsize::new(decision.as_json().len()).unwrap();
        assert!(decision.validate(exact).is_ok());
        assert!(
            decision
                .validate(std::num::NonZeroUsize::new(exact.get() - 1).unwrap())
                .is_err()
        );
        for valid in [r#"{"action":"decline"}"#, r#"{"action":"cancel"}"#] {
            assert!(McpElicitationDecision::from_json(valid, cap).is_ok());
        }
        for invalid in [
            r#"{"action":"accept"}"#,
            r#"{"action":"accept","content":null}"#,
            r#"{"action":"decline","content":{}}"#,
            r#"{"action":"cancel","content":null}"#,
            r#"{"action":"accept","content":{},"_meta":{}}"#,
            r#"{"action":"unknown"}"#,
            "[]",
        ] {
            assert!(McpElicitationDecision::from_json(invalid, cap).is_err());
        }
    }

    #[test]
    fn private_form_checks_byte_budgets_and_canonical_shape_without_logging_body() {
        let limits = McpElicitationFormLimits {
            max_message_bytes: std::num::NonZeroUsize::new(4).unwrap(),
            max_schema_bytes: std::num::NonZeroUsize::new(64).unwrap(),
        };
        let form = McpElicitationForm::new(
            "éé".into(),
            r#"{ "type": "object", "properties": {} }"#,
            limits,
        )
        .unwrap();
        assert_eq!(form.schema_json(), r#"{"properties":{},"type":"object"}"#);
        assert!(!format!("{form:?}").contains("é"));
        assert!(!format!("{form:?}").contains("properties"));
        assert!(McpElicitationForm::new("ééé".into(), form.schema_json(), limits).is_err());
        for invalid in ["false", "[]", "{}", r#"{"type":"string"}"#, "invalid"] {
            assert!(McpElicitationForm::new(String::new(), invalid, limits).is_err());
        }
        let tighter = McpElicitationFormLimits {
            max_schema_bytes: std::num::NonZeroUsize::new(1).unwrap(),
            ..limits
        };
        assert!(form.validate(tighter).is_err());
    }
}
