//! Form validation only. Successful validation is not a user decision or send permit.

use std::{collections::BTreeSet, num::NonZeroUsize};

use kiln_core::{McpElicitationForm, McpElicitationFormLimits};
use rmcp::model::{ElicitResult, ElicitationAction, ElicitationSchema};
use serde_json::Value;

use crate::schema::NoRetrieval;

#[derive(Debug, Clone, Copy)]
pub struct McpElicitationValidationLimits {
    pub form: McpElicitationFormLimits,
    /// Encoded ElicitResult allowance, including action and JSON escaping.
    pub max_response_bytes: NonZeroUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpElicitationError {
    LimitExceeded,
    InvalidSchema,
    UnsupportedSchema,
    InvalidResponse,
}

/// Compiles the supported SDK form vocabulary without retaining the prompt.
/// It performs no persistence, authentication, user interaction or network I/O.
pub struct McpElicitationValidator {
    validator: jsonschema::Validator,
    fields: BTreeSet<String>,
    max_response_bytes: NonZeroUsize,
}

impl McpElicitationValidator {
    pub fn new(
        form: &McpElicitationForm,
        limits: McpElicitationValidationLimits,
    ) -> Result<Self, McpElicitationError> {
        use McpElicitationError as Error;
        form.validate(limits.form)
            .map_err(|_| Error::LimitExceeded)?;
        let original: Value =
            serde_json::from_str(form.schema_json()).map_err(|_| Error::InvalidSchema)?;
        let schema: ElicitationSchema =
            serde_json::from_value(original.clone()).map_err(|_| Error::InvalidSchema)?;
        let normalized = serde_json::to_value(&schema).map_err(|_| Error::InvalidSchema)?;
        // SDK serde models can ignore unknown fields. Never silently drop a
        // constraint when accepting stored forms for presentation/validation.
        if !retains_fields(&original, &normalized) {
            return Err(Error::UnsupportedSchema);
        }
        // Earlier dialects do not implement all keywords in the SDK vocabulary
        // (notably const). Unknown dialects must not silently weaken validation.
        if schema.schema.as_deref().is_some_and(|dialect| {
            !matches!(
                dialect,
                "http://json-schema.org/draft-07/schema#"
                    | "http://json-schema.org/draft-07/schema"
                    | "https://json-schema.org/draft/2019-09/schema"
                    | "https://json-schema.org/draft/2020-12/schema"
            )
        }) {
            return Err(Error::UnsupportedSchema);
        }
        if schema.required.as_ref().is_some_and(|required| {
            required
                .iter()
                .any(|name| !schema.properties.contains_key(name))
        }) {
            return Err(Error::InvalidSchema);
        }
        // Compile the original values, not SDK float-normalized numeric bounds.
        // Supported form fields contain no server-defined regexes or references.
        let validator = jsonschema::options()
            .with_retriever(NoRetrieval)
            .should_validate_formats(true)
            .should_ignore_unknown_formats(false)
            .build(&original)
            .map_err(|_| Error::InvalidSchema)?;
        Ok(Self {
            validator,
            fields: schema.properties.into_keys().collect(),
            max_response_bytes: limits.max_response_bytes,
        })
    }

    /// Validate an explicit decision payload; never infer acceptance or fill
    /// server-provided defaults. The caller must separately authorize, journal
    /// and revalidate live ownership before sending the result.
    pub fn response(
        &self,
        action: ElicitationAction,
        content: Option<Value>,
    ) -> Result<ElicitResult, McpElicitationError> {
        use McpElicitationError as Error;
        let mut result = ElicitResult::new(action);
        result.content = content;
        // Bound serialization before schema validation (which may clone values).
        serde_json::to_writer(&mut ResponseBudget(self.max_response_bytes.get()), &result)
            .map_err(|_| Error::LimitExceeded)?;
        match (&result.action, &result.content) {
            (ElicitationAction::Accept, Some(Value::Object(fields))) => {
                if fields.keys().any(|key| !self.fields.contains(key))
                    || !self
                        .validator
                        .is_valid(result.content.as_ref().expect("matched content"))
                {
                    return Err(Error::InvalidResponse);
                }
            }
            (ElicitationAction::Decline | ElicitationAction::Cancel, None) => {}
            _ => return Err(Error::InvalidResponse),
        }
        Ok(result)
    }
}

pub(crate) fn normalize_form(
    request: rmcp::model::ElicitRequestParams,
    limits: McpElicitationValidationLimits,
) -> Result<(McpElicitationForm, McpElicitationValidator), McpElicitationError> {
    let rmcp::model::ElicitRequestParams::FormElicitationParams {
        message,
        requested_schema,
        ..
    } = request
    else {
        return Err(McpElicitationError::UnsupportedSchema);
    };
    if message.len() > limits.form.max_message_bytes.get() {
        return Err(McpElicitationError::LimitExceeded);
    }
    serde_json::to_writer(
        &mut ResponseBudget(limits.form.max_schema_bytes.get()),
        &requested_schema,
    )
    .map_err(|_| McpElicitationError::LimitExceeded)?;
    let schema =
        serde_json::to_string(&requested_schema).map_err(|_| McpElicitationError::InvalidSchema)?;
    let form = McpElicitationForm::new(message, &schema, limits.form)
        .map_err(|_| McpElicitationError::InvalidSchema)?;
    let validator = McpElicitationValidator::new(&form, limits)?;
    Ok((form, validator))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpElicitationDecisionError {
    Validation(McpElicitationError),
    Store(kiln_core::McpInvocationError),
}

/// Validate and persist an explicit user decision against one immutable form.
/// The caller must authenticate user access to interaction_run. This does not
/// resolve the input, dispatch a response, or authorize provider work.
pub async fn decide_elicitation_form<S: kiln_core::McpElicitationDecisionStore>(
    store: &S,
    input: &kiln_core::McpInputRecord,
    interaction_run: &kiln_core::RunId,
    decision: &kiln_core::McpElicitationDecision,
    limits: McpElicitationValidationLimits,
) -> Result<kiln_core::McpElicitationDecisionMutation, McpElicitationDecisionError> {
    use McpElicitationDecisionError as Error;
    decision
        .validate(limits.max_response_bytes)
        .map_err(Error::Store)?;
    let form = store
        .get_mcp_elicitation_form(input, interaction_run, limits.form)
        .await
        .map_err(Error::Store)?;
    let validator = McpElicitationValidator::new(&form.form, limits).map_err(Error::Validation)?;
    let result: ElicitResult = serde_json::from_str(decision.as_json())
        .map_err(|_| Error::Validation(McpElicitationError::InvalidResponse))?;
    validator
        .response(result.action, result.content)
        .map_err(Error::Validation)?;
    store
        .decide_mcp_elicitation_form(&form, decision, limits.form, limits.max_response_bytes)
        .await
        .map_err(Error::Store)
}

// Numeric bounds may serialize as floats in the SDK. Accept exact changes of
// representation (1 to 1.0), but reject rounding. All other values, field names
// and nesting must survive. This also rejects ignored extension keywords.
fn retains_fields(original: &Value, normalized: &Value) -> bool {
    match (original, normalized) {
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter().all(|(key, value)| {
                    b.get(key).is_some_and(|other| retains_fields(value, other))
                })
        }
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| retains_fields(a, b))
        }
        (Value::Number(a), Value::Number(b)) => {
            a == b || exact_float_conversion(a, b) || exact_float_conversion(b, a)
        }
        _ => original == normalized,
    }
}

fn exact_float_conversion(integer: &serde_json::Number, float: &serde_json::Number) -> bool {
    // i128 holds every i64/u64 JSON integer and the adjacent rounded f64 values.
    // Comparing in f64 would erase the very precision loss being checked.
    if !float.is_f64() {
        return false;
    }
    let integer = integer
        .as_i64()
        .map(i128::from)
        .or_else(|| integer.as_u64().map(i128::from));
    match (integer, float.as_f64()) {
        (Some(integer), Some(float)) => float.fract() == 0.0 && float as i128 == integer,
        _ => false,
    }
}

struct ResponseBudget(usize);
impl std::io::Write for ResponseBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_sub(bytes.len())
            .ok_or_else(|| std::io::Error::other("elicitation response exceeds budget"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn limits() -> McpElicitationValidationLimits {
        // Fixture allowances cover these forms and encoded response envelopes.
        McpElicitationValidationLimits {
            form: McpElicitationFormLimits {
                max_message_bytes: NonZeroUsize::new(64).unwrap(),
                max_schema_bytes: NonZeroUsize::new(4096).unwrap(),
            },
            max_response_bytes: NonZeroUsize::new(1024).unwrap(),
        }
    }
    fn form(schema: Value) -> McpElicitationForm {
        McpElicitationForm::new(
            "untrusted prompt".into(),
            &schema.to_string(),
            limits().form,
        )
        .unwrap()
    }

    #[test]
    fn elicitation_validates_supported_fields_and_formats() {
        for (property, valid, invalid) in [
            (
                json!({"type":"string","minLength":2,"maxLength":4}),
                json!("éé"),
                json!("a"),
            ),
            (
                json!({"type":"string","format":"email"}),
                json!("a@example.test"),
                json!("invalid"),
            ),
            (
                json!({"type":"string","format":"uri"}),
                json!("https://example.test/a"),
                json!("relative"),
            ),
            (
                json!({"type":"string","format":"date"}),
                json!("2026-09-27"),
                json!("2026-02-30"),
            ),
            (
                json!({"type":"string","format":"date-time"}),
                json!("2026-09-27T12:00:00Z"),
                json!("yesterday"),
            ),
            (
                json!({"type":"integer","minimum":1,"maximum":3}),
                json!(2),
                json!(2.5),
            ),
            (
                json!({"type":"number","minimum":1,"maximum":3}),
                json!(2.5),
                json!(4),
            ),
            (json!({"type":"boolean"}), json!(true), json!("true")),
            (
                json!({"type":"string","enum":["a","b"]}),
                json!("a"),
                json!("c"),
            ),
            (
                json!({"type":"string","oneOf":[{"const":"a","title":"A"},{"const":"b","title":"B"}]}),
                json!("b"),
                json!("c"),
            ),
            (
                json!({"type":"array","minItems":1,"maxItems":2,"items":{"type":"string","enum":["a","b"]}}),
                json!(["a", "b"]),
                json!(["c"]),
            ),
            (
                json!({"type":"array","items":{"anyOf":[{"const":"a","title":"A"},{"const":"b","title":"B"}]}}),
                json!(["a"]),
                json!(["c"]),
            ),
        ] {
            let form =
                form(json!({"type":"object","properties":{"field":property},"required":["field"]}));
            let validator = McpElicitationValidator::new(&form, limits()).unwrap();
            let result = validator
                .response(ElicitationAction::Accept, Some(json!({"field":valid})))
                .unwrap();
            assert!(result.meta.is_none());
            assert_eq!(
                validator
                    .response(ElicitationAction::Accept, Some(json!({"field":invalid})))
                    .err(),
                Some(McpElicitationError::InvalidResponse)
            );
            assert_eq!(
                validator
                    .response(ElicitationAction::Accept, Some(json!({})))
                    .err(),
                Some(McpElicitationError::InvalidResponse)
            );
        }
    }

    #[test]
    fn elicitation_rejects_dropped_constraints_and_unsupported_shapes() {
        for schema in [
            json!({"type":"object","properties":{},"$ref":"https://example.test/private"}),
            json!({"type":"object","properties":{},"additionalProperties":false}),
            json!({"type":"object","properties":{"f":{"type":"object","properties":{}}}}),
            json!({"type":"object","properties":{"f":{"type":"string","pattern":".*"}}}),
            json!({"type":"object","properties":{"f":{"type":"array","items":{"type":"string"}}}}),
            json!({"type":"object","properties":{"f":{"type":"string","format":"password"}}}),
            json!({"type":"object","properties":{},"required":["missing"]}),
            json!({"type":"object","properties":{},"$schema":"https://example.test/schema"}),
            json!({"type":"object","properties":{},"$schema":"http://json-schema.org/draft-04/schema#"}),
        ] {
            assert!(McpElicitationValidator::new(&form(schema), limits()).is_err());
        }
        for dialect in [
            "http://json-schema.org/draft-07/schema#",
            "https://json-schema.org/draft/2019-09/schema",
            "https://json-schema.org/draft/2020-12/schema",
        ] {
            let schema = form(json!({"type":"object","properties":{},"$schema":dialect}));
            assert!(McpElicitationValidator::new(&schema, limits()).is_ok());
        }
    }

    #[test]
    fn elicitation_rejects_lossy_sdk_numeric_bounds() {
        // 2^53 + 1 cannot be represented exactly by the SDK's f64 bound.
        for bound in [
            json!(9_007_199_254_740_993_u64),
            json!(u64::MAX),
            json!(i64::MAX),
        ] {
            let schema =
                form(json!({"type":"object","properties":{"f":{"type":"number","minimum":bound}}}));
            assert!(matches!(
                McpElicitationValidator::new(&schema, limits()),
                Err(McpElicitationError::UnsupportedSchema)
            ));
        }
        for bound in [
            json!(1),
            json!(9_007_199_254_740_992_u64),
            json!(i64::MIN),
            json!(1.25),
        ] {
            let schema =
                form(json!({"type":"object","properties":{"f":{"type":"number","minimum":bound}}}));
            assert!(McpElicitationValidator::new(&schema, limits()).is_ok());
        }
    }

    #[test]
    fn elicitation_decisions_do_not_add_defaults_or_disclose_extra_data() {
        let form = form(
            json!({"type":"object","properties":{"name":{"type":"string","default":"server value"}}}),
        );
        let validator = McpElicitationValidator::new(&form, limits()).unwrap();
        for action in [ElicitationAction::Decline, ElicitationAction::Cancel] {
            assert!(
                validator
                    .response(action.clone(), None)
                    .unwrap()
                    .content
                    .is_none()
            );
            assert_eq!(
                validator
                    .response(action, Some(json!({"name":"private"})))
                    .err(),
                Some(McpElicitationError::InvalidResponse)
            );
        }
        for content in [
            None,
            Some(Value::Null),
            Some(json!([])),
            Some(json!({"extra":"private"})),
        ] {
            assert_eq!(
                validator.response(ElicitationAction::Accept, content).err(),
                Some(McpElicitationError::InvalidResponse)
            );
        }
        assert_eq!(
            validator
                .response(ElicitationAction::Accept, Some(json!({})))
                .unwrap()
                .content,
            Some(json!({}))
        );
        let exact = json!({"action":"accept","content":{"name":"é\n"}})
            .to_string()
            .len();
        for (size, succeeds) in [(exact, true), (exact - 1, false)] {
            let bounded = McpElicitationValidator::new(
                &form,
                McpElicitationValidationLimits {
                    max_response_bytes: NonZeroUsize::new(size).unwrap(),
                    ..limits()
                },
            )
            .unwrap();
            assert_eq!(
                bounded
                    .response(ElicitationAction::Accept, Some(json!({"name":"é\n"})))
                    .is_ok(),
                succeeds
            );
        }
    }
}
