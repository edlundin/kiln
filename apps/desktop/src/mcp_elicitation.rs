//! Private form drafts belong to one live input and one desktop connection.

use crate::{connection, theme};
use gpui::{
    Context, Entity, IntoElement, Render, SharedString, Subscription, Window, div, prelude::*,
};
use gpui_component::{
    Disableable, Selectable,
    button::{Button, ButtonVariants},
    input::{Input, InputEvent, InputState},
};
use kiln_protocol::{McpElicitationDecisionRequest as Decision, McpElicitationFormResponse};
use serde_json::{Map, Value};
use std::{collections::BTreeSet, num::NonZeroU64, sync::Arc};
use tokio::{runtime::Runtime, sync::mpsc};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct InputId {
    pub tool_call_id: String,
    pub generation: String,
    pub ordinal: NonZeroU64,
}

#[derive(Clone)]
enum Kind {
    Text,
    Integer,
    Number,
    Choice {
        options: Vec<(String, Value)>,
        multiple: bool,
    },
}
struct FieldSpec {
    name: String,
    label: String,
    description: String,
    hint: String,
    required: bool,
    kind: Kind,
}
struct Field {
    spec: FieldSpec,
    included: bool,
    input: Option<Entity<InputState>>,
    selected: BTreeSet<usize>,
}
struct Form {
    server: String,
    message: String,
    validator: jsonschema::Validator,
    fields: Vec<Field>,
}
struct NoRetrieval;
impl jsonschema::Retrieve for NoRetrieval {
    fn retrieve(
        &self,
        _: &jsonschema::Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(std::io::Error::other("external schema retrieval is disabled").into())
    }
}

fn choices(schema: &Value) -> Result<Vec<(String, Value)>, &'static str> {
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        let names = schema.get("enumNames").and_then(Value::as_array);
        return values
            .iter()
            .enumerate()
            .map(|(i, value)| {
                let text = value.as_str().ok_or("Unsupported choice value")?;
                let label = names
                    .and_then(|names| names.get(i))
                    .and_then(Value::as_str)
                    .unwrap_or(text);
                Ok((label.to_owned(), value.clone()))
            })
            .collect();
    }
    let values = schema
        .get("oneOf")
        .or_else(|| schema.get("anyOf"))
        .and_then(Value::as_array)
        .ok_or("Missing choices")?;
    values
        .iter()
        .map(|item| {
            let value = item
                .get("const")
                .and_then(Value::as_str)
                .ok_or("Unsupported choice value")?;
            Ok((
                item.get("title")
                    .and_then(Value::as_str)
                    .unwrap_or(value)
                    .to_owned(),
                Value::String(value.to_owned()),
            ))
        })
        .collect()
}

fn field_specs(schema: &Value) -> Result<Vec<FieldSpec>, &'static str> {
    if schema.get("type").and_then(Value::as_str) != Some("object") {
        return Err("Unsupported form shape");
    }
    let properties = schema
        .get("properties")
        .and_then(Value::as_object)
        .ok_or("Missing form fields")?;
    let required = schema.get("required").and_then(Value::as_array);
    properties
        .iter()
        .map(|(name, property)| {
            let kind = match property.get("type").and_then(Value::as_str) {
                Some("boolean") => Kind::Choice {
                    options: vec![
                        ("Yes".into(), Value::Bool(true)),
                        ("No".into(), Value::Bool(false)),
                    ],
                    multiple: false,
                },
                Some("string")
                    if property.get("enum").is_some() || property.get("oneOf").is_some() =>
                {
                    Kind::Choice {
                        options: choices(property)?,
                        multiple: false,
                    }
                }
                Some("string") => Kind::Text,
                Some("integer") => Kind::Integer,
                Some("number") => Kind::Number,
                Some("array") => Kind::Choice {
                    options: choices(property.get("items").ok_or("Missing item choices")?)?,
                    multiple: true,
                },
                _ => return Err("This field type cannot be displayed safely"),
            };
            let mut hints = Vec::new();
            for (key, label) in [
                ("minimum", "Minimum"),
                ("maximum", "Maximum"),
                ("minLength", "Minimum characters"),
                ("maxLength", "Maximum characters"),
                ("minItems", "Minimum selections"),
                ("maxItems", "Maximum selections"),
            ] {
                if let Some(value) = property.get(key) {
                    hints.push(format!("{label}: {value}"));
                }
            }
            if let Some(format) = property.get("format").and_then(Value::as_str) {
                hints.push(format!("Format: {format}"));
            }
            let title = property
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or(name);
            Ok(FieldSpec {
                name: name.clone(),
                label: if title == name {
                    title.to_owned()
                } else {
                    format!("{title} ({name})")
                },
                description: property
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
                hint: hints.join(" · "),
                required: required
                    .is_some_and(|fields| fields.iter().any(|field| field.as_str() == Some(name))),
                kind,
            })
        })
        .collect()
}

fn text_value(kind: &Kind, text: &str) -> Result<Value, &'static str> {
    match kind {
        Kind::Text => Ok(Value::String(text.to_owned())),
        Kind::Integer => serde_json::from_str::<Value>(text.trim())
            .ok()
            .filter(|value| value.as_i64().is_some() || value.as_u64().is_some())
            .ok_or("Enter a whole number"),
        Kind::Number => serde_json::from_str::<Value>(text.trim())
            .ok()
            .filter(Value::is_number)
            .ok_or("Enter a finite number"),
        Kind::Choice { .. } => Err("Choose a value"),
    }
}

enum Update {
    Loaded(Result<(McpElicitationFormResponse, String), String>),
    Decided(Result<(), (String, Option<u16>)>),
}

pub struct ElicitationPanel {
    active: bool,
    client: kiln_client::Client,
    runtime: Arc<Runtime>,
    updates: mpsc::UnboundedSender<(ulid::Ulid, Update)>,
    id: InputId,
    owner: String,
    provenance: String,
    source_budget: std::num::NonZeroUsize,
    request: Option<ulid::Ulid>,
    form: Option<Form>,
    error: Option<String>,
    pending: Option<Decision>,
    submitted: bool,
    unavailable: bool,
    tried_accept: bool,
    subscriptions: Vec<Subscription>,
}
impl ElicitationPanel {
    pub fn invalidate(&mut self, cx: &mut Context<Self>) {
        self.active = false;
        self.request = None;
        self.form = None;
        self.pending = None;
        self.subscriptions.clear();
        cx.notify();
    }

    pub fn new(
        client: kiln_client::Client,
        runtime: Arc<Runtime>,
        id: InputId,
        owner: String,
        provenance: String,
        source_budget: std::num::NonZeroUsize,
        cx: &mut Context<Self>,
    ) -> Self {
        let (updates, mut receiver) = mpsc::unbounded_channel();
        cx.spawn(async move |this, cx| {
            while let Some((request, update)) = receiver.recv().await {
                if this
                    .update(cx, |this, cx| this.apply(request, update, cx))
                    .is_err()
                {
                    break;
                }
            }
        })
        .detach();
        let mut panel = Self {
            active: true,
            client,
            runtime,
            updates,
            id,
            owner,
            provenance,
            source_budget,
            request: None,
            form: None,
            error: None,
            pending: None,
            submitted: false,
            unavailable: false,
            tried_accept: false,
            subscriptions: Vec::new(),
        };
        panel.load(cx);
        panel
    }

    fn load(&mut self, cx: &mut Context<Self>) {
        if !self.active
            || self.unavailable
            || self.request.is_some()
            || self.pending.is_some()
            || self.submitted
        {
            return;
        }
        let request = ulid::Ulid::generate();
        self.request = Some(request);
        self.error = None;
        let (client, id, owner, updates) = (
            self.client.clone(),
            self.id.clone(),
            self.owner.clone(),
            self.updates.clone(),
        );
        let source_budget = self.source_budget;
        self.runtime.spawn(async move {
            let result = async {
                let inspection = client
                    .inspect_tool_call(&id.tool_call_id, source_budget)
                    .await
                    .map_err(|error| {
                        connection::error_message("Identify requesting server", &error)
                    })?;
                if inspection.tool_call_id != id.tool_call_id {
                    return Err("The request identity changed".to_owned());
                }
                let source = inspection
                    .source
                    .ok_or_else(|| "The requesting server cannot be identified".to_owned())?;
                let arguments: Value = serde_json::from_str(&source.arguments_json)
                    .map_err(|_| "The server request is invalid".to_owned())?;
                let server = arguments
                    .get("server_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "The requesting server cannot be identified".to_owned())?
                    .to_owned();
                let form = client
                    .inspect_mcp_elicitation(&owner, &id.tool_call_id, &id.generation, id.ordinal)
                    .await
                    .map_err(|error| connection::error_message("Load requested form", &error))?;
                Ok((form, server))
            }
            .await;
            let _ = updates.send((request, Update::Loaded(result)));
        });
        cx.notify();
    }

    fn apply(&mut self, request: ulid::Ulid, update: Update, cx: &mut Context<Self>) {
        if self.request != Some(request) {
            return;
        }
        self.request = None;
        match update {
            Update::Loaded(result) => {
                let result = result.and_then(|(response, server)| {
                    let schema: Value = serde_json::from_str(&response.schema_json)
                        .map_err(|_| "The form schema is invalid".to_owned())?;
                    let specs = field_specs(&schema).map_err(str::to_owned)?;
                    let validator = jsonschema::options()
                        .with_retriever(NoRetrieval)
                        .should_validate_formats(true)
                        .should_ignore_unknown_formats(false)
                        .build(&schema)
                        .map_err(|_| "The form constraints cannot be validated".to_owned())?;
                    Ok(Form {
                        server,
                        message: response.message,
                        validator,
                        fields: specs
                            .into_iter()
                            .map(|spec| Field {
                                included: spec.required,
                                spec,
                                input: None,
                                selected: BTreeSet::new(),
                            })
                            .collect(),
                    })
                });
                match result {
                    Ok(form) => self.form = Some(form),
                    Err(error) => self.error = Some(error),
                }
            }
            Update::Decided(Ok(())) => {
                self.pending = None;
                self.submitted = true;
                self.form = None;
                self.subscriptions.clear();
            }
            Update::Decided(Err((error, status))) => {
                self.error = Some(error);
                if status == Some(400) {
                    self.pending = None;
                } else if matches!(status, Some(404 | 409)) {
                    self.pending = None;
                    self.form = None;
                    self.subscriptions.clear();
                    self.unavailable = true;
                }
            }
        }
        cx.notify();
    }

    fn content(&self, cx: &Context<Self>) -> Result<Map<String, Value>, Vec<(String, String)>> {
        let Some(form) = &self.form else {
            return Err(vec![(String::new(), "Load the form first".into())]);
        };
        let mut content = Map::new();
        let mut errors = Vec::new();
        for field in &form.fields {
            if !field.included {
                continue;
            }
            let value = match &field.spec.kind {
                Kind::Choice {
                    options,
                    multiple: true,
                } => Ok(Value::Array(
                    field
                        .selected
                        .iter()
                        .map(|i| options[*i].1.clone())
                        .collect(),
                )),
                Kind::Choice {
                    options,
                    multiple: false,
                } => field
                    .selected
                    .first()
                    .map(|i| options[*i].1.clone())
                    .ok_or("Choose a value"),
                kind => field
                    .input
                    .as_ref()
                    .ok_or("Enter a value")
                    .and_then(|input| text_value(kind, input.read(cx).value().as_ref())),
            };
            match value {
                Ok(value) => {
                    content.insert(field.spec.name.clone(), value);
                }
                Err(error) => errors.push((field.spec.name.clone(), error.into())),
            }
        }
        if errors.is_empty() {
            let value = Value::Object(content.clone());
            for error in form.validator.iter_errors(&value) {
                let name = error
                    .instance_path()
                    .to_string()
                    .strip_prefix('/')
                    .unwrap_or("")
                    .split('/')
                    .next()
                    .unwrap_or("")
                    .replace("~1", "/")
                    .replace("~0", "~");
                errors.push((name, "Does not match the requested constraints".into()));
            }
        }
        if errors.is_empty() {
            Ok(content)
        } else {
            Err(errors)
        }
    }

    fn decide(&mut self, decision: Option<Decision>, cx: &mut Context<Self>) {
        if !self.active || self.unavailable || self.request.is_some() || self.submitted {
            return;
        }
        if self.pending.is_none() {
            self.pending = match decision {
                Some(decision) => Some(decision),
                None => {
                    self.tried_accept = true;
                    match self.content(cx) {
                        Ok(content) => Some(Decision::Accept { content }),
                        Err(_) => {
                            cx.notify();
                            return;
                        }
                    }
                }
            };
        }
        let Some(decision) = self.pending.clone() else {
            return;
        };
        self.error = None;
        let request = ulid::Ulid::generate();
        self.request = Some(request);
        let (client, id, owner, updates) = (
            self.client.clone(),
            self.id.clone(),
            self.owner.clone(),
            self.updates.clone(),
        );
        self.runtime.spawn(async move {
            let result = client
                .decide_mcp_elicitation(
                    &owner,
                    &id.tool_call_id,
                    &id.generation,
                    id.ordinal,
                    &decision,
                )
                .await
                .map(|_| ())
                .map_err(|error| {
                    let status = match &error {
                        kiln_client::Error::Api { status, .. } => Some(*status),
                        _ => None,
                    };
                    (
                        connection::error_message("Submit form decision", &error),
                        status,
                    )
                });
            let _ = updates.send((request, Update::Decided(result)));
        });
        cx.notify();
    }
}

impl Render for ElicitationPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(form) = &mut self.form {
            for field in &mut form.fields {
                if field.input.is_none() && !matches!(field.spec.kind, Kind::Choice { .. }) {
                    let input = cx.new(|cx| InputState::new(window, cx));
                    self.subscriptions.push(cx.subscribe_in(
                        &input,
                        window,
                        |_, _, event, _, cx| {
                            if matches!(event, InputEvent::Change) {
                                cx.notify();
                            }
                        },
                    ));
                    field.input = Some(input);
                }
            }
        }
        let locked = !self.active
            || self.unavailable
            || self.request.is_some()
            || self.pending.is_some()
            || self.submitted;
        let errors = if self.tried_accept {
            self.content(cx).err().unwrap_or_default()
        } else {
            Vec::new()
        };
        let mut panel = div().flex().flex_col().gap_3().p_4().min_w_0().bg(theme::SURFACE).border_1().border_color(theme::BORDER).rounded_md()
            .child(div().text_lg().child("Server requests information"))
            .child(div().text_sm().text_color(theme::MUTED).child(self.provenance.clone()))
            .child(div().text_sm().text_color(theme::MUTED).child("Review this server-provided form. Only the fields you include are sent to the server."));
        if let Some(form) = &self.form {
            panel = panel
                .child(div().text_sm().child(format!("Server: {}", form.server)))
                .child(div().whitespace_normal().child(form.message.clone()));
            for (index, field) in form.fields.iter().enumerate() {
                let label = format!(
                    "{}{}",
                    field.spec.label,
                    if field.spec.required {
                        " (required)"
                    } else {
                        " (optional)"
                    }
                );
                let mut row = div()
                    .id(SharedString::from(format!("field-{index}")))
                    .role(gpui::Role::Group)
                    .aria_label(label.clone())
                    .flex()
                    .flex_col()
                    .gap_2()
                    .min_w_0()
                    .child(div().text_sm().child(label.clone()));
                if !field.spec.description.is_empty() {
                    row = row.child(
                        div()
                            .text_sm()
                            .whitespace_normal()
                            .text_color(theme::MUTED)
                            .child(field.spec.description.clone()),
                    );
                }
                if !field.spec.hint.is_empty() {
                    row = row.child(
                        div()
                            .text_sm()
                            .text_color(theme::MUTED)
                            .child(field.spec.hint.clone()),
                    );
                }
                if !field.spec.required {
                    row = row.child(
                        Button::new(SharedString::from(format!("include-{index}")))
                            .label(if field.included {
                                "Included"
                            } else {
                                "Include field"
                            })
                            .selected(field.included)
                            .disabled(locked)
                            .on_click(cx.listener(move |this, _, _, cx| {
                                if let Some(form) = &mut this.form {
                                    form.fields[index].included = !form.fields[index].included;
                                }
                                cx.notify();
                            })),
                    );
                }
                match &field.spec.kind {
                    Kind::Choice { options, multiple } => {
                        let multiple = *multiple;
                        let mut choices = div().flex().flex_wrap().gap_2();
                        for (option_index, (label, _)) in options.iter().enumerate() {
                            choices =
                                choices.child(
                                    Button::new(SharedString::from(format!(
                                        "choice-{index}-{option_index}"
                                    )))
                                    .label(label.clone())
                                    .selected(field.selected.contains(&option_index))
                                    .disabled(locked || !field.included)
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        if let Some(form) = &mut this.form {
                                            let selected = &mut form.fields[index].selected;
                                            if multiple && selected.remove(&option_index) {
                                            } else {
                                                if !multiple {
                                                    selected.clear();
                                                }
                                                selected.insert(option_index);
                                            }
                                        }
                                        cx.notify();
                                    })),
                                );
                        }
                        row = row.child(choices);
                    }
                    _ => {
                        if let Some(input) = &field.input {
                            row = row.child(
                                Input::new(input)
                                    .aria_label(label)
                                    .disabled(locked || !field.included),
                            );
                        }
                    }
                }
                for (_, error) in errors.iter().filter(|(name, _)| name == &field.spec.name) {
                    row = row.child(
                        div()
                            .text_sm()
                            .text_color(theme::DANGER)
                            .child(error.clone()),
                    );
                }
                panel = panel.child(row);
            }
        }
        for (_, error) in errors.iter().filter(|(name, _)| name.is_empty()) {
            panel = panel.child(
                div()
                    .text_sm()
                    .text_color(theme::DANGER)
                    .child(error.clone()),
            );
        }
        if let Some(error) = &self.error {
            panel = panel.child(
                div()
                    .id("form-error")
                    .role(gpui::Role::Alert)
                    .aria_label(error.clone())
                    .text_sm()
                    .text_color(theme::DANGER)
                    .child(error.clone()),
            );
        }
        if self.unavailable {
            return panel
                .child(div().child(
                    "This request no longer accepts a decision. Waiting for updated state.",
                ));
        }
        if self.submitted {
            return panel
                .child(div().child("Decision recorded. Waiting for the server to continue…"));
        }
        if self.pending.is_some() {
            return panel
                .child(div().text_sm().child(if self.request.is_some() {
                    "Submitting decision…"
                } else {
                    "The result is unconfirmed. Retry sends exactly the same decision."
                }))
                .child(
                    Button::new("retry-decision")
                        .label("Retry same decision")
                        .disabled(self.request.is_some())
                        .on_click(cx.listener(|this, _, _, cx| this.decide(None, cx))),
                );
        }
        if self.form.is_none() {
            panel = panel.child(
                Button::new("reload-form")
                    .label(if self.request.is_some() {
                        "Loading form…"
                    } else {
                        "Retry loading form"
                    })
                    .disabled(locked)
                    .on_click(cx.listener(|this, _, _, cx| this.load(cx))),
            );
        }
        panel.child(
            div()
                .flex()
                .flex_wrap()
                .gap_2()
                .child(
                    Button::new("accept-form")
                        .primary()
                        .label("Send response")
                        .disabled(locked || self.form.is_none())
                        .on_click(cx.listener(|this, _, _, cx| this.decide(None, cx))),
                )
                .child(
                    Button::new("decline-form")
                        .label("Decline")
                        .disabled(locked)
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.decide(Some(Decision::Decline {}), cx)
                        })),
                )
                .child(
                    Button::new("cancel-form")
                        .label("Cancel request")
                        .disabled(locked)
                        .on_click(
                            cx.listener(|this, _, _, cx| {
                                this.decide(Some(Decision::Cancel {}), cx)
                            }),
                        ),
                ),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn mcp_form_fields_preserve_choice_values_and_exact_numbers() {
        let schema = json!({"type":"object","properties":{
            "plain":{"type":"string","default":"do not send this"},
            "one":{"type":"string","oneOf":[{"const":"wire-a","title":"Display A"}]},
            "many":{"type":"array","items":{"anyOf":[{"const":"wire-b","title":"Display B"}]}},
            "legacy":{"type":"string","enum":["wire-c"],"enumNames":["Display C"]},
            "flag":{"type":"boolean"}},"required":["one"]});
        let fields = field_specs(&schema).unwrap();
        assert!(
            fields
                .iter()
                .find(|field| field.name == "one")
                .unwrap()
                .required
        );
        assert!(
            !fields
                .iter()
                .find(|field| field.name == "plain")
                .unwrap()
                .required
        );
        for (name, value, multiple) in [
            ("one", "wire-a", false),
            ("many", "wire-b", true),
            ("legacy", "wire-c", false),
        ] {
            let field = fields.iter().find(|field| field.name == name).unwrap();
            let Kind::Choice {
                options,
                multiple: actual,
            } = &field.kind
            else {
                panic!("choice required")
            };
            assert_eq!(*actual, multiple);
            assert_eq!(options[0].1, json!(value));
        }
        assert_eq!(
            text_value(&Kind::Integer, "9007199254740993").unwrap(),
            json!(9007199254740993_u64)
        );
        assert_eq!(text_value(&Kind::Text, "").unwrap(), json!(""));
        for invalid in ["", "2.5", "18446744073709551616", "NaN"] {
            assert!(text_value(&Kind::Integer, invalid).is_err());
        }
        assert!(text_value(&Kind::Number, "1e999").is_err());
        assert!(
            field_specs(&json!({"type":"object","properties":{"secret":{"type":"object"}}}))
                .is_err()
        );
    }
}
