//! Version-checked host-local account mappings. Credentials never enter this view.

use std::sync::Arc;

use gpui::{Context, Entity, Render, SharedString, Window, div, prelude::*};
use gpui_component::{
    Disableable,
    button::{Button, ButtonVariants},
    input::{Input, InputState},
};
use kiln_client::Client;
use kiln_protocol::{
    ListModelAccountBindingsResponse, ModelAccountBindingResponse, ProviderAccountResponse,
    RemoveModelAccountBindingRequest, SetModelAccountBindingRequest,
};
use tokio::{runtime::Runtime, sync::mpsc};
use ulid::Ulid;

use crate::{connection, theme};

enum Update {
    Page(Result<ListModelAccountBindingsResponse, String>),
    Loaded(Result<(ModelAccountBindingResponse, Vec<ProviderAccountResponse>), String>),
    Saved(Result<ModelAccountBindingResponse, String>),
}

#[derive(Clone)]
enum Change {
    Set(SetModelAccountBindingRequest),
    Remove(RemoveModelAccountBindingRequest),
}

pub struct ModelBindingSettings {
    client: Client,
    runtime: Arc<Runtime>,
    updates: mpsc::UnboundedSender<(Ulid, Update)>,
    request: Option<Ulid>,
    online: bool,
    input: Option<Entity<InputState>>,
    page: Vec<ModelAccountBindingResponse>,
    next_cursor: Option<String>,
    binding: Option<ModelAccountBindingResponse>,
    accounts: Vec<ProviderAccountResponse>,
    confirmation: Option<Change>,
    error: Option<String>,
    notice: Option<String>,
}

impl ModelBindingSettings {
    pub fn new(client: Client, runtime: Arc<Runtime>, cx: &mut Context<Self>) -> Self {
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
        Self {
            client,
            runtime,
            updates,
            request: None,
            online: true,
            input: None,
            page: Vec::new(),
            next_cursor: None,
            binding: None,
            accounts: Vec::new(),
            confirmation: None,
            error: None,
            notice: None,
        }
    }

    pub fn set_online(&mut self, online: bool, cx: &mut Context<Self>) {
        if self.online != online {
            self.online = online;
            // An accepted write can finish after disconnect. Never infer its outcome
            // or reuse the old version; the user must read again on reconnect.
            self.request = None;
            self.binding = None;
            self.accounts.clear();
            self.confirmation = None;
            self.page.clear();
            self.next_cursor = None;
            self.notice = None;
            self.error = None;
        }
        cx.notify();
    }

    fn begin(&mut self, cx: &mut Context<Self>) -> Option<Ulid> {
        if !self.online || self.request.is_some() {
            return None;
        }
        let id = Ulid::generate();
        self.request = Some(id);
        self.confirmation = None;
        self.error = None;
        self.notice = None;
        cx.notify();
        Some(id)
    }

    fn list(&mut self, next: bool, cx: &mut Context<Self>) {
        let after = if next { self.next_cursor.clone() } else { None };
        if next && after.is_none() {
            return;
        }
        let Some(id) = self.begin(cx) else {
            return;
        };
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let result = client
                .list_model_account_bindings(None, after.as_deref())
                .await
                .map_err(|e| connection::error_message("Load model account bindings", &e));
            let _ = updates.send((id, Update::Page(result)));
        });
    }

    fn load(&mut self, key: String, cx: &mut Context<Self>) {
        let Some(id) = self.begin(cx) else {
            return;
        };
        self.binding = None;
        self.accounts.clear();
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let result = async {
                let binding = client.get_model_account_binding(&key).await?;
                let accounts = client.list_provider_accounts().await?.provider_accounts;
                Ok::<_, kiln_client::Error>((binding, accounts))
            }
            .await
            .map_err(|e| connection::error_message("Load binding and accounts", &e));
            let _ = updates.send((id, Update::Loaded(result)));
        });
    }

    fn save(&mut self, cx: &mut Context<Self>) {
        let Some(change) = self.confirmation.clone() else {
            return;
        };
        let Some(id) = self.begin(cx) else {
            return;
        };
        // Consuming the read also prevents blind retries on conflict or lost replies.
        self.binding = None;
        self.accounts.clear();
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let result = match change {
                Change::Set(request) => client.set_model_account_binding(&request).await,
                Change::Remove(request) => client.remove_model_account_binding(&request).await,
            }
            .map_err(|e| {
                format!(
                    "{} Load the binding again before making another change.",
                    connection::error_message("Save model account binding", &e)
                )
            });
            let _ = updates.send((id, Update::Saved(result)));
        });
    }

    fn apply(&mut self, request: Ulid, update: Update, cx: &mut Context<Self>) {
        if !self.online || self.request != Some(request) {
            return;
        }
        self.request = None;
        match update {
            Update::Page(Ok(page)) => {
                self.page = page.bindings;
                self.next_cursor = page.next_cursor;
                if self.page.is_empty() {
                    self.notice = Some("No bindings on this page.".into());
                }
            }
            Update::Loaded(Ok((binding, accounts))) => {
                self.binding = Some(binding);
                self.accounts = accounts;
            }
            Update::Saved(Ok(binding)) => {
                self.page.clear();
                self.next_cursor = None;
                self.notice = Some(format!(
                    "Saved {} at version {}. Existing Runs keep their model selection. Load the binding to edit again.",
                    binding.binding_key, binding.version
                ));
            }
            Update::Page(Err(error)) | Update::Loaded(Err(error)) | Update::Saved(Err(error)) => {
                self.error = Some(error);
            }
        }
        cx.notify();
    }
}

impl Render for ModelBindingSettings {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let input = self
            .input
            .get_or_insert_with(|| {
                cx.new(|cx| {
                    InputState::new(window, cx)
                        .placeholder("Account binding key from shared settings")
                })
            })
            .clone();
        let disabled = !self.online || self.request.is_some();
        let mut view = div().flex().flex_col().gap_3().min_w_0()
            .child(div().text_lg().child("Model account bindings"))
            .child(div().text_sm().text_color(theme::MUTED).child("Map a shared model’s account key to an account on this host. Credentials and Workspace access stay local. A connected account must also be enabled for the Run’s Workspace."))
            .when(!self.online, |v| v.child(div().text_sm().text_color(theme::ATTENTION).child("Reconnect, then load the binding again before editing.")))
            .child(div().text_sm().child("Shared account binding key"))
            .child(Input::new(&input).aria_label("Shared account binding key").disabled(disabled))
            .child(div().flex().flex_wrap().gap_2()
                .child(Button::new("load-model-binding").label(if self.request.is_some() { "Working…" } else { "Load binding" }).disabled(disabled)
                    .on_click(cx.listener(|this, _, _, cx| {
                        if let Some(input) = &this.input {
                            let key = input.read(cx).value().to_string();
                            this.load(key, cx);
                        }
                    })))
                .child(Button::new("list-model-bindings").label("Browse bindings").disabled(disabled)
                    .on_click(cx.listener(|this, _, _, cx| this.list(false, cx))))
                .when(self.next_cursor.is_some(), |v| v.child(Button::new("next-model-bindings").label("Next page").disabled(disabled)
                    .on_click(cx.listener(|this, _, _, cx| this.list(true, cx))))))
            .when_some(self.error.clone(), |v, error| v.child(div().id("model-binding-error").role(gpui::Role::Alert).aria_label(error.clone()).text_sm().text_color(theme::DANGER).child(error)))
            .when_some(self.notice.clone(), |v, notice| v.child(div().text_sm().child(notice)));
        for binding in &self.page {
            let key = binding.binding_key.clone();
            view = view.child(
                Button::new(SharedString::from(format!("binding-{}", key)))
                    .label(key.clone())
                    .disabled(disabled)
                    .on_click(cx.listener(move |this, _, window, cx| {
                        if let Some(input) = &this.input {
                            input.update(cx, |input, cx| input.set_value(key.clone(), window, cx));
                        }
                        this.load(key.clone(), cx);
                    })),
            );
        }
        if let Some(binding) = &self.binding {
            view = view
                .child(div().text_sm().child(format!(
                    "Loaded key: {} · version {}",
                    binding.binding_key, binding.version
                )))
                .child(div().text_sm().child(match &binding.provider_account_id {
                    Some(id) => format!(
                            "Current account: {} · {} · {} · {}",
                            binding
                                .provider_account_label
                                .as_deref()
                                .unwrap_or("Unknown"),
                            binding
                                .provider_type
                                .as_deref()
                                .unwrap_or("Unknown provider"),
                            binding
                                .provider_account_state
                                .as_deref()
                                .unwrap_or("Unknown state"),
                            id
                        ),
                    None => "No account mapped to this key.".into(),
                }));
            for account in &self.accounts {
                let request = SetModelAccountBindingRequest {
                    binding_key: binding.binding_key.clone(),
                    expected_version: binding.version,
                    expected_provider_type: account.provider_type.clone(),
                    provider_account_id: account.provider_account_id.clone(),
                };
                view = view.child(
                    Button::new(SharedString::from(format!(
                        "bind-{}",
                        account.provider_account_id
                    )))
                    .label(format!(
                        "Use {} · {} · {}",
                        account.label, account.provider_type, account.state
                    ))
                    .disabled(
                        disabled || account.state != "connected" || self.confirmation.is_some(),
                    )
                    .on_click(cx.listener(move |this, _, _, cx| {
                        if this.online && this.request.is_none() {
                            this.confirmation = Some(Change::Set(request.clone()));
                            cx.notify();
                        }
                    })),
                );
            }
            if self.accounts.is_empty() {
                view = view.child(
                    div()
                        .text_sm()
                        .text_color(theme::MUTED)
                        .child("No local accounts. Sign in below, then load this binding again."),
                );
            }
            if binding.provider_account_id.is_some() {
                let request = RemoveModelAccountBindingRequest {
                    binding_key: binding.binding_key.clone(),
                    expected_version: binding.version,
                };
                view = view.child(
                    Button::new("remove-model-binding")
                        .label("Remove mapping")
                        .disabled(disabled || self.confirmation.is_some())
                        .on_click(cx.listener(move |this, _, _, cx| {
                            if this.online && this.request.is_none() {
                                this.confirmation = Some(Change::Remove(request.clone()));
                                cx.notify();
                            }
                        })),
                );
            }
        }
        if let Some(change) = &self.confirmation {
            let description = match change {
                Change::Set(request) => format!(
                    "Map {} to {} ({})? This changes this host’s account choice for new Runs using this key.",
                    request.binding_key,
                    request.provider_account_id,
                    request.expected_provider_type
                ),
                Change::Remove(request) => format!(
                    "Remove the local mapping for {}? New Runs requiring this key will be unavailable until an account is mapped again.",
                    request.binding_key
                ),
            };
            view = view
                .child(
                    div()
                        .text_sm()
                        .text_color(theme::ATTENTION)
                        .child(description),
                )
                .child(
                    div()
                        .flex()
                        .flex_wrap()
                        .gap_2()
                        .child(
                            Button::new("confirm-model-binding")
                                .label("Confirm change")
                                .primary()
                                .disabled(disabled)
                                .on_click(cx.listener(|this, _, _, cx| this.save(cx))),
                        )
                        .child(
                            Button::new("cancel-model-binding")
                                .label("Cancel")
                                .disabled(disabled)
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.confirmation = None;
                                    cx.notify();
                                })),
                        ),
                );
        }
        view
    }
}
