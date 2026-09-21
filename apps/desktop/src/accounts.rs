//! Global provider settings. Only public protocol data enters the desktop.

use std::sync::Arc;

use gpui::{ClipboardItem, Context, Render, SharedString, Window, div, prelude::*};
use gpui_component::{
    Disableable,
    button::{Button, ButtonVariants},
};
use kiln_client::Client;
use kiln_protocol::{
    CreateProviderAccountRequest, ProviderAccountLoginResponse, ProviderAccountLoginState,
    ProviderAccountResponse, StartProviderAccountLoginResponse,
};
use tokio::{runtime::Runtime, sync::mpsc};

use crate::{connection, theme};

const CODEX_PROVIDER: &str = "openai_codex_subscription";
// The desktop opens only the device verification page defined by the Codex contract.
const VERIFICATION_URL: &str = "https://auth.openai.com/codex/device";

enum Update {
    Listed(Result<Vec<ProviderAccountResponse>, String>),
    Created(Result<ProviderAccountResponse, String>),
    Started(Result<StartProviderAccountLoginResponse, String>),
    Status {
        result: Result<ProviderAccountLoginResponse, String>,
        inactive: bool,
    },
}

pub struct AccountSettings {
    client: Client,
    runtime: Arc<Runtime>,
    updates: mpsc::UnboundedSender<Update>,
    accounts: Vec<ProviderAccountResponse>,
    loaded: bool,
    busy: bool,
    online: bool,
    error: Option<String>,
    // Retained across failed requests so a lost create response cannot create another account.
    create_key: String,
    login: Option<StartProviderAccountLoginResponse>,
    login_state: Option<ProviderAccountLoginState>,
    copied: bool,
}

impl AccountSettings {
    pub fn new(client: Client, runtime: Arc<Runtime>, cx: &mut Context<Self>) -> Self {
        let (updates, mut receiver) = mpsc::unbounded_channel();
        cx.spawn(async move |this, cx| {
            while let Some(update) = receiver.recv().await {
                if this.update(cx, |this, cx| this.apply(update, cx)).is_err() {
                    break;
                }
            }
        })
        .detach();
        let mut settings = Self {
            client,
            runtime,
            updates,
            accounts: Vec::new(),
            loaded: false,
            busy: false,
            online: true,
            error: None,
            create_key: ulid::Ulid::generate().to_string(),
            login: None,
            login_state: None,
            copied: false,
        };
        settings.refresh(cx);
        settings
    }

    pub fn set_online(&mut self, online: bool, cx: &mut Context<Self>) {
        self.online = online;
        cx.notify();
    }

    fn refresh(&mut self, cx: &mut Context<Self>) {
        if self.busy || !self.online {
            return;
        }
        self.busy = true;
        self.error = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let result = client
                .list_provider_accounts()
                .await
                .map(|response| response.provider_accounts)
                .map_err(|error| connection::error_message("Load provider accounts", &error));
            let _ = updates.send(Update::Listed(result));
        });
        cx.notify();
    }

    fn sign_in(&mut self, cx: &mut Context<Self>) {
        if self.busy || !self.online || !self.loaded || self.sign_in_blocked() {
            return;
        }
        if let Some(account) = self
            .accounts
            .iter()
            .find(|account| {
                account.provider_type == CODEX_PROVIDER
                    && matches!(account.state.as_str(), "connecting" | "disconnected")
            })
            .cloned()
        {
            self.begin(account, cx);
            return;
        }
        self.busy = true;
        self.error = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        let key = self.create_key.clone();
        self.runtime.spawn(async move {
            let result = client
                .create_provider_account(
                    &key,
                    &CreateProviderAccountRequest {
                        provider_type: CODEX_PROVIDER.to_owned(),
                        label: "Codex subscription".to_owned(),
                        workspace_ids: Vec::new(),
                    },
                )
                .await
                .map_err(|error| connection::error_message("Create Codex account", &error));
            let _ = updates.send(Update::Created(result));
        });
        cx.notify();
    }

    fn begin(&mut self, account: ProviderAccountResponse, cx: &mut Context<Self>) {
        self.busy = true;
        self.error = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let result = client
                .start_provider_account_login(&account.provider_account_id)
                .await
                .map_err(|error| connection::error_message("Start Codex sign-in", &error));
            let _ = updates.send(Update::Started(result));
        });
        cx.notify();
    }

    fn check_login(&mut self, cancel: bool, cx: &mut Context<Self>) {
        if self.busy || !self.online {
            return;
        }
        let Some(login) = &self.login else {
            return;
        };
        let account_id = login.account.provider_account_id.clone();
        let attempt_id = login.attempt_id.clone();
        self.busy = true;
        self.error = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let response = if cancel {
                client
                    .cancel_provider_account_login(&account_id, &attempt_id)
                    .await
            } else {
                client
                    .get_provider_account_login(&account_id, &attempt_id)
                    .await
            };
            let inactive = matches!(&response, Err(kiln_client::Error::Api { problem, .. })
                if problem.code == kiln_protocol::error_code::PROVIDER_ACCOUNT_LOGIN_NOT_FOUND);
            let result = response.map_err(|error| {
                connection::error_message(
                    if cancel {
                        "Cancel sign-in"
                    } else {
                        "Check sign-in"
                    },
                    &error,
                )
            });
            let _ = updates.send(Update::Status { result, inactive });
        });
        cx.notify();
    }

    fn upsert(&mut self, account: ProviderAccountResponse) {
        if let Some(previous) = self
            .accounts
            .iter_mut()
            .find(|item| item.provider_account_id == account.provider_account_id)
        {
            *previous = account;
        } else {
            self.accounts.push(account);
        }
    }

    fn apply(&mut self, update: Update, cx: &mut Context<Self>) {
        self.busy = false;
        match update {
            Update::Listed(Ok(accounts)) => {
                self.accounts = accounts;
                self.loaded = true;
            }
            Update::Created(Ok(account)) => {
                self.create_key = ulid::Ulid::generate().to_string();
                self.upsert(account.clone());
                // A disconnect may occur while creation is in flight. Do not start new work.
                if self.online && matches!(account.state.as_str(), "connecting" | "disconnected") {
                    self.begin(account, cx);
                }
            }
            Update::Started(Ok(login)) => {
                self.upsert(login.account.clone());
                self.login_state = Some(ProviderAccountLoginState::Pending);
                self.copied = false;
                if login.verification_url != VERIFICATION_URL {
                    self.error = Some("The daemon returned an unexpected sign-in address. Cancel this attempt and reconnect.".to_owned());
                }
                self.login = Some(login);
            }
            Update::Status {
                result: Ok(response),
                ..
            } => {
                if self.login.as_ref().is_some_and(|login| {
                    login.attempt_id == response.attempt_id
                        && login.account.provider_account_id == response.account.provider_account_id
                }) {
                    self.upsert(response.account);
                    self.login_state = Some(response.state);
                }
            }
            Update::Status {
                result: Err(_),
                inactive: true,
            } => {
                self.login = None;
                self.login_state = None;
                self.loaded = false;
                self.error = Some("This sign-in attempt is no longer active. Refresh accounts before starting another attempt.".to_owned());
            }
            Update::Listed(Err(error))
            | Update::Created(Err(error))
            | Update::Started(Err(error))
            | Update::Status {
                result: Err(error), ..
            } => {
                self.error = Some(error);
            }
        }
        cx.notify();
    }

    fn sign_in_blocked(&self) -> bool {
        matches!(
            self.login_state,
            Some(ProviderAccountLoginState::Pending | ProviderAccountLoginState::CleanupRequired)
        ) || self.accounts.iter().any(|account| {
            account.provider_type == CODEX_PROVIDER
                && matches!(account.state.as_str(), "connected" | "reauth_required")
        })
    }
}

impl Render for AccountSettings {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let disabled = self.busy || !self.online;
        let mut content = div().flex().flex_col().gap_4().w_full().min_w_0().max_w(theme::TRANSCRIPT_WIDTH)
            .child(div().text_lg().child("Provider accounts"))
            .child(div().text_sm().text_color(theme::MUTED)
                .child("Connect your Codex subscription using device sign-in. OpenAI account terms and data controls apply."))
            .child(div().text_sm().text_color(theme::MUTED)
                .child("Account connection is available. Runs still use the deterministic executor; live model execution is not yet available."))
            .when(!self.online, |view| view.child(div().text_sm().text_color(theme::ATTENTION)
                .child("Reconnect to the daemon to manage accounts. Signing in continues in the daemon.")))
            .when_some(self.error.clone(), |view, error| view.child(div()
                .id("provider-account-error").role(gpui::Role::Alert).aria_label(error.clone())
                .text_sm().text_color(theme::DANGER).child(error)))
            .child(div().flex().flex_wrap().gap_2()
                .child(Button::new("refresh-accounts").label(if self.busy { "Working…" } else { "Refresh accounts" })
                    .disabled(disabled).on_click(cx.listener(|this, _, _, cx| this.refresh(cx))))
                .child(Button::new("connect-codex").label("Sign in to Codex").primary()
                    .disabled(disabled || !self.loaded || self.sign_in_blocked())
                    .on_click(cx.listener(|this, _, _, cx| this.sign_in(cx)))));
        if self.loaded && self.accounts.is_empty() {
            content = content.child(
                div()
                    .text_sm()
                    .text_color(theme::MUTED)
                    .child("No provider accounts connected."),
            );
        }
        for account in &self.accounts {
            let provider = match account.provider_type.as_str() {
                CODEX_PROVIDER => "Codex subscription",
                "openai_api" => "OpenAI API",
                other => other,
            };
            let state = match account.state.as_str() {
                "connecting" => "Not signed in",
                "connected" => "Connected",
                "reauth_required" => {
                    "Sign-in expired — account recovery is not yet available in the desktop"
                }
                "disconnected" => "Disconnected",
                _ => "Unknown account state",
            };
            content = content.child(
                div()
                    .id(SharedString::from(account.provider_account_id.clone()))
                    .border_b_1()
                    .border_color(theme::BORDER)
                    .py_3()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .child(div().text_sm().child(account.label.clone()))
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme::MUTED)
                            .child(format!("{provider} · {state}")),
                    ),
            );
        }
        if let Some(login) = &self.login {
            let message = match self.login_state {
                Some(ProviderAccountLoginState::Connected) => "Codex account connected.",
                Some(ProviderAccountLoginState::Cancelled) => "Sign-in cancelled.",
                Some(ProviderAccountLoginState::Failed) => {
                    "Sign-in failed or expired. You can start a new attempt."
                }
                Some(ProviderAccountLoginState::CleanupRequired) => {
                    "Credential cleanup is required in the daemon. Do not start another sign-in until it is resolved."
                }
                _ => "Open the verification page, enter this code, then check sign-in status.",
            };
            let mut progress = div()
                .flex()
                .flex_col()
                .gap_3()
                .py_3()
                .child(div().text_sm().child(message));
            if self.login_state == Some(ProviderAccountLoginState::Pending) {
                let url_allowed = login.verification_url == VERIFICATION_URL;
                progress = progress
                    .child(
                        div()
                            .font_family(theme::MONO_FONT)
                            .text_lg()
                            .child(login.user_code.clone()),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme::MUTED)
                            .child(VERIFICATION_URL),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_wrap()
                            .gap_2()
                            .child(
                                Button::new("open-codex-login")
                                    .label("Open verification page")
                                    .disabled(!self.online || !url_allowed)
                                    .on_click(
                                        cx.listener(|_, _, _, cx| cx.open_url(VERIFICATION_URL)),
                                    ),
                            )
                            .child(
                                Button::new("copy-codex-code")
                                    .label(if self.copied {
                                        "Code copied"
                                    } else {
                                        "Copy code"
                                    })
                                    .disabled(!self.online || !url_allowed)
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        if let Some(login) = &this.login {
                                            cx.write_to_clipboard(ClipboardItem::new_string(
                                                login.user_code.clone(),
                                            ));
                                            this.copied = true;
                                            cx.notify();
                                        }
                                    })),
                            )
                            .child(
                                Button::new("check-codex-login")
                                    .label("Check sign-in")
                                    .disabled(disabled)
                                    .on_click(
                                        cx.listener(|this, _, _, cx| this.check_login(false, cx)),
                                    ),
                            )
                            .child(
                                Button::new("cancel-codex-login")
                                    .label("Cancel sign-in")
                                    .disabled(disabled)
                                    .on_click(
                                        cx.listener(|this, _, _, cx| this.check_login(true, cx)),
                                    ),
                            ),
                    );
            }
            content = content.child(progress);
        }
        div()
            .id("account-settings-scroll")
            .flex_1()
            .min_h_0()
            .overflow_y_scroll()
            .flex()
            .justify_center()
            .px_6()
            .py_6()
            .child(content)
    }
}
