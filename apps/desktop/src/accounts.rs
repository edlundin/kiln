//! Global provider settings. Only public protocol data enters the desktop.

use std::{collections::HashSet, sync::Arc};

use gpui::{ClipboardItem, Context, Entity, Render, SharedString, Window, div, prelude::*};
use gpui_component::{
    Disableable,
    button::{Button, ButtonVariants},
};
use kiln_client::Client;
use kiln_protocol::{
    CreateProviderAccountRequest, ProviderAccountLoginResponse, ProviderAccountLoginState,
    ProviderAccountResponse,
};
use tokio::{runtime::Runtime, sync::mpsc};

use crate::{
    configuration_sync::ConfigurationSyncSettings, connection,
    enrollment_requests::EnrollmentRequests,
    model_bindings::ModelBindingSettings, theme,
};

const CODEX_PROVIDER: &str = "openai_codex_subscription";
// Browser and device destinations are validated before opening.
const VERIFICATION_URL: &str = "https://auth.openai.com/codex/device";

// No Debug: browser URLs contain short-lived OAuth state.
struct Login {
    attempt_id: String,
    account: ProviderAccountResponse,
    url: String,
    user_code: Option<String>,
}

impl Login {
    fn url_allowed(&self) -> bool {
        if self.user_code.is_some() {
            return self.url == VERIFICATION_URL;
        }
        browser_url_allowed(&self.url)
    }
}

fn browser_url_allowed(value: &str) -> bool {
    let Ok(url) = url::Url::parse(value) else {
        return false;
    };
    if url.scheme() != "https"
        || url.host_str() != Some("auth.openai.com")
        || url.port().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/oauth/authorize"
        || url.fragment().is_some()
    {
        return false;
    }
    let mut pairs = std::collections::HashMap::new();
    for (key, value) in url.query_pairs() {
        if pairs.insert(key.into_owned(), value.into_owned()).is_some() {
            return false;
        }
    }
    let expected = [
        ("response_type", "code"),
        ("client_id", "app_EMoamEEZ73f0CkXaXp7hrann"),
        ("code_challenge_method", "S256"),
        ("scope", "openid profile email offline_access"),
        ("id_token_add_organizations", "true"),
        ("codex_cli_simplified_flow", "true"),
        ("originator", "kiln"),
    ];
    for (key, value) in expected {
        if pairs.remove(key).as_deref() != Some(value) {
            return false;
        }
    }
    if !matches!(
        pairs.remove("redirect_uri").as_deref(),
        Some("http://localhost:1455/auth/callback" | "http://localhost:1457/auth/callback")
    ) {
        return false;
    }
    for key in ["state", "code_challenge"] {
        let Some(value) = pairs.remove(key) else {
            return false;
        };
        if value.len() != 43
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return false;
        }
    }
    pairs.is_empty()
}

enum Update {
    Listed(Result<Vec<ProviderAccountResponse>, String>),
    Created(Result<ProviderAccountResponse, String>, bool),
    Started {
        account_id: String,
        result: Result<Login, String>,
        cleanup_required: bool,
    },
    Disconnected {
        account_id: String,
        result: Result<ProviderAccountResponse, String>,
    },
    Status {
        result: Result<ProviderAccountLoginResponse, String>,
        inactive: bool,
    },
}

pub struct AccountSettings {
    configuration_sync: Entity<ConfigurationSyncSettings>,
    model_bindings: Entity<ModelBindingSettings>,
    enrollment_requests: Entity<EnrollmentRequests>,
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
    login: Option<Login>,
    login_state: Option<ProviderAccountLoginState>,
    copied: bool,
    confirm_disconnect: Option<String>,
    cleanup_accounts: HashSet<String>,
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
            enrollment_requests: cx
                .new(|cx| EnrollmentRequests::new(client.clone(), runtime.clone(), cx)),
            model_bindings: cx
                .new(|cx| ModelBindingSettings::new(client.clone(), runtime.clone(), cx)),
            configuration_sync: cx
                .new(|cx| ConfigurationSyncSettings::new(client.clone(), runtime.clone(), cx)),
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
            confirm_disconnect: None,
            cleanup_accounts: HashSet::new(),
        };
        settings.refresh(cx);
        settings
    }

    pub fn set_online(&mut self, online: bool, cx: &mut Context<Self>) {
        self.online = online;
        self.enrollment_requests
            .update(cx, |settings, cx| settings.set_online(online, cx));
        self.model_bindings
            .update(cx, |settings, cx| settings.set_online(online, cx));
        self.configuration_sync
            .update(cx, |settings, cx| settings.set_online(online, cx));
        if !online {
            self.confirm_disconnect = None;
        }
        cx.notify();
    }

    fn refresh(&mut self, cx: &mut Context<Self>) {
        if self.busy || !self.online {
            return;
        }
        self.busy = true;
        self.confirm_disconnect = None;
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

    fn sign_in(&mut self, browser: bool, cx: &mut Context<Self>) {
        if self.busy
            || !self.online
            || !self.loaded
            || self.sign_in_blocked()
            || self.confirm_disconnect.is_some()
        {
            return;
        }
        if let Some(account) = self
            .accounts
            .iter()
            .filter(|account| {
                account.provider_type == CODEX_PROVIDER
                    && matches!(account.state.as_str(), "connecting" | "disconnected")
            })
            .min_by_key(|account| account.state != "connecting")
            .cloned()
        {
            self.begin(account, browser, cx);
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
            let _ = updates.send(Update::Created(result, browser));
        });
        cx.notify();
    }

    fn begin(&mut self, account: ProviderAccountResponse, browser: bool, cx: &mut Context<Self>) {
        self.busy = true;
        self.error = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let response = if browser {
                client
                    .start_provider_account_browser_login(&account.provider_account_id)
                    .await
                    .map(|login| Login {
                        attempt_id: login.attempt_id,
                        account: login.account,
                        url: login.authorization_url,
                        user_code: None,
                    })
            } else {
                client
                    .start_provider_account_login(&account.provider_account_id)
                    .await
                    .map(|login| Login {
                        attempt_id: login.attempt_id,
                        account: login.account,
                        url: login.verification_url,
                        user_code: Some(login.user_code),
                    })
            };
            let cleanup_required = matches!(&response, Err(kiln_client::Error::Api { problem, .. })
                if problem.code == kiln_protocol::error_code::PROVIDER_ACCOUNT_CLEANUP_REQUIRED);
            let result = response.map_err(|error| {
                let message = connection::error_message("Start Codex sign-in", &error);
                if browser {
                    format!(
                        "{message} You can use device sign-in if the local callback is unavailable."
                    )
                } else {
                    message
                }
            });
            let _ = updates.send(Update::Started {
                account_id: account.provider_account_id,
                result,
                cleanup_required,
            });
        });
        cx.notify();
    }

    fn disconnect(&mut self, account_id: String, cx: &mut Context<Self>) {
        if self.busy || !self.online || self.confirm_disconnect.as_ref() != Some(&account_id) {
            return;
        }
        self.busy = true;
        self.error = None;
        let client = self.client.clone();
        let updates = self.updates.clone();
        self.runtime.spawn(async move {
            let result = client
                .disconnect_provider_account(&account_id)
                .await
                .map_err(|error| connection::error_message("Disconnect account", &error));
            let _ = updates.send(Update::Disconnected { account_id, result });
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
            Update::Disconnected { account_id, result } => {
                // The daemon cancels sign-in before disconnecting. Even an error
                // can mean partial progress; stop presenting the old user code.
                if self
                    .login
                    .as_ref()
                    .is_some_and(|login| login.account.provider_account_id == account_id)
                {
                    self.login = None;
                    self.login_state = None;
                }
                match result {
                    Ok(account) => {
                        self.cleanup_accounts.remove(&account_id);
                        self.upsert(account);
                        self.confirm_disconnect = None;
                        self.loaded = false;
                        self.refresh(cx);
                    }
                    Err(error) => {
                        // Keep retry available even if refresh reports the account
                        // disconnected while an unpublished entry still needs cleanup.
                        self.cleanup_accounts.insert(account_id);
                        self.loaded = false;
                        self.error = Some(format!(
                            "{error} Refresh accounts to check the current state, or retry disconnect."
                        ));
                    }
                }
            }
            Update::Listed(Ok(accounts)) => {
                self.accounts = accounts;
                self.loaded = true;
            }
            Update::Created(Ok(account), browser) => {
                self.create_key = ulid::Ulid::generate().to_string();
                self.upsert(account.clone());
                // A disconnect may occur while creation is in flight. Do not start new work.
                if self.online && matches!(account.state.as_str(), "connecting" | "disconnected") {
                    self.begin(account, browser, cx);
                }
            }
            Update::Started {
                result: Ok(login), ..
            } => {
                self.upsert(login.account.clone());
                self.login_state = Some(ProviderAccountLoginState::Pending);
                self.copied = false;
                if !login.url_allowed() {
                    self.error = Some("The daemon returned an unexpected sign-in address. Cancel this attempt and reconnect.".to_owned());
                }
                self.login = Some(login);
            }
            Update::Started {
                account_id,
                result: Err(error),
                cleanup_required,
            } => {
                if cleanup_required {
                    self.cleanup_accounts.insert(account_id);
                }
                self.error = Some(error);
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
            | Update::Created(Err(error), _)
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
                && (matches!(account.state.as_str(), "connected" | "reauth_required")
                    || self.cleanup_accounts.contains(&account.provider_account_id))
        })
    }
}

impl Render for AccountSettings {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let disabled = self.busy || !self.online;
        let mut content = div().flex().flex_col().gap_4().w_full().min_w_0().max_w(theme::TRANSCRIPT_WIDTH)
            .child(self.configuration_sync.clone())
            .child(self.enrollment_requests.clone())
            .child(self.model_bindings.clone())
            .child(div().text_lg().child("Provider accounts"))
            .child(div().text_sm().text_color(theme::MUTED)
                .child("Connect your Codex subscription in a browser on this daemon’s host. Use device sign-in for a remote host or if the callback is unavailable. OpenAI account terms and data controls apply."))
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
                .child(Button::new("connect-codex").label("Sign in with browser").primary()
                    .disabled(disabled || !self.loaded || self.sign_in_blocked() || self.confirm_disconnect.is_some())
                    .on_click(cx.listener(|this, _, _, cx| this.sign_in(true, cx))))
                .child(Button::new("connect-codex-device").label("Use device sign-in")
                    .disabled(disabled || !self.loaded || self.sign_in_blocked() || self.confirm_disconnect.is_some())
                    .on_click(cx.listener(|this, _, _, cx| this.sign_in(false, cx)))));
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
                "connected" if account.provider_type == "openai_api" => {
                    "Credentials saved — provider access not verified"
                }
                "connected" => "Connected",
                "reauth_required" => {
                    "Sign-in expired or disconnect incomplete — disconnect, then sign in again"
                }
                "disconnected" => "Disconnected",
                _ => "Unknown account state",
            };
            let account_id = account.provider_account_id.clone();
            let confirm = self.confirm_disconnect.as_ref() == Some(&account_id);
            let cleanup_pending = self.login_state
                == Some(ProviderAccountLoginState::CleanupRequired)
                && self
                    .login
                    .as_ref()
                    .is_some_and(|login| login.account.provider_account_id == account_id);
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
                    )
                    .when(account.state != "disconnected" || confirm || cleanup_pending || self.cleanup_accounts.contains(&account_id), |view| {
                        if confirm {
                            let target = account_id.clone();
                            view.child(div().text_sm().text_color(theme::ATTENTION)
                                .child("Disconnect this account? This cancels sign-in and removes credentials from this Kiln instance. It does not revoke access at OpenAI."))
                                .child(div().flex().flex_wrap().gap_2()
                                    .child(Button::new("confirm-account-disconnect").label("Confirm disconnect")
                                        .disabled(disabled).on_click(cx.listener(move |this, _, _, cx| this.disconnect(target.clone(), cx))))
                                    .child(Button::new("cancel-account-disconnect").label("Keep account")
                                        .disabled(disabled).on_click(cx.listener(|this, _, _, cx| {
                                            this.confirm_disconnect = None;
                                            cx.notify();
                                        }))))
                        } else {
                            let target = account_id.clone();
                            view.child(Button::new("disconnect-account").label("Disconnect")
                                .disabled(disabled).on_click(cx.listener(move |this, _, _, cx| {
                                    this.confirm_disconnect = Some(target.clone());
                                    cx.notify();
                                })))
                        }
                    }),
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
                    "Credential cleanup is required. Choose Disconnect on this account to retry local cleanup before signing in again."
                }
                _ if login.user_code.is_none() => {
                    "Open browser sign-in on the daemon’s host, finish signing in, then check status here."
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
                let url_allowed = login.url_allowed();
                let url = login.url.clone();
                progress = progress
                    .when_some(login.user_code.clone(), |view, code| {
                        view.child(div().font_family(theme::MONO_FONT).text_lg().child(code))
                    })
                    .child(div().text_sm().text_color(theme::MUTED).child(
                        if login.user_code.is_some() {
                            VERIFICATION_URL
                        } else {
                            "auth.openai.com"
                        },
                    ))
                    .child(
                        div()
                            .flex()
                            .flex_wrap()
                            .gap_2()
                            .child(
                                Button::new("open-codex-login")
                                    .label(if login.user_code.is_some() {
                                        "Open verification page"
                                    } else {
                                        "Open browser sign-in"
                                    })
                                    .disabled(!self.online || !url_allowed)
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        if this.online
                                            && this.login.as_ref().is_some_and(|login| {
                                                login.url_allowed() && login.url == url
                                            })
                                        {
                                            cx.open_url(&url);
                                        }
                                    })),
                            )
                            .when_some(login.user_code.clone(), |view, code| {
                                view.child(
                                    Button::new("copy-codex-code")
                                        .label(if self.copied {
                                            "Code copied"
                                        } else {
                                            "Copy code"
                                        })
                                        .disabled(!self.online || !url_allowed)
                                        .on_click(cx.listener(move |this, _, _, cx| {
                                            if this.online
                                                && this.login.as_ref().is_some_and(|login| {
                                                    login.url_allowed()
                                                        && login.user_code.as_ref() == Some(&code)
                                                })
                                            {
                                                cx.write_to_clipboard(ClipboardItem::new_string(
                                                    code.clone(),
                                                ));
                                                this.copied = true;
                                                cx.notify();
                                            }
                                        })),
                                )
                            })
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
