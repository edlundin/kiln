//! Invocation-scoped roots/form mediation shared by legacy callbacks and MRTR.
// Roots remain part of Kiln's supported final protocol revisions.
#![allow(deprecated)]

use crate::{StdioCallLimits, catalog_state::CatalogEpochs};
use kiln_core::{
    McpDefinitionLimits, McpElicitationDecisionStore, McpElicitationFormMutation, McpInputKind,
    McpInputMutation, McpInputStore, McpInvocationRecord,
};
use rmcp::{
    ClientHandler, ErrorData, RoleClient,
    model::*,
    service::{NotificationContext, RequestContext},
};
use std::{
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

struct Active {
    invocation: McpInvocationRecord,
    alive: tokio::sync::watch::Sender<bool>,
    remaining: tokio::sync::Mutex<(usize, u64)>,
    deadline: tokio::time::Instant,
    max_result_bytes: usize,
}

pub(crate) struct ActiveInputGuard(Arc<Active>);
impl Drop for ActiveInputGuard {
    fn drop(&mut self) {
        self.0.alive.send_replace(false);
    }
}

pub(crate) struct RuntimeClient<S> {
    store: Arc<S>,
    limits: McpDefinitionLimits,
    pub(crate) epochs: CatalogEpochs,
    active: Arc<Mutex<Option<Arc<Active>>>>,
    host_bound: bool,
    elicitation: Option<crate::McpElicitationValidationLimits>,
}
impl<S> Clone for RuntimeClient<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            limits: self.limits,
            epochs: self.epochs.clone(),
            active: self.active.clone(),
            host_bound: self.host_bound,
            elicitation: self.elicitation,
        }
    }
}
impl<S: McpInputStore + McpElicitationDecisionStore> RuntimeClient<S> {
    pub(crate) async fn elicitation(
        &self,
        request: ElicitRequestParams,
    ) -> Result<ElicitResult, ErrorData> {
        let unavailable = || ErrorData::internal_error("MCP elicitation unavailable", None);
        let active = self
            .active
            .lock()
            .map_err(|_| unavailable())?
            .clone()
            .ok_or_else(unavailable)?;
        let mut limits = self.elicitation.ok_or_else(unavailable)?;
        limits.max_response_bytes = limits
            .max_response_bytes
            .min(std::num::NonZeroUsize::new(active.max_result_bytes).ok_or_else(unavailable)?);
        let mut stopped = active.alive.subscribe();
        let live = || *active.alive.borrow() && tokio::time::Instant::now() < active.deadline;
        let work = async {
            // Keep the shared roots/form quota lock for the whole interaction.
            let mut budget = active.remaining.lock().await;
            if !live() || budget.0 == 0 {
                return Err(unavailable());
            }
            budget.0 -= 1;
            let (form, validator) =
                crate::elicitation::normalize_form(request, limits).map_err(|_| unavailable())?;
            budget.1 = budget
                .1
                .checked_add(1)
                .filter(|n| *n <= i64::MAX as u64)
                .ok_or_else(unavailable)?;
            let ordinal = NonZeroU64::new(budget.1).ok_or_else(unavailable)?;
            // Subscribe before publishing required input, including fast decisions.
            let mut changes = self.store.subscribe_mcp_input_changes();
            let McpElicitationFormMutation::Applied(record) = self
                .store
                .require_mcp_elicitation_form(&active.invocation, ordinal, &form, limits.form)
                .await
                .map_err(|_| unavailable())?
            else {
                return Err(unavailable());
            };
            loop {
                changes.borrow_and_update();
                if !live() {
                    return Err(unavailable());
                }
                if let Some(decision) = self
                    .store
                    .get_mcp_elicitation_decision(&record, limits.form, limits.max_response_bytes)
                    .await
                    .map_err(|_| unavailable())?
                {
                    let result: ElicitResult =
                        serde_json::from_str(decision.as_json()).map_err(|_| unavailable())?;
                    let result = validator
                        .response(result.action, result.content)
                        .map_err(|_| unavailable())?;
                    if !live() {
                        return Err(unavailable());
                    }
                    let McpInputMutation::Applied(_) = self
                        .store
                        .resolve_mcp_input(&record.input)
                        .await
                        .map_err(|_| unavailable())?
                    else {
                        return Err(unavailable());
                    };
                    if !live() {
                        return Err(unavailable());
                    }
                    return Ok(result);
                }
                changes.changed().await.map_err(|_| unavailable())?;
            }
        };
        tokio::select! {
            biased;
            _ = stopped.changed() => Err(unavailable()),
            _ = tokio::time::sleep_until(active.deadline) => Err(unavailable()),
            result = work => result,
        }
    }
    pub(crate) fn new(
        store: Arc<S>,
        limits: McpDefinitionLimits,
        host_bound: bool,
        elicitation: Option<crate::McpElicitationValidationLimits>,
    ) -> Self {
        Self {
            store,
            limits,
            epochs: CatalogEpochs::default(),
            active: Arc::new(Mutex::new(None)),
            host_bound,
            elicitation,
        }
    }
    pub(crate) fn enter(
        &self,
        invocation: &McpInvocationRecord,
        limits: &StdioCallLimits,
    ) -> ActiveInputGuard {
        let active = Arc::new(Active {
            invocation: invocation.clone(),
            alive: tokio::sync::watch::channel(true).0,
            remaining: tokio::sync::Mutex::new((
                limits.max_input_requests.map_or(0, |n| n.get()),
                0,
            )),
            deadline: limits.deadline,
            max_result_bytes: limits.max_result_bytes.get(),
        });
        *self.active.lock().expect("MCP active input lock") = Some(active.clone());
        ActiveInputGuard(active)
    }
    pub(crate) async fn roots(&self) -> Result<ListRootsResult, ErrorData> {
        let unavailable = || ErrorData::internal_error("MCP roots unavailable", None);
        let active = self
            .active
            .lock()
            .map_err(|_| unavailable())?
            .clone()
            .ok_or_else(unavailable)?;
        // The per-invocation lock serializes concurrent legacy requests and MRTR.
        let mut budget = active.remaining.lock().await;
        let live = || *active.alive.borrow() && tokio::time::Instant::now() < active.deadline;
        if !self.host_bound || !live() || budget.0 == 0 {
            return Err(unavailable());
        }
        budget.0 -= 1;
        budget.1 = budget
            .1
            .checked_add(1)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or_else(unavailable)?;
        let ordinal = NonZeroU64::new(budget.1).ok_or_else(unavailable)?;
        let McpInputMutation::Applied(input) = self
            .store
            .require_mcp_input(&active.invocation, ordinal, McpInputKind::Roots)
            .await
            .map_err(|_| unavailable())?
        else {
            return Err(unavailable());
        };
        if !live() {
            return Err(unavailable());
        }
        let path = self
            .store
            .mcp_input_root(&input, self.limits)
            .await
            .map_err(|_| unavailable())?;
        let uri = reqwest::Url::from_directory_path(path).map_err(|_| unavailable())?;
        let result = ListRootsResult::new(vec![Root::new(uri.as_str())]);
        if serde_json::to_vec(&result)
            .map_err(|_| unavailable())?
            .len()
            > active.max_result_bytes
            || !live()
        {
            return Err(unavailable());
        }
        let McpInputMutation::Applied(_) = self
            .store
            .resolve_mcp_input(&input)
            .await
            .map_err(|_| unavailable())?
        else {
            return Err(unavailable());
        };
        if !live() {
            return Err(unavailable());
        }
        Ok(result)
    }
}
impl<S: McpInputStore + McpElicitationDecisionStore + 'static> ClientHandler for RuntimeClient<S> {
    fn get_info(&self) -> ClientConfig {
        let mut info = self.epochs.get_info();
        if self.host_bound {
            info.capabilities.roots = Some(RootsCapabilities::default());
        }
        if self.elicitation.is_some() {
            info.capabilities.elicitation = Some(
                ElicitationCapability::new()
                    .with_form(FormElicitationCapability::new().with_schema_validation(true)),
            );
        }
        info
    }
    async fn create_elicitation(
        &self,
        request: ElicitRequestParams,
        context: RequestContext<RoleClient>,
    ) -> Result<ElicitResult, ErrorData> {
        tokio::select! {
            biased;
            _ = context.ct.cancelled() => Err(ErrorData::internal_error("MCP elicitation unavailable", None)),
            result = self.elicitation(request) => result,
        }
    }
    async fn list_roots(
        &self,
        _: RequestContext<RoleClient>,
    ) -> Result<ListRootsResult, ErrorData> {
        self.roots().await
    }
    async fn on_tool_list_changed(&self, _: NotificationContext<RoleClient>) {
        self.epochs.invalidate(kiln_core::McpCatalogKind::Tools);
    }
    async fn on_prompt_list_changed(&self, _: NotificationContext<RoleClient>) {
        self.epochs.invalidate(kiln_core::McpCatalogKind::Prompts);
    }
    async fn on_resource_list_changed(&self, _: NotificationContext<RoleClient>) {
        self.epochs.invalidate(kiln_core::McpCatalogKind::Resources);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kiln_core::{McpInputRecord, McpInputState, McpInvocationError};
    use std::{
        num::NonZeroUsize,
        sync::atomic::{AtomicUsize, Ordering},
    };

    struct Store {
        entered: tokio::sync::Notify,
        release: tokio::sync::Semaphore,
        required: AtomicUsize,
        resolved: AtomicUsize,
        form: Mutex<Option<kiln_core::McpElicitationFormRecord>>,
        decision: Mutex<Option<kiln_core::McpElicitationDecision>>,
        changes: tokio::sync::watch::Sender<()>,
    }
    impl Store {
        fn new() -> Self {
            Self {
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Semaphore::new(0),
                required: AtomicUsize::new(0),
                resolved: AtomicUsize::new(0),
                form: Mutex::new(None),
                decision: Mutex::new(None),
                changes: tokio::sync::watch::channel(()).0,
            }
        }
    }
    impl McpInputStore for Store {
        async fn mcp_input_interaction_run(
            &self,
            _: &McpInputRecord,
        ) -> Result<kiln_core::RunId, McpInvocationError> {
            panic!("roots must not request user interaction")
        }
        async fn require_mcp_input(
            &self,
            invocation: &McpInvocationRecord,
            ordinal: NonZeroU64,
            kind: McpInputKind,
        ) -> Result<McpInputMutation, McpInvocationError> {
            self.required.fetch_add(1, Ordering::SeqCst);
            Ok(McpInputMutation::Applied(McpInputRecord {
                invocation: invocation.clone(),
                ordinal,
                kind,
                state: McpInputState::Required,
            }))
        }
        async fn mcp_input_root(
            &self,
            _: &McpInputRecord,
            _: McpDefinitionLimits,
        ) -> Result<std::path::PathBuf, McpInvocationError> {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
            Ok("/approved directory".into())
        }
        async fn resolve_mcp_input(
            &self,
            expected: &McpInputRecord,
        ) -> Result<McpInputMutation, McpInvocationError> {
            self.resolved.fetch_add(1, Ordering::SeqCst);
            let mut record = expected.clone();
            record.state = McpInputState::Resolved;
            Ok(McpInputMutation::Applied(record))
        }
    }

    // This fake exercises waiter lifetime and receipt handling only; SQLite
    // integration tests establish ownership, atomicity and schema-write checks.
    impl kiln_core::McpElicitationFormStore for Store {
        async fn require_mcp_elicitation_form(
            &self,
            invocation: &McpInvocationRecord,
            ordinal: NonZeroU64,
            form: &kiln_core::McpElicitationForm,
            _: kiln_core::McpElicitationFormLimits,
        ) -> Result<McpElicitationFormMutation, McpInvocationError> {
            let mut slot = self.form.lock().unwrap();
            if let Some(record) = &*slot {
                return Ok(McpElicitationFormMutation::Existing(record.clone()));
            }
            let record = kiln_core::McpElicitationFormRecord {
                input: McpInputRecord {
                    invocation: invocation.clone(),
                    ordinal,
                    kind: McpInputKind::Elicitation,
                    state: McpInputState::Required,
                },
                interaction_run: kiln_core::RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                form: form.clone(),
            };
            *slot = Some(record.clone());
            self.required.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.changes.send_replace(());
            Ok(McpElicitationFormMutation::Applied(record))
        }
        async fn get_mcp_elicitation_form(
            &self,
            _: &McpInputRecord,
            _: &kiln_core::RunId,
            _: kiln_core::McpElicitationFormLimits,
        ) -> Result<kiln_core::McpElicitationFormRecord, McpInvocationError> {
            Ok(self.form.lock().unwrap().clone().unwrap())
        }
    }
    impl McpElicitationDecisionStore for Store {
        fn subscribe_mcp_input_changes(&self) -> tokio::sync::watch::Receiver<()> {
            self.changes.subscribe()
        }
        async fn decide_mcp_elicitation_form(
            &self,
            _: &kiln_core::McpElicitationFormRecord,
            decision: &kiln_core::McpElicitationDecision,
            _: kiln_core::McpElicitationFormLimits,
            _: NonZeroUsize,
        ) -> Result<kiln_core::McpElicitationDecisionMutation, McpInvocationError> {
            *self.decision.lock().unwrap() = Some(decision.clone());
            self.changes.send_replace(());
            Ok(kiln_core::McpElicitationDecisionMutation::Applied)
        }
        async fn get_mcp_elicitation_decision(
            &self,
            _: &kiln_core::McpElicitationFormRecord,
            _: kiln_core::McpElicitationFormLimits,
            _: NonZeroUsize,
        ) -> Result<Option<kiln_core::McpElicitationDecision>, McpInvocationError> {
            Ok(self.decision.lock().unwrap().clone())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn form_waiter_obeys_decision_guard_deadline_and_receipts() {
        let request = || {
            serde_json::from_value(serde_json::json!({"mode":"form","message":"confirm","requestedSchema":{"type":"object","properties":{"proceed":{"type":"boolean"}},"required":["proceed"]}})).unwrap()
        };
        for mode in ["accept", "drop", "deadline", "invalid", "existing", "url"] {
            let store = Arc::new(Store::new());
            let n = NonZeroUsize::new(1).unwrap();
            let policy = crate::McpElicitationValidationLimits {
                form: kiln_core::McpElicitationFormLimits {
                    max_message_bytes: NonZeroUsize::new(64).unwrap(),
                    max_schema_bytes: NonZeroUsize::new(512).unwrap(),
                },
                max_response_bytes: NonZeroUsize::new(128).unwrap(),
            };
            let handler = RuntimeClient::new(
                store.clone(),
                McpDefinitionLimits {
                    max_key_bytes: 64,
                    max_metadata_bytes: 4096,
                    max_arguments: 1,
                    max_argument_bytes: 128,
                    max_environment: 1,
                    max_endpoint_bytes: 128,
                },
                true,
                Some(policy),
            );
            let info = handler.get_info();
            assert_eq!(
                info.capabilities
                    .elicitation
                    .unwrap()
                    .form
                    .unwrap()
                    .schema_validation,
                Some(true)
            );
            assert!(info.capabilities.sampling.is_none());
            assert!(handler.elicitation(request()).await.is_err());
            let invocation = McpInvocationRecord {
                tool_call_id: kiln_core::ToolCallId::parse("tcl_01ARZ3NDEKTSV4RRFFQ69G5FAV")
                    .unwrap(),
                generation: kiln_core::McpGenerationId::parse("mcg_01ARZ3NDEKTSV4RRFFQ69G5FAV")
                    .unwrap(),
                state: kiln_core::McpInvocationState::Dispatching,
            };
            let limits = StdioCallLimits {
                max_input_requests: Some(n),
                deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                max_result_bytes: policy.max_response_bytes,
                catalog: crate::McpCatalogLimits {
                    max_pages: n,
                    max_entries: n,
                    max_bytes: n,
                    max_regex_bytes: n,
                    max_regex_backtracks: n,
                },
            };
            let mut guard = Some(handler.enter(&invocation, &limits));
            if mode == "url" {
                let url = serde_json::from_value(serde_json::json!({
                    "mode": "url", "message": "confirm", "elicitationId": "example",
                    "url": "https://example.com/confirm"
                }))
                .unwrap();
                assert!(handler.elicitation(url).await.is_err());
                assert!(handler.elicitation(request()).await.is_err());
                assert_eq!(store.required.load(Ordering::SeqCst), 0);
                assert_eq!(store.resolved.load(Ordering::SeqCst), 0);
                continue;
            }
            let mut pending = Box::pin(handler.elicitation(request()));
            tokio::select! { _ = store.entered.notified() => {}, _ = &mut pending => panic!("decision must wait") }
            assert_eq!(store.resolved.load(Ordering::SeqCst), 0);
            if mode == "existing" {
                drop(pending);
                drop(guard.take());
                let _guard = handler.enter(&invocation, &limits);
                assert!(handler.elicitation(request()).await.is_err());
            } else {
                let content = if mode == "invalid" { "42" } else { "true" };
                let json = format!(r#"{{"action":"accept","content":{{"proceed":{content}}}}}"#);
                let decision =
                    kiln_core::McpElicitationDecision::from_json(&json, policy.max_response_bytes)
                        .unwrap();
                let record = store.form.lock().unwrap().clone().unwrap();
                store
                    .decide_mcp_elicitation_form(
                        &record,
                        &decision,
                        policy.form,
                        policy.max_response_bytes,
                    )
                    .await
                    .unwrap();
                if mode == "drop" {
                    drop(guard.take());
                }
                if mode == "deadline" {
                    tokio::time::advance(std::time::Duration::from_secs(5)).await;
                }
                let result = pending.await;
                if mode == "accept" {
                    assert_eq!(
                        result.unwrap().content,
                        Some(serde_json::json!({"proceed":true}))
                    );
                    assert!(handler.roots().await.is_err(), "shared quota exhausted");
                } else {
                    assert!(result.is_err(), "{mode}");
                }
            }
            assert_eq!(
                store.resolved.load(Ordering::SeqCst),
                usize::from(mode == "accept")
            );
        }
    }

    #[tokio::test]
    async fn roots_budget_and_dropped_invocation_revoke_mediation() {
        let store = Arc::new(Store::new());
        let handler = RuntimeClient::new(
            store.clone(),
            McpDefinitionLimits {
                max_key_bytes: 64,
                max_metadata_bytes: 4096,
                max_arguments: 1,
                max_argument_bytes: 128,
                max_environment: 1,
                max_endpoint_bytes: 128,
            },
            true,
            None,
        );
        let n = NonZeroUsize::new(1).unwrap();
        let mut limits = StdioCallLimits {
            max_input_requests: Some(n),
            deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(5),
            max_result_bytes: NonZeroUsize::new(256).unwrap(),
            catalog: crate::McpCatalogLimits {
                max_pages: n,
                max_entries: n,
                max_bytes: n,
                max_regex_bytes: n,
                max_regex_backtracks: n,
            },
        };
        let invocation = McpInvocationRecord {
            tool_call_id: kiln_core::ToolCallId::parse("tcl_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            generation: kiln_core::McpGenerationId::parse("mcg_01ARZ3NDEKTSV4RRFFQ69G5FAV")
                .unwrap(),
            state: kiln_core::McpInvocationState::Dispatching,
        };
        assert!(handler.get_info().capabilities.elicitation.is_none());
        assert!(handler.roots().await.is_err());
        let guard = handler.enter(&invocation, &limits);
        let request = handler.roots();
        tokio::pin!(request);
        tokio::select! { _ = store.entered.notified() => {}, _ = &mut request => panic!("lookup must wait") }
        drop(guard);
        store.release.add_permits(1);
        assert!(request.await.is_err());
        assert_eq!(store.resolved.load(Ordering::SeqCst), 0);
        assert!(handler.roots().await.is_err());
        let guard = handler.enter(&invocation, &limits);
        store.release.add_permits(1);
        assert_eq!(
            handler.roots().await.unwrap().roots[0].uri,
            "file:///approved%20directory/"
        );
        assert!(handler.roots().await.is_err());
        assert_eq!(store.resolved.load(Ordering::SeqCst), 1);
        assert_eq!(store.required.load(Ordering::SeqCst), 2);
        drop(guard);
        limits.max_input_requests = None;
        let _guard = handler.enter(&invocation, &limits);
        assert!(handler.roots().await.is_err());
        assert_eq!(store.required.load(Ordering::SeqCst), 2);
    }
}
