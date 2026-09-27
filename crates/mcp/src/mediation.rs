//! Invocation-scoped roots mediation shared by legacy callbacks and MRTR.
// Roots remain part of Kiln's supported final protocol revisions.
#![allow(deprecated)]

use crate::{StdioCallLimits, catalog_state::CatalogEpochs};
use kiln_core::{
    McpDefinitionLimits, McpInputKind, McpInputMutation, McpInputStore, McpInvocationRecord,
};
use rmcp::{
    ClientHandler, ErrorData, RoleClient,
    model::*,
    service::{NotificationContext, RequestContext},
};
use std::{
    num::NonZeroU64,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

struct Active {
    invocation: McpInvocationRecord,
    alive: AtomicBool,
    remaining: tokio::sync::Mutex<(usize, u64)>,
    deadline: tokio::time::Instant,
    max_result_bytes: usize,
}

pub(crate) struct ActiveInputGuard(Arc<Active>);
impl Drop for ActiveInputGuard {
    fn drop(&mut self) {
        self.0.alive.store(false, Ordering::SeqCst);
    }
}

pub(crate) struct RuntimeClient<S> {
    store: Arc<S>,
    limits: McpDefinitionLimits,
    pub(crate) epochs: CatalogEpochs,
    active: Arc<Mutex<Option<Arc<Active>>>>,
    host_bound: bool,
}
impl<S> Clone for RuntimeClient<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            limits: self.limits,
            epochs: self.epochs.clone(),
            active: self.active.clone(),
            host_bound: self.host_bound,
        }
    }
}
impl<S: McpInputStore> RuntimeClient<S> {
    pub(crate) fn new(store: Arc<S>, limits: McpDefinitionLimits, host_bound: bool) -> Self {
        Self {
            store,
            limits,
            epochs: CatalogEpochs::default(),
            active: Arc::new(Mutex::new(None)),
            host_bound,
        }
    }
    pub(crate) fn enter(
        &self,
        invocation: &McpInvocationRecord,
        limits: &StdioCallLimits,
    ) -> ActiveInputGuard {
        let active = Arc::new(Active {
            invocation: invocation.clone(),
            alive: AtomicBool::new(true),
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
        let live =
            || active.alive.load(Ordering::SeqCst) && tokio::time::Instant::now() < active.deadline;
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
impl<S: McpInputStore + 'static> ClientHandler for RuntimeClient<S> {
    fn get_info(&self) -> ClientConfig {
        let mut info = self.epochs.get_info();
        if self.host_bound {
            info.capabilities.roots = Some(RootsCapabilities::default());
        }
        info
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
    use std::{num::NonZeroUsize, sync::atomic::AtomicUsize};

    struct Store {
        entered: tokio::sync::Notify,
        release: tokio::sync::Semaphore,
        required: AtomicUsize,
        resolved: AtomicUsize,
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

    #[tokio::test]
    async fn roots_budget_and_dropped_invocation_revoke_mediation() {
        let store = Arc::new(Store {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
            required: AtomicUsize::new(0),
            resolved: AtomicUsize::new(0),
        });
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
