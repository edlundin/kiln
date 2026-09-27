//! Daemon-owned scope registry. Runs observe readiness; they do not own processes.

use std::{
    collections::HashMap,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use kiln_core::{
    McpDefinitionStore, McpDesiredState, McpDispatchPermit, McpInstanceError, McpInstanceKey,
    McpInstanceRecord, McpInstanceStore, McpInvocationStore, McpObservedState,
};
use tokio::sync::Mutex;

use crate::{
    StdioCallError, StdioCallLimits, StdioCallResult, StdioGeneration, StdioGenerationError,
    StdioGenerationLaunch,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StdioRegistryError {
    Generation(StdioGenerationError),
    Closed,
    Capacity,
    Stopping,
    BindingChanged,
}

impl From<StdioGenerationError> for StdioRegistryError {
    fn from(error: StdioGenerationError) -> Self {
        Self::Generation(error)
    }
}

struct Entry {
    owner: StdioGeneration,
    definition_version: u64,
    binding_revision: NonZeroU64,
    stopping: bool,
}

#[derive(Default)]
struct State {
    closed: bool,
    entries: HashMap<String, Entry>,
}

/// Own one registry per daemon store, after exclusive ownership and restart
/// reconciliation. The caller supplies a capacity from its resource budget.
/// Local binding revisions must change when resolved executable, environment,
/// credentials, directory authorization or other launch policy changes.
pub struct StdioRegistry<S> {
    store: Arc<S>,
    capacity: NonZeroUsize,
    state: Mutex<State>,
}

impl<S: McpInstanceStore + McpDefinitionStore + McpInvocationStore + 'static> StdioRegistry<S> {
    pub fn new(store: Arc<S>, capacity: NonZeroUsize) -> Self {
        Self {
            store,
            capacity,
            state: Mutex::new(State::default()),
        }
    }

    /// Concurrent demand for one key joins startup or reuses its live owner.
    /// Cancelling a waiter does not stop a server shared by other Runs. Inputs
    /// must already be authorized, including when an existing owner is reused.
    pub async fn ensure_ready(
        &self,
        launch: StdioGenerationLaunch,
        binding_revision: NonZeroU64,
    ) -> Result<McpInstanceRecord, StdioRegistryError> {
        let key = launch.key.clone();
        let definition_version = launch.definition_version;
        let host_binding_version = launch.host_binding_version.clone();
        if host_binding_version
            .as_ref()
            .is_some_and(|v| v.revision != binding_revision)
        {
            return Err(StdioRegistryError::BindingChanged);
        }
        let limits = launch.definition_limits;
        let mut observer = {
            let mut state = self.state.lock().await;
            if state.closed {
                return Err(StdioRegistryError::Closed);
            }
            // Only workers with known process cleanup may release slots. Retain
            // failed cleanup/lost workers for shutdown reporting; durable claims
            // also fence replacement when journal state remains uncertain.
            state
                .entries
                .retain(|_, entry| !entry.owner.observer().can_release());
            if let Some(entry) = state.entries.get(key.canonical_json()) {
                if entry.stopping {
                    return Err(StdioRegistryError::Stopping);
                }
                if entry.definition_version != definition_version {
                    return Err(
                        StdioGenerationError::Store(McpInstanceError::DefinitionChanged).into(),
                    );
                }
                if entry.binding_revision != binding_revision {
                    return Err(StdioRegistryError::BindingChanged);
                }
                entry.owner.observer()
            } else {
                if state.entries.len() >= self.capacity.get() {
                    return Err(StdioRegistryError::Capacity);
                }
                let owner = StdioGeneration::spawn(self.store.clone(), launch);
                let observer = owner.observer();
                state.entries.insert(
                    key.canonical_json().to_owned(),
                    Entry {
                        owner,
                        definition_version,
                        binding_revision,
                        stopping: false,
                    },
                );
                observer
            }
        };
        let ready = observer.wait_ready().await?;
        // A cached ready receipt cannot establish current durable readiness or
        // continued enablement. Revalidate every reuse before returning it.
        let definition = self
            .store
            .get_mcp_definition(key.definition_id(), limits)
            .await
            .map_err(StdioGenerationError::Definition)?
            .ok_or(StdioGenerationError::Store(
                McpInstanceError::DefinitionNotFound,
            ))?;
        if definition.version != definition_version {
            return Err(StdioGenerationError::Store(McpInstanceError::DefinitionChanged).into());
        }
        if !definition.definition.server().enabled {
            return Err(StdioGenerationError::Store(McpInstanceError::Disabled).into());
        }
        let current = self
            .store
            .get_mcp_instance(&key)
            .await
            .map_err(StdioGenerationError::Store)?
            .ok_or(StdioGenerationError::Store(McpInstanceError::NotFound))?;
        if current.generation != ready.generation
            || current.host_binding_version != host_binding_version
            || current.state_version != ready.state_version
            || current.observed != McpObservedState::Ready
            || current.desired != McpDesiredState::Running
        {
            return Err(StdioGenerationError::Store(McpInstanceError::Conflict).into());
        }
        let state = self.state.lock().await;
        if state.closed {
            return Err(StdioRegistryError::Closed);
        }
        if state
            .entries
            .get(key.canonical_json())
            .is_none_or(|entry| entry.stopping)
            || observer.is_finished()
        {
            return Err(StdioRegistryError::Stopping);
        }
        Ok(current)
    }

    /// The permit fixes the native ToolCall and generation; readiness alone is
    /// never an invocation token. The registry lock is not held during execution.
    pub async fn dispatch(
        &self,
        key: &McpInstanceKey,
        permit: McpDispatchPermit,
        limits: StdioCallLimits,
        cancellation: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<StdioCallResult, StdioCallError> {
        let sender = {
            let state = self.state.lock().await;
            if state.closed {
                return Err(StdioCallError::Rejected);
            }
            let entry = state
                .entries
                .get(key.canonical_json())
                .ok_or(StdioCallError::Rejected)?;
            if entry.stopping {
                return Err(StdioCallError::Rejected);
            }
            entry.owner.dispatch_sender()
        };
        crate::generation::dispatch_to(sender, permit, limits, cancellation).await
    }

    /// Stop a scope before reconfiguration/retirement. Cancellation leaves the
    /// stopping entry retained, so another stop can await the same cleanup.
    pub async fn stop(
        &self,
        key: &McpInstanceKey,
    ) -> Result<Option<McpInstanceRecord>, StdioRegistryError> {
        let mut observer = {
            let mut state = self.state.lock().await;
            let Some(entry) = state.entries.get_mut(key.canonical_json()) else {
                return Ok(None);
            };
            entry.stopping = true;
            entry.owner.request_stop();
            entry.owner.observer()
        };
        // Keep the finished entry until subsequent demand or registry drop.
        // Removing here could accidentally remove a concurrently created owner.
        observer.wait_finished().await.map(Some).map_err(Into::into)
    }

    /// Seal against new demand and signal every owner before awaiting any of
    /// them. Repeating after caller cancellation waits for the same workers.
    pub async fn shutdown(&self) -> Vec<Result<McpInstanceRecord, StdioGenerationError>> {
        let observers = {
            let mut state = self.state.lock().await;
            state.closed = true;
            state
                .entries
                .values_mut()
                .map(|entry| {
                    entry.stopping = true;
                    entry.owner.request_stop();
                    entry.owner.observer()
                })
                .collect::<Vec<_>>()
        };
        let mut results = Vec::with_capacity(observers.len());
        for mut observer in observers {
            results.push(observer.wait_finished().await);
        }
        results
    }
}
