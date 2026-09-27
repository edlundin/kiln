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
pub enum McpRegistryError {
    Generation(StdioGenerationError),
    Closed,
    Capacity,
    Stopping,
    BindingChanged,
    InvalidDirectory,
    DirectoryChanged,
}

impl From<StdioGenerationError> for McpRegistryError {
    fn from(error: StdioGenerationError) -> Self {
        Self::Generation(error)
    }
}

/// Compatibility names for existing stdio callers.
pub type StdioRegistry<S> = McpRegistry<S>;
pub type StdioRegistryError = McpRegistryError;

enum RegistryLaunch {
    Stdio(StdioGenerationLaunch),
    Http(crate::ResolvedHttpLaunch, rustix::fd::OwnedFd),
}

struct Entry {
    owner: StdioGeneration,
    definition_version: u64,
    binding_revision: NonZeroU64,
    // Keep the inode pinned while this owner is cached, even if the server
    // changes its own cwd or the original path is removed and recreated.
    directory: rustix::fd::OwnedFd,
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
pub struct McpRegistry<S> {
    store: Arc<S>,
    capacity: NonZeroUsize,
    state: Mutex<State>,
}

impl<S: McpInstanceStore + McpDefinitionStore + McpInvocationStore + 'static> McpRegistry<S> {
    pub fn new(store: Arc<S>, capacity: NonZeroUsize) -> Self {
        Self {
            store,
            capacity,
            state: Mutex::new(State::default()),
        }
    }

    pub(crate) fn store(&self) -> &S {
        &self.store
    }

    /// Concurrent demand for one key joins startup or reuses its live owner.
    /// Cancelling a waiter does not stop a server shared by other Runs. Inputs
    /// must already be authorized, including when an existing owner is reused.
    pub async fn ensure_ready(
        &self,
        launch: StdioGenerationLaunch,
        binding_revision: NonZeroU64,
    ) -> Result<McpInstanceRecord, McpRegistryError> {
        self.ensure_launch(RegistryLaunch::Stdio(launch), binding_revision)
            .await
    }

    /// HTTP reuse retains the approved directory's filesystem identity just as
    /// stdio does; remote transport does not widen a native ToolCall's scope.
    pub async fn ensure_http_ready(
        &self,
        launch: crate::ResolvedHttpLaunch,
        directory: rustix::fd::OwnedFd,
    ) -> Result<McpInstanceRecord, McpRegistryError> {
        let revision = launch.host_binding_version.revision;
        self.ensure_launch(RegistryLaunch::Http(launch, directory), revision)
            .await
    }

    async fn ensure_launch(
        &self,
        launch: RegistryLaunch,
        binding_revision: NonZeroU64,
    ) -> Result<McpInstanceRecord, McpRegistryError> {
        let (key, definition_version, host_binding_version, limits, pinned) = match &launch {
            RegistryLaunch::Stdio(launch) => (
                launch.key.clone(),
                launch.definition_version,
                launch.host_binding_version.clone(),
                launch.definition_limits,
                &launch.process.working_directory,
            ),
            RegistryLaunch::Http(launch, directory) => (
                launch.key.clone(),
                launch.definition_version,
                Some(launch.host_binding_version.clone()),
                launch.definition_limits,
                directory,
            ),
        };
        let directory =
            rustix::fs::fstat(pinned).map_err(|_| McpRegistryError::InvalidDirectory)?;
        if rustix::fs::FileType::from_raw_mode(directory.st_mode) != rustix::fs::FileType::Directory
        {
            return Err(McpRegistryError::InvalidDirectory);
        }
        if host_binding_version
            .as_ref()
            .is_some_and(|v| v.revision != binding_revision)
        {
            return Err(McpRegistryError::BindingChanged);
        }
        let mut observer = {
            let mut state = self.state.lock().await;
            if state.closed {
                return Err(McpRegistryError::Closed);
            }
            // Only workers with known local transport cleanup may release slots. Retain
            // failed cleanup/lost workers for shutdown reporting; durable claims
            // also fence replacement when journal state remains uncertain.
            state
                .entries
                .retain(|_, entry| !entry.owner.observer().can_release());
            if let Some(entry) = state.entries.get(key.canonical_json()) {
                if entry.stopping {
                    return Err(McpRegistryError::Stopping);
                }
                if entry.definition_version != definition_version {
                    return Err(
                        StdioGenerationError::Store(McpInstanceError::DefinitionChanged).into(),
                    );
                }
                if entry.binding_revision != binding_revision {
                    return Err(McpRegistryError::BindingChanged);
                }
                let existing = rustix::fs::fstat(&entry.directory)
                    .map_err(|_| McpRegistryError::InvalidDirectory)?;
                if existing.st_dev != directory.st_dev || existing.st_ino != directory.st_ino {
                    return Err(McpRegistryError::DirectoryChanged);
                }
                entry.owner.observer()
            } else {
                if state.entries.len() >= self.capacity.get() {
                    return Err(McpRegistryError::Capacity);
                }
                let directory = rustix::io::fcntl_dupfd_cloexec(pinned, 0)
                    .map_err(|_| McpRegistryError::InvalidDirectory)?;
                let owner = match launch {
                    RegistryLaunch::Stdio(launch) => {
                        StdioGeneration::spawn(self.store.clone(), launch)
                    }
                    RegistryLaunch::Http(launch, _) => {
                        crate::McpGeneration::spawn_http(self.store.clone(), launch)
                    }
                };
                let observer = owner.observer();
                state.entries.insert(
                    key.canonical_json().to_owned(),
                    Entry {
                        owner,
                        definition_version,
                        binding_revision,
                        directory,
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
            return Err(McpRegistryError::Closed);
        }
        if state
            .entries
            .get(key.canonical_json())
            .is_none_or(|entry| entry.stopping)
            || observer.is_finished()
        {
            return Err(McpRegistryError::Stopping);
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
        let sender = self.dispatch_sender(key).await?;
        crate::generation::dispatch_to(sender, permit, limits, cancellation)
            .await?
            .result
    }

    /// Like dispatch, but requires a durable outcome receipt and captures output
    /// for the normal native completion store. Archive exact bytes as
    /// TOOL_OUTPUT_MEDIA_TYPE; a storage failure never authorizes redispatch.
    pub async fn dispatch_tool_call<F, Fut, E>(
        &self,
        key: &McpInstanceKey,
        permit: McpDispatchPermit,
        limits: StdioCallLimits,
        cancellation: tokio::sync::oneshot::Receiver<()>,
        archive: F,
    ) -> Result<kiln_core::ToolCallResult, kiln_core::RunError>
    where
        F: FnOnce(Vec<u8>) -> Fut,
        Fut: std::future::Future<Output = Result<kiln_core::Artifact, E>>,
    {
        let sender = self
            .dispatch_sender(key)
            .await
            .map_err(|_| kiln_core::RunError::RunStoreUnavailable)?;
        let outcome = crate::generation::dispatch_to(sender, permit, limits, cancellation)
            .await
            .map_err(|_| kiln_core::RunError::RunStoreUnavailable)?;
        crate::output::capture(outcome, archive).await
    }

    async fn dispatch_sender(
        &self,
        key: &McpInstanceKey,
    ) -> Result<tokio::sync::mpsc::Sender<crate::dispatch::DispatchRequest>, StdioCallError> {
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
        Ok(sender)
    }

    /// Stop a scope before reconfiguration/retirement. Cancellation leaves the
    /// stopping entry retained, so another stop can await the same cleanup.
    pub async fn stop(
        &self,
        key: &McpInstanceKey,
    ) -> Result<Option<McpInstanceRecord>, McpRegistryError> {
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
