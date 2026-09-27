use std::{collections::HashMap, future::Future, sync::Arc};

use kiln_core::{
    ApprovalPolicy, Artifact, ContentHash, DETERMINISTIC_SUBPROCESS_CAPABILITY,
    INLINE_TOOL_OUTPUT_LIMIT, ModelInvocationApplication, ModelInvocationOutcome,
    ModelInvocationState, ReactToRunActivity, RunApplication, RunError, RunExecutionKind, RunId,
    RunInputMode, RunModelExecutorPolicy, RunModelSelection, RunModelSelectionSource,
    RunModelStartPolicy, RunMutation, RunSnapshot, RunState, RunStore, SendRunInput, SessionId,
    SessionStore, StartRunDisposition, SubprocessExecution, SubprocessExecutor, SubprocessOutput,
    SubprocessRequest, TOOL_OUTPUT_MEDIA_TYPE, TaskId, ToolCallId, WorkspacePathScope,
    WorkspaceStore,
};
use kiln_infrastructure::{
    DeterministicSubprocessExecutor, FileArtifactStore, SqliteStore, UlidIdGenerator,
    validate_subprocess_request,
};
use kiln_providers::ProviderRegistry;
use kiln_server::{
    ArtifactDownload, ArtifactFetchError, ArtifactOperations, ArtifactUploadError,
    EventBroadcaster, RunOperations,
};
use tokio::sync::{Mutex, Notify, oneshot, watch};

mod native;
#[cfg(unix)]
mod native_mcp;
mod native_tools;
#[cfg(unix)]
pub(crate) use native_mcp::{NativeMcp, configured_native_mcp};
pub(crate) use native_tools::configured_file_read;

fn legacy_subprocess_is_proven(snapshot: &RunSnapshot) -> bool {
    // Before execution_kind was persisted, the durable deterministic tool
    // capability was the only source that proved subprocess ownership.
    snapshot.execution_kind().is_none()
        && snapshot.model_selection().is_none()
        && snapshot.model_invocations().is_empty()
        && !snapshot.tool_calls().is_empty()
        && snapshot
            .tool_calls()
            .iter()
            .all(|call| call.capability() == DETERMINISTIC_SUBPROCESS_CAPABILITY)
}

struct ActiveRun {
    cancellation: Option<oneshot::Sender<()>>,
}

#[derive(Default)]
struct ActiveRuns {
    entries: Mutex<HashMap<RunId, ActiveRun>>,
    failure: Mutex<Option<RunError>>,
    changed: Notify,
}

#[derive(Clone)]
pub(crate) struct RunService {
    runs: Arc<RunApplication<SqliteStore, UlidIdGenerator>>,
    executor: DeterministicSubprocessExecutor,
    native_model: Option<crate::native_model::NativeModelSelection>,
    provider_registry: Arc<ProviderRegistry>,
    native_file_read: Option<Arc<kiln_core::WorkspaceFileReadTool>>,
    #[cfg(unix)]
    mcp_registry: Option<Arc<kiln_mcp::StdioRegistry<SqliteStore>>>,
    #[cfg(unix)]
    native_mcp: Option<Arc<NativeMcp>>,
    events: EventBroadcaster,
    commit_sequence: Arc<Mutex<()>>,
    active: Arc<ActiveRuns>,
    store: SqliteStore,
    artifacts: FileArtifactStore,
    approval_changed: watch::Sender<u64>,
}

impl RunService {
    pub(crate) fn new(
        runs: RunApplication<SqliteStore, UlidIdGenerator>,
        executor: DeterministicSubprocessExecutor,
        events: EventBroadcaster,
        store: SqliteStore,
        artifacts: FileArtifactStore,
    ) -> Self {
        Self {
            runs: Arc::new(runs),
            executor,
            native_model: None,
            provider_registry: Arc::new(ProviderRegistry::new()),
            native_file_read: None,
            #[cfg(unix)]
            mcp_registry: None,
            #[cfg(unix)]
            native_mcp: None,
            events,
            commit_sequence: Arc::new(Mutex::new(())),
            active: Arc::new(ActiveRuns::default()),
            store,
            artifacts,
            approval_changed: watch::channel(0).0,
        }
    }

    pub(crate) fn with_native_model(
        mut self,
        selection: Option<crate::native_model::NativeModelSelection>,
    ) -> Self {
        self.native_model = selection;
        self
    }

    #[cfg(unix)]
    pub(crate) fn with_mcp_registry(
        mut self,
        registry: Option<Arc<kiln_mcp::StdioRegistry<SqliteStore>>>,
    ) -> Self {
        self.mcp_registry = registry;
        self
    }

    pub(crate) fn with_provider_registry(mut self, registry: ProviderRegistry) -> Self {
        self.provider_registry = Arc::new(registry);
        self
    }

    pub(crate) fn with_native_file_read(
        mut self,
        tool: Option<kiln_core::WorkspaceFileReadTool>,
    ) -> Self {
        self.native_file_read = tool.map(Arc::new);
        self
    }

    async fn validate_start(
        &self,
        session_id: &SessionId,
        requested_scope: &WorkspacePathScope,
    ) -> Result<(), RunError> {
        let session = self
            .store
            .get_session(session_id)
            .await
            .map_err(|_| RunError::RunStoreUnavailable)?
            .ok_or(RunError::SessionNotFound)?;
        let workspace = self
            .store
            .get_workspace(session.workspace_id())
            .await
            .map_err(|_| RunError::RunStoreUnavailable)?
            .ok_or(RunError::WorkspaceRootNotFound)?;
        let root = workspace
            .root(requested_scope.workspace_root_id())
            .ok_or(RunError::WorkspaceRootNotFound)?;
        let request = SubprocessRequest::new(
            root.canonical_path().to_owned(),
            root.filesystem_identity().clone(),
            requested_scope.clone(),
        )?;
        validate_subprocess_request(&request)?;
        Ok(())
    }

    fn model_start_policy(&self) -> RunModelStartPolicy {
        let Some(selection) = self.native_model.as_ref() else {
            return RunModelStartPolicy::new(None, None);
        };
        let mut local_capabilities = selection.capabilities.clone();
        if self
            .native_tools()
            .map_or(true, |tools| tools.catalog().definitions().is_empty())
        {
            local_capabilities = kiln_core::ModelCapabilitySnapshot::new(
                selection.capabilities.version(),
                kiln_core::CapabilitySupport::Unsupported,
                selection.capabilities.vision(),
                selection.capabilities.structured_output(),
            )
            .expect("existing native capability metadata remains valid");
        }
        let local_default = RunModelSelection::new(
            selection.account_id.clone(),
            selection.settings.clone(),
            local_capabilities.clone(),
            RunModelSelectionSource::HostDefault,
        );
        let executor = RunModelExecutorPolicy::new(
            selection.settings.provider().clone(),
            self.provider_registry
                .supports_shared_model_selection(selection.settings.provider()),
            selection.settings.generation().max_output_tokens(),
            selection.settings.reasoning().effort().map(str::to_owned),
            local_capabilities,
        );
        RunModelStartPolicy::new(Some(local_default), Some(executor))
    }

    async fn start(
        &self,
        session_id: SessionId,
        idempotency_key: String,
        approval_policy: ApprovalPolicy,
        requested_scope: WorkspacePathScope,
    ) -> Result<kiln_core::StartRunMutation, RunError> {
        self.validate_start(&session_id, &requested_scope).await?;
        let (value, disposition) = {
            let _sequence = self.commit_sequence.lock().await;
            let model_policy = self.model_start_policy();
            let mutation = self
                .runs
                .start_root_run_with_model_policy(
                    session_id,
                    idempotency_key,
                    approval_policy,
                    requested_scope,
                    model_policy,
                )
                .await?;
            let value = mutation.value.clone();
            let disposition = mutation.disposition;
            self.events.publish(mutation.events);
            (value, disposition)
        };

        if disposition == StartRunDisposition::Created {
            let run_id = value.run().run_id().clone();
            self.spawn_active(run_id, true).await;
        }

        Ok(kiln_core::StartRunMutation::new(
            value,
            Vec::new(),
            disposition,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_child(
        &self,
        parent_run_id: RunId,
        task_id: Option<TaskId>,
        user_input_mode: RunInputMode,
        idempotency_key: String,
        approval_policy: ApprovalPolicy,
        requested_scope: WorkspacePathScope,
    ) -> Result<kiln_core::StartRunMutation, RunError> {
        let parent = self
            .store
            .get_run(&parent_run_id)
            .await
            .map_err(|_| RunError::RunStoreUnavailable)?
            .ok_or(RunError::ParentRunNotFound)?;
        self.validate_start(parent.run().session_id(), &requested_scope)
            .await?;
        let (value, disposition) = {
            let _sequence = self.commit_sequence.lock().await;
            let model_policy = self.model_start_policy();
            let mutation = self
                .runs
                .start_child_run_with_model_policy(
                    parent_run_id,
                    task_id,
                    user_input_mode,
                    idempotency_key,
                    approval_policy,
                    requested_scope,
                    model_policy,
                )
                .await?;
            let value = mutation.value.clone();
            let disposition = mutation.disposition;
            self.events.publish(mutation.events);
            (value, disposition)
        };
        if disposition == StartRunDisposition::Created {
            self.spawn_active(value.run().run_id().clone(), true).await;
        }
        Ok(kiln_core::StartRunMutation::new(
            value,
            Vec::new(),
            disposition,
        ))
    }

    async fn send_input(
        &self,
        command: SendRunInput,
    ) -> Result<kiln_core::SendRunInputMutation, RunError> {
        let _sequence = self.commit_sequence.lock().await;
        let mutation = self.runs.send_run_input(command).await?;
        Ok(self.publish_run_input_mutation(mutation))
    }

    async fn react_to_activity(
        &self,
        command: ReactToRunActivity,
    ) -> Result<kiln_core::SendRunInputMutation, RunError> {
        let _sequence = self.commit_sequence.lock().await;
        let mutation = self.runs.react_to_run_activity(command).await?;
        Ok(self.publish_run_input_mutation(mutation))
    }

    fn publish_run_input_mutation(
        &self,
        mutation: kiln_core::SendRunInputMutation,
    ) -> kiln_core::SendRunInputMutation {
        let value = mutation.value.clone();
        let disposition = mutation.disposition;
        self.events.publish(mutation.events);
        self.active.changed.notify_waiters();
        kiln_core::SendRunInputMutation::new(value, Vec::new(), disposition)
    }

    async fn execute(
        &self,
        run_id: RunId,
        cancellation: oneshot::Receiver<()>,
        approval_revision: watch::Receiver<u64>,
        initialize: bool,
    ) {
        let result = match self
            .execute_inner(run_id.clone(), cancellation, approval_revision, initialize)
            .await
        {
            Ok(snapshot)
                if matches!(
                    snapshot.run().state(),
                    RunState::Completed | RunState::Failed | RunState::Cancelled
                ) =>
            {
                Ok(snapshot)
            }
            Ok(_) => Err(RunError::InvalidTransition),
            Err(error) => Err(error),
        };
        if let Err(error) = &result {
            eprintln!("kilnd: Run execution stopped without a terminal state: {error:?}");
            self.active.failure.lock().await.get_or_insert(*error);
        }
        self.active.entries.lock().await.remove(&run_id);
        self.active.changed.notify_waiters();
    }

    async fn execute_inner(
        &self,
        run_id: RunId,
        mut cancellation: oneshot::Receiver<()>,
        mut approval_revision: watch::Receiver<u64>,
        initialize: bool,
    ) -> Result<RunSnapshot, RunError> {
        let snapshot = self.runs.get_run(run_id.clone()).await?;
        let saved_selection = snapshot.model_selection().cloned();
        match saved_selection {
            Some(run_selection) => {
                if matches!(
                    run_selection.source(),
                    RunModelSelectionSource::InvocationHistory
                ) || snapshot.execution_kind() == Some(RunExecutionKind::Subprocess)
                {
                    // History and conflicting durable metadata can describe a
                    // prior choice, but cannot authorize a new provider call or
                    // a switch to subprocess execution.
                    return Err(RunError::ModelUnavailable(
                        kiln_core::RunModelUnavailableReason::CapabilityUnavailable,
                    ));
                }
                let Some(runtime) = self.native_model.as_ref() else {
                    return Err(RunError::ModelUnavailable(
                        kiln_core::RunModelUnavailableReason::ExecutorUnavailable,
                    ));
                };
                if runtime.settings.provider() != run_selection.settings().provider() {
                    return Err(RunError::ModelUnavailable(
                        kiln_core::RunModelUnavailableReason::ExecutorUnavailable,
                    ));
                }
                if snapshot.model_invocations().iter().any(|invocation| {
                    invocation.settings() != run_selection.settings()
                        || invocation.provider_account_id() != run_selection.provider_account_id()
                        || invocation.capabilities() != run_selection.capabilities()
                }) {
                    return Err(RunError::ModelUnavailable(
                        kiln_core::RunModelUnavailableReason::CapabilityUnavailable,
                    ));
                }
                if !initialize {
                    return Err(RunError::ModelUnavailable(
                        kiln_core::RunModelUnavailableReason::ExecutorUnavailable,
                    ));
                }
                let selection = crate::native_model::NativeModelSelection {
                    account_id: run_selection.provider_account_id().clone(),
                    settings: run_selection.settings().clone(),
                    capabilities: run_selection.capabilities().clone(),
                    instruction: runtime.instruction,
                };
                return self
                    .execute_native(run_id, cancellation, &selection, &run_selection)
                    .await;
            }
            None => {
                let reason = if snapshot.model_invocations().is_empty() {
                    kiln_core::RunModelUnavailableReason::DefaultsMissing
                } else {
                    kiln_core::RunModelUnavailableReason::CapabilityUnavailable
                };
                let execution_kind = snapshot.execution_kind();
                let durable_subprocess_proof = execution_kind == Some(RunExecutionKind::Subprocess)
                    || (execution_kind.is_none() && legacy_subprocess_is_proven(&snapshot));
                if !snapshot.model_invocations().is_empty()
                    || execution_kind == Some(RunExecutionKind::NativeModel)
                    || !durable_subprocess_proof
                {
                    return Err(RunError::ModelUnavailable(reason));
                }
                if self.native_model.is_some() {
                    return Err(RunError::ModelUnavailable(
                        kiln_core::RunModelUnavailableReason::ExecutorUnavailable,
                    ));
                }
            }
        }
        if cancellation.try_recv().is_ok() {
            return self.wait_for_cancelled_run(run_id).await;
        }

        let running = if initialize {
            let _sequence = self.commit_sequence.lock().await;
            match self.runs.begin_execution(run_id.clone()).await {
                Ok(RunMutation {
                    value: running,
                    events,
                }) => {
                    self.events.publish(events);
                    running
                }
                Err(RunError::InvalidTransition) => return self.runs.get_run(run_id).await,
                Err(error) => return Err(error),
            }
        } else {
            self.runs.get_run(run_id.clone()).await?
        };

        // Resuming the fixture executor must never claim native model tools.
        // Native approval recovery belongs to the native loop coordinator.
        if !running.model_invocations().is_empty() {
            return Err(RunError::InvalidTransition);
        }

        if running.run().state() == RunState::WaitingForApproval {
            loop {
                let changed = approval_revision.changed();
                tokio::select! {
                    _ = &mut cancellation => {
                        let sequence = self.commit_sequence.lock().await;
                        let mutation = self.runs.request_cancellation(run_id.clone()).await?;
                        let changed = !mutation.events.is_empty();
                        self.events.publish(mutation.events.clone());
                        if changed {
                            self.active.changed.notify_waiters();
                        }
                        if mutation.value.run().state() == RunState::Cancelling {
                            drop(sequence);
                            return self.wait_for_cancelled_run(run_id).await;
                        }
                        return Ok(mutation.value);
                    }
                    _ = changed => {}
                }
                let snapshot = self.runs.get_run(run_id.clone()).await?;
                if snapshot.run().state() != RunState::WaitingForApproval {
                    break;
                }
            }
        }

        let tool_call_id = running
            .tool_calls()
            .first()
            .map(|tool_call| tool_call.tool_call_id().clone())
            .ok_or(RunError::InvalidTransition)?;

        {
            let sequence = self.commit_sequence.lock().await;
            let snapshot = self.runs.get_run(run_id.clone()).await?;
            match snapshot.run().state() {
                RunState::Running
                    if snapshot
                        .tool_call(&tool_call_id)
                        .is_some_and(|tool| tool.state() == kiln_core::ToolCallState::Ready) =>
                {
                    let RunMutation { events, .. } =
                        self.runs.begin_tool_call(tool_call_id.clone()).await?;
                    self.events.publish(events);
                }
                RunState::Running
                    if snapshot
                        .tool_call(&tool_call_id)
                        .is_some_and(|tool| tool.state() == kiln_core::ToolCallState::Denied) =>
                {
                    drop(sequence);
                    return self
                        .finish_owned_execution(run_id, tool_call_id, empty_output())
                        .await;
                }
                RunState::Cancelling => {
                    drop(sequence);
                    return self
                        .finish_owned_cancellation(run_id, tool_call_id, empty_output())
                        .await;
                }
                RunState::Cancelled => return Ok(snapshot),
                _ => return Err(RunError::InvalidTransition),
            }
        }

        let snapshot = self.runs.get_run(run_id.clone()).await?;
        let request = match self
            .subprocess_request(&snapshot, &tool_call_id)
            .await
            .and_then(|request| {
                validate_subprocess_request(&request)?;
                Ok(request)
            }) {
            Ok(request) => request,
            Err(error) => {
                return self
                    .finish_preflight_failure(
                        run_id,
                        tool_call_id,
                        preflight_failure_message(error),
                    )
                    .await;
            }
        };
        let execution = self
            .executor
            .execute(request, async move {
                let _ = cancellation.await;
            })
            .await;
        let execution = match execution {
            SubprocessExecution::Finished(output) => {
                SubprocessExecution::Finished(self.archive_large_output(output).await)
            }
            SubprocessExecution::Cancelled(output) => {
                SubprocessExecution::Cancelled(self.archive_large_output(output).await)
            }
            SubprocessExecution::CancellationFailed => SubprocessExecution::CancellationFailed,
        };

        if let SubprocessExecution::Finished(output) = execution {
            return self
                .finish_owned_execution(run_id, tool_call_id, output)
                .await;
        }

        let sequence = self.commit_sequence.lock().await;
        let snapshot = self.runs.get_run(run_id.clone()).await?;
        match (snapshot.run().state(), execution) {
            (RunState::Cancelling, SubprocessExecution::Cancelled(output)) => {
                drop(sequence);
                self.finish_owned_cancellation(run_id, tool_call_id, output)
                    .await
            }
            (_, SubprocessExecution::CancellationFailed) => Err(RunError::CancellationFailed),
            (RunState::Cancelled | RunState::Completed | RunState::Failed, _) => Ok(snapshot),
            _ => Err(RunError::InvalidTransition),
        }
    }

    async fn subprocess_request(
        &self,
        snapshot: &RunSnapshot,
        tool_call_id: &ToolCallId,
    ) -> Result<SubprocessRequest, RunError> {
        let tool = snapshot
            .tool_call(tool_call_id)
            .ok_or(RunError::RunNotFound)?;
        if tool.capability() != kiln_core::DETERMINISTIC_SUBPROCESS_CAPABILITY
            || !snapshot.model_invocations().is_empty()
        {
            return Err(RunError::InvalidTransition);
        }
        let scope = tool
            .effective_scope()
            .ok_or(RunError::InvalidTransition)?
            .clone();
        let session = self
            .store
            .get_session(snapshot.run().session_id())
            .await
            .map_err(|_| RunError::RunStoreUnavailable)?
            .ok_or(RunError::SessionNotFound)?;
        let workspace = self
            .store
            .get_workspace(session.workspace_id())
            .await
            .map_err(|_| RunError::RunStoreUnavailable)?
            .ok_or(RunError::WorkspaceRootNotFound)?;
        let root = workspace
            .root(scope.workspace_root_id())
            .ok_or(RunError::WorkspaceRootNotFound)?;
        SubprocessRequest::new(
            root.canonical_path().to_owned(),
            root.filesystem_identity().clone(),
            scope,
        )
    }

    async fn archive_large_output(&self, mut output: SubprocessOutput) -> SubprocessOutput {
        if output.stdout.len() > INLINE_TOOL_OUTPUT_LIMIT {
            let content = std::mem::take(&mut output.stdout);
            match self.store_output(content).await {
                Ok(artifact) => output.stdout_artifact = Some(artifact),
                Err(()) => return SubprocessOutput::spawn_failure("artifact storage unavailable"),
            }
        }
        if output.stderr.len() > INLINE_TOOL_OUTPUT_LIMIT {
            let content = std::mem::take(&mut output.stderr);
            match self.store_output(content).await {
                Ok(artifact) => output.stderr_artifact = Some(artifact),
                Err(()) => return SubprocessOutput::spawn_failure("artifact storage unavailable"),
            }
        }
        output
    }

    async fn store_output(&self, content: String) -> Result<Artifact, ()> {
        let artifacts = self.artifacts.clone();
        tokio::task::spawn_blocking(move || {
            artifacts.store(content.as_bytes(), TOOL_OUTPUT_MEDIA_TYPE)
        })
        .await
        .map_err(|_| ())?
        .map_err(|_| ())
    }

    async fn finish_preflight_failure(
        &self,
        run_id: RunId,
        tool_call_id: ToolCallId,
        message: &'static str,
    ) -> Result<RunSnapshot, RunError> {
        self.finish_owned_execution(
            run_id,
            tool_call_id,
            SubprocessOutput::spawn_failure(message),
        )
        .await
    }

    async fn finish_owned_execution(
        &self,
        run_id: RunId,
        tool_call_id: ToolCallId,
        output: SubprocessOutput,
    ) -> Result<RunSnapshot, RunError> {
        loop {
            self.wait_for_descendants_terminal(&run_id).await?;
            let sequence = self.commit_sequence.lock().await;
            let snapshot = self.runs.get_run(run_id.clone()).await?;
            match snapshot.run().state() {
                RunState::Running => {
                    let result = if snapshot
                        .tool_call(&tool_call_id)
                        .is_some_and(|tool| tool.state() == kiln_core::ToolCallState::Denied)
                    {
                        self.runs
                            .finish_denied_execution(run_id.clone(), tool_call_id.clone())
                            .await
                    } else {
                        self.runs
                            .finish_execution(run_id.clone(), tool_call_id.clone(), output.clone())
                            .await
                    };
                    match result {
                        Ok(RunMutation {
                            value: terminal,
                            events,
                        }) => {
                            self.events.publish(events);
                            return Ok(terminal);
                        }
                        Err(RunError::InvalidTransition)
                            if self.terminal_commit_raced(&run_id).await? =>
                        {
                            drop(sequence);
                        }
                        Err(error) => return Err(error),
                    }
                }
                RunState::Cancelling => {
                    drop(sequence);
                    return self
                        .finish_owned_cancellation(run_id, tool_call_id, output)
                        .await;
                }
                RunState::Cancelled | RunState::Completed | RunState::Failed => {
                    return Ok(snapshot);
                }
                _ => return Err(RunError::InvalidTransition),
            }
        }
    }

    async fn terminal_commit_raced(&self, run_id: &RunId) -> Result<bool, RunError> {
        let subtree = self.runs.list_run_subtree(run_id.clone()).await?;
        Ok(subtree
            .first()
            .is_some_and(|snapshot| snapshot.run().state() != RunState::Running)
            || subtree
                .iter()
                .skip(1)
                .any(|snapshot| !snapshot.run().state().is_terminal()))
    }

    async fn spawn_active(&self, run_id: RunId, initialize: bool) {
        let mut entries = self.active.entries.lock().await;
        if entries.contains_key(&run_id) {
            return;
        }
        let (cancellation_sender, cancellation_receiver) = oneshot::channel();
        entries.insert(
            run_id.clone(),
            ActiveRun {
                cancellation: Some(cancellation_sender),
            },
        );
        drop(entries);
        let service = self.clone();
        let approval_revision = self.approval_changed.subscribe();
        tokio::spawn(async move {
            service
                .execute(run_id, cancellation_receiver, approval_revision, initialize)
                .await;
        });
    }

    async fn finish_cancellation(
        &self,
        run_id: RunId,
        tool_call_id: ToolCallId,
        output: SubprocessOutput,
    ) -> Result<RunSnapshot, RunError> {
        let RunMutation {
            value: terminal,
            events,
        } = self
            .runs
            .finish_cancellation(run_id, tool_call_id, output)
            .await?;
        self.events.publish(events);
        self.active.changed.notify_waiters();
        Ok(terminal)
    }

    async fn finish_owned_cancellation(
        &self,
        run_id: RunId,
        tool_call_id: ToolCallId,
        output: SubprocessOutput,
    ) -> Result<RunSnapshot, RunError> {
        self.wait_for_descendants_terminal(&run_id).await?;
        let _sequence = self.commit_sequence.lock().await;
        let snapshot = self.runs.get_run(run_id.clone()).await?;
        match snapshot.run().state() {
            RunState::Cancelling => self.finish_cancellation(run_id, tool_call_id, output).await,
            RunState::Completed | RunState::Failed | RunState::Cancelled => Ok(snapshot),
            _ => Err(RunError::InvalidTransition),
        }
    }

    async fn wait_for_descendants_terminal(&self, run_id: &RunId) -> Result<(), RunError> {
        loop {
            let changed = self.active.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let subtree = {
                let _sequence = self.commit_sequence.lock().await;
                let subtree = self.runs.list_run_subtree(run_id.clone()).await?;
                if self.cancel_pending_invocations(&subtree).await? {
                    self.active.changed.notify_waiters();
                    self.runs.list_run_subtree(run_id.clone()).await?
                } else {
                    subtree
                }
            };
            if subtree
                .iter()
                .skip(1)
                .all(|snapshot| snapshot.run().state().is_terminal())
            {
                return Ok(());
            }
            if self.has_lost_execution(&subtree[1..]).await {
                return Err(RunError::CancellationFailed);
            }
            if let Some(error) = *self.active.failure.lock().await {
                return Err(error);
            }
            changed.await;
        }
    }

    async fn has_lost_execution(&self, snapshots: &[RunSnapshot]) -> bool {
        let entries = self.active.entries.lock().await;
        snapshots.iter().any(|snapshot| {
            !snapshot.run().state().is_terminal()
                && (snapshot
                    .tool_calls()
                    .iter()
                    .any(|tool_call| !tool_call.state().is_terminal())
                    || snapshot
                        .model_invocations()
                        .iter()
                        .any(|invocation| invocation.state() == ModelInvocationState::InFlight))
                && !entries.contains_key(snapshot.run().run_id())
        })
    }

    async fn cancel_pending_invocations(
        &self,
        snapshots: &[RunSnapshot],
    ) -> Result<bool, RunError> {
        let models = ModelInvocationApplication::new(self.store.clone(), UlidIdGenerator);
        let mut changed = false;
        for snapshot in snapshots {
            if snapshot.run().state() != RunState::Cancelling {
                continue;
            }
            for invocation in snapshot.model_invocations() {
                if invocation.state() != ModelInvocationState::Pending {
                    continue;
                }
                let mutation = models
                    .finish_model_invocation(
                        invocation.clone(),
                        ModelInvocationOutcome::cancelled(),
                    )
                    .await
                    .map_err(|_| RunError::RunStoreUnavailable)?;
                if !mutation.events.is_empty() {
                    self.events.publish(mutation.events);
                    changed = true;
                }
            }
        }
        Ok(changed)
    }

    async fn wait_for_cancelled_run(&self, run_id: RunId) -> Result<RunSnapshot, RunError> {
        loop {
            let changed = self.active.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let subtree = {
                let _sequence = self.commit_sequence.lock().await;
                let subtree = self.runs.list_run_subtree(run_id.clone()).await?;
                if self.cancel_pending_invocations(&subtree).await? {
                    self.active.changed.notify_waiters();
                    self.runs.list_run_subtree(run_id.clone()).await?
                } else {
                    subtree
                }
            };
            let snapshot = subtree.first().ok_or(RunError::RunNotFound)?;
            if snapshot.run().state().is_terminal() {
                return Ok(snapshot.clone());
            }
            if self.has_lost_execution(&subtree).await {
                return Err(RunError::CancellationFailed);
            }
            if let Some(error) = *self.active.failure.lock().await {
                return Err(error);
            }
            changed.await;
        }
    }

    async fn cancel(&self, run_id: RunId) -> Result<RunSnapshot, RunError> {
        let subtree_run_ids = {
            let _sequence = self.commit_sequence.lock().await;
            let root = self.runs.request_cancellation(run_id.clone()).await?;
            let mut changed = !root.events.is_empty();
            let root_snapshot = root.value;
            self.events.publish(root.events);
            if matches!(
                root_snapshot.run().state(),
                RunState::Completed | RunState::Failed
            ) {
                return Ok(root_snapshot);
            }
            let subtree = self.runs.list_run_subtree(run_id.clone()).await?;
            for descendant in subtree.iter().skip(1) {
                let mutation = self
                    .runs
                    .request_cancellation(descendant.run().run_id().clone())
                    .await?;
                changed |= !mutation.events.is_empty();
                self.events.publish(mutation.events);
            }
            if changed {
                self.active.changed.notify_waiters();
            }
            subtree
                .into_iter()
                .map(|snapshot| snapshot.run().run_id().clone())
                .collect::<Vec<_>>()
        };

        let cancellations = {
            let mut entries = self.active.entries.lock().await;
            subtree_run_ids
                .iter()
                .filter_map(|run_id| {
                    entries
                        .get_mut(run_id)
                        .and_then(|active| active.cancellation.take())
                })
                .collect::<Vec<_>>()
        };
        for cancellation in cancellations {
            let _ = cancellation.send(());
        }

        loop {
            let changed = self.active.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let subtree = {
                let _sequence = self.commit_sequence.lock().await;
                let snapshots = self.runs.list_run_subtree(run_id.clone()).await?;
                let mut published = false;
                for snapshot in snapshots.iter().rev() {
                    if snapshot.run().state() == RunState::Cancelling {
                        let mutation = self
                            .runs
                            .request_cancellation(snapshot.run().run_id().clone())
                            .await?;
                        published |= !mutation.events.is_empty();
                        self.events.publish(mutation.events);
                    }
                }
                if self.cancel_pending_invocations(&snapshots).await? {
                    published = true;
                }
                if published {
                    self.active.changed.notify_waiters();
                }
                self.runs.list_run_subtree(run_id.clone()).await?
            };
            if subtree.iter().all(|snapshot| {
                snapshot.run().state().is_terminal()
                    && snapshot
                        .tool_calls()
                        .iter()
                        .all(|tool_call| tool_call.state().is_terminal())
            }) {
                return subtree
                    .into_iter()
                    .find(|snapshot| snapshot.run().run_id() == &run_id)
                    .ok_or(RunError::RunNotFound);
            }
            if self.has_lost_execution(&subtree).await {
                return Err(RunError::CancellationFailed);
            }
            changed.await;
        }
    }

    pub(crate) async fn shutdown(&self) -> Result<(), RunError> {
        let run_ids = self
            .active
            .entries
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut cancellations = tokio::task::JoinSet::new();
        for run_id in run_ids {
            let service = self.clone();
            cancellations.spawn(async move { service.cancel(run_id).await });
        }

        let mut first_error = None;
        while let Some(result) = cancellations.join_next().await {
            match result {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    first_error.get_or_insert(error);
                }
                Err(_) => {
                    first_error.get_or_insert(RunError::CancellationFailed);
                }
            }
        }
        self.wait_until_idle().await;
        #[cfg(unix)]
        if let Some(registry) = &self.mcp_registry {
            if registry.shutdown().await.iter().any(Result::is_err) {
                first_error.get_or_insert(RunError::CancellationFailed);
            }
        }
        if let Some(error) = *self.active.failure.lock().await {
            first_error.get_or_insert(error);
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn wait_until_idle(&self) {
        loop {
            let changed = self.active.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.active.entries.lock().await.is_empty() {
                return;
            }
            changed.await;
        }
    }
}

fn empty_output() -> SubprocessOutput {
    SubprocessOutput {
        stdout: String::new(),
        stderr: String::new(),
        stdout_artifact: None,
        stderr_artifact: None,
        exit_code: None,
        spawn_error: None,
    }
}

fn preflight_failure_message(error: RunError) -> &'static str {
    match error {
        RunError::PathOutsideWorkspaceRoot => "path is outside workspace root",
        RunError::WorkspaceRootNotFound => "workspace root is unavailable",
        RunError::SessionNotFound => "session is unavailable",
        RunError::RunStoreUnavailable => "execution scope storage is unavailable",
        _ => "execution scope is invalid",
    }
}

impl RunOperations for RunService {
    async fn inspect_tool_call(
        &self,
        tool_call_id: ToolCallId,
        max_source_bytes: std::num::NonZeroUsize,
    ) -> Result<kiln_core::ToolCallInspection, kiln_core::ToolCallInspectionError> {
        kiln_core::ToolCallInspectionStore::inspect_tool_call(
            &self.store,
            &tool_call_id,
            max_source_bytes,
        )
        .await
    }
    fn start_run(
        &self,
        session_id: SessionId,
        idempotency_key: String,
        approval_policy: ApprovalPolicy,
        requested_scope: WorkspacePathScope,
    ) -> impl Future<Output = Result<kiln_core::StartRunMutation, RunError>> + Send {
        self.start(
            session_id,
            idempotency_key,
            approval_policy,
            requested_scope,
        )
    }

    fn start_child_run(
        &self,
        parent_run_id: RunId,
        task_id: Option<TaskId>,
        user_input_mode: RunInputMode,
        idempotency_key: String,
        approval_policy: ApprovalPolicy,
        requested_scope: WorkspacePathScope,
    ) -> impl Future<Output = Result<kiln_core::StartRunMutation, RunError>> + Send {
        self.start_child(
            parent_run_id,
            task_id,
            user_input_mode,
            idempotency_key,
            approval_policy,
            requested_scope,
        )
    }

    fn list_session_runs(
        &self,
        session_id: SessionId,
    ) -> impl Future<Output = Result<Vec<RunSnapshot>, RunError>> + Send {
        self.runs.list_session_runs(session_id)
    }

    fn get_run(&self, run_id: RunId) -> impl Future<Output = Result<RunSnapshot, RunError>> + Send {
        self.runs.get_run(run_id)
    }

    fn send_run_input(
        &self,
        command: SendRunInput,
    ) -> impl Future<Output = Result<kiln_core::SendRunInputMutation, RunError>> + Send {
        self.send_input(command)
    }

    fn react_to_run_activity(
        &self,
        command: ReactToRunActivity,
    ) -> impl Future<Output = Result<kiln_core::SendRunInputMutation, RunError>> + Send {
        self.react_to_activity(command)
    }

    fn cancel_run(
        &self,
        run_id: RunId,
    ) -> impl Future<Output = Result<RunSnapshot, RunError>> + Send {
        self.cancel(run_id)
    }

    fn decide_approval(
        &self,
        tool_call_id: ToolCallId,
        decision: kiln_core::ApprovalState,
        idempotency_key: String,
    ) -> impl Future<Output = Result<kiln_core::ApprovalDecisionMutation, RunError>> + Send {
        let service = self.clone();
        async move {
            let (run, _tool_call) = service
                .store
                .get_tool_call(&tool_call_id)
                .await
                .map_err(|_| RunError::RunStoreUnavailable)?
                .ok_or(RunError::ApprovalNotFound)?;
            let snapshot = service
                .store
                .get_run(run.run_id())
                .await
                .map_err(|_| RunError::RunStoreUnavailable)?
                .ok_or(RunError::RunNotFound)?;
            let approval = snapshot
                .approval_for_tool_call(&tool_call_id)
                .ok_or(RunError::ApprovalNotFound)?;
            let result = {
                let _sequence = service.commit_sequence.lock().await;
                let result = service
                    .runs
                    .decide_approval(approval.approval_id().clone(), decision, idempotency_key)
                    .await?;
                service.events.publish(result.events.clone());
                if result.value.run().state() == RunState::Running
                    && result.value.model_invocations().is_empty()
                {
                    service
                        .spawn_active(result.value.run().run_id().clone(), false)
                        .await;
                }
                service
                    .approval_changed
                    .send_modify(|revision| *revision = revision.wrapping_add(1));
                service.active.changed.notify_waiters();
                result
            };
            Ok(result)
        }
    }
}

impl ArtifactOperations for RunService {
    fn get_artifact(
        &self,
        content_hash: ContentHash,
    ) -> impl Future<Output = Result<ArtifactDownload, ArtifactFetchError>> + Send {
        let service = self.clone();
        async move {
            let artifact = service
                .store
                .get_artifact_metadata(&content_hash)
                .await
                .map_err(|_| ArtifactFetchError::Unavailable)?
                .ok_or(ArtifactFetchError::NotFound)?;
            let artifacts = service.artifacts.clone();
            let bytes = tokio::task::spawn_blocking(move || artifacts.read(&content_hash))
                .await
                .map_err(|_| ArtifactFetchError::Unavailable)?
                .map_err(|_| ArtifactFetchError::Unavailable)?
                .ok_or(ArtifactFetchError::Unavailable)?;
            ArtifactDownload::new(artifact, bytes)
        }
    }

    fn upload_artifact(
        &self,
        session_id: kiln_core::SessionId,
        bytes: Vec<u8>,
        media_type: String,
    ) -> impl Future<Output = Result<kiln_core::Artifact, ArtifactUploadError>> + Send {
        let service = self.clone();
        async move {
            let probe_hash = ContentHash::parse(&"0".repeat(64))
                .map_err(|_| ArtifactUploadError::InvalidMediaType)?;
            Artifact::new(
                probe_hash,
                &media_type,
                u64::try_from(bytes.len()).map_err(|_| ArtifactUploadError::TooLarge)?,
            )
            .map_err(|_| ArtifactUploadError::InvalidMediaType)?;
            let artifact = service
                .artifacts
                .store(&bytes, &media_type)
                .map_err(|_| ArtifactUploadError::Unavailable)?;
            service
                .store
                .register_artifact_owner(&session_id, &artifact)
                .await
                .map_err(|_| ArtifactUploadError::Unavailable)?;
            Ok(artifact)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kiln_infrastructure::DeterministicOutcome;

    #[tokio::test]
    async fn a_cancelled_parent_exits_when_descendant_execution_ownership_is_lost() {
        let data_directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(data_directory.path()).await.unwrap();
        let workspace_id = kiln_core::WorkspaceId::parse("wsp_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let workspace_root_id =
            kiln_core::WorkspaceRootId::parse("wrt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let workspace_root = kiln_core::WorkspaceRoot::new(
            workspace_root_id.clone(),
            "main".to_owned(),
            "/main".to_owned(),
            kiln_core::DiscoveredWorkspaceRoot {
                canonical_path: "/main".to_owned(),
                git_common_directory_path: "/main/.git".to_owned(),
                filesystem_identity: kiln_core::FilesystemIdentity::new("test:/main").unwrap(),
            },
            0,
            kiln_core::WorkspaceRootState::Available,
        )
        .unwrap();
        store
            .create_workspace(
                &kiln_core::Workspace::new(
                    workspace_id.clone(),
                    "Workspace".to_owned(),
                    vec![workspace_root],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let session = kiln_core::Session::new(
            SessionId::parse("ses_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            workspace_id.clone(),
        );
        store
            .create_session(
                &session,
                &kiln_core::SessionEvent::session_created(
                    kiln_core::EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                    session.id().clone(),
                    workspace_id,
                ),
            )
            .await
            .unwrap();
        let runs = RunApplication::new(store.clone(), UlidIdGenerator);
        let scope = WorkspacePathScope::new(workspace_root_id, ".").unwrap();
        let root = runs
            .start_root_run(
                session.id().clone(),
                "lost-owner-root".to_owned(),
                ApprovalPolicy::FullAccess,
                scope.clone(),
            )
            .await
            .unwrap();
        let root_id = root.value.run().run_id().clone();
        let child = runs
            .start_child_run(
                root_id.clone(),
                None,
                RunInputMode::Interactive,
                "lost-owner-child".to_owned(),
                ApprovalPolicy::FullAccess,
                scope,
            )
            .await
            .unwrap();
        let child_id = child.value.run().run_id().clone();
        runs.begin_execution(child_id.clone()).await.unwrap();
        assert_eq!(
            runs.request_cancellation(root_id.clone())
                .await
                .unwrap()
                .value
                .run()
                .state(),
            RunState::Cancelling
        );
        runs.request_cancellation(child_id).await.unwrap();
        let artifacts = FileArtifactStore::open(data_directory.path()).unwrap();
        let service = RunService::new(
            runs,
            DeterministicSubprocessExecutor::new(DeterministicOutcome::Success),
            EventBroadcaster::default(),
            store,
            artifacts,
        );
        let (cancellation, receiver) = oneshot::channel();
        service
            .active
            .entries
            .lock()
            .await
            .insert(root_id.clone(), ActiveRun { cancellation: None });
        cancellation.send(()).unwrap();
        let execution_service = service.clone();
        let execution = tokio::spawn(async move {
            execution_service
                .execute(
                    root_id.clone(),
                    receiver,
                    execution_service.approval_changed.subscribe(),
                    false,
                )
                .await;
        });

        assert_eq!(service.shutdown().await, Err(RunError::CancellationFailed));
        execution.await.unwrap();
        assert!(service.active.entries.lock().await.is_empty());
        assert_eq!(
            *service.active.failure.lock().await,
            Some(RunError::CancellationFailed)
        );
    }

    #[tokio::test]
    async fn execution_failure_exits_the_registry_and_shutdown_returns_the_error() {
        let data_directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(data_directory.path()).await.unwrap();
        let artifacts = FileArtifactStore::open(data_directory.path()).unwrap();
        let service = RunService::new(
            RunApplication::new(store.clone(), UlidIdGenerator),
            DeterministicSubprocessExecutor::new(DeterministicOutcome::Success),
            EventBroadcaster::default(),
            store,
            artifacts,
        );
        let run_id = RunId::parse("run_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let (cancellation, _) = oneshot::channel();
        service.active.entries.lock().await.insert(
            run_id.clone(),
            ActiveRun {
                cancellation: Some(cancellation),
            },
        );

        service
            .execute(
                run_id.clone(),
                oneshot::channel().1,
                service.approval_changed.subscribe(),
                true,
            )
            .await;

        assert!(!service.active.entries.lock().await.contains_key(&run_id));
        assert_eq!(
            *service.active.failure.lock().await,
            Some(RunError::RunNotFound)
        );
        assert_eq!(service.shutdown().await, Err(RunError::RunNotFound));
    }
}
