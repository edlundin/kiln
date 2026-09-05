use std::{collections::HashMap, sync::Arc};

use kiln_core::{
    ApprovalPolicy, Artifact, ContentHash, INLINE_TOOL_OUTPUT_LIMIT, RunApplication, RunError,
    RunId, RunInputMode, RunMutation, RunSnapshot, RunState, RunStore, SendRunInput, SessionId,
    SessionStore, StartRunDisposition, SubprocessExecution, SubprocessExecutor, SubprocessOutput,
    SubprocessRequest, TOOL_OUTPUT_MEDIA_TYPE, TaskId, ToolCallId, WorkspacePathScope,
    WorkspaceStore,
};
use kiln_infrastructure::{
    DeterministicSubprocessExecutor, FileArtifactStore, SqliteStore, UlidIdGenerator,
    validate_subprocess_request,
};
use kiln_server::{
    ArtifactDownload, ArtifactFetchError, ArtifactOperations, EventBroadcaster, RunOperations,
};
use tokio::sync::{Mutex, Notify, oneshot, watch};

struct ActiveRun {
    cancellation: Option<oneshot::Sender<()>>,
    completion: watch::Receiver<Option<Result<RunSnapshot, RunError>>>,
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
            events,
            commit_sequence: Arc::new(Mutex::new(())),
            active: Arc::new(ActiveRuns::default()),
            store,
            artifacts,
            approval_changed: watch::channel(0).0,
        }
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
            let mutation = self
                .runs
                .start_root_run(
                    session_id,
                    idempotency_key,
                    approval_policy,
                    requested_scope,
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
            let mutation = self
                .runs
                .start_child_run(
                    parent_run_id,
                    task_id,
                    user_input_mode,
                    idempotency_key,
                    approval_policy,
                    requested_scope,
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
        let value = mutation.value.clone();
        let disposition = mutation.disposition;
        self.events.publish(mutation.events);
        Ok(kiln_core::SendRunInputMutation::new(
            value,
            Vec::new(),
            disposition,
        ))
    }

    async fn execute(
        &self,
        run_id: RunId,
        cancellation: oneshot::Receiver<()>,
        completion: watch::Sender<Option<Result<RunSnapshot, RunError>>>,
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
        completion.send_replace(Some(result));
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
        if cancellation.try_recv().is_ok() {
            return self.runs.get_run(run_id).await;
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

        if running.run().state() == RunState::WaitingForApproval {
            loop {
                let changed = approval_revision.changed();
                tokio::select! {
                    _ = &mut cancellation => {
                        let _sequence = self.commit_sequence.lock().await;
                        let mutation = self.runs.request_cancellation(run_id.clone()).await?;
                        self.events.publish(mutation.events.clone());
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
            let _sequence = self.commit_sequence.lock().await;
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
                    let RunMutation { value, events } = self
                        .runs
                        .finish_denied_execution(run_id, tool_call_id)
                        .await?;
                    self.events.publish(events);
                    return Ok(value);
                }
                RunState::Cancelling => {
                    return self
                        .finish_cancellation(run_id, tool_call_id, empty_output())
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

        let _sequence = self.commit_sequence.lock().await;
        let snapshot = self.runs.get_run(run_id.clone()).await?;
        match (snapshot.run().state(), execution) {
            (RunState::Running, SubprocessExecution::Finished(output)) => {
                let RunMutation {
                    value: terminal,
                    events,
                } = self
                    .runs
                    .finish_execution(run_id, tool_call_id, output)
                    .await?;
                self.events.publish(events);
                Ok(terminal)
            }
            (
                RunState::Cancelling,
                SubprocessExecution::Finished(output) | SubprocessExecution::Cancelled(output),
            ) => self.finish_cancellation(run_id, tool_call_id, output).await,
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
        let scope = snapshot
            .tool_call(tool_call_id)
            .ok_or(RunError::RunNotFound)?
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
        let output = SubprocessOutput::spawn_failure(message);
        let _sequence = self.commit_sequence.lock().await;
        let snapshot = self.runs.get_run(run_id.clone()).await?;
        match snapshot.run().state() {
            RunState::Running => {
                let RunMutation {
                    value: terminal,
                    events,
                } = self
                    .runs
                    .finish_execution(run_id, tool_call_id, output)
                    .await?;
                self.events.publish(events);
                Ok(terminal)
            }
            RunState::Cancelling => self.finish_cancellation(run_id, tool_call_id, output).await,
            RunState::Cancelled | RunState::Completed | RunState::Failed => Ok(snapshot),
            _ => Err(RunError::InvalidTransition),
        }
    }

    async fn spawn_active(&self, run_id: RunId, initialize: bool) {
        let mut entries = self.active.entries.lock().await;
        if entries.contains_key(&run_id) {
            return;
        }
        let (cancellation_sender, cancellation_receiver) = oneshot::channel();
        let (completion_sender, completion_receiver) = watch::channel(None);
        entries.insert(
            run_id.clone(),
            ActiveRun {
                cancellation: Some(cancellation_sender),
                completion: completion_receiver,
            },
        );
        drop(entries);
        let service = self.clone();
        let approval_revision = self.approval_changed.subscribe();
        tokio::spawn(async move {
            service
                .execute(
                    run_id,
                    cancellation_receiver,
                    completion_sender,
                    approval_revision,
                    initialize,
                )
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
        Ok(terminal)
    }

    async fn cancel(&self, run_id: RunId) -> Result<RunSnapshot, RunError> {
        let requested = {
            let _sequence = self.commit_sequence.lock().await;
            let RunMutation { value, events } =
                self.runs.request_cancellation(run_id.clone()).await?;
            self.events.publish(events);
            value
        };

        let state = requested.run().state();
        if matches!(state, RunState::Completed | RunState::Failed) {
            return Ok(requested);
        }

        let active = {
            let mut entries = self.active.entries.lock().await;
            entries
                .get_mut(&run_id)
                .map(|active| (active.cancellation.take(), active.completion.clone()))
        };

        let Some((cancellation, mut completion)) = active else {
            let current = self.runs.get_run(run_id).await?;
            return if matches!(
                current.run().state(),
                RunState::Completed | RunState::Failed | RunState::Cancelled
            ) {
                Ok(current)
            } else {
                Err(RunError::CancellationFailed)
            };
        };
        if let Some(cancellation) = cancellation {
            let _ = cancellation.send(());
        }
        if state == RunState::Cancelled {
            return Ok(requested);
        }

        loop {
            if let Some(result) = completion.borrow().clone() {
                return result;
            }
            completion
                .changed()
                .await
                .map_err(|_| RunError::CancellationFailed)?;
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
                if result.value.run().state() == RunState::Running {
                    service
                        .spawn_active(result.value.run().run_id().clone(), false)
                        .await;
                }
                service
                    .approval_changed
                    .send_modify(|revision| *revision = revision.wrapping_add(1));
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use kiln_infrastructure::DeterministicOutcome;

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
        let (completion_sender, mut completion) = watch::channel(None);
        service.active.entries.lock().await.insert(
            run_id.clone(),
            ActiveRun {
                cancellation: Some(cancellation),
                completion: completion.clone(),
            },
        );

        service
            .execute(
                run_id.clone(),
                oneshot::channel().1,
                completion_sender,
                service.approval_changed.subscribe(),
                true,
            )
            .await;

        assert!(!service.active.entries.lock().await.contains_key(&run_id));
        completion.changed().await.unwrap();
        assert_eq!(
            completion.borrow().clone(),
            Some(Err(RunError::RunNotFound))
        );
        assert_eq!(service.shutdown().await, Err(RunError::RunNotFound));
    }
}
