use std::{collections::HashMap, sync::Arc};

use kiln_core::{
    RunApplication, RunError, RunId, RunMutation, RunSnapshot, RunState, SessionId,
    StartRunDisposition, SubprocessExecution, SubprocessExecutor, SubprocessOutput, ToolCallId,
};
use kiln_infrastructure::{DeterministicSubprocessExecutor, SqliteStore, UlidIdGenerator};
use kiln_server::{EventBroadcaster, RunOperations};
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
}

impl RunService {
    pub(crate) fn new(
        runs: RunApplication<SqliteStore, UlidIdGenerator>,
        executor: DeterministicSubprocessExecutor,
        events: EventBroadcaster,
    ) -> Self {
        Self {
            runs: Arc::new(runs),
            executor,
            events,
            commit_sequence: Arc::new(Mutex::new(())),
            active: Arc::new(ActiveRuns::default()),
        }
    }

    async fn start(
        &self,
        session_id: SessionId,
        idempotency_key: String,
    ) -> Result<kiln_core::StartRunMutation, RunError> {
        let (value, disposition) = {
            let _sequence = self.commit_sequence.lock().await;
            let mutation = self
                .runs
                .start_root_run(session_id, idempotency_key)
                .await?;
            let value = mutation.value.clone();
            let disposition = mutation.disposition;
            self.events.publish(mutation.events);
            (value, disposition)
        };

        if disposition == StartRunDisposition::Created {
            let run_id = value.run().run_id().clone();
            let (cancellation_sender, cancellation_receiver) = oneshot::channel();
            let (completion_sender, completion_receiver) = watch::channel(None);
            self.active.entries.lock().await.insert(
                run_id.clone(),
                ActiveRun {
                    cancellation: Some(cancellation_sender),
                    completion: completion_receiver,
                },
            );
            let service = self.clone();
            tokio::spawn(async move {
                service
                    .execute(run_id, cancellation_receiver, completion_sender)
                    .await;
            });
        }

        Ok(kiln_core::StartRunMutation::new(
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
    ) {
        let result = match self.execute_inner(run_id.clone(), cancellation).await {
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
    ) -> Result<RunSnapshot, RunError> {
        if cancellation.try_recv().is_ok() {
            return self.runs.get_run(run_id).await;
        }

        let running = {
            let _sequence = self.commit_sequence.lock().await;
            match self.runs.begin_execution(run_id.clone()).await {
                Ok(RunMutation {
                    value: running,
                    events,
                }) => {
                    self.events.publish(events);
                    running
                }
                Err(RunError::InvalidTransition) => {
                    return self.runs.get_run(run_id).await;
                }
                Err(error) => return Err(error),
            }
        };

        let tool_call_id = running
            .tool_calls()
            .first()
            .map(|tool_call| tool_call.tool_call_id().clone())
            .ok_or(RunError::InvalidTransition)?;

        {
            let _sequence = self.commit_sequence.lock().await;
            let snapshot = self.runs.get_run(run_id.clone()).await?;
            match snapshot.run().state() {
                RunState::Running => {
                    let RunMutation { events, .. } =
                        self.runs.begin_tool_call(tool_call_id.clone()).await?;
                    self.events.publish(events);
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

        let execution = self
            .executor
            .execute(async move {
                let _ = cancellation.await;
            })
            .await;

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
        exit_code: None,
        spawn_error: None,
    }
}

impl RunOperations for RunService {
    fn start_run(
        &self,
        session_id: SessionId,
        idempotency_key: String,
    ) -> impl Future<Output = Result<kiln_core::StartRunMutation, RunError>> + Send {
        self.start(session_id, idempotency_key)
    }

    fn get_run(&self, run_id: RunId) -> impl Future<Output = Result<RunSnapshot, RunError>> + Send {
        self.runs.get_run(run_id)
    }

    fn cancel_run(
        &self,
        run_id: RunId,
    ) -> impl Future<Output = Result<RunSnapshot, RunError>> + Send {
        self.cancel(run_id)
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
        let service = RunService::new(
            RunApplication::new(store, UlidIdGenerator),
            DeterministicSubprocessExecutor::new(DeterministicOutcome::Success),
            EventBroadcaster::default(),
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
            .execute(run_id.clone(), oneshot::channel().1, completion_sender)
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
