use std::sync::Arc;

use kiln_core::{
    RunApplication, RunError, RunId, RunMutation, RunSnapshot, SessionId, SubprocessExecutor,
};
use kiln_infrastructure::{DeterministicSubprocessExecutor, SqliteStore, UlidIdGenerator};
use kiln_server::{EventBroadcaster, RunOperations};

#[derive(Clone)]
pub(crate) struct RunService {
    runs: Arc<RunApplication<SqliteStore, UlidIdGenerator>>,
    executor: DeterministicSubprocessExecutor,
    events: EventBroadcaster,
    commit_sequence: Arc<tokio::sync::Mutex<()>>,
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
            commit_sequence: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    async fn start(&self, session_id: SessionId) -> Result<RunSnapshot, RunError> {
        let value = {
            let _sequence = self.commit_sequence.lock().await;
            let RunMutation { value, events } = self.runs.create_root_run(session_id).await?;
            self.events.publish(events);
            value
        };

        let run_id = value.run().run_id().clone();
        let service = self.clone();
        tokio::spawn(async move {
            service.execute(run_id).await;
        });

        Ok(value)
    }

    async fn execute(&self, run_id: RunId) {
        let running = {
            let _sequence = self.commit_sequence.lock().await;
            let RunMutation {
                value: running,
                events,
            } = match self.runs.begin_execution(run_id.clone()).await {
                Ok(mutation) => mutation,
                Err(error) => {
                    eprintln!("kilnd: cannot begin Run execution: {error:?}");
                    return;
                }
            };
            self.events.publish(events);
            running
        };

        let Some(tool_call_id) = running
            .tool_calls()
            .first()
            .map(|tool_call| tool_call.tool_call_id().clone())
        else {
            eprintln!("kilnd: running Run has no ToolCall");
            return;
        };

        {
            let _sequence = self.commit_sequence.lock().await;
            let RunMutation { events, .. } =
                match self.runs.begin_tool_call(tool_call_id.clone()).await {
                    Ok(mutation) => mutation,
                    Err(error) => {
                        eprintln!("kilnd: cannot begin ToolCall execution: {error:?}");
                        return;
                    }
                };
            self.events.publish(events);
        }

        let output = self.executor.execute().await;
        {
            let _sequence = self.commit_sequence.lock().await;
            let RunMutation { events, .. } = match self
                .runs
                .finish_execution(run_id, tool_call_id, output)
                .await
            {
                Ok(mutation) => mutation,
                Err(error) => {
                    eprintln!("kilnd: cannot finish Run execution: {error:?}");
                    return;
                }
            };
            self.events.publish(events);
        }
    }
}

impl RunOperations for RunService {
    fn start_run(
        &self,
        session_id: SessionId,
    ) -> impl Future<Output = Result<RunSnapshot, RunError>> + Send {
        self.start(session_id)
    }

    fn get_run(&self, run_id: RunId) -> impl Future<Output = Result<RunSnapshot, RunError>> + Send {
        self.runs.get_run(run_id)
    }
}
