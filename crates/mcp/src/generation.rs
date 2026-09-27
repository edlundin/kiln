//! One trusted runtime owner per durable generation; no tool execution API.

use std::sync::Arc;

use kiln_core::{
    McpDefinitionError, McpDefinitionLimits, McpDefinitionStore, McpGenerationId, McpInstanceClaim,
    McpInstanceError, McpInstanceKey, McpInstanceRecord, McpInstanceStore, McpInstanceTransition,
    SharedMcpTransport,
};
use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
    time::Instant,
};

use crate::{ProtocolVersion, StdioProcess, StdioProcessConfig, start_stdio_client};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StdioGenerationError {
    Definition(McpDefinitionError),
    Store(McpInstanceError),
    UnsupportedTransport,
    Existing,
    Spawn,
    Startup,
    StartupDeadline,
    Cleanup,
    Ended,
    WorkerLost,
}

#[derive(Clone)]
enum Status {
    Starting,
    Ready(McpInstanceRecord),
    Finished(Result<McpInstanceRecord, StdioGenerationError>),
}

/// Host-resolved, independently authorized launch inputs for one scoped key.
/// A definition or an acquired claim alone must never authorize constructing this.
pub struct StdioGenerationLaunch {
    pub key: McpInstanceKey,
    pub definition_version: u64,
    pub generation: McpGenerationId,
    pub definition_limits: McpDefinitionLimits,
    pub process: StdioProcessConfig,
    pub startup_deadline: Instant,
}

/// Dropping this handle requests stop but leaves the worker alive to reap and
/// journal cleanup. Orderly daemon shutdown must await `stop` before runtime exit.
/// Readiness is lifecycle information, never permission to invoke an MCP tool.
pub struct StdioGeneration {
    stop: Option<oneshot::Sender<()>>,
    status: watch::Receiver<Status>,
    worker: Option<JoinHandle<Result<McpInstanceRecord, StdioGenerationError>>>,
}

pub(crate) struct GenerationObserver {
    status: watch::Receiver<Status>,
}

impl GenerationObserver {
    pub(crate) fn can_release(&self) -> bool {
        // Cleanup failure and a lost worker provide no proof that ownership can
        // be forgotten. Keep those entries available to shutdown/reporting.
        matches!(&*self.status.borrow(), Status::Finished(result)
            if !matches!(result, Err(StdioGenerationError::Cleanup | StdioGenerationError::WorkerLost)))
    }

    pub(crate) fn is_finished(&self) -> bool {
        matches!(*self.status.borrow(), Status::Finished(_)) || self.status.has_changed().is_err()
    }

    pub(crate) async fn wait_ready(&mut self) -> Result<McpInstanceRecord, StdioGenerationError> {
        loop {
            match self.status.borrow_and_update().clone() {
                Status::Starting => {}
                Status::Ready(record) => return Ok(record),
                Status::Finished(result) => {
                    return Err(result.err().unwrap_or(StdioGenerationError::Ended));
                }
            }
            self.status
                .changed()
                .await
                .map_err(|_| StdioGenerationError::WorkerLost)?;
        }
    }

    pub(crate) async fn wait_finished(
        &mut self,
    ) -> Result<McpInstanceRecord, StdioGenerationError> {
        loop {
            if let Status::Finished(result) = self.status.borrow_and_update().clone() {
                return result;
            }
            self.status
                .changed()
                .await
                .map_err(|_| StdioGenerationError::WorkerLost)?;
        }
    }
}

impl StdioGeneration {
    pub fn spawn<S>(store: Arc<S>, launch: StdioGenerationLaunch) -> Self
    where
        S: McpInstanceStore + McpDefinitionStore + 'static,
    {
        let (stop, stopped) = oneshot::channel();
        let (status, receiver) = watch::channel(Status::Starting);
        let worker = tokio::spawn(async move {
            let result = run(store.as_ref(), launch, stopped, &status).await;
            status.send_replace(Status::Finished(result.clone()));
            result
        });
        Self {
            stop: Some(stop),
            status: receiver,
            worker: Some(worker),
        }
    }

    pub async fn wait_ready(&mut self) -> Result<McpInstanceRecord, StdioGenerationError> {
        self.observer().wait_ready().await
    }

    pub(crate) fn observer(&self) -> GenerationObserver {
        GenerationObserver {
            status: self.status.clone(),
        }
    }

    pub async fn stop(mut self) -> Result<McpInstanceRecord, StdioGenerationError> {
        self.request_stop();
        self.worker
            .take()
            .expect("generation worker")
            .await
            .map_err(|_| StdioGenerationError::WorkerLost)?
    }

    pub(crate) fn request_stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

impl Drop for StdioGeneration {
    fn drop(&mut self) {
        self.request_stop();
    }
}

async fn transition<S: McpInstanceStore>(
    store: &S,
    record: &McpInstanceRecord,
    next: McpInstanceTransition,
) -> Result<McpInstanceRecord, StdioGenerationError> {
    store
        .transition_mcp_instance(record, next)
        .await
        .map_err(StdioGenerationError::Store)
}

async fn stopped<S: McpInstanceStore>(
    store: &S,
    record: &McpInstanceRecord,
) -> Result<McpInstanceRecord, StdioGenerationError> {
    let record = transition(store, record, McpInstanceTransition::RequestStop).await?;
    transition(store, &record, McpInstanceTransition::Stopped).await
}

async fn run<S: McpInstanceStore + McpDefinitionStore>(
    store: &S,
    launch: StdioGenerationLaunch,
    mut stop: oneshot::Receiver<()>,
    status: &watch::Sender<Status>,
) -> Result<McpInstanceRecord, StdioGenerationError> {
    let definition = store
        .get_mcp_definition(launch.key.definition_id(), launch.definition_limits)
        .await
        .map_err(StdioGenerationError::Definition)?
        .ok_or(StdioGenerationError::Store(
            McpInstanceError::DefinitionNotFound,
        ))?;
    if definition.version != launch.definition_version {
        return Err(StdioGenerationError::Store(
            McpInstanceError::DefinitionChanged,
        ));
    }
    if !matches!(
        definition.definition.server().transport,
        SharedMcpTransport::Stdio { .. }
    ) {
        return Err(StdioGenerationError::UnsupportedTransport);
    }
    let record = match store
        .claim_mcp_instance(
            &launch.key,
            launch.definition_version,
            &launch.generation,
            launch.definition_limits,
        )
        .await
        .map_err(StdioGenerationError::Store)?
    {
        McpInstanceClaim::Acquired(record) => record,
        McpInstanceClaim::Existing(_) => return Err(StdioGenerationError::Existing),
    };
    // Do not cancel a store transaction. Once its outcome is known, an abandoned
    // request can terminate without ever creating an external process.
    if !matches!(stop.try_recv(), Err(oneshot::error::TryRecvError::Empty)) {
        return stopped(store, &record).await;
    }
    if Instant::now() >= launch.startup_deadline {
        transition(store, &record, McpInstanceTransition::StartupFailed).await?;
        return Err(StdioGenerationError::StartupDeadline);
    }
    let process = match StdioProcess::spawn(launch.process) {
        Ok(process) => process,
        Err(_) => {
            transition(store, &record, McpInstanceTransition::StartupFailed).await?;
            return Err(StdioGenerationError::Spawn);
        }
    };
    let (transport, cleanup) = process.into_managed();
    let startup = tokio::select! {
        biased;
        _ = &mut stop => None,
        result = tokio::time::timeout_at(launch.startup_deadline,
            start_stdio_client((), transport, definition.definition.protocol())) => Some(match result {
                Ok(Ok(client)) => Ok(client),
                Ok(Err(_)) => Err(StdioGenerationError::Startup),
                Err(_) => Err(StdioGenerationError::StartupDeadline),
            }),
    };
    let client = match startup {
        Some(Ok(client)) => client,
        other => {
            if cleanup.finish().await.is_err() {
                let _ = transition(store, &record, McpInstanceTransition::ConnectionLost).await;
                return Err(StdioGenerationError::Cleanup);
            }
            return match other {
                None => stopped(store, &record).await,
                Some(Err(error)) => {
                    transition(store, &record, McpInstanceTransition::StartupFailed).await?;
                    Err(error)
                }
                Some(Ok(_)) => unreachable!(),
            };
        }
    };
    let protocol = client
        .peer_info()
        .and_then(|info| ProtocolVersion::parse(info.protocol_version.as_str()).ok());
    let ready = match protocol {
        Some(protocol) => transition(store, &record, McpInstanceTransition::Ready(protocol)).await,
        None => Err(StdioGenerationError::Startup),
    };
    let ready = match ready {
        Ok(ready) => ready,
        Err(error) => {
            let _ = client.cancel().await;
            if cleanup.finish().await.is_err() {
                let _ = transition(store, &record, McpInstanceTransition::ConnectionLost).await;
                return Err(StdioGenerationError::Cleanup);
            }
            // If storage is unavailable/conflicted this remains nonterminal;
            // recovery must reconcile it, never infer successful cleanup.
            let _ = transition(store, &record, McpInstanceTransition::StartupFailed).await;
            return Err(error);
        }
    };
    status.send_replace(Status::Ready(ready.clone()));
    let cancellation = client.cancellation_token();
    let waiting = client.waiting();
    tokio::pin!(waiting);
    let requested = tokio::select! {
        biased;
        _ = &mut stop => { cancellation.cancel(); true },
        _ = &mut waiting => false,
    };
    // A journal failure must not skip process cleanup. Record the intent first,
    // retain its result, then wait for SDK ownership to return and reap.
    let terminal_base = transition(
        store,
        &ready,
        if requested {
            McpInstanceTransition::RequestStop
        } else {
            McpInstanceTransition::ConnectionLost
        },
    )
    .await;
    if requested {
        let _ = waiting.await;
    }
    if cleanup.finish().await.is_err() {
        return Err(StdioGenerationError::Cleanup);
    }
    transition(store, &terminal_base?, McpInstanceTransition::Stopped).await
}
