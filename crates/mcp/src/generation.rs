//! One trusted runtime owner per generation and serialized claimed dispatch.

use std::sync::Arc;

use kiln_core::{
    McpDefinitionError, McpDefinitionLimits, McpDefinitionStore, McpDispatchPermit,
    McpGenerationId, McpInstanceClaim, McpInstanceError, McpInstanceKey, McpInstanceRecord,
    McpInstanceStore, McpInstanceTransition, McpInvocationState, McpInvocationStore,
    SharedMcpTransport,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::Instant,
};

use crate::dispatch::{DispatchOutcome, DispatchRequest, send_once};
use crate::{
    ProtocolVersion, StdioCallError, StdioCallLimits, StdioCallResult, StdioProcess,
    StdioProcessConfig, start_stdio_client,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StdioGenerationError {
    Definition(McpDefinitionError),
    Store(McpInstanceError),
    Invocation(kiln_core::McpInvocationError),
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
    pub host_binding_version: Option<kiln_core::McpHostBindingVersion>,
    pub generation: McpGenerationId,
    pub definition_limits: McpDefinitionLimits,
    pub process: StdioProcessConfig,
    pub startup_deadline: Instant,
}

/// Dropping this handle requests stop but leaves the worker alive to reap and
/// journal cleanup. Orderly daemon shutdown must await `stop` before runtime exit.
/// Readiness is lifecycle information, never permission to invoke an MCP tool.
pub struct StdioGeneration {
    calls: mpsc::Sender<DispatchRequest>,
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
        S: McpInstanceStore + McpDefinitionStore + McpInvocationStore + 'static,
    {
        let (stop, stopped) = oneshot::channel();
        let (status, receiver) = watch::channel(Status::Starting);
        // Durable ownership permits at most one in-flight dispatch per generation.
        let (calls, requests) = mpsc::channel(1);
        let worker = tokio::spawn(async move {
            let result = run(store.as_ref(), launch, stopped, requests, &status).await;
            status.send_replace(Status::Finished(result.clone()));
            result
        });
        Self {
            calls,
            stop: Some(stop),
            status: receiver,
            worker: Some(worker),
        }
    }

    /// Consume a durable permit. Dropping the waiter cancels this operation and
    /// causes the worker to retire the generation if a send may have occurred.
    pub async fn dispatch(
        &self,
        permit: McpDispatchPermit,
        limits: StdioCallLimits,
        cancellation: oneshot::Receiver<()>,
    ) -> Result<StdioCallResult, StdioCallError> {
        dispatch_to(self.calls.clone(), permit, limits, cancellation)
            .await?
            .result
    }

    /// Capture the committed response using ordinary ToolCall inline/artifact
    /// boundaries. The archive callback stores the exact bytes as
    /// TOOL_OUTPUT_MEDIA_TYPE; this method never retries external work.
    pub async fn dispatch_tool_call<F, Fut, E>(
        &self,
        permit: McpDispatchPermit,
        limits: StdioCallLimits,
        cancellation: oneshot::Receiver<()>,
        archive: F,
    ) -> Result<kiln_core::ToolCallResult, kiln_core::RunError>
    where
        F: FnOnce(Vec<u8>) -> Fut,
        Fut: std::future::Future<Output = Result<kiln_core::Artifact, E>>,
    {
        let outcome = dispatch_to(self.calls.clone(), permit, limits, cancellation)
            .await
            .map_err(|_| kiln_core::RunError::RunStoreUnavailable)?;
        crate::output::capture(outcome, archive).await
    }

    pub(crate) fn dispatch_sender(&self) -> mpsc::Sender<DispatchRequest> {
        self.calls.clone()
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

async fn run<S: McpInstanceStore + McpDefinitionStore + McpInvocationStore>(
    store: &S,
    launch: StdioGenerationLaunch,
    mut stop: oneshot::Receiver<()>,
    mut requests: mpsc::Receiver<DispatchRequest>,
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
        .claim_mcp_instance_with_host_bindings(
            &launch.key,
            launch.definition_version,
            &launch.generation,
            launch.host_binding_version.as_ref(),
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
    let catalog_epochs = crate::catalog_state::CatalogEpochs::default();
    let mut catalog_cache = crate::catalog_cache::CatalogCache::default();
    let startup = tokio::select! {
        biased;
        _ = &mut stop => None,
        result = tokio::time::timeout_at(launch.startup_deadline,
            start_stdio_client(catalog_epochs.clone(), transport, definition.definition.protocol())) => Some(match result {
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
    let peer = client.peer().clone();
    let waiting = client.waiting();
    tokio::pin!(waiting);
    let mut retiring = None;
    let requested = loop {
        let mut call = tokio::select! {
            biased;
            _ = &mut stop => { cancellation.cancel(); break true },
            _ = &mut waiting => break false,
            Some(call) = requests.recv() => call,
        };
        let mut exit = None;
        let result = if call.permit.record().generation != ready.generation {
            Err(StdioCallError::Rejected)
        } else if call.reply.is_closed()
            || !matches!(
                call.cancellation.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            )
        {
            Err(StdioCallError::CancelledBeforeSend)
        } else if Instant::now() >= call.limits.deadline {
            Err(StdioCallError::DeadlineBeforeSend)
        } else {
            // Definition updates and lifecycle changes since permit acquisition
            // may prevent sending; the immutable claim itself never gets replayed.
            let current = store.get_mcp_instance(&ready.key).await;
            let definition = store
                .get_mcp_definition(ready.key.definition_id(), launch.definition_limits)
                .await;
            if !matches!(current, Ok(Some(ref current)) if current.generation == ready.generation
                && current.state_version == ready.state_version)
                || !matches!(definition, Ok(Some(ref definition)) if definition.version == ready.definition_version
                    && definition.definition.server().enabled)
            {
                Err(StdioCallError::Rejected)
            } else {
                tokio::select! {
                    biased;
                    _ = &mut stop => { exit = Some(true); Err(StdioCallError::Interrupted) },
                    _ = &mut waiting => { exit = Some(false); Err(StdioCallError::Interrupted) },
                    _ = &mut call.cancellation => Err(StdioCallError::Interrupted),
                    _ = call.reply.closed() => Err(StdioCallError::Interrupted),
                    _ = tokio::time::sleep_until(call.limits.deadline) => Err(StdioCallError::Interrupted),
                    result = send_once(&peer, call.permit.request().command(), &call.permit.record().generation, &catalog_epochs, &mut catalog_cache, &call.limits) => result,
                }
            }
        };
        let state = match &result {
            Ok(result) if result.is_error => McpInvocationState::Failed,
            Ok(_) => McpInvocationState::Completed,
            Err(StdioCallError::CancelledBeforeSend) => McpInvocationState::Cancelled,
            Err(StdioCallError::Interrupted | StdioCallError::UnsupportedContinuation) => {
                exit.get_or_insert(true);
                McpInvocationState::Interrupted
            }
            // A complete oversized result is a known response, not a replay opportunity.
            Err(_) => McpInvocationState::Failed,
        };
        if let Some(requested) = exit {
            // Remove durable readiness before releasing the serial invocation
            // slot; another Run must not claim this retiring generation.
            retiring = Some(
                transition(
                    store,
                    &ready,
                    if requested {
                        McpInstanceTransition::RequestStop
                    } else {
                        McpInstanceTransition::ConnectionLost
                    },
                )
                .await,
            );
        }
        // Never cancel this transaction. A failed journal leaves the claim
        // uncertain and retires the owner before accepting another request.
        let result = match store
            .finish_mcp_invocation(call.permit.record(), state)
            .await
        {
            Ok(receipt) => Ok(DispatchOutcome { receipt, result }),
            Err(error) => {
                exit.get_or_insert(true);
                Err(StdioCallError::Store(error))
            }
        };
        if exit.is_some() && retiring.is_none() {
            retiring = Some(transition(store, &ready, McpInstanceTransition::RequestStop).await);
        }
        let _ = call.reply.send(result);
        if let Some(requested) = exit {
            if requested {
                cancellation.cancel();
            }
            break requested;
        }
    };
    // A journal failure must not skip process cleanup. Record the intent first,
    // retain its result, then wait for SDK ownership to return and reap.
    let terminal_base = match retiring {
        Some(record) => record,
        None => {
            transition(
                store,
                &ready,
                if requested {
                    McpInstanceTransition::RequestStop
                } else {
                    McpInstanceTransition::ConnectionLost
                },
            )
            .await
        }
    };
    // A permit queued just before shutdown may never have reached the worker.
    // The unique active-generation index bounds this recovery batch to one.
    let interrupted = store
        .interrupt_mcp_invocations(
            Some(&ready.generation),
            std::num::NonZeroUsize::new(1).unwrap(),
        )
        .await;
    if requested {
        let _ = waiting.await;
    }
    if cleanup.finish().await.is_err() {
        return Err(StdioGenerationError::Cleanup);
    }
    interrupted.map_err(StdioGenerationError::Invocation)?;
    transition(store, &terminal_base?, McpInstanceTransition::Stopped).await
}

pub(crate) async fn dispatch_to(
    sender: mpsc::Sender<DispatchRequest>,
    permit: McpDispatchPermit,
    limits: StdioCallLimits,
    cancellation: oneshot::Receiver<()>,
) -> Result<DispatchOutcome, StdioCallError> {
    let (reply, result) = oneshot::channel();
    sender
        .send(DispatchRequest {
            permit,
            limits,
            cancellation,
            reply,
        })
        .await
        .map_err(|_| StdioCallError::WorkerLost)?;
    result.await.map_err(|_| StdioCallError::WorkerLost)?
}
