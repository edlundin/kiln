use kiln_core::{
    ModelInvocation, ModelToolExecutionClaim, ModelToolResolutionError, ProviderApplication,
    ToolCallResult, ToolCallState, WorkspaceFileReadTool,
};
use kiln_infrastructure::execute_workspace_file_read;

use super::*;

pub(super) enum NativeToolBatchOutcome {
    Completed(Vec<ToolCallId>),
    Cancelled,
    Rejected,
}

impl RunService {
    pub(super) fn native_tools(&self) -> Result<kiln_core::NativeTools<'_>, RunError> {
        #[cfg(unix)]
        let mcp = self.native_mcp.as_ref().map(|mcp| &mcp.tools);
        #[cfg(not(unix))]
        let mcp = None;
        kiln_core::NativeTools::new(
            self.native_file_read.as_deref(),
            mcp,
            self.native_output_page.as_deref(),
            kiln_core::ModelToolCatalogLimits {
                // Five locally constructed fixed definitions, no server schemas.
                max_tools: 5,
                max_definition_bytes: usize::MAX,
                max_total_definition_bytes: usize::MAX,
            },
        )
        .map_err(|_| RunError::InvalidTransition)
    }

    pub(super) async fn execute_native_tools(
        &self,
        invocation: &ModelInvocation,
        cancellation: &mut oneshot::Receiver<()>,
    ) -> Result<NativeToolBatchOutcome, RunError> {
        let resolver = self.native_tools()?;
        if resolver.catalog().definitions().is_empty() {
            return Ok(NativeToolBatchOutcome::Rejected);
        }
        let application = ProviderApplication::new(self.store.clone(), UlidIdGenerator);
        let mut completed = Vec::new();
        loop {
            // Claims consume the resolved command. Reload the immutable batch
            // for each sequential call; never retain an executable retry token.
            let batch = match application
                .resolve_tool_requests(invocation.invocation_id().clone(), &resolver)
                .await
            {
                Ok(batch) => batch,
                Err(
                    ModelToolResolutionError::ToolNotOffered { .. }
                    | ModelToolResolutionError::ImplementationUnavailable { .. }
                    | ModelToolResolutionError::DefinitionChanged { .. }
                    | ModelToolResolutionError::Arguments { .. },
                ) => return Ok(NativeToolBatchOutcome::Rejected),
                Err(_) => return Err(RunError::RunStoreUnavailable),
            };
            let position = completed.len();
            if position == batch.requests().len() {
                return Ok(NativeToolBatchOutcome::Completed(completed));
            }
            let tool_call_id = {
                let _sequence = self.commit_sequence.lock().await;
                let snapshot = self.runs.get_run(invocation.run_id().clone()).await?;
                if matches!(
                    snapshot.run().state(),
                    RunState::Cancelling | RunState::Cancelled
                ) {
                    return Ok(NativeToolBatchOutcome::Cancelled);
                }
                let scope = snapshot
                    .run()
                    .requested_scope()
                    .cloned()
                    .ok_or(RunError::InvalidTransition)?;
                let command = batch
                    .prepare_adoption(position, scope)
                    .map_err(|_| RunError::InvalidTransition)?;
                let mutation = application
                    .adopt_tool_request(&command)
                    .await
                    .map_err(|_| RunError::RunStoreUnavailable)?;
                self.events.publish(mutation.events);
                mutation.tool_call_id
            };
            let request = loop {
                let changed = self.active.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let _sequence = self.commit_sequence.lock().await;
                let snapshot = self.runs.get_run(invocation.run_id().clone()).await?;
                let tool = snapshot
                    .tool_call(&tool_call_id)
                    .ok_or(RunError::InvalidTransition)?;
                if matches!(
                    snapshot.run().state(),
                    RunState::Cancelling | RunState::Cancelled
                ) {
                    if tool.state() == ToolCallState::Ready {
                        let result = ToolCallResult::cancelled(empty_output())?;
                        let mutation = application
                            .finish_tool_call(&tool_call_id, &result)
                            .await
                            .map_err(|_| RunError::RunStoreUnavailable)?;
                        self.events.publish(mutation.events);
                    }
                    return Ok(NativeToolBatchOutcome::Cancelled);
                }
                if tool.state().is_terminal() {
                    break None;
                }
                match tool.state() {
                    ToolCallState::AwaitingApproval => {
                        drop(_sequence);
                        tokio::select! {
                            signal = &mut *cancellation => {
                                signal.map_err(|_| RunError::CancellationFailed)?;
                            },
                            _ = &mut changed => {},
                        }
                    }
                    ToolCallState::Ready => {
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
                        let scope = tool.effective_scope().ok_or(RunError::InvalidTransition)?;
                        let root = workspace
                            .root(scope.workspace_root_id())
                            .cloned()
                            .ok_or(RunError::WorkspaceRootNotFound)?;
                        let claim = application
                            .claim_tool_call(batch, position, tool_call_id.clone())
                            .await
                            .map_err(|_| RunError::RunStoreUnavailable)?;
                        match claim {
                            ModelToolExecutionClaim::Applied { request, events } => {
                                self.events.publish(events);
                                break Some((request, root));
                            }
                            ModelToolExecutionClaim::Duplicate { tool_call }
                                if tool_call.state().is_terminal() =>
                            {
                                break None;
                            }
                            ModelToolExecutionClaim::Duplicate { .. } => {
                                return Err(RunError::InvalidTransition);
                            }
                        }
                    }
                    // An ambiguous running claim never authorizes redispatch.
                    _ => return Err(RunError::InvalidTransition),
                }
            };
            if let Some((request, root)) = request {
                // Only Run cancellation stops a claimed native operation. Interrupt
                // input is delivered at the next generation boundary, after
                // this accepted sequential batch has terminal results.
                let result = match request.into_native() {
                    kiln_core::NativeToolExecutionRequest::ToolOutputPage(request) => {
                        self.execute_output_page(request, cancellation).await?
                    }
                    kiln_core::NativeToolExecutionRequest::FileRead(request) => {
                        execute_workspace_file_read(request, &root, self.artifacts.clone(), async {
                            let _ = (&mut *cancellation).await;
                        })
                        .await?
                    }
                    kiln_core::NativeToolExecutionRequest::Mcp(request) => {
                        #[cfg(unix)]
                        {
                            self.execute_mcp_native(request, cancellation).await?
                        }
                        #[cfg(not(unix))]
                        {
                            let _ = request;
                            return Err(RunError::InvalidTransition);
                        }
                    }
                };
                let _sequence = self.commit_sequence.lock().await;
                let mutation = application
                    .finish_tool_call(&tool_call_id, &result)
                    .await
                    .map_err(|_| RunError::RunStoreUnavailable)?;
                self.events.publish(mutation.events);
            }
            completed.push(tool_call_id);
            self.active.changed.notify_waiters();
        }
    }
}

pub(crate) fn configured_file_read() -> Result<Option<WorkspaceFileReadTool>, &'static str> {
    let path = std::env::var("KILN_NATIVE_READ_FILE_MAX_PATH_BYTES");
    let file = std::env::var("KILN_NATIVE_READ_FILE_MAX_BYTES");
    if matches!(path, Err(std::env::VarError::NotPresent))
        && matches!(file, Err(std::env::VarError::NotPresent))
    {
        return Ok(None);
    }
    let invalid = "native file reading requires valid positive path and file byte limits";
    let max_path_bytes = path.map_err(|_| invalid)?.parse().map_err(|_| invalid)?;
    let max_file_bytes = file.map_err(|_| invalid)?.parse().map_err(|_| invalid)?;
    // This catalog is one locally constructed definition with a fixed schema
    // and numeric limits. No provider-supplied description is allocated here.
    WorkspaceFileReadTool::new(
        kiln_core::WorkspaceFileReadLimits {
            max_path_bytes,
            max_file_bytes,
        },
        kiln_core::ModelToolCatalogLimits {
            max_tools: 1,
            max_definition_bytes: usize::MAX,
            max_total_definition_bytes: usize::MAX,
        },
    )
    .map(Some)
    .map_err(|_| invalid)
}
