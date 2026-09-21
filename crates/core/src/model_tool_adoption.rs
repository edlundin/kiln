use std::future::Future;

use crate::{
    Approval, ApprovalId, ApprovalPolicy, EventId, ModelInvocation, ModelInvocationStoreError,
    ModelToolCatalog, ModelToolCatalogError, ModelToolRequest, ModelToolRequestError,
    ResolvedModelToolBatch, Run, RunError, RunSnapshot, RunState, RunStoreError, SessionEvent,
    StoredSessionEvent, ToolCall, ToolCallId, ToolCallState, WorkspacePathScope,
};

/// Prepared from locally resolved command data. The store rechecks the durable
/// source, catalog, current Run scope/policy, and sequential execution boundary.
pub struct AdoptModelToolRequest {
    invocation: ModelInvocation,
    catalog: ModelToolCatalog,
    request: ModelToolRequest,
    position: usize,
    requested_scope: WorkspacePathScope,
}

impl<C> ResolvedModelToolBatch<C> {
    pub fn prepare_adoption(
        &self,
        position: usize,
        requested_scope: WorkspacePathScope,
    ) -> Result<AdoptModelToolRequest, ModelToolAdoptionError> {
        let request = self
            .requests()
            .get(position)
            .ok_or(ModelToolAdoptionError::InvalidRequest)?;
        Ok(AdoptModelToolRequest {
            invocation: self.invocation().clone(),
            catalog: self.catalog().clone(),
            request: request.source().clone(),
            position,
            requested_scope,
        })
    }
}

impl AdoptModelToolRequest {
    pub fn invocation(&self) -> &ModelInvocation {
        &self.invocation
    }
    pub fn catalog(&self) -> &ModelToolCatalog {
        &self.catalog
    }
    pub fn request(&self) -> &ModelToolRequest {
        &self.request
    }
    pub fn position(&self) -> usize {
        self.position
    }
    pub fn requested_scope(&self) -> &WorkspacePathScope {
        &self.requested_scope
    }

    /// Pure policy projection, recomputed inside the storage transaction.
    pub fn authorize(
        &self,
        run: &Run,
        ids: ModelToolAdoptionIds,
    ) -> Result<ModelToolAdoptionPlan, ModelToolAdoptionError> {
        if run.run_id() != self.invocation.run_id() || run.state() != RunState::Running {
            return Err(ModelToolAdoptionError::InvalidRun);
        }
        let run_scope = run
            .requested_scope()
            .ok_or(ModelToolAdoptionError::InvalidRun)?;
        if self.requested_scope.workspace_root_id() != run_scope.workspace_root_id()
            || !crate::scope_is_within(run_scope, &self.requested_scope)
        {
            return Err(ModelToolAdoptionError::ScopeOutsideRun);
        }
        let definition = self
            .catalog
            .find(self.request.name())
            .ok_or(ModelToolAdoptionError::InvalidRequest)?;
        let requested = ToolCall::new(
            ids.tool_call_id,
            run.run_id().clone(),
            definition.capability().to_owned(),
            self.requested_scope.clone(),
        );
        let mut events = vec![SessionEvent::tool_call_requested(
            ids.request_event_id,
            run.session_id().clone(),
            requested.clone(),
        )];
        let (next_run, tool_call, approval) = match run
            .approval_policy()
            .ok_or(ModelToolAdoptionError::InvalidRun)?
        {
            ApprovalPolicy::Ask => {
                let next_run = run
                    .transition(RunState::WaitingForApproval)
                    .map_err(ModelToolAdoptionError::Run)?;
                let tool_call = requested
                    .transition(ToolCallState::AwaitingApproval)
                    .map_err(ModelToolAdoptionError::Run)?;
                let approval = Approval::new(
                    ids.approval_id,
                    run.run_id().clone(),
                    tool_call.tool_call_id().clone(),
                    self.requested_scope.clone(),
                );
                events.push(SessionEvent::tool_call_state_changed(
                    ids.tool_event_id,
                    run.session_id().clone(),
                    tool_call.clone(),
                ));
                events.push(SessionEvent::approval_requested(
                    ids.approval_event_id,
                    run.session_id().clone(),
                    approval.clone(),
                ));
                events.push(SessionEvent::run_state_changed(ids.run_event_id, &next_run));
                (next_run, tool_call, Some(approval))
            }
            ApprovalPolicy::FullAccess => {
                let tool_call = requested
                    .with_effective_scope(self.requested_scope.clone())
                    .map_err(ModelToolAdoptionError::Run)?;
                events.push(SessionEvent::tool_call_state_changed(
                    ids.tool_event_id,
                    run.session_id().clone(),
                    tool_call.clone(),
                ));
                (run.clone(), tool_call, None)
            }
            ApprovalPolicy::ReadOnly => {
                // Capability-specific read-only policy is not defined yet. Keep
                // the existing conservative policy: deny instead of guessing.
                let tool_call = requested
                    .transition(ToolCallState::Denied)
                    .map_err(ModelToolAdoptionError::Run)?;
                events.push(SessionEvent::tool_call_denied(
                    ids.tool_event_id,
                    run.session_id().clone(),
                    tool_call.clone(),
                ));
                (run.clone(), tool_call, None)
            }
        };
        Ok(ModelToolAdoptionPlan {
            run: next_run,
            tool_call,
            approval,
            events,
        })
    }
}

pub struct ModelToolAdoptionIds {
    pub tool_call_id: ToolCallId,
    pub approval_id: ApprovalId,
    pub request_event_id: EventId,
    pub tool_event_id: EventId,
    pub approval_event_id: EventId,
    pub run_event_id: EventId,
}

pub struct ModelToolAdoptionPlan {
    pub run: Run,
    pub tool_call: ToolCall,
    pub approval: Option<Approval>,
    pub events: Vec<SessionEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelToolAdoptionDisposition {
    Applied,
    Duplicate,
}

pub struct ModelToolAdoptionMutation {
    pub value: RunSnapshot,
    pub tool_call_id: ToolCallId,
    pub events: Vec<StoredSessionEvent>,
    pub disposition: ModelToolAdoptionDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelToolAdoptionError {
    InvalidRequest,
    InvalidRun,
    ScopeOutsideRun,
    EarlierRequestPending,
    ActiveWork,
    IdempotencyConflict,
    IntegrityViolation,
    Unavailable,
    Invocation(ModelInvocationStoreError),
    Catalog(ModelToolCatalogError),
    Requests(ModelToolRequestError),
    Run(RunError),
    Store(RunStoreError),
}

pub trait ModelToolAdoptionStore: Send + Sync {
    fn adopt_model_tool_request(
        &self,
        command: &AdoptModelToolRequest,
        ids: ModelToolAdoptionIds,
    ) -> impl Future<Output = Result<ModelToolAdoptionMutation, ModelToolAdoptionError>> + Send;
}
