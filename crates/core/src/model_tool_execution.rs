use std::future::Future;

use crate::{
    AdoptModelToolRequest, EventId, ModelInvocationId, ModelToolAdoptionError, StoredSessionEvent,
    ToolCall, ToolCallId, WorkspacePathScope,
};

/// A fresh durable claim, consumed by a Kiln-owned executor. No Clone or Debug.
/// Dropping this request does not authorize redispatch; running work requires
/// explicit reconciliation after an ambiguous failure or process restart.
pub struct ModelToolExecutionRequest<C> {
    invocation_id: ModelInvocationId,
    provider_call_id: String,
    tool_call: ToolCall,
    scope: WorkspacePathScope,
    command: C,
}

impl<C> ModelToolExecutionRequest<C> {
    pub(crate) fn new(
        source: &AdoptModelToolRequest,
        tool_call: ToolCall,
        command: C,
    ) -> Result<Self, ModelToolAdoptionError> {
        let scope = tool_call
            .effective_scope()
            .cloned()
            .ok_or(ModelToolAdoptionError::IntegrityViolation)?;
        let definition = source
            .catalog()
            .find(source.request().name())
            .ok_or(ModelToolAdoptionError::IntegrityViolation)?;
        if tool_call.state() != crate::ToolCallState::Running
            || tool_call.run_id() != source.invocation().run_id()
            || tool_call.capability() != definition.capability()
            || tool_call.requested_scope() != Some(source.requested_scope())
            || scope.workspace_root_id() != source.requested_scope().workspace_root_id()
            || !crate::scope_is_within(source.requested_scope(), &scope)
        {
            return Err(ModelToolAdoptionError::IntegrityViolation);
        }
        Ok(Self {
            invocation_id: source.invocation().invocation_id().clone(),
            provider_call_id: source.request().provider_call_id().to_owned(),
            tool_call,
            scope,
            command,
        })
    }
    pub fn invocation_id(&self) -> &ModelInvocationId {
        &self.invocation_id
    }
    pub fn provider_call_id(&self) -> &str {
        &self.provider_call_id
    }
    pub fn tool_call(&self) -> &ToolCall {
        &self.tool_call
    }
    pub fn scope(&self) -> &WorkspacePathScope {
        &self.scope
    }
    pub fn command(&self) -> &C {
        &self.command
    }
}

pub enum ModelToolExecutionClaim<C> {
    Applied {
        request: ModelToolExecutionRequest<C>,
        events: Vec<StoredSessionEvent>,
    },
    /// No execution request is returned for running or terminal work.
    Duplicate { tool_call: ToolCall },
}

pub struct ModelToolExecutionMutation {
    pub tool_call: ToolCall,
    pub events: Vec<StoredSessionEvent>,
    pub disposition: ModelToolExecutionDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelToolExecutionDisposition {
    Applied,
    Duplicate,
}

pub trait ModelToolExecutionStore: Send + Sync {
    fn claim_model_tool_call(
        &self,
        source: &AdoptModelToolRequest,
        tool_call_id: &ToolCallId,
        event_id: EventId,
    ) -> impl Future<Output = Result<ModelToolExecutionMutation, ModelToolAdoptionError>> + Send;
}

impl ModelToolExecutionRequest<crate::NativeToolCommand> {
    /// Consume the original fresh claim and unwrap only its registered command
    /// variant. No caller-supplied mapping can replace the parsed command or any
    /// durable source/scope field, and no second execution request is retained.
    pub fn into_native(self) -> crate::NativeToolExecutionRequest {
        let Self {
            invocation_id,
            provider_call_id,
            tool_call,
            scope,
            command,
        } = self;
        match command {
            crate::NativeToolCommand::ToolOutputPage(command) => {
                crate::NativeToolExecutionRequest::ToolOutputPage(ModelToolExecutionRequest {
                    invocation_id,
                    provider_call_id,
                    tool_call,
                    scope,
                    command,
                })
            }
            crate::NativeToolCommand::FileRead(command) => {
                crate::NativeToolExecutionRequest::FileRead(ModelToolExecutionRequest {
                    invocation_id,
                    provider_call_id,
                    tool_call,
                    scope,
                    command,
                })
            }
            crate::NativeToolCommand::Mcp(command) => {
                crate::NativeToolExecutionRequest::Mcp(ModelToolExecutionRequest {
                    invocation_id,
                    provider_call_id,
                    tool_call,
                    scope,
                    command,
                })
            }
        }
    }
}

#[cfg(test)]
mod output_page_tests {
    use super::*;
    use crate::*;

    #[test]
    fn output_pages_require_same_session_terminal_source_and_contained_scope() {
        let scope =
            WorkspacePathScope::new(WorkspaceRootId::from_ulid(ulid::Ulid::generate()), "src")
                .unwrap();
        let session = SessionId::from_ulid(ulid::Ulid::generate());
        let run = Run::new(
            RunId::from_ulid(ulid::Ulid::generate()),
            session.clone(),
            ApprovalPolicy::Ask,
            scope.clone(),
        )
        .transition(RunState::Running)
        .unwrap();
        let source_id = ToolCallId::from_ulid(ulid::Ulid::generate());
        let parser = ToolOutputPageTool::new(
            ToolOutputPageLimits {
                max_request_bytes: 1024,
                max_artifact_bytes: 10000,
                max_page_bytes: 640,
            },
            ModelToolCatalogLimits {
                max_tools: 1,
                max_definition_bytes: 4096,
                max_total_definition_bytes: 4096,
            },
        )
        .unwrap();
        let mut arguments = serde_json::json!({"tool_call_id":source_id.as_str(),"stream":"stdout","offset":0,"limit":4});
        arguments.sort_all_objects();
        let command = parser
            .parse_arguments(&parser.catalog().definitions()[0], &arguments.to_string())
            .unwrap();
        let current = ToolCall::new(
            ToolCallId::from_ulid(ulid::Ulid::generate()),
            run.run_id().clone(),
            TOOL_OUTPUT_PAGE_CAPABILITY.into(),
            scope.clone(),
        )
        .with_effective_scope(scope.clone())
        .unwrap()
        .transition(ToolCallState::Running)
        .unwrap();
        let request = ModelToolExecutionRequest {
            invocation_id: ModelInvocationId::from_ulid(ulid::Ulid::generate()),
            provider_call_id: "page".into(),
            tool_call: current,
            scope: scope.clone(),
            command,
        };
        let artifact = Artifact::new(
            ContentHash::parse("a".repeat(64)).unwrap(),
            TOOL_OUTPUT_MEDIA_TYPE,
            5000,
        )
        .unwrap();
        for (source_session, source_scope, terminal, allowed) in [
            (session.clone(), scope.clone(), true, true),
            (
                session.clone(),
                WorkspacePathScope::new(scope.workspace_root_id().clone(), "src/child").unwrap(),
                true,
                true,
            ),
            (
                SessionId::from_ulid(ulid::Ulid::generate()),
                scope.clone(),
                true,
                false,
            ),
            (
                session.clone(),
                WorkspacePathScope::new(scope.workspace_root_id().clone(), "src-other").unwrap(),
                true,
                false,
            ),
            (
                session.clone(),
                WorkspacePathScope::new(scope.workspace_root_id().clone(), "").unwrap(),
                true,
                false,
            ),
            (
                session.clone(),
                WorkspacePathScope::new(WorkspaceRootId::from_ulid(ulid::Ulid::generate()), "src")
                    .unwrap(),
                true,
                false,
            ),
            (session.clone(), scope.clone(), false, false),
        ] {
            let source_run = Run::new(
                RunId::from_ulid(ulid::Ulid::generate()),
                source_session,
                ApprovalPolicy::Ask,
                source_scope.clone(),
            );
            let source = ToolCall::new(
                source_id.clone(),
                source_run.run_id().clone(),
                MCP_CALL_CAPABILITY.into(),
                source_scope.clone(),
            )
            .with_effective_scope(source_scope)
            .unwrap()
            .transition(ToolCallState::Running)
            .unwrap();
            let source = if terminal {
                let mut output = SubprocessOutput::success("", "", 0);
                output.stdout_artifact = Some(artifact.clone());
                source
                    .with_result(
                        &ToolCallResult::from_subprocess(ToolCallState::Completed, output).unwrap(),
                    )
                    .unwrap()
            } else {
                source
            };
            assert_eq!(
                request
                    .source_artifact(&run, &source_run, &source)
                    .is_some(),
                allowed
            );
        }
    }
}
