//! Capture untrusted MCP bytes only after a durable dispatch outcome.

use crate::{StdioCallError, dispatch::DispatchOutcome};
use kiln_core::{
    Artifact, INLINE_TOOL_OUTPUT_LIMIT, McpInvocationState, RunError, SubprocessOutput,
    TOOL_OUTPUT_MEDIA_TYPE, ToolCallResult, ToolCallState,
};

pub(crate) async fn capture<F, Fut, E>(
    outcome: DispatchOutcome,
    archive: F,
) -> Result<ToolCallResult, RunError>
where
    F: FnOnce(Vec<u8>) -> Fut,
    Fut: std::future::Future<Output = Result<Artifact, E>>,
{
    use StdioCallError as Error;
    let expected = match &outcome.result {
        Ok(result) if result.is_error => McpInvocationState::Failed,
        Ok(_) => McpInvocationState::Completed,
        Err(Error::CancelledBeforeSend) => McpInvocationState::Cancelled,
        Err(Error::Interrupted | Error::UnsupportedContinuation) => McpInvocationState::Interrupted,
        Err(Error::Store(_) | Error::WorkerLost) => return Err(RunError::RunStoreUnavailable),
        Err(_) => McpInvocationState::Failed,
    };
    if outcome.receipt.state != expected {
        return Err(RunError::RunStoreUnavailable);
    }
    let result = match outcome.result {
        Ok(result) => result,
        Err(Error::CancelledBeforeSend) => {
            return ToolCallResult::cancelled(SubprocessOutput::success("", "", 0));
        }
        Err(error) => {
            return failed(match error {
                Error::Rejected => {
                    "MCP dispatch was rejected before sending the operation request."
                }
                Error::Catalog(crate::McpCatalogError::SnapshotUnavailable) => {
                    "The MCP catalogue snapshot is unavailable or invalidated. Request fresh discovery; no metadata was silently refetched and no tool call, resource read or prompt retrieval was sent."
                }
                Error::Catalog(crate::McpCatalogError::CatalogChanged) => {
                    "The MCP catalogue is no longer valid for this request. No tool call, resource read or prompt retrieval was sent."
                }
                Error::Catalog(_) => {
                    "The MCP capability, catalogue or arguments failed validation. No tool call, resource read or prompt retrieval was sent."
                }
                Error::InvalidOutput => {
                    "The MCP server responded with output that failed validation. External effects may have occurred; the request was not retried."
                }
                Error::DeadlineBeforeSend => {
                    "The MCP deadline elapsed before sending the operation request."
                }
                Error::Server => {
                    "The MCP server returned a protocol error. The request was not retried."
                }
                Error::ResultTooLarge => {
                    "The MCP response exceeded the configured byte limit. The request was not retried."
                }
                Error::UnsupportedContinuation => {
                    "The MCP server requested an unsupported continuation. External effects may have occurred; the request was not retried."
                }
                Error::Interrupted => {
                    "The MCP operation was interrupted and its external outcome is unknown. The request was not retried."
                }
                Error::Store(_) | Error::WorkerLost | Error::CancelledBeforeSend => unreachable!(),
            });
        }
    };
    let text = match String::from_utf8(result.json) {
        Ok(text) => text,
        Err(_) => {
            return failed("The MCP response was not valid UTF-8. The request was not retried.");
        }
    };
    // The common ToolCall envelope represents success with exit code zero even
    // for non-process tools. This is not an MCP server process exit observation.
    let mut output = SubprocessOutput::success("", "", 0);
    let state = if result.is_error {
        output.exit_code = None;
        ToolCallState::Failed
    } else {
        ToolCallState::Completed
    };
    if text.len() <= INLINE_TOOL_OUTPUT_LIMIT {
        output.stdout = text;
    } else {
        let size = text.len() as u64;
        let artifact = match archive(text.into_bytes()).await {
            Ok(artifact)
                if artifact.size() == size && artifact.media_type() == TOOL_OUTPUT_MEDIA_TYPE =>
            {
                artifact
            }
            _ => {
                return failed(
                    "The MCP response was received but its output artifact could not be stored. The request was not retried.",
                );
            }
        };
        output.stdout_artifact = Some(artifact);
    }
    ToolCallResult::from_subprocess(state, output)
}

fn failed(message: &str) -> Result<ToolCallResult, RunError> {
    ToolCallResult::new(ToolCallState::Failed, String::new(), message.into(), None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StdioCallResult;
    use kiln_core::{McpGenerationId, McpInvocationRecord, ToolCallId};
    use kiln_infrastructure::FileArtifactStore;

    fn outcome(
        result: Result<StdioCallResult, StdioCallError>,
        state: McpInvocationState,
    ) -> DispatchOutcome {
        DispatchOutcome {
            result,
            receipt: McpInvocationRecord {
                tool_call_id: ToolCallId::from_ulid(ulid::Ulid::generate()),
                generation: McpGenerationId::from_ulid(ulid::Ulid::generate()),
                state,
            },
        }
    }

    #[tokio::test]
    async fn captured_responses_preserve_exact_bytes_and_normal_artifact_boundary() {
        let data = tempfile::tempdir().unwrap();
        let artifacts = FileArtifactStore::open(data.path()).unwrap();
        for (size, is_error) in [
            (INLINE_TOOL_OUTPUT_LIMIT, false),
            (INLINE_TOOL_OUTPUT_LIMIT + 1, false),
            (INLINE_TOOL_OUTPUT_LIMIT + 1, true),
        ] {
            let json = serde_json::json!({"text":"x".repeat(size - 11)})
                .to_string()
                .into_bytes();
            assert_eq!(json.len(), size);
            let expected = json.clone();
            let state = if is_error {
                McpInvocationState::Failed
            } else {
                McpInvocationState::Completed
            };
            let result = capture(
                outcome(Ok(StdioCallResult { json, is_error }), state),
                |bytes| {
                    assert_eq!(bytes, expected);
                    std::future::ready(artifacts.store(&bytes, TOOL_OUTPUT_MEDIA_TYPE))
                },
            )
            .await
            .unwrap();
            assert_eq!(
                result.state(),
                if is_error {
                    ToolCallState::Failed
                } else {
                    ToolCallState::Completed
                }
            );
            if size <= INLINE_TOOL_OUTPUT_LIMIT {
                assert_eq!(result.stdout().unwrap().as_bytes(), expected);
                assert!(result.stdout_artifact().is_none());
            } else {
                assert!(result.stdout().is_none());
                let artifact = result.stdout_artifact().unwrap();
                assert_eq!(artifact.size(), size as u64);
                assert_eq!(
                    artifacts.read(artifact.content_hash()).unwrap().unwrap(),
                    expected
                );
            }
        }
    }

    #[tokio::test]
    async fn capture_failures_never_claim_completion_or_undo_external_effects() {
        let no_archive = |_| std::future::ready(Err::<Artifact, ()>(()));
        let cancelled = capture(
            outcome(
                Err(StdioCallError::CancelledBeforeSend),
                McpInvocationState::Cancelled,
            ),
            no_archive,
        )
        .await
        .unwrap();
        assert_eq!(cancelled.state(), ToolCallState::Cancelled);
        for (error, diagnostic) in [
            (
                StdioCallError::Catalog(crate::McpCatalogError::InvalidArguments),
                "No operation request was sent",
            ),
            (
                StdioCallError::InvalidOutput,
                "External effects may have occurred",
            ),
        ] {
            let result = capture(outcome(Err(error), McpInvocationState::Failed), no_archive)
                .await
                .unwrap();
            assert_eq!(result.state(), ToolCallState::Failed);
            assert!(result.stderr().unwrap().contains(diagnostic));
        }
        let interrupted = capture(
            outcome(
                Err(StdioCallError::Interrupted),
                McpInvocationState::Interrupted,
            ),
            |_| async { Err::<Artifact, ()>(()) },
        )
        .await
        .unwrap();
        assert_eq!(interrupted.state(), ToolCallState::Failed);
        assert!(interrupted.stderr().unwrap().contains("outcome is unknown"));
        assert!(
            capture(
                outcome(
                    Err(StdioCallError::Interrupted),
                    McpInvocationState::Dispatching
                ),
                |_| async { Err::<Artifact, ()>(()) }
            )
            .await
            .is_err()
        );
        assert!(
            capture(
                outcome(
                    Err(StdioCallError::WorkerLost),
                    McpInvocationState::Interrupted
                ),
                |_| async { Err::<Artifact, ()>(()) }
            )
            .await
            .is_err()
        );
        let failed = capture(
            outcome(
                Ok(StdioCallResult {
                    json: vec![b'x'; INLINE_TOOL_OUTPUT_LIMIT + 1],
                    is_error: false,
                }),
                McpInvocationState::Completed,
            ),
            |_| async { Err::<Artifact, ()>(()) },
        )
        .await
        .unwrap();
        assert_eq!(failed.state(), ToolCallState::Failed);
        assert!(failed.stdout().unwrap().is_empty());
        assert!(
            failed
                .stderr()
                .unwrap()
                .contains("artifact could not be stored")
        );
        assert!(failed.stderr().unwrap().contains("not retried"));
    }
}
