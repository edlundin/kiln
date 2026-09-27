use kiln_core::{
    ModelToolExecutionRequest, ToolOutputPageCommand, ToolOutputPageLimits, ToolOutputPageTool,
};
use serde::Deserialize;

use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PageLimits {
    max_request_bytes: usize,
    max_artifact_bytes: u64,
    max_page_bytes: usize,
}

pub(crate) fn configured_output_page() -> Result<Option<ToolOutputPageTool>, &'static str> {
    match std::env::var("KILN_NATIVE_TOOL_OUTPUT_PAGE_LIMITS") {
        Err(std::env::VarError::NotPresent) => Ok(None),
        Ok(value) => parse_limits(&value).map(Some),
        Err(_) => Err("invalid native tool output page limits"),
    }
}

fn parse_limits(value: &str) -> Result<ToolOutputPageTool, &'static str> {
    let invalid =
        "native tool output paging requires complete valid request, artifact and page byte limits";
    let limits: PageLimits = serde_json::from_str(value).map_err(|_| invalid)?;
    ToolOutputPageTool::new(
        ToolOutputPageLimits {
            max_request_bytes: limits.max_request_bytes,
            max_artifact_bytes: limits.max_artifact_bytes,
            max_page_bytes: limits.max_page_bytes,
        },
        kiln_core::ModelToolCatalogLimits {
            max_tools: 1,
            max_definition_bytes: usize::MAX,
            max_total_definition_bytes: usize::MAX,
        },
    )
    .map_err(|_| invalid)
}

impl RunService {
    pub(super) async fn execute_output_page(
        &self,
        request: ModelToolExecutionRequest<ToolOutputPageCommand>,
        cancellation: &mut oneshot::Receiver<()>,
    ) -> Result<kiln_core::ToolCallResult, RunError> {
        use kiln_core::{ToolCallResult, ToolCallState};
        let failed = || {
            ToolCallResult::new(ToolCallState::Failed, String::new(),
            "The requested output page is unavailable, outside the approved scope, over its limit, or not valid UTF-8.".into(), None)
        };
        let Some((run, live)) = self
            .store
            .get_tool_call(request.tool_call().tool_call_id())
            .await
            .map_err(|_| RunError::RunStoreUnavailable)?
        else {
            return Err(RunError::InvalidTransition);
        };
        if &live != request.tool_call() {
            return Err(RunError::InvalidTransition);
        }
        let Some((source_run, source)) = self
            .store
            .get_tool_call(request.command().tool_call_id())
            .await
            .map_err(|_| RunError::RunStoreUnavailable)?
        else {
            return failed();
        };
        let Some(artifact) = request.source_artifact(&run, &source_run, &source).cloned() else {
            return failed();
        };
        tokio::select! {
            biased;
            _ = &mut *cancellation => return ToolCallResult::cancelled(empty_output()),
            _ = std::future::ready(()) => {}
        }
        let artifacts = self.artifacts.clone();
        let mut operation = tokio::task::spawn_blocking(move || {
            let command = request.command();
            let bytes = artifacts
                .read_page(
                    &artifact,
                    command.offset(),
                    std::num::NonZeroUsize::new(command.limit()).ok_or(())?,
                    command.max_artifact_bytes(),
                )
                .map_err(|_| ())?
                .ok_or(())?;
            page_json(&artifact, command.offset(), bytes)
        });
        tokio::select! {
            biased;
            _ = &mut *cancellation => {
                // Join the bounded filesystem scan before publishing cancellation.
                // Blocking filesystem I/O has no hard wall-clock deadline.
                let _ = operation.await;
                ToolCallResult::cancelled(empty_output())
            }
            result = &mut operation => match result {
                Ok(Ok(text)) => ToolCallResult::from_subprocess(ToolCallState::Completed, SubprocessOutput::success(text, "", 0)),
                _ => failed(),
            }
        }
    }
}

fn page_json(artifact: &Artifact, offset: u64, mut bytes: Vec<u8>) -> Result<String, ()> {
    match std::str::from_utf8(&bytes) {
        Ok(_) => {}
        Err(error)
            if error.error_len().is_none()
                && error.valid_up_to() > 0
                && offset + (bytes.len() as u64) < artifact.size() =>
        {
            bytes.truncate(error.valid_up_to());
        }
        Err(_) => return Err(()),
    }
    let next_offset = offset.checked_add(bytes.len() as u64).ok_or(())?;
    let text = String::from_utf8(bytes).map_err(|_| ())?;
    let output = serde_json::json!({"content_hash":artifact.content_hash().as_str(),
        "offset":offset,"next_offset":next_offset,"eof":next_offset == artifact.size(),"text":text})
    .to_string();
    if output.len() > kiln_core::INLINE_TOOL_OUTPUT_LIMIT {
        return Err(());
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_and_text_pages_are_bounded_and_continue_at_utf8_boundaries() {
        let config = serde_json::json!({"max_request_bytes":1024,"max_artifact_bytes":10000,"max_page_bytes":640});
        assert!(parse_limits(&config.to_string()).is_ok());
        for field in ["max_request_bytes", "max_artifact_bytes", "max_page_bytes"] {
            let mut bad = config.clone();
            bad.as_object_mut().unwrap().remove(field);
            assert!(parse_limits(&bad.to_string()).is_err());
            let mut bad = config.clone();
            bad[field] = 0.into();
            assert!(parse_limits(&bad.to_string()).is_err());
        }
        assert!(parse_limits("{\"max_request_bytes\":1024,\"max_request_bytes\":1024,\"max_artifact_bytes\":10000,\"max_page_bytes\":640}").is_err());
        let artifact =
            Artifact::new(ContentHash::parse("a".repeat(64)).unwrap(), "text/plain", 7).unwrap();
        let page: serde_json::Value = serde_json::from_str(
            &page_json(&artifact, 0, "abc😀".as_bytes()[..4].to_vec()).unwrap(),
        )
        .unwrap();
        assert_eq!(page["text"], "abc");
        assert_eq!(page["next_offset"], 3);
        assert_eq!(page["eof"], false);
        let page: serde_json::Value =
            serde_json::from_str(&page_json(&artifact, 3, "😀".as_bytes().to_vec()).unwrap())
                .unwrap();
        assert_eq!(page["text"], "😀");
        assert_eq!(page["next_offset"], 7);
        assert_eq!(page["eof"], true);
        assert!(page_json(&artifact, 4, vec![0x9f, 0x98, 0x80]).is_err());
        assert!(page_json(&artifact, 6, vec![0xf0]).is_err());
        let artifact = Artifact::new(
            ContentHash::parse("b".repeat(64)).unwrap(),
            "text/plain",
            u64::MAX,
        )
        .unwrap();
        let text = page_json(
            &artifact,
            u64::MAX - kiln_core::TOOL_OUTPUT_PAGE_MAX_BYTES as u64,
            vec![0; kiln_core::TOOL_OUTPUT_PAGE_MAX_BYTES],
        )
        .unwrap();
        assert!(text.len() <= kiln_core::INLINE_TOOL_OUTPUT_LIMIT);
    }
}
