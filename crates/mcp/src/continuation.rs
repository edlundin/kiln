//! Final-protocol MRTR decoding at the SDK boundary.
//!
//! rmcp 3.4.1 requires the removed legacy elicitationId even for modern URLs.
//! Such results arrive as CustomResult. Decode that shape locally without
//! changing the wire, inventing an ID, or weakening the other request decoders.

use std::{collections::BTreeMap, num::NonZeroUsize};

use rmcp::model::{InputRequest, ServerResult};
use serde::Deserialize;

use crate::StdioCallError;

pub(crate) enum Request {
    Sdk(InputRequest),
    Url { message: String, url: String },
}

// No Debug: requests and opaque continuation state may contain private data.
pub(crate) struct Continuation {
    pub requests: BTreeMap<String, Request>,
    pub state: Option<String>,
}

pub(crate) fn decode(
    result: &ServerResult,
    max_requests: Option<NonZeroUsize>,
) -> Result<Option<Continuation>, StdioCallError> {
    let unsupported = StdioCallError::UnsupportedContinuation;
    let (requests, state) = match result {
        ServerResult::InputRequiredResult(input) => {
            let requests = input.input_requests.as_ref().ok_or(unsupported)?;
            check_count(requests.len(), max_requests)?;
            // Keep typed requests on their existing mediation path, including
            // quota consumption before rejecting a legacy-shaped URL request.
            let requests = requests
                .iter()
                .map(|(id, request)| (id.clone(), Request::Sdk(request.clone())))
                .collect();
            (requests, input.request_state.clone())
        }
        ServerResult::CustomResult(custom)
            if custom.0.get("resultType").and_then(|value| value.as_str())
                == Some("input_required") =>
        {
            // Count before cloning/normalizing any individual request. The
            // enclosing transport already bounds the complete wire frame.
            let requests = custom
                .0
                .get("inputRequests")
                .and_then(|value| value.as_object())
                .ok_or(unsupported)?;
            check_count(requests.len(), max_requests)?;
            let state = match custom.0.get("requestState") {
                None | Some(serde_json::Value::Null) => None,
                Some(serde_json::Value::String(state)) => Some(state.clone()),
                _ => return Err(unsupported),
            };
            let requests = requests
                .iter()
                .map(|(id, request)| {
                    let normalized = if request.get("method").and_then(|v| v.as_str())
                        == Some("elicitation/create")
                        && request.pointer("/params/mode").and_then(|v| v.as_str()) == Some("url")
                    {
                        #[derive(Deserialize)]
                        struct UrlParams {
                            message: String,
                            url: String,
                        }
                        let params: UrlParams = serde_json::from_value(request["params"].clone())
                            .map_err(|_| unsupported)?;
                        Request::Url {
                            message: params.message,
                            url: params.url,
                        }
                    } else {
                        Request::Sdk(
                            serde_json::from_value(request.clone()).map_err(|_| unsupported)?,
                        )
                    };
                    Ok((id.clone(), normalized))
                })
                .collect::<Result<_, StdioCallError>>()?;
            (requests, state)
        }
        _ => return Ok(None),
    };
    Ok(Some(Continuation { requests, state }))
}

fn check_count(count: usize, max: Option<NonZeroUsize>) -> Result<(), StdioCallError> {
    if count == 0 || count > max.map_or(0, |n| n.get()) {
        Err(StdioCallError::UnsupportedContinuation)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::ServerJsonRpcMessage;
    use serde_json::json;

    #[test]
    fn modern_url_survives_sdk_without_invented_legacy_identity() {
        let wire = json!({"jsonrpc":"2.0","id":7,"result":{
            "resultType":"input_required", "requestState":"opaque-private-state",
            "inputRequests": {
                "url": {"method":"elicitation/create","params":{"mode":"url","message":"private-message","url":"https://example.com/?private-state"}},
                "roots": {"method":"roots/list"}
            }
        }});
        let message: ServerJsonRpcMessage = serde_json::from_value(wire).unwrap();
        let ServerJsonRpcMessage::Response(response) = message else {
            panic!()
        };
        assert!(matches!(&response.result, ServerResult::CustomResult(_)));
        let input = decode(&response.result, NonZeroUsize::new(2))
            .unwrap()
            .unwrap();
        assert_eq!(input.state.as_deref(), Some("opaque-private-state"));
        let Request::Url { message, url } = &input.requests["url"] else {
            panic!()
        };
        assert_eq!(message, "private-message");
        assert_eq!(url, "https://example.com/?private-state");
        assert!(matches!(
            &input.requests["roots"],
            Request::Sdk(InputRequest::ListRoots(_))
        ));
        assert!(decode(&response.result, NonZeroUsize::new(1)).is_err());
        assert!(decode(&response.result, None).is_err());
    }

    #[test]
    fn typed_forms_roots_and_complete_results_keep_the_existing_path() {
        let result: ServerResult = serde_json::from_value(json!({
            "resultType":"input_required", "requestState":"unchanged",
            "inputRequests": {
                "form": {"method":"elicitation/create","params":{"mode":"form","message":"message","requestedSchema":{"type":"object","properties":{}}}},
                "roots": {"method":"roots/list"}
            }
        })).unwrap();
        assert!(matches!(&result, ServerResult::InputRequiredResult(_)));
        let input = decode(&result, NonZeroUsize::new(2)).unwrap().unwrap();
        assert_eq!(input.state.as_deref(), Some("unchanged"));
        assert!(matches!(
            &input.requests["form"],
            Request::Sdk(InputRequest::Elicitation(_))
        ));
        assert!(matches!(
            &input.requests["roots"],
            Request::Sdk(InputRequest::ListRoots(_))
        ));
        let complete: ServerResult = serde_json::from_value(json!({"content":[]})).unwrap();
        assert!(decode(&complete, None).unwrap().is_none());
    }

    #[test]
    fn malformed_url_or_continuation_cannot_become_a_form_or_empty_round() {
        for input in [
            json!({"inputRequests":{}}),
            json!({"requestState":"only-state"}),
            json!({"requestState":{},"inputRequests":{"r":{"method":"roots/list"}}}),
            json!({"inputRequests":{"u":{"method":"elicitation/create","params":{"mode":"url","url":42,"message":"message"}}}}),
            json!({"inputRequests":{"u":{"method":"elicitation/create","params":{"mode":"url","url":"https://example.com/"}}}}),
        ] {
            let mut value = input;
            value["resultType"] = json!("input_required");
            let result = ServerResult::CustomResult(rmcp::model::CustomResult(value));
            assert!(decode(&result, NonZeroUsize::new(2)).is_err());
        }
    }
}
