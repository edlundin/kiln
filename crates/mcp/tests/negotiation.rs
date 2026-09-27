use std::{
    collections::VecDeque,
    convert::Infallible,
    sync::{Arc, Mutex},
};

use kiln_mcp::{ProtocolPolicy, ProtocolVersion, start_stdio_client};
use rmcp::{
    RoleClient,
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
    transport::Transport,
};
use serde_json::{Value, json};

struct Fixture {
    replies: VecDeque<ServerJsonRpcMessage>,
    sent: Arc<Mutex<Vec<Value>>>,
}

impl Transport<RoleClient> for Fixture {
    type Error = Infallible;
    fn send(
        &mut self,
        message: ClientJsonRpcMessage,
    ) -> impl Future<Output = Result<(), Infallible>> + Send + 'static {
        self.sent
            .lock()
            .unwrap()
            .push(serde_json::to_value(message).unwrap());
        std::future::ready(Ok(()))
    }
    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        match self.replies.pop_front() {
            Some(reply) => Some(reply),
            None => std::future::pending().await,
        }
    }
    async fn close(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
}

fn fixture(replies: Vec<Value>) -> (Fixture, Arc<Mutex<Vec<Value>>>) {
    let sent = Arc::new(Mutex::new(Vec::new()));
    (
        Fixture {
            replies: replies
                .into_iter()
                .map(|v| serde_json::from_value(v).unwrap())
                .collect(),
            sent: sent.clone(),
        },
        sent,
    )
}

fn unsupported(id: Value, code: i32) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "error":{"code":code,"message":"fixture"}})
}

fn initialized(id: u32, version: &str) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "result":{"protocolVersion":version,"capabilities":{},"serverInfo":{"name":"fixture","version":"1"}}})
}

#[tokio::test]
async fn structured_unsupported_allows_same_transport_legacy_startup() {
    let (transport, sent) = fixture(vec![
        unsupported(json!(0), -32601),
        initialized(1, "2025-11-25"),
    ]);
    let client = start_stdio_client((), transport, ProtocolPolicy::Auto)
        .await
        .unwrap();
    assert_eq!(
        client.peer_info().unwrap().protocol_version.as_str(),
        "2025-11-25"
    );
    client.cancel().await.unwrap();
    let messages = sent.lock().unwrap();
    assert_eq!(
        messages
            .iter()
            .map(|m| m["method"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["server/discover", "initialize", "notifications/initialized"]
    );
}

#[tokio::test(start_paused = true)]
async fn timeout_does_not_send_initialize() {
    let (transport, sent) = fixture(vec![]);
    assert!(
        start_stdio_client((), transport, ProtocolPolicy::Auto)
            .await
            .is_err()
    );
    assert_eq!(sent.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn arbitrary_or_uncorrelated_errors_never_downgrade() {
    for response in [
        unsupported(json!(0), -32603),
        unsupported(json!(99), -32601),
        unsupported(Value::Null, -32601),
    ] {
        let (transport, sent) = fixture(vec![response]);
        assert!(
            start_stdio_client((), transport, ProtocolPolicy::Auto)
                .await
                .is_err()
        );
        assert_eq!(sent.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn pins_send_exact_version_and_reject_other_versions_before_initialized() {
    for (pin, version) in [
        (ProtocolVersion::V20241105, "2024-11-05"),
        (ProtocolVersion::V20250326, "2025-03-26"),
        (ProtocolVersion::V20250618, "2025-06-18"),
        (ProtocolVersion::V20251125, "2025-11-25"),
    ] {
        let (transport, sent) = fixture(vec![initialized(0, version)]);
        let client = start_stdio_client((), transport, ProtocolPolicy::Pinned(pin))
            .await
            .unwrap();
        client.cancel().await.unwrap();
        assert_eq!(
            sent.lock().unwrap()[0]["params"]["protocolVersion"],
            version
        );
        let (transport, sent) = fixture(vec![initialized(0, "2099-01-01")]);
        assert!(
            start_stdio_client((), transport, ProtocolPolicy::Pinned(pin))
                .await
                .is_err()
        );
        assert_eq!(sent.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn modern_discovery_never_initializes() {
    let discovery = rmcp::model::DiscoverResult::new(
        vec![rmcp::model::ProtocolVersion::V_2026_07_28],
        Default::default(),
    );
    for policy in [
        ProtocolPolicy::Auto,
        ProtocolPolicy::Pinned(ProtocolVersion::V20260728),
    ] {
        let (transport, sent) = fixture(vec![json!({"jsonrpc":"2.0","id":0,"result":discovery})]);
        let client = start_stdio_client((), transport, policy).await.unwrap();
        client.cancel().await.unwrap();
        assert_eq!(sent.lock().unwrap().len(), 1);
    }
    let (transport, sent) = fixture(vec![unsupported(json!(0), -32601)]);
    assert!(
        start_stdio_client(
            (),
            transport,
            ProtocolPolicy::Pinned(ProtocolVersion::V20260728)
        )
        .await
        .is_err()
    );
    assert_eq!(sent.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn auto_rejects_draft_and_modern_versions_from_legacy_initialize() {
    for version in ["2099-01-01", "2026-07-28"] {
        let (transport, sent) =
            fixture(vec![unsupported(json!(0), -32601), initialized(1, version)]);
        assert!(
            start_stdio_client((), transport, ProtocolPolicy::Auto)
                .await
                .is_err()
        );
        assert_eq!(sent.lock().unwrap().len(), 2);
    }
}
