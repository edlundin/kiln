use std::num::NonZeroUsize;

use kiln_mcp::StdioTransport;
use rmcp::{model::ClientJsonRpcMessage, transport::Transport};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const RESPONSE: &[u8] =
    b"{\"jsonrpc\":\"2.0\",\"id\":0,\"error\":{\"code\":-32601,\"message\":\"fixture\"}}\n";

#[tokio::test]
async fn frame_budget_is_exact_and_corruption_is_terminal() {
    for (bytes, limit, accepted) in [
        (RESPONSE.to_vec(), RESPONSE.len(), true),
        (RESPONSE.to_vec(), RESPONSE.len() - 1, false),
        (
            RESPONSE[..RESPONSE.len() - 1].to_vec(),
            RESPONSE.len(),
            false,
        ),
        (
            [b"invalid\n".as_slice(), RESPONSE].concat(),
            RESPONSE.len(),
            false,
        ),
    ] {
        let mut transport = StdioTransport::new(
            bytes.as_slice(),
            tokio::io::sink(),
            NonZeroUsize::new(limit).unwrap(),
        );
        assert_eq!(transport.receive().await.is_some(), accepted);
        assert!(transport.receive().await.is_none());
    }
}

#[tokio::test]
async fn cancelled_receive_preserves_partial_frame() {
    let (mut server, client) = tokio::io::duplex(RESPONSE.len());
    let mut transport = StdioTransport::new(
        client,
        tokio::io::sink(),
        NonZeroUsize::new(RESPONSE.len()).unwrap(),
    );
    let split = RESPONSE.len() / 2;
    server.write_all(&RESPONSE[..split]).await.unwrap();
    let pending = std::future::poll_fn(|cx| {
        let mut receive = std::pin::pin!(transport.receive());
        assert!(receive.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    });
    pending.await;
    server.write_all(&RESPONSE[split..]).await.unwrap();
    assert!(transport.receive().await.is_some());
}

#[tokio::test]
async fn oversize_without_newline_terminates_without_waiting_for_eof() {
    let (mut server, client) = tokio::io::duplex(RESPONSE.len());
    let mut transport = StdioTransport::new(
        client,
        tokio::io::sink(),
        NonZeroUsize::new(RESPONSE.len()).unwrap(),
    );
    server.write_all(&vec![b'x'; RESPONSE.len()]).await.unwrap();
    assert!(transport.receive().await.is_none());
}

fn request() -> ClientJsonRpcMessage {
    serde_json::from_str(r#"{"jsonrpc":"2.0","id":0,"method":"ping"}"#).unwrap()
}

#[tokio::test]
async fn outgoing_budget_rejection_writes_nothing() {
    let request = request();
    let wire_bytes = serde_json::to_vec(&request).unwrap().len() + 1;
    for (limit, accepted) in [(wire_bytes, true), (wire_bytes - 1, false)] {
        let (client, mut server) = tokio::io::duplex(wire_bytes);
        let mut transport = StdioTransport::new(
            tokio::io::empty(),
            client,
            NonZeroUsize::new(limit).unwrap(),
        );
        assert_eq!(transport.send(request.clone()).await.is_ok(), accepted);
        transport.close().await.unwrap();
        let mut actual = Vec::new();
        server.read_to_end(&mut actual).await.unwrap();
        assert_eq!(actual.len(), if accepted { wire_bytes } else { 0 });
    }
}

#[tokio::test]
async fn cancelled_partial_write_closes_instead_of_appending_next_frame() {
    let (client, mut server) = tokio::io::duplex(1);
    let wire_bytes = serde_json::to_vec(&request()).unwrap().len() + 1;
    let mut transport = StdioTransport::new(
        tokio::io::empty(),
        client,
        NonZeroUsize::new(wire_bytes).unwrap(),
    );
    let mut send = Box::pin(transport.send(request()));
    std::future::poll_fn(|cx| {
        assert!(send.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(send);
    assert!(transport.send(request()).await.is_err());
    let mut actual = Vec::new();
    server.read_to_end(&mut actual).await.unwrap();
    assert_eq!(actual.len(), 1);
}
