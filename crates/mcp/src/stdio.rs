use std::{io, num::NonZeroUsize, sync::Arc};

use rmcp::{
    RoleClient,
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
    transport::Transport,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    sync::Mutex,
};

/// Strict newline-delimited MCP transport with a caller-owned wire byte budget.
/// The budget includes the terminating newline; no process or environment is
/// created here. Malformed, oversized or incomplete frames terminate reading.
pub struct StdioTransport<R, W> {
    reader: BufReader<R>,
    writer: Arc<Mutex<Option<W>>>,
    frame: Vec<u8>,
    max_frame_bytes: NonZeroUsize,
    ended: bool,
}

impl<R: AsyncRead, W> StdioTransport<R, W> {
    pub fn new(reader: R, writer: W, max_frame_bytes: NonZeroUsize) -> Self {
        Self {
            reader: BufReader::new(reader),
            writer: Arc::new(Mutex::new(Some(writer))),
            frame: Vec::new(),
            max_frame_bytes,
            ended: false,
        }
    }
}

struct BoundedFrame {
    bytes: Vec<u8>,
    limit: usize,
}

impl io::Write for BoundedFrame {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MCP frame exceeds wire budget",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<R, W> Transport<RoleClient> for StdioTransport<R, W>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send + 'static,
{
    type Error = io::Error;

    fn send(
        &mut self,
        message: ClientJsonRpcMessage,
    ) -> impl Future<Output = io::Result<()>> + Send + 'static {
        let writer = self.writer.clone();
        let limit = self.max_frame_bytes.get();
        async move {
            // Serialize within the same budget before touching the stream. A
            // rejected outgoing message must not leave a partial JSON frame.
            let mut frame = BoundedFrame {
                bytes: Vec::new(),
                limit: limit - 1,
            };
            serde_json::to_writer(&mut frame, &message).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "MCP frame cannot be encoded within wire budget",
                )
            })?;
            frame.bytes.push(b'\n');
            let mut slot = writer.lock().await;
            let mut output = slot.take().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "MCP transport is closed")
            })?;
            // Cancellation or error drops this writer and leaves the slot
            // empty, so another send cannot append to a partial frame.
            output.write_all(&frame.bytes).await?;
            output.flush().await?;
            *slot = Some(output);
            Ok(())
        }
    }

    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        if self.ended {
            return None;
        }
        loop {
            let available = match self.reader.fill_buf().await {
                Ok(bytes) if !bytes.is_empty() => bytes,
                _ => {
                    self.ended = true;
                    return None;
                }
            };
            let newline = available.iter().position(|byte| *byte == b'\n');
            let count = newline.map_or(available.len(), |position| position + 1);
            if count > self.max_frame_bytes.get().saturating_sub(self.frame.len()) {
                self.ended = true;
                return None;
            }
            self.frame.extend_from_slice(&available[..count]);
            self.reader.consume(count);
            if newline.is_some() {
                let parsed = serde_json::from_slice(&self.frame).ok();
                self.frame.clear();
                if parsed.is_none() {
                    self.ended = true;
                }
                return parsed;
            }
            // Without a newline the entire remaining budget has been used.
            if self.frame.len() == self.max_frame_bytes.get() {
                self.ended = true;
                return None;
            }
            // Partial bytes stay in self across cancellation of receive().
        }
    }

    async fn close(&mut self) -> io::Result<()> {
        self.ended = true;
        self.frame.clear();
        if let Some(mut writer) = self.writer.lock().await.take() {
            writer.shutdown().await?;
        }
        Ok(())
    }
}
