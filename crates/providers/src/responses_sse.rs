use std::fmt;

use serde_json::Value;

/// Caller-supplied resource ceilings. Frame bytes count CRLF as one line
/// terminator; total stream bytes count every wire byte, including comments.
#[derive(Debug, Clone, Copy)]
pub struct ResponsesSseLimits {
    pub max_frame_bytes: usize,
    pub max_stream_bytes: usize,
    pub max_events: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponsesSseError {
    InvalidLimits,
    FrameLimitExceeded,
    StreamLimitExceeded,
    EventLimitExceeded,
    InvalidUtf8,
    InvalidJson,
    InvalidEventType,
    EventAfterDone,
    TruncatedFrame,
    Closed,
}

/// Provider-private event data. Decoding grants no tool authority and does not
/// validate lifecycle, sequence numbers, usage or response identity.
pub enum ResponsesSseEvent {
    Event {
        kind: String,
        data: Value,
    },
    /// Optional transport sentinel, never evidence of successful model completion.
    Done,
}

impl fmt::Debug for ResponsesSseEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Event { .. } => f.write_str("ResponsesSseEvent::Event { .. }"),
            Self::Done => f.write_str("ResponsesSseEvent::Done"),
        }
    }
}

/// Incremental SSE framing across arbitrary byte/UTF-8 boundaries. This is not
/// a provider operation or a reconnecting EventSource. `id` and `retry` fields
/// never cause network retries. Errors permanently close the decoder.
pub struct ResponsesSseDecoder {
    limits: ResponsesSseLimits,
    total_bytes: usize,
    frame_bytes: usize,
    event_count: usize,
    line: Vec<u8>,
    data: String,
    event_type: Option<String>,
    first_line: bool,
    skip_lf: bool,
    done: bool,
    closed: bool,
}

impl ResponsesSseDecoder {
    pub fn new(limits: ResponsesSseLimits) -> Result<Self, ResponsesSseError> {
        if limits.max_frame_bytes == 0 || limits.max_stream_bytes == 0 || limits.max_events == 0 {
            return Err(ResponsesSseError::InvalidLimits);
        }
        Ok(Self {
            limits,
            total_bytes: 0,
            frame_bytes: 0,
            event_count: 0,
            line: Vec::new(),
            data: String::new(),
            event_type: None,
            first_line: true,
            skip_lf: false,
            done: false,
            closed: false,
        })
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<ResponsesSseEvent>, ResponsesSseError> {
        if self.closed {
            return Err(ResponsesSseError::Closed);
        }
        let result = self.push_inner(bytes);
        if result.is_err() {
            self.close();
        }
        result
    }

    fn push_inner(&mut self, bytes: &[u8]) -> Result<Vec<ResponsesSseEvent>, ResponsesSseError> {
        use ResponsesSseError as Error;
        self.total_bytes = self
            .total_bytes
            .checked_add(bytes.len())
            .ok_or(Error::StreamLimitExceeded)?;
        if self.total_bytes > self.limits.max_stream_bytes {
            return Err(Error::StreamLimitExceeded);
        }
        let mut events = Vec::new();
        for &byte in bytes {
            if self.skip_lf {
                self.skip_lf = false;
                if byte == b'\n' {
                    continue;
                }
            }
            self.frame_bytes = self
                .frame_bytes
                .checked_add(1)
                .ok_or(Error::FrameLimitExceeded)?;
            if self.frame_bytes > self.limits.max_frame_bytes {
                return Err(Error::FrameLimitExceeded);
            }
            if byte == b'\r' || byte == b'\n' {
                if let Some(event) = self.finish_line()? {
                    events.push(event);
                }
                self.skip_lf = byte == b'\r';
            } else {
                self.line.push(byte);
            }
        }
        Ok(events)
    }

    fn finish_line(&mut self) -> Result<Option<ResponsesSseEvent>, ResponsesSseError> {
        use ResponsesSseError as Error;
        let bytes = std::mem::take(&mut self.line);
        let mut line = std::str::from_utf8(&bytes).map_err(|_| Error::InvalidUtf8)?;
        if self.first_line {
            self.first_line = false;
            line = line.strip_prefix('\u{feff}').unwrap_or(line);
        }
        if line.is_empty() {
            self.frame_bytes = 0;
            let event_type = self.event_type.take();
            if self.data.is_empty() {
                return Ok(None);
            }
            // Each data line contributes LF, including empty data lines. SSE
            // strips only the final LF at dispatch, preserving multiline JSON.
            let mut data = std::mem::take(&mut self.data);
            data.pop();
            if self.done {
                return Err(Error::EventAfterDone);
            }
            if self.event_count >= self.limits.max_events {
                return Err(Error::EventLimitExceeded);
            }
            self.event_count += 1;
            if data == "[DONE]" {
                if event_type
                    .as_deref()
                    .is_some_and(|kind| !kind.is_empty() && kind != "message")
                {
                    return Err(Error::InvalidEventType);
                }
                self.done = true;
                return Ok(Some(ResponsesSseEvent::Done));
            }
            let data: Value = serde_json::from_str(&data).map_err(|_| Error::InvalidJson)?;
            let kind = data["type"].as_str().ok_or(Error::InvalidEventType)?;
            if kind.is_empty()
                || !kind.is_ascii()
                || kind
                    .bytes()
                    .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
                || event_type
                    .as_deref()
                    .is_some_and(|label| !label.is_empty() && label != "message" && label != kind)
            {
                return Err(Error::InvalidEventType);
            }
            return Ok(Some(ResponsesSseEvent::Event {
                kind: kind.to_owned(),
                data,
            }));
        }
        if line.starts_with(':') {
            return Ok(None);
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "data" => {
                self.data.push_str(value);
                self.data.push('\n');
            }
            "event" => self.event_type = Some(value.to_owned()),
            // Includes id/retry. No automatic resume or repeated model request.
            _ => {}
        }
        Ok(None)
    }

    /// EOF with pending event data is failure, not an implicit dispatch. A clean
    /// EOF still requires a validated terminal response in the semantic layer.
    pub fn finish(&mut self) -> Result<(), ResponsesSseError> {
        if self.closed {
            return Err(ResponsesSseError::Closed);
        }
        let truncated = !self.line.is_empty() || !self.data.is_empty() || self.event_type.is_some();
        self.close();
        if truncated {
            Err(ResponsesSseError::TruncatedFrame)
        } else {
            Ok(())
        }
    }

    fn close(&mut self) {
        self.closed = true;
        self.line.clear();
        self.data.clear();
        self.event_type = None;
    }
}
