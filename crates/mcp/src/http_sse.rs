//! Bounded SSE decoding for HTTP MCP responses, including id-only priming events.

use bytes::{Buf, Bytes};
use futures_util::{StreamExt, stream::BoxStream};
use sse_stream::{Error, Sse};

use crate::http_client::McpHttpError;

pub(crate) fn response_stream(
    response: reqwest::Response,
    max_event_bytes: usize,
    max_stream_bytes: usize,
) -> BoxStream<'static, Result<Sse, Error>> {
    let source = response.bytes_stream().boxed();
    let state = (
        source,
        Bytes::new(),
        Decoder::new(max_event_bytes),
        max_stream_bytes,
        false,
    );
    futures_util::stream::unfold(
        state,
        |(mut source, mut chunk, mut decoder, mut remaining, ended)| async move {
            if ended {
                return None;
            }
            loop {
                while chunk.has_remaining() {
                    let byte = chunk.get_u8();
                    match decoder.push(byte) {
                        Ok(Some(event)) => {
                            return Some((Ok(event), (source, chunk, decoder, remaining, false)));
                        }
                        Ok(None) => {}
                        Err(error) => {
                            return Some((
                                Err(Error::Body(Box::new(error))),
                                (source, chunk, decoder, remaining, true),
                            ));
                        }
                    }
                }
                match source.next().await {
                    Some(Ok(bytes)) => {
                        if bytes.len() > remaining {
                            return Some((
                                Err(Error::Body(Box::new(McpHttpError::BodyLimit))),
                                (source, chunk, decoder, remaining, true),
                            ));
                        }
                        remaining -= bytes.len();
                        chunk = bytes;
                    }
                    Some(Err(_)) => {
                        return Some((
                            Err(Error::Body(Box::new(McpHttpError::Network))),
                            (source, chunk, decoder, remaining, true),
                        ));
                    }
                    // SSE dispatch requires a blank line. An incomplete final event is discarded.
                    None => return None,
                }
            }
        },
    )
    .boxed()
}

struct Decoder {
    limit: usize,
    frame_bytes: usize,
    line: Vec<u8>,
    event: Sse,
    first_line: bool,
    skip_lf: bool,
}

impl Decoder {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            frame_bytes: 0,
            line: Vec::new(),
            event: Sse::default(),
            first_line: true,
            skip_lf: false,
        }
    }

    fn push(&mut self, byte: u8) -> Result<Option<Sse>, McpHttpError> {
        if self.skip_lf {
            self.skip_lf = false;
            if byte == b'\n' {
                return Ok(None);
            }
        }
        self.frame_bytes = self
            .frame_bytes
            .checked_add(1)
            .ok_or(McpHttpError::BodyLimit)?;
        if self.frame_bytes > self.limit {
            return Err(McpHttpError::BodyLimit);
        }
        if byte != b'\r' && byte != b'\n' {
            self.line.push(byte);
            return Ok(None);
        }
        self.skip_lf = byte == b'\r';
        let bytes = std::mem::take(&mut self.line);
        let mut line = String::from_utf8_lossy(&bytes);
        if self.first_line {
            self.first_line = false;
            if let Some(stripped) = line.strip_prefix('\u{feff}') {
                line = stripped.to_owned().into();
            }
        }
        if line.is_empty() {
            self.frame_bytes = 0;
            let event = std::mem::take(&mut self.event);
            return Ok((event != Sse::default()).then_some(event));
        }
        let (field, value) = line
            .split_once(':')
            .map_or((line.as_ref(), ""), |(field, value)| {
                (field, value.strip_prefix(' ').unwrap_or(value))
            });
        match field {
            "data" => match self.event.data.as_mut() {
                Some(data) => {
                    data.push('\n');
                    data.push_str(value);
                }
                None => self.event.data = Some(value.to_owned()),
            },
            "event" => self.event.event = (!value.is_empty()).then(|| value.to_owned()),
            "id" if !value.contains('\0') => self.event.id = Some(value.to_owned()),
            "retry" if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) => {
                if let Ok(retry) = value.parse() {
                    self.event.retry = Some(retry);
                }
            }
            _ => {}
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sse_fields_and_boundaries() {
        let mut decoder = Decoder::new(512);
        let input = "\u{feff}:comment\r\nunknown:ignored\rdata: one\r\ndata: two\nevent: old\nevent: message\nid: old\nid: new\nid: ignored\0\nretry: 12\nretry: invalid\n\nid:\n\n";
        let events: Vec<_> = input
            .bytes()
            .filter_map(|b| decoder.push(b).unwrap())
            .collect();
        assert_eq!(
            events,
            vec![
                Sse::default()
                    .data("one\ntwo")
                    .event("message")
                    .id("new")
                    .retry(12),
                Sse::default().id("")
            ]
        );
        let mut decoder = Decoder::new(8);
        assert!(
            b"data:a\n\n"
                .iter()
                .filter_map(|b| decoder.push(*b).unwrap())
                .next()
                .is_some()
        );
        assert!(
            b"data:b\n\n"
                .iter()
                .filter_map(|b| decoder.push(*b).unwrap())
                .next()
                .is_some()
        );
        for b in b":1234567" {
            decoder.push(*b).unwrap();
        }
        assert_eq!(decoder.push(b'8').unwrap_err(), McpHttpError::BodyLimit);
    }
}
