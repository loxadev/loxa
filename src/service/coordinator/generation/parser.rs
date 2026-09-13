use crate::history::MAX_SUFFIX_BYTES;
use serde::de::{IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt;

pub(super) const MAX_EVENT_BYTES: usize = 1024 * 1024;
pub(super) const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;

pub(in crate::service::coordinator) struct DecodeStep {
    pub(in crate::service::coordinator) consumed: usize,
    pub(in crate::service::coordinator) chunk: Option<String>,
    pub(in crate::service::coordinator) done: bool,
}

pub(in crate::service::coordinator) struct SseDecoder {
    line: Vec<u8>,
    data: Vec<u8>,
    has_data: bool,
    event_bytes: usize,
    generated_end: usize,
    prompt_tokens: Option<u32>,
    cached_prompt_tokens: Option<u32>,
    done: bool,
    poisoned: bool,
    skip_lf: bool,
    decoded: Option<DecodedContent>,
    chunk: String,
    chunk_ready: bool,
}

struct DecodedContent {
    content: String,
    offset: usize,
}

impl SseDecoder {
    pub(in crate::service::coordinator) fn new() -> Self {
        Self {
            line: Vec::with_capacity(4096),
            data: Vec::with_capacity(4096),
            has_data: false,
            event_bytes: 0,
            generated_end: 0,
            prompt_tokens: None,
            cached_prompt_tokens: None,
            done: false,
            poisoned: false,
            skip_lf: false,
            decoded: None,
            chunk: String::new(),
            chunk_ready: false,
        }
    }

    pub(in crate::service::coordinator) fn generated_end(&self) -> usize {
        self.generated_end
    }

    pub(super) fn prompt_tokens(&self) -> Option<u32> {
        self.prompt_tokens
    }

    #[cfg(all(test, target_os = "macos"))]
    pub(super) fn cached_prompt_tokens(&self) -> Option<u32> {
        self.cached_prompt_tokens
    }

    pub(super) fn retained_backing_bytes(&self) -> usize {
        self.line
            .capacity()
            .saturating_add(self.data.capacity())
            .saturating_add(self.chunk.capacity())
            .saturating_add(
                self.decoded
                    .as_ref()
                    .map_or(0, |decoded| decoded.content.capacity()),
            )
    }

    pub(in crate::service::coordinator) fn push(
        &mut self,
        bytes: &[u8],
    ) -> Result<DecodeStep, String> {
        if self.poisoned {
            return Err("engine stream parser is no longer usable".into());
        }
        self.fill_chunk();
        if let Some(chunk) = self.take_ready_chunk() {
            return Ok(DecodeStep {
                consumed: 0,
                chunk: Some(chunk),
                done: false,
            });
        }
        if self.done {
            return Ok(DecodeStep {
                consumed: 0,
                chunk: None,
                done: true,
            });
        }

        let mut consumed = 0;
        for &byte in bytes {
            self.event_bytes = self
                .event_bytes
                .checked_add(1)
                .ok_or_else(event_too_large)?;
            if self.event_bytes > MAX_EVENT_BYTES {
                return self.poison(event_too_large());
            }
            consumed += 1;
            if self.skip_lf {
                self.skip_lf = false;
                if byte == b'\n' {
                    continue;
                }
            }
            if byte == b'\r' || byte == b'\n' {
                if byte == b'\r' {
                    self.skip_lf = true;
                }
                if let Err(error) = self.end_line() {
                    return self.poison(error);
                }
                self.fill_chunk();
                if let Some(chunk) = self.take_ready_chunk() {
                    return Ok(DecodeStep {
                        consumed,
                        chunk: Some(chunk),
                        done: false,
                    });
                }
                if self.done {
                    break;
                }
            } else {
                self.line.push(byte);
            }
        }
        Ok(DecodeStep {
            consumed,
            chunk: None,
            done: self.done,
        })
    }

    pub(super) fn finish(&self) -> Result<(), String> {
        if self.poisoned {
            return Err("engine stream parser is no longer usable".into());
        }
        if self.done && self.decoded.is_none() {
            Ok(())
        } else {
            Err("engine stream ended before its terminal event".into())
        }
    }

    pub(in crate::service::coordinator) fn take_remaining_chunk(&mut self) -> Option<String> {
        self.fill_chunk();
        if let Some(chunk) = self.take_ready_chunk() {
            return Some(chunk);
        }
        if self.decoded.is_none() && !self.chunk.is_empty() {
            return Some(std::mem::take(&mut self.chunk));
        }
        None
    }

    fn end_line(&mut self) -> Result<(), String> {
        if self.line.is_empty() {
            self.dispatch()?;
            self.data = Vec::new();
            self.has_data = false;
            self.event_bytes = 0;
            return Ok(());
        }
        self.process_line()?;
        self.line.clear();
        Ok(())
    }

    fn process_line(&mut self) -> Result<(), String> {
        if self.line.starts_with(b":") {
            return Ok(());
        }
        let Some(mut start) = self.line.starts_with(b"data:").then_some(5) else {
            return Ok(());
        };
        if self.line.get(start) == Some(&b' ') {
            start += 1;
        }
        let value_len = self.line.len().saturating_sub(start);
        let separator = usize::from(self.has_data);
        if self
            .data
            .len()
            .saturating_add(separator)
            .saturating_add(value_len)
            > MAX_EVENT_BYTES
        {
            return Err(event_too_large());
        }
        if !self.has_data {
            self.line.drain(..start);
            std::mem::swap(&mut self.line, &mut self.data);
        } else {
            self.data.push(b'\n');
            self.data.extend_from_slice(&self.line[start..]);
        }
        self.has_data = true;
        Ok(())
    }

    fn dispatch(&mut self) -> Result<(), String> {
        if !self.has_data {
            return Ok(());
        }
        if self.data == b"[DONE]" {
            self.done = true;
            return Ok(());
        }
        let payload: StreamPayload = serde_json::from_slice(&self.data)
            .map_err(|_| "engine returned malformed streaming JSON".to_string())?;
        if payload.error.is_some() {
            return Err("engine reported a generation failure".into());
        }
        let choice = payload.choices.0;
        if let Some(usage) = payload.usage {
            if choice.is_some() {
                return Err("engine returned usage with a streaming choice".into());
            }
            let cached = usage
                .prompt_tokens_details
                .map(|details| details.cached_tokens);
            if cached.is_some_and(|cached| cached > usage.prompt_tokens) {
                return Err("engine returned an invalid cached-token count".into());
            }
            match (self.prompt_tokens, self.cached_prompt_tokens) {
                (None, None) => {
                    self.prompt_tokens = Some(usage.prompt_tokens);
                    self.cached_prompt_tokens = cached;
                }
                (Some(existing), existing_cached)
                    if existing == usage.prompt_tokens && existing_cached == cached => {}
                (Some(_), _) => return Err("engine changed its streaming input-token count".into()),
                _ => return Err("engine changed its streaming usage details".into()),
            }
            return Ok(());
        }
        let Some(choice) = choice else {
            return Ok(());
        };
        if choice.index != 0 {
            return Err("engine returned an unexpected streaming choice".into());
        }
        let Some(content) = choice.delta.content.filter(|content| !content.is_empty()) else {
            return Ok(());
        };
        let next = self
            .generated_end
            .checked_add(content.len())
            .ok_or_else(output_too_large)?;
        if next > MAX_OUTPUT_BYTES {
            return Err(output_too_large());
        }
        self.generated_end = next;
        self.decoded = Some(DecodedContent { content, offset: 0 });
        Ok(())
    }

    fn fill_chunk(&mut self) {
        loop {
            if self
                .decoded
                .as_ref()
                .is_some_and(|decoded| decoded.offset == decoded.content.len())
            {
                self.decoded = None;
                continue;
            }
            let Some(decoded) = self.decoded.as_ref() else {
                return;
            };
            if self.chunk.capacity() == 0 {
                self.chunk = String::with_capacity(MAX_SUFFIX_BYTES);
            }
            let available = MAX_SUFFIX_BYTES.saturating_sub(self.chunk.len());
            let remaining = &decoded.content[decoded.offset..];
            let split = utf8_prefix(remaining, available);
            if split == 0 {
                self.chunk_ready = true;
                return;
            }
            self.chunk.push_str(&remaining[..split]);
            self.decoded
                .as_mut()
                .expect("decoded content exists")
                .offset += split;
            if self.chunk.len() == MAX_SUFFIX_BYTES {
                self.chunk_ready = true;
                return;
            }
        }
    }

    fn take_ready_chunk(&mut self) -> Option<String> {
        self.chunk_ready.then(|| {
            self.chunk_ready = false;
            std::mem::take(&mut self.chunk)
        })
    }

    fn poison<T>(&mut self, error: String) -> Result<T, String> {
        self.poisoned = true;
        self.line.clear();
        self.data.clear();
        Err(error)
    }
}

fn utf8_prefix(value: &str, maximum: usize) -> usize {
    if value.len() <= maximum {
        return value.len();
    }
    let mut split = maximum;
    while split > 0 && !value.is_char_boundary(split) {
        split -= 1;
    }
    split
}

#[derive(Deserialize)]
struct StreamPayload {
    #[serde(default)]
    choices: AtMostOneChoice,
    error: Option<EngineError>,
    usage: Option<Usage>,
}

#[derive(Deserialize)]
struct Usage {
    prompt_tokens: u32,
    prompt_tokens_details: Option<PromptTokenDetails>,
}

#[derive(Deserialize)]
struct PromptTokenDetails {
    cached_tokens: u32,
}

#[derive(Default)]
struct AtMostOneChoice(Option<Choice>);

impl<'de> Deserialize<'de> for AtMostOneChoice {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ChoiceVisitor;
        impl<'de> Visitor<'de> for ChoiceVisitor {
            type Value = AtMostOneChoice;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("zero or one streaming choice")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let first = sequence.next_element::<Choice>()?;
                if sequence.next_element::<IgnoredAny>()?.is_some() {
                    return Err(serde::de::Error::custom(
                        "engine returned multiple streaming choices",
                    ));
                }
                Ok(AtMostOneChoice(first))
            }
        }
        deserializer.deserialize_seq(ChoiceVisitor)
    }
}

#[derive(Deserialize)]
struct Choice {
    index: u32,
    delta: Delta,
}

#[derive(Deserialize)]
struct Delta {
    content: Option<String>,
}

struct EngineError;

impl<'de> Deserialize<'de> for EngineError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ErrorVisitor;
        impl<'de> Visitor<'de> for ErrorVisitor {
            type Value = EngineError;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an engine error object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(EngineError)
            }
        }
        deserializer.deserialize_map(ErrorVisitor)
    }
}

fn event_too_large() -> String {
    format!("engine streaming event exceeds {MAX_EVENT_BYTES} bytes")
}

fn output_too_large() -> String {
    format!("assistant output exceeds {MAX_OUTPUT_BYTES} bytes")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(chunks: &[&[u8]]) -> Result<(Vec<String>, usize, usize), String> {
        let mut decoder = SseDecoder::new();
        let mut output = Vec::new();
        for bytes in chunks {
            let mut offset = 0;
            loop {
                let step = decoder.push(&bytes[offset..])?;
                offset += step.consumed;
                let emitted = step.chunk.is_some();
                if let Some(chunk) = step.chunk {
                    output.push(chunk);
                }
                if step.done {
                    break;
                }
                if offset == bytes.len() && !emitted {
                    break;
                }
                if step.consumed == 0 && !emitted {
                    return Err("decoder made no progress".into());
                }
            }
        }
        decoder.finish()?;
        while let Some(chunk) = decoder.take_remaining_chunk() {
            output.push(chunk);
        }
        Ok((
            output,
            decoder.generated_end(),
            decoder.retained_backing_bytes(),
        ))
    }

    #[test]
    fn fragmented_utf8_and_crlf_are_emitted_without_a_full_answer_copy() {
        let body = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"héllo\"}}]}\r\n\r\ndata: [DONE]\n\n";
        let split = body.find('é').unwrap() + 1;
        let (output, generated_end, _) =
            decode(&[&body.as_bytes()[..split], &body.as_bytes()[split..]]).unwrap();
        assert_eq!(generated_end, 6);
        assert_eq!(output.concat(), "héllo");
    }

    #[test]
    fn terminal_usage_records_total_prompt_tokens_without_becoming_output() {
        let body = concat!(
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":37,",
            "\"completion_tokens\":2,\"total_tokens\":39,",
            "\"prompt_tokens_details\":{\"cached_tokens\":11}}}\n\n",
            "data: [DONE]\n\n",
        );
        let mut decoder = SseDecoder::new();
        let step = decoder.push(body.as_bytes()).unwrap();
        assert!(step.done);
        assert!(step.chunk.is_none());
        decoder.finish().unwrap();
        assert_eq!(decoder.prompt_tokens(), Some(37));
        assert_eq!(decoder.cached_prompt_tokens, Some(11));
        assert_eq!(decoder.generated_end(), 0);

        let mut invalid = SseDecoder::new();
        assert!(invalid
            .push(
                b"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}],\"usage\":{\"prompt_tokens\":1}}\n\n"
            )
            .unwrap_err()
            .contains("usage with a streaming choice"));
    }

    #[test]
    fn one_large_event_is_sliced_into_bounded_utf8_chunks() {
        let content = "é".repeat(300_000);
        let body = format!(
            "data: {{\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"{content}\"}}}}]}}\n\ndata: [DONE]\n\n"
        );
        let (chunks, generated_end, backing) = decode(&[body.as_bytes()]).unwrap();
        assert_eq!(generated_end, content.len());
        assert_eq!(chunks.concat(), content);
        assert!(chunks
            .iter()
            .all(|chunk| chunk.capacity() <= MAX_SUFFIX_BYTES));
        assert!(backing <= 3 * MAX_EVENT_BYTES);
    }

    #[test]
    fn uneven_events_keep_each_canonical_chunk_at_exact_capacity() {
        let first = "a".repeat(40 * 1024);
        let second = "b".repeat(24 * 1024);
        let body = format!(
            "data: {{\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"{first}\"}}}}]}}\n\
             \ndata: {{\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"{second}\"}}}}]}}\n\
             \ndata: [DONE]\n\n"
        );
        let (chunks, generated_end, _) = decode(&[body.as_bytes()]).unwrap();
        assert_eq!(generated_end, MAX_SUFFIX_BYTES);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), MAX_SUFFIX_BYTES);
        assert_eq!(chunks[0].capacity(), MAX_SUFFIX_BYTES);
    }

    #[test]
    fn interrupted_large_event_keeps_every_observed_byte_drainable() {
        let content = "é".repeat(300_000);
        let body = format!(
            "data: {{\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"{content}\"}}}}]}}\n\n"
        );
        let mut decoder = SseDecoder::new();
        let first = decoder.push(body.as_bytes()).unwrap().chunk.unwrap();
        let mut recovered = first;
        while let Some(chunk) = decoder.take_remaining_chunk() {
            assert!(chunk.capacity() <= MAX_SUFFIX_BYTES);
            recovered.push_str(&chunk);
        }
        assert_eq!(recovered, content);
        assert_eq!(decoder.generated_end(), recovered.len());
    }

    #[test]
    fn terminal_requires_a_dispatched_event_and_errors_poison_the_decoder() {
        let mut decoder = SseDecoder::new();
        decoder.push(b"data: [DONE]").unwrap();
        assert_eq!(
            decoder.finish().unwrap_err(),
            "engine stream ended before its terminal event"
        );

        let mut decoder = SseDecoder::new();
        assert!(decoder
            .push(b"data: {\"choices\":[{\"index\":1,\"delta\":{\"content\":\"x\"}}]}\n\n")
            .unwrap_err()
            .contains("unexpected streaming choice"));
        assert!(decoder.push(b"data: [DONE]\n\n").is_err());
    }

    #[test]
    fn terminal_event_ignores_same_frame_trailing_content_for_all_delimiters() {
        for delimiter in ["\n\n", "\r\r", "\r\n\r\n"] {
            let body = format!(
                "data: [DONE]{delimiter}data: {{\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"BAD\"}}}}]}}\n\n"
            );
            let mut decoder = SseDecoder::new();
            let step = decoder.push(body.as_bytes()).unwrap();
            assert!(step.done);
            assert!(step.chunk.is_none());
            assert_eq!(decoder.generated_end(), 0);
            assert!(
                decoder
                    .push(&body.as_bytes()[step.consumed..])
                    .unwrap()
                    .done
            );
            assert_eq!(decoder.generated_end(), 0);
        }
    }

    #[test]
    fn split_crlf_terminal_consumes_no_following_event() {
        let mut decoder = SseDecoder::new();
        assert!(!decoder.push(b"data: [DONE]\r").unwrap().done);
        let trailing = b"\n\r\ndata: {broken}\r\n\r\n";
        let step = decoder.push(trailing).unwrap();
        assert!(step.done);
        assert_eq!(decoder.generated_end(), 0);
        assert!(decoder.push(&trailing[step.consumed..]).unwrap().done);
    }

    #[test]
    fn multiple_choices_and_large_engine_errors_fail_without_retaining_messages() {
        let mut decoder = SseDecoder::new();
        assert!(decoder
            .push(
                b"data: {\"choices\":[{\"index\":0,\"delta\":{}},{\"index\":1,\"delta\":{}}]}\n\n"
            )
            .unwrap_err()
            .contains("malformed streaming JSON"));

        let message = "x".repeat(MAX_EVENT_BYTES / 2);
        let event = format!("data: {{\"choices\":[],\"error\":{{\"message\":\"{message}\"}}}}\n\n");
        let mut decoder = SseDecoder::new();
        assert_eq!(
            decoder.push(event.as_bytes()).unwrap_err(),
            "engine reported a generation failure"
        );
        assert!(decoder.retained_backing_bytes() <= 3 * MAX_EVENT_BYTES);
    }

    #[test]
    fn crlf_comments_count_toward_the_raw_event_limit() {
        let comments = ":\r\n".repeat(MAX_EVENT_BYTES / 3 + 1);
        let mut decoder = SseDecoder::new();
        assert!(decoder.push(comments.as_bytes()).is_err());
    }

    #[test]
    fn event_and_total_output_limits_fail_at_the_boundary() {
        let mut decoder = SseDecoder::new();
        assert!(decoder.push(&vec![b'x'; MAX_EVENT_BYTES]).is_ok());
        assert!(decoder.push(b"x").is_err());

        let mut decoder = SseDecoder::new();
        decoder.generated_end = MAX_OUTPUT_BYTES;
        assert!(decoder
            .push(b"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}]}\n\n")
            .unwrap_err()
            .contains("assistant output"));
    }
}
