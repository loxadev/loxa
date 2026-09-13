use bytes::{BufMut, BytesMut};
use serde::{de::DeserializeOwned, Serialize};
use tokio::net::UnixStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub const MAX_FRAME_BYTES: usize = 32 * 1024;
pub const MAX_HISTORY_FRAME_BYTES: usize = 256 * 1024;
// The largest current wire collections contain 50 metadata records. Keeping
// this comfortably above that encoded shape prevents serde's internally
// tagged buffering from multiplying a compact adversarial object array past
// the per-connection allocation budget.
const MAX_JSON_STRUCTURAL_TOKENS: usize = 2 * 1024;

pub type IpcFramed = Framed<UnixStream, LengthDelimitedCodec>;

pub fn framed(stream: UnixStream) -> IpcFramed {
    let codec = LengthDelimitedCodec::builder()
        .length_field_type::<u32>()
        .big_endian()
        .max_frame_length(MAX_FRAME_BYTES)
        .new_codec();
    // The codec validates the length header before reserving the frame body.
    // Starting small avoids reserving the maximum for every idle connection;
    // MAX_FRAME_BYTES is the payload bound, not a whole-process memory cap.
    Framed::new(stream, codec)
}

pub fn encode<T: Serialize>(value: &T) -> Result<BytesMut, String> {
    encode_with_limit(value, MAX_FRAME_BYTES)
}

pub fn encode_with_limit<T: Serialize>(value: &T, limit: usize) -> Result<BytesMut, String> {
    if !matches!(limit, MAX_FRAME_BYTES | MAX_HISTORY_FRAME_BYTES) {
        return Err("invalid IPC frame limit".into());
    }
    let mut writer = BytesMut::with_capacity(limit).limit(limit).writer();
    serde_json::to_writer(&mut writer, value).map_err(|error| error.to_string())?;
    Ok(writer.into_inner().into_inner())
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    decode_with_limit(bytes, MAX_FRAME_BYTES)
}

pub fn decode_with_limit<T: DeserializeOwned>(bytes: &[u8], limit: usize) -> Result<T, String> {
    if !matches!(limit, MAX_FRAME_BYTES | MAX_HISTORY_FRAME_BYTES) {
        return Err("invalid IPC frame limit".into());
    }
    if bytes.len() > limit {
        return Err("IPC frame exceeds the configured limit".into());
    }
    bound_json_structure(bytes)?;
    serde_json::from_slice(bytes).map_err(|error| error.to_string())
}

// Serde's internally tagged enums temporarily buffer nested values. Bound the
// number of JSON containers and entries before deserialization so a compact
// scalar array cannot turn one 256 KiB frame into multi-megabyte Content Vecs.
// Full syntax and DTO validation remains serde's responsibility.
fn bound_json_structure(bytes: &[u8]) -> Result<(), String> {
    let mut in_string = false;
    let mut escaped = false;
    let mut tokens = 0_usize;
    for &byte in bytes {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'}' | b'[' | b']' | b',' | b':' => {
                tokens = tokens.saturating_add(1);
                if tokens > MAX_JSON_STRUCTURAL_TOKENS {
                    return Err("IPC JSON structure exceeds the configured limit".into());
                }
            }
            _ => {}
        }
    }
    Ok(())
}

pub fn set_frame_limit(framed: &mut IpcFramed, limit: usize) -> Result<(), String> {
    if !matches!(limit, MAX_FRAME_BYTES | MAX_HISTORY_FRAME_BYTES) {
        return Err("invalid IPC frame limit".into());
    }
    framed.codec_mut().set_max_frame_length(limit);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Large<'a> {
        value: &'a str,
    }

    #[test]
    fn serialization_stops_at_the_bound() {
        assert!(encode(&Large {
            value: &"x".repeat(MAX_FRAME_BYTES),
        })
        .is_err());
        assert!(encode(&Large { value: "small" }).is_ok());
    }

    #[test]
    fn decoder_rejects_trailing_data_and_oversized_input() {
        assert!(decode::<serde_json::Value>(br#"{"ok":true} false"#).is_err());
        assert!(decode::<serde_json::Value>(&vec![b' '; MAX_FRAME_BYTES + 1]).is_err());
    }

    #[test]
    fn negotiated_history_frames_use_the_larger_bound_only_when_selected() {
        let value = Large {
            value: &"x".repeat(MAX_FRAME_BYTES),
        };
        assert!(encode(&value).is_err());
        let encoded = encode_with_limit(&value, MAX_HISTORY_FRAME_BYTES).unwrap();
        assert!(decode::<serde_json::Value>(&encoded).is_err());
        assert!(decode_with_limit::<serde_json::Value>(&encoded, MAX_HISTORY_FRAME_BYTES).is_ok());
    }

    #[test]
    fn decoder_rejects_compact_structural_amplification_before_dto_buffering() {
        let mut payload = String::from(
            r#"{"type":"request","request_id":"r","command":{"type":"history","command":{"type":"get_history_status","padding":["#,
        );
        payload.push_str(&"0,".repeat(MAX_JSON_STRUCTURAL_TOKENS));
        payload.push_str("0]}}}");
        assert!(payload.len() < MAX_HISTORY_FRAME_BYTES);

        let error =
            decode_with_limit::<crate::ClientEnvelope>(payload.as_bytes(), MAX_HISTORY_FRAME_BYTES)
                .unwrap_err();
        assert!(error.contains("structure"), "{error}");
    }

    #[test]
    fn decoder_accepts_the_maximum_legal_conversation_page_count() {
        use crate::{
            ConversationPage, ConversationSummary, HistoryReply, Reply, ReplyOutcome,
            ServerEnvelope, MAX_CONVERSATION_PAGE_ITEMS,
        };

        let conversation = ConversationSummary {
            id: "a".repeat(32),
            model_id: "m".into(),
            title: "t".into(),
            created_ms: i64::MAX.to_string(),
            updated_ms: i64::MAX.to_string(),
            revision: i64::MAX.to_string(),
            profile_revision: i64::MAX.to_string(),
        };
        let envelope = ServerEnvelope::Reply(Reply {
            request_id: "r".into(),
            outcome: ReplyOutcome::History {
                reply: HistoryReply::ConversationPage(ConversationPage {
                    conversations: vec![conversation; MAX_CONVERSATION_PAGE_ITEMS],
                    next: None,
                }),
            },
        });
        let encoded = encode_with_limit(&envelope, MAX_HISTORY_FRAME_BYTES).unwrap();
        let decoded =
            decode_with_limit::<ServerEnvelope>(&encoded, MAX_HISTORY_FRAME_BYTES).unwrap();
        decoded.validate_shape().unwrap();
    }
}
