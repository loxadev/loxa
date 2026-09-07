use bytes::{BufMut, BytesMut};
use serde::{de::DeserializeOwned, Serialize};
use tokio::net::UnixStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub const MAX_FRAME_BYTES: usize = 32 * 1024;

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
    let mut writer = BytesMut::with_capacity(MAX_FRAME_BYTES)
        .limit(MAX_FRAME_BYTES)
        .writer();
    serde_json::to_writer(&mut writer, value).map_err(|error| error.to_string())?;
    Ok(writer.into_inner().into_inner())
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    if bytes.len() > MAX_FRAME_BYTES {
        return Err("IPC frame exceeds the configured limit".into());
    }
    serde_json::from_slice(bytes).map_err(|error| error.to_string())
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
}
