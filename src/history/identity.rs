use super::{HistoryError, HistoryErrorKind};
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn random_id() -> Result<[u8; 16], HistoryError> {
    let mut bytes = [0_u8; 16];
    let mut random = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open("/dev/urandom")
        .map_err(|_| HistoryError::new(HistoryErrorKind::Io, "OS randomness is unavailable"))?;
    random
        .read_exact(&mut bytes)
        .map_err(|_| HistoryError::new(HistoryErrorKind::Io, "OS randomness is unavailable"))?;
    Ok(bytes)
}

pub(super) fn unix_time_ms() -> Result<i64, HistoryError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| HistoryError::new(HistoryErrorKind::Io, "system clock is unavailable"))?
        .as_millis();
    i64::try_from(millis)
        .map_err(|_| HistoryError::new(HistoryErrorKind::LimitExceeded, "system time overflow"))
}

pub(super) fn next_updated_ms(previous: i64) -> Result<i64, HistoryError> {
    let now = unix_time_ms()?;
    if now > previous {
        Ok(now)
    } else {
        previous
            .checked_add(1)
            .ok_or_else(|| HistoryError::new(HistoryErrorKind::LimitExceeded, "timestamp overflow"))
    }
}

pub(crate) fn parse_revision(value: &str) -> Result<i64, HistoryError> {
    let revision = parse_nonnegative(value, "invalid conversation revision")?;
    if revision == 0 {
        return Err(HistoryError::new(
            HistoryErrorKind::InvalidInput,
            "invalid conversation revision",
        ));
    }
    Ok(revision)
}

pub(super) fn parse_nonnegative(value: &str, context: &'static str) -> Result<i64, HistoryError> {
    if value.is_empty()
        || value.len() > 19
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(HistoryError::new(HistoryErrorKind::InvalidInput, context));
    }
    value
        .parse::<i64>()
        .map_err(|_| HistoryError::new(HistoryErrorKind::InvalidInput, context))
}

pub(crate) fn decode_id(value: &str) -> Result<[u8; 16], HistoryError> {
    if value.len() != 32 {
        return Err(HistoryError::new(
            HistoryErrorKind::InvalidInput,
            "invalid history identity",
        ));
    }
    let mut bytes = [0_u8; 16];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_digit(pair[0])? << 4) | hex_digit(pair[1])?;
    }
    Ok(bytes)
}

pub(crate) fn encode_id(value: [u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(32);
    for byte in value {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn hex_digit(byte: u8) -> Result<u8, HistoryError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(HistoryError::new(
            HistoryErrorKind::InvalidInput,
            "invalid hexadecimal identity",
        )),
    }
}
