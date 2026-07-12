#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn token_is_created_once_private_and_never_disclosed_by_debug() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.token");
        let first = ControlToken::load_or_create(&path).unwrap();
        let second = ControlToken::load_or_create(&path).unwrap();
        assert!(first.matches(&second));
        assert_eq!(format!("{first:?}"), "ControlToken([REDACTED])");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn concurrent_token_creation_publishes_one_value_without_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.token");
        let workers = (0..8)
            .map(|_| {
                let path = path.clone();
                std::thread::spawn(move || ControlToken::load_or_create(&path).unwrap())
            })
            .collect::<Vec<_>>();
        let tokens = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert!(tokens.iter().all(|token| token.matches(&tokens[0])));
    }

    #[test]
    fn malformed_and_insecure_existing_tokens_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.token");
        fs::write(&path, "short").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert_eq!(
            ControlToken::load_or_create(&path).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::write(
                &path,
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n",
            )
            .unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
            assert_eq!(
                ControlToken::load_or_create(&path).unwrap_err().kind(),
                std::io::ErrorKind::PermissionDenied
            );
        }
    }

    #[test]
    fn bearer_and_origin_policy_fail_closed() {
        let token = ControlToken::from_bytes([7; 32]);
        let bearer = format!("Bearer {}", token.expose_for_authorization());
        let exposed = token.expose_for_authorization();
        let policy = AuthPolicy::new(token, ["tauri://localhost", "http://127.0.0.1:1420"]);
        assert!(
            policy
                .authorize(Some("tauri://localhost"), Some(&bearer))
                .is_ok()
        );
        assert!(policy.authorize(None, Some(&bearer)).is_ok());
        assert_eq!(
            policy.authorize(Some("https://evil.invalid"), Some(&bearer)),
            Err(AuthError::OriginDenied)
        );
        assert_eq!(
            policy.authorize(Some("tauri://localhost"), None),
            Err(AuthError::MissingBearer)
        );
        assert_eq!(
            policy.authorize(Some("tauri://localhost"), Some("Bearer wrong")),
            Err(AuthError::WrongBearer)
        );
        assert!(!format!("{policy:?}").contains(&exposed));
    }
}
use std::collections::BTreeSet;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use subtle::ConstantTimeEq;

const TOKEN_BYTES: usize = 32;
const TOKEN_HEX_LEN: usize = TOKEN_BYTES * 2;

#[derive(Clone, PartialEq, Eq)]
pub struct ControlToken([u8; TOKEN_BYTES]);

impl fmt::Debug for ControlToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ControlToken([REDACTED])")
    }
}

impl ControlToken {
    #[cfg(test)]
    fn from_bytes(bytes: [u8; TOKEN_BYTES]) -> Self {
        Self(bytes)
    }

    pub fn load_or_create(path: &Path) -> io::Result<Self> {
        if path.exists() {
            return Self::load(path);
        }
        let parent = path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "control token path has no parent",
            )
        })?;
        fs::create_dir_all(parent)?;
        let mut bytes = [0_u8; TOKEN_BYTES];
        getrandom::fill(&mut bytes).map_err(io::Error::other)?;
        let token = Self(bytes);
        let mut suffix = [0_u8; 8];
        getrandom::fill(&mut suffix).map_err(io::Error::other)?;
        let temp = parent.join(format!(
            ".control-token-{}-{}",
            std::process::id(),
            encode_hex(&suffix)
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        let encoded = token.expose_for_authorization();
        if let Err(error) = (|| {
            file.write_all(encoded.as_bytes())?;
            file.write_all(b"\n")?;
            file.sync_all()
        })() {
            let _ = fs::remove_file(&temp);
            return Err(error);
        }
        match fs::hard_link(&temp, path) {
            Ok(()) => {
                fs::remove_file(&temp)?;
                sync_parent(parent)?;
                Ok(token)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let _ = fs::remove_file(&temp);
                Self::load(path)
            }
            Err(error) => {
                let _ = fs::remove_file(&temp);
                Err(error)
            }
        }
    }

    pub fn load(path: &Path) -> io::Result<Self> {
        validate_permissions(path)?;
        let text = fs::read_to_string(path)?;
        let trimmed = text.strip_suffix('\n').unwrap_or(&text);
        if trimmed.len() != TOKEN_HEX_LEN || trimmed.contains(char::is_whitespace) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "malformed control token",
            ));
        }
        decode_hex(trimmed).map(Self)
    }

    pub fn matches(&self, other: &Self) -> bool {
        bool::from(self.0.ct_eq(&other.0))
    }
    pub fn expose_for_authorization(&self) -> String {
        encode_hex(&self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthError {
    OriginDenied,
    MissingBearer,
    WrongBearer,
}

pub struct AuthPolicy {
    token: ControlToken,
    origins: BTreeSet<String>,
}

impl fmt::Debug for AuthPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthPolicy")
            .field("token", &"[REDACTED]")
            .field("origins", &self.origins)
            .finish()
    }
}

impl AuthPolicy {
    pub fn new<I, S>(token: ControlToken, origins: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            token,
            origins: origins.into_iter().map(Into::into).collect(),
        }
    }
    pub fn authorize(
        &self,
        origin: Option<&str>,
        authorization: Option<&str>,
    ) -> Result<(), AuthError> {
        if let Some(origin) = origin {
            if !self.origins.contains(origin) {
                return Err(AuthError::OriginDenied);
            }
        }
        let authorization = authorization.ok_or(AuthError::MissingBearer)?;
        let supplied = authorization
            .strip_prefix("Bearer ")
            .ok_or(AuthError::WrongBearer)?;
        let expected = self.token.expose_for_authorization();
        if supplied.len() != expected.len()
            || !bool::from(supplied.as_bytes().ct_eq(expected.as_bytes()))
        {
            return Err(AuthError::WrongBearer);
        }
        Ok(())
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}

fn decode_hex(text: &str) -> io::Result<[u8; TOKEN_BYTES]> {
    let mut bytes = [0_u8; TOKEN_BYTES];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Ok(bytes)
}
fn nibble(byte: u8) -> io::Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "malformed control token",
        )),
    }
}

#[cfg(unix)]
fn validate_permissions(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.file_type().is_file()
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "control token permissions must be 0600",
        ));
    }
    Ok(())
}
#[cfg(not(unix))]
fn validate_permissions(path: &Path) -> io::Result<()> {
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "control token must not be a symlink",
        ));
    }
    Ok(())
}

fn sync_parent(path: &Path) -> io::Result<()> {
    FileSync::sync(path)
}
struct FileSync;
impl FileSync {
    #[cfg(unix)]
    fn sync(path: &Path) -> io::Result<()> {
        fs::File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    fn sync(_path: &Path) -> io::Result<()> {
        Ok(())
    }
}
