#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn development_origin_accepts_only_one_canonical_ipv4_loopback_port() {
        assert_eq!(
            validated_dev_origin("http://127.0.0.1:1421").as_deref(),
            Some("http://127.0.0.1:1421")
        );
        for rejected in [
            "http://127.0.0.1:0",
            "http://127.0.0.1:01421",
            "http://127.0.0.1:1421/",
            "http://localhost:1421",
            "http://0.0.0.0:1421",
            "https://127.0.0.1:1421",
            "http://127.0.0.1:65536",
            "http://127.0.0.1:1421.example.com",
        ] {
            assert_eq!(validated_dev_origin(rejected), None, "accepted {rejected}");
        }
        assert_eq!(
            desktop_origins_for(Some("http://127.0.0.1:1421"), true),
            ["tauri://localhost", "http://127.0.0.1:1421"]
        );
        assert_eq!(
            desktop_origins_for(Some("http://127.0.0.1:1421"), false),
            ["tauri://localhost", "http://127.0.0.1:1420"]
        );
        assert_eq!(
            desktop_origins_for(Some("http://localhost:1421"), true),
            ["tauri://localhost", "http://127.0.0.1:1420"]
        );
    }

    #[test]
    fn token_is_created_once_private_and_never_disclosed_by_debug() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".loxa").join("control.token");
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
        let path = dir.path().join(".loxa").join("control.token");
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

    #[cfg(unix)]
    #[test]
    fn token_creation_makes_a_private_owned_parent() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join(".loxa");
        ControlToken::load_or_create(&parent.join("control.token")).unwrap();
        let metadata = fs::metadata(parent).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
        assert_eq!(metadata.uid(), current_user_id());
    }

    #[test]
    fn token_matching_rejects_a_difference_in_every_byte_position() {
        let expected = ControlToken::from_bytes([7; TOKEN_BYTES]);
        assert!(expected.matches(&ControlToken::from_bytes([7; TOKEN_BYTES])));
        for index in 0..TOKEN_BYTES {
            let mut different = [7; TOKEN_BYTES];
            different[index] ^= 1;
            assert!(!expected.matches(&ControlToken::from_bytes(different)));
        }
    }

    #[test]
    fn node_identity_proof_is_domain_separated_binary_and_constant_time_verified() {
        use crate::control::contracts::NodeStatus;
        let token = ControlToken::from_bytes([7; TOKEN_BYTES]);
        let nonce = "01".repeat(32);
        let vectors = [
            (
                NodeStatus::Unloaded,
                "fec2cf06e169de949d213f6b0a9c7475890c0ff6a93070a6533deac9193a0e2d",
            ),
            (
                NodeStatus::Loading,
                "bc470c85531eb19dc0e6941a45731df9fc79e5e4b14311ef5f9010060d04e350",
            ),
            (
                NodeStatus::Ready,
                "a8168ad8ea645fdd1c91f495f8d28e3de0a1d13a7d728c218ce3797661baa1a0",
            ),
            (
                NodeStatus::Unloading,
                "cd67c4caeeac907169970d3a3cc421ffcff1b402977e63699b5bfe31d9fa5b7f",
            ),
            (
                NodeStatus::RecoveryRequired,
                "1433e64080eaeb1106f201d3bd5a35b7353da9452c0da10202fa0b7dfee07e1b",
            ),
            (
                NodeStatus::Error,
                "bce2ad7996d8d4e0c84fbef730ce63985b3fbf8f1a257317976e50f88a553319",
            ),
        ];
        for (status, expected) in vectors {
            assert_eq!(
                token
                    .node_identity_proof(&nonce, "node", "runtime", status)
                    .unwrap(),
                expected
            );
            assert!(token.verify_node_identity_proof(&nonce, "node", "runtime", status, expected));
        }
        let proof = "fec2cf06e169de949d213f6b0a9c7475890c0ff6a93070a6533deac9193a0e2d";
        assert!(!token.verify_node_identity_proof(
            &nonce,
            "node",
            "runtime-2",
            NodeStatus::Unloaded,
            proof,
        ));
        assert!(!token.verify_node_identity_proof(
            &nonce,
            "node",
            "runtime",
            NodeStatus::Ready,
            proof,
        ));
        assert!(!token.verify_node_identity_proof(
            &"02".repeat(32),
            "node",
            "runtime",
            NodeStatus::Unloaded,
            proof
        ));
        assert!(!token.verify_node_identity_proof(
            &nonce,
            "node-2",
            "runtime",
            NodeStatus::Unloaded,
            proof
        ));
        for malformed in [
            String::new(),
            "00".into(),
            "0".repeat(62),
            "0".repeat(66),
            "AA".repeat(32),
            format!("{}g", "0".repeat(63)),
        ] {
            assert!(!token.verify_node_identity_proof(
                &nonce,
                "node",
                "runtime",
                NodeStatus::Unloaded,
                &malformed
            ));
        }
        assert!(token
            .node_identity_proof("AA", "node", "runtime", NodeStatus::Ready)
            .is_err());
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
        assert!(policy
            .authorize(Some("tauri://localhost"), Some(&bearer))
            .is_ok());
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

    #[cfg(unix)]
    #[test]
    fn token_load_rejects_symlinks_unsafe_parents_and_wrong_owner_evidence() {
        use std::os::unix::fs::{symlink, MetadataExt, PermissionsExt};
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real.token");
        fs::write(
            &real,
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n",
        )
        .unwrap();
        fs::set_permissions(&real, fs::Permissions::from_mode(0o600)).unwrap();
        let link = dir.path().join("link.token");
        symlink(&real, &link).unwrap();
        assert_eq!(
            ControlToken::load(&link).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );

        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o777)).unwrap();
        assert_eq!(
            ControlToken::load(&real).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();

        let file = open_token_file(&real).unwrap();
        assert_eq!(
            validate_open_token(&file, file.metadata().unwrap().uid().saturating_add(1))
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[cfg(unix)]
    #[test]
    fn opened_token_descriptor_is_not_redirected_by_path_swap() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.token");
        fs::write(
            &path,
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n",
        )
        .unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let mut file = open_token_file(&path).unwrap();
        validate_open_token(&file, current_user_id()).unwrap();
        let moved = dir.path().join("moved.token");
        fs::rename(&path, &moved).unwrap();
        symlink("/dev/null", &path).unwrap();
        let token = read_token_from(&mut file).unwrap();
        assert_eq!(
            token.expose_for_authorization(),
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
    }
}
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use std::collections::BTreeSet;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;
use subtle::ConstantTimeEq;

const TOKEN_BYTES: usize = 32;
const TOKEN_HEX_LEN: usize = TOKEN_BYTES * 2;

#[derive(Clone)]
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
        let parent = path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "control token path has no parent",
            )
        })?;
        ensure_secure_parent(parent)?;
        if path.exists() {
            return Self::load(path);
        }
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
        let parent = path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "control token path has no parent",
            )
        })?;
        ensure_secure_parent(parent)?;
        let mut file = open_token_file(path)?;
        validate_open_token(&file, current_user_id())?;
        read_token_from(&mut file)
    }

    fn parse(text: &str) -> io::Result<Self> {
        let trimmed = text.strip_suffix('\n').unwrap_or(text);
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

    pub fn node_identity_proof(
        &self,
        nonce_hex: &str,
        node_id: &str,
        runtime_identity: &str,
        status: crate::control::contracts::NodeStatus,
    ) -> io::Result<String> {
        let nonce = decode_hex_32(nonce_hex)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0)
            .map_err(|_| io::Error::other("invalid control proof key"))?;
        mac.update(b"loxa-control-node-identity-v1\0");
        mac.update(&crate::control::contracts::CONTROL_PROTOCOL_VERSION.to_be_bytes());
        mac.update(&nonce);
        update_length_prefixed(&mut mac, node_id.as_bytes())?;
        update_length_prefixed(&mut mac, runtime_identity.as_bytes())?;
        mac.update(&[status.proof_discriminant()]);
        Ok(encode_hex(&mac.finalize().into_bytes()))
    }

    pub fn verify_node_identity_proof(
        &self,
        nonce_hex: &str,
        node_id: &str,
        runtime_identity: &str,
        status: crate::control::contracts::NodeStatus,
        supplied_hex: &str,
    ) -> bool {
        let Ok(expected) = self.node_identity_proof(nonce_hex, node_id, runtime_identity, status)
        else {
            return false;
        };
        let (Ok(expected), Ok(supplied)) = (decode_hex_32(&expected), decode_hex_32(supplied_hex))
        else {
            return false;
        };
        bool::from(expected.ct_eq(&supplied))
    }
}

fn update_length_prefixed(mac: &mut Hmac<Sha256>, bytes: &[u8]) -> io::Result<()> {
    let length = u32::try_from(bytes.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "control identity field too long",
        )
    })?;
    mac.update(&length.to_be_bytes());
    mac.update(bytes);
    Ok(())
}

fn decode_hex_32(text: &str) -> io::Result<[u8; 32]> {
    if text.len() != 64
        || text
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "malformed lowercase hex proof",
        ));
    }
    decode_hex(text)
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
        let supplied = ControlToken::parse(supplied).map_err(|_| AuthError::WrongBearer)?;
        if !self.token.matches(&supplied) {
            return Err(AuthError::WrongBearer);
        }
        Ok(())
    }
}

pub fn desktop_origins() -> Vec<String> {
    desktop_origins_for(
        std::env::var("LOXA_DEV_ORIGIN").ok().as_deref(),
        cfg!(debug_assertions),
    )
}

pub fn is_desktop_origin(origin: &str) -> bool {
    desktop_origins().iter().any(|allowed| allowed == origin)
}

fn validated_dev_origin(value: &str) -> Option<String> {
    let port = value
        .strip_prefix("http://127.0.0.1:")?
        .parse::<u16>()
        .ok()?;
    if port == 0 || value != format!("http://127.0.0.1:{port}") {
        return None;
    }
    Some(value.to_owned())
}

fn desktop_origins_for(dev_origin: Option<&str>, development: bool) -> Vec<String> {
    let browser_origin = if development {
        dev_origin
            .and_then(validated_dev_origin)
            .unwrap_or_else(|| "http://127.0.0.1:1420".to_owned())
    } else {
        "http://127.0.0.1:1420".to_owned()
    };
    vec!["tauri://localhost".to_owned(), browser_origin]
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

fn read_token_from(file: &mut File) -> io::Result<ControlToken> {
    let mut text = String::new();
    file.take((TOKEN_HEX_LEN + 2) as u64)
        .read_to_string(&mut text)?;
    ControlToken::parse(&text)
}

#[cfg(unix)]
fn open_token_file(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
    {
        Ok(file) => Ok(file),
        Err(_error)
            if fs::symlink_metadata(path)
                .is_ok_and(|metadata| metadata.file_type().is_symlink()) =>
        {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "control token must not be a symlink",
            ))
        }
        Err(error) => Err(error),
    }
}

#[cfg(not(unix))]
fn open_token_file(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).open(path)
}

#[cfg(unix)]
fn validate_open_token(file: &File, expected_uid: u32) -> io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file()
        || metadata.permissions().mode() & 0o777 != 0o600
        || metadata.uid() != expected_uid
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "control token must be a user-owned regular file with mode 0600",
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_open_token(file: &File, _expected_uid: u32) -> io::Result<()> {
    if !file.metadata()?.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "control token must be a regular file",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn current_user_id() -> u32 {
    unsafe { libc::geteuid() }
}
#[cfg(not(unix))]
fn current_user_id() -> u32 {
    0
}

#[cfg(unix)]
fn ensure_secure_parent(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_dir()
                || metadata.uid() != current_user_id()
                || metadata.permissions().mode() & 0o022 != 0
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "control token directory must be user-owned and not group/other writable",
                ));
            }
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    ensure_secure_parent(path)
                }
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

#[cfg(not(unix))]
fn ensure_secure_parent(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)
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
