use loxa_protocol::NodeId;
use std::fmt;
use std::io;
use std::path::Path;

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
mod non_unix;
#[cfg(any(target_os = "macos", target_os = "linux"))]
mod unix;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IdentityErrorClass {
    UnsupportedPlatform,
    UnsafeRoot,
    UnsafeDirectory,
    UnsafeRecord,
    Corrupt,
    SchemaUnsupported,
    Conflict,
    ConcurrentChange,
    Io,
    Durability,
}

impl IdentityErrorClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedPlatform => "unsupported_platform",
            Self::UnsafeRoot => "unsafe_root",
            Self::UnsafeDirectory => "unsafe_directory",
            Self::UnsafeRecord => "unsafe_record",
            Self::Corrupt => "identity_corrupt",
            Self::SchemaUnsupported => "identity_schema_unsupported",
            Self::Conflict => "identity_conflict",
            Self::ConcurrentChange => "identity_concurrent_change",
            Self::Io => "identity_io",
            Self::Durability => "identity_durability",
        }
    }
}

pub(crate) struct IdentityError {
    class: IdentityErrorClass,
    _source: Option<io::Error>,
}

impl IdentityError {
    fn classified(class: IdentityErrorClass) -> Self {
        Self {
            class,
            _source: None,
        }
    }

    fn with_source(class: IdentityErrorClass, source: io::Error) -> Self {
        Self {
            class,
            _source: Some(source),
        }
    }

    #[cfg(test)]
    const fn class(&self) -> IdentityErrorClass {
        self.class
    }
}

impl fmt::Display for IdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.class.as_str())
    }
}

impl fmt::Debug for IdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "IdentityError({})", self.class.as_str())
    }
}

impl std::error::Error for IdentityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

pub(crate) fn open_or_create(loxa_root: &Path) -> Result<NodeId, IdentityError> {
    platform::open_or_create(loxa_root)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
use non_unix as platform;
#[cfg(any(target_os = "macos", target_os = "linux"))]
use unix as platform;

#[cfg(test)]
mod tests;
