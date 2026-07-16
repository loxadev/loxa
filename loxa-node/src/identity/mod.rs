use loxa_protocol::NodeId;
use std::fmt;
use std::io;
use std::path::Path;

#[cfg(not(unix))]
mod non_unix;
#[cfg(unix)]
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

#[derive(Debug)]
pub(crate) struct IdentityError {
    class: IdentityErrorClass,
    source: Option<io::Error>,
}

impl IdentityError {
    const fn classified(class: IdentityErrorClass) -> Self {
        Self {
            class,
            source: None,
        }
    }

    fn with_source(class: IdentityErrorClass, source: io::Error) -> Self {
        Self {
            class,
            source: Some(source),
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

impl std::error::Error for IdentityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

pub(crate) fn open_or_create(loxa_root: &Path) -> Result<NodeId, IdentityError> {
    platform::open_or_create(loxa_root)
}

#[cfg(not(unix))]
use non_unix as platform;
#[cfg(unix)]
use unix as platform;

#[cfg(test)]
mod tests;
