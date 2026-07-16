use super::{IdentityError, IdentityErrorClass};
use loxa_protocol::NodeId;
use std::path::Path;

pub(super) fn open_or_create(_loxa_root: &Path) -> Result<NodeId, IdentityError> {
    Err(IdentityError::classified(
        IdentityErrorClass::UnsupportedPlatform,
    ))
}
