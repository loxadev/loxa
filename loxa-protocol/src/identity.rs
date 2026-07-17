use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParseIdentityError;

impl fmt::Display for ParseIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid Loxa identity")
    }
}

impl std::error::Error for ParseIdentityError {}

pub(crate) fn parse_uuid_v4(text: &str) -> Result<Uuid, ParseIdentityError> {
    if text.len() != 36 || !matches!(text.as_bytes()[19], b'8' | b'9' | b'a' | b'b') {
        return Err(ParseIdentityError);
    }
    let value = Uuid::parse_str(text).map_err(|_| ParseIdentityError)?;
    if value.is_nil() || value.get_version_num() != 4 || value.hyphenated().to_string() != text {
        return Err(ParseIdentityError);
    }
    Ok(value)
}

macro_rules! identity_type {
    ($name:ident, $expecting:literal) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Uuid);

        impl $name {
            #[must_use]
            pub fn new_v4() -> Self {
                Self(Uuid::new_v4())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{}", self.0.hyphenated())
            }
        }

        impl FromStr for $name {
            type Err = ParseIdentityError;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                parse_uuid_v4(text).map(Self)
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                struct IdentityVisitor;

                impl Visitor<'_> for IdentityVisitor {
                    type Value = $name;

                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        formatter.write_str($expecting)
                    }

                    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                        value.parse().map_err(E::custom)
                    }
                }

                deserializer.deserialize_str(IdentityVisitor)
            }
        }
    };
}

identity_type!(NodeId, "a canonical lowercase non-nil UUIDv4 node ID");
identity_type!(
    NodeInstanceId,
    "a canonical lowercase non-nil UUIDv4 node instance ID"
);
