use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

use super::{error, file, PreferenceError, PreferenceErrorKind, MAX_PREFERENCES_BYTES};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Appearance {
    System,
    Light,
    Dark,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MotionPreference {
    System,
    Reduce,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SendKey {
    Enter,
    CommandEnter,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReadingWidth {
    Compact,
    Comfortable,
    Wide,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Preferences {
    pub(crate) revision: u64,
    pub(crate) appearance: Appearance,
    pub(crate) chat_text_size_px: u8,
    pub(crate) motion: MotionPreference,
    pub(crate) spellcheck: bool,
    pub(crate) send_key: SendKey,
    pub(crate) reading_width: ReadingWidth,
    pub(crate) tail_follow: bool,
}

impl Default for Preferences {
    fn default() -> Self {
        Self {
            revision: 0,
            appearance: Appearance::System,
            chat_text_size_px: 14,
            motion: MotionPreference::System,
            spellcheck: true,
            send_key: SendKey::Enter,
            reading_width: ReadingWidth::Comfortable,
            tail_follow: true,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PreferencesPatch {
    pub(crate) appearance: Option<Appearance>,
    pub(crate) chat_text_size_px: Option<u8>,
    pub(crate) motion: Option<MotionPreference>,
    pub(crate) spellcheck: Option<bool>,
    pub(crate) send_key: Option<SendKey>,
    pub(crate) reading_width: Option<ReadingWidth>,
    pub(crate) tail_follow: Option<bool>,
}

impl PreferencesPatch {
    pub(super) fn is_empty(&self) -> bool {
        self.appearance.is_none()
            && self.chat_text_size_px.is_none()
            && self.motion.is_none()
            && self.spellcheck.is_none()
            && self.send_key.is_none()
            && self.reading_width.is_none()
            && self.tail_follow.is_none()
    }

    pub(super) fn apply(self, preferences: &mut Preferences) {
        if let Some(value) = self.appearance {
            preferences.appearance = value;
        }
        if let Some(value) = self.chat_text_size_px {
            preferences.chat_text_size_px = value;
        }
        if let Some(value) = self.motion {
            preferences.motion = value;
        }
        if let Some(value) = self.spellcheck {
            preferences.spellcheck = value;
        }
        if let Some(value) = self.send_key {
            preferences.send_key = value;
        }
        if let Some(value) = self.reading_width {
            preferences.reading_width = value;
        }
        if let Some(value) = self.tail_follow {
            preferences.tail_follow = value;
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SavedPreferences {
    version: u32,
    revision: u64,
    appearance: Appearance,
    chat_text_size_px: u8,
    motion: MotionPreference,
    spellcheck: bool,
    send_key: SendKey,
    reading_width: ReadingWidth,
    tail_follow: bool,
}

pub(super) fn load(path: &Path) -> Result<Preferences, PreferenceError> {
    let Some(bytes) = file::read(path).map_err(|_| {
        error(
            PreferenceErrorKind::Unavailable,
            "desktop preferences could not be read safely",
        )
    })?
    else {
        return Ok(Preferences::default());
    };
    let saved: SavedPreferences = serde_json::from_slice(&bytes).map_err(|_| {
        error(
            PreferenceErrorKind::InvalidInput,
            "desktop preferences are not a valid versioned document",
        )
    })?;
    if saved.version != 1 || saved.revision == 0 {
        return Err(error(
            PreferenceErrorKind::InvalidInput,
            "desktop preferences use an unsupported version or revision",
        ));
    }
    let preferences = Preferences {
        revision: saved.revision,
        appearance: saved.appearance,
        chat_text_size_px: saved.chat_text_size_px,
        motion: saved.motion,
        spellcheck: saved.spellcheck,
        send_key: saved.send_key,
        reading_width: saved.reading_width,
        tail_follow: saved.tail_follow,
    };
    validate(&preferences)?;
    Ok(preferences)
}

pub(super) fn encode(preferences: &Preferences) -> Result<Arc<[u8]>, PreferenceError> {
    let encoded = serde_json::to_vec(&SavedPreferences {
        version: 1,
        revision: preferences.revision,
        appearance: preferences.appearance,
        chat_text_size_px: preferences.chat_text_size_px,
        motion: preferences.motion,
        spellcheck: preferences.spellcheck,
        send_key: preferences.send_key,
        reading_width: preferences.reading_width,
        tail_follow: preferences.tail_follow,
    })
    .map_err(|_| {
        error(
            PreferenceErrorKind::Unavailable,
            "desktop preferences could not be encoded",
        )
    })?;
    if encoded.len() > MAX_PREFERENCES_BYTES {
        return Err(error(
            PreferenceErrorKind::InvalidInput,
            "encoded desktop preferences exceed the byte limit",
        ));
    }
    Ok(encoded.into())
}

pub(super) fn validate(preferences: &Preferences) -> Result<(), PreferenceError> {
    if !(13..=18).contains(&preferences.chat_text_size_px) {
        return Err(error(
            PreferenceErrorKind::InvalidInput,
            "chat text size must be between 13 and 18 pixels",
        ));
    }
    Ok(())
}
