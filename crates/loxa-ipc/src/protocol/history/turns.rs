use super::{
    bounded_decimal, positive_bounded_decimal, validate_hex_id, MAX_ATTEMPT_CONTENT_BYTES,
};
use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

pub const MAX_TURN_PAGE_ITEMS: usize = 50;
pub const MAX_TURN_PAGE_BYTES: usize = 24 * 1024;
pub const MAX_CONTENT_RANGE_BYTES: usize = 24 * 1024;
const MAX_USER_TEXT_BYTES: u64 = 32 * 1024;
const MAX_STORED_INTEGER: u64 = i64::MAX as u64;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TurnCursor {
    pub ordinal: String,
}

impl TurnCursor {
    pub(super) fn validate_shape(&self) -> Result<(), &'static str> {
        positive_bounded_decimal(&self.ordinal, MAX_STORED_INTEGER, "invalid turn cursor")
            .map(|_| ())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptExecution {
    Pending,
    Completed,
    Stopped,
    Failed,
    Interrupted,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptSave {
    Open,
    Saved,
    Failed,
    Interrupted,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStopReason {
    Completed,
    OutputLimit,
    UserStop,
    Failure,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptStatistics {
    pub qualified_input_tokens: Option<u32>,
    pub qualified_output_tokens: Option<u32>,
    pub service_first_output_latency_ms: Option<String>,
    pub qualified_engine_decode_tokens_per_second: Option<EngineDecodeRate>,
    pub service_total_duration_ms: String,
    pub stop_reason: AttemptStopReason,
}

impl AttemptStatistics {
    fn validate_shape(&self) -> Result<(), &'static str> {
        if self
            .qualified_engine_decode_tokens_per_second
            .is_some_and(|_| {
                self.qualified_output_tokens
                    .is_none_or(|tokens| tokens == 0)
            })
        {
            return Err("invalid attempt statistics");
        }
        let total = bounded_decimal(
            &self.service_total_duration_ms,
            MAX_STORED_INTEGER,
            "invalid service total duration",
        )?;
        let first_output = self
            .service_first_output_latency_ms
            .as_ref()
            .map(|value| {
                bounded_decimal(
                    value,
                    MAX_STORED_INTEGER,
                    "invalid service first output latency",
                )
            })
            .transpose()?;
        if first_output.is_some_and(|value| value > total) {
            return Err("invalid attempt statistics");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct EngineDecodeRate(f64);

impl EngineDecodeRate {
    pub fn new(value: f64) -> Option<Self> {
        (value.is_finite() && value > 0.0 && value < 1.0e308).then_some(Self(value))
    }

    pub fn get(self) -> f64 {
        self.0
    }
}

impl PartialEq for EngineDecodeRate {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for EngineDecodeRate {}

impl Serialize for EngineDecodeRate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f64(self.0)
    }
}

impl<'de> Deserialize<'de> for EngineDecodeRate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = f64::deserialize(deserializer)?;
        Self::new(value).ok_or_else(|| serde::de::Error::custom("invalid engine decode rate"))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptSummary {
    pub id: String,
    pub attempt_number: String,
    pub execution: AttemptExecution,
    pub save: AttemptSave,
    pub saved_end: String,
    pub generated_end: Option<String>,
    pub terminal_saved_end: Option<String>,
    pub failure_code: Option<String>,
    pub statistics: Option<AttemptStatistics>,
    pub created_ms: String,
    pub updated_ms: String,
}

impl AttemptSummary {
    pub(super) fn validate_shape(&self) -> Result<(), &'static str> {
        validate_hex_id(&self.id)?;
        positive_bounded_decimal(
            &self.attempt_number,
            MAX_STORED_INTEGER,
            "invalid attempt number",
        )?;
        let saved_end = bounded_decimal(
            &self.saved_end,
            MAX_ATTEMPT_CONTENT_BYTES,
            "invalid saved content end",
        )?;
        let generated_end = self
            .generated_end
            .as_ref()
            .map(|end| {
                bounded_decimal(
                    end,
                    MAX_ATTEMPT_CONTENT_BYTES,
                    "invalid generated content end",
                )
            })
            .transpose()?;
        let terminal_saved_end = self
            .terminal_saved_end
            .as_ref()
            .map(|end| {
                bounded_decimal(
                    end,
                    MAX_ATTEMPT_CONTENT_BYTES,
                    "invalid terminal saved content end",
                )
            })
            .transpose()?;
        if generated_end.is_some_and(|end| saved_end > end) {
            return Err("saved content end exceeds generated content end");
        }
        match self.save {
            AttemptSave::Saved
                if generated_end != Some(saved_end) || terminal_saved_end != Some(saved_end) =>
            {
                return Err("saved attempt has inconsistent terminal content ends");
            }
            AttemptSave::Open | AttemptSave::Failed | AttemptSave::Interrupted
                if terminal_saved_end.is_some() =>
            {
                return Err("non-saved attempt has a terminal saved content end");
            }
            _ => {}
        }
        if self.save == AttemptSave::Saved && self.execution == AttemptExecution::Pending {
            return Err("pending attempt cannot be fully saved");
        }
        if self.failure_code.as_ref().is_some_and(|value| {
            value.is_empty() || value.len() > 64 || value.capacity() > 64 || !value.is_ascii()
        }) {
            return Err("invalid attempt failure code");
        }
        if let Some(statistics) = &self.statistics {
            if self.save != AttemptSave::Saved {
                return Err("attempt statistics require a saved terminal outcome");
            }
            let matching_outcome = matches!(
                (self.execution, statistics.stop_reason),
                (
                    AttemptExecution::Completed,
                    AttemptStopReason::Completed | AttemptStopReason::OutputLimit
                ) | (AttemptExecution::Stopped, AttemptStopReason::UserStop)
                    | (AttemptExecution::Failed, AttemptStopReason::Failure)
            );
            if !matching_outcome {
                return Err("attempt statistics do not match the execution outcome");
            }
            statistics.validate_shape()?;
        }
        let created = bounded_decimal(
            &self.created_ms,
            MAX_STORED_INTEGER,
            "invalid attempt creation time",
        )?;
        let updated = bounded_decimal(
            &self.updated_ms,
            MAX_STORED_INTEGER,
            "invalid attempt update time",
        )?;
        if updated < created {
            return Err("attempt update time precedes creation time");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TurnSummary {
    pub id: String,
    pub ordinal: String,
    pub user_text_end: String,
    pub selected_attempt: Option<AttemptSummary>,
}

impl TurnSummary {
    pub(super) fn validate_shape(&self) -> Result<(), &'static str> {
        validate_hex_id(&self.id)?;
        positive_bounded_decimal(&self.ordinal, MAX_STORED_INTEGER, "invalid turn ordinal")?;
        positive_bounded_decimal(
            &self.user_text_end,
            MAX_USER_TEXT_BYTES,
            "invalid user content end",
        )?;
        if let Some(attempt) = &self.selected_attempt {
            attempt.validate_shape()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TurnPage {
    #[serde(deserialize_with = "deserialize_turns")]
    pub turns: Vec<TurnSummary>,
    pub next: Option<TurnCursor>,
}

impl TurnPage {
    pub(super) fn validate_shape(&self) -> Result<(), &'static str> {
        if self.turns.len() > MAX_TURN_PAGE_ITEMS {
            return Err("too many turns in a page");
        }
        for turn in &self.turns {
            turn.validate_shape()?;
        }
        if turn_backing_bytes(&self.turns, self.turns.capacity(), self.next.as_ref())
            > MAX_TURN_PAGE_BYTES
        {
            return Err("turn page exceeds the decoded backing limit");
        }
        if let Some(next) = &self.next {
            next.validate_shape()?;
            if self.turns.is_empty() {
                return Err("empty turn page has a cursor");
            }
        }
        let encoded = serde_json::to_vec(self).map_err(|_| "turn page is invalid")?;
        if encoded.len() > MAX_TURN_PAGE_BYTES {
            return Err("turn page exceeds the byte limit");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentSource {
    User { turn_id: String },
    Assistant { attempt_id: String },
}

impl ContentSource {
    pub(super) fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::User { turn_id } => validate_hex_id(turn_id),
            Self::Assistant { attempt_id } => validate_hex_id(attempt_id),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContentRange {
    pub start: String,
    pub end: String,
    pub prefix_end: String,
    pub content: String,
}

impl ContentRange {
    pub(super) fn validate_shape(&self) -> Result<(), &'static str> {
        let start = bounded_decimal(
            &self.start,
            MAX_ATTEMPT_CONTENT_BYTES,
            "invalid content range start",
        )?;
        let end = bounded_decimal(
            &self.end,
            MAX_ATTEMPT_CONTENT_BYTES,
            "invalid content range end",
        )?;
        let prefix_end = bounded_decimal(
            &self.prefix_end,
            MAX_ATTEMPT_CONTENT_BYTES,
            "invalid content range prefix end",
        )?;
        if self.content.len() > MAX_CONTENT_RANGE_BYTES
            || self.content.capacity() > MAX_CONTENT_RANGE_BYTES
        {
            return Err("content range exceeds the byte limit");
        }
        if start > end || end > prefix_end || end - start != self.content.len() as u64 {
            return Err("content range offsets do not match its content");
        }
        Ok(())
    }
}

fn deserialize_turns<'de, D>(deserializer: D) -> Result<Vec<TurnSummary>, D::Error>
where
    D: Deserializer<'de>,
{
    struct TurnsVisitor;

    impl<'de> Visitor<'de> for TurnsVisitor {
        type Value = Vec<TurnSummary>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("at most 50 turn summaries")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            if sequence
                .size_hint()
                .is_some_and(|size| size > MAX_TURN_PAGE_ITEMS)
            {
                return Err(serde::de::Error::custom("too many turns in a page"));
            }
            let mut values =
                Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAX_TURN_PAGE_ITEMS));
            while let Some(value) = sequence.next_element()? {
                if values.len() == MAX_TURN_PAGE_ITEMS {
                    return Err(serde::de::Error::custom("too many turns in a page"));
                }
                values.push(value);
            }
            values.shrink_to_fit();
            if turn_backing_bytes(&values, values.capacity(), None) > MAX_TURN_PAGE_BYTES {
                return Err(serde::de::Error::custom(
                    "turn page exceeds the decoded backing limit",
                ));
            }
            Ok(values)
        }
    }

    deserializer.deserialize_seq(TurnsVisitor)
}

fn turn_backing_bytes(items: &[TurnSummary], capacity: usize, next: Option<&TurnCursor>) -> usize {
    let structs = capacity.saturating_mul(std::mem::size_of::<TurnSummary>());
    let items = items.iter().fold(structs, |total, item| {
        let total = total
            .saturating_add(item.id.capacity())
            .saturating_add(item.ordinal.capacity())
            .saturating_add(item.user_text_end.capacity());
        item.selected_attempt.as_ref().map_or(total, |attempt| {
            total
                .saturating_add(attempt.id.capacity())
                .saturating_add(attempt.attempt_number.capacity())
                .saturating_add(attempt.saved_end.capacity())
                .saturating_add(attempt.generated_end.as_ref().map_or(0, String::capacity))
                .saturating_add(
                    attempt
                        .terminal_saved_end
                        .as_ref()
                        .map_or(0, String::capacity),
                )
                .saturating_add(attempt.failure_code.as_ref().map_or(0, String::capacity))
                .saturating_add(attempt.created_ms.capacity())
                .saturating_add(attempt.updated_ms.capacity())
        })
    });
    next.map_or(items, |cursor| {
        items.saturating_add(cursor.ordinal.capacity())
    })
}
