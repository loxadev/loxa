use std::time::Instant;

use crate::menu::catalog::TransferStage;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransferReadout {
    headline: String,
    detail: Option<String>,
}

impl TransferReadout {
    fn new(headline: String, detail: Option<String>) -> Self {
        Self { headline, detail }
    }

    pub(crate) fn message(headline: impl Into<String>) -> Self {
        Self::new(headline.into(), None)
    }

    pub(crate) fn headline(&self) -> &str {
        &self.headline
    }

    pub(crate) fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct TransferEstimate {
    baseline: Option<Baseline>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Baseline {
    generation: u64,
    stage: TransferStage,
    rate_transferred_bytes: u64,
    latest_transferred_bytes: u64,
    total_bytes: u64,
    at: Instant,
}

impl TransferEstimate {
    pub(crate) fn reset(&mut self) {
        self.baseline = None;
    }

    pub(crate) fn update(
        &mut self,
        generation: u64,
        stage: TransferStage,
        transferred_bytes: u64,
        total_bytes: u64,
        now: Instant,
    ) -> TransferReadout {
        let reset = self.baseline.is_none_or(|baseline| {
            baseline.generation != generation
                || baseline.stage != stage
                || baseline.total_bytes != total_bytes
                || transferred_bytes < baseline.latest_transferred_bytes
        });
        if reset {
            self.baseline = Some(Baseline {
                generation,
                stage,
                rate_transferred_bytes: transferred_bytes,
                latest_transferred_bytes: transferred_bytes,
                total_bytes,
                at: now,
            });
        } else if let Some(baseline) = self.baseline.as_mut() {
            baseline.latest_transferred_bytes = transferred_bytes;
        }

        match stage {
            TransferStage::Transferring => {
                self.transfer_readout(transferred_bytes, total_bytes, now, reset)
            }
            TransferStage::Verifying => TransferReadout::new("Verifying download…".into(), None),
            TransferStage::Publishing => {
                TransferReadout::new("Finishing installation…".into(), None)
            }
        }
    }

    fn transfer_readout(
        &self,
        transferred_bytes: u64,
        total_bytes: u64,
        now: Instant,
        reset: bool,
    ) -> TransferReadout {
        let headline = if total_bytes == 0 {
            format!("Downloading · {}", format_bytes(transferred_bytes))
        } else {
            let fraction = transferred_bytes.min(total_bytes) as f64 / total_bytes as f64;
            let percent = (fraction * 100.0).round() as u64;
            format!(
                "{percent}% · {} of {}",
                format_bytes(transferred_bytes),
                format_bytes(total_bytes)
            )
        };

        if reset || total_bytes == 0 {
            return TransferReadout::new(headline, None);
        }
        let Some(baseline) = self.baseline else {
            return TransferReadout::new(headline, None);
        };
        let Some(elapsed) = now.checked_duration_since(baseline.at) else {
            return TransferReadout::new(headline, None);
        };
        let delta = transferred_bytes.saturating_sub(baseline.rate_transferred_bytes);
        if elapsed.as_secs_f64() < 1.0 || delta == 0 {
            return TransferReadout::new(headline, None);
        }

        let bytes_per_second = delta as f64 / elapsed.as_secs_f64();
        let remaining_bytes = total_bytes.saturating_sub(transferred_bytes);
        let eta_seconds = (remaining_bytes as f64 / bytes_per_second).ceil();
        let eta_seconds = eta_seconds.min(u64::MAX as f64) as u64;
        TransferReadout::new(
            headline,
            Some(format!(
                "{}/s · {} remaining",
                format_bytes(bytes_per_second.round() as u64),
                format_duration(eta_seconds)
            )),
        )
    }
}

fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1_000.0;
    const MB: f64 = 1_000_000.0;
    const GB: f64 = 1_000_000_000.0;

    if bytes >= 1_000_000_000 {
        format!("{:.1} GB", bytes as f64 / GB)
    } else if bytes >= 1_000_000 {
        format!("{:.1} MB", bytes as f64 / MB)
    } else if bytes >= 1_000 {
        format!("{:.1} KB", bytes as f64 / KB)
    } else {
        format!("{bytes} bytes")
    }
}

fn format_duration(seconds: u64) -> String {
    if seconds < 60 {
        return format!("{seconds}s");
    }
    let minutes = seconds / 60;
    let seconds = seconds % 60;
    if seconds == 0 {
        format!("{minutes}m")
    } else {
        format!("{minutes}m {seconds}s")
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::TransferEstimate;
    use crate::menu::catalog::TransferStage;

    #[test]
    fn observed_delta_drives_decimal_speed_and_ceiling_eta() {
        let start = Instant::now();
        let mut estimate = TransferEstimate::default();

        let first = estimate.update(
            7,
            TransferStage::Transferring,
            15_900_000,
            88_200_000,
            start,
        );
        assert_eq!(first.headline(), "18% · 15.9 MB of 88.2 MB");
        assert_eq!(first.detail(), None);

        let second = estimate.update(
            7,
            TransferStage::Transferring,
            20_900_000,
            88_200_000,
            start + Duration::from_secs(1),
        );
        assert_eq!(second.detail(), Some("5.0 MB/s · 14s remaining"));
    }

    #[test]
    fn resumed_prefix_is_never_counted_as_current_session_speed() {
        let start = Instant::now();
        let mut estimate = TransferEstimate::default();

        let first = estimate.update(
            11,
            TransferStage::Transferring,
            57_000_000,
            88_200_000,
            start,
        );
        assert_eq!(first.detail(), None);

        let second = estimate.update(
            11,
            TransferStage::Transferring,
            62_000_000,
            88_200_000,
            start + Duration::from_secs(1),
        );
        assert_eq!(second.detail(), Some("5.0 MB/s · 6s remaining"));
    }

    #[test]
    fn adjacent_byte_rollback_resets_the_rate_baseline() {
        let start = Instant::now();
        let mut estimate = TransferEstimate::default();

        assert_eq!(
            estimate
                .update(
                    11,
                    TransferStage::Transferring,
                    57_000_000,
                    88_200_000,
                    start,
                )
                .detail(),
            None
        );
        assert_eq!(
            estimate
                .update(
                    11,
                    TransferStage::Transferring,
                    62_000_000,
                    88_200_000,
                    start + Duration::from_secs(1),
                )
                .detail(),
            Some("5.0 MB/s · 6s remaining")
        );

        let rolled_back = estimate.update(
            11,
            TransferStage::Transferring,
            60_000_000,
            88_200_000,
            start + Duration::from_secs(2),
        );
        assert_eq!(rolled_back.headline(), "68% · 60.0 MB of 88.2 MB");
        assert_eq!(rolled_back.detail(), None);
    }

    #[test]
    fn generation_stage_and_total_changes_reset_the_baseline() {
        let start = Instant::now();
        let mut estimate = TransferEstimate::default();

        assert_eq!(
            estimate
                .update(
                    1,
                    TransferStage::Transferring,
                    57_000_000,
                    88_200_000,
                    start,
                )
                .detail(),
            None
        );
        assert_eq!(
            estimate
                .update(
                    1,
                    TransferStage::Transferring,
                    62_000_000,
                    88_200_000,
                    start + Duration::from_secs(1),
                )
                .detail(),
            Some("5.0 MB/s · 6s remaining")
        );

        assert_eq!(
            estimate
                .update(
                    2,
                    TransferStage::Transferring,
                    63_000_000,
                    88_200_000,
                    start + Duration::from_secs(2),
                )
                .detail(),
            None,
            "a new generation must not inherit the prior transfer speed"
        );

        let verifying = estimate.update(
            2,
            TransferStage::Verifying,
            88_200_000,
            88_200_000,
            start + Duration::from_secs(3),
        );
        assert_eq!(verifying.headline(), "Verifying download…");
        assert_eq!(verifying.detail(), None);

        let publishing = estimate.update(
            2,
            TransferStage::Publishing,
            88_200_000,
            88_200_000,
            start + Duration::from_secs(4),
        );
        assert_eq!(publishing.headline(), "Finishing installation…");
        assert_eq!(publishing.detail(), None);

        assert_eq!(
            estimate
                .update(
                    2,
                    TransferStage::Transferring,
                    70_000_000,
                    88_200_000,
                    start + Duration::from_secs(5),
                )
                .detail(),
            None,
            "a stage change must establish a fresh transfer baseline"
        );

        let changed_total = estimate.update(
            2,
            TransferStage::Transferring,
            71_000_000,
            90_000_000,
            start + Duration::from_secs(6),
        );
        assert_eq!(changed_total.headline(), "79% · 71.0 MB of 90.0 MB");
        assert_eq!(changed_total.detail(), None);
    }

    #[test]
    fn zero_total_and_long_eta_have_bounded_truthful_readouts() {
        let start = Instant::now();
        let mut estimate = TransferEstimate::default();

        let unknown_total = estimate.update(3, TransferStage::Transferring, 500_000, 0, start);
        assert_eq!(unknown_total.headline(), "Downloading · 500.0 KB");
        assert_eq!(unknown_total.detail(), None);
        assert_eq!(
            estimate
                .update(
                    3,
                    TransferStage::Transferring,
                    1_500_000,
                    0,
                    start + Duration::from_secs(1),
                )
                .detail(),
            None,
            "an unknown total must never invent an ETA"
        );

        let mut long = TransferEstimate::default();
        long.update(
            4,
            TransferStage::Transferring,
            1_000_000,
            121_000_000,
            start,
        );
        assert_eq!(
            long.update(
                4,
                TransferStage::Transferring,
                2_000_000,
                121_000_000,
                start + Duration::from_secs(1),
            )
            .detail(),
            Some("1.0 MB/s · 1m 59s remaining")
        );
    }
}
