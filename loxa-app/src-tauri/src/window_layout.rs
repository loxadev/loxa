const DISPLAY_USAGE_RATIO: f64 = 0.8;
const USABLE_MIN_WIDTH: f64 = 800.0;
const USABLE_MIN_HEIGHT: f64 = 600.0;

#[derive(Debug, PartialEq)]
pub(crate) struct WindowLayout {
    pub(crate) width: f64,
    pub(crate) height: f64,
    pub(crate) min_width: f64,
    pub(crate) min_height: f64,
}

pub(crate) fn calculate_window_layout(
    work_width: u32,
    work_height: u32,
    scale_factor: f64,
) -> Option<WindowLayout> {
    if work_width == 0 || work_height == 0 || !scale_factor.is_normal() || scale_factor <= 0.0 {
        return None;
    }

    let available_width = f64::from(work_width) / scale_factor;
    let available_height = f64::from(work_height) / scale_factor;
    let min_width = USABLE_MIN_WIDTH.min(available_width);
    let min_height = USABLE_MIN_HEIGHT.min(available_height);

    Some(WindowLayout {
        width: (available_width * DISPLAY_USAGE_RATIO)
            .round()
            .clamp(min_width, available_width),
        height: (available_height * DISPLAY_USAGE_RATIO)
            .round()
            .clamp(min_height, available_height),
        min_width,
        min_height,
    })
}

#[cfg(test)]
mod tests {
    use super::calculate_window_layout;

    #[test]
    fn sizes_the_window_from_the_monitor_work_area_without_a_fixed_maximum() {
        let laptop = calculate_window_layout(1512, 945, 1.0).expect("valid laptop layout");
        assert_eq!((laptop.width, laptop.height), (1210.0, 756.0));
        assert_eq!((laptop.min_width, laptop.min_height), (800.0, 600.0));

        let large_display =
            calculate_window_layout(3840, 2160, 2.0).expect("valid large-display layout");
        assert_eq!((large_display.width, large_display.height), (1536.0, 864.0));
        assert!(large_display.width > laptop.width);

        let fractional_dpi =
            calculate_window_layout(1920, 1200, 1.5).expect("valid fractional-DPI layout");
        assert_eq!(
            (fractional_dpi.width, fractional_dpi.height),
            (1024.0, 640.0)
        );
    }

    #[test]
    fn never_requests_a_window_larger_than_the_available_work_area() {
        let compact = calculate_window_layout(640, 480, 1.0).expect("valid compact layout");
        assert_eq!((compact.width, compact.height), (640.0, 480.0));
        assert_eq!((compact.min_width, compact.min_height), (640.0, 480.0));
    }

    #[test]
    fn rejects_invalid_monitor_metrics() {
        assert!(calculate_window_layout(0, 945, 1.0).is_none());
        assert!(calculate_window_layout(1512, 0, 1.0).is_none());
        assert!(calculate_window_layout(1512, 945, 0.0).is_none());
        assert!(calculate_window_layout(1512, 945, f64::NAN).is_none());
        assert!(calculate_window_layout(1512, 945, f64::MIN_POSITIVE / 2.0).is_none());
    }
}
