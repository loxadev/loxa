use anstyle::{AnsiColor, Style};
use indicatif::ProgressBar;
use std::time::Duration;

pub(crate) fn accent() -> Style {
    Style::new().fg_color(Some(AnsiColor::Cyan.into())).bold()
}

pub(crate) fn success() -> Style {
    Style::new().fg_color(Some(AnsiColor::Green.into())).bold()
}

pub(crate) fn danger() -> Style {
    Style::new().fg_color(Some(AnsiColor::Red.into())).bold()
}

pub(crate) fn muted() -> Style {
    Style::new().dimmed()
}

pub(crate) fn spinner(message: String) -> ProgressBar {
    let spinner = ProgressBar::new_spinner();
    spinner.set_message(message);
    spinner.enable_steady_tick(Duration::from_millis(80));
    spinner
}
