use anstyle::{AnsiColor, Style};

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

pub(crate) fn assistant() -> Style {
    Style::new()
        .fg_color(Some(AnsiColor::Magenta.into()))
        .bold()
}
