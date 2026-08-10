#![deny(unsafe_op_in_unsafe_fn)]

mod app;
mod menu;
mod native_menu;

fn main() {
    app::run();
}
