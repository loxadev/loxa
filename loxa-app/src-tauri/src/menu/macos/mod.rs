mod catalog_rows;
mod controller;
mod installed_rows;
mod rows;
#[cfg(not(test))]
mod timer;

pub(crate) use controller::NativePopoverController;
