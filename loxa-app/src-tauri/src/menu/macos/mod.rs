mod catalog_rows;
mod controller;
#[cfg(test)]
mod controller_probe_tests;
mod incomplete_rows;
mod installed_rows;
mod rows;
#[cfg(not(test))]
mod timer;

#[cfg(test)]
pub(crate) use controller::{exit_resources_api, exit_resources_for_test};
pub(crate) use controller::{NativeExitResources, NativePopoverController};
