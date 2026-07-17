//! Portable wire contracts shared by Loxa components.
//!
//! Installation and daemon-incarnation identifiers are intentionally distinct:
//!
//! ```compile_fail
//! use loxa_protocol::{NodeId, NodeInstanceId};
//!
//! fn takes_node_id(_: NodeId) {}
//! let instance_id = NodeInstanceId::new_v4();
//! takes_node_id(instance_id);
//! ```

mod identity;
pub mod v1;
pub mod v2;

pub use identity::{NodeId, NodeInstanceId, ParseIdentityError};
