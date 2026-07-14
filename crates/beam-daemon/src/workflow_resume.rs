//! Workflow resume and cold-attach logic.
//!
//! Extracted from `lib.rs` (Task 9.1) to separate the resume/recovery/dangling-effects
//! machinery from route handlers and app wiring.
//!
//! This module handles:
//! - Feishu IM dangling effect reconciliation (idempotent re-submission)
//! - Attempt resume infrastructure (cold-attach worker lifecycle)
//! - Wait/cancel/worker-crashed recovery helpers
//! - Resume response building
//!
//! ## Internal layout
//! - [`request`]: request/input parsing helpers.
//! - [`response`]: resume response JSON builders.
//! - [`recovery`]: reconciliation and recovery-invocation logic.

mod recovery;
mod request;
mod response;

pub(crate) use recovery::*;
pub(crate) use request::*;
pub(crate) use response::*;
