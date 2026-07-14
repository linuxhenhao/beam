pub(crate) mod attachments;
/// Items re-exported from submodules for crate-wide compatibility.
/// lib.rs does `pub(crate) use final_output::*;` so every `pub(crate)` item
/// in the submodules must be lifted into final_output's namespace via these
/// re-exports.
pub(crate) mod attention;
pub(crate) mod delivery;
pub(crate) mod pending;
pub(crate) mod retry;

// Some re-exports appear unused in lib builds but are consumed by test builds
// and by `lib.rs`'s glob re-export of `final_output::*`.
#[allow(unused_imports)]
pub(crate) use attachments::*;
#[allow(unused_imports)]
pub(crate) use attention::*;
#[allow(unused_imports)]
pub(crate) use delivery::*;
#[allow(unused_imports)]
pub(crate) use pending::*;
#[allow(unused_imports)]
pub(crate) use retry::*;

/// Bring crate-level items into final_output's namespace so that the test
/// module (which does `use super::*;`) can access helpers like
/// `resolve_tui_prompt_final_text`, `Session`, etc.
#[cfg(test)]
#[allow(unused_imports)]
use crate::*;

#[cfg(test)]
mod tests_attachments_retry;
#[cfg(test)]
mod tests_delivery;
#[cfg(test)]
mod tests_support_attention;
