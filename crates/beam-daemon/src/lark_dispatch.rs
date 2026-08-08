mod preflight;
mod routing;
mod session;

pub(crate) use preflight::{
    evaluate_lark_preflight, handle_introduce_command, lark_event_dedupe_key,
};
pub(crate) use routing::{
    decide_lark_dispatch, resolve_existing_lark_session, resolve_lark_card_action_session_id,
    validate_resume_target,
};
pub(crate) use session::update_session_from_lark_message;

// Test-only re-exports
#[cfg(test)]
pub(crate) use routing::{decide_lark_event_outcome, session_for_lark_anchor};

#[cfg(test)]
mod tests_dispatch;
#[cfg(test)]
mod tests_dispatch_events;
#[cfg(test)]
mod tests_routing;
#[cfg(test)]
mod tests_session;
