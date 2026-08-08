use beam_core::Session;

use crate::{ParsedLarkInboundMessage, clear_agent_attention, lark_locale_or_english};

pub(crate) fn update_session_from_lark_message(
    session: &mut Session,
    parsed: &ParsedLarkInboundMessage,
) {
    session.quote_target_id = Some(parsed.message_id.clone());
    // Update the mention-back target to the sender of the current
    // trigger message.  In multi-user group chats this may differ
    // from owner_open_id (the session creator).
    if let Some(ref sender_id) = parsed.sender_open_id {
        session.quote_target_sender_open_id = Some(sender_id.clone());
    }
    if session.thread_id.is_none()
        && let Some(ref tid) = parsed.thread_id
    {
        session.thread_id = Some(tid.clone());
    }
    if session.locale.is_none() {
        session.locale = Some(lark_locale_or_english(parsed.locale.as_deref()).to_string());
    }
    // Clear agent attention on next user inbound message (botmux parity).
    clear_agent_attention(session);
}
