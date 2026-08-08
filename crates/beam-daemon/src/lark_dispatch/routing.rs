use std::collections::HashMap;

use axum::http::StatusCode;
use beam_core::{Session, SessionScope, SessionStatus};

use crate::{
    CustomTrigger, LarkEventOutcome, LarkTextAction, ParsedLarkCardAction,
    ParsedLarkInboundMessage, build_adopt_already_attached_reply, classify_lark_text_action,
    session_anchor_matches,
};

pub(crate) fn resolve_lark_card_action_session_id(
    sessions: &HashMap<String, Session>,
    lark_app_id: &str,
    action: &ParsedLarkCardAction,
) -> Option<String> {
    if let Some(session_id) = action.session_id.as_ref() {
        return Some(session_id.clone());
    }
    let root_id = action.root_id.as_ref()?;
    sessions
        .values()
        .find(|session| {
            session.lark_app_id == lark_app_id
                && session.status == SessionStatus::Active
                && session.root_message_id == *root_id
        })
        .map(|session| session.session_id.clone())
}

pub(crate) fn decide_lark_event_outcome(
    action: LarkTextAction,
    existing: Option<&Session>,
) -> LarkEventOutcome {
    let has_existing_session = existing.is_some();
    match action {
        LarkTextAction::Close => LarkEventOutcome::CloseSession {
            reply: if has_existing_session {
                "session closed"
            } else {
                "no active session"
            }
            .to_string(),
        },
        LarkTextAction::Restart => LarkEventOutcome::RestartSession {
            reply: if has_existing_session {
                "session restarted"
            } else {
                "no active session"
            }
            .to_string(),
        },
        LarkTextAction::Card => LarkEventOutcome::ShowCard {
            reply: if has_existing_session {
                "session card"
            } else {
                "no active session"
            }
            .to_string(),
        },
        LarkTextAction::AdoptZellij(target) => {
            if let Some(session) = existing.filter(|session| session.adopted_from.is_some()) {
                LarkEventOutcome::ReplyOnly {
                    reply: build_adopt_already_attached_reply(session),
                }
            } else {
                LarkEventOutcome::AdoptZellij { target }
            }
        }
        LarkTextAction::AdoptList => {
            if let Some(session) = existing.filter(|session| session.adopted_from.is_some()) {
                LarkEventOutcome::ReplyOnly {
                    reply: build_adopt_already_attached_reply(session),
                }
            } else {
                LarkEventOutcome::AdoptList
            }
        }
        LarkTextAction::PassthroughInput(text) => {
            if has_existing_session {
                LarkEventOutcome::PassthroughInput { text }
            } else {
                LarkEventOutcome::ReplyOnly {
                    reply: "this command requires an active CLI session".to_string(),
                }
            }
        }
        LarkTextAction::ReuseSessionInput => LarkEventOutcome::ReuseSession,
        LarkTextAction::CreateSession => LarkEventOutcome::CreateSession,
    }
}

pub(crate) fn resolve_existing_lark_session(
    sessions: &HashMap<String, Session>,
    lark_app_id: &str,
    parsed: &ParsedLarkInboundMessage,
) -> Option<Session> {
    sessions
        .values()
        .find(|session| {
            session.scope == parsed.scope
                && session_anchor_matches(session, lark_app_id, &parsed.chat_id, &parsed.anchor)
        })
        .cloned()
}

pub(crate) fn decide_lark_dispatch(
    sessions: &HashMap<String, Session>,
    lark_app_id: &str,
    parsed: &ParsedLarkInboundMessage,
    custom_trigger: Option<&CustomTrigger>,
    trigger_activation: bool,
) -> (Option<Session>, LarkEventOutcome) {
    let existing = resolve_existing_lark_session(sessions, lark_app_id, parsed);
    let mut action = classify_lark_text_action(&parsed.text, existing.is_some());
    if custom_trigger.is_some()
        && trigger_activation
        && existing.is_none()
        && matches!(action, LarkTextAction::PassthroughInput(_))
    {
        // A configured trigger only activates when the message's own anchor
        // has no active session (a regular group is one Chat anchor; each
        // topic is its own Thread anchor). On activation a "/" trigger would
        // otherwise be routed as a passthrough command; treat it as session
        // creation instead. Inside an existing session (or when the trigger
        // is not activating) the message keeps its normal behavior.
        action = LarkTextAction::CreateSession;
    }
    let outcome = decide_lark_event_outcome(action, existing.as_ref());
    (existing, outcome)
}

#[allow(dead_code)]
pub(crate) fn session_for_lark_anchor(
    sessions: &HashMap<String, Session>,
    lark_app_id: &str,
    chat_id: &str,
    root_message_id: &str,
) -> Option<Session> {
    sessions
        .values()
        .find(|session| session_anchor_matches(session, lark_app_id, chat_id, root_message_id))
        .cloned()
}

fn active_anchor_owner(sessions: &HashMap<String, Session>, candidate: &Session) -> Option<String> {
    let anchor = match candidate.scope {
        SessionScope::Thread => candidate.thread_id.as_deref()?,
        SessionScope::Chat => &candidate.chat_id,
    };
    sessions
        .values()
        .find(|session| {
            session.session_id != candidate.session_id
                && session.scope == candidate.scope
                && session_anchor_matches(
                    session,
                    &candidate.lark_app_id,
                    &candidate.chat_id,
                    anchor,
                )
        })
        .map(|session| session.session_id.clone())
}

pub(crate) fn validate_resume_target(
    sessions: &HashMap<String, Session>,
    session_id: &str,
) -> Result<Session, (StatusCode, String)> {
    let session = sessions
        .get(session_id)
        .cloned()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "session not found".to_string()))?;
    if session.status != SessionStatus::Closed {
        return Err((StatusCode::CONFLICT, "session is not closed".to_string()));
    }
    if session.adopted_from.is_some() {
        return Err((
            StatusCode::CONFLICT,
            "adopted sessions cannot be resumed yet".to_string(),
        ));
    }
    if let Some(owner) = active_anchor_owner(sessions, &session) {
        return Err((
            StatusCode::CONFLICT,
            format!(
                "session anchor is already owned by active session {}",
                owner
            ),
        ));
    }
    Ok(session)
}
