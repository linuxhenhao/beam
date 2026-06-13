use crate::config::BotConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TalkReason {
    AllowedUser,
    Oncall,
    Peer,
    AllowedChatGroup,
    ChatGrant,
    GlobalGrant,
    Open,
    None,
}

#[derive(Debug, Clone)]
pub struct TalkEvaluation {
    pub allowed: bool,
    pub reason: TalkReason,
    pub quota_key: Option<String>,
}

pub fn evaluate_talk(
    bot: &BotConfig,
    chat_id: &str,
    sender_open_id: &str,
    resolved_allowed_users: &[String],
    peer_bot_open_ids: &[String],
) -> TalkEvaluation {
    let sender_is_allowed = resolved_allowed_users.iter().any(|id| id == sender_open_id);

    if sender_is_allowed {
        return TalkEvaluation {
            allowed: true,
            reason: TalkReason::AllowedUser,
            quota_key: None,
        };
    }

    let oncall_match = bot.oncall_chats.iter().any(|oc| oc.chat_id == chat_id);
    if oncall_match {
        return TalkEvaluation {
            allowed: true,
            reason: TalkReason::Oncall,
            quota_key: None,
        };
    }

    let peer_match = peer_bot_open_ids.iter().any(|id| id == sender_open_id);
    if peer_match {
        return TalkEvaluation {
            allowed: true,
            reason: TalkReason::Peer,
            quota_key: None,
        };
    }

    let chat_group_match = bot.allowed_chat_groups.iter().any(|cg| cg == chat_id);
    if chat_group_match {
        return TalkEvaluation {
            allowed: true,
            reason: TalkReason::AllowedChatGroup,
            quota_key: None,
        };
    }

    if let Some(granted) = bot.chat_grants.get(chat_id) {
        if granted.iter().any(|id| id == sender_open_id) {
            return TalkEvaluation {
                allowed: true,
                reason: TalkReason::ChatGrant,
                quota_key: Some(format!("chat:{}:{}", chat_id, sender_open_id)),
            };
        }
    }

    if bot.global_grants.iter().any(|id| id == sender_open_id) {
        return TalkEvaluation {
            allowed: true,
            reason: TalkReason::GlobalGrant,
            quota_key: Some(format!("global:{}", sender_open_id)),
        };
    }

    if bot.allowed_users.is_empty()
        && bot.allowed_chat_groups.is_empty()
        && bot.chat_grants.is_empty()
        && bot.global_grants.is_empty()
        && bot.oncall_chats.is_empty()
    {
        return TalkEvaluation {
            allowed: true,
            reason: TalkReason::Open,
            quota_key: None,
        };
    }

    TalkEvaluation {
        allowed: false,
        reason: TalkReason::None,
        quota_key: None,
    }
}

pub fn can_operate(
    bot: &BotConfig,
    sender_open_id: &str,
    resolved_allowed_users: &[String],
    peer_bot_open_ids: &[String],
) -> bool {
    let sender_is_allowed = resolved_allowed_users.iter().any(|id| id == sender_open_id);

    if sender_is_allowed {
        return true;
    }

    let peer_match = peer_bot_open_ids.iter().any(|id| id == sender_open_id);
    if peer_match {
        return true;
    }

    let has_allowlist = !bot.allowed_users.is_empty()
        || !bot.allowed_chat_groups.is_empty()
        || !bot.chat_grants.is_empty()
        || !bot.global_grants.is_empty()
        || !bot.oncall_chats.is_empty();

    if !has_allowlist {
        return true;
    }

    false
}

pub fn is_owner(open_id: &str, resolved_allowed_users: &[String]) -> bool {
    resolved_allowed_users
        .first()
        .map(|owner| owner == open_id)
        .unwrap_or(false)
}

pub fn get_owner_open_id(resolved_allowed_users: &[String]) -> Option<String> {
    resolved_allowed_users.first().cloned()
}

pub fn grant_restricted(talk_eval: &TalkEvaluation, restrict_grant_commands: bool) -> bool {
    if !restrict_grant_commands {
        return false;
    }
    matches!(
        talk_eval.reason,
        TalkReason::ChatGrant | TalkReason::GlobalGrant
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BotConfig, OncallChatBinding};
    use std::collections::HashMap;

    fn default_bot() -> BotConfig {
        BotConfig {
            name: None,
            lark_app_id: "app-1".to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
            model: None,
            working_dir: None,
            backend_type: None,
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: vec![],
            private_card: false,
            allowed_chat_groups: vec![],
            chat_grants: HashMap::new(),
            global_grants: vec![],
            oncall_chats: vec![],
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: HashMap::new(),
        }
    }

    #[test]
    fn evaluate_talk_open_when_no_restrictions() {
        let bot = default_bot();
        let eval = evaluate_talk(&bot, "chat-1", "ou_user", &[], &[]);
        assert!(eval.allowed);
        assert_eq!(eval.reason, TalkReason::Open);
    }

    #[test]
    fn evaluate_talk_allowed_user() {
        let bot = BotConfig {
            allowed_users: vec!["ou_owner".to_string()],
            ..default_bot()
        };
        let eval = evaluate_talk(&bot, "chat-1", "ou_owner", &["ou_owner".to_string()], &[]);
        assert!(eval.allowed);
        assert_eq!(eval.reason, TalkReason::AllowedUser);
    }

    #[test]
    fn evaluate_talk_denies_non_allowed_user() {
        let bot = BotConfig {
            allowed_users: vec!["ou_owner".to_string()],
            ..default_bot()
        };
        let eval = evaluate_talk(&bot, "chat-1", "ou_other", &["ou_owner".to_string()], &[]);
        assert!(!eval.allowed);
        assert_eq!(eval.reason, TalkReason::None);
    }

    #[test]
    fn evaluate_talk_allowed_chat_group() {
        let bot = BotConfig {
            allowed_chat_groups: vec!["chat-open".to_string()],
            ..default_bot()
        };
        let eval = evaluate_talk(&bot, "chat-open", "ou_anyone", &[], &[]);
        assert!(eval.allowed);
        assert_eq!(eval.reason, TalkReason::AllowedChatGroup);
    }

    #[test]
    fn evaluate_talk_oncall_chat() {
        let bot = BotConfig {
            oncall_chats: vec![OncallChatBinding {
                chat_id: "oncall-1".to_string(),
                working_dir: None,
            }],
            ..default_bot()
        };
        let eval = evaluate_talk(&bot, "oncall-1", "ou_member", &[], &[]);
        assert!(eval.allowed);
        assert_eq!(eval.reason, TalkReason::Oncall);
    }

    #[test]
    fn evaluate_talk_chat_grant_with_quota_key() {
        let mut chat_grants = HashMap::new();
        chat_grants.insert("chat-1".to_string(), vec!["ou_granted".to_string()]);
        let bot = BotConfig {
            allowed_users: vec!["ou_owner".to_string()],
            chat_grants,
            ..default_bot()
        };
        let eval = evaluate_talk(&bot, "chat-1", "ou_granted", &["ou_owner".to_string()], &[]);
        assert!(eval.allowed);
        assert_eq!(eval.reason, TalkReason::ChatGrant);
        assert_eq!(eval.quota_key, Some("chat:chat-1:ou_granted".to_string()));
    }

    #[test]
    fn evaluate_talk_global_grant() {
        let bot = BotConfig {
            allowed_users: vec!["ou_owner".to_string()],
            global_grants: vec!["ou_global".to_string()],
            ..default_bot()
        };
        let eval = evaluate_talk(
            &bot,
            "any-chat",
            "ou_global",
            &["ou_owner".to_string()],
            &[],
        );
        assert!(eval.allowed);
        assert_eq!(eval.reason, TalkReason::GlobalGrant);
        assert_eq!(eval.quota_key, Some("global:ou_global".to_string()));
    }

    #[test]
    fn can_operate_empty_allowlist() {
        let bot = default_bot();
        assert!(can_operate(&bot, "ou_any", &[], &[]));
    }

    #[test]
    fn can_operate_respects_allowlist() {
        let bot = BotConfig {
            allowed_users: vec!["ou_owner".to_string()],
            ..default_bot()
        };
        assert!(can_operate(
            &bot,
            "ou_owner",
            &["ou_owner".to_string()],
            &[]
        ));
        assert!(!can_operate(
            &bot,
            "ou_other",
            &["ou_owner".to_string()],
            &[]
        ));
    }

    #[test]
    fn can_operate_is_locked_by_talk_only_grants() {
        let bot = BotConfig {
            allowed_chat_groups: vec!["chat-open".to_string()],
            ..default_bot()
        };
        assert!(!can_operate(&bot, "ou_any", &[], &[]));
    }

    #[test]
    fn grant_restricted_blocks_chat_grant_when_enabled() {
        let eval = TalkEvaluation {
            allowed: true,
            reason: TalkReason::ChatGrant,
            quota_key: Some("chat:chat-1:ou_user".to_string()),
        };
        assert!(grant_restricted(&eval, true));
        assert!(!grant_restricted(&eval, false));
    }

    #[test]
    fn grant_restricted_allows_allowed_user_even_when_enabled() {
        let eval = TalkEvaluation {
            allowed: true,
            reason: TalkReason::AllowedUser,
            quota_key: None,
        };
        assert!(!grant_restricted(&eval, true));
    }
}
