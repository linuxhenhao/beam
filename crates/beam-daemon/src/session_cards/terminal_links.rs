use std::collections::HashSet;
use std::net::Ipv4Addr;

use url::Url;

use super::actions::card_text;
use crate::{BeamPaths, Session, card_i18n, terminal_auth, terminal_base_url, zellij_web};

/// Returns true if the given host string is a Tailscale IPv4 address (100.64.0.0/10).
fn is_tailscale_ipv4_host(host: &str) -> bool {
    let Ok(ip) = host.parse::<Ipv4Addr>() else {
        return false;
    };
    let [a, b, _, _] = ip.octets();
    a == 100 && (64..=127).contains(&b)
}

/// Returns true if the given host string is an RFC 1918 private LAN IPv4 address.
fn is_rfc1918_lan_ipv4_host(host: &str) -> bool {
    let Ok(ip) = host.parse::<Ipv4Addr>() else {
        return false;
    };
    let [a, b, _, _] = ip.octets();
    matches!((a, b), (10, _) | (172, 16..=31) | (192, 168))
}

/// Returns Chinese and English labels for a terminal link candidate host,
/// based on whether it is the first (recommended) candidate, Tailscale, LAN, or generic.
pub(crate) fn terminal_link_candidate_labels(host: &str, is_first: bool) -> (String, String) {
    if host == "localhost" {
        return ("本机地址".to_string(), "Localhost".to_string());
    }
    if is_first {
        return (
            format!("推荐地址 {}", host),
            format!("Recommended address {}", host),
        );
    }
    if is_tailscale_ipv4_host(host) {
        return (
            format!("Tailscale 地址 {}", host),
            format!("Tailscale address {}", host),
        );
    }
    if is_rfc1918_lan_ipv4_host(host) {
        return (
            format!("局域网地址 {}", host),
            format!("LAN address {}", host),
        );
    }
    (
        format!("候选地址 {}", host),
        format!("Candidate address {}", host),
    )
}

/// A terminal link candidate with bilingual labels and a URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TerminalLinkCandidate {
    pub(crate) label_zh: String,
    pub(crate) label_en: String,
    pub(crate) url: String,
}

impl TerminalLinkCandidate {
    pub(crate) fn new(
        label_zh: impl Into<String>,
        label_en: impl Into<String>,
        url: impl Into<String>,
    ) -> Self {
        Self {
            label_zh: label_zh.into(),
            label_en: label_en.into(),
            url: url.into(),
        }
    }
}

/// Normalizes a terminal base URL by stripping query and fragment.
fn normalize_terminal_base_url(url: &str) -> Option<String> {
    let mut parsed = Url::parse(url.trim()).ok()?;
    parsed.set_query(None);
    parsed.set_fragment(None);
    Some(parsed.to_string())
}

/// Builds the full list of terminal link candidates for a session, including
/// network-discovered hosts and the current terminal URL if not already present.
pub(crate) fn terminal_link_choice_candidates(
    session: &Session,
    permission: terminal_auth::TerminalPermission,
    candidate_hosts: &[String],
    proxy_base_port: u16,
) -> Vec<TerminalLinkCandidate> {
    let candidate_url =
        |base_url: &str| build_terminal_url_with_ticket(base_url, &session.session_id, permission);
    let mut candidates = Vec::new();
    let mut candidate_base_urls = HashSet::new();

    for (idx, host) in candidate_hosts.iter().enumerate() {
        let base_url = terminal_base_url(host, proxy_base_port, &session.session_id);
        if !candidate_base_urls.insert(base_url.clone()) {
            continue;
        }
        let (label_zh, label_en) = terminal_link_candidate_labels(host, idx == 0);
        candidates.push(TerminalLinkCandidate::new(
            label_zh,
            label_en,
            candidate_url(&base_url),
        ));
    }

    if let Some(current_base) = session
        .terminal_url
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .and_then(normalize_terminal_base_url)
    {
        if !candidate_base_urls.contains(&current_base) {
            candidates.push(TerminalLinkCandidate::new(
                "当前地址",
                "Current address",
                candidate_url(&current_base),
            ));
        }
    }

    candidates
}

/// Builds a terminal link choice card JSON string (Lark card format).
pub(crate) fn build_terminal_link_choice_card(
    session: &Session,
    header_zh: &str,
    header_en: &str,
    body_zh: &str,
    body_en: &str,
    candidates: &[TerminalLinkCandidate],
) -> String {
    let locale = session.locale.as_deref();
    let title = if session.title.trim().is_empty() {
        session
            .cli_id
            .clone()
            .unwrap_or_else(|| session.session_id.clone())
    } else {
        session.title.clone()
    };
    let display_title = title.clone();
    let actions: Vec<serde_json::Value> = candidates
        .iter()
        .enumerate()
        .map(|(idx, candidate)| {
            let text = card_i18n::plain_text(locale, &candidate.label_zh, &candidate.label_en);
            serde_json::json!({
                "tag": "button",
                "text": text,
                "type": if idx == 0 { "primary" } else { "default" },
                "multi_url": {
                    "url": candidate.url,
                    "pc_url": candidate.url,
                    "android_url": candidate.url,
                    "ios_url": candidate.url,
                },
            })
        })
        .collect();
    serde_json::json!({
        "config": { "wide_screen_mode": true },
        "header": {
            "title": card_i18n::plain_text(locale, header_zh, header_en),
            "template": "blue"
        },
        "elements": [
            {
                "tag": "markdown",
                "content": card_text(locale, body_zh, body_en),
                "i18n_content": {
                    "zh_cn": body_zh,
                    "en_us": body_en,
                },
            },
            {
                "tag": "markdown",
                "content": card_text(
                    locale,
                    &format!("**会话** `{}`", display_title),
                    &format!("**Session** `{}`", display_title),
                ),
                "i18n_content": {
                    "zh_cn": format!("**会话** `{}`", display_title),
                    "en_us": format!("**Session** `{}`", display_title),
                },
            },
            { "tag": "action", "actions": actions }
        ]
    })
    .to_string()
}

/// Builds a writable terminal session card JSON with a single write URL.
#[allow(dead_code)]
pub(crate) fn build_writable_session_card(session: &Session, write_url: &str) -> String {
    build_terminal_link_choice_card(
        session,
        "选择可写终端入口",
        "Choose writable terminal entry",
        "如果某个入口打不开，请返回后选择其他入口。",
        "If one entry does not open, go back and choose another.",
        &[TerminalLinkCandidate::new(
            "可写终端",
            "Writable terminal",
            write_url,
        )],
    )
}

/// Builds a read-only terminal link card JSON with a single read-only URL.
#[allow(dead_code)]
pub(crate) fn build_readonly_link_card(session: &Session, ro_url: &str, _ro_token: &str) -> String {
    build_terminal_link_choice_card(
        session,
        "选择只读终端入口",
        "Choose read-only terminal entry",
        "如果某个入口打不开，请返回后选择其他入口。",
        "If one entry does not open, go back and choose another.",
        &[TerminalLinkCandidate::new(
            "只读终端",
            "Read-only terminal",
            ro_url,
        )],
    )
}

/// Load zellij web tokens from the standard paths location (for card rendering).
/// Returns None if the file doesn't exist or can't be parsed.
pub(crate) fn load_zellij_web_tokens_for_card() -> Option<zellij_web::ZellijWebTokens> {
    let paths = BeamPaths::discover().ok()?;
    zellij_web::load_zellij_web_tokens(&paths.zellij_web_tokens_json())
        .ok()
        .flatten()
}

/// Build a terminal URL with a Beam ticket attached, falling back to raw token
/// if ticket generation is not available (e.g., zellij tokens not loaded).
pub(crate) fn build_terminal_url_with_ticket(
    base_url: &str,
    session_id: &str,
    permission: terminal_auth::TerminalPermission,
) -> String {
    let ticket = terminal_auth::generate_terminal_ticket(session_id, permission);
    let sep = if base_url.contains('?') { "&" } else { "?" };
    format!(
        "{}{sep}{}={}",
        base_url,
        terminal_auth::TICKET_QUERY_PARAM,
        ticket
    )
}
