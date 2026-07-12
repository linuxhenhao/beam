use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr};
use std::process::{Command, Stdio};
use std::str::FromStr;
use std::time::{Duration, Instant};

use url::Url;

use super::*;

fn is_unspecified_web_host(host: &str) -> bool {
    matches!(host.trim(), "" | "0.0.0.0" | "::" | "[::]")
}

fn is_usable_lan_ipv4(ip: Ipv4Addr) -> bool {
    !ip.is_loopback()
        && !ip.is_unspecified()
        && !ip.is_multicast()
        && !ip.is_link_local()
        && !is_cgnat_ipv4(ip)
}

fn is_usable_tailscale_ipv4(ip: Ipv4Addr) -> bool {
    !ip.is_loopback() && !ip.is_unspecified() && !ip.is_multicast() && !ip.is_link_local()
}

fn is_cgnat_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, _, _] = ip.octets();
    a == 100 && (64..=127).contains(&b)
}

fn strip_ipv6_brackets(host: &str) -> &str {
    let trimmed = host.trim();
    if trimmed.starts_with('[') && trimmed.ends_with(']') && trimmed.len() > 2 {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    }
}

pub(crate) fn host_for_url(host: &str) -> String {
    let trimmed = strip_ipv6_brackets(host);
    if trimmed.contains(':') && !trimmed.starts_with('[') && !trimmed.ends_with(']') {
        format!("[{}]", trimmed)
    } else {
        trimmed.to_string()
    }
}

fn host_from_env() -> Option<String> {
    std::env::var("BEAM_WEB_EXTERNAL_HOST")
        .ok()
        .map(|value| strip_ipv6_brackets(&value).to_string())
        .filter(|value| !value.is_empty())
}

fn host_from_bind(bind_host: &str) -> Option<String> {
    let host = strip_ipv6_brackets(bind_host);
    if is_unspecified_web_host(host) {
        None
    } else {
        Some(host.to_string())
    }
}

fn first_valid_ipv4_from_text(text: &str) -> Option<Ipv4Addr> {
    text.split_whitespace().find_map(|part| {
        Ipv4Addr::from_str(part)
            .ok()
            .and_then(|ip| is_usable_tailscale_ipv4(ip).then_some(ip))
    })
}

fn tailscale_ipv4() -> Option<Ipv4Addr> {
    let mut child = Command::new("tailscale")
        .args(["ip", "-4"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .ok()?;
    let deadline = Instant::now() + Duration::from_millis(500);
    loop {
        match child.try_wait().ok()? {
            Some(_) => break,
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            None => std::thread::sleep(Duration::from_millis(20)),
        }
    }
    let output = child.wait_with_output().ok()?;
    if !output.status.success() {
        return None;
    }
    first_valid_ipv4_from_text(&String::from_utf8_lossy(&output.stdout))
}

fn lan_ipv4_candidates() -> Vec<Ipv4Addr> {
    let Ok(interfaces) = netwatcher::list_interfaces() else {
        return Vec::new();
    };
    let mut ips = Vec::new();
    for interface in interfaces.values() {
        for addr in &interface.ips {
            if let IpAddr::V4(ipv4) = addr.ip {
                if is_usable_lan_ipv4(ipv4) {
                    ips.push(ipv4);
                }
            }
        }
    }
    ips.sort_unstable();
    ips.dedup();
    ips
}

pub(crate) fn external_host_candidates_from_sources(
    env_host: Option<&str>,
    bind_host: Option<&str>,
    tailscale_ip: Option<Ipv4Addr>,
    lan_ips: &[Ipv4Addr],
) -> Vec<String> {
    fn push_candidate(
        candidates: &mut Vec<String>,
        seen: &mut HashSet<String>,
        host: String,
    ) -> bool {
        if seen.insert(host.clone()) {
            candidates.push(host);
            true
        } else {
            false
        }
    }

    let mut candidates = Vec::new();
    let mut seen = HashSet::new();
    let mut has_any_candidate = false;

    if let Some(host) = env_host
        .map(strip_ipv6_brackets)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter(|value| !is_unspecified_web_host(value))
    {
        has_any_candidate |= push_candidate(&mut candidates, &mut seen, host.to_string());
    }
    if let Some(host) = bind_host
        .map(strip_ipv6_brackets)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter(|value| !is_unspecified_web_host(value))
    {
        has_any_candidate |= push_candidate(&mut candidates, &mut seen, host.to_string());
    }
    if let Some(ip) = tailscale_ip.filter(|ip| is_usable_tailscale_ipv4(*ip)) {
        has_any_candidate |= push_candidate(&mut candidates, &mut seen, ip.to_string());
    }
    let mut lan_ips = lan_ips.to_vec();
    lan_ips.sort_unstable();
    lan_ips.dedup();
    for ip in lan_ips {
        if is_usable_lan_ipv4(ip) {
            has_any_candidate |= push_candidate(&mut candidates, &mut seen, ip.to_string());
        }
    }
    if !has_any_candidate {
        let _ = push_candidate(&mut candidates, &mut seen, "localhost".to_string());
    }
    candidates
}

pub(crate) fn external_host_candidates(bind_host: &str) -> Vec<String> {
    let env_host = host_from_env();
    let bind_host = host_from_bind(bind_host);
    let tailscale_ip = tailscale_ipv4();
    let lan_ips = lan_ipv4_candidates();
    external_host_candidates_from_sources(
        env_host.as_deref(),
        bind_host.as_deref(),
        tailscale_ip,
        &lan_ips,
    )
}

pub(crate) fn resolve_external_host(bind_host: &str) -> String {
    external_host_candidates(bind_host)
        .into_iter()
        .next()
        .unwrap_or_else(|| "localhost".to_string())
}

#[allow(dead_code)]
pub(crate) fn resolve_external_host_from_sources(
    env_host: Option<&str>,
    bind_host: Option<&str>,
    tailscale_ip: Option<Ipv4Addr>,
    lan_ip: Option<Ipv4Addr>,
) -> String {
    let lan_ips = lan_ip.into_iter().collect::<Vec<_>>();
    external_host_candidates_from_sources(env_host, bind_host, tailscale_ip, &lan_ips)
        .into_iter()
        .next()
        .unwrap_or_else(|| "localhost".to_string())
}

pub(crate) fn terminal_base_url(host: &str, port: u16, session_id: &str) -> String {
    format!("http://{}:{}/s/{}", host_for_url(host), port, session_id)
}

pub(crate) fn rewrite_terminal_url(url: &str, host: &str, port: u16) -> Option<String> {
    let mut parsed = Url::parse(url).ok()?;
    parsed.set_scheme("http").ok()?;
    parsed.set_host(Some(host)).ok()?;
    parsed.set_port(Some(port)).ok()?;
    Some(parsed.to_string())
}

pub(crate) fn rewrite_session_terminal_urls(
    sessions: &mut HashMap<String, Session>,
    host: &str,
    port: u16,
) -> usize {
    let mut updated = 0;
    for session in sessions.values_mut() {
        let Some(current) = session.terminal_url.as_ref() else {
            continue;
        };
        let Some(next) = rewrite_terminal_url(current, host, port) else {
            continue;
        };
        if next != *current {
            session.terminal_url = Some(next);
            updated += 1;
        }
    }
    updated
}

pub(crate) async fn current_external_host(state: &AppState) -> String {
    state.external_host.read().await.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::make_session;

    #[test]
    fn resolver_prefers_env_then_bind_then_tailscale_then_lan_then_localhost() {
        let lan = Some(Ipv4Addr::new(192, 168, 1, 20));
        let tailscale = Some(Ipv4Addr::new(100, 64, 12, 34));
        assert_eq!(
            resolve_external_host_from_sources(
                Some("beam.example.com"),
                Some("0.0.0.0"),
                tailscale,
                lan,
            ),
            "beam.example.com"
        );
        assert_eq!(
            resolve_external_host_from_sources(None, Some("10.0.0.8"), tailscale, lan),
            "10.0.0.8"
        );
        assert_eq!(
            resolve_external_host_from_sources(None, Some("0.0.0.0"), tailscale, lan),
            "100.64.12.34"
        );
        assert_eq!(
            resolve_external_host_from_sources(None, Some("0.0.0.0"), None, lan),
            "192.168.1.20"
        );
        assert_eq!(
            resolve_external_host_from_sources(None, Some("0.0.0.0"), None, None),
            "localhost"
        );
    }

    #[test]
    fn candidate_list_includes_tailscale_and_lan_and_dedupes_unspecified_bind() {
        let candidates = external_host_candidates_from_sources(
            Some("beam.example.com"),
            Some("0.0.0.0"),
            Some(Ipv4Addr::new(100, 64, 12, 34)),
            &[
                Ipv4Addr::new(192, 168, 31, 20),
                Ipv4Addr::new(192, 168, 31, 20),
                Ipv4Addr::new(192, 168, 31, 21),
            ],
        );
        assert_eq!(
            candidates,
            vec![
                "beam.example.com".to_string(),
                "100.64.12.34".to_string(),
                "192.168.31.20".to_string(),
                "192.168.31.21".to_string(),
            ]
        );
        assert!(!candidates.contains(&"0.0.0.0".to_string()));
        assert!(!candidates.contains(&"localhost".to_string()));
    }

    #[test]
    fn candidate_list_falls_back_to_localhost_when_empty() {
        let candidates = external_host_candidates_from_sources(None, Some("0.0.0.0"), None, &[]);
        assert_eq!(candidates, vec!["localhost".to_string()]);
    }

    #[test]
    fn resolver_treats_localhost_as_explicit_bind_host() {
        assert_eq!(
            resolve_external_host_from_sources(
                None,
                Some("127.0.0.1"),
                Some(Ipv4Addr::new(100, 64, 12, 34)),
                Some(Ipv4Addr::new(192, 168, 1, 20)),
            ),
            "127.0.0.1"
        );
    }

    #[test]
    fn resolver_treats_ipv6_unspecified_as_auto_select() {
        assert_eq!(
            resolve_external_host_from_sources(
                None,
                Some("[::]"),
                Some(Ipv4Addr::new(100, 64, 12, 34)),
                None,
            ),
            "100.64.12.34"
        );
    }

    #[test]
    fn rewrite_session_terminal_urls_updates_only_terminal_sessions() {
        let mut sessions = HashMap::new();
        let mut keep = make_session("sess-keep");
        keep.terminal_url = None;
        let mut rewrite = make_session("sess-rewrite");
        rewrite.terminal_url = Some("http://old.example.com:8800/s/sess-rewrite?x=1".to_string());
        sessions.insert(keep.session_id.clone(), keep);
        sessions.insert(rewrite.session_id.clone(), rewrite);

        let changed = rewrite_session_terminal_urls(&mut sessions, "new.example.com", 9900);
        assert_eq!(changed, 1);
        assert_eq!(
            sessions
                .get("sess-rewrite")
                .and_then(|s| s.terminal_url.as_deref()),
            Some("http://new.example.com:9900/s/sess-rewrite?x=1")
        );
        assert_eq!(
            sessions
                .get("sess-keep")
                .and_then(|s| s.terminal_url.as_deref()),
            None
        );
    }
}
