use std::net::Ipv4Addr;
use std::process::Command;

fn tailscale_ipv4_from_cli() -> Option<Ipv4Addr> {
    let output = Command::new("tailscale").args(["ip", "-4"]).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .find_map(|part| part.parse::<Ipv4Addr>().ok())
}

fn skip_if_external_host_override_is_set() -> bool {
    match std::env::var("BEAM_WEB_EXTERNAL_HOST") {
        Ok(value) if !value.trim().is_empty() => {
            eprintln!("skipping live test: BEAM_WEB_EXTERNAL_HOST is explicitly set");
            true
        }
        _ => false,
    }
}

#[test]
#[ignore = "live test: requires local tailscale CLI/daemon and a Tailscale IPv4"]
fn live_external_host_prefers_tailscale_ip_for_wildcard_bind() {
    if skip_if_external_host_override_is_set() {
        return;
    }
    let Some(tailscale_ip) = tailscale_ipv4_from_cli() else {
        eprintln!("skipping live test: `tailscale ip -4` did not return an IPv4");
        return;
    };

    let resolved = beam_daemon::__test_resolve_external_host("0.0.0.0");
    assert_eq!(
        resolved,
        tailscale_ip.to_string(),
        "wildcard web host should prefer the Tailscale IPv4 returned by `tailscale ip -4`"
    );
}

#[test]
#[ignore = "live test: requires local tailscale CLI/daemon and a Tailscale IPv4"]
fn live_external_host_keeps_explicit_localhost_bind() {
    if skip_if_external_host_override_is_set() {
        return;
    }
    let Some(_) = tailscale_ipv4_from_cli() else {
        eprintln!("skipping live test: `tailscale ip -4` did not return an IPv4");
        return;
    };

    let resolved = beam_daemon::__test_resolve_external_host("127.0.0.1");
    assert_eq!(
        resolved, "127.0.0.1",
        "explicit localhost bind must not be replaced with the Tailscale IP"
    );
}
