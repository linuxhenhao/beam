use anyhow::{Result, bail};

use crate::adapter::SpawnSpec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LaunchPlatform {
    Linux,
    Other,
}

pub(crate) fn current_launch_platform() -> LaunchPlatform {
    if cfg!(target_os = "linux") {
        LaunchPlatform::Linux
    } else {
        LaunchPlatform::Other
    }
}

/// Inputs for the pane command. Adapter argv is already in `spec`.
pub(crate) struct LaunchInput {
    pub cgroup_slice: Option<String>,
    pub session_id: String,
    pub beam_home: String,
    pub beam_bin: String,
    pub path_prepend: String,
    pub extra_env: Vec<(String, String)>,
    pub spec: SpawnSpec,
}

pub(crate) fn normalize_cgroup_slice(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// Non-Linux + a set slice is an error; empty slice is the portable env path.
pub(crate) fn build_launch_spec(
    platform: LaunchPlatform,
    input: LaunchInput,
) -> Result<(String, Vec<String>)> {
    let slice = normalize_cgroup_slice(input.cgroup_slice.as_deref());
    let env_pairs = launch_env_pairs(&input);
    match slice {
        None => Ok(env_launch(env_pairs, input.spec)),
        Some(slice) if platform == LaunchPlatform::Linux => {
            Ok(systemd_scope_launch(&slice, env_pairs, input.spec))
        }
        Some(slice) => {
            bail!(
                "cgroupSlice ({slice}) is Linux-only; macOS/other hosts cannot place the CLI in a cgroup"
            )
        }
    }
}

fn launch_env_pairs(input: &LaunchInput) -> Vec<(String, String)> {
    let mut pairs = vec![
        ("BEAM_SESSION_ID".to_string(), input.session_id.clone()),
        ("BEAM_HOME".to_string(), input.beam_home.clone()),
        ("BEAM_BIN".to_string(), input.beam_bin.clone()),
    ];
    let path = if input.path_prepend.is_empty() {
        std::env::var("PATH").unwrap_or_default()
    } else if let Ok(current) = std::env::var("PATH") {
        if current.is_empty() {
            input.path_prepend.clone()
        } else {
            format!("{}:{current}", input.path_prepend)
        }
    } else {
        input.path_prepend.clone()
    };
    pairs.push(("PATH".to_string(), path));
    pairs.extend(input.extra_env.iter().cloned());
    pairs
}

fn env_launch(env_pairs: Vec<(String, String)>, spec: SpawnSpec) -> (String, Vec<String>) {
    let mut args = Vec::with_capacity(env_pairs.len() + 1 + spec.args.len());
    for (key, value) in env_pairs {
        args.push(format!("{key}={value}"));
    }
    args.push(spec.bin);
    args.extend(spec.args);
    ("/usr/bin/env".to_string(), args)
}

fn systemd_scope_launch(
    slice: &str,
    env_pairs: Vec<(String, String)>,
    spec: SpawnSpec,
) -> (String, Vec<String>) {
    // systemd-run --scope resolves the exec binary in its own PATH, not
    // from -E PATH. Put /usr/bin/env after -- so short cliBin names still
    // resolve via the injected PATH.
    let (env_bin, env_args) = env_launch(env_pairs, spec);
    let mut args = vec![
        "--user".to_string(),
        "--scope".to_string(),
        format!("--slice={slice}"),
        "--quiet".to_string(),
        "--".to_string(),
        env_bin,
    ];
    args.extend(env_args);
    ("/usr/bin/systemd-run".to_string(), args)
}

pub(crate) fn systemd_run_available() -> bool {
    std::process::Command::new("/usr/bin/systemd-run")
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_spec() -> SpawnSpec {
        SpawnSpec {
            bin: "grok".to_string(),
            args: vec![
                "--always-approve".to_string(),
                "--session-id".to_string(),
                "sid".to_string(),
            ],
        }
    }

    fn sample_input(slice: Option<&str>) -> LaunchInput {
        LaunchInput {
            cgroup_slice: slice.map(str::to_string),
            session_id: "sess-1".to_string(),
            beam_home: "/tmp/beam-home".to_string(),
            beam_bin: "/opt/beam/beam-worker".to_string(),
            path_prepend: "/opt/beam".to_string(),
            extra_env: vec![("TERM".to_string(), "xterm-256color".to_string())],
            spec: sample_spec(),
        }
    }

    #[test]
    fn env_launch_injects_beam_env_then_real_cli() {
        let (bin, args) =
            build_launch_spec(LaunchPlatform::Other, sample_input(None)).expect("env launch");
        assert_eq!(bin, "/usr/bin/env");
        assert!(args.iter().any(|a| a == "BEAM_SESSION_ID=sess-1"));
        assert!(args.iter().any(|a| a == "BEAM_HOME=/tmp/beam-home"));
        assert!(args.iter().any(|a| a == "BEAM_BIN=/opt/beam/beam-worker"));
        assert!(
            args.iter()
                .any(|a| a == "PATH=/opt/beam" || a.starts_with("PATH=/opt/beam:"))
        );
        assert!(args.iter().any(|a| a == "TERM=xterm-256color"));
        let grok = args.iter().position(|a| a == "grok").expect("cli bin");
        assert_eq!(
            &args[grok..],
            ["grok", "--always-approve", "--session-id", "sid"]
        );
    }

    #[test]
    fn empty_slice_uses_env_path() {
        let (bin, _) = build_launch_spec(LaunchPlatform::Linux, sample_input(Some("  ")))
            .expect("blank slice is env");
        assert_eq!(bin, "/usr/bin/env");
    }

    #[test]
    fn linux_slice_uses_systemd_scope_and_keeps_cli_together() {
        let (bin, args) = build_launch_spec(
            LaunchPlatform::Linux,
            sample_input(Some("cgtproxy-gateway.slice")),
        )
        .expect("scope launch");
        assert_eq!(bin, "/usr/bin/systemd-run");
        assert_eq!(args[0], "--user");
        assert_eq!(args[1], "--scope");
        assert_eq!(args[2], "--slice=cgtproxy-gateway.slice");
        let sep = args.iter().position(|a| a == "--").expect("sep");
        assert_eq!(args[sep], "--");
        assert_eq!(args[sep + 1], "/usr/bin/env");
        assert!(
            args[sep + 2..]
                .iter()
                .any(|a| a == "BEAM_SESSION_ID=sess-1")
        );
        let grok = args.iter().position(|a| a == "grok").expect("cli bin");
        assert!(grok > sep);
        assert_eq!(
            &args[grok..],
            ["grok", "--always-approve", "--session-id", "sid"]
        );
    }

    #[test]
    fn slice_on_non_linux_is_an_error() {
        let err = build_launch_spec(
            LaunchPlatform::Other,
            sample_input(Some("cgtproxy-gateway.slice")),
        )
        .expect_err("mac must reject slice");
        let msg = err.to_string();
        assert!(msg.contains("cgroupSlice"), "{msg}");
        assert!(msg.contains("Linux-only"), "{msg}");
    }
}
