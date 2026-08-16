//! Live launch checks for the env / systemd-run spawn path.
//!
//! Requires `zellij` and Linux `/proc`. The slice test also needs
//! `/usr/bin/systemd-run` and a working user systemd.
//!
//! ```text
//! cargo test -p beam-worker live_launch -- --ignored --nocapture
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use crate::adapter::SpawnSpec;

use super::launch::{LaunchInput, LaunchPlatform, build_launch_spec};

const SLICE: &str = "beam-live-launch.slice";

struct LiveGuard {
    session: String,
    tmp: PathBuf,
}

impl Drop for LiveGuard {
    fn drop(&mut self) {
        let _ = Command::new("zellij")
            .args(["delete-session", &self.session, "-f"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = fs::remove_dir_all(&self.tmp);
    }
}

fn has_bin(name: &str) -> bool {
    Command::new("which")
        .arg(name)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn kdl_string(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn write_probe(dir: &Path) -> PathBuf {
    let bindir = dir.join("bin");
    fs::create_dir_all(&bindir).expect("bindir");
    let probe = bindir.join("shortcli");
    let script = format!(
        "#!/bin/sh\n{{\n  echo tty0=$( [ -t 0 ] && echo yes || echo no )\n  echo BEAM_SESSION_ID=${{BEAM_SESSION_ID-UNSET}}\n  echo BEAM_HOME=${{BEAM_HOME-UNSET}}\n  echo BEAM_BIN=${{BEAM_BIN-UNSET}}\n  echo PATH_HAS_BIN=$(printf '%s' \"$PATH\" | grep -F -c '{bindir}' || true)\n  echo argv0=$0\n  echo cgroup=$(cat /proc/self/cgroup)\n}} > '{out}'\nsleep 2\n",
        bindir = bindir.display(),
        out = dir.join("probe.out").display(),
    );
    fs::write(&probe, script).expect("write probe");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&probe, fs::Permissions::from_mode(0o755)).expect("chmod probe");
    }
    probe
}

fn start_zellij(session: &str, cwd: &Path, bin: &str, args: &[String]) {
    let layout = cwd.join("layout.kdl");
    let pane_args = args
        .iter()
        .map(|a| kdl_string(a))
        .collect::<Vec<_>>()
        .join(" ");
    let body = format!(
        "layout {{\n    tab name=\"beam\" {{\n        pane command={} close_on_exit=true cwd={} {{\n            args {}\n        }}\n    }}\n}}\n",
        kdl_string(bin),
        kdl_string(&cwd.display().to_string()),
        pane_args,
    );
    fs::write(&layout, body).expect("write layout");
    let status = Command::new("zellij")
        .args([
            "--session",
            session,
            "--new-session-with-layout",
            layout.to_str().expect("layout utf8"),
            "attach",
            "--create-background",
            session,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("spawn zellij");
    assert!(
        status.success(),
        "zellij failed to start session {session}: {status}"
    );
}

fn wait_probe_out(path: &Path) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(text) = fs::read_to_string(path)
            && text.contains("BEAM_SESSION_ID=")
        {
            return text;
        }
        if Instant::now() > deadline {
            panic!("probe.out not ready within 10s (path={})", path.display());
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn field<'a>(out: &'a str, key: &str) -> &'a str {
    out.lines()
        .find_map(|line| line.strip_prefix(&format!("{key}=")))
        .unwrap_or_else(|| panic!("missing {key} in:\n{out}"))
}

fn sample_input(tmp: &Path, slice: Option<&str>) -> LaunchInput {
    LaunchInput {
        cgroup_slice: slice.map(str::to_string),
        session_id: "live-sess".to_string(),
        beam_home: tmp.join("beam-home").display().to_string(),
        beam_bin: tmp.join("beam-bin").display().to_string(),
        path_prepend: tmp.join("bin").display().to_string(),
        extra_env: vec![],
        spec: SpawnSpec {
            bin: "shortcli".to_string(),
            args: vec!["--probe".to_string()],
        },
    }
}

/// cargo test -p beam-worker live_launch_env -- --ignored --nocapture
#[test]
#[ignore = "live test: requires zellij and Linux /proc"]
fn live_launch_env_in_zellij_injects_beam_env() {
    if !has_bin("zellij") {
        eprintln!("skipping: zellij not on PATH");
        return;
    }
    if !Path::new("/proc").is_dir() {
        eprintln!("skipping: /proc missing");
        return;
    }

    let tmp = std::env::temp_dir().join(format!("beam-live-env-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&tmp).expect("tmp");
    write_probe(&tmp);
    let session = format!("beam-live-env-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let _guard = LiveGuard {
        session: session.clone(),
        tmp: tmp.clone(),
    };

    let (bin, args) = build_launch_spec(LaunchPlatform::Linux, sample_input(&tmp, None))
        .expect("env launch spec");
    start_zellij(&session, &tmp, &bin, &args);
    let out = wait_probe_out(&tmp.join("probe.out"));
    eprintln!("env probe:\n{out}");
    assert_eq!(field(&out, "BEAM_SESSION_ID"), "live-sess");
    assert!(field(&out, "BEAM_HOME").ends_with("beam-home"));
    assert_eq!(field(&out, "PATH_HAS_BIN"), "1");
    assert!(
        field(&out, "argv0").ends_with("shortcli"),
        "expected short-name argv0, got {}",
        field(&out, "argv0")
    );
    assert_eq!(field(&out, "tty0"), "yes");
}

/// cargo test -p beam-worker live_launch_slice -- --ignored --nocapture
#[test]
#[ignore = "live test: requires zellij, /usr/bin/systemd-run, user systemd, Linux /proc"]
fn live_launch_slice_resolves_short_bin_and_enters_cgroup() {
    if !has_bin("zellij") {
        eprintln!("skipping: zellij not on PATH");
        return;
    }
    if !Path::new("/usr/bin/systemd-run").is_file() {
        eprintln!("skipping: /usr/bin/systemd-run missing");
        return;
    }
    if !Path::new("/proc").is_dir() {
        eprintln!("skipping: /proc missing");
        return;
    }

    let tmp = std::env::temp_dir().join(format!("beam-live-slice-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&tmp).expect("tmp");
    write_probe(&tmp);
    let session = format!("beam-live-slice-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let _guard = LiveGuard {
        session: session.clone(),
        tmp: tmp.clone(),
    };

    let (bin, args) = build_launch_spec(LaunchPlatform::Linux, sample_input(&tmp, Some(SLICE)))
        .expect("slice launch spec");
    assert_eq!(bin, "/usr/bin/systemd-run");
    start_zellij(&session, &tmp, &bin, &args);
    let out = wait_probe_out(&tmp.join("probe.out"));
    eprintln!("slice probe:\n{out}");
    assert_eq!(field(&out, "BEAM_SESSION_ID"), "live-sess");
    assert_eq!(field(&out, "PATH_HAS_BIN"), "1");
    assert!(
        field(&out, "argv0").ends_with("shortcli"),
        "short cliBin must resolve via env PATH after systemd-run --"
    );
    assert!(
        field(&out, "cgroup").contains(SLICE),
        "process must enter {SLICE}, got {}",
        field(&out, "cgroup")
    );
    assert_eq!(field(&out, "tty0"), "yes");
}
