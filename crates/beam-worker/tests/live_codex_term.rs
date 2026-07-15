// Live integration test: verify that codex started through beam-worker/zellij
// receives TERM=xterm-256color even when the worker environment has TERM=dumb.
//
// This test requires locally installed and authenticated `codex`, `zellij`,
// and a Linux /proc filesystem. It is ignored by default.

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Read a fresh UUID from the Linux kernel interface.
fn read_uuid() -> String {
    fs::read_to_string("/proc/sys/kernel/random/uuid")
        .expect("failed to read UUID from /proc/sys/kernel/random/uuid (not Linux?)")
        .trim()
        .to_string()
}

/// Check whether `name` is available in PATH.
fn has_command(name: &str) -> bool {
    Command::new("which")
        .arg(name)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Collect PIDs whose `/proc/<pid>/environ` contains `key=value`.
///
/// Skips unreadable and non-numeric entries instead of bailing out early.
fn all_pids_by_env(key: &str, value: &str) -> Vec<u32> {
    let Ok(proc_dir) = fs::read_dir("/proc") else {
        return vec![];
    };
    let needle = format!("{key}={value}\0");
    let mut result = Vec::new();
    for entry in proc_dir.flatten() {
        let name = match entry.file_name().to_str() {
            Some(s) => s.to_string(),
            None => continue,
        };
        let pid: u32 = match name.parse() {
            Ok(p) => p,
            Err(_) => continue,
        };
        let environ_path = entry.path().join("environ");
        let data = match fs::read(&environ_path) {
            Ok(d) => d,
            Err(_) => continue,
        };
        if data.windows(needle.len()).any(|w| w == needle.as_bytes()) {
            result.push(pid);
        }
    }
    result
}

/// Parse a null-separated `/proc/<pid>/cmdline` into argument strings.
fn get_cmdline_args(pid: u32) -> Option<Vec<String>> {
    let data = fs::read(format!("/proc/{pid}/cmdline")).ok()?;
    Some(
        data.split(|&b| b == 0)
            .filter_map(|s| std::str::from_utf8(s).ok())
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect(),
    )
}

/// Determine whether `args` (from cmdline) identifies the actual codex CLI
/// binary process rather than a shell wrapper or a node shim.
///
/// Rules:
///   1. Reject if argv[0] is a shell (`/bin/sh`, `sh`, `bash`, …).
///   2. Accept only if argv[0]'s basename is `"codex"` (the vendored CLI
///      binary).  This excludes the node shim (`node …/codex …`) whose
///      argv[0] is `"node"`.
fn argv_points_to_codex(args: &[String]) -> bool {
    let argv0 = match args.first() {
        Some(a) => a,
        None => return false,
    };
    let base = Path::new(argv0)
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or("");

    // Reject shells.
    if matches!(base, "sh" | "bash" | "dash" | "zsh") {
        return false;
    }

    // Accept only when argv[0] itself is (or contains) "codex".
    base == "codex"
}

/// Format a cmdline argument vector into a human-readable string.
fn format_cmdline(args: &[String]) -> String {
    let joined: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    joined.join(" ")
}

/// Extract a single environment variable from a `/proc/<pid>/environ` blob
/// (null-separated "KEY=value" entries).
fn get_env_var(data: &[u8], key: &str) -> Option<String> {
    let prefix = format!("{key}=");
    for entry in data.split(|&b| b == 0) {
        if let Ok(s) = std::str::from_utf8(entry) {
            if let Some(val) = s.strip_prefix(&prefix) {
                return Some(val.to_string());
            }
        }
    }
    None
}

/// Parse the zellij session name from a WorkerToDaemon::Ready JSON line.
///
/// Expected serialised form (with `#[serde(tag = "type", rename_all =
/// "snake_case")]`):
///   {"type":"ready","zellij_session":"beam-xxxxxxxx"}
fn parse_ready_session(line: &str) -> Option<String> {
    let key = r#""zellij_session":""#;
    let start = line.find(key)?;
    let rest = &line[start + key.len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// Build a minimal InitConfig JSON string using only std formatting.
fn make_init_json(session_id: &str, working_dir: &str) -> String {
    let wd = working_dir.replace('\\', "\\\\").replace('"', "\\\"");
    format!(
        r#"{{"session_id":"{}","title":"test","chat_id":"test","root_message_id":"test","working_dir":"{}","cli_id":"codex","cli_bin":"codex","cli_args":[],"prompt":"","resume":false,"lark_app_id":"local","lark_app_secret":""}}"#,
        session_id, wd
    )
}

// ---------------------------------------------------------------------------
// non‑blocking stdout reader (background thread + channel)
// ---------------------------------------------------------------------------

/// Spawn a thread that reads lines from `pipe` and sends them through the
/// returned receiver. The thread exits when the pipe is closed or the
/// receiver is dropped.
fn spawn_line_reader<R: 'static + std::io::Read + Send>(
    pipe: R,
    label: &'static str,
) -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(pipe);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    if tx.send(line.clone()).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("[{label}] read error: {e}");
                    break;
                }
            }
        }
    });
    rx
}

// ---------------------------------------------------------------------------
// RAII cleanup guard — runs on normal exit and on panic
// ---------------------------------------------------------------------------

struct Guard {
    worker: Option<Child>,
    zellij_session: Option<String>,
    tmp_dir: Option<PathBuf>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        // Kill the worker first so it stops holding the zellij session.
        if let Some(ref mut child) = self.worker {
            let _ = child.kill();
            let _ = child.wait();
        }
        // Delete the zellij session that was created for this test.
        if let Some(ref session) = self.zellij_session {
            let _ = Command::new("zellij")
                .args(["delete-session", session, "-f"])
                .output();
        }
        // Remove the temporary directory.
        if let Some(ref dir) = self.tmp_dir {
            let _ = fs::remove_dir_all(dir);
        }
    }
}

// ---------------------------------------------------------------------------
// test
// ---------------------------------------------------------------------------

#[test]
#[ignore = "live test: requires locally installed and authenticated `codex`, `zellij`, and Linux /proc"]
fn live_codex_term_injected_in_zellij() {
    // ---------- prerequisite checks (skip gracefully) ----------
    if !has_command("codex") {
        eprintln!("skipping live test: `codex` not found in PATH");
        return;
    }
    if !has_command("zellij") {
        eprintln!("skipping live test: `zellij` not found in PATH");
        return;
    }
    if !Path::new("/proc").is_dir() {
        eprintln!("skipping live test: /proc not available (not Linux?)");
        return;
    }

    let uuid = read_uuid();
    let session_id = uuid; // unique BEAM_SESSION_ID discriminator
    let short = &session_id[..session_id.len().min(8)];
    let zellij_session_name = format!("beam-{short}");

    // ---------- create temp dir and init config ----------
    let tmp = PathBuf::from("/tmp").join(format!("beam-test-{session_id}"));
    fs::create_dir_all(&tmp).expect("failed to create temporary directory");

    // Set zellij_session in the guard immediately so it is cleaned up even if
    // we panic before parsing the Ready message.
    let mut guard = Guard {
        worker: None,
        zellij_session: Some(zellij_session_name.clone()),
        tmp_dir: Some(tmp.clone()),
    };

    let init_json = make_init_json(&session_id, tmp.to_str().unwrap());
    let init_path = tmp.join("init.json");
    fs::write(&init_path, &init_json).expect("failed to write init config");

    // ---------- spawn beam-worker ----------
    let worker_bin = std::env!("CARGO_BIN_EXE_beam-worker");
    let mut worker = Command::new(worker_bin)
        .arg("--init-path")
        .arg(&init_path)
        .env("TERM", "dumb") // force dumb TERM in the worker's own env
        .env("BEAM_HOME", &tmp)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::piped()) // keep open so the worker does not exit
        .spawn()
        .unwrap_or_else(|e| panic!("failed to spawn beam-worker: {e}"));

    // Forward stderr to the test output for debugging.
    let stderr = worker.stderr.take().expect("worker stderr not captured");
    let _stderr_rx = spawn_line_reader(stderr, "worker-stderr");

    // Read stdout via a background thread so we can poll with timeout.
    let stdout = worker.stdout.take().expect("worker stdout not captured");
    let stdout_rx = spawn_line_reader(stdout, "worker-stdout");

    // Use the proc::Child in the guard NOW after taking the pipes.
    guard.worker = Some(worker);

    // ---------- wait for Ready message (30 s timeout) ----------
    // The worker serialises WorkerToDaemon with #[serde(tag = "type",
    // rename_all = "snake_case")], so Ready becomes:
    //   {"type":"ready","zellij_session":"beam-xxxxxxxx"}
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        if Instant::now() > deadline {
            panic!(
                "worker did not send Ready within 30 s timeout \
                 (session_id={session_id}, zellij_session={zellij_session_name})"
            );
        }
        match stdout_rx.recv_timeout(Duration::from_millis(500)) {
            Ok(line) => {
                let trimmed = line.trim().to_string();
                if trimmed.contains(r#""type":"ready""#) {
                    let parsed = parse_ready_session(&trimmed).unwrap_or_else(|| {
                        panic!("could not extract zellij_session from Ready line: {trimmed}")
                    });
                    assert_eq!(
                        parsed, zellij_session_name,
                        "Ready reports zellij_session={parsed} \
                         but we expected {zellij_session_name}"
                    );
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("worker stdout reader exited unexpectedly");
            }
        }
    }

    eprintln!("worker Ready received, zellij session={zellij_session_name}");

    // ---------- find codex PID via BEAM_SESSION_ID + cmdline (15 s) ----------
    // The wrapper script exports BEAM_SESSION_ID before exec'ing codex.
    // After exec the process is either "codex" directly or a node shim
    // ("node …/codex.js").  We verify the cmdline to ensure we are looking
    // at the real codex process, not a transient shell wrapper.
    let deadline = Instant::now() + Duration::from_secs(15);
    let codex_pid = loop {
        if Instant::now() > deadline {
            // Dump every candidate for diagnosis.
            let all = all_pids_by_env("BEAM_SESSION_ID", &session_id);
            eprintln!("[diagnostic] all PIDs with BEAM_SESSION_ID={session_id}:");
            for pid in &all {
                let args = get_cmdline_args(*pid).unwrap_or_default();
                let verdict = if argv_points_to_codex(&args) {
                    "ACCEPT"
                } else {
                    "REJECT (not codex — probably wrapper shell)"
                };
                eprintln!(
                    "  pid={} cmdline=[{}]  {verdict}",
                    pid,
                    format_cmdline(&args),
                );
            }
            panic!(
                "could not find a codex process with BEAM_SESSION_ID={} \
                 within 15 s timeout ({} candidate(s) found, none accepted)",
                session_id,
                all.len(),
            );
        }

        // Collect all candidates that pass BOTH the env check and the
        // cmdline check.
        let mut accepted = Vec::new();
        for pid in all_pids_by_env("BEAM_SESSION_ID", &session_id) {
            if let Some(args) = get_cmdline_args(pid) {
                if argv_points_to_codex(&args) {
                    accepted.push((pid, args));
                }
            }
        }
        if accepted.len() == 1 {
            break accepted.into_iter().next().unwrap().0;
        }
        if accepted.len() > 1 {
            let desc: Vec<String> = accepted
                .iter()
                .map(|(pid, args)| format!("pid={} cmdline=[{}]", pid, format_cmdline(args)))
                .collect();
            panic!(
                "multiple ({}) codex candidates for session {}: {}",
                accepted.len(),
                session_id,
                desc.join("; "),
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    };

    eprintln!("found codex pid={codex_pid}");

    // ---------- assert TERM=xterm-256color ----------
    let environ_path = format!("/proc/{codex_pid}/environ");
    let environ_data = fs::read(&environ_path)
        .unwrap_or_else(|e| panic!("failed to read {environ_path}: {e} (codex exited?)"));

    let term_val = get_env_var(&environ_data, "TERM")
        .unwrap_or_else(|| panic!("TERM not found in environ of codex pid={codex_pid}"));

    assert_eq!(
        term_val, "xterm-256color",
        "codex (pid={codex_pid}) should have TERM=xterm-256color \
         despite worker TERM=dumb; got TERM={term_val}"
    );

    eprintln!("PASS: codex pid={codex_pid} TERM={term_val}");
}
