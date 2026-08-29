// Live integration test: pin the observe frame contract against a real herdr.
// Real 0.8.2 frames are NDJSON `terminal.frame` events with base64 ANSI in
// `bytes` and `"full":true` (full-screen renders), so the worker may cache
// them into `latest_raw_screen` like zellij subscribe.
//
// Requires a locally installed `herdr` >= 0.8.2. Ignored by default:
//   cargo test -p beam-worker --test live_herdr_observe -- --ignored

use std::io::BufRead;
use std::process::{Command, Stdio};
use std::time::Duration;

use beam_worker::parse_herdr_frame_line;

fn has_herdr() -> bool {
    Command::new("herdr")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tokio::test]
#[ignore = "requires a real herdr binary and server"]
async fn live_observe_frames_are_full_screen_ansi() {
    if !has_herdr() {
        eprintln!("skipping: herdr not installed");
        return;
    }
    // Create a throwaway workspace and read one observe line from it.
    let label = format!("beam-observe-{}", std::process::id());
    let created = Command::new("herdr")
        .args([
            "workspace",
            "create",
            "--cwd",
            "/tmp",
            "--label",
            &label,
            "--no-focus",
        ])
        .output()
        .expect("herdr workspace create");
    assert!(created.status.success(), "workspace create failed");
    let created: serde_json::Value = serde_json::from_slice(&created.stdout).expect("create JSON");
    let workspace_id = created["result"]["workspace"]["workspace_id"]
        .as_str()
        .expect("workspace id");
    let pane_id = created["result"]["root_pane"]["pane_id"]
        .as_str()
        .expect("pane id");

    let mut child = Command::new("herdr")
        .args([
            "terminal", "session", "observe", pane_id, "--cols", "80", "--rows", "24",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("herdr observe spawn");
    let stdout = child.stdout.take().expect("observe stdout");
    let mut lines = std::io::BufReader::new(stdout).lines();

    let mut observed_frame = None;
    let mut observed_full = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while let Some(Ok(line)) = lines.next() {
        if line.contains("terminal.closed") {
            break;
        }
        let v: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if v["type"].as_str() == Some("terminal.frame") {
            observed_full = v["full"].as_bool();
            observed_frame = parse_herdr_frame_line(&line);
            break;
        }
        if std::time::Instant::now() > deadline {
            break;
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    let _ = Command::new("herdr")
        .args(["workspace", "close", workspace_id])
        .output();

    // Pin the 0.8.2 contract: terminal.frame + bytes + full:true, decoding to
    // non-empty ANSI. A regression in the frame shape must fail here, not in
    // production screenshot delivery.
    assert!(
        observed_frame
            .as_deref()
            .map(|f| !f.is_empty())
            .unwrap_or(false),
        "no parseable frame observed; last line shape changed?"
    );
    assert_eq!(
        observed_full,
        Some(true),
        "0.8.2 observe frames are full-screen renders"
    );
}
