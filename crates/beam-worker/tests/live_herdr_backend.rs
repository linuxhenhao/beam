// Live integration test: drive a real `herdr` binary through the worker's
// `HerdrBackend` (managed spawn → shell wait → pane run → input → read →
// kill → destroy). Also locks the `workspace close` implementation gate:
// whether close kills the pane process, and the force flag contract.
//
// Requires a locally installed `herdr` >= 0.8.2 (https://herdr.dev) and a
// running herdr server. Ignored by default:
//   cargo test -p beam-worker --test live_herdr_backend -- --ignored

use std::process::Command;
use std::time::Duration;

use beam_worker::{HerdrBackend, SessionBackend};

fn has_herdr() -> bool {
    Command::new("herdr")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn sid() -> String {
    format!("live-{}", std::process::id())
}

#[tokio::test]
#[ignore = "requires a real herdr binary and server"]
async fn live_managed_spawn_input_read_destroy() {
    if !has_herdr() {
        eprintln!("skipping: herdr not installed");
        return;
    }
    let label = sid();
    let backend = HerdrBackend::new(label.clone(), std::env::temp_dir().display().to_string());
    backend
        .spawn(
            "/usr/bin/env",
            &["sleep".to_string(), "3600".to_string()],
            beam_worker::SpawnOpts {
                cwd: std::env::temp_dir().display().to_string(),
                cols: 160,
                rows: 50,
                env: Vec::new(),
            },
        )
        .await
        .expect("managed spawn");

    let ids = backend.herdr_ids().expect("herdr ids after spawn");
    assert_eq!(ids.workspace_id, ids.pane_id.split(':').next().unwrap());

    // Input reaches the pane.
    backend
        .send_text("echo herdr-live-ok\n")
        .await
        .expect("send text");
    backend.send_enter().await.expect("send enter");

    // Screen capture returns ANSI.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let screen = backend.capture_viewport().await.expect("capture viewport");
    assert!(!screen.is_empty());

    // Liveness: foreground sleep is alive.
    assert!(backend.is_alive().await.expect("is_alive"));

    // Destroy force-closes the managed workspace (implementation gate).
    backend.destroy_session().await.expect("destroy session");
    assert!(!backend.is_alive().await.expect("dead after destroy"));
}
