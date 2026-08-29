//! Live integration test: end-to-end Herdr browser terminal through the
//! daemon terminal proxy.
//!
//! Pins the external herdr contracts the web bridge depends on:
//! - ticket login → Set-Cookie → built-in terminal page
//! - readonly WS: `hello` + a `terminal.frame` stream from `observe`
//! - write WS: input reaches the pane, controller conflict is rejected, and a
//!   graceful close releases the controller for the next writable connection
//!
//! Requires a locally installed `herdr` >= 0.8.2 and a running herdr server.
//! Ignored by default:
//!   cargo test -p beam-daemon --test live_herdr_terminal -- --ignored

use std::collections::HashMap;
use std::net::TcpListener;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use beam_core::Session;
use beam_daemon::test_hooks::{__test_generate_terminal_ticket, __test_start_terminal_proxy};
use futures_util::SinkExt;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::{ClientRequestBuilder, Message};

fn has_herdr() -> bool {
    Command::new("herdr")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn pick_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .expect("pick free port")
}

fn run_herdr(args: &[&str]) -> String {
    let out = Command::new("herdr")
        .args(args)
        .output()
        .expect("herdr invocation");
    assert!(
        out.status.success(),
        "herdr {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Create a managed workspace and return (workspace_id, pane_id).
fn create_pane(label: &str) -> (String, String) {
    let payload = run_herdr(&[
        "workspace",
        "create",
        "--cwd",
        std::env::temp_dir().to_str().unwrap(),
        "--label",
        label,
        "--no-focus",
    ]);
    let value: serde_json::Value = serde_json::from_str(&payload).expect("workspace create JSON");
    let workspace_id = value
        .pointer("/result/workspace/workspace_id")
        .and_then(serde_json::Value::as_str)
        .expect("workspace_id");
    let pane_id = value
        .pointer("/result/root_pane/pane_id")
        .and_then(serde_json::Value::as_str)
        .expect("pane_id");
    run_herdr(&["pane", "run", pane_id, "sleep 3600"]);
    (workspace_id.to_string(), pane_id.to_string())
}

fn make_session(sid: &str, workspace_id: &str, pane_id: &str) -> Session {
    serde_json::from_str::<Session>(&format!(
        r#"{{
            "session_id": "{sid}",
            "title": "live",
            "chat_id": "chat-live",
            "root_message_id": "root-live",
            "scope": "thread",
            "status": "active",
            "created_at": "2026-01-01T00:00:00Z",
            "lark_app_id": "local",
            "backend_kind": "herdr",
            "herdr_workspace_id": "{workspace_id}",
            "herdr_pane_id": "{pane_id}"
        }}"#
    ))
    .expect("session JSON")
}

async fn ticket_login(base: &str, sid: &str) -> String {
    let ticket = __test_generate_terminal_ticket(sid, false);
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let resp = client
        .get(format!("{base}/s/{sid}?beam_terminal_ticket={ticket}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 303, "ticket login should redirect");
    let set_cookie = resp
        .headers()
        .get("set-cookie")
        .and_then(|v| v.to_str().ok())
        .expect("set-cookie on redirect");
    assert!(set_cookie.contains("beam_terminal_session="));
    set_cookie
        .split(';')
        .next()
        .unwrap()
        .split_once('=')
        .map(|(_, value)| value.to_string())
        .unwrap()
}

async fn ws_connect(
    base: &str,
    sid: &str,
    cookie: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let uri: axum::http::Uri = format!("{base}/s/{sid}/ws/herdr").parse().expect("ws uri");
    let mut builder = ClientRequestBuilder::new(uri);
    builder = builder.with_header("Cookie", format!("beam_terminal_session={cookie}"));
    let (ws, _) = tokio_tungstenite::connect_async(builder)
        .await
        .expect("ws connect");
    ws
}

async fn next_json(
    ws: &mut (
             impl futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
             + Unpin
         ),
) -> serde_json::Value {
    use futures_util::StreamExt;
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => {
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                    return value;
                }
            }
            Some(Ok(_)) => continue,
            other => panic!("ws closed while waiting for JSON: {other:?}"),
        }
    }
}

async fn wait_for_frame(
    ws: &mut (
             impl futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
             + Unpin
         ),
    timeout: Duration,
) -> String {
    use futures_util::StreamExt;
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!("no terminal.frame within {timeout:?}");
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let value: serde_json::Value = serde_json::from_str(&text).expect("frame JSON");
                if value.get("type").and_then(|t| t.as_str()) == Some("frame") {
                    return value
                        .get("bytes")
                        .and_then(|b| b.as_str())
                        .unwrap_or_default()
                        .to_string();
                }
            }
            Ok(Some(_)) => continue,
            Ok(None) => panic!("ws closed while waiting for a frame"),
            Err(_) => continue,
        }
    }
}

async fn expect_close_code(
    ws: &mut (
             impl futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
             + Unpin
         ),
) -> u16 {
    use futures_util::StreamExt;
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Close(frame)))) => {
                return frame.map(|f| f.code.into()).unwrap_or(1000);
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(err))) => panic!("ws error while waiting for close frame: {err}"),
            Ok(None) => panic!("ws closed without a close frame"),
            Err(_) => panic!("timeout waiting for close frame"),
        }
    }
}

#[tokio::test]
#[ignore = "requires a real herdr binary and server"]
async fn live_herdr_web_terminal_readonly_and_write() {
    if !has_herdr() {
        eprintln!("skipping: herdr not installed");
        return;
    }
    let label = format!("beam-web-live-{}", std::process::id());
    let (workspace_id, pane_id) = create_pane(&label);
    let sid = format!("live-herdr-{}", std::process::id());
    let session = make_session(&sid, &workspace_id, &pane_id);
    let sessions: Arc<Mutex<HashMap<String, Session>>> =
        Arc::new(Mutex::new(HashMap::from([(sid.clone(), session)])));

    let proxy_port = pick_port();
    let port = __test_start_terminal_proxy(
        "127.0.0.1",
        proxy_port,
        proxy_port + 1,
        sessions,
        true,
        4,
        16,
    )
    .await
    .expect("start terminal proxy");
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    // Ticket login sets the beam cookie; the page is the built-in terminal.
    let cookie = ticket_login(&base, &sid).await;
    let page = reqwest::Client::new()
        .get(format!("{base}/s/{sid}"))
        .header("Cookie", format!("beam_terminal_session={cookie}"))
        .send()
        .await
        .unwrap();
    let page = page.text().await.unwrap();
    assert!(page.contains("xterm.min.js"), "built-in terminal page");

    // Readonly WS: hello + at least one frame with decodable ANSI bytes.
    let mut ro = ws_connect(&ws_base, &sid, &cookie).await;
    let hello = next_json(&mut ro).await;
    assert_eq!(hello.get("type").and_then(|t| t.as_str()), Some("hello"));
    assert_eq!(hello.get("mode").and_then(|m| m.as_str()), Some("readonly"));
    let frame_b64 = wait_for_frame(&mut ro, Duration::from_secs(8)).await;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&frame_b64)
        .expect("frame bytes base64");
    assert!(!decoded.is_empty(), "readonly frame must carry ANSI bytes");
    let _ = ro.close(None).await;

    // Write WS: input reaches the pane.
    let write_ticket = __test_generate_terminal_ticket(&sid, true);
    let write_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let resp = write_client
        .get(format!(
            "{base}/s/{sid}?beam_terminal_ticket={write_ticket}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 303);
    let write_cookie = resp
        .headers()
        .get("set-cookie")
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .split_once('=')
        .map(|(_, v)| v.to_string())
        .unwrap();

    let mut w1 = ws_connect(&ws_base, &sid, &write_cookie).await;
    let hello = next_json(&mut w1).await;
    assert_eq!(hello.get("mode").and_then(|m| m.as_str()), Some("write"));
    w1.send(Message::Text(
        r#"{"type":"input","text":"echo herdr-web-live-ok\n"}"#
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    let screen = run_herdr(&[
        "pane", "read", &pane_id, "--source", "visible", "--format", "ansi",
    ]);
    assert!(
        screen.contains("herdr-web-live-ok"),
        "write input must reach the pane, screen: {screen}"
    );

    // A second writable connection is rejected while the first owns the pane.
    let mut w2 = ws_connect(&ws_base, &sid, &write_cookie).await;
    assert_eq!(
        expect_close_code(&mut w2).await,
        4001,
        "second write WS must be rejected with 4001"
    );

    // Graceful close releases the controller for the next writable connection.
    w1.send(Message::Close(None)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let mut w3 = ws_connect(&ws_base, &sid, &write_cookie).await;
    let hello = next_json(&mut w3).await;
    assert_eq!(
        hello.get("mode").and_then(|m| m.as_str()),
        Some("write"),
        "controller must be releasable after a graceful close"
    );
    let _ = w3.close(None).await;

    let _ = Command::new("herdr")
        .args(["workspace", "close", &workspace_id])
        .output();
}
