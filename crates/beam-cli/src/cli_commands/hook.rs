use crate::*;
use anyhow::{Context, Result, bail};

pub(crate) fn find_runtime(paths: &BeamPaths) -> Result<DaemonRuntimeState> {
    let raw = std::fs::read(paths.runtime_state_json()).with_context(|| {
        format!(
            "daemon state not found at {}",
            paths.runtime_state_json().display()
        )
    })?;
    Ok(serde_json::from_slice(&raw)?)
}

pub(crate) struct ApiClient {
    http: Client,
    base: String,
    key: Option<String>,
}

impl ApiClient {
    pub(crate) fn base(&self) -> &str {
        &self.base
    }

    pub(crate) fn get(&self, url: String) -> SignedBuilder {
        SignedBuilder::new(self.http.clone(), self.http.get(url), self.key.clone())
    }

    pub(crate) fn post(&self, url: String) -> SignedBuilder {
        SignedBuilder::new(self.http.clone(), self.http.post(url), self.key.clone())
    }
}

/// RequestBuilder wrapper whose `send` HMAC-signs the built request with the
/// local api token (when available). The signature covers method, path+query,
/// and the body hash, so the token itself never appears on the wire.
pub(crate) struct SignedBuilder {
    http: Client,
    inner: reqwest::RequestBuilder,
    key: Option<String>,
}

impl SignedBuilder {
    fn new(http: Client, inner: reqwest::RequestBuilder, key: Option<String>) -> Self {
        Self { http, inner, key }
    }

    pub(crate) fn json<T: serde::Serialize + ?Sized>(self, value: &T) -> Self {
        Self::new(self.http, self.inner.json(value), self.key)
    }

    pub(crate) fn query<T: serde::Serialize + ?Sized>(self, value: &T) -> Self {
        Self::new(self.http, self.inner.query(value), self.key)
    }

    pub(crate) fn header(self, key: &str, value: String) -> Self {
        Self::new(self.http, self.inner.header(key, value), self.key)
    }

    pub(crate) async fn send(self) -> Result<reqwest::Response> {
        let mut request = self.inner.build()?;
        if let Some(key) = self.key.as_deref() {
            sign_built_request(&mut request, key)?;
        }
        Ok(self.http.execute(request).await?)
    }
}

fn sign_built_request(request: &mut reqwest::Request, key: &str) -> Result<()> {
    use beam_core::api_token::{
        SIG_HEADER, SIG_NONCE_HEADER, SIG_TIMESTAMP_HEADER, generate_sig_nonce, now_unix_secs,
        sign_request,
    };
    let body_bytes: &[u8] = match request.body() {
        None => &[],
        Some(body) => match body.as_bytes() {
            Some(bytes) => bytes,
            // Streaming/multipart bodies cannot be hashed; send unsigned.
            None => return Ok(()),
        },
    };
    let method = request.method().as_str().to_string();
    let url = request.url().clone();
    let path_query = match url.query() {
        Some(query) => format!("{}?{}", url.path(), query),
        None => url.path().to_string(),
    };
    let ts = now_unix_secs();
    let nonce = generate_sig_nonce();
    let sig = sign_request(key, ts, &nonce, &method, &path_query, body_bytes);
    let headers = request.headers_mut();
    headers.insert(
        SIG_TIMESTAMP_HEADER,
        reqwest::header::HeaderValue::from_str(&ts.to_string())?,
    );
    headers.insert(
        SIG_NONCE_HEADER,
        reqwest::header::HeaderValue::from_str(&nonce)?,
    );
    headers.insert(SIG_HEADER, reqwest::header::HeaderValue::from_str(&sig)?);
    Ok(())
}

pub(crate) async fn api_client(paths: &BeamPaths) -> Result<ApiClient> {
    let runtime = find_runtime(paths)?;
    // Load the local api token (written and rotated daily by the daemon) to
    // HMAC-sign requests; the token itself is never sent.
    let key = beam_core::api_token::read_api_token(paths);
    Ok(ApiClient {
        http: Client::new(),
        base: format!("http://{}", runtime.api_addr),
        key,
    })
}

pub(crate) fn resolve_session_owner_approver(
    paths: &BeamPaths,
    session_id: &str,
) -> Option<String> {
    let raw = std::fs::read(paths.session_store_json()).ok()?;
    let sessions =
        serde_json::from_slice::<std::collections::HashMap<String, Session>>(&raw).ok()?;
    sessions
        .get(session_id)
        .and_then(|session| session.owner_open_id.as_ref())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(crate) async fn post_ask(
    paths: &BeamPaths,
    body: &serde_json::Value,
) -> Result<serde_json::Value> {
    let api = api_client(paths).await?;
    let auth = api.get(format!("{}/api/auth", api.base())).send().await?;
    let auth_json: serde_json::Value = auth.json().await?;
    let dashboard_token = auth_json
        .get("token")
        .and_then(|v| v.as_str())
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.to_string());
    tracing::info!(
        session_id = body
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or_default(),
        chat_id = body
            .get("chatId")
            .and_then(|v| v.as_str())
            .unwrap_or_default(),
        lark_app_id = body
            .get("larkAppId")
            .and_then(|v| v.as_str())
            .unwrap_or_default(),
        approvers = body
            .get("approvers")
            .and_then(|v| v.as_array())
            .map(|arr| arr.len())
            .unwrap_or(0),
        "beam hook submitting ask request"
    );
    let mut body = body.clone();
    let needs_approver = body
        .get("approvers")
        .and_then(|value| value.as_array())
        .map(|arr| {
            arr.iter()
                .all(|value| value.as_str().unwrap_or("").trim().is_empty())
        })
        .unwrap_or(true);
    if needs_approver
        && let Some(session_id) = body.get("sessionId").and_then(|value| value.as_str())
        && let Some(owner_open_id) = resolve_session_owner_approver(paths, session_id)
        && let Some(obj) = body.as_object_mut()
    {
        obj.insert("approvers".to_string(), serde_json::json!([owner_open_id]));
    }
    let resp = api
        .post(format!("{}/api/asks", api.base()))
        .header("x-dashboard-token", dashboard_token.unwrap_or_default())
        .json(&body)
        .send()
        .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        tracing::warn!(%status, error = %text, "beam hook ask submission failed");
        bail!("{}", text);
    }
    let result = resp.json().await?;
    tracing::info!("beam hook ask request accepted");
    Ok(result)
}

pub(crate) fn resolve_ask_context(
    paths: &BeamPaths,
    payload: &serde_json::Value,
) -> Result<(String, String, String, Option<String>)> {
    let payload_cli_session_id = payload
        .get("sessionID")
        .and_then(|v| v.as_str())
        .or_else(|| payload.get("sessionId").and_then(|v| v.as_str()))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let mut chat_id = std::env::var("BEAM_CHAT_ID")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let mut lark_app_id = std::env::var("BEAM_LARK_APP_ID")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let mut root_message_id = std::env::var("BEAM_ROOT_MESSAGE_ID")
        .ok()
        .and_then(|value| {
            let value = value.trim().to_string();
            if value.is_empty() { None } else { Some(value) }
        });

    let mut session_id = None;
    if let Ok(raw) = std::fs::read_to_string(paths.session_store_json())
        && let Ok(sessions) =
            serde_json::from_str::<std::collections::HashMap<String, Session>>(&raw)
    {
        if let Some(cli_session_id) = payload_cli_session_id.as_deref()
            && let Some((beam_session_id, session)) = sessions
                .iter()
                .find(|(_, session)| session.cli_session_id.as_deref() == Some(cli_session_id))
        {
            session_id = Some(beam_session_id.clone());
            chat_id.get_or_insert_with(|| session.chat_id.clone());
            lark_app_id.get_or_insert_with(|| session.lark_app_id.clone());
            if root_message_id.is_none() {
                let value = session.root_message_id.trim().to_string();
                if !value.is_empty() {
                    root_message_id = Some(value);
                }
            }
        }
        if session_id.is_none() {
            let active_sessions = sessions
                .iter()
                .filter(|(_, session)| {
                    session.cli_id.as_deref() == Some("opencode")
                        && session.status == SessionStatus::Active
                })
                .collect::<Vec<_>>();
            if active_sessions.len() == 1 {
                let (beam_session_id, session) = active_sessions[0];
                session_id = Some(beam_session_id.clone());
                chat_id.get_or_insert_with(|| session.chat_id.clone());
                lark_app_id.get_or_insert_with(|| session.lark_app_id.clone());
                if root_message_id.is_none() {
                    let value = session.root_message_id.trim().to_string();
                    if !value.is_empty() {
                        root_message_id = Some(value);
                    }
                }
            }
        }
    }

    if session_id.is_none() {
        session_id = Some(discover_session_id(paths)?);
    }

    if (chat_id.is_none() || lark_app_id.is_none() || root_message_id.is_none())
        && let Ok(raw) = std::fs::read_to_string(paths.session_store_json())
        && let Ok(sessions) =
            serde_json::from_str::<std::collections::HashMap<String, Session>>(&raw)
        && let Some(session_key) = session_id.as_deref()
        && let Some(session) = sessions.get(session_key)
    {
        chat_id.get_or_insert_with(|| session.chat_id.clone());
        lark_app_id.get_or_insert_with(|| session.lark_app_id.clone());
        if root_message_id.is_none() {
            let value = session.root_message_id.trim().to_string();
            if !value.is_empty() {
                root_message_id = Some(value);
            }
        }
    }

    let session_id = session_id.unwrap_or_default();
    let chat_id = chat_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("unable to resolve chat_id for session {}", session_id))?;
    let lark_app_id = lark_app_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!("unable to resolve lark_app_id for session {}", session_id)
        })?;

    Ok((session_id, chat_id, lark_app_id, root_message_id))
}

pub(crate) fn discover_session_id(paths: &BeamPaths) -> Result<String> {
    let env_session_id = std::env::var("BEAM_SESSION_ID")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    discover_session_id_from_pid(paths, std::process::id(), env_session_id.as_deref())
}

pub(crate) fn resolve_cli_session_id(
    paths: &BeamPaths,
    explicit: Option<String>,
) -> Result<String> {
    match explicit {
        Some(value) if !value.trim().is_empty() => Ok(value.trim().to_string()),
        _ => discover_session_id(paths).map_err(|_| {
            anyhow::anyhow!(
                "无法推断 session-id。请在 beam 会话里的 CLI 中运行，或传 --session-id <id>。"
            )
        }),
    }
}

pub(crate) fn discover_session_id_from_pid(
    paths: &BeamPaths,
    mut pid: u32,
    env_session_id: Option<&str>,
) -> Result<String> {
    if let Some(value) = env_session_id {
        return Ok(value.to_string());
    }

    let markers = paths.cli_pid_markers_dir();
    loop {
        let candidate = markers.join(pid.to_string());
        if let Ok(raw) = std::fs::read_to_string(&candidate) {
            let session_id = raw.trim().to_string();
            if !session_id.is_empty() {
                return Ok(session_id);
            }
        }

        let stat_path = format!("/proc/{}/stat", pid);
        let stat = match std::fs::read_to_string(stat_path) {
            Ok(stat) => stat,
            Err(_) => break,
        };
        let end = match stat.rfind(')') {
            Some(end) => end,
            None => break,
        };
        let rest = stat.get(end + 2..).unwrap_or_default();
        let mut parts = rest.split_whitespace();
        let _state = parts.next();
        let ppid = match parts.next().and_then(|value| value.parse::<u32>().ok()) {
            Some(ppid) if ppid > 1 && ppid != pid => ppid,
            _ => break,
        };
        pid = ppid;
    }

    bail!("could not infer session id from BEAM_SESSION_ID or cli pid markers")
}

pub(crate) fn read_send_content(content: Option<String>) -> Result<String> {
    if let Some(content) = content {
        return Ok(content);
    }
    let mut body = String::new();
    use std::io::Read;
    std::io::stdin().read_to_string(&mut body)?;
    let body = body.trim_end().to_string();
    if body.is_empty() {
        bail!("send content is empty");
    }
    Ok(body)
}

pub(crate) async fn cmd_hook(cli_id: Option<String>, _paths: &BeamPaths) -> Result<()> {
    if cli_id.as_deref().unwrap_or("").is_empty() {
        eprintln!("Usage: beam hook <cliId>");
        std::process::exit(1);
    }

    use std::io::Read;
    let mut sink = String::new();
    let _ = std::io::stdin().read_to_string(&mut sink);
    let payload: serde_json::Value = match serde_json::from_str(&sink) {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };
    let Some(cli_id) = cli_id else {
        return Ok(());
    };
    let Some(parsed) = ask_hook::parse_questions(&cli_id, &payload) else {
        return Ok(());
    };
    let (session_id, chat_id, lark_app_id, root_message_id) =
        match resolve_ask_context(_paths, &payload) {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(error = %err, "beam hook ask context not resolved");
                return Ok(());
            }
        };
    let body = serde_json::json!({
        "sessionId": session_id,
        "chatId": chat_id,
        "larkAppId": lark_app_id,
        "rootMessageId": root_message_id,
        "questions": parsed.questions.iter().map(|q| serde_json::json!({
            "prompt": q.prompt,
            "options": q.options,
            "multiSelect": q.multi_select,
        })).collect::<Vec<_>>(),
        "timeoutMs": 3_600_000u64,
        "approvers": [],
    });
    let result = match post_ask(_paths, &body).await {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };
    let ask_result: beam_core::AskResult = match serde_json::from_value(result) {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };
    match ask_result {
        beam_core::AskResult::Answered { answers, .. } => {
            let directive = ask_hook::format_answer(&cli_id, &answers, &parsed)?;
            if !directive.is_empty() {
                println!("{directive}");
            }
        }
        _ => {
            let directive = ask_hook::passthrough(&cli_id, &payload)?;
            if !directive.is_empty() {
                println!("{directive}");
            }
        }
    }
    Ok(())
}
