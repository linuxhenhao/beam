use std::time::Duration;

use anyhow::{Context, Result, bail};
use qrcode::{QrCode, render::unicode};
use reqwest::Client;
use serde::Deserialize;
use tokio::time::{Instant, sleep};
use url::Url;

const FEISHU_BASE_URL: &str = "https://accounts.feishu.cn";
const LARK_BASE_URL: &str = "https://accounts.larksuite.com";
const REGISTRATION_PATH: &str = "/oauth/v1/app/registration";

#[derive(Debug)]
pub struct SetupCredentials {
    pub app_id: String,
    pub app_secret: String,
    pub user_open_id: Option<String>,
    tenant_brand: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct UserInfo {
    open_id: Option<String>,
    tenant_brand: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct RegistrationResponse {
    device_code: Option<String>,
    verification_uri_complete: Option<String>,
    expires_in: Option<u64>,
    interval: Option<u64>,
    client_id: Option<String>,
    client_secret: Option<String>,
    user_info: Option<UserInfo>,
    error: Option<String>,
    error_description: Option<String>,
}

#[derive(Debug)]
enum PollOutcome {
    Pending,
    SlowDown,
    SwitchToLark,
    Complete(SetupCredentials),
}

pub async fn prompt_credentials() -> Result<SetupCredentials> {
    println!("── 飞书应用建立 ──\n");
    println!("1) 扫码建应用（推荐，一步拿到 AppID/Secret，需要飞书 App 扫码）");
    println!("2) 手动粘 AppID/Secret（已经在开放平台创建好应用了）\n");
    let choice = super::ask_line("选择 [1]: ")?;

    if choice.trim() != "2" {
        match register_app().await {
            Ok(credentials) => {
                println!("\n✅ 应用创建成功");
                println!("   App ID: {}", credentials.app_id);
                if let Some(open_id) = &credentials.user_open_id {
                    println!("   扫码人 open_id: {}（已加入允许用户）", open_id);
                }
                return Ok(credentials);
            }
            Err(err) => {
                if err.to_string().contains("用户取消扫码") {
                    return Err(err);
                }
                println!("\n⚠️  扫码创建失败: {err:#}");
                println!("   已降级到手动输入 AppID/Secret。\n");
            }
        }
    }

    let app_id = super::ask_line("Lark AppID: ")?;
    let app_secret = super::ask_line("Lark AppSecret: ")?;
    if app_id.is_empty() || app_secret.is_empty() {
        bail!("AppID/AppSecret 不能为空");
    }
    Ok(SetupCredentials {
        app_id,
        app_secret,
        user_open_id: None,
        tenant_brand: None,
    })
}

async fn register_app() -> Result<SetupCredentials> {
    let client = Client::new();
    let begin = request_registration(
        &client,
        FEISHU_BASE_URL,
        &[
            ("action", "begin"),
            ("archetype", "PersonalAgent"),
            ("auth_method", "client_secret"),
            ("request_user_info", "open_id"),
        ],
    )
    .await
    .context("无法发起飞书应用注册")?;

    if let Some(error) = begin.error {
        bail!("{}: {}", error, begin.error_description.unwrap_or_default());
    }
    let device_code = begin.device_code.context("注册响应缺少 device_code")?;
    let mut verification_url = Url::parse(
        begin
            .verification_uri_complete
            .as_deref()
            .context("注册响应缺少 verification_uri_complete")?,
    )?;
    verification_url
        .query_pairs_mut()
        .append_pair("from", "sdk")
        .append_pair("source", "node-sdk/beam")
        .append_pair("tp", "sdk");

    let expires_in = begin.expires_in.unwrap_or(600);
    print_qr_code(verification_url.as_str(), expires_in)?;

    let deadline = Instant::now() + Duration::from_secs(expires_in);
    let mut interval = Duration::from_secs(begin.interval.unwrap_or(5));
    let mut base_url = FEISHU_BASE_URL;
    let mut switched_to_lark = false;

    loop {
        if Instant::now() >= deadline {
            bail!("二维码已过期，请重试");
        }
        let poll_params = [("action", "poll"), ("device_code", device_code.as_str())];
        let poll = request_registration(&client, base_url, &poll_params);
        let response = tokio::select! {
            result = poll => result.context("轮询应用注册状态失败")?,
            _ = tokio::signal::ctrl_c() => bail!("用户取消扫码"),
        };

        match classify_poll(response, !switched_to_lark)? {
            PollOutcome::Complete(credentials) => {
                if credentials.tenant_brand.as_deref() == Some("lark") {
                    bail!("检测到 Lark 国际版租户；beam 当前运行链路仅支持飞书 (feishu.cn)");
                }
                return Ok(credentials);
            }
            PollOutcome::Pending => {}
            PollOutcome::SlowDown => {
                interval += Duration::from_secs(5);
                println!("轮询过快，间隔自动调整到 {} 秒。", interval.as_secs());
            }
            PollOutcome::SwitchToLark if !switched_to_lark => {
                base_url = LARK_BASE_URL;
                switched_to_lark = true;
                println!("识别到国际版租户，已切换到 larksuite.com 继续轮询。");
                continue;
            }
            PollOutcome::SwitchToLark => {}
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        tokio::select! {
            _ = sleep(interval.min(remaining)) => {}
            _ = tokio::signal::ctrl_c() => bail!("用户取消扫码"),
        }
    }
}

async fn request_registration(
    client: &Client,
    base_url: &str,
    params: &[(&str, &str)],
) -> Result<RegistrationResponse> {
    let body: String = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(params.iter().copied())
        .finish();
    let response = client
        .post(format!("{base_url}{REGISTRATION_PATH}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await?;
    let status = response.status();
    let body = response
        .json::<RegistrationResponse>()
        .await
        .context("应用注册接口返回了无效 JSON")?;
    if !status.is_success() && body.error.is_none() {
        bail!("应用注册接口返回 HTTP {status}");
    }
    Ok(body)
}

fn classify_poll(response: RegistrationResponse, allow_lark_switch: bool) -> Result<PollOutcome> {
    if response
        .user_info
        .as_ref()
        .and_then(|info| info.tenant_brand.as_deref())
        == Some("lark")
        && allow_lark_switch
    {
        return Ok(PollOutcome::SwitchToLark);
    }

    if let (Some(app_id), Some(app_secret)) = (response.client_id, response.client_secret) {
        let tenant_brand = response
            .user_info
            .as_ref()
            .and_then(|info| info.tenant_brand.clone());
        let user_open_id = response
            .user_info
            .and_then(|info| info.open_id)
            .filter(|value| value.starts_with("ou_"));
        return Ok(PollOutcome::Complete(SetupCredentials {
            app_id,
            app_secret,
            user_open_id,
            tenant_brand,
        }));
    }

    match response.error.as_deref() {
        None | Some("authorization_pending") => Ok(PollOutcome::Pending),
        Some("slow_down") => Ok(PollOutcome::SlowDown),
        Some("access_denied") => bail!("用户拒绝了应用创建授权"),
        Some("expired_token") => bail!("二维码已过期，请重试"),
        Some(error) => bail!(
            "{}: {}",
            error,
            response.error_description.unwrap_or_default()
        ),
    }
}

fn print_qr_code(url: &str, expires_in: u64) -> Result<()> {
    let code = QrCode::new(url.as_bytes()).context("无法生成二维码")?;
    let image = code.render::<unicode::Dense1x2>().quiet_zone(true).build();
    let minutes = ((expires_in + 30) / 60).max(1);
    eprintln!("\n请用飞书 App 扫码完成应用创建：\n\n{image}");
    eprintln!("\n二维码有效期约 {minutes} 分钟。也可在浏览器打开：\n  {url}\n");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(raw: &str) -> RegistrationResponse {
        serde_json::from_str(raw).unwrap()
    }

    #[test]
    fn poll_success_returns_credentials_and_scanner_open_id() {
        let outcome = classify_poll(
            response(
                r#"{
                "client_id":"cli_test",
                "client_secret":"secret",
                "user_info":{"tenant_brand":"feishu","open_id":"ou_owner"}
            }"#,
            ),
            true,
        )
        .unwrap();
        let PollOutcome::Complete(credentials) = outcome else {
            panic!("expected completed registration");
        };
        assert_eq!(credentials.app_id, "cli_test");
        assert_eq!(credentials.app_secret, "secret");
        assert_eq!(credentials.user_open_id.as_deref(), Some("ou_owner"));
    }

    #[test]
    fn lark_tenant_switches_domain_before_accepting_credentials() {
        let outcome = classify_poll(
            response(r#"{"user_info":{"tenant_brand":"lark","open_id":"ou_owner"}}"#),
            true,
        )
        .unwrap();
        assert!(matches!(outcome, PollOutcome::SwitchToLark));
    }

    #[test]
    fn lark_credentials_complete_after_domain_switch() {
        let outcome = classify_poll(
            response(
                r#"{
                    "client_id":"cli_lark",
                    "client_secret":"secret",
                    "user_info":{"tenant_brand":"lark","open_id":"ou_owner"}
                }"#,
            ),
            false,
        )
        .unwrap();
        let PollOutcome::Complete(credentials) = outcome else {
            panic!("expected completed registration");
        };
        assert_eq!(credentials.tenant_brand.as_deref(), Some("lark"));
    }

    #[test]
    fn pending_and_slow_down_are_retryable() {
        assert!(matches!(
            classify_poll(response(r#"{"error":"authorization_pending"}"#), true).unwrap(),
            PollOutcome::Pending
        ));
        assert!(matches!(
            classify_poll(response(r#"{"error":"slow_down"}"#), true).unwrap(),
            PollOutcome::SlowDown
        ));
    }

    #[test]
    fn denied_and_expired_are_terminal() {
        assert!(
            classify_poll(response(r#"{"error":"access_denied"}"#), true)
                .unwrap_err()
                .to_string()
                .contains("拒绝")
        );
        assert!(
            classify_poll(response(r#"{"error":"expired_token"}"#), true)
                .unwrap_err()
                .to_string()
                .contains("过期")
        );
    }
}
