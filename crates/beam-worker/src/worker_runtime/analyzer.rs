use super::*;

const SCREEN_ANALYZER_SYSTEM_PROMPT: &str = r#"You analyze a terminal screen from an AI coding CLI.

Decide whether the CLI is waiting for user interaction, such as a menu choice, permission prompt, confirmation, text input, or multi-select. If the CLI is still working, printing output, or only showing status text, set needsInteraction=false.

Return only JSON:
{
  "needsInteraction": boolean,
  "description": string,
  "options": [{"label": string, "text": string, "selected": boolean}],
  "multiSelect": boolean,
  "toggleKey": string|null,
  "confirmKey": string|null,
  "checkAgainWhen": "content_changed"|"after_5s"|"after_10s"|"not_needed"
}"#;

#[derive(Debug, Clone, Default)]
pub(crate) struct AnalyzerRuntime {
    pub(crate) last_snapshot: String,
    pub(crate) stable_count: u32,
    pub(crate) last_analyzed_snapshot: String,
    pub(crate) waiting_for_content_change: bool,
    pub(crate) cooldown_until_ms: u64,
    pub(crate) is_analyzing: bool,
    pub(crate) prompt_active: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AnalyzerChatResponse {
    choices: Vec<AnalyzerChoice>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AnalyzerChoice {
    message: AnalyzerMessage,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AnalyzerMessage {
    content: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AnalyzerParsedResponse {
    #[serde(rename = "needsInteraction")]
    needs_interaction: Option<bool>,
    description: Option<String>,
    options: Option<Vec<AnalyzerParsedOption>>,
    #[serde(rename = "multiSelect")]
    multi_select: Option<bool>,
    #[serde(rename = "toggleKey")]
    toggle_key: Option<String>,
    #[serde(rename = "confirmKey")]
    confirm_key: Option<String>,
    #[serde(rename = "checkAgainWhen")]
    check_again_when: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AnalyzerParsedOption {
    label: Option<String>,
    text: Option<String>,
    selected: Option<bool>,
    #[serde(rename = "type")]
    option_type: Option<String>,
    index: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AnalyzerResult {
    pub(crate) needs_interaction: bool,
    pub(crate) description: Option<String>,
    pub(crate) options: Vec<TuiPromptOption>,
    pub(crate) multi_select: bool,
    pub(crate) check_again_when: String,
}

pub(crate) fn screen_analyzer_enabled(cfg: &ScreenAnalyzerConfig) -> bool {
    cfg.enabled
        && !cfg.base_url.trim().is_empty()
        && !cfg.api_key.trim().is_empty()
        && !cfg.model.trim().is_empty()
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn parse_screen_analyzer_response(content: &str) -> AnalyzerResult {
    let json_str = content
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();
    let parsed = serde_json::from_str::<AnalyzerParsedResponse>(json_str).ok();
    let check_again_when = parsed
        .as_ref()
        .and_then(|parsed| parsed.check_again_when.clone())
        .filter(|value| {
            matches!(
                value.as_str(),
                "content_changed" | "after_5s" | "after_10s" | "not_needed"
            )
        })
        .unwrap_or_else(|| "content_changed".to_string());
    let toggle_key = parsed
        .as_ref()
        .and_then(|parsed| parsed.toggle_key.clone())
        .unwrap_or_else(|| "Space".to_string());
    let confirm_key = parsed
        .as_ref()
        .and_then(|parsed| parsed.confirm_key.clone())
        .unwrap_or_else(|| "Enter".to_string());
    let options = parsed
        .as_ref()
        .and_then(|parsed| parsed.options.as_ref())
        .map(|options| build_tui_prompt_options(options, &toggle_key, &confirm_key))
        .unwrap_or_default();
    AnalyzerResult {
        needs_interaction: parsed
            .as_ref()
            .and_then(|parsed| parsed.needs_interaction)
            .unwrap_or(false),
        description: parsed
            .as_ref()
            .and_then(|parsed| parsed.description.clone()),
        options,
        multi_select: parsed
            .as_ref()
            .and_then(|parsed| parsed.multi_select)
            .unwrap_or(false),
        check_again_when,
    }
}

pub(crate) fn apply_screen_analyzer_result(
    runtime: &mut AnalyzerRuntime,
    check_again_when: &str,
    now_ms: u64,
) {
    match check_again_when {
        "after_5s" => {
            runtime.cooldown_until_ms = now_ms + 5_000;
            runtime.waiting_for_content_change = false;
        }
        "after_10s" => {
            runtime.cooldown_until_ms = now_ms + 10_000;
            runtime.waiting_for_content_change = false;
        }
        "not_needed" | "content_changed" => {
            runtime.waiting_for_content_change = true;
            runtime.cooldown_until_ms = 0;
        }
        _ => {
            runtime.waiting_for_content_change = true;
            runtime.cooldown_until_ms = 0;
        }
    }
}

pub(crate) fn build_tui_prompt_options(
    options: &[AnalyzerParsedOption],
    toggle_key: &str,
    confirm_key: &str,
) -> Vec<TuiPromptOption> {
    options
        .iter()
        .enumerate()
        .map(|(i, option)| {
            let index = option.index.unwrap_or(i);
            let option_type = option
                .option_type
                .clone()
                .filter(|kind| matches!(kind.as_str(), "select" | "toggle" | "confirm" | "input"))
                .unwrap_or_else(|| "select".to_string());
            let mut keys = Vec::new();
            for _ in 0..index {
                keys.push("Down".to_string());
            }
            match option_type.as_str() {
                "toggle" => {
                    keys.push(toggle_key.to_string());
                    for _ in 0..index {
                        keys.push("Up".to_string());
                    }
                }
                "select" | "confirm" | "input" => {
                    keys.push(confirm_key.to_string());
                }
                _ => {}
            }
            TuiPromptOption {
                label: option.label.clone().or_else(|| Some((i + 1).to_string())),
                text: option
                    .text
                    .clone()
                    .unwrap_or_default()
                    .replace('\n', " ")
                    .trim()
                    .to_string(),
                selected: option.selected.unwrap_or(false),
                option_type: Some(option_type),
                keys,
            }
        })
        .collect()
}

pub(crate) async fn call_screen_analyzer(
    client: &Client,
    cfg: &ScreenAnalyzerConfig,
    snapshot: &str,
) -> Result<AnalyzerResult> {
    let url = format!("{}/chat/completions", cfg.base_url.trim_end_matches('/'));
    let mut body = serde_json::json!({
        "model": cfg.model,
        "messages": [
            { "role": "system", "content": SCREEN_ANALYZER_SYSTEM_PROMPT },
            { "role": "user", "content": snapshot },
        ],
        "temperature": 0,
        "max_tokens": 2048,
    });
    if let Some(map) = body.as_object_mut() {
        for (key, value) in &cfg.extra_body {
            map.insert(key.clone(), value.clone());
        }
    }

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json".parse()?);
    headers.insert("authorization", format!("Bearer {}", cfg.api_key).parse()?);
    for (key, value) in &cfg.extra_headers {
        headers.insert(key.parse::<reqwest::header::HeaderName>()?, value.parse()?);
    }

    let response = client
        .post(url)
        .headers(headers)
        .json(&body)
        .timeout(Duration::from_secs(15))
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        anyhow::bail!(
            "screen analyzer API {}: {}",
            status,
            text.chars().take(200).collect::<String>()
        );
    }
    let payload = response.json::<AnalyzerChatResponse>().await?;
    let content = payload
        .choices
        .first()
        .map(|choice| choice.message.content.clone())
        .unwrap_or_default();
    Ok(parse_screen_analyzer_response(&content))
}
