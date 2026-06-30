mod adapter;
mod adapters;
mod backend;

use std::path::Path;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use ab_glyph::{Font, FontVec, PxScale, ScaleFont, point};
use anyhow::{Context, Result};
use beam_core::{
    BeamPaths, CliUsageLimitKind, CliUsageLimitState, DEFAULT_TERMINAL_COLS, DEFAULT_TERMINAL_ROWS,
    DaemonToWorker, DisplayMode, InitConfig, ScreenAnalyzerConfig, ScreenStatus, TermActionKey,
    TuiPromptOption, WorkerToDaemon,
};
use image::{ColorType, ImageBuffer, ImageEncoder, Rgba, codecs::png::PngEncoder};
use reqwest::multipart::{Form, Part};
use reqwest::{Client, header::HeaderMap};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, RwLock, broadcast};
use tracing::{info, warn};
use unicode_width::UnicodeWidthChar;
use uuid::Uuid;

use crate::adapter::CliAdapter;
use crate::backend::{SessionBackend, SpawnOpts, ZellijBackend, ZellijObserveBackend};

fn render_screen_for_display_mode(screen: &str, mode: DisplayMode) -> String {
    match mode {
        DisplayMode::Hidden => "[screen hidden]".to_string(),
        DisplayMode::Screenshot => strip_ansi(screen).replace('\r', ""),
    }
}

const SCREEN_ANALYZER_SYSTEM_PROMPT: &str = "You are a terminal screen analyzer. Determine whether the CLI is showing a blocking interactive prompt. Return only JSON with fields needsInteraction, description, options, multiSelect, toggleKey, confirmKey, checkAgainWhen. checkAgainWhen must be one of content_changed, after_5s, after_10s, not_needed.";

#[derive(Debug, Clone, Default)]
struct AnalyzerRuntime {
    last_snapshot: String,
    stable_count: u32,
    last_analyzed_snapshot: String,
    waiting_for_content_change: bool,
    cooldown_until_ms: u64,
    is_analyzing: bool,
    prompt_active: bool,
}

#[derive(Debug, Deserialize)]
struct AnalyzerChatResponse {
    choices: Vec<AnalyzerChoice>,
}

#[derive(Debug, Deserialize)]
struct AnalyzerChoice {
    message: AnalyzerMessage,
}

#[derive(Debug, Deserialize)]
struct AnalyzerMessage {
    content: String,
}

#[derive(Debug, Deserialize)]
struct AnalyzerParsedResponse {
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
struct AnalyzerParsedOption {
    label: Option<String>,
    text: Option<String>,
    selected: Option<bool>,
    #[serde(rename = "type")]
    option_type: Option<String>,
    index: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AnalyzerResult {
    needs_interaction: bool,
    description: Option<String>,
    options: Vec<TuiPromptOption>,
    multi_select: bool,
    check_again_when: String,
}

fn screen_analyzer_enabled(cfg: &ScreenAnalyzerConfig) -> bool {
    cfg.enabled
        && !cfg.base_url.trim().is_empty()
        && !cfg.api_key.trim().is_empty()
        && !cfg.model.trim().is_empty()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn parse_screen_analyzer_response(content: &str) -> AnalyzerResult {
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

fn apply_screen_analyzer_result(
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

fn build_tui_prompt_options(
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

async fn call_screen_analyzer(
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

fn term_action_keys(key: TermActionKey) -> Vec<String> {
    match key {
        TermActionKey::Esc => vec!["Escape".to_string()],
        TermActionKey::CtrlC => vec!["C-c".to_string()],
        TermActionKey::Tab => vec!["Tab".to_string()],
        TermActionKey::Enter => vec!["Enter".to_string()],
        TermActionKey::Space => vec!["Space".to_string()],
        TermActionKey::Up => vec!["Up".to_string()],
        TermActionKey::Down => vec!["Down".to_string()],
        TermActionKey::Left => vec!["Left".to_string()],
        TermActionKey::Right => vec!["Right".to_string()],
        TermActionKey::HalfPageUp => vec!["PageUp".to_string()],
        TermActionKey::HalfPageDown => vec!["PageDown".to_string()],
    }
}

fn shell_quote(input: &str) -> String {
    if input.is_empty() {
        return "''".to_string();
    }
    if !input.bytes().any(|b| {
        matches!(
            b,
            b' ' | b'\t'
                | b'\n'
                | b'\''
                | b'"'
                | b'\\'
                | b'$'
                | b'`'
                | b'!'
                | b'&'
                | b'|'
                | b';'
                | b'<'
                | b'>'
                | b'('
                | b')'
                | b'['
                | b']'
                | b'{'
                | b'}'
                | b'*'
                | b'?'
                | b'#'
        )
    }) {
        return input.to_string();
    }
    format!("'{}'", input.replace('\'', "'\"'\"'"))
}

fn has_pattern(text: &str, patterns: &[&str]) -> bool {
    let lower = text.to_ascii_lowercase();
    patterns.iter().any(|pattern| lower.contains(pattern))
}

#[derive(Debug, serde::Deserialize)]
struct LarkTokenResponse {
    code: i32,
    msg: Option<String>,
    tenant_access_token: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct LarkImageUploadResponse {
    code: i32,
    msg: Option<String>,
    image_key: Option<String>,
    data: Option<LarkImageUploadData>,
}

#[derive(Debug, serde::Deserialize)]
struct LarkImageUploadData {
    image_key: Option<String>,
}

fn parse_retry_time(text: &str, now_ms: u64) -> Option<(u64, String)> {
    let lower = text.to_ascii_lowercase();
    let marker = ["try again at ", "resets at ", "reset at ", "resets "]
        .into_iter()
        .find_map(|needle| lower.find(needle).map(|idx| (idx, needle)))?;
    let start = marker.0 + marker.1.len();
    let tail = text.get(start..)?.trim_start();
    let mut chars = tail.chars().peekable();
    let mut hour = String::new();
    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_digit() {
            hour.push(ch);
            chars.next();
        } else {
            break;
        }
    }
    if hour.is_empty() {
        return None;
    }
    let mut minute = String::new();
    if chars.peek() == Some(&':') {
        chars.next();
        while let Some(ch) = chars.peek().copied() {
            if ch.is_ascii_digit() {
                minute.push(ch);
                chars.next();
            } else {
                break;
            }
        }
    }
    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_whitespace() {
            chars.next();
        } else {
            break;
        }
    }
    let mut meridiem = String::new();
    while let Some(ch) = chars.peek().copied() {
        if matches!(ch.to_ascii_lowercase(), 'a' | 'p' | 'm' | '.') {
            meridiem.push(ch);
            chars.next();
        } else {
            break;
        }
    }
    let meridiem = meridiem.to_ascii_lowercase().replace('.', "");
    if meridiem != "am" && meridiem != "pm" {
        return None;
    }
    let raw_hour = hour.parse::<u32>().ok()?;
    let minute = if minute.is_empty() {
        0
    } else {
        minute.parse::<u32>().ok()?
    };
    if !(1..=12).contains(&raw_hour) || minute > 59 {
        return None;
    }
    let now = chrono::DateTime::<chrono::Utc>::from(
        SystemTime::UNIX_EPOCH + Duration::from_millis(now_ms),
    );
    let mut hour24 = raw_hour % 12;
    if meridiem == "pm" {
        hour24 += 12;
    }
    let mut retry_at = now
        .date_naive()
        .and_hms_opt(hour24, minute, 0)?
        .and_utc()
        .timestamp_millis() as u64;
    if retry_at < now_ms && hour24 < 12 {
        retry_at += 24 * 60 * 60 * 1000;
    }
    let label = tail
        .split_whitespace()
        .take(2)
        .collect::<Vec<_>>()
        .join(" ")
        .trim_end_matches(|ch: char| ch == '.' || ch == ',' || ch == ';')
        .to_string();
    Some((retry_at, label))
}

fn detect_cli_usage_limit(text: &str, now_ms: u64) -> Option<CliUsageLimitState> {
    if !text.to_ascii_lowercase().contains("again") && !text.to_ascii_lowercase().contains("reset")
    {
        return None;
    }
    let (retry_at_ms, retry_label) = parse_retry_time(text, now_ms)?;
    let kind = if has_pattern(
        text,
        &["rate limit reached", "rate limit exceeded", "rate limited"],
    ) {
        CliUsageLimitKind::Rate
    } else if has_pattern(
        text,
        &[
            "hit your usage limit",
            "hit usage limit",
            "usage limit reached",
            "usage limit exceeded",
            "quota reached",
            "quota exceeded",
            "limit reached",
            "limit exceeded",
            "reached your usage limit",
            "exceeded your usage limit",
        ],
    ) {
        CliUsageLimitKind::Usage
    } else {
        return None;
    };
    Some(CliUsageLimitState {
        limited: true,
        kind,
        retry_at_ms,
        retry_label,
        retry_ready: now_ms >= retry_at_ms,
    })
}

fn usage_limit_state_key(state: &CliUsageLimitState) -> String {
    format!(
        "{:?}:{}:{}",
        state.kind, state.retry_at_ms, state.retry_label
    )
}

static PRIMARY_FONT: LazyLock<StdMutex<Option<FontVec>>> = LazyLock::new(|| StdMutex::new(None));
static CJK_FONT: LazyLock<StdMutex<Option<FontVec>>> = LazyLock::new(|| StdMutex::new(None));
const FONT_SIZE: f32 = 14.0;
const CELL_W: f32 = 8.4;
const CELL_H: f32 = 18.0;
const PADDING: u32 = 12;
const BG_COLOR: Rgba<u8> = Rgba([26, 27, 38, 255]);
const FG_COLOR: Rgba<u8> = Rgba([169, 177, 214, 255]);

fn home_font_dir() -> Option<std::path::PathBuf> {
    std::env::var("HOME")
        .ok()
        .map(|h| std::path::PathBuf::from(h).join(".beam").join("fonts"))
}

fn load_font_files() {
    let mut primary = PRIMARY_FONT.lock().unwrap();
    if primary.is_some() {
        return;
    }

    let search_paths: Vec<std::path::PathBuf> = {
        let mut paths = Vec::new();
        if let Some(d) = home_font_dir() {
            paths.push(d.join("JetBrainsMono-Regular.ttf"));
            paths.push(d.join("DejaVuSansMono.ttf"));
            paths.push(d.join("NotoSansMonoCJKsc-Regular.otf"));
        }
        paths.push("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf".into());
        paths.push("/usr/share/fonts/dejavu/DejaVuSansMono.ttf".into());
        paths.push("/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf".into());
        paths.push("/usr/share/fonts/liberation/LiberationMono-Regular.ttf".into());
        paths.push("/usr/share/fonts/truetype/jetbrains-mono/JetBrainsMono-Regular.ttf".into());
        paths
    };

    for path in &search_paths {
        if let Ok(data) = std::fs::read(path) {
            if let Ok(font) = FontVec::try_from_vec(data) {
                *primary = Some(font);
                break;
            }
        }
    }

    let cjk_search: Vec<std::path::PathBuf> = {
        let mut paths = Vec::new();
        if let Some(d) = home_font_dir() {
            paths.push(d.join("NotoSansMonoCJKsc-Regular.otf"));
        }
        paths.push("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc".into());
        paths.push("/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc".into());
        paths.push("/usr/share/fonts/google-noto-cjk/NotoSansCJK-Regular.ttc".into());
        paths
    };

    let mut cjk = CJK_FONT.lock().unwrap();
    for path in &cjk_search {
        if let Ok(data) = std::fs::read(path) {
            if let Ok(font) = FontVec::try_from_vec(data) {
                *cjk = Some(font);
                break;
            }
        }
    }
}

fn is_fullwidth(ch: char) -> bool {
    matches!(UnicodeWidthChar::width(ch), Some(2))
}

fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            if chars.peek() == Some(&'[') {
                chars.next();
                while let Some(&c) = chars.peek() {
                    if c.is_ascii_alphanumeric() || c == ';' {
                        chars.next();
                        if c.is_ascii_alphabetic() {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            } else if chars.peek() == Some(&']') {
                chars.next();
                while let Some(&c) = chars.peek() {
                    chars.next();
                    if c == '\x07' || (c == '\x1b' && chars.peek() == Some(&'\\')) {
                        if c == '\x1b' {
                            chars.next();
                        }
                        break;
                    }
                }
            }
        } else {
            out.push(ch);
        }
    }
    out
}

fn find_glyph_font<'a>(
    ch: char,
    primary: &'a FontVec,
    cjk: Option<&'a FontVec>,
) -> (&'a FontVec, f32) {
    let primary_id = primary.glyph_id(ch);
    if primary_id.0 != 0 {
        return (primary, 1.0);
    }
    if let Some(cjk_font) = cjk {
        let cjk_id = cjk_font.glyph_id(ch);
        if cjk_id.0 != 0 {
            return (cjk_font, if is_fullwidth(ch) { 2.0 } else { 1.0 });
        }
    }
    (primary, 1.0)
}

fn render_text_screenshot_png(screen_raw: &str) -> Result<Vec<u8>> {
    load_font_files();

    let screen = strip_ansi(screen_raw);
    let lines: Vec<&str> = screen.lines().collect();
    let rows = lines.len().max(1);

    let primary_guard = PRIMARY_FONT.lock().unwrap();
    let cjk_guard = CJK_FONT.lock().unwrap();
    let primary = primary_guard.as_ref();
    let cjk = cjk_guard.as_ref();

    let primary = match primary {
        Some(f) => f,
        None => return fallback_bitmap_png(&screen),
    };

    let scale = PxScale::from(FONT_SIZE);
    let scaled = primary.as_scaled(scale);
    let baseline_offset =
        ((CELL_H - (scaled.ascent() + scaled.descent())).max(0.0) / 2.0) + scaled.ascent();

    let cols = lines
        .iter()
        .map(|line| {
            line.chars()
                .map(|ch| if is_fullwidth(ch) { 2u32 } else { 1u32 })
                .sum::<u32>()
        })
        .max()
        .unwrap_or(1)
        .max(1);

    let width = ((cols as f32 * CELL_W).ceil() as u32 + PADDING * 2).max(64);
    let height = ((rows as f32 * CELL_H).ceil() as u32 + PADDING * 2).max(32);

    let mut image = ImageBuffer::from_pixel(width, height, BG_COLOR);

    for (row, line) in lines.iter().enumerate() {
        let mut col_cells: u32 = 0;
        for ch in line.chars() {
            let (font, char_width) = find_glyph_font(ch, primary, cjk);
            let scaled = font.as_scaled(scale);
            let x = PADDING as f32 + col_cells as f32 * CELL_W;
            let y = PADDING as f32 + row as f32 * CELL_H;

            if ch != ' ' {
                let cell_px = char_width * CELL_W;
                let advance = scaled.h_advance(scaled.glyph_id(ch));
                let glyph_x = x + ((cell_px - advance).max(0.0) / 2.0);
                let baseline = y + baseline_offset;
                let mut glyph = scaled.scaled_glyph(ch);
                glyph.position = point(glyph_x, baseline);
                if let Some(outline) = font.outline_glyph(glyph) {
                    let bounds = outline.px_bounds();
                    outline.draw(|gx, gy, cv| {
                        let px = bounds.min.x as i32 + gx as i32;
                        let py = bounds.min.y as i32 + gy as i32;
                        if px >= 0
                            && py >= 0
                            && (px as u32) < width
                            && (py as u32) < height
                            && cv > 0.0
                        {
                            let alpha = (cv * 255.0).min(255.0) as u8;
                            if alpha == 255 {
                                image.put_pixel(px as u32, py as u32, FG_COLOR);
                            } else {
                                let existing = image.get_pixel(px as u32, py as u32);
                                let blended = blend_alpha(*existing, FG_COLOR, alpha);
                                image.put_pixel(px as u32, py as u32, blended);
                            }
                        }
                    });
                }
            }

            col_cells += char_width.ceil() as u32;
        }
    }

    let mut out = Vec::new();
    let encoder = PngEncoder::new(&mut out);
    encoder.write_image(image.as_raw(), width, height, ColorType::Rgba8.into())?;
    Ok(out)
}

fn blend_alpha(bg: Rgba<u8>, fg: Rgba<u8>, alpha: u8) -> Rgba<u8> {
    let a = alpha as f32 / 255.0;
    let r = (fg.0[0] as f32 * a + bg.0[0] as f32 * (1.0 - a)) as u8;
    let g = (fg.0[1] as f32 * a + bg.0[1] as f32 * (1.0 - a)) as u8;
    let b = (fg.0[2] as f32 * a + bg.0[2] as f32 * (1.0 - a)) as u8;
    Rgba([r, g, b, 255])
}

fn fallback_bitmap_png(screen: &str) -> Result<Vec<u8>> {
    use font8x8::UnicodeFonts;

    let lines: Vec<&str> = screen.lines().collect();
    let rows = lines.len().max(1);
    let cols = lines
        .iter()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(1)
        .max(1);
    let scale = 2u32;
    let glyph_w = 8u32 * scale;
    let glyph_h = 8u32 * scale;
    let width = (cols as u32 * glyph_w + PADDING * 2).max(64);
    let height = (rows as u32 * glyph_h + PADDING * 2).max(32);
    let bg = Rgba([15, 23, 42, 255]);
    let fg = Rgba([226, 232, 240, 255]);
    let mut image = ImageBuffer::from_pixel(width, height, bg);

    for (row, line) in lines.iter().enumerate() {
        for (col, ch) in line.chars().take(cols as usize).enumerate() {
            let glyph = font8x8::BASIC_FONTS
                .get(ch)
                .or_else(|| font8x8::BASIC_FONTS.get('?'))
                .unwrap_or([0; 8]);
            for (gy, bits) in glyph.iter().enumerate() {
                for gx in 0..8 {
                    if (bits >> gx) & 1 == 0 {
                        continue;
                    }
                    for sy in 0..scale {
                        for sx in 0..scale {
                            let x = PADDING + col as u32 * glyph_w + (7 - gx) as u32 * scale + sx;
                            let y = PADDING + row as u32 * glyph_h + gy as u32 * scale + sy;
                            if x < width && y < height {
                                image.put_pixel(x, y, fg);
                            }
                        }
                    }
                }
            }
        }
    }

    let mut out = Vec::new();
    let encoder = PngEncoder::new(&mut out);
    encoder.write_image(image.as_raw(), width, height, ColorType::Rgba8.into())?;
    Ok(out)
}

fn lark_base_url() -> &'static str {
    "https://open.feishu.cn/open-apis"
}

async fn lark_tenant_token(app_id: &str, secret: &str) -> Result<String> {
    let body = reqwest::Client::new()
        .post(format!(
            "{}/auth/v3/tenant_access_token/internal",
            lark_base_url()
        ))
        .json(&serde_json::json!({
            "app_id": app_id,
            "app_secret": secret,
        }))
        .send()
        .await?
        .json::<LarkTokenResponse>()
        .await?;
    if body.code != 0 {
        anyhow::bail!(
            "lark tenant_access_token failed: {}",
            body.msg.unwrap_or_else(|| "unknown error".to_string())
        );
    }
    body.tenant_access_token
        .context("lark tenant_access_token missing")
}

async fn upload_image_buffer(app_id: &str, secret: &str, image: Vec<u8>) -> Result<String> {
    let token = lark_tenant_token(app_id, secret).await?;
    let form = Form::new().text("image_type", "message").part(
        "image",
        Part::bytes(image)
            .file_name("screen.png")
            .mime_str("image/png")?,
    );
    let body = reqwest::Client::new()
        .post(format!("{}/im/v1/images", lark_base_url()))
        .bearer_auth(token)
        .multipart(form)
        .send()
        .await?
        .json::<LarkImageUploadResponse>()
        .await?;
    if body.code != 0 {
        anyhow::bail!(
            "lark image upload failed: {}",
            body.msg.unwrap_or_else(|| "unknown error".to_string())
        );
    }
    body.image_key
        .or_else(|| body.data.and_then(|data| data.image_key))
        .context("lark image upload missing image_key")
}

async fn maybe_send_screenshot_upload(
    stdout: &Arc<Mutex<tokio::io::Stdout>>,
    app_id: &str,
    app_secret: &str,
    screen: &str,
    status: ScreenStatus,
    usage_limit: Option<CliUsageLimitState>,
    last_uploaded_hash: &Arc<Mutex<Option<String>>>,
) {
    if app_id == "local" || app_secret.is_empty() {
        return;
    }
    let hash = format!("{:x}", Sha256::digest(strip_ansi(screen).as_bytes()));
    {
        let guard = last_uploaded_hash.lock().await;
        if guard.as_deref() == Some(hash.as_str()) {
            return;
        }
    }
    let png = match render_text_screenshot_png(screen) {
        Ok(png) => png,
        Err(err) => {
            warn!("failed to render terminal screenshot: {err:#}");
            return;
        }
    };
    let image_key = match upload_image_buffer(app_id, app_secret, png).await {
        Ok(image_key) => image_key,
        Err(err) => {
            warn!("failed to upload terminal screenshot: {err:#}");
            return;
        }
    };
    let _ = send_message(
        stdout,
        &WorkerToDaemon::ScreenshotUploaded {
            image_key,
            status,
            usage_limit,
        },
    )
    .await;
    *last_uploaded_hash.lock().await = Some(hash);
}

#[derive(Debug, Default)]
struct UsageLimitTracker {
    turn_seq: u64,
    detected_turn: Option<u64>,
    suppressed_retry_ready_key: Option<String>,
}

impl UsageLimitTracker {
    fn begin_turn(&mut self, snapshot: &str, now_ms: u64) -> u64 {
        self.turn_seq += 1;
        self.detected_turn = None;
        self.suppressed_retry_ready_key = detect_cli_usage_limit(snapshot, now_ms)
            .filter(|state| state.retry_ready)
            .map(|state| usage_limit_state_key(&state));
        self.turn_seq
    }

    fn classify(
        &mut self,
        content: &str,
        status: ScreenStatus,
        now_ms: u64,
    ) -> (ScreenStatus, Option<CliUsageLimitState>) {
        let Some(detected) = detect_cli_usage_limit(content, now_ms) else {
            return (status, None);
        };
        let key = usage_limit_state_key(&detected);
        if detected.retry_ready && self.suppressed_retry_ready_key.as_deref() == Some(key.as_str())
        {
            return (status, None);
        }
        self.suppressed_retry_ready_key = None;
        self.detected_turn = Some(self.turn_seq);
        (ScreenStatus::Limited, Some(detected))
    }
}

async fn prepare_wrapper(init: &InitConfig, paths: &BeamPaths) -> Result<std::path::PathBuf> {
    tokio::fs::create_dir_all(paths.run_dir()).await?;
    let wrapper = paths.worker_wrapper_sh(&init.session_id);
    let exe_path = std::env::current_exe()
        .ok()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "beam".to_string());
    let content = format!(
        "#!/bin/sh\ncd {cwd}\nexport BEAM_SESSION_ID={sid}\nexport BEAM_HOME={home}\nexport BEAM_BIN={exe}\nif [ -n \"$PATH\" ]; then\n  export PATH={bindir}:$PATH\nelse\n  export PATH={bindir}\nfi\nexec \"$@\"\n",
        cwd = shell_quote(&init.working_dir),
        sid = shell_quote(&init.session_id),
        home = shell_quote(&paths.root().display().to_string()),
        exe = shell_quote(&exe_path),
        bindir = shell_quote(
            &std::env::current_exe()
                .ok()
                .and_then(|p| p.parent().map(|v| v.display().to_string()))
                .unwrap_or_default()
        ),
    );
    tokio::fs::write(&wrapper, content).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o755);
        tokio::fs::set_permissions(&wrapper, perms).await?;
    }
    Ok(wrapper)
}

async fn send_message(stdout: &Arc<Mutex<tokio::io::Stdout>>, msg: &WorkerToDaemon) -> Result<()> {
    let mut out = stdout.lock().await;
    out.write_all(serde_json::to_string(msg)?.as_bytes())
        .await?;
    out.write_all(b"\n").await?;
    out.flush().await?;
    Ok(())
}

async fn handle_tui_keys(
    backend: &Arc<Mutex<Box<dyn SessionBackend>>>,
    analyzer_runtime: &Arc<RwLock<AnalyzerRuntime>>,
    keys: &[String],
    is_final: bool,
) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    let guard = backend.lock().await;
    for key in keys {
        guard.send_special_keys(std::slice::from_ref(key)).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drop(guard);
    if is_final {
        analyzer_runtime.write().await.prompt_active = false;
    }
    Ok(())
}

async fn handle_tui_text_input(
    backend: &Arc<Mutex<Box<dyn SessionBackend>>>,
    adapter: &Arc<Mutex<CliAdapter>>,
    analyzer_runtime: &Arc<RwLock<AnalyzerRuntime>>,
    keys: &[String],
    text: &str,
) -> Result<()> {
    let nav_keys = if keys.last().map(String::as_str) == Some("Enter") {
        &keys[..keys.len().saturating_sub(1)]
    } else {
        keys
    };
    if !nav_keys.is_empty() {
        let guard = backend.lock().await;
        for key in nav_keys {
            guard.send_special_keys(std::slice::from_ref(key)).await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    analyzer_runtime.write().await.prompt_active = false;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let guard = backend.lock().await;
    let _ = adapter
        .lock()
        .await
        .write_input(guard.as_ref(), text)
        .await?;
    Ok(())
}

async fn handle_tui_prompt_override(
    stdout: &Arc<Mutex<tokio::io::Stdout>>,
    analyzer_runtime: &Arc<RwLock<AnalyzerRuntime>>,
) {
    let was_active = {
        let mut runtime = analyzer_runtime.write().await;
        if runtime.prompt_active {
            runtime.prompt_active = false;
            true
        } else {
            false
        }
    };
    if was_active {
        let _ = send_message(
            stdout,
            &WorkerToDaemon::TuiPromptResolved {
                selected_text: Some("user-override".to_string()),
            },
        )
        .await;
    }
}

pub async fn run(init: InitConfig) -> Result<()> {
    let stdout = Arc::new(Mutex::new(tokio::io::stdout()));
    let session_name = format!("beam-{}", &init.session_id[..8.min(init.session_id.len())]);
    let paths = BeamPaths::discover()?;
    let adapter = Arc::new(Mutex::new(CliAdapter::from_init(&init)?));
    let wrapper = if init.resume || init.adopted_from.is_some() {
        None
    } else {
        Some(prepare_wrapper(&init, &paths).await?)
    };
    let (mut backend_impl, attach_context): (Box<dyn SessionBackend>, &'static str) =
        if let Some(adopted) = init.adopted_from.as_ref() {
            if let Some(pane_id) = adopted.zellij_pane_id.clone() {
                let session = adopted.zellij_session.clone().unwrap_or_else(|| {
                    format!("beam-{}", &init.session_id[..8.min(init.session_id.len())])
                });
                let observe = ZellijObserveBackend::new(
                    session,
                    pane_id,
                    u32::try_from(adopted.original_cli_pid).ok(),
                );
                (Box::new(observe), "observe")
            } else {
                let zellij = ZellijBackend::new(session_name.clone());
                (Box::new(zellij), "spawn")
            }
        } else {
            let zellij = ZellijBackend::new(session_name.clone());
            (Box::new(zellij), "spawn")
        };
    let spawn_spec = adapter.lock().await.build_spawn_spec(&init);
    let args = if let Some(wrapper) = wrapper {
        let mut args = Vec::with_capacity(2 + init.cli_args.len());
        args.push(wrapper.display().to_string());
        args.push(spawn_spec.bin.clone());
        args.extend(spawn_spec.args.clone());
        ("/bin/sh".to_string(), args)
    } else {
        (spawn_spec.bin, spawn_spec.args)
    };
    let spawn_opts = SpawnOpts {
        cwd: init.working_dir.clone(),
        cols: DEFAULT_TERMINAL_COLS,
        rows: DEFAULT_TERMINAL_ROWS,
        env: Vec::new(),
    };
    backend_impl
        .spawn(&args.0, &args.1, spawn_opts)
        .await
        .with_context(|| format!("failed to {} session {}", attach_context, init.session_id))?;
    let backend: Arc<Mutex<Box<dyn SessionBackend>>> = Arc::new(Mutex::new(backend_impl));
    let mut cli_pid_marker = None;
    let child_pid = backend.lock().await.child_pid().await?;
    adapter.lock().await.on_spawned(child_pid);
    if let Some(pid) = child_pid {
        tokio::fs::create_dir_all(paths.cli_pid_markers_dir()).await?;
        let marker = paths.cli_pid_markers_dir().join(pid.to_string());
        tokio::fs::write(&marker, init.session_id.as_bytes()).await?;
        cli_pid_marker = Some(marker);
    }
    let latest_screen = Arc::new(RwLock::new(String::new()));
    let latest_raw_screen = Arc::new(RwLock::new(String::new()));
    let display_mode = Arc::new(RwLock::new(DisplayMode::Hidden));
    let analyzer_runtime = Arc::new(RwLock::new(AnalyzerRuntime::default()));
    let usage_limit_tracker = Arc::new(Mutex::new(UsageLimitTracker::default()));
    let current_turn_id = Arc::new(RwLock::new(String::new()));
    let (updates, _) = broadcast::channel::<String>(256);

    send_message(
        &stdout,
        &WorkerToDaemon::Ready {
            zellij_session: session_name.clone(),
        },
    )
    .await?;

    let sample_backend = backend.clone();
    let sample_screen = latest_screen.clone();
    let sample_raw_screen = latest_raw_screen.clone();
    let sample_updates = updates.clone();
    let sample_stdout = stdout.clone();
    let sample_adapter = adapter.clone();
    let sample_display_mode = display_mode.clone();
    let sample_usage_limit_tracker = usage_limit_tracker.clone();
    let sample_current_turn_id = current_turn_id.clone();
    let screenshot_app_id = init.lark_app_id.clone();
    let screenshot_app_secret = init.lark_app_secret.clone();
    let last_uploaded_hash = Arc::new(Mutex::new(None::<String>));
    let sample_last_uploaded_hash = last_uploaded_hash.clone();
    let last_broadcast_hash: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let sample_last_broadcast_hash = last_broadcast_hash.clone();
    let sample_analyzer_runtime = analyzer_runtime.clone();
    let screen_capture_task = tokio::spawn(async move {
        let mut last_emitted_status = ScreenStatus::Starting;
        let mut last_emitted_usage_limit: Option<CliUsageLimitState> = None;
        loop {
            let (screen, alive) = {
                let guard = sample_backend.lock().await;
                let screen = guard.capture_viewport().await.unwrap_or_default();
                let alive = guard.is_alive().await.unwrap_or(false);
                (screen, alive)
            };

            let hash_changed;

            {
                *sample_raw_screen.write().await = screen.clone();
                let mode = *sample_display_mode.read().await;
                let rendered = render_screen_for_display_mode(&screen, mode);
                let now_ms = now_ms();
                let analyzing = sample_analyzer_runtime.read().await.is_analyzing;
                let base_status = if analyzing {
                    ScreenStatus::Analyzing
                } else {
                    ScreenStatus::Working
                };
                let (status, usage_limit) =
                    sample_usage_limit_tracker
                        .lock()
                        .await
                        .classify(&screen, base_status, now_ms);
                let rendered_hash = format!("{:x}", Sha256::digest(rendered.as_bytes()));
                {
                    let guard = sample_last_broadcast_hash.lock().await;
                    hash_changed = guard.as_deref() != Some(&rendered_hash);
                }

                if hash_changed
                    || last_emitted_status != status
                    || last_emitted_usage_limit != usage_limit
                {
                    *sample_last_broadcast_hash.lock().await = Some(rendered_hash.clone());
                    let mut current = sample_screen.write().await;
                    *current = rendered.clone();
                    let _ = sample_updates.send(rendered.clone());
                    let _ = send_message(
                        &sample_stdout,
                        &WorkerToDaemon::ScreenUpdate {
                            content: rendered.clone(),
                            status,
                            usage_limit: usage_limit.clone(),
                        },
                    )
                    .await;
                    last_emitted_status = status;
                    last_emitted_usage_limit = usage_limit.clone();
                }

                if mode == DisplayMode::Screenshot
                    && screenshot_app_id != "local"
                    && !screenshot_app_secret.is_empty()
                {
                    maybe_send_screenshot_upload(
                        &sample_stdout,
                        &screenshot_app_id,
                        &screenshot_app_secret,
                        &screen,
                        status,
                        usage_limit.clone(),
                        &sample_last_uploaded_hash,
                    )
                    .await;
                }
            }

            if let Ok(poll) = sample_adapter.lock().await.poll() {
                if let Some(cli_session_id) = poll.cli_session_id {
                    let _ = send_message(
                        &sample_stdout,
                        &WorkerToDaemon::CliSessionId { cli_session_id },
                    )
                    .await;
                }
                if let Some((user_text, assistant_text)) = poll.adopt_preamble {
                    let _ = send_message(
                        &sample_stdout,
                        &WorkerToDaemon::AdoptPreamble {
                            user_text,
                            assistant_text,
                        },
                    )
                    .await;
                }
                if let Some(content) = poll.final_output {
                    let turn_id = sample_current_turn_id.read().await.clone();
                    let _ = send_message(
                        &sample_stdout,
                        &WorkerToDaemon::FinalOutput {
                            content,
                            turn_id,
                            kind: poll.final_output_kind,
                            user_text: poll.final_output_user_text,
                        },
                    )
                    .await;
                }
                if poll.prompt_ready {
                    let _ = send_message(&sample_stdout, &WorkerToDaemon::PromptReady).await;
                    let rendered = sample_screen.read().await.clone();
                    let raw = sample_raw_screen.read().await.clone();
                    let now_ms = now_ms();
                    let analyzing = sample_analyzer_runtime.read().await.is_analyzing;
                    let base_status = if analyzing {
                        ScreenStatus::Analyzing
                    } else {
                        ScreenStatus::Idle
                    };
                    let (status, usage_limit) =
                        sample_usage_limit_tracker
                            .lock()
                            .await
                            .classify(&raw, base_status, now_ms);
                    let _ = send_message(
                        &sample_stdout,
                        &WorkerToDaemon::ScreenUpdate {
                            content: rendered,
                            status,
                            usage_limit,
                        },
                    )
                    .await;
                }
            }

            if !alive {
                let _ = send_message(
                    &sample_stdout,
                    &WorkerToDaemon::CliExit {
                        code: Some(0),
                        signal: None,
                    },
                )
                .await;
                break;
            }

            tokio::time::sleep(Duration::from_millis(5000)).await;
        }
    });
    let mut worker_joins = tokio::task::JoinSet::new();
    worker_joins.spawn(async move {
        let _ = screen_capture_task.await;
    });

    if screen_analyzer_enabled(&init.screen_analyzer) {
        let analyzer_cfg = init.screen_analyzer.clone();
        let analyzer_raw_screen = latest_raw_screen.clone();
        let analyzer_runtime_state = analyzer_runtime.clone();
        let analyzer_stdout = stdout.clone();
        let analyzer_task = tokio::spawn(async move {
            let client = Client::new();
            loop {
                tokio::time::sleep(Duration::from_millis(analyzer_cfg.interval_ms)).await;
                let snapshot = analyzer_raw_screen.read().await.clone();
                if snapshot.is_empty() {
                    continue;
                }
                let truncated = if snapshot.len() > analyzer_cfg.snapshot_max_chars {
                    snapshot[snapshot.len() - analyzer_cfg.snapshot_max_chars..].to_string()
                } else {
                    snapshot
                };
                let now = now_ms();
                {
                    let mut runtime = analyzer_runtime_state.write().await;
                    if truncated == runtime.last_snapshot {
                        runtime.stable_count = runtime.stable_count.saturating_add(1);
                    } else {
                        runtime.stable_count = 1;
                        runtime.last_snapshot = truncated.clone();
                        if runtime.waiting_for_content_change {
                            runtime.waiting_for_content_change = false;
                        }
                    }
                    if runtime.stable_count < analyzer_cfg.stable_count {
                        continue;
                    }
                    if runtime.waiting_for_content_change
                        && truncated == runtime.last_analyzed_snapshot
                    {
                        continue;
                    }
                    if runtime.cooldown_until_ms > now {
                        continue;
                    }
                    runtime.is_analyzing = true;
                    runtime.last_analyzed_snapshot = truncated.clone();
                }

                let result = call_screen_analyzer(&client, &analyzer_cfg, &truncated).await;

                let mut runtime = analyzer_runtime_state.write().await;
                runtime.is_analyzing = false;
                match result {
                    Ok(analysis) => {
                        apply_screen_analyzer_result(
                            &mut runtime,
                            &analysis.check_again_when,
                            now_ms(),
                        );
                        if analysis.needs_interaction && !analysis.options.is_empty() {
                            if !runtime.prompt_active {
                                runtime.prompt_active = true;
                                let _ = send_message(
                                    &analyzer_stdout,
                                    &WorkerToDaemon::TuiPrompt {
                                        description: analysis.description.clone().unwrap_or_else(
                                            || "CLI needs your selection".to_string(),
                                        ),
                                        options: analysis.options.clone(),
                                        multi_select: analysis.multi_select,
                                    },
                                )
                                .await;
                            }
                        } else if runtime.prompt_active {
                            runtime.prompt_active = false;
                            let _ = send_message(
                                &analyzer_stdout,
                                &WorkerToDaemon::TuiPromptResolved {
                                    selected_text: None,
                                },
                            )
                            .await;
                        }
                    }
                    Err(_) => {
                        runtime.waiting_for_content_change = true;
                        runtime.cooldown_until_ms = 0;
                    }
                }
            }
        });
        worker_joins.spawn(async move {
            let _ = analyzer_task.await;
        });
    }

    if !init.prompt.is_empty() && !crate::adapters::passes_initial_prompt_via_args(&init.cli_id) {
        usage_limit_tracker.lock().await.begin_turn(
            "",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        );
        *current_turn_id.write().await = init
            .prompt_turn_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        *last_uploaded_hash.lock().await = None;
        let guard = backend.lock().await;
        let submit = adapter
            .lock()
            .await
            .write_input(guard.as_ref(), &init.prompt)
            .await?;
        if let Some(cli_session_id) = submit.cli_session_id {
            send_message(&stdout, &WorkerToDaemon::CliSessionId { cli_session_id }).await?;
        }
    }

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(_) => break,
        };
        if line.trim().is_empty() {
            continue;
        }
        let msg: DaemonToWorker = serde_json::from_str(&line)?;
        match msg {
            DaemonToWorker::Message { content, turn_id } => {
                handle_tui_prompt_override(&stdout, &analyzer_runtime).await;
                let snapshot = latest_raw_screen.read().await.clone();
                usage_limit_tracker.lock().await.begin_turn(
                    &snapshot,
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                );
                *current_turn_id.write().await = turn_id;
                *last_uploaded_hash.lock().await = None;
                let guard = backend.lock().await;
                let submit = adapter
                    .lock()
                    .await
                    .write_input(guard.as_ref(), &content)
                    .await?;
                if let Some(cli_session_id) = submit.cli_session_id {
                    send_message(&stdout, &WorkerToDaemon::CliSessionId { cli_session_id }).await?;
                }
                if !submit.submitted {
                    let message = submit
                        .failure_reason
                        .unwrap_or_else(|| "CLI submit could not be confirmed".to_string());
                    send_message(&stdout, &WorkerToDaemon::UserNotify { message }).await?;
                }
            }
            DaemonToWorker::RawInput { content, turn_id } => {
                handle_tui_prompt_override(&stdout, &analyzer_runtime).await;
                let snapshot = latest_raw_screen.read().await.clone();
                usage_limit_tracker.lock().await.begin_turn(
                    &snapshot,
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                );
                *current_turn_id.write().await = turn_id;
                *last_uploaded_hash.lock().await = None;
                let guard = backend.lock().await;
                guard.raw_input(&content).await?;
            }
            DaemonToWorker::Close => {
                let mut guard = backend.lock().await;
                guard.destroy_session().await?;
                break;
            }
            DaemonToWorker::Restart => {
                let mut guard = backend.lock().await;
                guard.destroy_session().await?;
                break;
            }
            DaemonToWorker::RefreshScreen => {
                let guard = backend.lock().await;
                let screen = guard.capture_viewport().await?;
                *latest_raw_screen.write().await = screen.clone();
                let mode = *display_mode.read().await;
                let rendered = render_screen_for_display_mode(&screen, mode);
                let now_ms = now_ms();
                let analyzing = analyzer_runtime.read().await.is_analyzing;
                let base_status = if analyzing {
                    ScreenStatus::Analyzing
                } else {
                    ScreenStatus::Working
                };
                let (status, usage_limit) =
                    usage_limit_tracker
                        .lock()
                        .await
                        .classify(&screen, base_status, now_ms);
                *latest_screen.write().await = rendered.clone();
                let _ = updates.send(rendered.clone());
                send_message(
                    &stdout,
                    &WorkerToDaemon::ScreenUpdate {
                        content: rendered.clone(),
                        status,
                        usage_limit: usage_limit.clone(),
                    },
                )
                .await?;
                let rendered_hash = format!("{:x}", Sha256::digest(rendered.as_bytes()));
                *last_broadcast_hash.lock().await = Some(rendered_hash);
                if mode == DisplayMode::Screenshot {
                    maybe_send_screenshot_upload(
                        &stdout,
                        &init.lark_app_id,
                        &init.lark_app_secret,
                        &screen,
                        status,
                        usage_limit,
                        &last_uploaded_hash,
                    )
                    .await;
                }
            }
            DaemonToWorker::SetDisplayMode { mode } => {
                *display_mode.write().await = mode;
                let raw = latest_raw_screen.read().await.clone();
                let rendered = render_screen_for_display_mode(&raw, mode);
                let now_ms = now_ms();
                let analyzing = analyzer_runtime.read().await.is_analyzing;
                let base_status = if analyzing {
                    ScreenStatus::Analyzing
                } else {
                    ScreenStatus::Working
                };
                let (status, usage_limit) =
                    usage_limit_tracker
                        .lock()
                        .await
                        .classify(&raw, base_status, now_ms);
                *latest_screen.write().await = rendered.clone();
                let _ = updates.send(rendered.clone());
                send_message(
                    &stdout,
                    &WorkerToDaemon::ScreenUpdate {
                        content: rendered,
                        status,
                        usage_limit: usage_limit.clone(),
                    },
                )
                .await?;
                if mode == DisplayMode::Screenshot {
                    maybe_send_screenshot_upload(
                        &stdout,
                        &init.lark_app_id,
                        &init.lark_app_secret,
                        &raw,
                        status,
                        usage_limit,
                        &last_uploaded_hash,
                    )
                    .await;
                }
            }
            DaemonToWorker::TermAction { key } => {
                let keys = term_action_keys(key);
                let guard = backend.lock().await;
                guard.send_special_keys(&keys).await?;
            }
            DaemonToWorker::SpecialKeys { keys } => {
                let guard = backend.lock().await;
                guard.send_special_keys(&keys).await?;
            }
            DaemonToWorker::TuiKeys { keys, is_final } => {
                handle_tui_keys(&backend, &analyzer_runtime, &keys, is_final).await?;
            }
            DaemonToWorker::TuiTextInput { keys, text } => {
                handle_tui_text_input(&backend, &adapter, &analyzer_runtime, &keys, &text).await?;
            }
            DaemonToWorker::Init(_) => {}
        }
    }

    worker_joins.abort_all();
    while worker_joins.join_next().await.is_some() {}
    {
        let mut guard = backend.lock().await;
        let _ = guard.kill().await;
    }
    if let Some(marker) = cli_pid_marker {
        let _ = tokio::fs::remove_file(marker).await;
    }

    info!("worker exiting");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_screen_for_display_mode_hides_or_shows_content() {
        assert_eq!(
            render_screen_for_display_mode("hello", DisplayMode::Hidden),
            "[screen hidden]"
        );
        assert_eq!(
            render_screen_for_display_mode("hello", DisplayMode::Screenshot),
            "hello"
        );
    }

    #[test]
    fn render_screen_for_screenshot_mode_preserves_full_text() {
        let screen = (0..80)
            .map(|idx| format!("{idx:02}:{}", "x".repeat(140)))
            .collect::<Vec<_>>()
            .join("\n");
        let rendered = render_screen_for_display_mode(&screen, DisplayMode::Screenshot);
        let lines: Vec<&str> = rendered.lines().collect();

        assert_eq!(lines.len(), 80);
        assert!(lines[0].starts_with("00:"));
        assert!(lines[79].starts_with("79:"));
        assert!(lines[0].chars().count() > 120);
    }

    #[test]
    fn term_action_keys_maps_supported_actions() {
        assert_eq!(term_action_keys(TermActionKey::Esc), vec!["Escape"]);
        assert_eq!(term_action_keys(TermActionKey::CtrlC), vec!["C-c"]);
        assert_eq!(term_action_keys(TermActionKey::Enter), vec!["Enter"]);
        assert_eq!(
            term_action_keys(TermActionKey::HalfPageDown),
            vec!["PageDown"]
        );
    }

    #[test]
    fn detect_cli_usage_limit_finds_usage_and_rate_limits() {
        let usage = detect_cli_usage_limit(
            "You have hit your usage limit. Try again at 3:15 PM.",
            1_700_000_000_000,
        )
        .expect("usage limit detected");
        assert_eq!(usage.kind, CliUsageLimitKind::Usage);
        assert_eq!(usage.retry_label, "3:15 PM");
        assert!(usage.limited);

        let rate = detect_cli_usage_limit("Rate limited. Resets at 11:00 AM.", 1_700_000_000_000)
            .expect("rate limit detected");
        assert_eq!(rate.kind, CliUsageLimitKind::Rate);
        assert_eq!(rate.retry_label, "11:00 AM");
    }

    #[test]
    fn usage_limit_tracker_suppresses_stale_retry_ready_banner_on_new_turn() {
        let now_ms = 1_700_000_000_000;
        let text = "Usage limit reached. Try again at 3:15 PM.";
        let mut tracker = UsageLimitTracker::default();
        let initial = detect_cli_usage_limit(text, now_ms).expect("limit");
        tracker.begin_turn(text, initial.retry_at_ms + 1);
        let (status, usage_limit) =
            tracker.classify(text, ScreenStatus::Working, initial.retry_at_ms + 1);
        assert_eq!(status, ScreenStatus::Working);
        assert_eq!(usage_limit, None);
    }

    #[test]
    fn render_text_screenshot_png_produces_png_bytes() {
        let png = render_text_screenshot_png("hello\nworld").expect("png rendered");
        assert!(png.starts_with(&[0x89, b'P', b'N', b'G']));
        assert!(png.len() > 64);
    }

    #[test]
    fn render_text_screenshot_png_uses_full_screenshot_input() {
        let screen = (0..80)
            .map(|_| "x".repeat(200))
            .collect::<Vec<_>>()
            .join("\n");
        let png = render_text_screenshot_png(&screen).expect("png rendered");
        let image = image::load_from_memory(&png).expect("png should decode");
        let expected_width = ((200f32 * CELL_W).ceil() as u32 + PADDING * 2).max(64);
        let expected_height = ((80f32 * CELL_H).ceil() as u32 + PADDING * 2).max(32);

        assert_eq!(image.width(), expected_width);
        assert_eq!(image.height(), expected_height);
    }

    #[test]
    fn screen_analyzer_enablement_requires_complete_config() {
        let mut cfg = ScreenAnalyzerConfig::default();
        assert!(!screen_analyzer_enabled(&cfg));
        cfg.enabled = true;
        cfg.base_url = "https://example.com".to_string();
        cfg.api_key = "k".to_string();
        cfg.model = "m".to_string();
        assert!(screen_analyzer_enabled(&cfg));
    }

    #[test]
    fn parse_screen_analyzer_response_accepts_markdown_wrapped_json() {
        let content = "```json\n{\"needsInteraction\":false,\"checkAgainWhen\":\"after_5s\"}\n```";
        assert_eq!(
            parse_screen_analyzer_response(content).check_again_when,
            "after_5s"
        );
        assert_eq!(
            parse_screen_analyzer_response("{\"needsInteraction\":false}").check_again_when,
            "content_changed"
        );
    }

    #[test]
    fn parse_screen_analyzer_response_builds_tui_prompt_keys() {
        let content = r#"{
          "needsInteraction": true,
          "description": "pick one",
          "multiSelect": false,
          "confirmKey": "Enter",
          "options": [
            { "label": "1", "text": "alpha", "type": "select", "index": 0 },
            { "label": "2", "text": "beta", "type": "confirm", "index": 1 }
          ],
          "checkAgainWhen": "content_changed"
        }"#;
        let parsed = parse_screen_analyzer_response(content);
        assert!(parsed.needs_interaction);
        assert_eq!(parsed.description.as_deref(), Some("pick one"));
        assert_eq!(parsed.options.len(), 2);
        assert_eq!(parsed.options[0].keys, vec!["Enter"]);
        assert_eq!(parsed.options[1].keys, vec!["Down", "Enter"]);
    }
}

pub async fn run_from_init_path(path: &Path) -> Result<()> {
    let payload = tokio::fs::read_to_string(path).await?;
    let init = serde_json::from_str::<InitConfig>(&payload)?;
    run(init).await
}
