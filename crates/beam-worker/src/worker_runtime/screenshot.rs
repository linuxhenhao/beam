use super::*;

pub(crate) fn render_screen_for_display_mode(screen: &str, mode: DisplayMode) -> String {
    match mode {
        DisplayMode::Hidden => "[screen hidden]".to_string(),
        DisplayMode::Screenshot => strip_ansi(screen).replace('\r', ""),
    }
}

pub(crate) fn has_pattern(text: &str, patterns: &[&str]) -> bool {
    let lower = text.to_ascii_lowercase();
    patterns.iter().any(|pattern| lower.contains(pattern))
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct LarkTokenResponse {
    code: i32,
    msg: Option<String>,
    tenant_access_token: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct LarkImageUploadResponse {
    code: i32,
    msg: Option<String>,
    image_key: Option<String>,
    data: Option<LarkImageUploadData>,
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct LarkImageUploadData {
    image_key: Option<String>,
}

pub(crate) fn parse_retry_time(text: &str, now_ms: u64) -> Option<(u64, String)> {
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

pub(crate) fn detect_cli_usage_limit(text: &str, now_ms: u64) -> Option<CliUsageLimitState> {
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

pub(crate) fn usage_limit_state_key(state: &CliUsageLimitState) -> String {
    format!(
        "{:?}:{}:{}",
        state.kind, state.retry_at_ms, state.retry_label
    )
}

static PRIMARY_FONT: LazyLock<StdMutex<Option<FontVec>>> = LazyLock::new(|| StdMutex::new(None));
static CJK_FONT: LazyLock<StdMutex<Option<FontVec>>> = LazyLock::new(|| StdMutex::new(None));
const FONT_SIZE: f32 = 14.0;
pub(crate) const CELL_W: f32 = 8.4;
pub(crate) const CELL_H: f32 = 18.0;
pub(crate) const PADDING: u32 = 12;
const BG_COLOR: Rgba<u8> = Rgba([26, 27, 38, 255]);
const FG_COLOR: Rgba<u8> = Rgba([169, 177, 214, 255]);

pub(crate) fn home_font_dir() -> Option<std::path::PathBuf> {
    std::env::var("HOME")
        .ok()
        .map(|h| std::path::PathBuf::from(h).join(".beam").join("fonts"))
}

pub(crate) fn load_font_files() {
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

pub(crate) fn is_fullwidth(ch: char) -> bool {
    matches!(UnicodeWidthChar::width(ch), Some(2))
}

pub(crate) fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Compute a content-based hash from the **visual** screenshot (rendered
/// PNG bytes), not the raw terminal string.
///
/// Invisible control characters (e.g. `\r`, bare `\x1b`, private CSI) that
/// don't change the rendered image are ignored by this hash.  The
/// coordinator uses this hash for dedup (`should_upload` /
/// `record_upload`) so that consecutive identical-looking screenshots are
/// not re-uploaded.
///
/// Returns an error only when PNG rendering itself fails (unlikely for
/// in-memory rendering; the fallback bitmap path is infallible).
pub(crate) fn screenshot_visual_hash(screen: &str) -> Result<String> {
    let png = render_text_screenshot_png(screen)?;
    Ok(lower_hex(&Sha256::digest(&png)))
}

pub(crate) fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            if chars.peek() == Some(&'[') {
                chars.next(); // consume '['
                // Consume CSI parameter bytes (0x30..=0x3F), intermediate bytes
                // (0x20..=0x2F), and the final byte (0x40..=0x7E), so that
                // private-mode sequences like \x1b[?25h are fully stripped.
                while let Some(&c) = chars.peek() {
                    let cu = c as u32;
                    if (0x30..=0x3F).contains(&cu) || (0x20..=0x2F).contains(&cu) {
                        chars.next();
                    } else if (0x40..=0x7E).contains(&cu) {
                        chars.next(); // final byte
                        break;
                    } else {
                        break; // unexpected character, stop consuming
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

pub(crate) fn find_glyph_font<'a>(
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

pub(crate) fn render_text_screenshot_png(screen_raw: &str) -> Result<Vec<u8>> {
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

pub(crate) fn blend_alpha(bg: Rgba<u8>, fg: Rgba<u8>, alpha: u8) -> Rgba<u8> {
    let a = alpha as f32 / 255.0;
    let r = (fg.0[0] as f32 * a + bg.0[0] as f32 * (1.0 - a)) as u8;
    let g = (fg.0[1] as f32 * a + bg.0[1] as f32 * (1.0 - a)) as u8;
    let b = (fg.0[2] as f32 * a + bg.0[2] as f32 * (1.0 - a)) as u8;
    Rgba([r, g, b, 255])
}

pub(crate) fn fallback_bitmap_png(screen: &str) -> Result<Vec<u8>> {
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

pub(crate) fn lark_base_url() -> &'static str {
    "https://open.feishu.cn/open-apis"
}

pub(crate) async fn lark_tenant_token(app_id: &str, secret: &str) -> Result<String> {
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

pub(crate) async fn upload_image_buffer(
    app_id: &str,
    secret: &str,
    image: Vec<u8>,
) -> Result<String> {
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

/// Core screenshot pipeline: render → Feishu upload → IPC.
///
/// Does **not** update the shared `last_uploaded_hash` — the caller is
/// responsible for dedup state.
///
/// Use [`perform_screenshot_upload`] when the caller wants the shared hash
/// update to happen inside the same await (blocking path).
pub(crate) async fn do_screenshot_upload(
    stdout: &Arc<Mutex<tokio::io::Stdout>>,
    session_id: &str,
    trigger_source: &str,
    app_id: &str,
    app_secret: &str,
    screen: &str,
    status: ScreenStatus,
    usage_limit: Option<CliUsageLimitState>,
    _hash: &str,
    turn_id: Option<String>,
) -> bool {
    let t0 = std::time::Instant::now();
    info!(
        session_id = %session_id,
        trigger = %trigger_source,
        "screenshot_upload start",
    );

    let png = match render_text_screenshot_png(screen) {
        Ok(png) => png,
        Err(err) => {
            let elapsed_ms = t0.elapsed().as_millis() as u64;
            warn!(
                session_id = %session_id,
                trigger = %trigger_source,
                stage = "render",
                elapsed_ms = elapsed_ms,
                error = %err,
                "screenshot_upload failed",
            );
            return false;
        }
    };
    let render_ms = t0.elapsed().as_millis() as u64;
    let png_bytes = png.len();

    let t1 = std::time::Instant::now();
    let image_key = match upload_image_buffer(app_id, app_secret, png).await {
        Ok(image_key) => image_key,
        Err(err) => {
            let elapsed_ms = t0.elapsed().as_millis() as u64;
            warn!(
                session_id = %session_id,
                trigger = %trigger_source,
                stage = "upload",
                elapsed_ms = elapsed_ms,
                error = %err,
                "screenshot_upload failed",
            );
            return false;
        }
    };
    let upload_ms = t1.elapsed().as_millis() as u64;
    let total_ms = t0.elapsed().as_millis() as u64;

    if send_message(
        stdout,
        &WorkerToDaemon::ScreenshotUploaded {
            image_key,
            status,
            usage_limit,
            turn_id,
        },
    )
    .await
    .is_err()
    {
        let elapsed_ms = t0.elapsed().as_millis() as u64;
        warn!(
            session_id = %session_id,
            trigger = %trigger_source,
            stage = "ipc",
            elapsed_ms = elapsed_ms,
            "screenshot_upload failed: IPC send error",
        );
        return false;
    }

    info!(
        session_id = %session_id,
        trigger = %trigger_source,
        render_ms = render_ms,
        upload_ms = upload_ms,
        total_ms = total_ms,
        png_bytes = png_bytes,
        "screenshot_upload success",
    );
    true
}

/// Perform the full screenshot render + Feishu upload + ScreenshotUploaded IPC
/// without dedup checks.
///
/// Convenience wrapper: calls [`do_screenshot_upload`] and also updates the
/// shared `last_uploaded_hash` on success.
///
/// Returns `true` when all three stages (render, Feishu upload, IPC send)
/// succeeded.  The caller should only update its own dedup state (e.g.
/// [`crate::worker_runtime::coordinator::record_upload`]) when this returns
/// `true`.
///
/// On failure the shared `last_uploaded_hash` is **not** updated, so the next
/// tick will retry.
///
/// Note: kept for API completeness; new code should prefer
/// [`do_screenshot_upload`] and manage dedup state separately.
#[allow(dead_code)]
pub(crate) async fn perform_screenshot_upload(
    stdout: &Arc<Mutex<tokio::io::Stdout>>,
    session_id: &str,
    trigger_source: &str,
    app_id: &str,
    app_secret: &str,
    screen: &str,
    status: ScreenStatus,
    usage_limit: Option<CliUsageLimitState>,
    last_uploaded_hash: &Arc<Mutex<Option<String>>>,
    hash: &str,
    turn_id: Option<String>,
) -> bool {
    let ok = do_screenshot_upload(
        stdout,
        session_id,
        trigger_source,
        app_id,
        app_secret,
        screen,
        status,
        usage_limit,
        hash,
        turn_id,
    )
    .await;
    if ok {
        *last_uploaded_hash.lock().await = Some(hash.to_string());
    }
    ok
}

#[derive(Debug, Default)]
pub(crate) struct UsageLimitTracker {
    turn_seq: u64,
    detected_turn: Option<u64>,
    suppressed_retry_ready_key: Option<String>,
}

impl UsageLimitTracker {
    pub(crate) fn begin_turn(&mut self, snapshot: &str, now_ms: u64) -> u64 {
        self.turn_seq += 1;
        self.detected_turn = None;
        self.suppressed_retry_ready_key = detect_cli_usage_limit(snapshot, now_ms)
            .filter(|state| state.retry_ready)
            .map(|state| usage_limit_state_key(&state));
        self.turn_seq
    }

    pub(crate) fn classify(
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

#[cfg(test)]
mod tests {
    use super::strip_ansi;

    #[test]
    fn strip_ansi_private_csi_cursor_show_hide() {
        // \x1b[?25h and \x1b[?25l should be fully consumed — no residue.
        let input = "\x1b[?25hHello\x1b[?25l";
        assert_eq!(strip_ansi(input), "Hello");

        let input2 = "A\x1b[?25hB\x1b[?25lC";
        assert_eq!(strip_ansi(input2), "ABC");
    }

    #[test]
    fn strip_ansi_mixed_sgr_private_and_cursor() {
        let input = "\x1b[1;31mRed\x1b[0m \x1b[?25h\x1b[10;20HCursor\x1b[2J\nNext line";
        let expected = "Red Cursor\nNext line";
        assert_eq!(strip_ansi(input), expected);
    }

    #[test]
    fn strip_ansi_incomplete_csi_safe_no_panic() {
        // End-of-string right after ESC [
        assert_eq!(strip_ansi("\x1b["), "");

        // End-of-string with partial parameter bytes (no final byte)
        assert_eq!(strip_ansi("abc\x1b[?25"), "abc");

        // End-of-string with longer partial params
        assert_eq!(strip_ansi("x\x1b[38;2;"), "x");

        // Only the param marker '>' without final byte — nothing leaked
        assert_eq!(strip_ansi("x\x1b[>"), "x");
    }

    // ── screenshot_visual_hash ────────────────────────────────────────

    /// Two raw inputs that differ only in invisible control characters
    /// (`\r` carriage return) must produce the same visual hash, because
    /// the rendered PNG is identical.
    ///
    /// `strip_ansi` does NOT strip `\r`, so the old dedup
    /// (`Sha256::digest(strip_ansi(&screen))`) would see them as
    /// different.  `render_text_screenshot_png` uses `.lines()` which
    /// treats `\r\n` as a single line ending → identical visual output.
    #[test]
    fn screenshot_visual_hash_same_when_only_control_chars_differ() {
        let screen1 = "hello world\n";
        // \r\n is invisible in the rendered PNG — .lines() treats it as
        // one line ending, same as a bare \n.
        let screen2 = "hello world\r\n";

        let h1 =
            super::screenshot_visual_hash(screen1).expect("visual hash should succeed for screen1");
        let h2 =
            super::screenshot_visual_hash(screen2).expect("visual hash should succeed for screen2");

        assert_eq!(
            h1, h2,
            "visual hashes must match when only invisible control chars differ"
        );
    }

    /// When the visible text content differs, the visual hash must also
    /// differ.
    #[test]
    fn screenshot_visual_hash_different_when_text_differs() {
        let screen1 = "hello world\n";
        let screen2 = "goodbye world\n";

        let h1 =
            super::screenshot_visual_hash(screen1).expect("visual hash should succeed for screen1");
        let h2 =
            super::screenshot_visual_hash(screen2).expect("visual hash should succeed for screen2");

        assert_ne!(
            h1, h2,
            "visual hashes must differ when visible text content differs"
        );
    }
}
