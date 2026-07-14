use crate::worker_runtime::*;

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
