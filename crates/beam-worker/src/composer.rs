//! Learn the TUI composer's real-input color, then tell draft from hint.
//!
//! After we type, the cells after the prompt are our draft and their
//! foreground is the session's input color. After submit, an empty payload
//! or a uniformly colored payload that is *not* that color is a placeholder
//! (accepted / queued). Same-color leftover text is still a draft.

use std::future::Future;
use std::time::Duration;

use anyhow::Result;
use image::Rgba;
use tracing::debug;

use crate::backend::SessionBackend;
use crate::worker_runtime::screenshot_ansi::{StyledCell, parse_ansi_screen};

/// Structural description of a TUI composer line.
///
/// The prompt character is deliberately *not* part of this hint. The ready
/// gate and the draft/hint parsers locate the composer by shape (leading
/// prompt glyph, optionally behind a box border) and accept the whole glyph
/// family, so a CLI that restyles its prompt (codex `›`, traex `❯`, a future
/// `❭`) needs no per-character configuration anywhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ComposerHint {
    /// Whether the composer sits inside a bordered box (`│ … │`).
    pub boxed: bool,
}

/// Composer on a bare line, no box chrome (codex, traex).
pub(crate) const PLAIN_COMPOSER: ComposerHint = ComposerHint { boxed: false };
/// Composer inside a bordered box (kimi, grok).
pub(crate) const BOXED_COMPOSER: ComposerHint = ComposerHint { boxed: true };

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ComposerState {
    Missing,
    Empty,
    Placeholder,
    Draft,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubmitLook {
    Accepted,
    Retry,
    Hold,
}

const CONFIRM_ATTEMPTS: usize = 4;
const CONFIRM_INTERVAL: Duration = Duration::from_millis(800);

pub(crate) fn sample_draft_fgs(screen: &str, hint: ComposerHint) -> Vec<Rgba<u8>> {
    let Some(cells) = composer_payload(screen, hint) else {
        debug!(boxed = hint.boxed, "composer draft sample: chrome missing");
        return Vec::new();
    };
    let fgs = unique_fgs(&cells);
    debug!(
        boxed = hint.boxed,
        payload_cells = cells.len(),
        draft_fgs = %fmt_fgs(&fgs),
        "composer learned draft colors"
    );
    fgs
}

pub(crate) fn composer_state(
    screen: &str,
    hint: ComposerHint,
    draft_fgs: &[Rgba<u8>],
) -> ComposerState {
    let Some(cells) = composer_payload(screen, hint) else {
        return ComposerState::Missing;
    };
    let fgs = unique_fgs(&cells);
    if fgs.is_empty() {
        return ComposerState::Empty;
    }
    if fgs.len() == 1 && !draft_fgs.is_empty() && !draft_fgs.iter().any(|fg| *fg == fgs[0]) {
        return ComposerState::Placeholder;
    }
    ComposerState::Draft
}

pub(crate) fn screen_mentions_queue(screen: &str) -> bool {
    screen.to_ascii_lowercase().contains("queued")
}

pub(crate) fn screen_looks_busy(screen: &str) -> bool {
    let lower = screen.to_ascii_lowercase();
    lower.contains("waiting for") || lower.contains("interrupt")
}

pub(crate) async fn confirm_typed_submit<T, R, Fut>(
    backend: &dyn SessionBackend,
    hint: ComposerHint,
    draft_fgs: &[Rgba<u8>],
    submit_via: &str,
    mut transcript_ok: T,
    mut resubmit: R,
) -> Result<bool>
where
    T: FnMut() -> Result<bool>,
    R: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    debug!(
        boxed = hint.boxed,
        submit_via,
        draft_fgs = %fmt_fgs(draft_fgs),
        "composer confirm start"
    );
    for attempt in 0..CONFIRM_ATTEMPTS {
        tokio::time::sleep(CONFIRM_INTERVAL).await;
        let screen = backend.capture_viewport().await.unwrap_or_default();
        let decision = submit_decision(&screen, hint, draft_fgs, &mut transcript_ok)?;
        debug!(
            attempt,
            submit_via,
            look = ?decision.look,
            via = decision.via,
            composer = ?decision.composer,
            seen_fgs = %decision.seen_fgs,
            "composer submit look"
        );
        match decision.look {
            SubmitLook::Accepted => return Ok(true),
            SubmitLook::Retry if attempt + 1 < CONFIRM_ATTEMPTS => {
                debug!(attempt, submit_via, "composer resubmit");
                resubmit().await?;
            }
            SubmitLook::Retry | SubmitLook::Hold => {}
        }
    }
    debug!(submit_via, "composer confirm timed out");
    Ok(false)
}

struct SubmitDecision {
    look: SubmitLook,
    composer: ComposerState,
    via: &'static str,
    seen_fgs: String,
}

fn submit_decision(
    screen: &str,
    hint: ComposerHint,
    draft_fgs: &[Rgba<u8>],
    transcript_ok: &mut impl FnMut() -> Result<bool>,
) -> Result<SubmitDecision> {
    let transcript = transcript_ok()?;
    let queued_badge = screen_mentions_queue(screen);
    let composer = composer_state(screen, hint, draft_fgs);
    let seen_fgs = composer_payload(screen, hint)
        .map(|cells| fmt_fgs(&unique_fgs(&cells)))
        .unwrap_or_default();
    let (look, via) = if transcript {
        (SubmitLook::Accepted, "transcript")
    } else if queued_badge {
        (SubmitLook::Accepted, "queued_badge")
    } else {
        match composer {
            ComposerState::Empty => (SubmitLook::Accepted, "empty"),
            ComposerState::Placeholder => (SubmitLook::Accepted, "placeholder"),
            ComposerState::Draft => (SubmitLook::Retry, "draft"),
            ComposerState::Missing => (SubmitLook::Hold, "missing"),
        }
    };
    Ok(SubmitDecision {
        look,
        composer,
        via,
        seen_fgs,
    })
}

fn fmt_fgs(fgs: &[Rgba<u8>]) -> String {
    fgs.iter()
        .map(|fg| format!("#{:02x}{:02x}{:02x}", fg.0[0], fg.0[1], fg.0[2]))
        .collect::<Vec<_>>()
        .join(",")
}

fn composer_payload(screen: &str, hint: ComposerHint) -> Option<Vec<StyledCell>> {
    let rows = parse_ansi_screen(screen);
    let line = rows
        .into_iter()
        .rev()
        .find(|row| prompt_cell_index(row, hint).is_some())?;
    let prompt_at = prompt_cell_index(&line, hint)?;
    let mut payload: Vec<StyledCell> = line[prompt_at + 1..]
        .iter()
        .copied()
        .skip_while(|cell| cell.ch.is_whitespace())
        .collect();
    while payload
        .last()
        .is_some_and(|cell| cell.ch.is_whitespace() || is_box_drawing(cell.ch))
    {
        payload.pop();
    }
    Some(payload)
}

/// Whether the viewport currently shows a composer line.
///
/// Used by the TUI-ready gate (`ReadyProbe::PromptLine`) so the gate and the
/// payload parsers agree on what a composer looks like.
pub(crate) fn screen_has_composer(screen: &str, hint: ComposerHint) -> bool {
    parse_ansi_screen(screen)
        .iter()
        .rev()
        .find_map(|row| prompt_cell_index(row, hint).map(|index| (row, index)))
        .is_some_and(|(row, index)| !looks_like_option_row(row, index))
}

/// Whether the payload after a prompt glyph is a numbered menu option.
///
/// Observed on a live codex start-up: the "Update available!" dialog renders
/// its selected entry as `› 1. Update now (runs \`npm install -g ...\`)`, with
/// the remaining options on the following rows. That row is glyph-shaped but is
/// not an input field, so the ready gate must not mistake it for the composer.
/// Only the presence check uses this; payload parsing keeps accepting numbered
/// drafts a user actually typed.
fn looks_like_option_row(row: &[StyledCell], prompt_at: usize) -> bool {
    let mut cells = row[prompt_at + 1..]
        .iter()
        .skip_while(|cell| cell.ch.is_whitespace());
    let mut saw_digit = false;
    loop {
        match cells.next() {
            Some(cell) if cell.ch.is_ascii_digit() => saw_digit = true,
            Some(cell) if saw_digit && matches!(cell.ch, '.' | ')') => {
                return cells.next().is_none_or(|cell| cell.ch.is_whitespace());
            }
            _ => return false,
        }
    }
}

/// Index of the prompt cell on one screen row, or `None` when the row is not a
/// composer line.
///
/// Only the row's leading cells are inspected: after skipping indentation the
/// first content cell must be a prompt glyph (bare composer), or a vertical
/// border followed by a prompt glyph (boxed composer). Everything else, notably
/// the footer/status rows a TUI parks *below* its input box, is rejected.
fn prompt_cell_index(row: &[StyledCell], hint: ComposerHint) -> Option<usize> {
    let mut saw_leading_border = false;
    for (index, cell) in row.iter().enumerate() {
        if cell.ch.is_whitespace() {
            continue;
        }
        if is_prompt_glyph(cell.ch) {
            if hint.boxed && !saw_leading_border {
                return None;
            }
            return Some(index);
        }
        if hint.boxed && !saw_leading_border && is_vertical_border(cell.ch) {
            saw_leading_border = true;
            continue;
        }
        return None;
    }
    None
}

/// Characters TUI composers use as a prompt marker.
///
/// This is a shape description ("angle, arrow or quote mark") rather than a
/// per-CLI literal, so `>` (kimi), `›` U+203A (codex), `❯` U+276F (traex/grok),
/// `»`, `→`, `⟩` and future restylings all match. Letters, digits, whitespace
/// and box-drawing are excluded, and the punctuation bands stop short of prose
/// marks (`—`, `•`, `”`) that show up at the start of transcript lines.
pub(crate) fn is_prompt_glyph(ch: char) -> bool {
    if ch.is_whitespace() || ch.is_alphanumeric() || is_box_drawing(ch) {
        return false;
    }
    matches!(ch,
        // ASCII prompt and the Latin-1 angle quotes.
        '>' | '«' | '»'
        // Single angle quotation marks (`›` U+203A codex, `‹` U+2039).
        | '\u{2039}'..='\u{203A}'
        // Arrows (`→` U+2192).
        | '\u{2190}'..='\u{21FF}'
        // Geometric shapes (`▶` U+25B6, `▸` U+25B8, `●` U+25CF).
        | '\u{25A0}'..='\u{25FF}'
        // Quote ornaments (`❯` U+276F traex/grok, `❭` U+276D, `❮` U+276E).
        | '\u{275B}'..='\u{2775}'
        // Math brackets (`⟩` U+27E9, `⟫` U+27EB).
        | '\u{27E6}'..='\u{27EF}'
        // Supplemental punctuation prompts (`⸜` U+2E1C, `⸝` U+2E1D).
        | '\u{2E00}'..='\u{2E7F}'
        // CJK angle brackets (`〈` U+3008, `《` U+300A, `「` U+300C).
        | '\u{3008}'..='\u{3011}'
    )
}

fn is_vertical_border(ch: char) -> bool {
    matches!(ch, '│' | '┃' | '║' | '|')
}

fn unique_fgs(cells: &[StyledCell]) -> Vec<Rgba<u8>> {
    let mut fgs = Vec::new();
    for cell in cells {
        if cell.ch.is_whitespace() || is_box_drawing(cell.ch) {
            continue;
        }
        if !fgs.contains(&cell.fg) {
            fgs.push(cell.fg);
        }
    }
    fgs
}

fn is_box_drawing(ch: char) -> bool {
    matches!(
        ch,
        '│' | '┃' | '╭' | '╮' | '╰' | '╯' | '─' | '━' | '├' | '┤' | '|'
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boxed(prompt: char, body: &str) -> String {
        format!("  ╭──────────────╮\n  │ {prompt} {body}│\n  ╰──────────────╯\n")
    }

    #[test]
    fn empty_box_is_empty() {
        let screen = boxed('❯', "            ");
        assert_eq!(
            composer_state(&screen, BOXED_COMPOSER, &[]),
            ComposerState::Empty
        );
    }

    #[test]
    fn learned_color_then_uniform_other_color_is_placeholder() {
        let typed = "› \x1b[38;2;200;200;210mhello world\x1b[0m\n";
        let draft = sample_draft_fgs(typed, PLAIN_COMPOSER);
        assert_eq!(draft, vec![Rgba([200, 200, 210, 255])]);

        let hint = "› \x1b[38;2;80;90;120mImplement {feature}\x1b[0m\n  deepseek-v4-flash\n";
        assert_eq!(
            composer_state(hint, PLAIN_COMPOSER, &draft),
            ComposerState::Placeholder
        );
    }

    #[test]
    fn leftover_same_color_is_still_draft() {
        let typed = boxed('❯', "\x1b[38;2;180;180;200mstay\x1b[0m");
        let draft = sample_draft_fgs(&typed, BOXED_COMPOSER);
        assert_eq!(
            composer_state(&typed, BOXED_COMPOSER, &draft),
            ComposerState::Draft
        );
    }

    #[test]
    fn missing_chrome_is_missing() {
        assert_eq!(
            composer_state("no prompt here", BOXED_COMPOSER, &[]),
            ComposerState::Missing
        );
    }

    #[test]
    fn prompt_glyph_family_covers_restylings() {
        for glyph in ['>', '›', '❯', '❭', '❮', '»', '→', '⟩', '▶', '⸜'] {
            assert!(is_prompt_glyph(glyph), "{glyph:?} should be a prompt glyph");
        }
        // Box chrome, prose punctuation and alphanumerics never read as prompts.
        for other in ['│', '─', ' ', 'a', '9', '中', '✓', '—', '•', '”', '·'] {
            assert!(!is_prompt_glyph(other), "{other:?} is not a prompt glyph");
        }
    }

    #[test]
    fn bare_composer_matches_any_prompt_glyph() {
        for glyph in ['›', '❯', '❭'] {
            let screen =
                format!("{glyph} \x1b[38;2;200;200;210mhello\x1b[0m\n  deepseek-v4-flash\n");
            let draft = sample_draft_fgs(&screen, PLAIN_COMPOSER);
            assert_eq!(
                draft,
                vec![Rgba([200, 200, 210, 255])],
                "{glyph:?} composer should expose its draft"
            );
        }
    }

    #[test]
    fn footer_below_the_box_is_not_the_composer() {
        // A status footer that happens to start with the same glyph must not
        // win over the real box row.
        let screen = format!(
            "{}\n  ❯ tab to switch mode\n",
            boxed('❯', "\x1b[38;2;180;180;200mstay\x1b[0m")
        );
        let draft = sample_draft_fgs(&screen, BOXED_COMPOSER);
        assert_eq!(
            composer_state(&screen, BOXED_COMPOSER, &draft),
            ComposerState::Draft
        );
    }

    #[test]
    fn typed_prompt_glyph_is_not_resliced() {
        // kimi's prompt is ASCII `>`; a draft containing `>` must keep the whole
        // payload instead of slicing after the last occurrence.
        let screen = boxed('>', "if a > b then");
        let cells = composer_payload(&screen, BOXED_COMPOSER).expect("composer payload");
        let text: String = cells.iter().map(|cell| cell.ch).collect();
        assert_eq!(text.trim(), "if a > b then");
    }

    #[test]
    fn out_of_box_row_is_not_boxed_composer() {
        assert!(!screen_has_composer("  > bare line\n", BOXED_COMPOSER));
        assert!(screen_has_composer("  > bare line\n", PLAIN_COMPOSER));
    }

    /// Screen rows captured from a real codex start-up (2026-09-23): the
    /// update dialog occupies the input area and its selected entry is
    /// glyph-shaped. The ready gate must not fire on it.
    #[test]
    fn startup_option_dialog_is_not_a_composer() {
        let dialog = concat!(
            "  ✨\u{200a}Update available! 0.154.0 -> 0.156.1\n",
            "\n",
            "  Release notes: https://github.com/openai/codex/releases\n",
            "\n",
            "› 1. Update now (runs `npm install -g @openai/codex`)\n",
            "  2. Skip\n",
            "  3. Skip until next version\n",
            "\n",
            "  Press enter to continue\n",
        );
        assert!(
            !screen_has_composer(dialog, PLAIN_COMPOSER),
            "a numbered option list must not satisfy the ready gate"
        );
        // An idle composer is still recognized by the gate.
        assert!(screen_has_composer(
            "› Implement {feature}\n  deepseek-v4-flash\n",
            PLAIN_COMPOSER
        ));
        // The gate is deliberately conservative about numbered payloads, while
        // payload parsing still reads a numbered draft the user actually typed.
        assert!(!screen_has_composer("› 1. do the thing\n", PLAIN_COMPOSER));
        let cells = composer_payload("› 1. do the thing\n", PLAIN_COMPOSER).expect("payload");
        let text: String = cells.iter().map(|cell| cell.ch).collect();
        assert_eq!(text.trim(), "1. do the thing");
    }

    /// Screen rows captured from a live codex composer (2026-09-23). The model /
    /// cwd footer sits directly *below* the input row, so anchoring on the
    /// bottom-most non-empty line would have picked the footer instead.
    #[test]
    fn live_codex_composer_is_found_above_its_footer() {
        let screen = concat!(
            "› Ask Codex to do anything\n",
            "\n",
            "  deepseek-flash xhigh · ~/gitrepo/beam/crates/be…\n",
            "\n",
        );
        assert!(screen_has_composer(screen, PLAIN_COMPOSER));
        let cells = composer_payload(screen, PLAIN_COMPOSER).expect("payload");
        let text: String = cells.iter().map(|cell| cell.ch).collect();
        assert_eq!(text, "Ask Codex to do anything");
    }

    #[test]
    fn queued_badge_counts_as_queue() {
        assert!(screen_mentions_queue("── input · 2 queued ──"));
        assert!(!screen_mentions_queue("hello world"));
    }

    #[test]
    fn submit_look_accepts_empty_without_transcript() {
        let screen = boxed('>', "     ");
        let look = submit_decision(&screen, BOXED_COMPOSER, &[], &mut || Ok(false))
            .unwrap()
            .look;
        assert_eq!(look, SubmitLook::Accepted);
    }

    #[test]
    fn submit_look_holds_when_composer_missing() {
        let look = submit_decision("plain", BOXED_COMPOSER, &[], &mut || Ok(false))
            .unwrap()
            .look;
        assert_eq!(look, SubmitLook::Hold);
    }

    // -----------------------------------------------------------------------
    // Live test: drive the real CLI through the zellij backend and check the
    // structural composer detection against real bytes (which prompt glyph the
    // TUI actually renders, and that footers below the input do not win).
    //
    // Requires a locally installed and authenticated `codex` plus `zellij` on
    // PATH. Runs inside the repository so the CLI does not stop on a first-run
    // "trust this folder" prompt; the zellij session it creates is deleted on
    // drop.
    //
    // Run manually with:
    //   cargo test -p beam-worker live_composer -- --ignored --nocapture
    // -----------------------------------------------------------------------

    fn live_has_command(name: &str) -> bool {
        std::process::Command::new("which")
            .arg(name)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    struct LiveZellijSession {
        name: String,
    }

    impl Drop for LiveZellijSession {
        fn drop(&mut self) {
            let _ = std::process::Command::new("zellij")
                .args(["delete-session", &self.name, "-f"])
                .output();
        }
    }

    /// Bottom-most row the structural detector accepts, as plain text.
    fn detected_composer_row(screen: &str, hint: ComposerHint) -> Option<String> {
        parse_ansi_screen(screen).into_iter().rev().find_map(|row| {
            prompt_cell_index(&row, hint)
                .map(|_| row.iter().map(|cell| cell.ch).collect::<String>())
        })
    }

    /// Test-only: whether the bottom-most glyph-shaped row is a numbered option,
    /// i.e. a start-up dialog owns the input area.
    fn screen_shows_option_row(screen: &str) -> bool {
        parse_ansi_screen(screen)
            .into_iter()
            .rev()
            .find_map(|row| prompt_cell_index(&row, PLAIN_COMPOSER).map(|index| (row, index)))
            .is_some_and(|(row, index)| looks_like_option_row(&row, index))
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "live test: requires locally installed and authenticated `codex` and `zellij`"]
    async fn live_codex_composer_row_detection() {
        use crate::backend::{SessionBackend, SpawnOpts, ZellijBackend};
        use uuid::Uuid;

        if !live_has_command("codex") || !live_has_command("zellij") {
            eprintln!("skipping live test: `codex` or `zellij` not found in PATH");
            return;
        }
        let workspace = std::env::current_dir().expect("cwd");
        let name = format!("beam-live-composer-{}", &Uuid::new_v4().to_string()[..8]);
        let _guard = LiveZellijSession { name: name.clone() };

        let backend = ZellijBackend::new(name);
        backend
            .spawn(
                "codex",
                &[
                    "--dangerously-bypass-approvals-and-sandbox".to_string(),
                    "--no-alt-screen".to_string(),
                ],
                SpawnOpts {
                    cwd: workspace.display().to_string(),
                    cols: 120,
                    rows: 40,
                    env: vec![],
                },
            )
            .await
            .expect("spawn codex through the zellij backend");

        let mut detected = None;
        let mut last_screen = String::new();
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let screen = backend.capture_viewport().await.unwrap_or_default();
            last_screen = screen.clone();
            if screen_has_composer(&screen, PLAIN_COMPOSER) {
                detected = detected_composer_row(&screen, PLAIN_COMPOSER);
                if detected.is_some() {
                    break;
                }
            }
            // A start-up dialog (update prompt, trust prompt) owns the input
            // area until it is answered; dismiss it and keep waiting.
            if screen_shows_option_row(&screen) {
                println!("dismissing a start-up dialog before the composer appears");
                let _ = backend.send_special_keys(&["Escape".to_string()]).await;
            }
        }
        for row in parse_ansi_screen(&last_screen).iter().rev().take(12).rev() {
            let text: String = row.iter().map(|cell| cell.ch).collect();
            println!("ROW {:?}", text);
        }
        let row = detected.unwrap_or_else(|| {
            panic!("structural detection never matched the live codex composer")
        });
        let glyph = row
            .chars()
            .find(|ch| is_prompt_glyph(*ch))
            .expect("detected row has no prompt glyph");
        println!(
            "live codex composer row = {row:?} (glyph {:?} = U+{:04X})",
            glyph, glyph as u32
        );
        assert!(
            is_prompt_glyph(glyph),
            "detected glyph {glyph:?} is outside the prompt family"
        );
    }
}
