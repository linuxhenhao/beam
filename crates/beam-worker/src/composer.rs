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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ComposerHint {
    pub prompt: char,
    pub boxed: bool,
}

pub(crate) const GROK_COMPOSER: ComposerHint = ComposerHint {
    prompt: '❯',
    boxed: true,
};
pub(crate) const KIMI_COMPOSER: ComposerHint = ComposerHint {
    prompt: '>',
    boxed: true,
};
pub(crate) const CODEX_COMPOSER: ComposerHint = ComposerHint {
    prompt: '›',
    boxed: false,
};

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
        debug!(
            prompt = %hint.prompt,
            boxed = hint.boxed,
            "composer draft sample: chrome missing"
        );
        return Vec::new();
    };
    let fgs = unique_fgs(&cells);
    debug!(
        prompt = %hint.prompt,
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
        prompt = %hint.prompt,
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
    let line = rows.into_iter().rev().find(|row| line_matches(row, hint))?;
    let prompt_at = line.iter().rposition(|cell| cell.ch == hint.prompt)?;
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

fn line_matches(row: &[StyledCell], hint: ComposerHint) -> bool {
    let has_prompt = row.iter().any(|cell| cell.ch == hint.prompt);
    if !has_prompt {
        return false;
    }
    if hint.boxed {
        return row.iter().any(|cell| cell.ch == '│' || cell.ch == '|');
    }
    true
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
            composer_state(&screen, GROK_COMPOSER, &[]),
            ComposerState::Empty
        );
    }

    #[test]
    fn learned_color_then_uniform_other_color_is_placeholder() {
        let typed = boxed('›', "\x1b[38;2;200;200;210mhello world\x1b[0m");
        let draft = sample_draft_fgs(&typed, CODEX_COMPOSER);
        assert_eq!(draft, vec![Rgba([200, 200, 210, 255])]);

        let hint = "› \x1b[38;2;80;90;120mImplement {feature}\x1b[0m\n  deepseek-v4-flash\n";
        assert_eq!(
            composer_state(hint, CODEX_COMPOSER, &draft),
            ComposerState::Placeholder
        );
    }

    #[test]
    fn leftover_same_color_is_still_draft() {
        let typed = boxed('❯', "\x1b[38;2;180;180;200mstay\x1b[0m");
        let draft = sample_draft_fgs(&typed, GROK_COMPOSER);
        assert_eq!(
            composer_state(&typed, GROK_COMPOSER, &draft),
            ComposerState::Draft
        );
    }

    #[test]
    fn missing_chrome_is_missing() {
        assert_eq!(
            composer_state("no prompt here", GROK_COMPOSER, &[]),
            ComposerState::Missing
        );
    }

    #[test]
    fn queued_badge_counts_as_queue() {
        assert!(screen_mentions_queue("── input · 2 queued ──"));
        assert!(!screen_mentions_queue("hello world"));
    }

    #[test]
    fn submit_look_accepts_empty_without_transcript() {
        let screen = boxed('>', "     ");
        let look = submit_decision(&screen, KIMI_COMPOSER, &[], &mut || Ok(false))
            .unwrap()
            .look;
        assert_eq!(look, SubmitLook::Accepted);
    }

    #[test]
    fn submit_look_holds_when_composer_missing() {
        let look = submit_decision("plain", GROK_COMPOSER, &[], &mut || Ok(false))
            .unwrap()
            .look;
        assert_eq!(look, SubmitLook::Hold);
    }
}
