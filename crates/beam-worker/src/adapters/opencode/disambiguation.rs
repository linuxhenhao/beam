//! Screen vs transcript disambiguation for opencode sessions.
//!
//! When multiple opencode sessions match the working directory, the adapter
//! captures the current terminal viewport and scores it against each session's
//! transcript tail.  A clear winner auto-selects the session; weak / near-tie
//! scores keep the ambiguity for the user.

use std::collections::HashSet;

use anyhow::Result;
use tracing::info;

use super::OpenCodeState;
use crate::backend::SessionBackend;

use super::transcript::read_transcript_tail;
use super::types::{
    MIN_DISAMBIGUATION_SCORE, MIN_SCORE_LEAD_RATIO, OpenCodeTranscriptSource, SCREEN_TAIL_CHARS,
    TRANSCRIPT_TAIL_CHARS,
};

/// Try to resolve an ambiguous multi-candidate situation by comparing the
/// current TUI viewport content against each candidate's transcript tail.
///
/// Returns `Some(source)` when one candidate clearly stands out; returns
/// `None` when scores are too low or too close to call.
pub(crate) async fn disambiguate_by_screen(
    _state: &OpenCodeState,
    backend: &dyn SessionBackend,
    candidates: &[OpenCodeTranscriptSource],
) -> Result<Option<OpenCodeTranscriptSource>> {
    if candidates.len() < 2 {
        return Ok(candidates.first().cloned());
    }

    let screen = backend.capture_viewport().await.unwrap_or_default();
    let screen_tail = normalize_for_scoring(&screen, SCREEN_TAIL_CHARS);
    if screen_tail.is_empty() {
        return Ok(None);
    }

    let mut scored: Vec<(f64, &OpenCodeTranscriptSource)> = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let transcript_text = read_transcript_tail(candidate).unwrap_or_default();
        let transcript_tail = normalize_for_scoring(&transcript_text, TRANSCRIPT_TAIL_CHARS);
        let score = score_texts(&screen_tail, &transcript_tail);
        scored.push((score, candidate));
    }

    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    let &(top_score, top_source) = &scored[0];

    let score_too_low = top_score < MIN_DISAMBIGUATION_SCORE;
    let score_gap_too_small = scored.len() > 1 && {
        let second_score = scored[1].0;
        second_score > 0.0 && top_score / second_score < MIN_SCORE_LEAD_RATIO
    };

    if score_too_low || score_gap_too_small {
        info!(
            adapter = "opencode",
            candidate_count = candidates.len(),
            "screen disambiguation rejected: top_score={:.3}<{:.3} or scores too close",
            top_score,
            MIN_DISAMBIGUATION_SCORE
        );
        return Ok(None);
    }

    info!(
        adapter = "opencode",
        candidate_count = candidates.len(),
        transcript_session = %top_source.session_id,
        "screen disambiguation selected (score={:.3}, lead={:.2}x)",
        top_score,
        top_score / scored.get(1).map(|s| s.0).unwrap_or(1.0)
    );

    Ok(Some(top_source.clone()))
}

// ---------------------------------------------------------------------------
// Scoring helpers
// ---------------------------------------------------------------------------

/// Strip ANSI escape sequences (CSI + OSC) from terminal output.
pub(crate) fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\x1b' {
            out.push(ch);
            continue;
        }
        match chars.peek() {
            Some('[') => {
                chars.next(); // consume '['
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
            }
            Some(']') => {
                chars.next(); // consume ']'
                while let Some(&c) = chars.peek() {
                    chars.next();
                    if c == '\x07' || (c == '\x1b' && chars.peek() == Some(&'\\')) {
                        if c == '\x1b' {
                            chars.next(); // consume '\\'
                        }
                        break;
                    }
                }
            }
            _ => {
                // unrecognized escape – consume the ESC itself
            }
        }
    }
    out
}

/// Normalize text for similarity scoring: strip ANSI, collapse whitespace, truncate to tail.
///
/// `tail_chars` counts Rust `char`s (Unicode scalar values), not bytes, so
/// multi-byte sequences (Chinese, emoji, etc.) are safe.
pub(crate) fn normalize_for_scoring(text: &str, tail_chars: usize) -> String {
    let plain = strip_ansi(text);
    let collapsed: String = plain.split_whitespace().collect::<Vec<_>>().join(" ");
    let char_count = collapsed.chars().count();
    if char_count <= tail_chars {
        return collapsed;
    }
    // Take the suffix of approximately tail_chars characters.
    let skip = char_count - tail_chars;
    let tail: String = collapsed.chars().skip(skip).collect();
    // Prefer to start at a word boundary: skip the first (possibly partial)
    // word and start after the next space.
    if let Some(space_pos) = tail.find(' ') {
        // `find(' ')` returns a byte index – safe because `tail` was built
        // via `collect()` and `space_pos` + 1 always lands on a char boundary.
        tail[space_pos + 1..].to_string()
    } else {
        tail
    }
}

/// Compute a combined similarity score between two normalized text blobs.
/// Uses Jaccard on word sets (0.5 weight) + bigram overlap (0.5 weight).
fn score_texts(screen: &str, transcript: &str) -> f64 {
    if screen.is_empty() || transcript.is_empty() {
        return 0.0;
    }

    let sw: Vec<&str> = screen.split_whitespace().collect();
    let tw: Vec<&str> = transcript.split_whitespace().collect();
    if sw.is_empty() || tw.is_empty() {
        return 0.0;
    }

    let screen_set: HashSet<&str> = sw.iter().copied().collect();
    let transcript_set: HashSet<&str> = tw.iter().copied().collect();
    let inter = screen_set.intersection(&transcript_set).count() as f64;
    let union = screen_set.union(&transcript_set).count() as f64;
    let jaccard = inter / union.max(1.0);

    let screen_bigrams: HashSet<(&str, &str)> = sw.windows(2).map(|w| (w[0], w[1])).collect();
    let transcript_bigrams: HashSet<(&str, &str)> = tw.windows(2).map(|w| (w[0], w[1])).collect();
    let bg_inter = screen_bigrams.intersection(&transcript_bigrams).count() as f64;
    let bg_union = screen_bigrams.union(&transcript_bigrams).count() as f64;
    let bigram = bg_inter / bg_union.max(1.0);

    (jaccard + bigram) / 2.0
}
