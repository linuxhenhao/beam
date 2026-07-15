//! Source resolution for opencode sessions.
//!
//! Resolves the correct opencode transcript source by checking:
//! 1. Expected session ID (exact match)
//! 2. Adopted PID (cmdline /proc parsing + liveness gate)
//! 3. Recent CLI log discovery
//! 4. Directory-based candidate fallback

use std::path::PathBuf;
use std::time::Duration;

use crate::adapter::{OpenCodeState, ResolveOutcome};

use super::transcript::{
    find_all_opencode_sessions_by_directory, find_opencode_session_by_id,
    find_opencode_session_row_by_id, opencode_db_candidates,
};
use super::types::OpenCodeSourceResolution;

// ---------------------------------------------------------------------------
// Main resolution entry point
// ---------------------------------------------------------------------------

pub(crate) fn current_source(state: &OpenCodeState) -> OpenCodeSourceResolution {
    let db_paths = opencode_db_candidates(&state.data_dir);
    if let Some(source) =
        find_opencode_session_by_id(state.expected_session_id.as_deref(), &db_paths)
    {
        return ResolveOutcome::Found(source);
    }
    // When adopting, try stronger PID-based resolution; if that cannot
    // produce a definitive answer, fall through to normal cwd disambiguation
    // (including screen/transcript scoring in write_input).
    if let Some(filtered) = try_adopted_pid_filter(state, &db_paths) {
        return filtered;
    }
    if let Some(source) = resolve_opencode_session_via_logs(state, &db_paths) {
        return ResolveOutcome::Found(source);
    }
    let candidates = find_all_opencode_sessions_by_directory(Some(&state.working_dir), &db_paths);
    match candidates.len() {
        0 => ResolveOutcome::NotFound {
            reason: "OpenCode transcript source not found".to_string(),
        },
        1 => ResolveOutcome::Found(candidates.into_iter().next().unwrap()),
        n => ResolveOutcome::Ambiguous {
            candidates,
            reason: format!(
                "OpenCode transcript source ambiguous: {} sessions in directory {}",
                n, state.working_dir
            ),
        },
    }
}

pub(crate) async fn wait_for_source(state: &OpenCodeState) -> OpenCodeSourceResolution {
    for _ in 0..12 {
        let resolution = current_source(state);
        if matches!(resolution, ResolveOutcome::Found(_)) {
            return resolution;
        }
        if matches!(resolution, ResolveOutcome::Ambiguous { .. }) {
            return resolution;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    current_source(state)
}

// ---------------------------------------------------------------------------
// Adopted-PID resolution (Linux only)
// ---------------------------------------------------------------------------

/// Parse an opencode session id from `/proc/<pid>/cmdline` by looking for a
/// `--session <id>` argument pair.
///
/// Returns `None` when the flag is absent; this is common for sessions that
/// were started without an explicit `--session`.
#[cfg(target_os = "linux")]
fn try_resolve_opencode_session_via_cmdline(pid: u32) -> Option<String> {
    let raw = std::fs::read(format!("/proc/{}/cmdline", pid)).ok()?;
    parse_session_from_cmdline(&raw)
}

/// Pure parser: extract `--session <id>` from null-separated cmdline bytes.
pub(crate) fn parse_session_from_cmdline(raw: &[u8]) -> Option<String> {
    let args: Vec<&str> = raw
        .split(|b| *b == 0)
        .filter_map(|b| std::str::from_utf8(b).ok())
        .filter(|s| !s.is_empty())
        .collect();
    for window in args.windows(2) {
        if window[0] == "--session" {
            return Some(window[1].to_string());
        }
    }
    None
}

/// When the worker was initialised for an adopted opencode process (the
/// `adopted_pid` field is set), attempt to resolve the session by stronger
/// signals than directory-based candidate collection alone.
///
/// Resolution order (on Linux):
/// 1. **Strong mapping**: read `/proc/<pid>/cmdline`, look for `--session <id>`.
///    If found, look up that exact session id — this is a reliable PID→session
///    link.  Return `Found` or `NotFound`.
/// 2. **Liveness gate**: if `/proc/<pid>` does not exist the adopted process
///    has died.  Return `NotFound` to fail safe rather than silently binding an
///    arbitrary historical session.
/// 3. **No mapping, but alive**: fall through by returning `None`.  The caller
///    will proceed with normal directory-based disambiguation (including
///    screen-vs-transcript scoring in [`write_input`]) — it will *not* auto-select
///    a single candidate just because the PID is alive.
///
/// On non-Linux we cannot verify liveness; the function returns `None` and
/// normal directory disambiguation is used.
#[cfg(target_os = "linux")]
pub(crate) fn try_adopted_pid_filter(
    state: &OpenCodeState,
    db_paths: &[PathBuf],
) -> Option<OpenCodeSourceResolution> {
    let pid = state.adopted_pid?;

    // 1. Strong PID→session mapping via --session in cmdline.
    if let Some(session_id) = try_resolve_opencode_session_via_cmdline(pid) {
        if let Some(source) = find_opencode_session_by_id(Some(&session_id), db_paths) {
            return Some(ResolveOutcome::Found(source));
        }
        return Some(ResolveOutcome::NotFound {
            reason: format!(
                "opencode session {} (from pid {} cmdline) not found in db",
                session_id, pid
            ),
        });
    }

    // 2. Liveness gate: dead process → fail safe.
    if !is_process_alive(pid) {
        return Some(ResolveOutcome::NotFound {
            reason: format!(
                "adopted opencode process (pid {}) is no longer running",
                pid
            ),
        });
    }

    // 3. Alive but no reliable session mapping → do NOT auto-select.
    // Fall through to normal directory-based disambiguation.
    None
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn try_adopted_pid_filter(
    _state: &OpenCodeState,
    _db_paths: &[PathBuf],
) -> Option<OpenCodeSourceResolution> {
    // Cannot verify process liveness on non-Linux; fall through to
    // normal directory-based disambiguation.
    None
}

#[cfg(target_os = "linux")]
pub(crate) fn is_process_alive(pid: u32) -> bool {
    std::path::Path::new(&format!("/proc/{}", pid)).exists()
}

// ---------------------------------------------------------------------------
// CLI log discovery
// ---------------------------------------------------------------------------

fn recent_opencode_session_ids(log_dir: &PathBuf) -> Vec<String> {
    use std::collections::HashSet;

    let Ok(entries) = std::fs::read_dir(log_dir) else {
        return Vec::new();
    };
    let mut files: Vec<(u128, PathBuf)> = entries
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("log") {
                return None;
            }
            let modified = entry
                .metadata()
                .ok()
                .and_then(|meta| meta.modified().ok())
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|duration| duration.as_nanos())
                .unwrap_or(0);
            Some((modified, path))
        })
        .collect();
    files.sort_by(|a, b| b.0.cmp(&a.0));

    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for (_, path) in files {
        let Ok(content) = std::fs::read_to_string(&path) else {
            continue;
        };
        for line in content.lines().rev() {
            let mut search_start = 0;
            while let Some(pos) = line[search_start..].find("session.id=") {
                let id_start = search_start + pos + "session.id=".len();
                let id = line[id_start..]
                    .chars()
                    .take_while(|ch| {
                        !ch.is_whitespace()
                            && *ch != '"'
                            && *ch != '\''
                            && *ch != ','
                            && *ch != ')'
                            && *ch != ']'
                            && *ch != '}'
                    })
                    .collect::<String>();
                if !id.is_empty() && seen.insert(id.clone()) {
                    ids.push(id);
                }
                search_start = id_start;
            }
        }
    }
    ids
}

fn session_matches_working_dir_and_is_root(
    row: &super::types::OpenCodeSessionRow,
    working_dir: &str,
) -> bool {
    row.directory == working_dir && row.time_archived.is_none() && row.parent_id.is_none()
}

fn resolve_opencode_session_via_logs(
    state: &OpenCodeState,
    db_paths: &[PathBuf],
) -> Option<super::types::OpenCodeTranscriptSource> {
    let log_dir = state.data_dir.join("log");
    for session_id in recent_opencode_session_ids(&log_dir) {
        if let Some((db_path, row)) = find_opencode_session_row_by_id(Some(&session_id), db_paths)
            && session_matches_working_dir_and_is_root(&row, &state.working_dir)
        {
            return Some(super::types::OpenCodeTranscriptSource {
                db_path,
                session_id: row.id,
            });
        }
    }
    None
}
