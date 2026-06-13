use std::fs;
use std::path::Path;

use anyhow::Result;

use crate::WorkflowEventEnvelope;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventWindow {
    pub events: Vec<WorkflowEventEnvelope>,
    pub oldest_seq: Option<u64>,
    pub newest_seq: Option<u64>,
    pub total_count: usize,
    pub has_older: bool,
    pub has_newer: bool,
}

const DEFAULT_LIMIT: usize = 200;
const MAX_LIMIT: usize = 1000;
const DEFAULT_TAIL: usize = 100;

pub fn event_seq_from_id(event_id: &str) -> u64 {
    event_id
        .rsplit_once('-')
        .and_then(|(_, seq)| seq.parse::<u64>().ok())
        .unwrap_or(0)
}

pub fn infer_run_status(events: &[WorkflowEventEnvelope]) -> String {
    for ev in events.iter().rev() {
        match ev.event_type.as_str() {
            "runCanceled" => return "cancelled".to_string(),
            "runSucceeded" => return "succeeded".to_string(),
            "runFailed" => return "failed".to_string(),
            "runStarted" => return "running".to_string(),
            "runCreated" => return "pending".to_string(),
            _ => {}
        }
    }
    "pending".to_string()
}

pub fn read_run_events_pure(run_dir: &Path) -> Result<Option<Vec<WorkflowEventEnvelope>>> {
    let file = run_dir.join("events.ndjson");
    let raw = match fs::read_to_string(&file) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let mut events = Vec::new();
    for line in raw.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed = serde_json::from_str::<WorkflowEventEnvelope>(line);
        match parsed {
            Ok(ev) => events.push(ev),
            Err(_) => return Ok(None),
        }
    }
    Ok(Some(events))
}

pub fn read_event_window(events: &[WorkflowEventEnvelope], opts: EventWindowOpts) -> EventWindow {
    let total = events.len();
    if total == 0 {
        return EventWindow {
            events: Vec::new(),
            oldest_seq: None,
            newest_seq: None,
            total_count: 0,
            has_older: false,
            has_newer: false,
        };
    }
    let limit = clamp_limit(opts.limit);
    if let Some(after_seq) = opts.after_seq {
        let idx = events
            .iter()
            .position(|e| event_seq_from_id(&e.event_id) > after_seq)
            .unwrap_or(total);
        let slice = events[idx..std::cmp::min(idx + limit, total)].to_vec();
        return EventWindow {
            oldest_seq: slice.first().map(|e| event_seq_from_id(&e.event_id)),
            newest_seq: slice.last().map(|e| event_seq_from_id(&e.event_id)),
            has_older: idx > 0,
            has_newer: idx + slice.len() < total,
            total_count: total,
            events: slice,
        };
    }
    if let Some(before_seq) = opts.before_seq {
        let end_idx = events
            .iter()
            .position(|e| event_seq_from_id(&e.event_id) >= before_seq)
            .unwrap_or(total);
        let start_idx = end_idx.saturating_sub(limit);
        let slice = events[start_idx..end_idx].to_vec();
        return EventWindow {
            oldest_seq: slice.first().map(|e| event_seq_from_id(&e.event_id)),
            newest_seq: slice.last().map(|e| event_seq_from_id(&e.event_id)),
            has_older: start_idx > 0,
            has_newer: end_idx < total,
            total_count: total,
            events: slice,
        };
    }
    let start_idx = total.saturating_sub(opts.tail.unwrap_or(DEFAULT_TAIL).clamp(1, MAX_LIMIT));
    let slice = events[start_idx..].to_vec();
    EventWindow {
        oldest_seq: slice.first().map(|e| event_seq_from_id(&e.event_id)),
        newest_seq: slice.last().map(|e| event_seq_from_id(&e.event_id)),
        has_older: start_idx > 0,
        has_newer: false,
        total_count: total,
        events: slice,
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EventWindowOpts {
    pub tail: Option<usize>,
    pub before_seq: Option<u64>,
    pub after_seq: Option<u64>,
    pub limit: Option<usize>,
}

fn clamp_limit(raw: Option<usize>) -> usize {
    match raw {
        Some(0) | None => DEFAULT_LIMIT,
        Some(v) => v.min(MAX_LIMIT),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(event_id: &str, event_type: &str) -> WorkflowEventEnvelope {
        WorkflowEventEnvelope {
            event_id: event_id.to_string(),
            run_id: "run-1".to_string(),
            timestamp: 1,
            schema_version: 1,
            actor: crate::WorkflowActor::System,
            event_type: event_type.to_string(),
            payload: serde_json::json!({}),
            payload_hash: None,
        }
    }

    #[test]
    fn infer_run_status_prefers_latest_terminal_event() {
        let events = vec![ev("run-1-1", "runCreated"), ev("run-1-2", "runCanceled")];
        assert_eq!(infer_run_status(&events), "cancelled");
    }

    #[test]
    fn read_event_window_supports_tail_and_after_seq() {
        let events = vec![
            ev("run-1-1", "runCreated"),
            ev("run-1-2", "runStarted"),
            ev("run-1-3", "nodeWaiting"),
            ev("run-1-4", "runSucceeded"),
        ];
        let tail = read_event_window(
            &events,
            EventWindowOpts {
                tail: Some(2),
                before_seq: None,
                after_seq: None,
                limit: None,
            },
        );
        assert_eq!(tail.events.len(), 2);
        assert_eq!(tail.oldest_seq, Some(3));
        assert_eq!(tail.newest_seq, Some(4));

        let after = read_event_window(
            &events,
            EventWindowOpts {
                tail: None,
                before_seq: None,
                after_seq: Some(2),
                limit: Some(2),
            },
        );
        assert_eq!(after.events.len(), 2);
        assert_eq!(after.oldest_seq, Some(3));
        assert_eq!(after.newest_seq, Some(4));
    }
}
