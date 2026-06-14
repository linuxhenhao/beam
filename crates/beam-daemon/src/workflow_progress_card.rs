use beam_core::{ActivityStatus, NodeStatus, RunSnapshotDTO, RunStatus};
use serde_json::json;

pub fn build_workflow_progress_card(
    snapshot: &RunSnapshotDTO,
    _run_id: &str,
    workflow_id: &str,
) -> serde_json::Value {
    let total_nodes = snapshot.nodes.len();
    let completed = snapshot
        .nodes
        .iter()
        .filter(|n| {
            matches!(
                n.status,
                NodeStatus::Succeeded
                    | NodeStatus::Skipped
                    | NodeStatus::Failed
                    | NodeStatus::Cancelled
            )
        })
        .count();

    let progress_text = format!("{}/{} nodes completed", completed, total_nodes);

    let status_emoji = match &snapshot.run.status {
        RunStatus::Running => "\u{1f504}",
        RunStatus::Succeeded => "\u{2705}",
        RunStatus::Failed => "\u{274c}",
        RunStatus::Cancelled => "\u{23f9}\u{fe0f}",
        _ => "\u{23f3}",
    };

    let status_color = match &snapshot.run.status {
        RunStatus::Running => "blue",
        RunStatus::Succeeded => "green",
        RunStatus::Failed => "red",
        RunStatus::Cancelled => "grey",
        _ => "default",
    };

    let running_nodes: Vec<String> = snapshot
        .nodes
        .iter()
        .filter(|n| n.status == NodeStatus::Running)
        .map(|n| format!("  \u{2022} {}", n.node_id))
        .collect();

    let waiting_nodes: Vec<String> = snapshot
        .nodes
        .iter()
        .filter(|n| n.status == NodeStatus::Waiting)
        .map(|n| format!("  \u{2022} {} (awaiting approval)", n.node_id))
        .collect();

    let mut elements = Vec::new();

    let header_text = format!("{} Workflow Run: {}", status_emoji, workflow_id);
    if !running_nodes.is_empty() {
        let running_str = running_nodes.join("\n");
        elements.push(json!({
            "tag": "markdown",
            "content": format!("{}\n\nRunning:\n{}", header_text, running_str),
        }));
    } else if !waiting_nodes.is_empty() {
        let waiting_str = waiting_nodes.join("\n");
        elements.push(json!({
            "tag": "markdown",
            "content": format!("{}\n\nAwaiting:\n{}", header_text, waiting_str),
        }));
    } else {
        elements.push(json!({
            "tag": "markdown",
            "content": header_text,
        }));
    }

    elements.push(json!({
        "tag": "hr",
    }));

    elements.push(json!({
        "tag": "markdown",
        "content": progress_text,
    }));

    if !snapshot.activities.is_empty() {
        let activity_lines: Vec<String> = snapshot
            .activities
            .iter()
            .map(|a| {
                let emoji = match a.status {
                    ActivityStatus::Running => "\u{23f3}",
                    ActivityStatus::Succeeded => "\u{2705}",
                    ActivityStatus::Failed => "\u{274c}",
                    ActivityStatus::Cancelled => "\u{23f9}\u{fe0f}",
                    ActivityStatus::Waiting => "\u{23f8}\u{fe0f}",
                    _ => "\u{2b1c}",
                };
                format!("  {} {}", emoji, a.activity_id)
            })
            .collect();
        elements.push(json!({
            "tag": "markdown",
            "content": format!("Activities:\n{}", activity_lines.join("\n")),
        }));
    }

    json!({
        "config": { "wide_screen_mode": true },
        "header": {
            "template": status_color,
            "title": {
                "tag": "plain_text",
                "content": format!("Workflow {}", workflow_id),
            },
        },
        "elements": elements,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use beam_core::{ActivityState, DanglingSnapshot, NodeState, RunState};
    use std::collections::BTreeMap;

    fn mock_snapshot(
        run_id: &str,
        status: RunStatus,
        nodes: Vec<NodeState>,
        activities: Vec<ActivityState>,
    ) -> RunSnapshotDTO {
        RunSnapshotDTO {
            run_id: run_id.to_string(),
            run: RunState {
                run_id: run_id.to_string(),
                status,
                workflow_id: Some("flow-test".to_string()),
                revision_id: Some("rev-1".to_string()),
                initiator: Some("cli".to_string()),
                input: None,
                output: None,
                failed_node_id: None,
                root_cause_event_id: None,
                cancel_origin_event_id: None,
                bot_snapshots: None,
                cancelled_run_intent: None,
                cancelled_node_intents: BTreeMap::new(),
            },
            last_seq: 1,
            nodes,
            activities,
            loops: None,
            dangling: DanglingSnapshot {
                activities: vec![],
                effect_attempted: vec![],
                waits: vec![],
                wait_resolutions: vec![],
                cancels: vec![],
            },
            outputs: BTreeMap::new(),
            attempt_io: BTreeMap::new(),
            chat_binding: None,
            updated_at: 1_700_000_000_000,
        }
    }

    fn node(id: &str, status: NodeStatus) -> NodeState {
        NodeState {
            node_id: id.to_string(),
            status,
            activity_id: None,
            retry_count: 0,
            next_attempt_at: None,
            error_class: None,
            condition_event_id: None,
            cancel_origin_event_id: None,
        }
    }

    fn activity(id: &str, status: ActivityStatus) -> ActivityState {
        ActivityState {
            activity_id: id.to_string(),
            attempts: vec![],
            status,
            current_attempt_id: None,
            owner_node_id: None,
        }
    }

    #[test]
    fn card_shows_running_status_with_running_nodes() {
        let snap = mock_snapshot(
            "run-1",
            RunStatus::Running,
            vec![
                node("a", NodeStatus::Running),
                node("b", NodeStatus::Running),
                node("c", NodeStatus::Succeeded),
            ],
            vec![
                activity("act-a", ActivityStatus::Running),
                activity("act-b", ActivityStatus::Running),
                activity("act-c", ActivityStatus::Succeeded),
            ],
        );
        let card = build_workflow_progress_card(&snap, "run-1", "flow-test");

        assert_eq!(card["header"]["template"], "blue");
        assert_eq!(card["header"]["title"]["content"], "Workflow flow-test");

        let content0 = card["elements"][0]["content"].as_str().unwrap();
        assert!(content0.contains("\u{1f504}"));
        assert!(content0.contains("Workflow Run: flow-test"));
        assert!(content0.contains("Running:"));
        assert!(content0.contains("a"));
        assert!(content0.contains("b"));
        assert!(!content0.contains("c"));

        let progress = card["elements"][2]["content"].as_str().unwrap();
        assert_eq!(progress, "1/3 nodes completed");

        let activities_text = card["elements"][3]["content"].as_str().unwrap();
        assert!(activities_text.contains("act-a"));
        assert!(activities_text.contains("act-b"));
        assert!(activities_text.contains("act-c"));
    }

    #[test]
    fn card_shows_succeeded_status() {
        let snap = mock_snapshot(
            "run-2",
            RunStatus::Succeeded,
            vec![
                node("a", NodeStatus::Succeeded),
                node("b", NodeStatus::Succeeded),
            ],
            vec![
                activity("act-a", ActivityStatus::Succeeded),
                activity("act-b", ActivityStatus::Succeeded),
            ],
        );
        let card = build_workflow_progress_card(&snap, "run-2", "flow-test");

        assert_eq!(card["header"]["template"], "green");
        let content0 = card["elements"][0]["content"].as_str().unwrap();
        assert!(content0.contains("\u{2705}"));
        assert!(!content0.contains("Running:"));
        assert!(!content0.contains("Awaiting:"));

        let progress = card["elements"][2]["content"].as_str().unwrap();
        assert_eq!(progress, "2/2 nodes completed");

        let activities_text = card["elements"][3]["content"].as_str().unwrap();
        assert!(activities_text.contains("\u{2705} act-a"));
        assert!(activities_text.contains("\u{2705} act-b"));
    }

    #[test]
    fn card_shows_failed_status() {
        let snap = mock_snapshot(
            "run-3",
            RunStatus::Failed,
            vec![
                node("a", NodeStatus::Succeeded),
                node("b", NodeStatus::Failed),
            ],
            vec![
                activity("act-a", ActivityStatus::Succeeded),
                activity("act-b", ActivityStatus::Failed),
            ],
        );
        let card = build_workflow_progress_card(&snap, "run-3", "flow-test");

        assert_eq!(card["header"]["template"], "red");
        let content0 = card["elements"][0]["content"].as_str().unwrap();
        assert!(content0.contains("\u{274c}"));

        let activities_text = card["elements"][3]["content"].as_str().unwrap();
        assert!(activities_text.contains("\u{274c} act-b"));
    }

    #[test]
    fn card_shows_cancelled_status() {
        let snap = mock_snapshot(
            "run-4",
            RunStatus::Cancelled,
            vec![node("a", NodeStatus::Cancelled)],
            vec![activity("act-a", ActivityStatus::Cancelled)],
        );
        let card = build_workflow_progress_card(&snap, "run-4", "flow-test");

        assert_eq!(card["header"]["template"], "grey");
        assert_eq!(card["elements"][2]["content"], "1/1 nodes completed");
    }

    #[test]
    fn card_shows_waiting_nodes() {
        let snap = mock_snapshot(
            "run-5",
            RunStatus::Running,
            vec![node("gate", NodeStatus::Waiting)],
            vec![],
        );
        let card = build_workflow_progress_card(&snap, "run-5", "flow-test");

        let content0 = card["elements"][0]["content"].as_str().unwrap();
        assert!(content0.contains("Awaiting:"));
        assert!(content0.contains("gate"));
        assert!(content0.contains("awaiting approval"));
    }

    #[test]
    fn card_progress_counts_all_terminal_nodes() {
        let snap = mock_snapshot(
            "run-6",
            RunStatus::Running,
            vec![
                node("a", NodeStatus::Succeeded),
                node("b", NodeStatus::Failed),
                node("c", NodeStatus::Cancelled),
                node("d", NodeStatus::Skipped),
                node("e", NodeStatus::Running),
            ],
            vec![],
        );
        let card = build_workflow_progress_card(&snap, "run-6", "flow-test");

        let progress = card["elements"][2]["content"].as_str().unwrap();
        assert_eq!(progress, "4/5 nodes completed");
    }
}
