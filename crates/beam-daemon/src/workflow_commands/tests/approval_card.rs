//! Tests for [`crate::workflow_approval_target_message_id`] — determines
//! which message ID to use for workflow approval card operations.

use crate::{ParsedLarkCardAction, workflow_approval_target_message_id};

#[test]
fn workflow_approval_target_message_id_prefers_clicked_message() {
    let action = ParsedLarkCardAction {
        action: "wf_approve".to_string(),
        session_id: None,
        root_id: Some("om_root".to_string()),
        clicked_message_id: Some("om_clicked".to_string()),
        operator_open_id: Some("ou_user".to_string()),
        term_key: None,
        visibility: None,
        card_nonce: Some("nonce".to_string()),
        special_keys: None,
        selected_text: None,
        input_keys: None,
        input_text: None,
        option_type: None,
        selected_index: None,
        is_final: false,
        workflow_run_id: Some("run-1".to_string()),
        workflow_id: Some("flow-a".to_string()),
        workflow_revision_id: Some("rev-9".to_string()),
        workflow_node_id: Some("node-1".to_string()),
        workflow_activity_id: Some("act-1".to_string()),
        workflow_attempt_id: Some("att-1".to_string()),
        workflow_comment: None,
        raw_value: None,
        ask_id: None,
        ask_nonce: None,
        ask_question_index: None,
        ask_key: None,
        ask_submit: false,
        pending_id: None,
        working_dir: None,
        dir_search_keyword: None,
        cli_session_id: None,
    };
    assert_eq!(
        workflow_approval_target_message_id(&action).as_deref(),
        Some("om_clicked")
    );
}
