//! Tests for [`crate::prepare_retry_last_task`] — clears usage limit
//! and marks session as Working for retry.

use super::*;
use crate::prepare_retry_last_task;
use beam_core::{CliUsageLimitKind, CliUsageLimitState, ScreenStatus};

#[test]
fn prepare_retry_last_task_clears_limit_and_marks_working() {
    let mut session = make_session("sess-retry");
    session.last_cli_input = Some("continue".to_string());
    session.last_screen_status = Some(ScreenStatus::Limited);
    session.usage_limit = Some(CliUsageLimitState {
        limited: true,
        kind: CliUsageLimitKind::Usage,
        retry_at_ms: 10,
        retry_label: "3:15 PM".to_string(),
        retry_ready: true,
    });

    let (updated, cli_input) = prepare_retry_last_task(&session, 10).expect("retry prepared");
    assert_eq!(cli_input, "continue");
    assert_eq!(updated.usage_limit, None);
    assert_eq!(updated.last_screen_status, Some(ScreenStatus::Working));
}
