use super::*;

use crate::dir_select::{MAX_RECENT_DIRS, PendingCreateSession, RecentDirsStore};

#[test]
fn test_record_recent_dir() {
    let mut store = RecentDirsStore::default();
    let key = "app:chat:user";
    record_recent_dir(&mut store, key, "project-a");
    record_recent_dir(&mut store, key, "project-b");
    record_recent_dir(&mut store, key, "project-a"); // should move to front
    let entries = &store.entries[key];
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].dir, "project-a");
    assert_eq!(entries[1].dir, "project-b");
}

#[test]
fn test_record_recent_dir_trims() {
    let mut store = RecentDirsStore::default();
    let key = "app:chat:user";
    for i in 0..MAX_RECENT_DIRS + 5 {
        record_recent_dir(&mut store, key, &format!("dir-{}", i));
    }
    assert_eq!(store.entries[key].len(), MAX_RECENT_DIRS);
    // Most recent first
    assert_eq!(
        store.entries[key][0].dir,
        format!("dir-{}", MAX_RECENT_DIRS + 4)
    );
}

#[test]
fn test_build_recent_dir_key() {
    assert_eq!(
        build_recent_dir_key("app1", "chat1", Some("user1")),
        "app1:chat1:user1"
    );
    assert_eq!(build_recent_dir_key("app1", "chat1", None), "app1:chat1");
    assert_eq!(
        build_recent_dir_key("app1", "chat1", Some("")),
        "app1:chat1"
    );
}

#[test]
fn test_prune_expired_pending_creates_removes_expired() {
    use std::collections::HashMap;

    let mut map: HashMap<String, PendingCreateSession> = HashMap::new();

    // Helper to make a minimal pending with given age
    let make_pending = |id: &str, created_at_ms: i64| -> PendingCreateSession {
        PendingCreateSession {
            pending_id: id.to_string(),
            lark_app_id: "app".to_string(),
            chat_id: "chat".to_string(),
            chat_type: None,
            message_id: "msg".to_string(),
            anchor: "anchor".to_string(),
            scope: beam_core::SessionScope::Chat,
            thread_id: None,
            root_id: None,
            title: "t".to_string(),
            text: "".to_string(),
            sender_open_id: None,
            sender_type: None,
            locale: None,
            parent_id: None,
            mentions_json: "[]".to_string(),
            quota_key: None,
            created_at: created_at_ms,
            cli_id: "codex".to_string(),
            cli_bin: "codex".to_string(),
            cgroup_slice: None,
            cli_args: Vec::new(),
            root_working_dir: "/tmp".to_string(),
            candidate_dirs: vec![".".to_string()],
            card_message_id: None,
        }
    };

    let now: i64 = 1_700_000_000_000; // some fixed timestamp in ms

    // fresh: created 5 min ago
    map.insert(
        "fresh".to_string(),
        make_pending("fresh", now - 5 * 60 * 1000),
    );
    // borderline: created 29 min ago (within TTL)
    map.insert(
        "borderline".to_string(),
        make_pending("borderline", now - 29 * 60 * 1000),
    );
    // expired: created 31 min ago
    map.insert(
        "expired".to_string(),
        make_pending("expired", now - 31 * 60 * 1000),
    );
    // very old: created 2 hours ago
    map.insert(
        "old".to_string(),
        make_pending("old", now - 2 * 60 * 60 * 1000),
    );

    assert_eq!(map.len(), 4);

    let pruned = prune_expired_pending_creates(&mut map, now);
    assert_eq!(pruned, 2, "should prune 2 expired entries");
    assert_eq!(map.len(), 2);
    assert!(map.contains_key("fresh"));
    assert!(map.contains_key("borderline"));
    assert!(!map.contains_key("expired"));
    assert!(!map.contains_key("old"));
}

#[test]
fn test_pending_create_session_defaults_missing_cli_args() {
    let raw = r#"{
        "pending_id":"pid",
        "lark_app_id":"app",
        "chat_id":"chat",
        "message_id":"msg",
        "anchor":"anchor",
        "scope":"chat",
        "title":"title",
        "text":"",
        "created_at":123,
        "cli_id":"codex",
        "cli_bin":"codex",
        "root_working_dir":"/tmp",
        "candidate_dirs":["."]
    }"#;
    let pending: PendingCreateSession = serde_json::from_str(raw).expect("deserialize");
    assert!(pending.cli_args.is_empty());
}

#[test]
fn test_prune_expired_pending_creates_empty_map() {
    let mut map: std::collections::HashMap<String, PendingCreateSession> = Default::default();
    let pruned = prune_expired_pending_creates(&mut map, 1_700_000_000_000);
    assert_eq!(pruned, 0);
    assert!(map.is_empty());
}
