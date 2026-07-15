use super::*;

#[test]
fn test_is_dir_under_root_absolute() {
    let result = is_dir_under_root("/tmp/root/sub", "/tmp/root");
    assert!(result);
}

#[test]
fn test_is_dir_under_root_not_under() {
    let result = is_dir_under_root("/etc/passwd", "/tmp/root");
    assert!(!result);
}

#[test]
fn test_is_dir_under_root_equal() {
    let result = is_dir_under_root("/tmp/root", "/tmp/root");
    assert!(result);
}

#[test]
fn test_is_dir_under_root_relative() {
    let result = is_dir_under_root("sub/dir", "/tmp/root");
    assert!(result);
}

#[test]
fn test_is_dir_under_root_dot_root_accepts_relative() {
    // root="." should accept relative paths like "crates"
    assert!(is_dir_under_root("crates", "."));
    assert!(is_dir_under_root("crates/beam-daemon", "."));
}

#[test]
fn test_is_dir_under_root_dot_root_rejects_escape() {
    assert!(!is_dir_under_root("../x", "."), ".. should be rejected");
    assert!(
        !is_dir_under_root("crates/../../etc", "."),
        ".. should be rejected"
    );
}

#[test]
fn test_is_dir_under_root_dot_root_rejects_absolute() {
    assert!(
        !is_dir_under_root("/tmp/x", "."),
        "absolute path should be rejected when root is '.'"
    );
}

#[test]
fn test_is_valid_candidate_dot_root() {
    let candidates = vec![
        "crates".to_string(),
        "crates/beam-daemon".to_string(),
        "src".to_string(),
    ];
    assert!(is_valid_candidate("crates", ".", &candidates));
    assert!(is_valid_candidate("src", ".", &candidates));
    assert!(!is_valid_candidate("nonexistent", ".", &candidates));
    assert!(!is_valid_candidate("../x", ".", &candidates));
    assert!(!is_valid_candidate("/tmp/x", ".", &candidates));
}
