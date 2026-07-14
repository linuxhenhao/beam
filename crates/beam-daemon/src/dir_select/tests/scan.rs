use super::*;

#[test]
fn test_expand_tilde_home() {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_string());
    let result = expand_tilde("~/projects");
    assert_eq!(result, format!("{}/projects", home));
}

#[test]
fn test_expand_tilde_alone() {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_string());
    let result = expand_tilde("~");
    assert_eq!(result, home);
}

#[test]
fn test_expand_tilde_no_tilde() {
    let result = expand_tilde("/abs/path");
    assert_eq!(result, "/abs/path");
}

#[test]
fn test_tokenize_keywords() {
    let tokens = tokenize_keywords("beam daemon");
    assert_eq!(tokens, vec!["beam", "daemon"]);
}

#[test]
fn test_tokenize_keywords_extra_spaces() {
    let tokens = tokenize_keywords("  beam   daemon  ");
    assert_eq!(tokens, vec!["beam", "daemon"]);
}

#[test]
fn test_tokenize_keywords_empty() {
    let tokens = tokenize_keywords("");
    assert!(tokens.is_empty());
}

#[test]
fn test_match_dirs_and() {
    let dirs = vec![
        ".".to_string(),
        "beam-daemon".to_string(),
        "beam-core".to_string(),
        "beam-cli".to_string(),
        "docs/design/beam.md".to_string(),
        "README.md".to_string(),
    ];
    let matched = match_dirs(&dirs, &["beam", "daemon"]);
    assert_eq!(matched, vec!["beam-daemon".to_string()]);
}

#[test]
fn test_match_dirs_case_insensitive() {
    let dirs = vec!["MyProject".to_string(), "myproject".to_string()];
    let matched = match_dirs(&dirs, &["myproject"]);
    assert_eq!(matched.len(), 2);
}

#[test]
fn test_match_dirs_empty_keywords() {
    let dirs = vec!["a".to_string(), "b".to_string()];
    let matched = match_dirs(&dirs, &[]);
    assert_eq!(matched, dirs);
}

#[test]
fn test_filter_dirs() {
    let dirs = vec![
        ".".to_string(),
        "crates/beam-daemon".to_string(),
        "crates/beam-core".to_string(),
        "docs".to_string(),
    ];
    let result = filter_dirs(&dirs, "crates beam");
    assert_eq!(
        result,
        vec![
            "crates/beam-daemon".to_string(),
            "crates/beam-core".to_string(),
        ]
    );
}

#[test]
fn test_find_best_match_unique() {
    let dirs = vec![
        ".".to_string(),
        "projects/foo".to_string(),
        "projects/bar".to_string(),
        "projects".to_string(),
    ];
    let best = find_best_match(&dirs, "foo");
    assert_eq!(best, Some("projects/foo".to_string()));
}

#[test]
fn test_find_best_match_ambiguous() {
    let dirs = vec![
        ".".to_string(),
        "foo/bar".to_string(),
        "foo/baz".to_string(),
    ];
    let best = find_best_match(&dirs, "foo");
    assert_eq!(best, None);
}

#[test]
fn test_find_best_match_multiple_different_lengths_returns_none() {
    // Even though "foo" is shorter than "foo/bar/baz", there are 2 matches
    // and the new conservative logic requires exactly 1 match.
    let dirs = vec![
        ".".to_string(),
        "foo".to_string(),
        "foo/bar/baz".to_string(),
    ];
    let best = find_best_match(&dirs, "foo");
    assert_eq!(
        best, None,
        "2 matches (different lengths) should return None"
    );
}

#[test]
fn test_find_best_match_no_match() {
    let dirs = vec![".".to_string(), "a".to_string(), "b".to_string()];
    let best = find_best_match(&dirs, "xyz");
    assert_eq!(best, None);
}

#[test]
fn test_find_best_match_empty_search() {
    let dirs = vec![".".to_string(), "a".to_string()];
    let best = find_best_match(&dirs, "");
    assert_eq!(best, None);
}

#[test]
fn test_determine_root_working_dir() {
    let result = determine_root_working_dir(Some("/my/project"), &[]);
    assert_eq!(result, "/my/project");

    let result = determine_root_working_dir(None, &["/daemon/dir".to_string()]);
    assert_eq!(result, "/daemon/dir");

    let result = determine_root_working_dir(None, &[]);
    // Fallback is ".", expand_tilde(".") = "." (no tilde to expand)
    assert_eq!(result, ".");
}

#[test]
fn test_scan_candidate_dirs_includes_root() {
    let tmp = std::env::temp_dir().join("beam_dir_select_test");
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    std::fs::create_dir_all(tmp.join("sub_a")).unwrap();
    std::fs::create_dir_all(tmp.join("sub_b")).unwrap();
    std::fs::create_dir_all(tmp.join(".hidden_dir")).unwrap();
    std::fs::create_dir_all(tmp.join("__pycache__")).unwrap();
    std::fs::create_dir_all(tmp.join(".git")).unwrap();

    let dirs = scan_candidate_dirs(&tmp);
    assert!(dirs.contains(&".".to_string()));
    assert!(dirs.contains(&"sub_a".to_string()));
    assert!(dirs.contains(&"sub_b".to_string()));
    // Hidden and skipped dirs should not be included
    assert!(!dirs.iter().any(|d| d.contains(".hidden_dir")));
    assert!(!dirs.iter().any(|d| d.contains("__pycache__")));
    assert!(!dirs.iter().any(|d| d.contains(".git")));

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_resolve_dir() {
    assert_eq!(resolve_dir("/root", "."), "/root");
    assert_eq!(resolve_dir("/root", "sub"), "/root/sub");
    assert_eq!(resolve_dir("/root", "sub/deep"), "/root/sub/deep");
}

#[test]
#[cfg(unix)]
fn test_scan_candidate_dirs_skips_symlinks() {
    use std::os::unix::fs as unix_fs;

    let tmp = std::env::temp_dir().join("beam_dir_select_symlink_test");
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    // real subdirectory
    std::fs::create_dir_all(tmp.join("real_subdir")).unwrap();
    // symlink pointing outside root
    let external = std::env::temp_dir().join("beam_dir_select_external_target");
    std::fs::create_dir_all(&external).unwrap();
    unix_fs::symlink(&external, tmp.join("symlink_out")).unwrap();
    // symlink pointing inside root
    unix_fs::symlink(tmp.join("real_subdir"), tmp.join("symlink_in")).unwrap();

    let dirs = scan_candidate_dirs(&tmp);
    // Root "." must be present
    assert!(dirs.contains(&".".to_string()), "root must be included");
    // Real directory must be present
    assert!(
        dirs.contains(&"real_subdir".to_string()),
        "real_subdir must be included, got: {:?}",
        dirs
    );
    // Symlink to external must NOT be present
    assert!(
        !dirs
            .iter()
            .any(|d| d == "symlink_out" || d.contains("symlink_out")),
        "symlink to external must be excluded, got: {:?}",
        dirs
    );
    // Symlink to internal must NOT be present (all symlinks skipped)
    assert!(
        !dirs
            .iter()
            .any(|d| d == "symlink_in" || d.contains("symlink_in")),
        "symlink to internal must be excluded, got: {:?}",
        dirs
    );

    let _ = std::fs::remove_dir_all(&tmp);
    let _ = std::fs::remove_dir_all(&external);
}
