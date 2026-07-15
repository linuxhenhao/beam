use std::{path::PathBuf, process::Command};

#[test]
fn rust_source_files_stay_within_the_line_limit() {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("beam-core should be in the workspace crates directory")
        .to_path_buf();
    let check = workspace_root.join("scripts/check-rust-line-count.sh");

    let status = Command::new("bash")
        .arg(&check)
        .status()
        .unwrap_or_else(|error| panic!("failed to run {}: {error}", check.display()));

    assert!(status.success(), "Rust source file line-count check failed");
}
