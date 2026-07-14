//! Security validation for directory paths.
//!
//! Ensures that user-selected directories are within the allowed root
//! by using path normalization and boundary checks. The canonical/normalize
//! semantics here must be preserved exactly — directory traversal and
//! symlink-escaping are the primary threat model.

use std::path::{Path, PathBuf};

/// Check if a directory path (absolute or relative) is under the given root.
/// Uses pure path manipulation; does NOT require the paths to exist on disk.
/// Handles boundary cases like `/tmp/rootX` NOT being under `/tmp/root`,
/// and relative roots like `"."`.
pub fn is_dir_under_root(dir: &str, root: &str) -> bool {
    let root_path = Path::new(root);
    let dir_path = Path::new(dir);

    // Reject paths that attempt to escape via ".."
    if dir_path
        .components()
        .any(|c| c == std::path::Component::ParentDir)
    {
        return false;
    }

    // Reject absolute dir when root is relative (can't be under it)
    if dir_path.is_absolute() && !root_path.is_absolute() {
        return false;
    }

    // Normalize both paths (resolve ".", "..", etc.)
    let normalized_root = normalize_path(root_path);
    let normalized_dir = if dir_path.is_absolute() {
        normalize_path(dir_path)
    } else {
        normalize_path(&root_path.join(dir_path))
    };

    if normalized_dir == normalized_root {
        return true;
    }

    // If root is empty/current-dir (e.g., "."), accept any non-absolute,
    // non-escape relative path (already verified above).
    let root_str = normalized_root.to_string_lossy();
    if root_str.is_empty() {
        return true;
    }

    // Check that dir starts with root + separator
    let dir_str = normalized_dir.to_string_lossy();
    if dir_str.len() > root_str.len() {
        let remainder = &dir_str[root_str.len()..];
        remainder.starts_with(std::path::MAIN_SEPARATOR)
    } else {
        false
    }
}

/// Normalize a path by resolving components where possible.
/// For non-existing paths, this does a best-effort normalization
/// by collapsing ".." and "." components.
fn normalize_path(path: &Path) -> PathBuf {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                result.pop();
            }
            std::path::Component::CurDir => {}
            c => {
                result.push(c.as_os_str());
            }
        }
    }
    result
}

/// Check if a directory is a valid candidate (exists in candidate list and under root).
pub fn is_valid_candidate(dir: &str, root: &str, candidates: &[String]) -> bool {
    if !is_dir_under_root(dir, root) {
        return false;
    }
    // dir should be in the candidate list (or be root itself)
    candidates.contains(&dir.to_string()) || dir == "."
}

#[cfg(test)]
#[path = "tests/validation.rs"]
mod tests;
