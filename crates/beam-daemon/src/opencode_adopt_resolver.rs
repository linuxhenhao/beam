use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::Deserialize;
use serde::de::DeserializeOwned;

const OPENCODE_DIRECTORY_FALLBACK_LIMIT: usize = 10;

#[derive(Debug, Clone, Deserialize)]
struct OpenCodeSessionRow {
    id: String,
    directory: String,
    time_archived: Option<u64>,
    parent_id: Option<String>,
}

pub(crate) fn resolve_opencode_adopt_session(cli_id: &str, working_dir: &str) -> Option<String> {
    if cli_id != "opencode" {
        return None;
    }
    let home = std::env::var("HOME").ok()?;
    let data_dir = PathBuf::from(home).join(".local/share/opencode");
    resolve_opencode_adopt_session_in_data_dir(&data_dir, working_dir)
}

fn resolve_opencode_adopt_session_in_data_dir(
    data_dir: &Path,
    working_dir: &str,
) -> Option<String> {
    let db_paths = opencode_db_candidates(data_dir);
    if db_paths.is_empty() {
        return None;
    }
    if let Some(session_id) = resolve_opencode_session_via_logs(data_dir, working_dir, &db_paths) {
        return Some(session_id);
    }
    let candidates = find_all_opencode_sessions_by_directory(Some(working_dir), &db_paths);
    if candidates.len() == 1 {
        return candidates.into_iter().next().map(|row| row.id);
    }
    None
}

fn opencode_db_candidates(data_dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(data_dir) else {
        return Vec::new();
    };
    entries
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?;
            if !name.starts_with("opencode") || !name.ends_with(".db") {
                return None;
            }
            match entry.file_type() {
                Ok(ft) if ft.is_file() => Some(path),
                _ => None,
            }
        })
        .collect()
}

fn find_opencode_session_row_by_id(
    session_id: Option<&str>,
    db_paths: &[PathBuf],
) -> Option<(PathBuf, OpenCodeSessionRow)> {
    let session_id = session_id?;
    for db_path in db_paths {
        if !db_path.exists() {
            continue;
        }
        if let Ok(Some(row)) = query_session_by_id(db_path, session_id) {
            return Some((db_path.clone(), row));
        }
    }
    None
}

fn find_all_opencode_sessions_by_directory(
    directory: Option<&str>,
    db_paths: &[PathBuf],
) -> Vec<OpenCodeSessionRow> {
    let Some(directory) = directory else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for db_path in db_paths {
        if !db_path.exists() {
            continue;
        }
        if let Ok(mut found) = query_all_sessions_by_directory(db_path, directory) {
            rows.append(&mut found);
        }
    }
    rows
}

fn query_session_by_id(
    db_path: &Path,
    session_id: &str,
) -> anyhow::Result<Option<OpenCodeSessionRow>> {
    let mut script = String::from(
        r#"
import json
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.row_factory = sqlite3.Row
row = conn.execute(
    "SELECT id, directory, time_updated, time_archived, parent_id FROM session WHERE id = ? LIMIT 1",
    (__SESSION_ID__,),
).fetchone()
print(json.dumps(dict(row), ensure_ascii=False) if row else "null")
"#,
    );
    script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
    script = script.replace("__SESSION_ID__", &json_string(session_id));
    run_python_json(&script)
}

fn query_all_sessions_by_directory(
    db_path: &Path,
    directory: &str,
) -> anyhow::Result<Vec<OpenCodeSessionRow>> {
    let mut script = format!(
        r#"
import json
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.row_factory = sqlite3.Row
rows = conn.execute(
    """
    SELECT id, directory, time_updated, time_archived, parent_id
    FROM session
    WHERE directory = ?
      AND time_archived IS NULL
      AND parent_id IS NULL
    ORDER BY time_updated DESC
    LIMIT {}
    """,
    (__DIRECTORY__,),
).fetchall()
print(json.dumps([dict(r) for r in rows], ensure_ascii=False))
"#,
        OPENCODE_DIRECTORY_FALLBACK_LIMIT
    );
    script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
    script = script.replace("__DIRECTORY__", &json_string(directory));
    run_python_json(&script)
}

fn recent_opencode_session_ids(log_dir: &Path) -> Vec<String> {
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
    files.sort_by_key(|(modified, _)| std::cmp::Reverse(*modified));

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

fn session_matches_working_dir_and_is_root(row: &OpenCodeSessionRow, working_dir: &str) -> bool {
    row.directory == working_dir && row.time_archived.is_none() && row.parent_id.is_none()
}

fn resolve_opencode_session_via_logs(
    data_dir: &Path,
    working_dir: &str,
    db_paths: &[PathBuf],
) -> Option<String> {
    let log_dir = data_dir.join("log");
    for session_id in recent_opencode_session_ids(&log_dir) {
        if let Some((_db_path, row)) = find_opencode_session_row_by_id(Some(&session_id), db_paths)
            && session_matches_working_dir_and_is_root(&row, working_dir)
        {
            return Some(row.id);
        }
    }
    None
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).expect("json string")
}

fn run_python_json<T: DeserializeOwned>(script: &str) -> anyhow::Result<T> {
    let proc = Command::new("python3").args(["-c", script]).output()?;
    if !proc.status.success() {
        anyhow::bail!("{}", String::from_utf8_lossy(&proc.stderr).trim());
    }
    let stdout = String::from_utf8_lossy(&proc.stdout).trim().to_string();
    if stdout.is_empty() {
        anyhow::bail!("python3 returned empty output");
    }
    Ok(serde_json::from_str(&stdout)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("beam-opencode-adopt-{name}-{nonce}"))
    }

    #[allow(clippy::type_complexity)]
    fn create_db_with_session_rows(
        db_path: &Path,
        sessions: &[(&str, &str, u64, Option<u64>, Option<&str>)],
    ) {
        let mut script = String::from(
            r#"
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.executescript("""
CREATE TABLE session (
  id TEXT PRIMARY KEY,
  directory TEXT,
  time_updated INTEGER,
  time_archived INTEGER,
  parent_id TEXT
);
""")
"#,
        );
        script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
        for &(id, directory, time_updated, time_archived, parent_id) in sessions {
            script.push_str(&format!(
                "conn.execute(\"INSERT INTO session (id, directory, time_updated, time_archived, parent_id) VALUES (?, ?, ?, ?, ?)\", ({}, {}, {}, {}, {}))\n",
                json_string(id),
                json_string(directory),
                time_updated,
                time_archived.map(|value| value.to_string()).unwrap_or_else(|| "None".to_string()),
                parent_id.map(json_string).unwrap_or_else(|| "None".to_string()),
            ));
        }
        script.push_str("conn.commit()\n");
        let status = Command::new("python3")
            .args(["-c", &script])
            .status()
            .expect("python3 available");
        assert!(status.success(), "failed to create test sqlite db");
    }

    #[test]
    fn resolver_prefers_recent_matching_log_session() {
        let root = temp_dir("logs");
        let data_dir = root.join("opencode");
        let log_dir = data_dir.join("log");
        fs::create_dir_all(&log_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        create_db_with_session_rows(
            &db_path,
            &[
                ("sess-old", "/repo/logged", 1000, None, None),
                ("sess-new", "/repo/logged", 2000, None, None),
                ("sess-other", "/repo/other", 3000, None, None),
            ],
        );
        fs::write(
            log_dir.join("opencode.log"),
            "session.id=sess-other\nsession.id=sess-old\nsession.id=sess-new\n",
        )
        .unwrap();

        assert_eq!(
            resolve_opencode_adopt_session_in_data_dir(&data_dir, "/repo/logged").as_deref(),
            Some("sess-new")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolver_only_uses_directory_fallback_when_unique() {
        let root = temp_dir("fallback");
        let data_dir = root.join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        create_db_with_session_rows(
            &db_path,
            &[
                ("sess-root", "/repo/one", 1000, None, None),
                ("sess-child", "/repo/one", 2000, None, Some("sess-root")),
                ("sess-archived", "/repo/one", 3000, Some(3001), None),
                ("sess-a", "/repo/many", 1000, None, None),
                ("sess-b", "/repo/many", 2000, None, None),
            ],
        );

        assert_eq!(
            resolve_opencode_adopt_session_in_data_dir(&data_dir, "/repo/one").as_deref(),
            Some("sess-root")
        );
        assert_eq!(
            resolve_opencode_adopt_session_in_data_dir(&data_dir, "/repo/many"),
            None
        );
        let _ = fs::remove_dir_all(root);
    }
}
