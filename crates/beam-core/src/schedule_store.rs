use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use chrono::SecondsFormat;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::BeamPaths;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ParsedScheduleKind {
    Once,
    Interval,
    Cron,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ParsedSchedule {
    pub kind: ParsedScheduleKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minutes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expr: Option<String>,
    pub display: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleRepeat {
    pub times: Option<u64>,
    #[serde(default)]
    pub completed: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ScheduleChatType {
    Group,
    P2p,
    TopicGroup,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ScheduleDeliver {
    Origin,
    Local,
}

impl Default for ScheduleDeliver {
    fn default() -> Self {
        Self::Origin
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScheduledTask {
    pub id: String,
    pub name: String,
    pub schedule: String,
    pub parsed: ParsedSchedule,
    pub prompt: String,
    pub working_dir: String,
    pub chat_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_type: Option<ScheduleChatType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lark_app_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator_chat_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator_root_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator_lark_app_id: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_run_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_delivery_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repeat: Option<ScheduleRepeat>,
    #[serde(default)]
    pub deliver: ScheduleDeliver,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreateTaskInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub name: String,
    pub schedule: String,
    pub parsed: ParsedSchedule,
    pub prompt: String,
    pub working_dir: String,
    pub chat_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_type: Option<ScheduleChatType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lark_app_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator_chat_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator_root_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator_lark_app_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_run_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repeat: Option<ScheduleRepeat>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deliver: Option<ScheduleDeliver>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleTaskUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_run_at: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_status: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_delivery_error: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repeat: Option<Option<ScheduleRepeat>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_message_id: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_type: Option<Option<ScheduleChatType>>,
}

#[derive(Debug, Error)]
pub enum ScheduleStoreError {
    #[error(
        "IdempotencyConflict: schedule task {task_id} exists with different canonical input (existing={existing_input_hash}…, incoming={incoming_input_hash}…)"
    )]
    IdempotencyConflict {
        task_id: String,
        existing_input_hash: String,
        incoming_input_hash: String,
    },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

pub fn create_task(
    paths: &BeamPaths,
    input: CreateTaskInput,
) -> Result<ScheduledTask, ScheduleStoreError> {
    let mut tasks = load_tasks(paths)?;
    if let Some(id) = &input.id
        && let Some(existing) = tasks.get(id)
    {
        let existing_hash = compute_input_hash(&canonical_schedule_input(existing))?;
        let incoming_hash = compute_input_hash(&canonical_schedule_input_input(&input))?;
        if existing_hash == incoming_hash {
            return Ok(existing.clone());
        }
        return Err(ScheduleStoreError::IdempotencyConflict {
            task_id: id.clone(),
            existing_input_hash: existing_hash,
            incoming_input_hash: incoming_hash,
        });
    }

    let id = input.id.unwrap_or_else(|| {
        Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(8)
            .collect()
    });
    let task = ScheduledTask {
        id: id.clone(),
        name: input.name,
        schedule: input.schedule,
        parsed: input.parsed,
        prompt: input.prompt,
        working_dir: input.working_dir,
        chat_id: input.chat_id,
        root_message_id: input.root_message_id,
        scope: input.scope,
        chat_type: input.chat_type,
        lark_app_id: input.lark_app_id,
        creator_chat_id: input.creator_chat_id,
        creator_root_message_id: input.creator_root_message_id,
        creator_lark_app_id: input.creator_lark_app_id,
        enabled: true,
        created_at: UtcNow::now(),
        last_run_at: None,
        next_run_at: input.next_run_at,
        last_status: None,
        last_error: None,
        last_delivery_error: None,
        repeat: input.repeat,
        deliver: input.deliver.unwrap_or_default(),
    };
    tasks.insert(id, task.clone());
    save_tasks(paths, &tasks)?;
    Ok(task)
}

pub fn get_task(
    paths: &BeamPaths,
    id: &str,
) -> Result<Option<ScheduledTask>, ScheduleStoreError> {
    let tasks = load_tasks(paths)?;
    Ok(tasks.get(id).cloned())
}

pub fn remove_task(paths: &BeamPaths, id: &str) -> Result<bool, ScheduleStoreError> {
    let mut tasks = load_tasks(paths)?;
    let existed = tasks.remove(id).is_some();
    if existed {
        save_tasks(paths, &tasks)?;
    }
    Ok(existed)
}

pub fn update_task(
    paths: &BeamPaths,
    id: &str,
    updates: ScheduleTaskUpdate,
) -> Result<(), ScheduleStoreError> {
    let mut tasks = load_tasks(paths)?;
    if let Some(task) = tasks.get_mut(id) {
        if let Some(enabled) = updates.enabled {
            task.enabled = enabled;
        }
        if let Some(last_run_at) = updates.last_run_at {
            task.last_run_at = Some(last_run_at);
        }
        if let Some(next_run_at) = updates.next_run_at {
            task.next_run_at = next_run_at;
        }
        if let Some(last_status) = updates.last_status {
            task.last_status = last_status;
        }
        if let Some(last_error) = updates.last_error {
            task.last_error = last_error;
        }
        if let Some(last_delivery_error) = updates.last_delivery_error {
            task.last_delivery_error = last_delivery_error;
        }
        if let Some(repeat) = updates.repeat {
            task.repeat = repeat;
        }
        if let Some(root_message_id) = updates.root_message_id {
            task.root_message_id = root_message_id;
        }
        if let Some(chat_type) = updates.chat_type {
            task.chat_type = chat_type;
        }
        save_tasks(paths, &tasks)?;
    }
    Ok(())
}

pub fn mark_run(
    paths: &BeamPaths,
    id: &str,
    success: bool,
    error: Option<&str>,
    delivery_error: Option<&str>,
) -> Result<(), ScheduleStoreError> {
    let mut tasks = load_tasks(paths)?;
    let Some(task) = tasks.get_mut(id) else {
        return Ok(());
    };

    task.last_run_at = Some(UtcNow::now());
    task.last_status = Some(if success {
        "ok".to_string()
    } else {
        "error".to_string()
    });
    task.last_error = if success {
        None
    } else {
        error.map(|s| s.to_string())
    };
    task.last_delivery_error = delivery_error.map(|s| s.to_string());

    if let Some(repeat) = task.repeat.as_mut() {
        repeat.completed = repeat.completed.saturating_add(1);
        if matches!(repeat.times, Some(times) if times > 0 && repeat.completed >= times) {
            tasks.remove(id);
            save_tasks(paths, &tasks)?;
            return Ok(());
        }
    }

    if matches!(task.parsed.kind, ParsedScheduleKind::Once) {
        task.enabled = false;
        task.next_run_at = None;
    }

    save_tasks(paths, &tasks)?;
    Ok(())
}

pub fn list_tasks(paths: &BeamPaths) -> Result<Vec<ScheduledTask>, ScheduleStoreError> {
    let tasks = load_tasks(paths)?;
    Ok(tasks.values().cloned().collect())
}

pub fn append_output_log(
    paths: &BeamPaths,
    task_id: &str,
    content: &str,
) -> Result<PathBuf, ScheduleStoreError> {
    let dir = task_output_dir(paths, task_id);
    fs::create_dir_all(&dir)?;
    let fname = format!("{}.md", UtcNow::now().replace(':', "-").replace('.', "-"));
    let path = dir.join(fname);
    fs::write(&path, content)?;
    Ok(path)
}

fn load_tasks(
    paths: &BeamPaths,
) -> Result<BTreeMap<String, ScheduledTask>, ScheduleStoreError> {
    let path = paths.schedules_json();
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let raw = fs::read_to_string(&path)?;
    if raw.trim().is_empty() {
        return Ok(BTreeMap::new());
    }
    let tasks = serde_json::from_str(&raw)?;
    Ok(tasks)
}

fn save_tasks(
    paths: &BeamPaths,
    tasks: &BTreeMap<String, ScheduledTask>,
) -> Result<(), ScheduleStoreError> {
    let path = paths.schedules_json();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(tasks)?)?;
    fs::rename(&tmp, &path)?;
    Ok(())
}

fn canonical_schedule_input(task: &ScheduledTask) -> serde_json::Value {
    serde_json::json!({
        "name": task.name,
        "schedule": task.schedule,
        "parsed": {
            "kind": task.parsed.kind,
            "runAt": task.parsed.run_at,
            "minutes": task.parsed.minutes,
            "expr": task.parsed.expr,
        },
        "prompt": task.prompt,
        "workingDir": task.working_dir,
        "chatId": task.chat_id,
        "rootMessageId": task.root_message_id,
        "scope": task.scope,
        "larkAppId": task.lark_app_id,
        "repeat": task.repeat.as_ref().map(|repeat| serde_json::json!({ "times": repeat.times })),
        "deliver": match task.deliver {
            ScheduleDeliver::Origin => "origin",
            ScheduleDeliver::Local => "local",
        }
    })
}

fn canonical_schedule_input_input(input: &CreateTaskInput) -> serde_json::Value {
    serde_json::json!({
        "name": input.name,
        "schedule": input.schedule,
        "parsed": {
            "kind": input.parsed.kind,
            "runAt": input.parsed.run_at,
            "minutes": input.parsed.minutes,
            "expr": input.parsed.expr,
        },
        "prompt": input.prompt,
        "workingDir": input.working_dir,
        "chatId": input.chat_id,
        "rootMessageId": input.root_message_id,
        "scope": input.scope,
        "larkAppId": input.lark_app_id,
        "repeat": input.repeat.as_ref().map(|repeat| serde_json::json!({ "times": repeat.times })),
        "deliver": match input.deliver.clone().unwrap_or_default() {
            ScheduleDeliver::Origin => "origin",
            ScheduleDeliver::Local => "local",
        }
    })
}

fn compute_input_hash(value: &serde_json::Value) -> Result<String, ScheduleStoreError> {
    let canonical = canonical_json(value);
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn canonical_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(v) => if *v { "true" } else { "false" }.to_string(),
        serde_json::Value::Number(v) => v.to_string(),
        serde_json::Value::String(v) => serde_json::to_string(v).expect("string serializable"),
        serde_json::Value::Array(items) => {
            let mut out = String::from("[");
            let mut first = true;
            for item in items {
                if !first {
                    out.push(',');
                }
                first = false;
                out.push_str(&canonical_json(item));
            }
            out.push(']');
            out
        }
        serde_json::Value::Object(map) => {
            let mut keys: Vec<_> = map.keys().collect();
            keys.sort();
            let mut out = String::from("{");
            let mut first = true;
            for key in keys {
                if !first {
                    out.push(',');
                }
                first = false;
                out.push_str(&serde_json::to_string(key).expect("key serializable"));
                out.push(':');
                out.push_str(&canonical_json(&map[key]));
            }
            out.push('}');
            out
        }
    }
}

fn task_output_dir(paths: &BeamPaths, task_id: &str) -> PathBuf {
    paths.schedules_output_dir().join(task_id)
}

struct UtcNow;

impl UtcNow {
    fn now() -> String {
        chrono::Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
    }
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_paths(label: &str) -> BeamPaths {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        BeamPaths::from_root(std::env::temp_dir().join(format!(
            "beam-schedule-store-{label}-{nanos}-{}",
            std::process::id()
        )))
    }

    fn parsed_cron() -> ParsedSchedule {
        ParsedSchedule {
            kind: ParsedScheduleKind::Cron,
            run_at: None,
            minutes: None,
            expr: Some("0 9 * * *".to_string()),
            display: "0 9 * * *".to_string(),
        }
    }

    #[test]
    fn create_task_returns_existing_when_canonical_input_matches() {
        let paths = temp_paths("identical");
        let input = CreateTaskInput {
            id: Some("wf_task".to_string()),
            name: "schedule-demo daily 9am".to_string(),
            schedule: "0 9 * * *".to_string(),
            parsed: parsed_cron(),
            prompt: "Schedule demo: run workflow self-check.".to_string(),
            working_dir: "/tmp/beam-schedule-demo".to_string(),
            chat_id: "oc_workflow_demo".to_string(),
            root_message_id: None,
            scope: Some("thread".to_string()),
            chat_type: None,
            lark_app_id: None,
            creator_chat_id: None,
            creator_root_message_id: None,
            creator_lark_app_id: None,
            next_run_at: None,
            repeat: None,
            deliver: None,
        };
        let created = create_task(&paths, input.clone()).expect("create");
        let returned = create_task(&paths, input).expect("create existing");
        assert_eq!(created.id, "wf_task");
        assert_eq!(returned.id, "wf_task");
        assert_eq!(list_tasks(&paths).expect("list").len(), 1);
        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[test]
    fn create_task_conflicts_when_canonical_input_differs() {
        let paths = temp_paths("conflict");
        let input = CreateTaskInput {
            id: Some("wf_task".to_string()),
            name: "schedule-demo daily 9am".to_string(),
            schedule: "0 9 * * *".to_string(),
            parsed: parsed_cron(),
            prompt: "Schedule demo: run workflow self-check.".to_string(),
            working_dir: "/tmp/beam-schedule-demo".to_string(),
            chat_id: "oc_workflow_demo".to_string(),
            root_message_id: None,
            scope: Some("thread".to_string()),
            chat_type: None,
            lark_app_id: None,
            creator_chat_id: None,
            creator_root_message_id: None,
            creator_lark_app_id: None,
            next_run_at: None,
            repeat: None,
            deliver: None,
        };
        let _ = create_task(&paths, input).expect("create");
        let changed = CreateTaskInput {
            id: Some("wf_task".to_string()),
            name: "schedule-demo daily 9am".to_string(),
            schedule: "0 9 * * *".to_string(),
            parsed: parsed_cron(),
            prompt: "changed prompt".to_string(),
            working_dir: "/tmp/beam-schedule-demo".to_string(),
            chat_id: "oc_workflow_demo".to_string(),
            root_message_id: None,
            scope: Some("thread".to_string()),
            chat_type: None,
            lark_app_id: None,
            creator_chat_id: None,
            creator_root_message_id: None,
            creator_lark_app_id: None,
            next_run_at: None,
            repeat: None,
            deliver: None,
        };
        let err = create_task(&paths, changed).expect_err("conflict");
        assert!(matches!(
            err,
            ScheduleStoreError::IdempotencyConflict { .. }
        ));
        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[test]
    fn mark_run_removes_finite_repeat_after_completion() {
        let paths = temp_paths("mark");
        let input = CreateTaskInput {
            id: Some("wf_task".to_string()),
            name: "schedule-demo daily 9am".to_string(),
            schedule: "0 9 * * *".to_string(),
            parsed: parsed_cron(),
            prompt: "Schedule demo: run workflow self-check.".to_string(),
            working_dir: "/tmp/beam-schedule-demo".to_string(),
            chat_id: "oc_workflow_demo".to_string(),
            root_message_id: None,
            scope: Some("thread".to_string()),
            chat_type: None,
            lark_app_id: None,
            creator_chat_id: None,
            creator_root_message_id: None,
            creator_lark_app_id: None,
            next_run_at: None,
            repeat: Some(ScheduleRepeat {
                times: Some(1),
                completed: 0,
            }),
            deliver: None,
        };
        let _ = create_task(&paths, input).expect("create");
        mark_run(&paths, "wf_task", true, None, None).expect("mark run");
        assert!(get_task(&paths, "wf_task").expect("get").is_none());
        let _ = std::fs::remove_dir_all(paths.root());
    }
}
