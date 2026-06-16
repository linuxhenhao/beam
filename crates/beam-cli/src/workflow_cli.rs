use std::collections::{BTreeMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use beam_core::{
    BootstrapWorkflowRunInput, BeamPaths, EventDraft, EventLog, WorkflowActor,
    WorkflowEventEnvelope, bootstrap_workflow_run, infer_run_status, mint_workflow_run_id,
    read_run_events_pure, resume_schedule_dangling_effects,
};
use clap::{Args, Subcommand};
use serde::Serialize;
use serde_json::Value;
use tokio::fs;

#[derive(Debug, Subcommand)]
pub enum WorkflowCommand {
    Run(WorkflowRunArgs),
    Resume(WorkflowResumeArgs),
    Cancel(WorkflowCancelArgs),
    Validate(WorkflowValidateArgs),
    Ls(WorkflowLsArgs),
    Tail(WorkflowTailArgs),
    Show(WorkflowShowArgs),
}

#[derive(Debug, Args)]
pub struct WorkflowRunArgs {
    pub workflow_id: String,
    #[arg(trailing_var_arg = true)]
    pub rest: Vec<String>,
}

#[derive(Debug, Args)]
pub struct WorkflowResumeArgs {
    pub run_id: String,
    #[arg(long, default_value = "false")]
    pub wait: bool,
    #[arg(long, short = 'v', default_value = "false")]
    pub verbose: bool,
}

#[derive(Debug, Args)]
pub struct WorkflowCancelArgs {
    pub run_id: String,
    #[arg(long, default_value = "cancelled via beam workflow cancel")]
    pub reason: String,
}

#[derive(Debug, Args)]
pub struct WorkflowValidateArgs {
    pub path: PathBuf,
}

#[derive(Debug, Args)]
pub struct WorkflowLsArgs {
    #[arg(long)]
    pub all: bool,
    #[arg(long)]
    pub wide: bool,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub status: Option<String>,
}

#[derive(Debug, Args)]
pub struct WorkflowTailArgs {
    pub run_id: String,
    #[arg(long, default_value_t = 1)]
    pub from: u64,
    #[arg(short, long)]
    pub follow: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct WorkflowShowArgs {
    pub run_id: String,
}

pub async fn handle(command: WorkflowCommand, paths: &BeamPaths) -> Result<()> {
    match command {
        WorkflowCommand::Validate(args) => cmd_validate(&args.path).await,
        WorkflowCommand::Ls(args) => cmd_ls(paths, &args).await,
        WorkflowCommand::Tail(args) => cmd_tail(paths, &args).await,
        WorkflowCommand::Show(args) => cmd_show(paths, &args).await,
        WorkflowCommand::Run(args) => cmd_run(paths, &args).await,
        WorkflowCommand::Resume(args) => cmd_resume(paths, &args).await,
        WorkflowCommand::Cancel(args) => cmd_cancel(paths, &args).await,
    }
}

async fn cmd_run(paths: &BeamPaths, args: &WorkflowRunArgs) -> Result<()> {
    let def_path = load_workflow_definition_path(&args.workflow_id).await?;
    let raw_def = fs::read_to_string(&def_path)
        .await
        .with_context(|| format!("读取 {} 失败", def_path.display()))?;
    let def_json: Value = serde_json::from_str(&raw_def)
        .with_context(|| format!("解析 {} 失败", def_path.display()))?;
    let workflow_id = def_json
        .get("workflowId")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(&args.workflow_id)
        .to_string();
    if workflow_id != args.workflow_id {
        bail!(
            "workflowId mismatch: requested={} file={}",
            args.workflow_id,
            workflow_id
        );
    }

    let params = parse_raw_params(&args.rest)?;
    let run_id = create_workflow_run_id(&workflow_id);
    let bootstrap = bootstrap_workflow_run(
        paths,
        BootstrapWorkflowRunInput {
            run_id: &run_id,
            workflow_json: &raw_def,
            expected_workflow_id: Some(&workflow_id),
            params: &params,
            initiator: "cli",
            chat_binding: None,
        },
    )?;

    println!(
        "workflow run created: {} (workflow={}, status=running)",
        bootstrap.run_id, bootstrap.workflow_id
    );
    if !args.rest.is_empty() {
        println!("args: {}", args.rest.join(" "));
    }
    Ok(())
}

async fn cmd_resume(paths: &BeamPaths, args: &WorkflowResumeArgs) -> Result<()> {
    let mut log = EventLog::new(args.run_id.clone(), paths.workflow_runs_dir())?;
    let events = log.read_all()?;
    if events.is_empty() {
        bail!(
            "runId={} 没找到任何事件 (runsDir={})",
            args.run_id,
            paths.workflow_runs_dir().display()
        );
    }
    if events.first().map(|ev| ev.event_type.as_str()) != Some("runCreated") {
        bail!("runId={} 的第一个事件不是 runCreated", args.run_id);
    }
    let status = infer_run_status(&events);
    if is_terminal_status(&status) {
        bail!("runId={} 已经是终态 ({}), 无需 resume", args.run_id, status);
    }
    let result = resume_schedule_dangling_effects(&mut log, paths, "beam-cli", None).await?;
    if args.verbose {
        if !result.reconciled.is_empty() {
            println!("已恢复 {} 个 effect:", result.reconciled.len());
            for r in &result.reconciled {
                println!("  - {}/{} reconciled", r.activity_id, r.attempt_id);
            }
        }
        if !result.fresh_retry.is_empty() {
            println!("重新发起 {} 个 effect:", result.fresh_retry.len());
            for r in &result.fresh_retry {
                println!("  - {}/{} retry", r.activity_id, r.attempt_id);
            }
        }
        if !result.skipped.is_empty() {
            println!("跳过 {} 个 effect:", result.skipped.len());
            for r in &result.skipped {
                println!("  - {}", r);
            }
        }
    }
    println!(
        "workflow resume: {} (reconciled={}, freshRetry={}, skipped={})",
        args.run_id,
        result.reconciled.len(),
        result.fresh_retry.len(),
        result.skipped.len()
    );

    if args.wait {
        println!("等待 run 完成...");
        let events_path = paths.workflow_run_dir(&args.run_id).join("events.ndjson");
        let mut offset = tokio::fs::metadata(&events_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        let mut buffer = String::new();
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let meta = match tokio::fs::metadata(&events_path).await {
                Ok(meta) => meta,
                Err(_) => {
                    println!("(events.ndjson 不可读，停止等待)");
                    break;
                }
            };
            if meta.len() < offset {
                println!("(events.ndjson 大小回退，停止等待)");
                break;
            }
            if meta.len() == offset {
                continue;
            }
            let mut file = tokio::fs::File::open(&events_path).await?;
            use tokio::io::{AsyncReadExt, AsyncSeekExt};
            file.seek(std::io::SeekFrom::Start(offset)).await?;
            let mut chunk = vec![0u8; (meta.len() - offset) as usize];
            file.read_exact(&mut chunk).await?;
            offset = meta.len();
            buffer.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(pos) = buffer.find('\n') {
                let line = buffer[..pos].trim().to_string();
                buffer = buffer[pos + 1..].to_string();
                if line.is_empty() {
                    continue;
                }
                let ev: WorkflowEventEnvelope = match serde_json::from_str(&line) {
                    Ok(ev) => ev,
                    Err(_) => continue,
                };
                if args.verbose {
                    println!(
                        "  {} {} {}",
                        &ev.event_id[..8.min(ev.event_id.len())],
                        ev.event_type,
                        ev.timestamp
                    );
                }
                if ev.event_type == "runSucceeded"
                    || ev.event_type == "runFailed"
                    || ev.event_type == "runCanceled"
                {
                    println!("workflow {} {}", args.run_id, ev.event_type);
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}

async fn cmd_cancel(paths: &BeamPaths, args: &WorkflowCancelArgs) -> Result<()> {
    let mut log = EventLog::new(args.run_id.clone(), paths.workflow_runs_dir())?;
    let events = log.read_all()?;
    if events.is_empty() {
        bail!(
            "runId={} 没找到任何事件 (runsDir={})",
            args.run_id,
            paths.workflow_runs_dir().display()
        );
    }
    let status = infer_run_status(&events);
    if is_terminal_status(&status) {
        bail!("runId={} 已经是终态 ({})", args.run_id, status);
    }
    let cancel_requested = log.append(EventDraft {
        event_type: "cancelRequested".to_string(),
        actor: WorkflowActor::Human,
        payload: serde_json::json!({
            "target": { "kind": "run", "runId": args.run_id },
            "reason": args.reason,
            "by": "beam-cli",
        }),
        timestamp: None,
        payload_hash: None,
    })?;
    let _run_canceled = log.append(EventDraft {
        event_type: "runCanceled".to_string(),
        actor: WorkflowActor::Scheduler,
        payload: serde_json::json!({
            "cancelOriginEventId": cancel_requested.event_id,
        }),
        timestamp: None,
        payload_hash: None,
    })?;
    println!(
        "workflow cancel recorded: {} reason={}",
        args.run_id, args.reason
    );
    Ok(())
}

async fn load_workflow_definition_path(workflow_id: &str) -> Result<PathBuf> {
    let mut candidates = vec![
        env::current_dir()?
            .join("workflows")
            .join(format!("{workflow_id}.workflow.json")),
    ];
    if let Some(home) = env::var_os("HOME") {
        candidates.push(
            PathBuf::from(home)
                .join(".beam/workflows")
                .join(format!("{workflow_id}.workflow.json")),
        );
    }
    for candidate in &candidates {
        if fs::metadata(candidate).await.is_ok() {
            return Ok(candidate.clone());
        }
    }
    bail!(
        "Workflow '{}' not found. Looked in:\n{}",
        workflow_id,
        candidates
            .iter()
            .map(|p| format!("- {}", p.display()))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

fn parse_raw_params(rest: &[String]) -> Result<BTreeMap<String, Value>> {
    let mut out = BTreeMap::new();
    let mut seen = std::collections::HashSet::new();
    for part in rest {
        let Some((key, value)) = part.split_once('=') else {
            bail!("参数必须是 key=value，收到: {}", part);
        };
        let key = key.trim();
        if key.is_empty() {
            bail!("参数 key 不能为空: {}", part);
        }
        if !seen.insert(key.to_string()) {
            bail!("重复参数: {}", key);
        }
        out.insert(key.to_string(), Value::String(value.to_string()));
    }
    Ok(out)
}

fn create_workflow_run_id(workflow_id: &str) -> String {
    mint_workflow_run_id(
        workflow_id,
        chrono::Utc::now().timestamp_millis().max(0) as u64,
    )
}

async fn cmd_validate(path: &Path) -> Result<()> {
    let raw = fs::read_to_string(path)
        .await
        .with_context(|| format!("读取 {} 失败", path.display()))?;
    let json: Value =
        serde_json::from_str(&raw).with_context(|| format!("解析 {} 失败", path.display()))?;
    let workflow_id = json
        .get("workflowId")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("workflowId 缺失"))?;
    let version = json.get("version").and_then(Value::as_u64).unwrap_or(0);
    println!(
        "workflow valid: {} (version={}, keys={})",
        workflow_id,
        version,
        json.as_object().map(|m| m.len()).unwrap_or(0)
    );
    Ok(())
}

async fn cmd_ls(paths: &BeamPaths, args: &WorkflowLsArgs) -> Result<()> {
    let runs_dir = paths.workflow_runs_dir();
    let rows = list_runs(&runs_dir, args).await?;
    if args.json {
        for row in rows {
            println!("{}", serde_json::to_string(&row)?);
        }
        return Ok(());
    }
    if rows.is_empty() {
        println!("(no runs match)");
        return Ok(());
    }
    let headers = if args.wide {
        vec![
            "RUN_ID", "WORKFLOW", "STATUS", "LAST_SEQ", "UPDATED", "EVENTS", "RUN_DIR",
        ]
    } else {
        vec!["RUN_ID", "WORKFLOW", "STATUS", "LAST_SEQ", "UPDATED"]
    };
    let mut cell_rows: Vec<Vec<String>> = Vec::new();
    for row in &rows {
        let updated = format_timestamp(row.updated_at);
        let base = vec![
            row.run_id.clone(),
            row.workflow_id.clone(),
            row.status.clone(),
            row.last_seq.to_string(),
            updated,
        ];
        cell_rows.push(if args.wide {
            let mut row_cells = base;
            row_cells.push(row.events.to_string());
            row_cells.push(row.run_dir.clone());
            row_cells
        } else {
            base
        });
    }
    let widths: Vec<usize> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| {
            std::iter::once(h.len())
                .chain(cell_rows.iter().map(|row| row[i].len()))
                .max()
                .unwrap_or(0)
        })
        .collect();
    let pad = |s: &str, w: usize| format!("{}{}", s, " ".repeat(w.saturating_sub(s.len())));
    println!(
        "{}",
        headers
            .iter()
            .enumerate()
            .map(|(i, h)| pad(h, widths[i]))
            .collect::<Vec<_>>()
            .join("  ")
    );
    for row in cell_rows {
        println!(
            "{}",
            row.iter()
                .enumerate()
                .map(|(i, c)| pad(c, widths[i]))
                .collect::<Vec<_>>()
                .join("  ")
        );
    }
    Ok(())
}

async fn cmd_tail(paths: &BeamPaths, args: &WorkflowTailArgs) -> Result<()> {
    let run_dir = paths.workflow_run_dir(&args.run_id);
    let Some(events) = read_run_events_pure(&run_dir)? else {
        bail!(
            "runId={} 没找到任何事件 (runsDir={})",
            args.run_id,
            paths.workflow_runs_dir().display()
        );
    };
    if events.is_empty() {
        bail!(
            "runId={} 没找到任何事件 (runsDir={})",
            args.run_id,
            paths.workflow_runs_dir().display()
        );
    }
    let mut last_seq = 0;
    for ev in &events {
        let seq = event_seq(&ev.event_id);
        if seq < args.from {
            continue;
        }
        print_event(ev, args.json);
        last_seq = seq;
    }
    if !args.follow {
        return Ok(());
    }

    let events_path = paths.workflow_run_dir(&args.run_id).join("events.ndjson");
    let mut offset = fs::metadata(&events_path)
        .await
        .map(|m| m.len())
        .unwrap_or(0);
    let mut buffer = String::new();
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let meta = match fs::metadata(&events_path).await {
            Ok(meta) => meta,
            Err(_) => continue,
        };
        if meta.len() < offset {
            println!("(events.ndjson 大小回退，停止 tail)");
            return Ok(());
        }
        if meta.len() == offset {
            continue;
        }
        let mut file = fs::File::open(&events_path).await?;
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut chunk = vec![0u8; (meta.len() - offset) as usize];
        file.read_exact(&mut chunk).await?;
        offset = meta.len();
        buffer.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(pos) = buffer.find('\n') {
            let line = buffer[..pos].trim().to_string();
            buffer = buffer[pos + 1..].to_string();
            if line.is_empty() {
                continue;
            }
            let ev: WorkflowEventEnvelope = match serde_json::from_str(&line) {
                Ok(ev) => ev,
                Err(_) => continue,
            };
            let seq = event_seq(&ev.event_id);
            if seq <= last_seq || seq < args.from {
                continue;
            }
            last_seq = seq;
            print_event(&ev, args.json);
        }
    }
}

async fn cmd_show(paths: &BeamPaths, args: &WorkflowShowArgs) -> Result<()> {
    let run_dir = paths.workflow_run_dir(&args.run_id);
    let Some(events) = read_run_events_pure(&run_dir)? else {
        bail!(
            "runId={} 没找到任何事件 (runsDir={})",
            args.run_id,
            paths.workflow_runs_dir().display()
        );
    };
    if events.is_empty() {
        bail!(
            "runId={} 没找到任何事件 (runsDir={})",
            args.run_id,
            paths.workflow_runs_dir().display()
        );
    }
    let meta = load_workflow_meta(paths, &args.run_id).await?;
    let summary = build_summary(
        &args.run_id,
        meta,
        &events,
        &paths.workflow_run_dir(&args.run_id),
    );
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowRunRow {
    run_id: String,
    workflow_id: String,
    status: String,
    last_seq: u64,
    updated_at: u64,
    events: usize,
    run_dir: String,
}

async fn list_runs(runs_dir: &Path, args: &WorkflowLsArgs) -> Result<Vec<WorkflowRunRow>> {
    let statuses: Option<HashSet<String>> = args.status.as_ref().map(|value| {
        value
            .split(',')
            .map(|part| part.trim().to_string())
            .filter(|part| !part.is_empty())
            .collect()
    });
    let mut rows = Vec::new();
    let mut entries = match fs::read_dir(runs_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    while let Some(entry) = entries.next_entry().await? {
        let ty = entry.file_type().await?;
        if !ty.is_dir() {
            continue;
        }
        let run_id = entry.file_name().to_string_lossy().to_string();
        let run_dir = entry.path();
        let log = EventLog::new(run_id.clone(), runs_dir)?;
        let events = log.read_all().unwrap_or_default();
        if events.is_empty() {
            continue;
        }
        let (workflow_id, status, last_seq) = infer_row(&run_id, &run_dir, &events).await?;
        if !args.all && is_terminal_status(&status) {
            continue;
        }
        if let Some(filter) = &statuses {
            if !filter.contains(&status) {
                continue;
            }
        }
        let meta_path = run_dir.join("events.ndjson");
        let updated_at = fs::metadata(&meta_path)
            .await
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64)
            .unwrap_or_default();
        rows.push(WorkflowRunRow {
            run_id,
            workflow_id,
            status,
            last_seq,
            updated_at,
            events: events.len(),
            run_dir: run_dir.display().to_string(),
        });
    }
    rows.sort_by(|a, b| a.run_id.cmp(&b.run_id));
    Ok(rows)
}

async fn infer_row(
    run_id: &str,
    run_dir: &Path,
    events: &[WorkflowEventEnvelope],
) -> Result<(String, String, u64)> {
    let meta = load_workflow_meta_from_dir(run_dir)
        .await
        .unwrap_or_default();
    let workflow_id = meta
        .get("workflowId")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            events.iter().find_map(|ev| {
                if ev.event_type != "runCreated" {
                    return None;
                }
                ev.payload
                    .get("workflowId")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
        })
        .unwrap_or_else(|| "unknown".to_string());
    let status = infer_run_status(events);
    let last_seq = events
        .last()
        .map(|ev| event_seq(&ev.event_id))
        .unwrap_or_default();
    let _ = run_id;
    Ok((workflow_id, status, last_seq))
}

async fn load_workflow_meta(paths: &BeamPaths, run_id: &str) -> Result<Value> {
    load_workflow_meta_from_dir(&paths.workflow_run_dir(run_id)).await
}

async fn load_workflow_meta_from_dir(run_dir: &Path) -> Result<Value> {
    let path = run_dir.join("workflow.json");
    match fs::read_to_string(&path).await {
        Ok(raw) => Ok(serde_json::from_str(&raw)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Value::Null),
        Err(err) => Err(err.into()),
    }
}

fn build_summary(
    run_id: &str,
    meta: Value,
    events: &[WorkflowEventEnvelope],
    run_dir: &Path,
) -> Value {
    let workflow_id = meta
        .get("workflowId")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            events.iter().find_map(|ev| {
                if ev.event_type != "runCreated" {
                    return None;
                }
                ev.payload
                    .get("workflowId")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
        })
        .unwrap_or_else(|| "unknown".to_string());
    serde_json::json!({
        "runId": run_id,
        "workflowId": workflow_id,
        "status": infer_run_status(events),
        "lastSeq": events.last().map(|ev| event_seq(&ev.event_id)).unwrap_or_default(),
        "events": events.len(),
        "runDir": run_dir.display().to_string(),
        "lastEventType": events.last().map(|ev| ev.event_type.clone()),
    })
}

fn is_terminal_status(status: &str) -> bool {
    matches!(status, "succeeded" | "failed" | "cancelled")
}

fn event_seq(event_id: &str) -> u64 {
    event_id
        .rsplit_once('-')
        .and_then(|(_, seq)| seq.parse::<u64>().ok())
        .unwrap_or_default()
}

fn format_timestamp(ms: u64) -> String {
    if ms == 0 {
        return "-".to_string();
    }
    let secs = ms / 1000;
    let tm = chrono::DateTime::<chrono::Utc>::from_timestamp(secs as i64, 0);
    tm.map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn print_event(ev: &WorkflowEventEnvelope, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::to_string(ev).unwrap_or_else(|_| "{}".to_string())
        );
        return;
    }
    let seq = event_seq(&ev.event_id).to_string();
    let actor = match ev.actor {
        WorkflowActor::Scheduler => "scheduler",
        WorkflowActor::Human => "human",
        WorkflowActor::System => "system",
        WorkflowActor::Worker => "worker",
        WorkflowActor::HostExecutor => "hostExecutor",
        WorkflowActor::Supervisor => "supervisor",
    };
    let payload_hint = ev
        .payload
        .get("nodeId")
        .or_else(|| ev.payload.get("activityId"))
        .or_else(|| ev.payload.get("runId"))
        .and_then(Value::as_str)
        .unwrap_or("");
    if payload_hint.is_empty() {
        println!("{:>4}  {:<20}  {}", seq, ev.event_type, actor);
    } else {
        println!(
            "{:>4}  {:<20}  {}  {}",
            seq, ev.event_type, actor, payload_hint
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beam_core::BeamPaths;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_paths(label: &str) -> BeamPaths {
        let base = std::env::temp_dir().join(format!(
            "beam-cli-workflow-{label}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        BeamPaths::from_root(base)
    }

    #[test]
    fn infer_status_handles_terminal_events() {
        let events = vec![
            WorkflowEventEnvelope {
                event_id: "run-1-1".to_string(),
                run_id: "run-1".to_string(),
                timestamp: 1,
                schema_version: 1,
                actor: WorkflowActor::System,
                event_type: "runCreated".to_string(),
                payload: serde_json::json!({"workflowId":"flow-a"}),
                payload_hash: None,
            },
            WorkflowEventEnvelope {
                event_id: "run-1-2".to_string(),
                run_id: "run-1".to_string(),
                timestamp: 2,
                schema_version: 1,
                actor: WorkflowActor::Worker,
                event_type: "runSucceeded".to_string(),
                payload: serde_json::json!({"outputRef":{"outputHash":"sha256:0000000000000000000000000000000000000000000000000000000000000000","outputBytes":0,"outputSchemaVersion":1}}),
                payload_hash: None,
            },
        ];
        assert_eq!(infer_run_status(&events), "succeeded");
    }

    #[tokio::test]
    async fn cmd_validate_accepts_workflow_json() {
        let paths = temp_paths("validate");
        fs::create_dir_all(paths.root()).unwrap();
        let file = paths.root().join("workflow.json");
        fs::write(&file, r#"{"workflowId":"flow-a","version":1}"#).unwrap();
        cmd_validate(&file).await.expect("validate");
        let _ = fs::remove_dir_all(paths.root());
    }

    #[test]
    fn parse_raw_params_rejects_bad_pairs_and_keeps_order() {
        let parsed =
            parse_raw_params(&["foo=bar".to_string(), "baz=qux".to_string()]).expect("params");
        assert_eq!(parsed.get("baz"), Some(&Value::String("qux".to_string())));
        assert_eq!(parsed.get("foo"), Some(&Value::String("bar".to_string())));
        assert!(parse_raw_params(&["missing_equal".to_string()]).is_err());
        assert!(parse_raw_params(&["a=1".to_string(), "a=2".to_string()]).is_err());
    }

    #[test]
    fn create_workflow_run_id_sanitizes_workflow_name() {
        let run_id = create_workflow_run_id("flow/a:b");
        assert!(run_id.starts_with("flow_a_b-"));
        assert!(!run_id.contains('/'));
        assert!(!run_id.contains(':'));
    }
}
