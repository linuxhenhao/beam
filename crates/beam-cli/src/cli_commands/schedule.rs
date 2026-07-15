use crate::*;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ScheduleRecord {
    #[serde(rename = "scheduleId")]
    schedule_id: String,
    content: String,
    #[serde(rename = "createdAt")]
    created_at: String,
    status: String,
}

pub(crate) fn read_schedule_records(paths: &BeamPaths) -> Result<Vec<ScheduleRecord>> {
    match std::fs::read_to_string(paths.schedules_json()) {
        Ok(raw) => Ok(serde_json::from_str(&raw)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err.into()),
    }
}

pub(crate) fn write_schedule_records(paths: &BeamPaths, records: &[ScheduleRecord]) -> Result<()> {
    if let Some(parent) = paths.schedules_json().parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(
        paths.schedules_json(),
        serde_json::to_string_pretty(records)? + "\n",
    )?;
    Ok(())
}

pub(crate) fn cmd_schedule(args: Vec<String>, paths: &BeamPaths) -> Result<()> {
    let sub = args.first().map(|s| s.as_str()).unwrap_or("list");
    let rest = if args.is_empty() { &[][..] } else { &args[1..] };
    let mut records = read_schedule_records(paths)?;

    let find_id =
        |rest: &[String]| -> Option<String> { rest.iter().find(|s| !s.starts_with('-')).cloned() };

    match sub {
        "list" | "ls" => {
            if records.is_empty() {
                println!("暂无定时任务。");
                return Ok(());
            }
            for task in &records {
                println!(
                    "[{}] {} | {} | {}",
                    task.schedule_id, task.status, task.created_at, task.content
                );
            }
        }
        "add" => {
            let positional: Vec<String> = rest
                .iter()
                .filter(|arg| !arg.starts_with("--"))
                .cloned()
                .collect();
            if positional.is_empty() {
                anyhow::bail!("Usage: beam schedule add <schedule> <prompt>");
            }
            let raw_schedule = positional[0].clone();
            let prompt = positional
                .iter()
                .skip(1)
                .cloned()
                .collect::<Vec<_>>()
                .join(" ");
            let parsed =
                beam_core::parse_schedule(&raw_schedule).map_err(|err| anyhow::anyhow!(err))?;
            let content = if prompt.is_empty() {
                if let Some(natural) = beam_core::parse_natural_schedule(&positional.join(" ")) {
                    natural.prompt
                } else {
                    raw_schedule.clone()
                }
            } else {
                prompt
            };
            let task = ScheduleRecord {
                schedule_id: format!(
                    "sched-{}-{}",
                    chrono::Utc::now().timestamp_millis(),
                    std::process::id()
                ),
                content,
                created_at: chrono::Utc::now().to_rfc3339(),
                status: "active".to_string(),
            };
            println!("parsed schedule: {}", parsed.display);
            println!("{}", serde_json::to_string_pretty(&task)?);
            records.push(task);
            write_schedule_records(paths, &records)?;
        }
        "remove" | "rm" | "delete" | "del" => {
            let Some(id) = find_id(rest) else {
                anyhow::bail!("Usage: beam schedule remove <scheduleId>");
            };
            let before = records.len();
            records.retain(|task| task.schedule_id != id);
            if records.len() == before {
                anyhow::bail!("未找到任务 {}", id);
            }
            write_schedule_records(paths, &records)?;
            println!("已删除任务 {}", id);
        }
        "pause" | "disable" => {
            let Some(id) = find_id(rest) else {
                anyhow::bail!("Usage: beam schedule pause <scheduleId>");
            };
            let mut found = false;
            for task in &mut records {
                if task.schedule_id == id {
                    task.status = "paused".to_string();
                    found = true;
                }
            }
            if !found {
                anyhow::bail!("未找到任务 {}", id);
            }
            write_schedule_records(paths, &records)?;
            println!("已暂停任务 {}", id);
        }
        "resume" | "enable" => {
            let Some(id) = find_id(rest) else {
                anyhow::bail!("Usage: beam schedule resume <scheduleId>");
            };
            let mut found = false;
            for task in &mut records {
                if task.schedule_id == id {
                    task.status = "active".to_string();
                    found = true;
                }
            }
            if !found {
                anyhow::bail!("未找到任务 {}", id);
            }
            write_schedule_records(paths, &records)?;
            println!("已恢复任务 {}", id);
        }
        "run" => {
            let Some(id) = find_id(rest) else {
                anyhow::bail!("Usage: beam schedule run <scheduleId>");
            };
            let Some(task) = records.iter().find(|task| task.schedule_id == id) else {
                anyhow::bail!("未找到任务 {}", id);
            };
            println!(
                "{{\"scheduleId\":\"{}\",\"content\":\"{}\",\"status\":\"{}\",\"run\":\"now\"}}",
                task.schedule_id,
                task.content.replace('"', "\\\""),
                task.status,
            );
        }
        "logs" => {
            let Some(id) = find_id(rest) else {
                anyhow::bail!("Usage: beam schedule logs <scheduleId>");
            };
            let dir = paths.schedules_output_dir().join(&id);
            if !dir.exists() {
                println!("无日志：{}", dir.display());
            } else {
                for entry in std::fs::read_dir(dir)? {
                    let entry = entry?;
                    println!("{}", entry.path().display());
                }
            }
        }
        _ => {
            anyhow::bail!("未知子命令: {}", sub);
        }
    }

    Ok(())
}
