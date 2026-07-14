use crate::*;
use anyhow::{Context, Result, bail};

#[derive(Debug, Default)]
pub(crate) struct MigrateFlags {
    pub(crate) dry_run: bool,
    pub(crate) force: bool,
}

pub(crate) fn parse_migrate_flags(args: &[String]) -> Result<MigrateFlags> {
    let mut flags = MigrateFlags::default();
    for arg in args {
        match arg.as_str() {
            "--dry-run" => flags.dry_run = true,
            "--force" | "-f" => flags.force = true,
            "--backup" => {}
            other => bail!("未知 migrate 参数: {}", other),
        }
    }
    Ok(flags)
}

pub(crate) async fn cmd_migrate(paths: &BeamPaths, args: Vec<String>) -> Result<()> {
    let flags = parse_migrate_flags(&args)?;
    let home = std::env::var("HOME").context("HOME env var not set")?;
    let ts_root = std::path::PathBuf::from(home).join(".beam");
    let ts_bots = ts_root.join("bots.json");

    if !ts_bots.exists() {
        println!("No TS bots.json found at {}", ts_bots.display());
        println!("Nothing to migrate.");
        return Ok(());
    }

    let raw = std::fs::read_to_string(&ts_bots)
        .with_context(|| format!("failed to read {}", ts_bots.display()))?;
    let bots: serde_json::Value = serde_json::from_str(&raw)
        .with_context(|| format!("invalid JSON in {}", ts_bots.display()))?;

    let rs_bots = paths.bots_json();
    let mut conflict_report = Vec::new();
    let existing_bots = if rs_bots.exists() {
        let existing = std::fs::read_to_string(&rs_bots)?;
        if existing.trim().len() > 2 {
            conflict_report.push(format!("bots: {}", rs_bots.display()));
            Some(existing)
        } else {
            None
        }
    } else {
        None
    };

    let ts_sessions = ts_root.join("sessions.json");
    let sessions: Vec<serde_json::Value> = if ts_sessions.exists() {
        let raw = std::fs::read_to_string(&ts_sessions)
            .with_context(|| format!("failed to read {}", ts_sessions.display()))?;
        serde_json::from_str(&raw)
            .with_context(|| format!("invalid JSON in {}", ts_sessions.display()))?
    } else {
        Vec::new()
    };

    let session_plan: Vec<(String, std::path::PathBuf, serde_json::Value)> = sessions
        .iter()
        .map(|session| {
            let session_id = session
                .get("sessionId")
                .or_else(|| session.get("session_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let out_path = paths.sessions_dir().join(format!("{}.json", session_id));
            (session_id, out_path, session.clone())
        })
        .collect();

    let session_conflicts: Vec<String> = session_plan
        .iter()
        .filter(|(_, path, _)| path.exists())
        .map(|(id, path, _)| format!("session {} -> {}", id, path.display()))
        .collect();
    conflict_report.extend(session_conflicts.iter().cloned());

    if !conflict_report.is_empty() {
        println!("迁移冲突报告:");
        for item in &conflict_report {
            println!("  - {}", item);
        }
        if !flags.force {
            println!("使用 --force 可覆盖这些冲突目标；使用 --dry-run 仅查看报告。");
            if flags.dry_run {
                return Ok(());
            }
            bail!("迁移目标已有内容，已停止。");
        }
    }

    if flags.dry_run {
        println!("Dry run:");
        println!(
            "  TS bots: {}",
            bots.as_array().map(|a| a.len()).unwrap_or(0)
        );
        println!("  TS sessions: {}", session_plan.len());
        println!("  Rust bots target: {}", rs_bots.display());
        for (_, path, _) in &session_plan {
            println!("  session target: {}", path.display());
        }
        return Ok(());
    }

    std::fs::create_dir_all(paths.root())?;
    if existing_bots.is_some() {
        let backup = rs_bots.with_extension("json.bak");
        std::fs::copy(&rs_bots, &backup)?;
        println!("旧 bots 备份: {}", backup.display());
    }
    std::fs::write(&rs_bots, serde_json::to_string_pretty(&bots)?)?;
    println!("Migrated {} -> {}", ts_bots.display(), rs_bots.display());
    println!(
        "{} bot(s) migrated.",
        bots.as_array().map(|a| a.len()).unwrap_or(0)
    );

    if !session_plan.is_empty() {
        std::fs::create_dir_all(paths.sessions_dir())?;
        let mut migrated = 0usize;
        for (session_id, out_path, session) in session_plan {
            if out_path.exists() {
                let backup = out_path.with_extension("json.bak");
                std::fs::copy(&out_path, &backup)?;
                println!("旧 session 备份: {}", backup.display());
            }
            let migrated_session = serde_json::json!({
                "session_id": session_id,
                "title": session.get("title").unwrap_or(&serde_json::json!("migrated")),
                "chat_id": session.get("chatId").or_else(|| session.get("chat_id")).unwrap_or(&serde_json::json!("")),
                "root_message_id": session.get("rootMessageId").or_else(|| session.get("root_message_id")).unwrap_or(&serde_json::json!("")),
                "scope": session.get("scope").unwrap_or(&serde_json::json!("thread")),
                "status": "closed",
                "created_at": chrono::Utc::now().to_rfc3339(),
                "lark_app_id": session.get("larkAppId").or_else(|| session.get("lark_app_id")).unwrap_or(&serde_json::json!("unknown")),
                "owner_open_id": session.get("ownerOpenId").or_else(|| session.get("owner_open_id")),
                "cli_id": session.get("cliId").or_else(|| session.get("cli_id")),
                "cli_bin": session.get("cliBin").or_else(|| session.get("cli_bin")),
                "working_dir": session.get("workingDir").or_else(|| session.get("working_dir")),
            });
            tokio::fs::write(
                &out_path,
                serde_json::to_string_pretty(&migrated_session)? + "\n",
            )
            .await?;
            migrated += 1;
        }
        println!("Migrated {} session(s).", migrated);
    }

    Ok(())
}
