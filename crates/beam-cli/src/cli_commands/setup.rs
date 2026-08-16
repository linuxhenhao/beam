use super::load_bots;
use crate::*;
use anyhow::{Context, Result, bail};
use beam_core::cli_specs::{CLI_SPECS, cli_spec};

pub(crate) fn setup_backup_file(path: &Path) -> Result<Option<PathBuf>> {
    if !path.exists() {
        return Ok(None);
    }
    let backup = path.with_extension(format!(
        "{}.bak",
        path.extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("bak")
    ));
    std::fs::copy(path, &backup)?;
    Ok(Some(backup))
}

pub(crate) async fn validate_setup_credentials(app_id: &str, app_secret: &str) -> Result<()> {
    if std::env::var("BEAM_SKIP_SETUP_VALIDATION").ok().as_deref() == Some("1") {
        println!("⚠️  已跳过远程凭证校验（BEAM_SKIP_SETUP_VALIDATION=1）。");
        return Ok(());
    }
    let client = reqwest::Client::new();
    let resp = client
        .post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal")
        .json(&serde_json::json!({
            "app_id": app_id,
            "app_secret": app_secret,
        }))
        .send()
        .await
        .context("failed to reach Feishu/Lark credential endpoint")?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or_else(|_| serde_json::json!({}));
    if !status.is_success() {
        bail!("凭证校验失败: HTTP {}", status);
    }
    match body.get("code").and_then(|v| v.as_i64()).unwrap_or(-1) {
        0 => Ok(()),
        code => bail!(
            "凭证校验失败: code={} msg={}",
            code,
            body.get("msg").and_then(|v| v.as_str()).unwrap_or("")
        ),
    }
}

pub(crate) fn default_cli_args_for_cli_id(cli_id: &str) -> Vec<String> {
    cli_spec(cli_id)
        .map(|spec| {
            spec.default_cli_args
                .iter()
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn parse_cli_args_input(input: &str, defaults: &[String]) -> Vec<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return defaults.to_vec();
    }
    if matches!(trimmed.to_ascii_lowercase().as_str(), "clear" | "none") {
        return Vec::new();
    }
    trimmed
        .split_whitespace()
        .map(ToString::to_string)
        .collect()
}

pub(crate) fn setup_prompts_cgroup_slice() -> bool {
    cfg!(target_os = "linux")
}

pub(crate) fn parse_cgroup_slice_input(input: &str) -> Option<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() || matches!(trimmed.to_ascii_lowercase().as_str(), "clear" | "none") {
        None
    } else {
        Some(trimmed.to_string())
    }
}

pub(crate) fn detect_installed_clis() -> Vec<&'static beam_core::cli_specs::CliSpec> {
    CLI_SPECS
        .iter()
        .filter(|spec| spec.bin_candidates.iter().any(|b| which_exists(b)))
        .collect()
}

pub(crate) fn bin_candidates_for_cli_id(cli_id: &str) -> Option<&'static [&'static str]> {
    cli_spec(cli_id).map(|spec| spec.bin_candidates)
}

pub(crate) fn probe_cli_bin(cli_id: &str) -> Option<String> {
    bin_candidates_for_cli_id(cli_id)
        .and_then(|bins| bins.iter().find(|b| which_exists(b)).copied())
        .map(String::from)
}

pub(crate) fn resolve_allowed_users(input: &str, user_open_id: Option<&str>) -> Vec<String> {
    let mut allowed: Vec<String> = input
        .split(',')
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .collect();
    if let Some(open_id) = user_open_id
        && !allowed.iter().any(|item| item == open_id)
    {
        allowed.push(open_id.to_string());
    }
    allowed
}

pub(crate) fn which_exists(bin: &str) -> bool {
    std::process::Command::new("which")
        .arg(bin)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

pub(crate) fn prompt_cli_id() -> Result<String> {
    let installed = detect_installed_clis();
    if installed.is_empty() {
        println!("未检测到已安装的 CLI 工具。");
        let value = ask_line("请手动输入 CLI ID [claude-code]: ")?;
        let value = value.trim();
        if value.is_empty() {
            return Ok("claude-code".to_string());
        }
        let valid_ids: Vec<&str> = CLI_SPECS.iter().map(|spec| spec.cli_id).collect();
        if valid_ids.contains(&value) {
            return Ok(value.to_string());
        }
        println!(
            "不支持的 CLI ID \"{}\"，支持: {}",
            value,
            valid_ids.join(", ")
        );
        return Ok("claude-code".to_string());
    }

    println!("已检测到以下 CLI 工具:");
    for (i, spec) in installed.iter().enumerate() {
        let bin_str = spec.bin_candidates.join(" / ");
        println!("  {}) {}  ({})", i + 1, spec.label, bin_str);
    }

    let value = ask_line("CLI 适配器 [1]: ")?;
    let value = value.trim();
    if value.is_empty() {
        return Ok(installed[0].cli_id.to_string());
    }
    if let Ok(num) = value.parse::<usize>() {
        if num >= 1 && num <= installed.len() {
            return Ok(installed[num - 1].cli_id.to_string());
        }
        println!("无效序号 \"{}\"，请输入 1-{}", num, installed.len());
    } else {
        let valid_ids: Vec<&str> = CLI_SPECS.iter().map(|spec| spec.cli_id).collect();
        if valid_ids.contains(&value) {
            return Ok(value.to_string());
        }
        println!(
            "不支持的 CLI ID \"{}\"，支持: {}",
            value,
            valid_ids.join(", ")
        );
    }
    Ok(installed[0].cli_id.to_string())
}

pub(crate) async fn prompt_setup_bot() -> Result<BotConfig> {
    let name = ask_line("机器人名称（留空=不设）: ")?;
    let credentials = register_app::prompt_credentials().await?;
    let cli_id = prompt_cli_id()?;
    let default_cli_args = default_cli_args_for_cli_id(&cli_id);
    let default_cli_args_display = if default_cli_args.is_empty() {
        "（无）".to_string()
    } else {
        default_cli_args.join(" ")
    };
    let cli_args_input = ask_line(&format!(
        "启动参数 cliArgs [{}]（回车使用默认，输入 clear 清空）: ",
        default_cli_args_display
    ))?;
    let cli_args = parse_cli_args_input(&cli_args_input, &default_cli_args);
    let cgroup_slice = if setup_prompts_cgroup_slice() {
        let value =
            ask_line("cgroup slice cgroupSlice [空]（回车跳过，例如 cgtproxy-gateway.slice）: ")?;
        parse_cgroup_slice_input(&value)
    } else {
        None
    };
    let cli_bin = probe_cli_bin(&cli_id).filter(|bin| bin != &cli_id);
    let working_dir = {
        let value = ask_line("默认工作目录 [~]: ")?;
        if value.trim().is_empty() {
            Some("~".to_string())
        } else {
            Some(value)
        }
    };
    let skip_working_dir_prompt = {
        let value = ask_line("是否跳过工作目录选择？[y/N]: ")?;
        matches!(value.trim().to_lowercase().as_str(), "y" | "yes")
    };
    let allowed_users = {
        let hint = if credentials.user_open_id.is_some() {
            "允许用户 open_id（逗号分隔，留空=仅限自己，你的 open_id 已自动加入）: "
        } else {
            "允许用户 open_id（逗号分隔，例如 ou_xxxxx；留空=不限制，任何人都可对话⚠️）: "
        };
        let value = ask_line(hint)?;
        let resolved = resolve_allowed_users(&value, credentials.user_open_id.as_deref());
        if credentials.user_open_id.is_none() && resolved.is_empty() {
            println!("   ⚠️  未设置允许用户：当前为开放模式，任何人都可以和机器人对话。");
            println!(
                "   💡 可在 bots.json 中手动填写 allowedUsers 字段（open_id 以 ou_ 开头），或后续用 /grant 命令授权。"
            );
        }
        resolved
    };

    Ok(BotConfig {
        name: if name.trim().is_empty() {
            None
        } else {
            Some(name)
        },
        lark_app_id: credentials.app_id,
        lark_app_secret: credentials.app_secret,
        cli_id,
        cli_bin,
        cgroup_slice,
        cli_args,
        model: None,
        working_dir,
        skip_working_dir_prompt,
        lark_encrypt_key: None,
        lark_verification_token: None,
        allowed_users,
        private_card: false,
        allowed_chat_groups: Vec::new(),
        chat_grants: std::collections::HashMap::new(),
        global_grants: Vec::new(),
        oncall_chats: Vec::new(),
        restrict_grant_commands: false,
        message_quota: None,
        quota_state: std::collections::HashMap::new(),
        custom_triggers: Vec::new(),
    })
}

pub(crate) async fn cmd_setup(paths: &BeamPaths) -> Result<()> {
    let root = paths.root();
    std::fs::create_dir_all(root)?;
    for dir in [
        paths.logs_dir(),
        paths.run_dir(),
        paths.sessions_dir(),
        paths.workflows_dir(),
        paths.workflow_runs_dir(),
        paths.state_dir(),
        paths.cli_pid_markers_dir(),
        paths.observed_bots_dir(),
        paths.schedules_output_dir(),
    ] {
        std::fs::create_dir_all(&dir)?;
    }

    let cfg = paths.config_toml();
    if !cfg.exists() {
        let defaults = "\
[daemon]\nworking_dirs = [\"~\"]\n\n\
[web]\nhost = \"0.0.0.0\"\nproxy_base_port = 8800\n\n\
";
        std::fs::write(&cfg, defaults)?;
        println!("Wrote {}", cfg.display());
    } else {
        println!("Config exists: {}", cfg.display());
    }

    let bots = paths.bots_json();
    if !bots.exists() {
        std::fs::write(&bots, "[]\n")?;
        println!("Wrote {}", bots.display());
    } else {
        println!("Bots config exists: {}", bots.display());
    }

    println!("Setup complete. Data root: {}", root.display());
    println!("现有 bots 数量: {}", load_bots(paths)?.len());
    println!();

    let existing = load_bots(paths)?;
    let action = if existing.is_empty() {
        "replace".to_string()
    } else {
        println!("已检测到现有机器人配置：");
        for (i, bot) in existing.iter().enumerate() {
            println!(
                "  {}. {} ({})",
                i + 1,
                bot.name.clone().unwrap_or_else(|| bot.cli_id.clone()),
                bot.lark_app_id
            );
        }
        ask_line("操作 [replace/add/skip] [replace]: ")?
    };

    let action = action.trim().to_lowercase();
    let action = if action.is_empty() {
        "replace".to_string()
    } else {
        action
    };
    if action == "skip" {
        println!("已跳过 setup 写盘。");
        return Ok(());
    }

    let mut next_bots = if action == "add" {
        existing.clone()
    } else {
        Vec::new()
    };
    let next_bot = prompt_setup_bot().await?;
    validate_setup_credentials(&next_bot.lark_app_id, &next_bot.lark_app_secret).await?;
    next_bots.push(next_bot);

    if bots.exists()
        && let Some(backup) = setup_backup_file(&bots)?
    {
        println!("旧配置已备份: {}", backup.display());
    }
    std::fs::write(&bots, serde_json::to_string_pretty(&next_bots)? + "\n")?;
    println!("✅ 已写入 {}", bots.display());

    if let Err(err) = hook_setup::install_hooks() {
        eprintln!("hook install skipped: {}", err);
    } else {
        println!("Installed hook config for Claude/OpenCode.");
    }

    println!("提示：先运行 `beam start`，再用 `beam autostart enable` 注册自启。");
    Ok(())
}
