use std::fs::{read_to_string, remove_file, write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use beam_core::BeamPaths;

const LABEL: &str = "com.beam.daemon";
const SERVICE_NAME: &str = "beam.service";
const UNIT_FILE_NAME: &str = "beam.service";

#[derive(Debug, Clone)]
pub struct AutostartOpts {
    pub exe: PathBuf,
    pub paths: BeamPaths,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutostartAction {
    Enable,
    Disable,
    Status,
    Refresh,
}

fn platform() -> &'static str {
    if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else {
        "unsupported"
    }
}

fn path_env() -> String {
    std::env::var("PATH").unwrap_or_else(|_| {
        if cfg!(target_os = "macos") {
            "/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin".to_string()
        } else {
            "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_string()
        }
    })
}

fn plist_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    Path::new(&home)
        .join("Library")
        .join("LaunchAgents")
        .join(format!("{LABEL}.plist"))
}

fn unit_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    Path::new(&home)
        .join(".config")
        .join("systemd")
        .join("user")
        .join(UNIT_FILE_NAME)
}

fn cli_js(opts: &AutostartOpts) -> PathBuf {
    opts.exe.clone()
}

fn launchctl_uid() -> String {
    let out = Command::new("id")
        .args(["-u"])
        .stderr(Stdio::null())
        .output();
    match out {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).trim().to_string(),
        _ => "0".to_string(),
    }
}

fn launchctl_bootstrap(plist: &Path) -> bool {
    let uid = launchctl_uid();
    let r = Command::new("launchctl")
        .args([
            "bootstrap",
            &format!("gui/{uid}"),
            &plist.display().to_string(),
        ])
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .status();
    if matches!(r, Ok(status) if status.success()) {
        return true;
    }
    let r = Command::new("launchctl")
        .args(["load", "-w", &plist.display().to_string()])
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .status();
    matches!(r, Ok(status) if status.success())
}

fn launchctl_bootout() -> bool {
    let uid = launchctl_uid();
    let r = Command::new("launchctl")
        .args(["bootout", &format!("gui/{uid}/{LABEL}")])
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .status();
    if matches!(r, Ok(status) if status.success()) {
        return true;
    }
    let r = Command::new("launchctl")
        .args(["unload", "-w", &plist_path().display().to_string()])
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .status();
    matches!(r, Ok(status) if status.success())
}

fn launchctl_loaded() -> bool {
    let uid = launchctl_uid();
    Command::new("launchctl")
        .args(["print", &format!("gui/{uid}/{LABEL}")])
        .stderr(Stdio::null())
        .stdout(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn plist_content(opts: &AutostartOpts) -> String {
    let cli = cli_js(opts);
    let cwd = opts.paths.root();
    let out_log = opts.paths.logs_dir().join("autostart-out.log");
    let err_log = opts.paths.logs_dir().join("autostart-err.log");
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{node}</string>
        <string>{cli}</string>
        <string>start</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <false/>
    <key>WorkingDirectory</key>
    <string>{cwd}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>{path}</string>
    </dict>
    <key>StandardOutPath</key>
    <string>{out_log}</string>
    <key>StandardErrorPath</key>
    <string>{err_log}</string>
</dict>
</plist>
"#,
        label = LABEL,
        node = cli.display(),
        cli = cli.display(),
        cwd = cwd.display(),
        path = path_env(),
        out_log = out_log.display(),
        err_log = err_log.display(),
    )
}

fn unit_content(opts: &AutostartOpts) -> String {
    format!(
        r#"[Unit]
Description=beam daemon (IM <-> AI coding CLI bridge)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory={cwd}
Environment=PATH={path}
ExecStart={exe} start
ExecStop={exe} stop

[Install]
WantedBy=default.target
"#,
        cwd = opts.paths.root().display(),
        path = path_env(),
        exe = opts.exe.display(),
    )
}

fn user_systemd_available() -> bool {
    Command::new("systemctl")
        .args(["--user", "show-environment"])
        .stderr(Stdio::null())
        .stdout(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn linger_enabled() -> bool {
    let username = std::env::var("USER").unwrap_or_default();
    if username.is_empty() {
        return false;
    }
    let out = Command::new("loginctl")
        .args(["show-user", &username, "--property=Linger"])
        .stderr(Stdio::null())
        .output();
    match out {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout)
            .trim()
            .ends_with("=yes"),
        _ => false,
    }
}

fn enable_macos(opts: &AutostartOpts) -> Result<()> {
    let path = plist_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::create_dir_all(opts.paths.logs_dir())?;
    write(&path, plist_content(opts))?;
    println!("✅ 已写入 LaunchAgent: {}", path.display());
    if launchctl_loaded() {
        let _ = launchctl_bootout();
        if launchctl_bootstrap(&path) {
            println!("✅ 已重新加载到 launchd (路径已更新)");
        }
    } else {
        println!("   下次登录时自动启动。立即启动: beam start");
    }
    Ok(())
}

fn disable_macos() -> Result<()> {
    let path = plist_path();
    if launchctl_loaded() {
        if launchctl_bootout() {
            println!("✅ 已从 launchd 卸载 {}", LABEL);
        } else {
            println!("⚠️  launchctl 卸载失败，继续删除 plist");
        }
    }
    if path.exists() {
        remove_file(&path)?;
        println!("✅ 已删除 {}", path.display());
        println!("   pm2 daemon 仍在运行；要停止请跑 beam stop");
    } else {
        println!("ℹ️  {} 不存在，无需删除", path.display());
    }
    Ok(())
}

fn status_macos() -> Result<()> {
    let path = plist_path();
    let loaded = launchctl_loaded();
    println!("平台: macOS (launchd)");
    println!("Plist 路径: {}", path.display());
    println!("Plist 存在: {}", if path.exists() { "yes" } else { "no" });
    println!("launchd 已加载: {}", if loaded { "yes" } else { "no" });
    if path.exists() && !loaded {
        println!("提示: plist 存在但未加载，运行 beam autostart enable 重新激活");
    }
    Ok(())
}

fn refresh_macos(opts: &AutostartOpts) -> Result<bool> {
    let path = plist_path();
    if !path.exists() {
        return Ok(false);
    }
    let next = plist_content(opts);
    let prev = read_to_string(&path)?;
    if prev == next {
        return Ok(false);
    }
    write(&path, next)?;
    if launchctl_loaded() {
        let _ = launchctl_bootout();
        let _ = launchctl_bootstrap(&path);
    }
    Ok(true)
}

fn enable_linux(opts: &AutostartOpts) -> Result<()> {
    if !user_systemd_available() {
        bail!("当前会话连不上 user systemd（缺少 DBus / 容器环境）");
    }
    let path = unit_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    write(&path, unit_content(opts))?;
    println!("✅ 已写入 systemd unit: {}", path.display());
    let reload = Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status()
        .context("systemctl --user daemon-reload failed")?;
    if !reload.success() {
        bail!("systemctl --user daemon-reload 失败");
    }
    let en = Command::new("systemctl")
        .args(["--user", "enable", SERVICE_NAME])
        .status()
        .context("systemctl --user enable failed")?;
    if !en.success() {
        bail!("systemctl --user enable 失败");
    }
    println!("✅ 已启用 {}", SERVICE_NAME);
    println!("   下次开机自动启动。立即启动: beam start");
    if !linger_enabled() {
        let username = std::env::var("USER").unwrap_or_default();
        if !username.is_empty() {
            println!();
            println!("⚠️  Linger 未启用：登出当前会话后服务会停止。");
            println!("   要让服务跨登出/重启常驻，运行（需要 sudo）:");
            println!("     sudo loginctl enable-linger {}", username);
        }
    }
    Ok(())
}

fn disable_linux() -> Result<()> {
    if !user_systemd_available() {
        bail!("当前会话连不上 user systemd。");
    }
    let path = unit_path();
    let dis = Command::new("systemctl")
        .args(["--user", "disable", SERVICE_NAME])
        .status()
        .context("systemctl --user disable failed")?;
    if dis.success() {
        println!("✅ 已禁用 {}", SERVICE_NAME);
    } else {
        println!("⚠️  disable 返回非零（可能本来就未启用）");
    }
    if path.exists() {
        remove_file(&path)?;
        println!("✅ 已删除 {}", path.display());
        let _ = Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .status();
    } else {
        println!("ℹ️  {} 不存在", path.display());
    }
    println!("   pm2 daemon 仍在运行；要停止请跑 beam stop");
    Ok(())
}

fn status_linux() -> Result<()> {
    let path = unit_path();
    println!("平台: Linux (user systemd)");
    println!("Unit 路径: {}", path.display());
    println!("Unit 存在: {}", if path.exists() { "yes" } else { "no" });
    if !user_systemd_available() {
        println!("user systemd: 不可用（缺少 DBus / 容器环境）");
        return Ok(());
    }
    let is_enabled = Command::new("systemctl")
        .args(["--user", "is-enabled", SERVICE_NAME])
        .output()?;
    let is_active = Command::new("systemctl")
        .args(["--user", "is-active", SERVICE_NAME])
        .output()?;
    println!(
        "enabled: {}",
        String::from_utf8_lossy(&is_enabled.stdout)
            .trim()
            .to_string()
    );
    println!(
        "active: {}",
        String::from_utf8_lossy(&is_active.stdout)
            .trim()
            .to_string()
    );
    println!(
        "Linger: {}",
        if linger_enabled() {
            "yes"
        } else {
            "no（登出后服务会停）"
        }
    );
    Ok(())
}

fn refresh_linux(opts: &AutostartOpts) -> Result<bool> {
    let path = unit_path();
    if !path.exists() {
        return Ok(false);
    }
    let next = unit_content(opts);
    let prev = read_to_string(&path)?;
    if prev == next {
        return Ok(false);
    }
    write(&path, next)?;
    if user_systemd_available() {
        let _ = Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .status();
    }
    Ok(true)
}

pub fn enable_autostart(opts: &AutostartOpts) -> Result<()> {
    match platform() {
        "macos" => enable_macos(opts),
        "linux" => enable_linux(opts),
        _ => bail!(
            "当前平台 {} 暂不支持 beam autostart。",
            std::env::consts::OS
        ),
    }
}

pub fn disable_autostart() -> Result<()> {
    match platform() {
        "macos" => disable_macos(),
        "linux" => disable_linux(),
        _ => bail!(
            "当前平台 {} 暂不支持 beam autostart。",
            std::env::consts::OS
        ),
    }
}

pub fn autostart_status() -> Result<()> {
    match platform() {
        "macos" => status_macos(),
        "linux" => status_linux(),
        _ => {
            println!("平台: {} (不支持)", std::env::consts::OS);
            Ok(())
        }
    }
}

pub fn refresh_autostart(opts: &AutostartOpts) -> Result<bool> {
    match platform() {
        "macos" => refresh_macos(opts),
        "linux" => refresh_linux(opts),
        _ => Ok(false),
    }
}

pub fn parse_action(args: &[String]) -> AutostartAction {
    match args.first().map(|s| s.as_str()) {
        Some("disable" | "uninstall") => AutostartAction::Disable,
        Some("status") => AutostartAction::Status,
        Some("refresh") => AutostartAction::Refresh,
        Some("enable" | "install") | None => AutostartAction::Enable,
        Some(_) => AutostartAction::Enable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beam_core::BeamPaths;

    #[test]
    fn parse_action_defaults_to_enable() {
        assert_eq!(parse_action(&[]), AutostartAction::Enable);
        assert_eq!(
            parse_action(&["enable".to_string()]),
            AutostartAction::Enable
        );
        assert_eq!(
            parse_action(&["status".to_string()]),
            AutostartAction::Status
        );
        assert_eq!(
            parse_action(&["disable".to_string()]),
            AutostartAction::Disable
        );
    }

    #[test]
    fn render_contents_include_paths() {
        let root = std::env::temp_dir().join(format!("beam-autostart-{}", std::process::id()));
        let paths = BeamPaths::from_root(root);
        let opts = AutostartOpts {
            exe: PathBuf::from("/tmp/beam"),
            paths,
        };
        let unit = unit_content(&opts);
        assert!(unit.contains("/tmp/beam start"));
        let plist = plist_content(&opts);
        assert!(plist.contains("<string>start</string>"));
    }
}
