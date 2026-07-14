use crate::*;
use anyhow::Result;

pub(crate) fn cmd_autostart(paths: &BeamPaths, args: Vec<String>) -> Result<()> {
    let action = autostart::parse_action(&args);
    let opts = autostart::AutostartOpts {
        exe: std::env::current_exe()?,
        paths: paths.clone(),
    };
    match action {
        autostart::AutostartAction::Enable => autostart::enable_autostart(&opts),
        autostart::AutostartAction::Disable => autostart::disable_autostart(),
        autostart::AutostartAction::Status => autostart::autostart_status(),
        autostart::AutostartAction::Refresh => {
            if autostart::refresh_autostart(&opts)? {
                println!("✅ autostart 已刷新");
            } else {
                println!("ℹ️  autostart 无需更新");
            }
            Ok(())
        }
    }
}
