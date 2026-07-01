pub mod antigravity;
pub mod claude;
pub mod coco;
pub mod codex;
pub mod gemini;
pub mod hermes;
pub mod opencode;

use anyhow::{Result, bail};
use beam_core::InitConfig;

use crate::adapter::{AdapterKind, CliAdapter, SpawnSpec};
use crate::backend::SessionBackend;

pub fn create_adapter(init: &InitConfig) -> Result<CliAdapter> {
    let cli_id = init.cli_id.as_str();
    let adapter = match cli_id {
        "claude-code" => CliAdapter {
            kind: AdapterKind::Claude(claude::create_state(init)),
        },
        "codex" => CliAdapter {
            kind: AdapterKind::Codex(codex::create_state(init)),
        },
        "traex" => CliAdapter {
            kind: AdapterKind::Codex(codex::create_state(init)),
        },
        "opencode" => CliAdapter {
            kind: AdapterKind::OpenCode(opencode::create_state(init)),
        },
        "gemini" => CliAdapter {
            kind: AdapterKind::Gemini(gemini::create_state(init)),
        },
        "coco" => CliAdapter {
            kind: AdapterKind::CoCo(coco::create_state(init)),
        },
        "hermes" => CliAdapter {
            kind: AdapterKind::Hermes(hermes::create_state(init)),
        },
        "antigravity" => CliAdapter {
            kind: AdapterKind::Antigravity(antigravity::create_state(init)),
        },
        _ => bail!("unsupported cli adapter: {}", init.cli_id),
    };
    Ok(adapter)
}

pub fn build_spawn_spec(adapter: &CliAdapter, init: &InitConfig) -> SpawnSpec {
    match &adapter.kind {
        AdapterKind::Claude(state) => claude::build_spawn_spec(state, init),
        AdapterKind::Codex(state) => codex::build_spawn_spec(state, init),
        AdapterKind::OpenCode(state) => opencode::build_spawn_spec(state, init),
        AdapterKind::Gemini(state) => gemini::build_spawn_spec(state, init),
        AdapterKind::CoCo(state) => coco::build_spawn_spec(state, init),
        AdapterKind::Hermes(state) => hermes::build_spawn_spec(state, init),
        AdapterKind::Antigravity(state) => antigravity::build_spawn_spec(state, init),
    }
}

pub async fn write_input(
    adapter: &mut CliAdapter,
    backend: &dyn SessionBackend,
    content: &str,
) -> Result<crate::adapter::SubmitResult> {
    match &mut adapter.kind {
        AdapterKind::Claude(state) => claude::write_input(state, backend, content).await,
        AdapterKind::Codex(state) => codex::write_input(state, backend, content).await,
        AdapterKind::OpenCode(state) => opencode::write_input(state, backend, content).await,
        AdapterKind::Gemini(state) => gemini::write_input(state, backend, content).await,
        AdapterKind::CoCo(state) => coco::write_input(state, backend, content).await,
        AdapterKind::Hermes(state) => hermes::write_input(state, backend, content).await,
        AdapterKind::Antigravity(state) => antigravity::write_input(state, backend, content).await,
    }
}

pub fn poll(adapter: &mut CliAdapter) -> Result<crate::adapter::PollResult> {
    match &mut adapter.kind {
        AdapterKind::Claude(state) => claude::poll(state),
        AdapterKind::Codex(state) => codex::poll(state),
        AdapterKind::OpenCode(state) => opencode::poll(state),
        AdapterKind::Gemini(state) => gemini::poll(state),
        AdapterKind::CoCo(state) => coco::poll(state),
        AdapterKind::Hermes(state) => hermes::poll(state),
        AdapterKind::Antigravity(state) => antigravity::poll(state),
    }
}

pub fn on_spawned(adapter: &mut CliAdapter, child_pid: Option<u32>) {
    match &mut adapter.kind {
        AdapterKind::Claude(state) => state.cli_pid = child_pid,
        AdapterKind::Codex(state) => state.cli_pid = child_pid,
        AdapterKind::OpenCode(_)
        | AdapterKind::Gemini(_)
        | AdapterKind::CoCo(_)
        | AdapterKind::Hermes(_)
        | AdapterKind::Antigravity(_) => {}
    }
}

pub fn passes_initial_prompt_via_args(cli_id: &str) -> bool {
    matches!(cli_id, "opencode" | "gemini")
}

#[cfg(test)]
mod tests {
    use super::create_adapter;
    use beam_core::{InitConfig, ScreenAnalyzerConfig};

    fn init(cli_id: &str) -> InitConfig {
        InitConfig {
            session_id: "sid".to_string(),
            title: "title".to_string(),
            chat_id: "chat".to_string(),
            root_message_id: "root".to_string(),
            working_dir: ".".to_string(),
            cli_id: cli_id.to_string(),
            cli_bin: cli_id.to_string(),
            cli_args: vec![],

            prompt: String::new(),
            resume: false,
            cli_session_id: None,
            lark_app_id: "app".to_string(),
            lark_app_secret: "secret".to_string(),
            prompt_turn_id: None,
            owner_open_id: None,
            adopted_from: None,
            adopt_restored_from_metadata: false,
            screen_analyzer: ScreenAnalyzerConfig::default(),
            initial_prompt: None,
            model: None,
            locale: None,
            bot_name: None,
            bot_open_id: None,
            resume_session_id: None,
            disable_cli_bypass: false,
        }
    }

    #[test]
    fn create_adapter_rejects_unknown_cli_ids() {
        let err = create_adapter(&init("unknown-cli")).expect_err("unknown cli should fail");
        assert!(err.to_string().contains("unsupported cli adapter"));
    }

    #[test]
    fn create_adapter_accepts_traex() {
        let adapter = create_adapter(&InitConfig {
            cli_id: "traex".to_string(),
            cli_bin: "traex".to_string(),
            cli_args: vec!["-y".to_string()],
            ..init("traex")
        })
        .expect("traex should be supported");

        let spec = crate::adapters::build_spawn_spec(
            &adapter,
            &InitConfig {
                cli_id: "traex".to_string(),
                cli_bin: "traex".to_string(),
                cli_args: vec!["-y".to_string()],
                ..init("traex")
            },
        );
        assert_eq!(spec.bin, "traex");
        assert!(spec.args.iter().any(|arg| arg == "-y"));
    }
}
