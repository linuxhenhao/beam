pub mod antigravity;
pub mod claude;
pub mod coco;
pub mod codex;
pub mod gemini;
pub mod grok;
pub mod hermes;
pub mod kimi;
pub mod opencode;

use anyhow::{Result, bail};
use beam_core::InitConfig;

use crate::adapter::{Adapter, CliAdapter};

type AdapterFactory = fn(&InitConfig) -> Box<dyn Adapter>;

/// cli_id → adapter factory.
///
/// Adding a new adapter takes three steps: a new module in this directory
/// implementing [`Adapter`], one row here, and one row in beam-core's
/// `cli_specs::CLI_SPECS` (setup wizard / adopt / resume metadata). See
/// `docs/design/add-cli-adapter.md`.
static REGISTRY: &[(&str, AdapterFactory)] = &[
    ("claude-code", claude::create),
    ("codex", codex::create),
    ("traex", codex::create_traex),
    ("opencode", opencode::create),
    ("gemini", gemini::create),
    ("coco", coco::create),
    ("hermes", hermes::create),
    ("antigravity", antigravity::create),
    ("kimi", kimi::create),
    ("grok", grok::create),
];

pub fn create_adapter(init: &InitConfig) -> Result<CliAdapter> {
    let Some((_, factory)) = REGISTRY.iter().find(|(cli_id, _)| *cli_id == init.cli_id) else {
        bail!("unsupported cli adapter: {}", init.cli_id);
    };
    Ok(CliAdapter::new(factory(init)))
}

pub fn passes_initial_prompt_via_args(cli_id: &str) -> bool {
    beam_core::cli_specs::cli_spec(cli_id)
        .map(|spec| spec.passes_initial_prompt_via_args)
        .unwrap_or(false)
}

/// The TUI-ready marker the worker should wait for (case-insensitive) before
/// the first `write_input`, if the CLI's TUI exposes one. `None` disables the
/// gate for CLIs that accept the initial prompt via spawn args or that gate
/// themselves inside `write_input` (codex/traex).
pub fn tui_ready_marker(cli_id: &str) -> Option<&'static str> {
    beam_core::cli_specs::cli_spec(cli_id).and_then(|spec| spec.tui_ready_marker)
}

#[cfg(test)]
mod tests {
    use super::{create_adapter, tui_ready_marker};
    use crate::adapter::test_support::test_init;

    #[test]
    fn create_adapter_rejects_unknown_cli_ids() {
        let err = create_adapter(&test_init("unknown-cli")).expect_err("unknown cli should fail");
        assert!(err.to_string().contains("unsupported cli adapter"));
    }

    #[test]
    fn create_adapter_accepts_traex() {
        let init = test_init("traex");
        let adapter = create_adapter(&init).expect("traex should be supported");

        let spec = adapter.build_spawn_spec(&beam_core::InitConfig {
            cli_args: vec!["-y".to_string()],
            ..test_init("traex")
        });
        assert_eq!(spec.bin, "traex");
        assert!(spec.args.iter().any(|arg| arg == "-y"));
    }

    #[test]
    fn tui_ready_marker_lookup() {
        assert_eq!(tui_ready_marker("kimi"), Some("Welcome to Kimi Code"));
        assert_eq!(tui_ready_marker("claude-code"), Some("Welcome"));
        assert_eq!(tui_ready_marker("coco"), Some("Welcome"));
        assert_eq!(tui_ready_marker("hermes"), Some("Welcome"));
        assert_eq!(tui_ready_marker("antigravity"), Some("Welcome"));
        assert_eq!(tui_ready_marker("grok"), Some("Grok"));
        assert_eq!(tui_ready_marker("codex"), None);
        assert_eq!(tui_ready_marker("gemini"), None);
        assert_eq!(tui_ready_marker("unknown-cli"), None);
    }

    #[test]
    fn registry_covers_every_cli_spec() {
        for spec in beam_core::cli_specs::CLI_SPECS {
            assert!(
                create_adapter(&test_init(spec.cli_id)).is_ok(),
                "CLI_SPECS entry {} has no adapter factory",
                spec.cli_id
            );
        }
    }
}
