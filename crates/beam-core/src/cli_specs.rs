//! Static metadata for supported CLI adapters.
//!
//! Single source of truth for per-CLI registration data shared by
//! beam-cli (setup wizard), beam-daemon (zellij adopt, workflow resume),
//! and beam-worker (spawn env, prompt passing). Adding a new CLI adapter
//! requires exactly one new row in [`CLI_SPECS`] here (plus the adapter
//! implementation in beam-worker).

/// Static description of one supported CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CliSpec {
    /// Canonical CLI identifier used in bot config and session records.
    pub cli_id: &'static str,
    /// Human-readable label shown in the setup wizard.
    pub label: &'static str,
    /// Binary names probed on PATH by the setup wizard, in priority order.
    pub bin_candidates: &'static [&'static str],
    /// Default launch args suggested by the setup wizard for this CLI.
    pub default_cli_args: &'static [&'static str],
    /// Lowercase substrings matched (in [`CLI_SPECS`] order) against a zellij
    /// pane command basename to recognize this CLI during adopt. Empty means
    /// the CLI is never auto-recognized from a pane command.
    pub adopt_command_patterns: &'static [&'static str],
    /// Whether the adapter implements `init.resume` (workflow resume).
    pub supports_resume: bool,
    /// Whether the CLI accepts an initial prompt via spawn args while staying
    /// interactive (opencode `--prompt`, gemini `-i`).
    pub passes_initial_prompt_via_args: bool,
    /// Case-insensitive substring the CLI's TUI renders once its input UI is
    /// initialized (e.g. kimi's "Welcome to Kimi Code"). The worker waits for
    /// this marker before the first `write_input` so keystrokes typed during
    /// TUI boot are not dropped. `None` disables the gate (CLIs that accept
    /// the initial prompt via spawn args, or adapters that gate themselves).
    pub tui_ready_marker: Option<&'static str>,
    /// Whether the worker injects `TERM=xterm-256color` when the inherited
    /// TERM is missing/empty/`dumb` (codex/traex require it).
    pub inject_term_xterm: bool,
}

/// All supported CLIs, in setup-wizard display order.
pub const CLI_SPECS: &[CliSpec] = &[
    CliSpec {
        cli_id: "claude-code",
        label: "Claude",
        bin_candidates: &["claude"],
        default_cli_args: &[],
        adopt_command_patterns: &["claude"],
        supports_resume: true,
        passes_initial_prompt_via_args: false,
        tui_ready_marker: Some("Welcome"),
        inject_term_xterm: false,
    },
    CliSpec {
        cli_id: "codex",
        label: "Codex",
        bin_candidates: &["codex"],
        default_cli_args: &[
            "--dangerously-bypass-approvals-and-sandbox",
            "--no-alt-screen",
        ],
        adopt_command_patterns: &["codex"],
        supports_resume: true,
        passes_initial_prompt_via_args: false,
        tui_ready_marker: None,
        inject_term_xterm: true,
    },
    CliSpec {
        cli_id: "traex",
        label: "Traex",
        bin_candidates: &["traex"],
        default_cli_args: &["-y"],
        adopt_command_patterns: &["traex"],
        supports_resume: true,
        passes_initial_prompt_via_args: false,
        tui_ready_marker: None,
        inject_term_xterm: true,
    },
    CliSpec {
        cli_id: "coco",
        label: "CoCo",
        bin_candidates: &["coco"],
        default_cli_args: &[],
        adopt_command_patterns: &[],
        supports_resume: true,
        passes_initial_prompt_via_args: false,
        tui_ready_marker: Some("Welcome"),
        inject_term_xterm: false,
    },
    CliSpec {
        cli_id: "gemini",
        label: "Gemini",
        bin_candidates: &["gemini"],
        default_cli_args: &[],
        adopt_command_patterns: &["gemini"],
        supports_resume: false,
        passes_initial_prompt_via_args: true,
        tui_ready_marker: None,
        inject_term_xterm: false,
    },
    CliSpec {
        cli_id: "opencode",
        label: "OpenCode",
        bin_candidates: &["opencode-cli", "opencode"],
        default_cli_args: &[],
        adopt_command_patterns: &["opencode"],
        supports_resume: false,
        passes_initial_prompt_via_args: true,
        tui_ready_marker: None,
        inject_term_xterm: false,
    },
    CliSpec {
        cli_id: "hermes",
        label: "Hermes",
        bin_candidates: &["hermes"],
        default_cli_args: &[],
        adopt_command_patterns: &["hermes"],
        supports_resume: true,
        passes_initial_prompt_via_args: false,
        tui_ready_marker: Some("Welcome"),
        inject_term_xterm: false,
    },
    CliSpec {
        cli_id: "antigravity",
        label: "Antigravity",
        bin_candidates: &["agy"],
        default_cli_args: &[],
        adopt_command_patterns: &[],
        supports_resume: true,
        passes_initial_prompt_via_args: false,
        tui_ready_marker: Some("Welcome"),
        inject_term_xterm: false,
    },
    CliSpec {
        cli_id: "kimi",
        label: "Kimi",
        bin_candidates: &["kimi"],
        default_cli_args: &[],
        adopt_command_patterns: &["kimi"],
        supports_resume: true,
        passes_initial_prompt_via_args: false,
        tui_ready_marker: Some("Welcome to Kimi Code"),
        inject_term_xterm: false,
    },
    CliSpec {
        cli_id: "grok",
        label: "Grok Build",
        bin_candidates: &["grok"],
        default_cli_args: &[],
        adopt_command_patterns: &["grok"],
        supports_resume: true,
        passes_initial_prompt_via_args: false,
        tui_ready_marker: Some("Grok"),
        inject_term_xterm: false,
    },
];

/// Look up the spec for a CLI id. Returns `None` for unknown ids.
pub fn cli_spec(cli_id: &str) -> Option<&'static CliSpec> {
    CLI_SPECS.iter().find(|spec| spec.cli_id == cli_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_spec_has_id_label_and_bins() {
        for spec in CLI_SPECS {
            assert!(!spec.cli_id.is_empty(), "empty cli_id");
            assert!(!spec.label.is_empty(), "{}: empty label", spec.cli_id);
            assert!(
                !spec.bin_candidates.is_empty(),
                "{}: empty bin_candidates",
                spec.cli_id
            );
        }
    }

    #[test]
    fn cli_spec_lookup() {
        assert_eq!(cli_spec("kimi").map(|s| s.label), Some("Kimi"));
        assert!(cli_spec("no-such-cli").is_none());
    }

    #[test]
    fn cli_ids_are_unique() {
        let mut ids: Vec<_> = CLI_SPECS.iter().map(|s| s.cli_id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), CLI_SPECS.len(), "duplicate cli_id in CLI_SPECS");
    }

    // The assertions below lock the flags to the behavior that was previously
    // hard-coded at the individual call sites. Update them only when a CLI
    // genuinely changes capability.

    #[test]
    fn resume_support_matches_legacy_allow_list() {
        let resume: Vec<_> = CLI_SPECS
            .iter()
            .filter(|s| s.supports_resume)
            .map(|s| s.cli_id)
            .collect();
        assert_eq!(
            resume,
            vec![
                "claude-code",
                "codex",
                "traex",
                "coco",
                "hermes",
                "antigravity",
                "kimi",
                "grok"
            ]
        );
    }

    #[test]
    fn initial_prompt_via_args_matches_legacy_list() {
        let pass: Vec<_> = CLI_SPECS
            .iter()
            .filter(|s| s.passes_initial_prompt_via_args)
            .map(|s| s.cli_id)
            .collect();
        assert_eq!(pass, vec!["gemini", "opencode"]);
    }

    #[test]
    fn tui_ready_markers_match_expected_list() {
        let gated: Vec<_> = CLI_SPECS
            .iter()
            .filter_map(|s| s.tui_ready_marker.map(|marker| (s.cli_id, marker)))
            .collect();
        assert_eq!(
            gated,
            vec![
                ("claude-code", "Welcome"),
                ("coco", "Welcome"),
                ("hermes", "Welcome"),
                ("antigravity", "Welcome"),
                ("kimi", "Welcome to Kimi Code"),
                ("grok", "Grok"),
            ]
        );
    }

    #[test]
    fn term_injection_matches_legacy_list() {
        let term: Vec<_> = CLI_SPECS
            .iter()
            .filter(|s| s.inject_term_xterm)
            .map(|s| s.cli_id)
            .collect();
        assert_eq!(term, vec!["codex", "traex"]);
    }

    #[test]
    fn default_args_match_legacy_values() {
        assert_eq!(
            cli_spec("codex").unwrap().default_cli_args,
            &[
                "--dangerously-bypass-approvals-and-sandbox",
                "--no-alt-screen"
            ]
        );
        assert_eq!(cli_spec("traex").unwrap().default_cli_args, &["-y"]);
        for spec in CLI_SPECS {
            if spec.cli_id != "codex" && spec.cli_id != "traex" {
                assert!(
                    spec.default_cli_args.is_empty(),
                    "{}: unexpected default args",
                    spec.cli_id
                );
            }
        }
    }

    #[test]
    fn adopt_patterns_match_legacy_recognition() {
        let recognized: Vec<_> = CLI_SPECS
            .iter()
            .filter(|s| !s.adopt_command_patterns.is_empty())
            .map(|s| s.cli_id)
            .collect();
        assert_eq!(
            recognized,
            vec![
                "claude-code",
                "codex",
                "traex",
                "gemini",
                "opencode",
                "hermes",
                "kimi",
                "grok"
            ]
        );
    }

    #[test]
    fn adopt_patterns_do_not_overlap_across_clis() {
        // A pattern of one CLI must not be a substring of another CLI's
        // pattern, otherwise CLI_SPECS ordering would decide recognition.
        for (i, a) in CLI_SPECS.iter().enumerate() {
            for b in CLI_SPECS.iter().skip(i + 1) {
                for pa in a.adopt_command_patterns {
                    for pb in b.adopt_command_patterns {
                        assert!(
                            !pa.contains(pb) && !pb.contains(pa),
                            "adopt patterns overlap: {} ({}) vs {} ({})",
                            a.cli_id,
                            pa,
                            b.cli_id,
                            pb
                        );
                    }
                }
            }
        }
    }
}
