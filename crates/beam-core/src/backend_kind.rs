use serde::{Deserialize, Serialize};

/// Terminal multiplexer / agent runtime backend for a Beam session.
///
/// Existing deployments default to [`BackendKind::Zellij`]; Herdr is an
/// opt-in first-class backend selected per daemon or per bot. The value is
/// persisted on `Session` at create/adopt time so a later config flip cannot
/// silently move an existing session to the wrong mux.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum BackendKind {
    #[default]
    Zellij,
    Herdr,
}

impl BackendKind {
    pub fn as_str(self) -> &'static str {
        match self {
            BackendKind::Zellij => "zellij",
            BackendKind::Herdr => "herdr",
        }
    }
}
