//! Unified tracing subscriber initialization for all beam binaries.
//!
//! Provides `init_tracing()` to configure a compact, human-readable tracing
//! subscriber writing to stderr with target display enabled for module-level
//! filtering. The default level is INFO, overridable via the standard
//! `RUST_LOG` environment variable.
//!
//! # Examples
//!
//! ```rust,no_run
//! beam_core::logging::init_tracing();
//! ```
//!
//! Change log level at runtime via `RUST_LOG`:
//!
//! ```bash
//! RUST_LOG='beam_daemon=debug,beam_worker=trace' beam restart
//! ```

use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

/// Initialize the tracing subscriber exactly once per process.
///
/// Safe to call multiple times; subsequent calls are no-ops. Configures:
///
/// - Default level: `INFO`
/// - `RUST_LOG` support via [`EnvFilter::from_env_lossy`]
/// - Compact, human-readable single-line format
/// - Target (module path) display enabled for module-level filtering
/// - Output exclusively to stderr (worker stdout is reserved for JSON IPC)
///
/// This function does not read or log secrets, tokens, or user content.
/// It does not expose a global mutable reload handle.
pub fn init_tracing() {
    // try_init is idempotent: returns Ok on first call, Err on subsequent
    // calls after a global subscriber has already been set. Silently
    // ignoring the error is the standard tracing ecosystem pattern.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_writer(std::io::stderr)
        .with_target(true)
        .compact()
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_tracing_is_idempotent() {
        // Should not panic when called multiple times within the same process.
        init_tracing();
        init_tracing();
        init_tracing();
    }

    #[test]
    fn default_env_filter_builds_with_info() {
        // Building a filter with only the default directive should succeed
        // and produce a usable filter. from_env_lossy() parses RUST_LOG; even
        // when the env var is absent or invalid, the builder guarantees the
        // default INFO directive is present.
        let filter = EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .from_env_lossy();
        // A valid filter always provides a max level hint.
        assert!(filter.max_level_hint().is_some());
    }

    #[test]
    fn valid_module_directive_parses() {
        // Simulate what RUST_LOG=beam_core=debug would produce.
        let filter = EnvFilter::try_new("beam_core=debug").expect("valid directive should parse");
        assert!(filter.max_level_hint().is_some());
    }

    #[test]
    fn invalid_rust_log_fallback_does_not_panic() {
        // A completely invalid RUST_LOG value must not cause a panic or
        // process abort. from_env_lossy() silently falls back to the default
        // directive in this case. We test the parsing path directly.
        let filter = EnvFilter::try_new("!!!invalid!!!");
        // It may parse as a regex target filter or return an error; either
        // way, the critical invariant is "does not panic".
        if let Err(_e) = filter {
            // Invalid directives are expected to fail parsing. from_env_lossy()
            // handles this gracefully by falling back to the default.
        }
    }
}
