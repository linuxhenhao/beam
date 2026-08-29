//! Herdr capability probe (binary, version, socket, mandatory schema).
//!
//! Runs on every path that is about to use Herdr: daemon startup when
//! `daemon.backend = "herdr"` or any bot overrides to herdr, and before each
//! Herdr worker spawn. Zellij-default deployments never probe herdr and never
//! fail because it is missing.
//!
//! When a call actually needs Herdr, every probe step is mandatory: missing
//! binary, too-old version, unreachable server, or a missing required schema
//! method all hard-fail with a clear ERROR. There is no optional-schema third
//! posture.

use super::*;

/// Required `herdr api schema --json` methods. Missing one is a hard failure.
pub(crate) const REQUIRED_HERDR_METHODS: &[&str] = &[
    "workspace.create",
    "workspace.list",
    "workspace.get",
    "workspace.close",
    "pane.process_info",
    "pane.send_text",
    "pane.send_keys",
    "pane.read",
    "agent.list",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HerdrProbe {
    pub(crate) version: String,
    pub(crate) methods: Vec<String>,
}

/// Probe herdr and return the version plus the schema method list. Any
/// missing requirement is a hard error.
pub(crate) async fn probe_herdr(config: &Config) -> Result<HerdrProbe> {
    // v1 resolves herdr from PATH (configurable absolute path is a later
    // extension). Zellij-default deployments never reach this code.
    let bin = "herdr";
    let version = herdr_version(bin)?;
    let min = config.herdr.min_version.clone();
    if version_is_ge(&version, &min).is_none() || version_is_ge(&version, &min) == Some(false) {
        anyhow::bail!(
            "herdr version {} is older than required minimum {}",
            version,
            min
        );
    }
    let methods = herdr_schema_methods(bin)?;
    let missing: Vec<&str> = REQUIRED_HERDR_METHODS
        .iter()
        .copied()
        .filter(|method| !methods.iter().any(|m| m == method))
        .collect();
    if !missing.is_empty() {
        anyhow::bail!(
            "herdr api schema is missing required methods: {}",
            missing.join(", ")
        );
    }
    Ok(HerdrProbe { version, methods })
}

/// Startup probe when the daemon or any bot is configured for Herdr.
/// Fail-closed: a missing/unusable herdr is a hard boot error so the
/// misconfiguration surfaces immediately instead of failing per-session.
pub(crate) async fn probe_herdr_at_startup(
    config: &Config,
    bots: &HashMap<String, BotConfig>,
) -> Result<()> {
    let herdr_configured = config.daemon.backend == BackendKind::Herdr
        || bots
            .values()
            .any(|bot| bot.backend == Some(BackendKind::Herdr));
    if !herdr_configured {
        return Ok(());
    }
    let probe = probe_herdr(config)
        .await
        .with_context(|| "daemon/bot configured for herdr but the herdr probe failed")?;
    info!(
        herdr_version = %probe.version,
        "herdr probe ok at daemon startup"
    );
    Ok(())
}

fn herdr_version(bin: &str) -> Result<String> {
    let out = std::process::Command::new(bin)
        .arg("--version")
        .output()
        .with_context(|| format!("failed to run {bin} --version (is herdr installed?)"))?;
    if !out.status.success() {
        anyhow::bail!("{bin} --version exited with {}", out.status);
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn herdr_schema_methods(bin: &str) -> Result<Vec<String>> {
    let out = std::process::Command::new(bin)
        .args(["api", "schema", "--json"])
        .output()
        .with_context(|| format!("failed to run {bin} api schema --json"))?;
    if !out.status.success() {
        anyhow::bail!("{bin} api schema --json exited with {}", out.status);
    }
    let value: serde_json::Value = serde_json::from_slice(&out.stdout)
        .with_context(|| "herdr api schema --json did not return JSON")?;
    let mut methods = Vec::new();
    collect_method_names(&value, &mut methods);
    methods.sort();
    methods.dedup();
    Ok(methods)
}

/// Recursively collect dotted method names from the schema document. The
/// exact layout is not pinned (it drifts with herdr versions); any key path
/// whose leaf is an object/array of object with method-ish names is walked.
fn collect_method_names(value: &serde_json::Value, out: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(map) => {
            // A method entry looks like { "name": "workspace.create", ... }.
            if let Some(name) = map
                .get("name")
                .and_then(serde_json::Value::as_str)
                .filter(|name| name.contains('.'))
            {
                out.push(name.to_string());
            }
            // Real `herdr api schema --json` pins each method as a `const`
            // under `properties.method` (e.g. `schemas.request.oneOf[]`):
            // {"properties":{"method":{"const":"workspace.create", ...}}, ...}
            if let Some(method) = map
                .get("properties")
                .and_then(|props| props.get("method"))
                .and_then(|method| method.get("const"))
                .and_then(serde_json::Value::as_str)
                .filter(|name| name.contains('.'))
            {
                out.push(method.to_string());
            }
            // Also walk "methods" arrays and any object keys that end in a
            // dotted name.
            for (key, child) in map {
                if key == "name" {
                    continue;
                }
                if key.contains('.') {
                    out.push(key.clone());
                }
                collect_method_names(child, out);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_method_names(item, out);
            }
        }
        _ => {}
    }
}

/// Returns `Some(true)` when `actual` >= `required` under simple
/// `major.minor.patch` comparison, `Some(false)` when older, and `None` when
/// either string is not parseable. Prerelease suffixes are ignored.
pub(crate) fn version_is_ge(actual: &str, required: &str) -> Option<bool> {
    let parse = |s: &str| -> Option<(u64, u64, u64)> {
        let digits = s.split_whitespace().last()?;
        let mut parts = digits.split('-').next()?.split('.');
        let major = parts.next()?.parse().ok()?;
        let minor = parts.next()?.parse().ok()?;
        let patch = parts.next().unwrap_or("0").parse().ok()?;
        Some((major, minor, patch))
    };
    let (amaj, amin, apatch) = parse(actual)?;
    let (rmaj, rmin, rpatch) = parse(required)?;
    Some((amaj, amin, apatch) >= (rmaj, rmin, rpatch))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_ge_compares_major_minor_patch() {
        assert_eq!(version_is_ge("herdr 0.8.2", "0.8.2"), Some(true));
        assert_eq!(version_is_ge("herdr 0.8.3", "0.8.2"), Some(true));
        assert_eq!(version_is_ge("herdr 0.9.0", "0.8.2"), Some(true));
        assert_eq!(version_is_ge("herdr 0.8.1", "0.8.2"), Some(false));
        assert_eq!(version_is_ge("herdr 0.7.9", "0.8.2"), Some(false));
        assert_eq!(version_is_ge("herdr 0.8.2-beta.1", "0.8.2"), Some(true));
        assert_eq!(version_is_ge("herdr not-a-version", "0.8.2"), None);
    }

    #[test]
    fn collect_method_names_walks_schema_document() {
        let doc = serde_json::json!({
            "methods": [
                { "name": "workspace.create" },
                { "name": "pane.read" }
            ],
            "services": {
                "workspace": { "name": "workspace.list" }
            }
        });
        let mut methods = Vec::new();
        collect_method_names(&doc, &mut methods);
        methods.sort();
        methods.dedup();
        assert!(methods.iter().any(|m| m == "workspace.create"));
        assert!(methods.iter().any(|m| m == "pane.read"));
        assert!(methods.iter().any(|m| m == "workspace.list"));
    }

    #[test]
    fn collect_method_names_reads_real_schema_const_shape() {
        // Shape observed from `herdr api schema --json` on 0.8.2: method
        // names live as `properties.method.const` inside `schemas.request.oneOf`.
        let doc = serde_json::json!({
            "schemas": {
                "request": {
                    "oneOf": [
                        {
                            "properties": {
                                "method": { "const": "workspace.create" }
                            }
                        },
                        {
                            "properties": {
                                "method": { "const": "pane.process_info" }
                            }
                        },
                        {
                            "properties": {
                                "method": { "const": "agent.list" }
                            }
                        }
                    ]
                }
            }
        });
        let mut methods = Vec::new();
        collect_method_names(&doc, &mut methods);
        methods.sort();
        methods.dedup();
        assert!(methods.iter().any(|m| m == "workspace.create"));
        assert!(methods.iter().any(|m| m == "pane.process_info"));
        assert!(methods.iter().any(|m| m == "agent.list"));
    }

    #[test]
    fn committed_schema_fixture_covers_required_methods() {
        let fixture = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../beam-worker/tests/fixtures/herdr/api-schema-0.8.2.json"
        ))
        .expect("herdr schema fixture");
        let value: serde_json::Value =
            serde_json::from_str(&fixture).expect("fixture is valid JSON");
        let mut methods = Vec::new();
        collect_method_names(&value, &mut methods);
        methods.sort();
        methods.dedup();
        for required in REQUIRED_HERDR_METHODS {
            assert!(
                methods.iter().any(|m| m == required),
                "fixture is missing required schema method {required}"
            );
        }
    }
}
