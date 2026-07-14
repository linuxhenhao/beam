use std::collections::BTreeMap;

use anyhow::Result;
use serde_json::Value;

use crate::{ParamDef, WorkflowDefinition};

pub fn normalize_workflow_params(
    def: &WorkflowDefinition,
    input: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>> {
    let Some(params_def) = &def.params else {
        // No schema defined — reject any supplied params.
        if input.is_empty() {
            return Ok(BTreeMap::new());
        }
        let unknown: Vec<String> = input.keys().cloned().collect();
        anyhow::bail!(
            "unknown workflow parameter(s): {}. No parameters are declared for this workflow.",
            unknown.join(", ")
        );
    };

    // Empty params schema — same behaviour as no schema.
    if params_def.is_empty() {
        if input.is_empty() {
            return Ok(BTreeMap::new());
        }
        let unknown: Vec<String> = input.keys().cloned().collect();
        anyhow::bail!(
            "unknown workflow parameter(s): {}. No parameters are declared for this workflow.",
            unknown.join(", ")
        );
    }

    // ── Reject unknown keys ────────────────────────────────────────────
    let defined_keys: std::collections::HashSet<&String> = params_def.keys().collect();
    let unknown: Vec<String> = input
        .keys()
        .filter(|k| !defined_keys.contains(*k))
        .cloned()
        .collect();

    if !unknown.is_empty() {
        let available: Vec<&str> = params_def.keys().map(String::as_str).collect();
        anyhow::bail!(
            "unknown workflow parameter(s): {}. Available parameters: [{}]",
            unknown.join(", "),
            available.join(", ")
        );
    }

    // ── Validate and normalize each defined param ──────────────────────
    let mut normalized = BTreeMap::new();
    let mut missing: Vec<String> = Vec::new();

    for (name, param_def) in params_def {
        match input.get(name) {
            Some(value) => {
                // Coerce string inputs to the target type declared in the
                // schema (e.g. "true" → bool, "42" → integer).  Type/syntax
                // errors from coercion are surfaced here.
                let coerced = coerce_param_value(name, param_def, value)?;

                validate_param_type(name, param_def, &coerced)?;
                validate_param_format(name, param_def, &coerced)?;

                // Special handling: required string that is blank/whitespace
                // is treated as missing (preserving previous semantics).
                if param_def.required == Some(true) && param_def.param_type == "string" {
                    if let Value::String(s) = &coerced {
                        if s.trim().is_empty() {
                            missing.push(name.clone());
                            continue;
                        }
                    }
                }
                // For non-string required types with blank input, type
                // validation already failed above (e.g. "" is not a bool).
                normalized.insert(name.clone(), coerced);
            }
            None => {
                if param_def.required == Some(true) {
                    missing.push(name.clone());
                } else if let Some(default) = &param_def.default {
                    // Default values are NOT coerced — they must already be
                    // the correct typed JSON in the schema definition.
                    validate_param_type(name, param_def, default)?;
                    validate_param_format(name, param_def, default)?;
                    normalized.insert(name.clone(), default.clone());
                }
                // else: not required, no default → not written.
            }
        }
    }

    if !missing.is_empty() {
        anyhow::bail!(
            "missing required workflow parameter(s): {}",
            missing.join(", ")
        );
    }

    Ok(normalized)
}

/// Validate a single param value against its declared type.
fn validate_param_type(name: &str, def: &ParamDef, value: &Value) -> Result<()> {
    match def.param_type.as_str() {
        "string" => {
            if !value.is_string() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'string', got {}",
                    name,
                    describe_value_kind(value)
                );
            }
        }
        "number" => {
            if !value.is_number() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'number', got {}",
                    name,
                    describe_value_kind(value)
                );
            }
        }
        "integer" => match value {
            Value::Number(n) => {
                let is_int = n.as_i64().is_some()
                    || n.as_u64().is_some()
                    || n.as_f64().map(|f| f.fract() == 0.0).unwrap_or(false);
                if !is_int {
                    anyhow::bail!(
                        "workflow parameter '{}' expects type 'integer', got non-integer number",
                        name
                    );
                }
            }
            _ => anyhow::bail!(
                "workflow parameter '{}' expects type 'integer', got {}",
                name,
                describe_value_kind(value)
            ),
        },
        "boolean" => {
            if !value.is_boolean() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'boolean', got {}",
                    name,
                    describe_value_kind(value)
                );
            }
        }
        "object" => {
            if !value.is_object() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'object', got {}",
                    name,
                    describe_value_kind(value)
                );
            }
        }
        "array" => {
            if !value.is_array() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'array', got {}",
                    name,
                    describe_value_kind(value)
                );
            }
        }
        unknown => {
            anyhow::bail!(
                "workflow parameter '{}' has unknown type '{}'",
                name,
                unknown
            );
        }
    }
    Ok(())
}

/// Validate the format annotation of a param value.
///
/// Format only applies to string-typed parameters.  Unknown formats and
/// format-on-non-string are hard errors (no silent ignore).
fn validate_param_format(name: &str, def: &ParamDef, value: &Value) -> Result<()> {
    let Some(format) = &def.format else {
        return Ok(());
    };

    // Format is only valid for string-typed params.
    if def.param_type != "string" {
        anyhow::bail!(
            "workflow parameter '{}' has format '{}' but type is '{}'; format is only valid for string type",
            name,
            format,
            def.param_type,
        );
    }

    let Value::String(s) = value else {
        // Type validation should have caught mismatches already; be safe.
        return Ok(());
    };

    match format.as_str() {
        "date" => validate_date(name, s),
        "date-time" => validate_date_time(name, s),
        "email" => validate_email(name, s),
        unknown => {
            anyhow::bail!(
                "workflow parameter '{}' has unknown format '{}'",
                name,
                unknown,
            );
        }
    }
}

/// Validate a `date` format string: must be a real calendar date in YYYY-MM-DD.
fn validate_date(name: &str, value: &str) -> Result<()> {
    chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| {
        anyhow::anyhow!(
            "workflow parameter '{}' with format 'date' must be a valid date (YYYY-MM-DD), got: {}",
            name,
            value,
        )
    })?;
    Ok(())
}

/// Validate a `date-time` format string: must parse as RFC 3339.
fn validate_date_time(name: &str, value: &str) -> Result<()> {
    // chrono::DateTime::parse_from_rfc3339 handles timezone offset correctly.
    chrono::DateTime::parse_from_rfc3339(value).map_err(|_| {
        anyhow::anyhow!(
            "workflow parameter '{}' with format 'date-time' must be valid RFC 3339, got: {}",
            name,
            value,
        )
    })?;
    Ok(())
}

/// Validate an `email` format string: lightweight checks (one @, non-empty
/// local/domain, domain has at least one dot, domain labels are non-empty and
/// don't start/end with `-`, no whitespace).
fn validate_email(name: &str, value: &str) -> Result<()> {
    // Must have exactly one '@'.
    let at_count = value.chars().filter(|&c| c == '@').count();
    if at_count != 1 {
        anyhow::bail!(
            "workflow parameter '{}' with format 'email' must contain exactly one '@', got: {}",
            name,
            value,
        );
    }

    let at_pos = value.find('@').unwrap();
    let local = &value[..at_pos];
    let domain = &value[at_pos + 1..];

    if local.is_empty() {
        anyhow::bail!(
            "workflow parameter '{}' with format 'email' has empty local part, got: {}",
            name,
            value,
        );
    }
    if domain.is_empty() {
        anyhow::bail!(
            "workflow parameter '{}' with format 'email' has empty domain part, got: {}",
            name,
            value,
        );
    }

    // Domain must contain at least one dot.
    if !domain.contains('.') {
        anyhow::bail!(
            "workflow parameter '{}' with format 'email' domain must contain at least one dot, got: {}",
            name,
            value,
        );
    }

    // Reject whitespace anywhere.
    if value.chars().any(|c| c.is_whitespace()) {
        anyhow::bail!(
            "workflow parameter '{}' with format 'email' must not contain whitespace, got: {}",
            name,
            value,
        );
    }

    // Domain labels must be non-empty and not start/end with '-'.
    for label in domain.split('.') {
        if label.is_empty() {
            anyhow::bail!(
                "workflow parameter '{}' with format 'email' domain has empty label, got: {}",
                name,
                value,
            );
        }
        if label.starts_with('-') || label.ends_with('-') {
            anyhow::bail!(
                "workflow parameter '{}' with format 'email' domain label must not start or end with '-', got: {}",
                name,
                value,
            );
        }
    }

    Ok(())
}

fn describe_value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Coerce a string input value to the target parameter type declared in the
/// workflow params schema.
///
/// If the input value is already the target JSON type, it passes through
/// unchanged.  Only `Value::String` inputs are coerced; other mismatched
/// types are left for the subsequent type-validation step to reject.
///
/// Coercion rules per type:
/// - `string`:  pass through unchanged (no JSON parse).
/// - `boolean`: accept case-sensitive `true` / `false` strings (trimmed).
/// - `number`:  parse as JSON number via `serde_json`; rejects NaN/inf/
///              blank/objects/arrays/non-numeric JSON.
/// - `integer`: accept only decimal integer strings (e.g. `42`, `-1`);
///              rejects `1.0`, `1.5`, `1e3` and other formats.
/// - `object`:  parse string as JSON, must produce a JSON object.
/// - `array`:   parse string as JSON, must produce a JSON array.
///
/// This coercion is only applied to external user input, never to schema
/// default values (so schema authors cannot paper over type errors in the
/// definition).
fn coerce_param_value(name: &str, def: &ParamDef, value: &Value) -> Result<Value> {
    let target_type = def.param_type.as_str();

    // If the value is already the target JSON type, no coercion needed.
    let already_typed = match target_type {
        "string" => value.is_string(),
        "number" | "integer" => value.is_number(),
        "boolean" => value.is_boolean(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        _ => false,
    };
    if already_typed {
        return Ok(value.clone());
    }

    // Only attempt coercion from string.
    if !value.is_string() {
        // Not a string and not the target type — let type validation handle it.
        return Ok(value.clone());
    }

    let s = value.as_str().unwrap();

    match target_type {
        "string" => {
            // string → string: no coercion, pass through as-is.
            Ok(value.clone())
        }
        "boolean" => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'boolean', got empty string",
                    name
                );
            }
            match trimmed {
                "true" => Ok(Value::Bool(true)),
                "false" => Ok(Value::Bool(false)),
                _ => anyhow::bail!(
                    "workflow parameter '{}' expects type 'boolean', got string '{}' (only 'true' or 'false' are accepted)",
                    name,
                    trimmed
                ),
            }
        }
        "number" => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'number', got empty string",
                    name
                );
            }
            let v: Value = serde_json::from_str(trimmed).map_err(|e| {
                anyhow::anyhow!(
                    "workflow parameter '{}' expects type 'number', failed to parse '{}' as a number: {}",
                    name, trimmed, e
                )
            })?;
            if !v.is_number() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'number', but string '{}' parsed to {}",
                    name,
                    trimmed,
                    describe_value_kind(&v)
                );
            }
            Ok(v)
        }
        "integer" => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'integer', got empty string",
                    name
                );
            }

            // Only accept decimal integer strings: optional leading '-', then
            // only ASCII digits. Reject floats, scientific notation, etc.
            let is_decimal_integer = trimmed.chars().enumerate().all(|(i, c)| {
                if i == 0 && c == '-' {
                    trimmed.len() > 1 // "-" alone is not valid
                } else {
                    c.is_ascii_digit()
                }
            });

            if !is_decimal_integer {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'integer', got string '{}' (only decimal integer strings like '42' or '-1' are accepted)",
                    name,
                    trimmed
                );
            }

            // Parse as i128 and construct a JSON number.
            let n: i128 = trimmed.parse().map_err(|_| {
                anyhow::anyhow!(
                    "workflow parameter '{}' expects type 'integer', failed to parse '{}'",
                    name,
                    trimmed
                )
            })?;
            let num = serde_json::Number::from_i128(n).ok_or_else(|| {
                anyhow::anyhow!(
                    "workflow parameter '{}' expects type 'integer', number out of range: '{}'",
                    name,
                    trimmed
                )
            })?;
            Ok(Value::Number(num))
        }
        "object" => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'object', got empty string",
                    name
                );
            }
            let v: Value = serde_json::from_str(trimmed).map_err(|e| {
                anyhow::anyhow!(
                    "workflow parameter '{}' expects type 'object', but string value must be valid JSON (failed to parse: {})",
                    name, e
                )
            })?;
            if !v.is_object() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'object', but string value parsed to {} (must be a JSON object like '{{\"a\":1}}')",
                    name,
                    describe_value_kind(&v)
                );
            }
            Ok(v)
        }
        "array" => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'array', got empty string",
                    name
                );
            }
            let v: Value = serde_json::from_str(trimmed).map_err(|e| {
                anyhow::anyhow!(
                    "workflow parameter '{}' expects type 'array', but string value must be valid JSON (failed to parse: {})",
                    name, e
                )
            })?;
            if !v.is_array() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'array', but string value parsed to {} (must be a JSON array like '[\"a\",\"b\"]')",
                    name,
                    describe_value_kind(&v)
                );
            }
            Ok(v)
        }
        unknown => {
            anyhow::bail!(
                "workflow parameter '{}' has unknown type '{}'",
                name,
                unknown
            );
        }
    }
}
