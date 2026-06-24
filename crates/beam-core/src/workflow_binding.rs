use std::fs;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

use anyhow::{Context, Result};
use serde_json::Value;

use crate::{RunSnapshotDTO, WorkflowDefinition, WorkflowOutputRef};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingError(pub String);

impl std::fmt::Display for BindingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BindingError {}

#[derive(Debug, Clone, PartialEq)]
pub struct BindingContext<'a> {
    pub snapshot: &'a RunSnapshotDTO,
    pub def: &'a WorkflowDefinition,
    pub run_dir: &'a Path,
    pub loop_context: Option<LoopContext<'a>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LoopContext<'a> {
    pub loop_id: &'a str,
    pub iteration: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParsedRef<'a> {
    Output {
        node_id: &'a str,
        path: Vec<&'a str>,
    },
    Params {
        path: Vec<&'a str>,
    },
    Previous {
        node_id: &'a str,
        path: Vec<&'a str>,
    },
}

const REF_MARKER: &str = ".output.";
const PREVIOUS_MARKER: &str = ".previous.";
const PARAMS_PREFIX: &str = "params.";
const FORBIDDEN_SEGMENTS: [&str; 3] = ["__proto__", "prototype", "constructor"];

pub fn resolve_bindings<'a>(
    value: &'a Value,
    ctx: &'a BindingContext<'a>,
) -> Pin<Box<dyn Future<Output = Result<Value>> + Send + 'a>> {
    Box::pin(async move {
        if let Some(ref_spec) = output_ref_spec(value) {
            return resolve_output_ref(ref_spec, ctx).await;
        }
        if let Some(s) = value.as_str() {
            return resolve_interpolated_string(s, ctx).await.map(Value::String);
        }
        if let Some(arr) = value.as_array() {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                out.push(resolve_bindings(item, ctx).await?);
            }
            return Ok(Value::Array(out));
        }
        if let Some(obj) = value.as_object() {
            let mut out = serde_json::Map::new();
            for (k, v) in obj {
                out.insert(k.clone(), resolve_bindings(v, ctx).await?);
            }
            return Ok(Value::Object(out));
        }
        Ok(value.clone())
    })
}

pub fn resolve_bound_string<'a>(
    value: &'a Value,
    ctx: &'a BindingContext<'a>,
) -> Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>> {
    Box::pin(async move {
        let resolved = resolve_bindings(value, ctx).await?;
        match resolved {
            Value::String(s) => Ok(s),
            other => anyhow::bail!(
                "bound string field resolved to {}",
                describe_json_kind(&other)
            ),
        }
    })
}

fn output_ref_spec(value: &Value) -> Option<&str> {
    let obj = value.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    obj.get("$ref")?.as_str()
}

fn parse_ref<'a>(ref_spec: &'a str) -> Result<ParsedRef<'a>> {
    if let Some(rest) = ref_spec.strip_prefix(PARAMS_PREFIX) {
        return Ok(ParsedRef::Params {
            path: parse_segments(rest, ref_spec)?,
        });
    }
    if let Some(idx) = ref_spec.find(PREVIOUS_MARKER) {
        let node_id = &ref_spec[..idx];
        if node_id.is_empty() {
            anyhow::bail!("$ref '{}' has empty nodeId before '.previous.'", ref_spec);
        }
        return Ok(ParsedRef::Previous {
            node_id,
            path: parse_segments(&ref_spec[idx + PREVIOUS_MARKER.len()..], ref_spec)?,
        });
    }
    let Some(idx) = ref_spec.find(REF_MARKER) else {
        anyhow::bail!(
            "$ref '{}' missing '.output.' separator (expected '<nodeId>.output.<path>', '<nodeId>.previous.<path>', or 'params.<path>')",
            ref_spec
        );
    };
    let node_id = &ref_spec[..idx];
    if node_id.is_empty() {
        anyhow::bail!("$ref '{}' has empty nodeId before '.output.'", ref_spec);
    }
    Ok(ParsedRef::Output {
        node_id,
        path: parse_segments(&ref_spec[idx + REF_MARKER.len()..], ref_spec)?,
    })
}

fn parse_segments<'a>(raw_path: &'a str, ref_spec: &str) -> Result<Vec<&'a str>> {
    if raw_path.is_empty() {
        anyhow::bail!("$ref '{}' has empty path", ref_spec);
    }
    let mut out = Vec::new();
    for seg in raw_path.split('.') {
        if seg.is_empty() {
            anyhow::bail!("$ref '{}' has empty path segment", ref_spec);
        }
        if FORBIDDEN_SEGMENTS.contains(&seg) {
            anyhow::bail!("$ref '{}' uses forbidden segment '{}'", ref_spec, seg);
        }
        if !seg
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-'))
        {
            anyhow::bail!(
                "$ref '{}' has invalid segment '{}' (must match [A-Za-z0-9_-]+)",
                ref_spec,
                seg
            );
        }
        out.push(seg);
    }
    Ok(out)
}

async fn resolve_output_ref(ref_spec: &str, ctx: &BindingContext<'_>) -> Result<Value> {
    let parsed = parse_ref(ref_spec)?;
    let (output_ref, path, node_id) = match parsed {
        ParsedRef::Params { path } => {
            let params = load_run_params(ref_spec, ctx).await?;
            return walk_path(params, &path, ref_spec);
        }
        ParsedRef::Output { node_id, path } => (latest_output_ref(node_id, ctx)?, path, node_id),
        ParsedRef::Previous { node_id, path } => {
            let loop_ctx = ctx.loop_context.context(format!(
                "$ref '{}' uses '.previous.' outside a loop iteration context",
                ref_spec
            ))?;
            if loop_ctx.iteration <= 1 {
                // First iteration has no previous.  For Decision nodes
                // produce an empty synthetic JSON so that string
                // interpolation (e.g. ${reviewDecision.previous.comment})
                // yields an empty string without changing the global
                // null → "null" mapping.
                if let Some(crate::WorkflowNode::Decision(_)) = ctx.def.nodes.get(node_id) {
                    let empty = serde_json::json!({"by": null, "comment": ""});
                    return walk_path(empty, &path, ref_spec);
                }
                return Ok(Value::Null);
            }
            let prev_iteration = loop_ctx.iteration - 1;

            // For Decision nodes, read decision metadata from the loop
            // iteration state rather than from activity outputs.  This
            // supports `${reviewDecision.previous.comment}` even when
            // the previous iteration was rejected (activityFailed, no
            // output blob).
            if let Some(crate::WorkflowNode::Decision(_)) = ctx.def.nodes.get(node_id) {
                if let Some(iter_state) = ctx
                    .snapshot
                    .loops
                    .as_ref()
                    .and_then(|loops| loops.get(loop_ctx.loop_id))
                    .and_then(|ls| {
                        ls.iterations
                            .iter()
                            .find(|it| it.iteration == prev_iteration)
                    })
                {
                    let decision_data = serde_json::json!({
                        "by": iter_state.decision_by,
                        "comment": iter_state.decision_comment,
                    });
                    return walk_path(decision_data, &path, ref_spec);
                }
                // No previous iteration data yet — for Decision
                // nodes return empty synthetic JSON.
                let empty = serde_json::json!({"by": null, "comment": ""});
                return walk_path(empty, &path, ref_spec);
            }

            (
                previous_loop_output_ref(node_id, ctx, loop_ctx.loop_id, prev_iteration)?
                    .ok_or_else(|| anyhow::anyhow!(
                        "$ref '{}' references node '{}' which has not produced a successful output yet",
                        ref_spec, node_id
                    ))?,
                path,
                node_id,
            )
        }
    };

    let blob = fs::read_to_string(&output_ref.output_path).with_context(|| {
        format!(
            "$ref '{}' failed to read output blob at {}",
            ref_spec, output_ref.output_path
        )
    })?;
    let raw: Value = serde_json::from_str(&blob)
        .with_context(|| format!("$ref '{}' output blob is not valid JSON", ref_spec))?;
    let logical_root = match ctx.def.nodes.get(node_id) {
        Some(crate::WorkflowNode::HostExecutor(_)) => {
            raw.get("output").cloned().unwrap_or(Value::Null)
        }
        _ => raw,
    };
    walk_path(logical_root, &path, ref_spec)
}

fn latest_output_ref<'a>(node_id: &'a str, ctx: &BindingContext<'a>) -> Result<WorkflowOutputRef> {
    let run_id = &ctx.snapshot.run.run_id;
    let plain = ctx
        .snapshot
        .outputs
        .get(&format!("{}::work::{}", run_id, node_id));
    if let Some(out) = plain {
        return Ok(out.clone());
    }
    find_loop_output_ref(node_id, ctx, None, None)
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "$ref targets node '{}' which has not produced a successful output yet",
                node_id
            )
        })
}

fn previous_loop_output_ref<'a>(
    node_id: &'a str,
    ctx: &BindingContext<'a>,
    loop_id: &'a str,
    iteration: u64,
) -> Result<Option<WorkflowOutputRef>> {
    Ok(find_loop_output_ref(node_id, ctx, Some(loop_id), Some(iteration)).cloned())
}

fn find_loop_output_ref<'a>(
    node_id: &'a str,
    ctx: &'a BindingContext<'a>,
    loop_id: Option<&'a str>,
    iteration: Option<u64>,
) -> Option<&'a WorkflowOutputRef> {
    let node_def = ctx.def.nodes.get(node_id)?;
    let expected_kind = match node_def {
        crate::WorkflowNode::Decision(_) => "gate",
        _ => "work",
    };
    let mut best: Option<(u64, &WorkflowOutputRef)> = None;
    for (activity_id, output_ref) in &ctx.snapshot.outputs {
        let Some(parsed) = parse_activity_id(activity_id) else {
            continue;
        };
        if parsed.node_id != node_id || parsed.activity_kind != expected_kind {
            continue;
        }
        if let Some(loop_id) = loop_id {
            if parsed.loop_id.as_deref() != Some(loop_id) {
                continue;
            }
        }
        if let Some(iteration) = iteration {
            if parsed.iteration != Some(iteration) {
                continue;
            }
        }
        let iter = parsed.iteration.unwrap_or(0);
        if best.map(|(prev, _)| iter > prev).unwrap_or(true) {
            best = Some((iter, output_ref));
        }
    }
    best.map(|(_, output_ref)| output_ref)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedActivityId<'a> {
    node_id: &'a str,
    activity_kind: &'a str,
    loop_id: Option<&'a str>,
    iteration: Option<u64>,
}

fn parse_activity_id<'a>(s: &'a str) -> Option<ParsedActivityId<'a>> {
    if let Some(loop_idx) = s.find("::loop::") {
        let run_id_end = loop_idx;
        let after_loop = &s[loop_idx + "::loop::".len()..];
        let iter_end = after_loop.find("::")?;
        let loop_part = &after_loop[..iter_end];
        let (loop_id, iter) = loop_part.rsplit_once('.')?;
        let iteration = iter.parse().ok()?;
        let after_iter = &after_loop[iter_end + 2..];
        let mut segs = after_iter.splitn(2, "::");
        let activity_kind = segs.next()?;
        let node_id = segs.next()?;
        let _ = run_id_end; // keep the parser symmetric with TS, run id is not needed here.
        return Some(ParsedActivityId {
            node_id,
            activity_kind,
            loop_id: Some(loop_id),
            iteration: Some(iteration),
        });
    }
    let mut parts = s.rsplitn(3, "::");
    let node_id = parts.next()?;
    let activity_kind = parts.next()?;
    let _run_id = parts.next()?;
    Some(ParsedActivityId {
        node_id,
        activity_kind,
        loop_id: None,
        iteration: None,
    })
}

async fn load_run_params(ref_spec: &str, ctx: &BindingContext<'_>) -> Result<Value> {
    let input_path = ctx
        .snapshot
        .run
        .input
        .as_ref()
        .context(format!("$ref '{}' requires run input", ref_spec))?
        .output_path
        .clone();
    let raw = fs::read_to_string(&input_path).with_context(|| {
        format!(
            "$ref '{}' failed to read run params at {}",
            ref_spec, input_path
        )
    })?;
    let parsed: Value = serde_json::from_str(&raw)
        .with_context(|| format!("$ref '{}' run params blob is not valid JSON", ref_spec))?;
    if !parsed.is_object() {
        anyhow::bail!(
            "$ref '{}' resolved run params to non-object input",
            ref_spec
        );
    }
    Ok(parsed)
}

fn walk_path(value: Value, segments: &[&str], ref_spec: &str) -> Result<Value> {
    let mut cursor = value;
    for seg in segments {
        if cursor.is_null() {
            anyhow::bail!("$ref '{}' hit null at '{}'", ref_spec, seg);
        }
        if let Some(arr) = cursor.as_array() {
            let idx: usize = seg
                .parse()
                .with_context(|| format!("$ref '{}' array index '{}' invalid", ref_spec, seg))?;
            cursor = arr.get(idx).cloned().with_context(|| {
                format!("$ref '{}' array index '{}' out of bounds", ref_spec, seg)
            })?;
            continue;
        }
        let obj = cursor
            .as_object()
            .with_context(|| format!("$ref '{}' segment '{}' not found", ref_spec, seg))?;
        cursor = obj
            .get(*seg)
            .cloned()
            .with_context(|| format!("$ref '{}' segment '{}' not found", ref_spec, seg))?;
    }
    Ok(cursor)
}

async fn resolve_interpolated_string(value: &str, ctx: &BindingContext<'_>) -> Result<String> {
    if !value.contains("${") {
        return Ok(value.to_string());
    }
    let mut out = String::new();
    let mut cursor = 0usize;
    while let Some(start_rel) = value[cursor..].find("${") {
        let start = cursor + start_rel;
        out.push_str(&value[cursor..start]);
        let end_rel = value[start + 2..].find('}').context(format!(
            "unterminated string ref interpolation in '{}'",
            value
        ))?;
        let end = start + 2 + end_rel;
        let ref_spec = &value[start + 2..end];
        if ref_spec.is_empty() {
            anyhow::bail!("empty string ref interpolation in '{}'", value);
        }
        let resolved = resolve_output_ref(ref_spec, ctx).await?;
        out.push_str(&stringify_interpolated_value(ref_spec, resolved)?);
        cursor = end + 1;
    }
    out.push_str(&value[cursor..]);
    Ok(out)
}

fn stringify_interpolated_value(ref_spec: &str, value: Value) -> Result<String> {
    match value {
        Value::Null => Ok("null".to_string()),
        Value::String(s) => Ok(s),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        other => anyhow::bail!(
            "string interpolation '${{{}}}' resolved to {}",
            ref_spec,
            describe_json_kind(&other)
        ),
    }
}

fn describe_json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow_definition::NodeBase;
    use crate::workflow_snapshot::NodeStatus;
    use crate::{RunState, RunStatus, WorkflowNode, WorkflowOutputRef};
    use std::collections::BTreeMap;

    fn ctx<'a>(
        run_dir: &'a Path,
        snapshot: &'a RunSnapshotDTO,
        def: &'a WorkflowDefinition,
    ) -> BindingContext<'a> {
        BindingContext {
            snapshot,
            def,
            run_dir,
            loop_context: None,
        }
    }

    #[tokio::test]
    async fn resolve_bound_string_handles_params_and_output_refs() {
        let temp = std::env::temp_dir().join(format!("beam-binding-{}", std::process::id()));
        let _ = fs::remove_dir_all(&temp);
        fs::create_dir_all(&temp).unwrap();
        fs::write(temp.join("params.json"), r#"{"name":"beam"}"#).unwrap();
        let snapshot = RunSnapshotDTO {
            run_id: "run-1".to_string(),
            run: RunState {
                run_id: "run-1".to_string(),
                status: RunStatus::Running,
                workflow_id: Some("flow-a".to_string()),
                revision_id: Some("rev-a".to_string()),
                initiator: None,
                input: Some(WorkflowOutputRef {
                    output_hash: "sha256:params".to_string(),
                    output_path: temp.join("params.json").display().to_string(),
                    output_bytes: 17,
                    output_schema_version: 1,
                    content_type: Some("application/json".to_string()),
                }),
                output: None,
                failed_node_id: None,
                root_cause_event_id: None,
                cancel_origin_event_id: None,
                bot_snapshots: None,
                cancelled_run_intent: None,
                cancelled_node_intents: BTreeMap::new(),
            },
            last_seq: 1,
            nodes: vec![crate::NodeState {
                node_id: "a".to_string(),
                status: NodeStatus::Succeeded,
                activity_id: Some("run-1::work::a".to_string()),
                retry_count: 0,
                next_attempt_at: None,
                error_class: None,
                condition_event_id: None,
                cancel_origin_event_id: None,
            }],
            activities: Vec::new(),
            loops: None,
            dangling: crate::DanglingSnapshot {
                activities: Vec::new(),
                effect_attempted: Vec::new(),
                waits: Vec::new(),
                wait_resolutions: Vec::new(),
                cancels: Vec::new(),
            },
            outputs: BTreeMap::from([(
                "run-1::work::a".to_string(),
                WorkflowOutputRef {
                    output_hash: "sha256:out".to_string(),
                    output_path: temp.join("out.json").display().to_string(),
                    output_bytes: 15,
                    output_schema_version: 1,
                    content_type: Some("application/json".to_string()),
                },
            )]),
            attempt_io: BTreeMap::new(),
            chat_binding: None,
            updated_at: 1,
        };
        fs::write(temp.join("out.json"), r#"{"message":"ok"}"#).unwrap();
        let def = WorkflowDefinition {
            workflow_id: "flow-a".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([(
                "a".to_string(),
                WorkflowNode::Subagent(crate::SubagentNode {
                    base: NodeBase {
                        description: None,
                        depends: None,
                        human_gate: None,
                        retry_policy: None,
                        timeout_ms: None,
                        max_output_bytes: None,
                        output_schema: None,
                        unsafe_allow_ungated: None,
                    },
                    bot: "bot-a".to_string(),
                    prompt: Value::String("hello ${params.name} ${a.output.message}".to_string()),
                    working_dir: None,
                    model_overrides: None,
                    tool_policy: None,
                }),
            )]),
        };
        let resolved = resolve_bindings(
            &Value::String("hello ${params.name} ${a.output.message}".to_string()),
            &ctx(&temp, &snapshot, &def),
        )
        .await
        .unwrap();
        assert_eq!(resolved.as_str(), Some("hello beam ok"));
        let _ = fs::remove_dir_all(&temp);
    }

    /// Null in string interpolation remains the literal "null".
    #[tokio::test]
    async fn string_interpolation_null_produces_literal_null() {
        let temp = std::env::temp_dir().join(format!("beam-binding-null-{}", std::process::id()));
        let _ = fs::remove_dir_all(&temp);
        fs::create_dir_all(&temp).unwrap();

        let snapshot = RunSnapshotDTO {
            run_id: "run-null".to_string(),
            run: RunState {
                run_id: "run-null".to_string(),
                status: RunStatus::Running,
                workflow_id: None,
                revision_id: None,
                initiator: None,
                input: None,
                output: None,
                failed_node_id: None,
                root_cause_event_id: None,
                cancel_origin_event_id: None,
                bot_snapshots: None,
                cancelled_run_intent: None,
                cancelled_node_intents: BTreeMap::new(),
            },
            last_seq: 0,
            nodes: vec![crate::NodeState {
                node_id: "a".to_string(),
                status: NodeStatus::Succeeded,
                activity_id: Some("run-null::work::a".to_string()),
                retry_count: 0,
                next_attempt_at: None,
                error_class: None,
                condition_event_id: None,
                cancel_origin_event_id: None,
            }],
            activities: Vec::new(),
            loops: None,
            dangling: crate::DanglingSnapshot {
                activities: Vec::new(),
                effect_attempted: Vec::new(),
                waits: Vec::new(),
                wait_resolutions: Vec::new(),
                cancels: Vec::new(),
            },
            outputs: BTreeMap::from([(
                "run-null::work::a".to_string(),
                WorkflowOutputRef {
                    output_hash: "sha256:null-out".to_string(),
                    output_path: temp.join("null-out.json").display().to_string(),
                    output_bytes: 16,
                    output_schema_version: 1,
                    content_type: Some("application/json".to_string()),
                },
            )]),
            attempt_io: BTreeMap::new(),
            chat_binding: None,
            updated_at: 1,
        };
        // Output blob: { "val": null }
        fs::write(temp.join("null-out.json"), r#"{"val":null}"#).unwrap();

        let def = WorkflowDefinition {
            workflow_id: "flow-null".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([(
                "a".to_string(),
                WorkflowNode::Subagent(crate::SubagentNode {
                    base: NodeBase {
                        description: None,
                        depends: None,
                        human_gate: None,
                        retry_policy: None,
                        timeout_ms: None,
                        max_output_bytes: None,
                        output_schema: None,
                        unsafe_allow_ungated: None,
                    },
                    bot: "bot-a".to_string(),
                    prompt: Value::String("x".to_string()),
                    working_dir: None,
                    model_overrides: None,
                    tool_policy: None,
                }),
            )]),
        };

        // ${a.output.val} — the val field is null.
        let resolved = resolve_bindings(
            &Value::String("before ${a.output.val} after".to_string()),
            &ctx(&temp, &snapshot, &def),
        )
        .await
        .unwrap();
        assert_eq!(
            resolved.as_str(),
            Some("before null after"),
            "null interpolation should produce literal 'null'"
        );
        let _ = fs::remove_dir_all(&temp);
    }
}
