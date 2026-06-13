use anyhow::{Context, Result};
use serde_json::Value;

pub const WORKFLOW_OUTPUT_BEGIN: &str = "<WORKFLOW_OUTPUT>";
pub const WORKFLOW_OUTPUT_END: &str = "</WORKFLOW_OUTPUT>";

pub fn with_workflow_output_protocol(prompt: &str) -> String {
    if prompt.contains(WORKFLOW_OUTPUT_BEGIN) {
        return prompt.to_string();
    }
    format!(
        "{prompt}\n\n---\nWhen you finish, emit your final structured output between the markers below as a single valid JSON value. Do not include anything else inside the markers.\n\n{begin}\n{{\"...your JSON output...\"}}\n{end}\n",
        prompt = prompt,
        begin = WORKFLOW_OUTPUT_BEGIN,
        end = WORKFLOW_OUTPUT_END
    )
}

pub fn parse_workflow_output(text: &str) -> Result<Value> {
    let last_end = text
        .rfind(WORKFLOW_OUTPUT_END)
        .context("workflow output missing end marker")?;
    let begin_before_end = text[..last_end]
        .rfind(WORKFLOW_OUTPUT_BEGIN)
        .context("workflow output missing begin marker")?;
    let raw = text[begin_before_end + WORKFLOW_OUTPUT_BEGIN.len()..last_end]
        .trim()
        .to_string();
    let sanitized = sanitize_workflow_output_block(&raw);
    let value = serde_json::from_str::<Value>(&sanitized)
        .with_context(|| "workflow output block is not valid JSON")?;
    Ok(value)
}

fn sanitize_workflow_output_block(block: &str) -> String {
    block
        .replace("\u{1b}][", "")
        .replace("\u{1b}[", "")
        .replace('\u{7}', "")
        .replace('\u{1b}', "")
        .replace('\u{0}', "")
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_is_idempotent() {
        let prompt = with_workflow_output_protocol("hello");
        assert!(prompt.contains(WORKFLOW_OUTPUT_BEGIN));
        assert_eq!(prompt, with_workflow_output_protocol(&prompt));
    }

    #[test]
    fn parse_output_extracts_json_between_markers() {
        let parsed = parse_workflow_output(
            "noise\n<WORKFLOW_OUTPUT>\n{\"ok\":true}\n</WORKFLOW_OUTPUT>\nnoise",
        )
        .expect("parse");
        assert_eq!(parsed["ok"], Value::Bool(true));
    }
}
