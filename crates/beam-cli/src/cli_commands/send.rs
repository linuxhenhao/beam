use crate::*;
use anyhow::{Context, Result, bail};

pub(crate) fn parse_mention(raw: &str) -> Result<MentionTarget> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("--mention value must not be empty");
    }
    if let Some((open_id, name)) = trimmed.split_once(':') {
        let open_id = open_id.trim();
        let name = name.trim();
        if open_id.is_empty() {
            bail!("--mention open_id must not be empty in \"{}\"", trimmed);
        }
        Ok(MentionTarget {
            open_id: open_id.to_string(),
            name: if name.is_empty() {
                None
            } else {
                Some(name.to_string())
            },
        })
    } else {
        Ok(MentionTarget {
            open_id: trimmed.to_string(),
            name: None,
        })
    }
}

/// Build a FinalOutputRequest from parsed CLI args, validating conflicts.
pub(crate) fn build_send_request(args: SendArgs) -> Result<FinalOutputRequest> {
    // --- validate mention policy conflicts ---
    let has_explicit_mentions = !args.mention.is_empty();
    let mention_count = [has_explicit_mentions, args.mention_back, args.no_mention]
        .iter()
        .filter(|&&v| v)
        .count();
    if mention_count > 1 {
        bail!(
            "incompatible mention flags: --no-mention cannot be combined with --mention or --mention-back. \
             Choose exactly one mention policy."
        );
    }
    if !has_explicit_mentions && !args.mention_back && !args.no_mention {
        bail!(
            "no mention decision: you must choose exactly one of --mention-back, \
             --mention <open_id[:name]>, or --no-mention for every beam send. \
             The daemon will refuse messages without an explicit mention policy."
        );
    }

    // --- validate --attention kind ---
    if let Some(ref kind) = args.attention {
        let allowed = ["authz", "decision", "blocked", "help"];
        if !allowed.contains(&kind.as_str()) {
            bail!(
                "invalid attention kind \"{}\": must be one of {}",
                kind,
                allowed.join("|")
            );
        }
    }

    // --- build mentions ---
    let mentions: Vec<MentionTarget> = args
        .mention
        .iter()
        .map(|raw| parse_mention(raw))
        .collect::<Result<Vec<_>>>()?;

    // --- read content ---
    let content = read_send_content_v2(args.content, args.content_file.as_deref())?;

    // --- validate --attention usage constraints (botmux parity: attentionUsageError) ---
    if args.attention.is_some() {
        if args.top_level {
            bail!(
                "--attention cannot be combined with --top-level. Attention is for the current session context only."
            );
        }
        if args.chat_id.is_some() {
            bail!(
                "--attention cannot be combined with --chat-id. Attention is for the current session context only."
            );
        }
        if args.into.is_some() {
            bail!(
                "--attention cannot be combined with --into. Attention is for the current session context only."
            );
        }
        if args.voice {
            bail!(
                "--attention cannot be combined with --voice. Attention requires a text/card message."
            );
        }
        if content.trim().is_empty() {
            bail!("--attention requires a non-empty text reason in the message body.");
        }
    }

    Ok(FinalOutputRequest {
        content,
        mentions,
        mention_back: args.mention_back,
        no_mention: args.no_mention,
        files: args.files,
        images: args.images,
        top_level: args.top_level,
        chat_id: args.chat_id,
        into: args.into,
        quote: args.quote,
        no_quote: args.no_quote,
        voice: args.voice,
        attention: args.attention,
        card: args.card,
        text: args.text,
        anyway: args.anyway,
    })
}

pub(crate) fn read_send_content_v2(
    content_arg: Option<String>,
    content_file: Option<&Path>,
) -> Result<String> {
    let mut parts: Vec<String> = Vec::new();

    if let Some(ref file_path) = content_file {
        let file_content = std::fs::read_to_string(file_path)
            .with_context(|| format!("failed to read content file: {}", file_path.display()))?;
        let trimmed = file_content.trim_end().to_string();
        if !trimmed.is_empty() {
            parts.push(trimmed);
        }
    }

    if let Some(content) = content_arg {
        let trimmed = content.trim().to_string();
        if !trimmed.is_empty() {
            parts.push(trimmed);
        }
    }

    if !parts.is_empty() {
        return Ok(parts.join("\n"));
    }

    // Neither --content-file nor positional: read from stdin.
    let mut stdin_body = String::new();
    use std::io::Read;
    std::io::stdin().read_to_string(&mut stdin_body)?;
    let stdin_body = stdin_body.trim_end().to_string();
    if stdin_body.is_empty() {
        bail!("send content is empty (provide positional arg, --content-file, or stdin)");
    }
    Ok(stdin_body)
}
