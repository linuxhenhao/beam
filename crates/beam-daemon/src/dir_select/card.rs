//! Directory selection card JSON rendering.
//!
//! Builds the Feishu interactive card that lets users pick a working
//! directory from buttons or a select_static dropdown.

use std::collections::HashMap;
use std::path::Path;

use serde_json::Value;

use super::{MAX_BUTTON_DIRS, MAX_SELECT_DIRS};
use crate::card_i18n;

// --- Card Building ---

pub(crate) fn is_zh_locale(locale: Option<&str>) -> bool {
    locale
        .map(|value| {
            let normalized = value.to_ascii_lowercase().replace('-', "_");
            normalized == "zh" || normalized.starts_with("zh_")
        })
        .unwrap_or(false)
}

pub(crate) fn card_text<'a>(locale: Option<&str>, zh: &'a str, en: &'a str) -> &'a str {
    if is_zh_locale(locale) { zh } else { en }
}

/// Build the directory selection card JSON string.
///
/// The card has two directory-picking entry points:
/// 1. **Buttons** — primary for recommended/filtered dirs (capped at MAX_BUTTON_DIRS).
/// 2. **select_static dropdown** — "more directories / more matches" fallback
///    (capped at MAX_SELECT_DIRS). Each option value is a JSON string with
///    `{ action, pending_id, working_dir }`, parsed by `parse_lark_card_action`
///    via the `/action/option` fallback in lib.rs.
///
/// Parameters:
/// - pending_id: unique ID for this pending session creation
/// - root_dir: the root working directory (displayed to user)
/// - title: the session title (user message summary)
/// - recommended_dirs: list of recommended directories (relative paths from root)
/// - all_candidates: all candidate directories (for the select_static dropdown)
/// - filter_result: optional filtered subset to show as current results
/// - search_keyword: current search keyword (for restoring input field value)
/// - message: optional info/warning message to display
pub fn build_dir_select_card(
    pending_id: &str,
    root_dir: &str,
    title: &str,
    recommended_dirs: &[String],
    all_candidates: &[String],
    filter_result: Option<&[String]>,
    search_keyword: Option<&str>,
    message: Option<&str>,
    locale: Option<&str>,
) -> String {
    let mut elements: Vec<Value> = Vec::new();

    // Header: root dir display (sanitize: escape backticks to avoid lark_md code tag errors)
    let display_root = sanitize_lark_md(&truncate_str_tail(root_dir, 60));
    elements.push(serde_json::json!({
        "tag": "div",
        "text": {
            "tag": "lark_md",
            "content": format!("📁 **{}:** {}", card_text(locale, "根目录", "Root"), display_root),
            "i18n_content": {
                "zh_cn": format!("📁 **{}:** {}", "根目录", display_root),
                "en_us": format!("📁 **{}:** {}", "Root", display_root),
            }
        }
    }));

    // Message summary (sanitize: escape backticks in user text)
    let display_title = sanitize_lark_md(&truncate_str_head(title, 60));
    elements.push(serde_json::json!({
        "tag": "div",
        "text": {
            "tag": "lark_md",
            "content": format!("💬 **{}:** {}", card_text(locale, "消息", "Message"), display_title),
            "i18n_content": {
                "zh_cn": format!("💬 **{}:** {}", "消息", display_title),
                "en_us": format!("💬 **{}:** {}", "Message", display_title),
            }
        }
    }));

    // Optional message
    if let Some(msg) = message {
        elements.push(serde_json::json!({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": msg,
                "i18n_content": {
                    "zh_cn": msg,
                    "en_us": msg,
                }
            }
        }));
    }

    let is_filtered = filter_result.is_some();

    // --- Determine button dirs and select_static dirs ---
    let (button_dirs, select_dirs) = if let Some(fr) = filter_result {
        // Filtered view: both buttons and select_static draw from filter results.
        let btn: Vec<&String> = fr.iter().take(MAX_BUTTON_DIRS).collect();
        let sel: Vec<&String> = fr.iter().take(MAX_SELECT_DIRS).collect();
        (btn, sel)
    } else {
        // Initial view: buttons from recommended, select_static from all_candidates.
        let btn: Vec<&String> = recommended_dirs.iter().take(MAX_BUTTON_DIRS).collect();
        let sel: Vec<&String> = all_candidates.iter().take(MAX_SELECT_DIRS).collect();
        (btn, sel)
    };

    // --- Section label ---
    let total_count = if let Some(fr) = filter_result {
        fr.len()
    } else {
        select_dirs.len()
    };
    let section_label = if is_filtered {
        if total_count > MAX_BUTTON_DIRS && button_dirs.len() < select_dirs.len() {
            if is_zh_locale(locale) {
                format!(
                    "📋 **当前结果（共 {} 个，按钮显示前 {} 个）：**",
                    total_count, MAX_BUTTON_DIRS
                )
            } else {
                format!(
                    "📋 **Current results ({} total, showing first {} as buttons):**",
                    total_count, MAX_BUTTON_DIRS
                )
            }
        } else {
            if is_zh_locale(locale) {
                format!("📋 **当前结果（{} 个）：**", total_count)
            } else {
                format!("📋 **Current results ({}):**", total_count)
            }
        }
    } else {
        format!(
            "📋 **{}:**",
            card_text(locale, "推荐目录", "Recommended directories")
        )
    };

    // --- Build button labels with smart display ---
    // Recommended section: always short names (even if conflicting).
    // Filtered section: conflict-aware (short name if unique, relative path if duplicate).
    let button_labels = build_dir_labels(
        &button_dirs
            .iter()
            .map(|d| d.to_string())
            .collect::<Vec<String>>(),
        root_dir,
        is_filtered, // detect conflicts only in filtered mode
    );

    // --- Buttons ---
    if !button_dirs.is_empty() {
        elements.push(serde_json::json!({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": section_label,
                "i18n_content": {
                    "zh_cn": if is_filtered {
                        if total_count > MAX_BUTTON_DIRS && button_dirs.len() < select_dirs.len() {
                            format!("📋 **当前结果（共 {} 个，按钮显示前 {} 个）：**", total_count, MAX_BUTTON_DIRS)
                        } else {
                            format!("📋 **当前结果（{} 个）：**", total_count)
                        }
                    } else {
                        "📋 **推荐目录：**".to_string()
                    },
                    "en_us": if is_filtered {
                        if total_count > MAX_BUTTON_DIRS && button_dirs.len() < select_dirs.len() {
                            format!("📋 **Current results ({} total, showing first {} as buttons):**", total_count, MAX_BUTTON_DIRS)
                        } else {
                            format!("📋 **Current results ({}):**", total_count)
                        }
                    } else {
                        "📋 **Recommended directories:**".to_string()
                    },
                }
            }
        }));

        // Each directory gets its own action row with a single button
        // (one button per row avoids the "two buttons per row" issue).
        for (i, dir) in button_dirs.iter().enumerate() {
            let display = &button_labels[i];
            let conflict = display.1;
            let label = &display.0;

            let truncated = if !conflict || dir.as_str() == "." {
                truncate_str(label, 22)
            } else {
                truncate_str_tail(label, 22)
            };

            let pick_value = serde_json::json!({
                "action": "dir_select_pick",
                "pending_id": pending_id,
                "working_dir": dir
            });
            elements.push(serde_json::json!({
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "text": card_i18n::plain_text(locale, truncated.clone(), truncated),
                        "type": if dir.as_str() == "." { "primary" } else { "default" },
                        "value": pick_value
                    }
                ]
            }));
        }
    } else {
        elements.push(serde_json::json!({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": card_text(
                    locale,
                    "⚠️ 没有匹配的目录，请尝试其他关键词。",
                    "⚠️ No matching directories. Try another keyword."
                ),
                "i18n_content": {
                    "zh_cn": "⚠️ 没有匹配的目录，请尝试其他关键词。",
                    "en_us": "⚠️ No matching directories. Try another keyword.",
                }
            }
        }));
    }

    // --- select_static: "more directories / more matches" ---
    if !select_dirs.is_empty() {
        let select_total = if is_filtered {
            if let Some(fr) = filter_result {
                fr.len()
            } else {
                select_dirs.len()
            }
        } else {
            all_candidates.len()
        };
        let select_shown = select_dirs.len();
        let select_label_zh = if is_filtered {
            if select_total > select_shown {
                format!(
                    "📋 **更多匹配（共 {} 个，下拉显示前 {} 个）：**",
                    select_total, select_shown
                )
            } else {
                format!("📋 **{}:**", "下拉选择")
            }
        } else if select_total > select_shown {
            format!(
                "📋 **更多目录（共 {} 个，下拉显示前 {} 个）：**",
                select_total, select_shown
            )
        } else {
            format!("📋 **更多目录（共 {} 个）：**", select_total)
        };
        let select_label_en = if is_filtered {
            if select_total > select_shown {
                format!(
                    "📋 **More matches ({} total, showing first {} in dropdown):**",
                    select_total, select_shown
                )
            } else {
                format!("📋 **{}:**", "Dropdown selection")
            }
        } else if select_total > select_shown {
            format!(
                "📋 **More directories ({} total, showing first {} in dropdown):**",
                select_total, select_shown
            )
        } else {
            format!("📋 **More directories ({} total):**", select_total)
        };
        elements.push(serde_json::json!({
            "tag": "div",
            "text": card_i18n::lark_md(locale, select_label_zh, select_label_en)
        }));

        let select_labels = build_dir_labels(
            &select_dirs
                .iter()
                .map(|d| d.to_string())
                .collect::<Vec<String>>(),
            root_dir,
            is_filtered, // detect conflicts only in filtered mode
        );

        let mut options: Vec<Value> = Vec::new();
        for (i, dir) in select_dirs.iter().enumerate() {
            let display = &select_labels[i];
            let label = &display.0;

            // Option value is a JSON string containing action/pending_id/working_dir.
            // Parsed by try_parse_select_option → parse_lark_card_action in lib.rs.
            let option_value = serde_json::json!({
                "action": "dir_select_pick",
                "pending_id": pending_id,
                "working_dir": dir,
            });
            let option_value_str = serde_json::to_string(&option_value).unwrap_or_default();

            options.push(serde_json::json!({
                "text": card_i18n::plain_text(locale, label.clone(), label),
                "value": option_value_str
            }));
        }

        // Wrap select_static in an action module so it actually renders and
        // dispatches events. A bare select_static as a top-level card element
        // is not valid in the Feishu card schema and will be silently ignored.
        elements.push(serde_json::json!({
            "tag": "action",
            "actions": [
                {
                "tag": "select_static",
                "placeholder": {
                    "tag": "plain_text",
                    "content": card_text(locale, "请选择目录...", "Select a directory..."),
                    "i18n_content": {
                        "zh_cn": "请选择目录...",
                        "en_us": "Select a directory...",
                    }
                },
                "options": options
            }
            ]
        }));
    }

    // Separator before search section
    elements.push(serde_json::json!({ "tag": "hr" }));

    // Search hint: must be a standalone div outside the form.
    // Feishu card forms only accept input + button; div is not allowed inside form.
    // Use plain instructional text (not a bold title) so users don't mistake
    // the label for a clickable element.
    elements.push(serde_json::json!({
        "tag": "div",
        "text": {
            "tag": "lark_md",
            "content": card_text(
                locale,
                "🔍 在下方输入关键词，点击「筛选」过滤目录，或点击「使用最优匹配启动」自动选择最佳目录",
                "🔍 Enter a keyword below, click \"Filter\" to narrow directories, or click \"Start with best match\" to choose automatically"
            ),
            "i18n_content": {
                "zh_cn": "🔍 在下方输入关键词，点击「筛选」过滤目录，或点击「使用最优匹配启动」自动选择最佳目录",
                "en_us": "🔍 Enter a keyword below, click \"Filter\" to narrow directories, or click \"Start with best match\" to choose automatically",
            }
        }
    }));

    // Form container: input + two form_submit buttons.
    // Must be a single "tag": "form" so that the input value is submitted
    // as /action/form_value/dir_search_keyword when either button is clicked.
    let mut form_elements: Vec<Value> = Vec::new();

    form_elements.push(serde_json::json!({
        "tag": "input",
        "name": "dir_search_keyword",
        "placeholder": {
            "tag": "plain_text",
            "content": card_text(locale, "输入关键词筛选目录...", "Type a keyword to filter directories..."),
            "i18n_content": {
                "zh_cn": "输入关键词筛选目录...",
                "en_us": "Type a keyword to filter directories...",
            }
        },
        "default_value": search_keyword.unwrap_or("")
    }));

    form_elements.push(serde_json::json!({
        "tag": "button",
        "text": {
            "tag": "plain_text",
            "content": card_text(locale, "🔍 筛选", "🔍 Filter"),
            "i18n_content": {
                "zh_cn": "🔍 筛选",
                "en_us": "🔍 Filter",
            }
        },
        "type": "primary",
        "action_type": "form_submit",
        "name": "dir_select_filter_btn",
        "value": {
            "action": "dir_select_filter",
            "pending_id": pending_id
        }
    }));

    form_elements.push(serde_json::json!({
        "tag": "button",
        "text": {
            "tag": "plain_text",
            "content": card_text(locale, "🚀 使用最优匹配启动", "🚀 Start with best match"),
            "i18n_content": {
                "zh_cn": "🚀 使用最优匹配启动",
                "en_us": "🚀 Start with best match",
            }
        },
        "type": "default",
        "action_type": "form_submit",
        "name": "dir_select_best_btn",
        "value": {
            "action": "dir_select_best",
            "pending_id": pending_id
        }
    }));

    elements.push(serde_json::json!({
        "tag": "form",
        "name": "dir_search_form",
        "elements": form_elements
    }));

    // Build card with config
    let card = serde_json::json!({
        "config": {
            "wide_screen_mode": true
        },
        "header": {
            "title": {
                "tag": "plain_text",
                "content": card_text(locale, "请选择工作目录", "Choose a working directory"),
                "i18n_content": {
                    "zh_cn": "请选择工作目录",
                    "en_us": "Choose a working directory",
                }
            },
            "template": "blue"
        },
        "elements": elements
    });

    serde_json::to_string(&card).unwrap_or_default()
}

/// Build a simple "session starting" card to replace the dir select card.
pub fn build_dir_session_starting_card(
    working_dir: &str,
    _title: &str,
    locale: Option<&str>,
) -> String {
    let selected_dir = sanitize_lark_md(working_dir);
    let body_zh = format!("✅ **已选择工作目录**\n\n{}", selected_dir);
    let body_en = format!("✅ **Selected working directory**\n\n{}", selected_dir);
    let card = serde_json::json!({
        "config": {
            "wide_screen_mode": true
        },
        "header": {
            "title": {
                "tag": "plain_text",
                "content": card_text(locale, "工作目录已选择", "Working directory selected"),
                "i18n_content": {
                    "zh_cn": "工作目录已选择",
                    "en_us": "Working directory selected",
                }
            },
            "template": "blue"
        },
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": body_en,
                    "i18n_content": {
                        "zh_cn": body_zh,
                        "en_us": body_en,
                    }
                }
            }
        ]
    });
    serde_json::to_string(&card).unwrap_or_default()
}

// --- Helpers ---

/// Escape backticks in lark_md content to prevent Feishu from interpreting them
/// as code tags (which triggers "unsupported html tag code" errors).
pub(crate) fn sanitize_lark_md(s: &str) -> String {
    s.replace('`', "'")
}

fn root_dir_basename(root_dir: &str) -> String {
    Path::new(root_dir)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(root_dir)
        .to_string()
}

fn dir_display_name(rel_path: &str) -> String {
    if rel_path == "." {
        return ".".to_string();
    }
    // Show the last component
    Path::new(rel_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(rel_path)
        .to_string()
}

/// Build display labels for a list of directories.
///
/// Returns a vector of `(label, is_conflict)` tuples.
///
/// - `detect_conflicts`: when true, short-name conflicts (multiple dirs sharing
///   the same base name) are resolved by showing the full relative path for
///   conflicting entries. When false, short names are always used.
/// - The root directory "." is always shown as the root basename with a 📁 prefix.
/// - Labels are NOT truncated here; callers apply truncation as needed.
fn build_dir_labels(
    dirs: &[String],
    root_dir: &str,
    detect_conflicts: bool,
) -> Vec<(String, bool)> {
    if dirs.is_empty() {
        return Vec::new();
    }

    let short_names: Vec<String> = dirs
        .iter()
        .map(|dir| {
            if dir == "." {
                root_dir_basename(root_dir)
            } else {
                dir_display_name(dir)
            }
        })
        .collect();

    let conflict_map: HashMap<String, usize> = if detect_conflicts {
        let mut map: HashMap<String, usize> = HashMap::new();
        for sn in &short_names {
            *map.entry(sn.clone()).or_insert(0) += 1;
        }
        map
    } else {
        HashMap::new()
    };

    dirs.iter()
        .enumerate()
        .map(|(i, dir)| {
            let short = &short_names[i];
            let conflict = detect_conflicts && conflict_map.get(short).copied().unwrap_or(1) > 1;

            let display = if dir == "." {
                format!("📁 {}", root_dir_basename(root_dir))
            } else if conflict {
                format!("📁 {}", dir)
            } else {
                format!("📁 {}", short)
            };

            (display, conflict)
        })
        .collect()
}

/// Char-safe truncation: truncate from right, append "..".
pub(crate) fn truncate_str(s: &str, max_len: usize) -> String {
    if s.chars().count() <= max_len {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max_len.saturating_sub(2)).collect();
        format!("{}..", truncated)
    }
}

/// Char-safe truncation: keep the first `max_chars` characters.
pub(crate) fn truncate_str_head(s: &str, max_chars: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max_chars {
        s.to_string()
    } else {
        let truncated: String = chars
            .into_iter()
            .take(max_chars.saturating_sub(1))
            .collect();
        format!("{}…", truncated)
    }
}

/// Char-safe truncation: keep the last `max_chars` characters, prefix with "…".
pub(crate) fn truncate_str_tail(s: &str, max_chars: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max_chars {
        s.to_string()
    } else {
        let truncated: String = chars
            .into_iter()
            .rev()
            .take(max_chars.saturating_sub(1))
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        format!("…{}", truncated)
    }
}

#[cfg(test)]
#[path = "tests/card.rs"]
mod tests;
