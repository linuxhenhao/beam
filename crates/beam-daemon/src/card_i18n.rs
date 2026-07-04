use serde_json::{Value, json};

use crate::prompt;

pub fn card_locales() -> Value {
    json!(["zh_cn", "en_us"])
}

pub fn plain_text(locale: Option<&str>, zh: impl AsRef<str>, en: impl AsRef<str>) -> Value {
    let zh = zh.as_ref();
    let en = en.as_ref();
    json!({
        "tag": "plain_text",
        "content": prompt::is_zh_locale(locale).then_some(zh).unwrap_or(en),
        "i18n_content": {
            "zh_cn": zh,
            "en_us": en,
        }
    })
}

pub fn lark_md(locale: Option<&str>, zh: impl AsRef<str>, en: impl AsRef<str>) -> Value {
    let zh = zh.as_ref();
    let en = en.as_ref();
    json!({
        "tag": "lark_md",
        "content": prompt::is_zh_locale(locale).then_some(zh).unwrap_or(en),
        "i18n_content": {
            "zh_cn": zh,
            "en_us": en,
        }
    })
}
