use serde_json::{Value, json};

use crate::prompt;

pub fn plain_text(locale: Option<&str>, zh: impl AsRef<str>, en: impl AsRef<str>) -> Value {
    let zh = zh.as_ref();
    let en = en.as_ref();
    json!({
        "tag": "plain_text",
        "content": if prompt::is_zh_locale(locale) { zh } else { en },
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
        "content": if prompt::is_zh_locale(locale) { zh } else { en },
        "i18n_content": {
            "zh_cn": zh,
            "en_us": en,
        }
    })
}
