use std::env;
use std::fs;
use std::path::PathBuf;

use beam_core::i18n::Locale;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum VoiceEngine {
    Sami,
    Openai,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct VoiceSamiCreds {
    #[serde(default)]
    pub access_key: Option<String>,
    #[serde(default)]
    pub secret_key: Option<String>,
    #[serde(default)]
    pub appkey: Option<String>,
    #[serde(default)]
    pub token_url: Option<String>,
    #[serde(default)]
    pub ws_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct VoiceOpenAIConfig {
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct VoiceConfig {
    #[serde(default)]
    pub engine: Option<VoiceEngine>,
    #[serde(default)]
    pub speaker: Option<String>,
    #[serde(default)]
    pub rate: Option<f64>,
    #[serde(default)]
    pub sami: Option<VoiceSamiCreds>,
    #[serde(default)]
    pub openai: Option<VoiceOpenAIConfig>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct GlobalConfig {
    pub lang: Option<Locale>,
    pub voice: Option<VoiceConfig>,
}

fn home_dir() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

pub fn global_config_path() -> PathBuf {
    home_dir().join(".beam").join("config.json")
}

fn read_raw_config() -> Map<String, Value> {
    let path = global_config_path();
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Map::new(),
        Err(_) => return Map::new(),
    };
    match serde_json::from_str::<Value>(&raw) {
        Ok(Value::Object(map)) => map,
        Ok(_) => Map::new(),
        Err(err) => {
            eprintln!("[beam] failed to parse {}: {}", path.display(), err);
            Map::new()
        }
    }
}

fn write_raw_config(current: Map<String, Value>) -> Result<(), std::io::Error> {
    let path = global_config_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut body = serde_json::to_vec_pretty(&current).unwrap_or_else(|_| b"{}".to_vec());
    body.push(b'\n');
    fs::write(path, body)
}

fn read_voice(raw: &Value) -> Option<VoiceConfig> {
    let Value::Object(map) = raw else {
        return None;
    };
    let engine = match map.get("engine").and_then(Value::as_str) {
        Some("sami") => Some(VoiceEngine::Sami),
        Some("openai") => Some(VoiceEngine::Openai),
        Some(_) => return None,
        None => None,
    };

    let sami = map
        .get("sami")
        .and_then(|value| serde_json::from_value::<VoiceSamiCreds>(value.clone()).ok());
    let openai = map
        .get("openai")
        .and_then(|value| serde_json::from_value::<VoiceOpenAIConfig>(value.clone()).ok());

    if engine.is_none() && sami.is_none() && openai.is_none() {
        return None;
    }

    let speaker = map
        .get("speaker")
        .and_then(Value::as_str)
        .map(str::to_string);
    let rate = map.get("rate").and_then(Value::as_f64);

    Some(VoiceConfig {
        engine,
        speaker,
        rate,
        sami,
        openai,
    })
}

pub fn read_global_config() -> GlobalConfig {
    let raw = read_raw_config();
    let lang = raw
        .get("lang")
        .and_then(Value::as_str)
        .map(Locale::from_str);
    let voice = raw.get("voice").and_then(read_voice);
    GlobalConfig { lang, voice }
}

fn merge_global_config(patch: &[(&str, Option<Value>)]) -> Result<(), std::io::Error> {
    let mut current = read_raw_config();
    for (key, value) in patch {
        match value {
            Some(value) => {
                current.insert((*key).to_string(), value.clone());
            }
            None => {
                current.remove(*key);
            }
        }
    }
    write_raw_config(current)
}

pub fn set_global_locale(locale: Option<Locale>) -> Result<(), std::io::Error> {
    let value = locale.map(|loc| Value::String(loc.as_str().to_string()));
    merge_global_config(&[("lang", value)])
}

pub fn set_global_voice(voice: Option<VoiceConfig>) -> Result<(), std::io::Error> {
    let value = match voice {
        Some(voice) => Some(serde_json::to_value(voice).unwrap_or(Value::Null)),
        None => None,
    };
    merge_global_config(&[("voice", value)])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root() -> PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "beam-global-config-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        dir
    }

    #[test]
    fn merge_preserves_unknown_keys() {
        let root = temp_root();
        let path = root.join(".beam").join("config.json");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, r#"{"unknown": 1, "lang": "en"}"#).unwrap();
        let old_home = env::var_os("HOME");
        unsafe {
            env::set_var("HOME", &root);
        }

        set_global_locale(Some(Locale::Zh)).unwrap();
        let raw = fs::read_to_string(&path).unwrap();
        assert!(raw.contains("\"unknown\""));
        assert!(raw.contains("\"lang\""));

        if let Some(old_home) = old_home {
            unsafe {
                env::set_var("HOME", old_home);
            }
        } else {
            unsafe {
                env::remove_var("HOME");
            }
        }
        let _ = fs::remove_dir_all(root);
    }
}
