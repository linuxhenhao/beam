use crate::*;
use anyhow::Result;

pub(crate) fn print_lang_status() {
    let cfg = global_config::read_global_config();
    let global_lang = cfg.lang.map(|loc| loc.as_str().to_string());
    let effective = global_lang.clone().unwrap_or_else(|| "zh".to_string());
    println!(
        "Global lang: {}",
        global_lang.as_deref().unwrap_or("(unset, defaults to zh)")
    );
    println!("Effective for CLI:    {}", effective);
    println!(
        "Config file:          {}",
        global_config::global_config_path().display()
    );
}

pub(crate) fn cmd_lang(args: Vec<String>) -> Result<()> {
    let sub = args.first().map(|s| s.as_str()).unwrap_or("");
    if sub.is_empty() {
        print_lang_status();
        return Ok(());
    }
    if sub == "--unset" {
        global_config::set_global_locale(None)?;
        println!("✅ Cleared global lang (will default to zh).");
        println!("Run `beam restart` for changes to take effect.");
        return Ok(());
    }
    match sub {
        "zh" | "en" => {
            let locale = beam_core::i18n::Locale::from_code(sub);
            global_config::set_global_locale(Some(locale))?;
            println!("✅ Set global lang → {}.", sub);
            println!("Run `beam restart` for changes to take effect.");
            Ok(())
        }
        _ => {
            eprintln!("Unknown locale \"{}\". Supported: zh, en.", sub);
            eprintln!("Usage: beam lang [zh|en|--unset]");
            std::process::exit(1);
        }
    }
}

pub(crate) fn mask_secret(s: Option<&str>) -> String {
    match s {
        Some(value) if !value.is_empty() => {
            let prefix: String = value.chars().take(4).collect();
            format!("{}***", prefix)
        }
        _ => "(未设)".to_string(),
    }
}

pub(crate) fn ask_line(prompt: &str) -> Result<String> {
    use std::io::{self, Write};
    let mut stdout = io::stdout();
    stdout.write_all(prompt.as_bytes())?;
    stdout.flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    Ok(line.trim().to_string())
}

pub(crate) fn cmd_voice(args: Vec<String>) -> Result<()> {
    let sub = args.first().map(|s| s.as_str()).unwrap_or("");
    if sub == "status" {
        let cfg = global_config::read_global_config().voice;
        if let Some(v) = cfg {
            println!("当前语音配置（全局 ~/.beam/config.json）:");
            println!(
                "  引擎: {}",
                match v.engine {
                    Some(global_config::VoiceEngine::Openai) => "openai",
                    _ => "sami",
                }
            );
            println!("  音色: {}", v.speaker.as_deref().unwrap_or("(默认)"));
            if let Some(rate) = v.rate {
                println!("  语速: {}", rate);
            }
            if let Some(sami) = v.sami {
                println!(
                    "  SAMI: accessKey={} secretKey={} appkey={}{}{}",
                    mask_secret(sami.access_key.as_deref()),
                    mask_secret(sami.secret_key.as_deref()),
                    sami.appkey.as_deref().unwrap_or("(未设)"),
                    sami.token_url
                        .as_deref()
                        .map(|v| format!(" tokenUrl={}", v))
                        .unwrap_or_default(),
                    sami.ws_url
                        .as_deref()
                        .map(|v| format!(" wsUrl={}", v))
                        .unwrap_or_default(),
                );
            }
            if let Some(openai) = v.openai {
                println!(
                    "  OpenAI: baseUrl={} model={} apiKey={}",
                    openai.base_url.as_deref().unwrap_or("(未设)"),
                    openai.model.as_deref().unwrap_or("(未设)"),
                    mask_secret(openai.api_key.as_deref())
                );
            }
        } else {
            println!("语音功能未配置。运行 `beam voice` 配置。");
        }
        return Ok(());
    }

    if sub == "disable" || sub == "off" {
        global_config::set_global_voice(None)?;
        println!(
            "✅ 已移除全局语音配置（回复卡片不再显示「🔊 语音总结」按钮）。重启 daemon 生效。"
        );
        return Ok(());
    }

    if !sub.is_empty() && sub != "setup" {
        eprintln!("用法: beam voice [status|disable]（无参 = 交互式配置）");
        std::process::exit(1);
    }

    println!("🔊 配置语音总结（高级功能）。写入全局 ~/.beam/config.json，重启后生效。\n");
    let engine = ask_line(
        "选择 TTS 引擎  [1] SAMI（需 AK/SK/appkey）  [2] OpenAI 兼容（自带 baseUrl/key）: ",
    )?;
    let mut voice = global_config::VoiceConfig::default();
    if engine == "2" || engine.to_lowercase().contains("openai") {
        voice.engine = Some(global_config::VoiceEngine::Openai);
        let base_url = ask_line(
            "baseUrl（如 https://api.openai.com/v1，自托管如 http://127.0.0.1:8880/v1）: ",
        )?;
        let api_key = ask_line("apiKey（无则留空）: ")?;
        let model = ask_line("model（如 tts-1 / kokoro）: ")?;
        if base_url.is_empty() || model.is_empty() {
            eprintln!("❌ baseUrl 和 model 必填，未写入。");
            return Ok(());
        }
        voice.openai = Some(global_config::VoiceOpenAIConfig {
            base_url: Some(base_url),
            api_key: if api_key.is_empty() {
                None
            } else {
                Some(api_key)
            },
            model: Some(model),
        });
        let sp = ask_line("音色 voice（留空=默认 alloy）: ")?;
        if !sp.is_empty() {
            voice.speaker = Some(sp);
        }
    } else {
        voice.engine = Some(global_config::VoiceEngine::Sami);
        let access_key = ask_line("SAMI accessKey: ")?;
        let secret_key = ask_line("SAMI secretKey: ")?;
        let appkey = ask_line("SAMI appkey: ")?;
        if access_key.is_empty() || secret_key.is_empty() || appkey.is_empty() {
            eprintln!("❌ accessKey/secretKey/appkey 都必填，未写入。");
            return Ok(());
        }
        let mut sami = global_config::VoiceSamiCreds {
            access_key: Some(access_key),
            secret_key: Some(secret_key),
            appkey: Some(appkey),
            token_url: None,
            ws_url: None,
        };
        let sp = ask_line("音色 speaker（留空=默认灿灿）: ")?;
        if !sp.is_empty() {
            voice.speaker = Some(sp);
        }
        let adv = ask_line("自定义 SAMI 端点？一般不用，回车跳过 (y/N): ")?;
        if adv.to_lowercase() == "y" {
            let token_url = ask_line("tokenUrl（留空用默认）: ")?;
            let ws_url = ask_line("wsUrl（留空用默认）: ")?;
            if !token_url.is_empty() {
                sami.token_url = Some(token_url);
            }
            if !ws_url.is_empty() {
                sami.ws_url = Some(ws_url);
            }
        }
        voice.sami = Some(sami);
    }

    let rate = ask_line("语速倍率（留空=1.1）: ")?;
    if !rate.is_empty()
        && let Ok(parsed) = rate.parse::<f64>()
    {
        voice.rate = Some(parsed);
    }

    global_config::set_global_voice(Some(voice))?;
    println!(
        "\n✅ 已写入 voice 配置。`beam restart` 后，配了语音的机器人回复卡片底部会出现「🔊 语音总结」按钮。"
    );
    println!("   查看：`beam voice status`  关闭：`beam voice disable`");
    Ok(())
}
