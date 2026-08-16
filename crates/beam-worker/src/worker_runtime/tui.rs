use super::*;

pub(crate) fn term_action_keys(key: TermActionKey) -> Vec<String> {
    match key {
        TermActionKey::Esc => vec!["Escape".to_string()],
        TermActionKey::CtrlC => vec!["C-c".to_string()],
        TermActionKey::Tab => vec!["Tab".to_string()],
        TermActionKey::Enter => vec!["Enter".to_string()],
        TermActionKey::Space => vec!["Space".to_string()],
        TermActionKey::Up => vec!["Up".to_string()],
        TermActionKey::Down => vec!["Down".to_string()],
        TermActionKey::Left => vec!["Left".to_string()],
        TermActionKey::Right => vec!["Right".to_string()],
        TermActionKey::HalfPageUp => vec!["PageUp".to_string()],
        TermActionKey::HalfPageDown => vec!["PageDown".to_string()],
    }
}

pub(crate) async fn handle_tui_keys(
    backend: &Arc<dyn SessionBackend>,
    analyzer_runtime: &Arc<RwLock<AnalyzerRuntime>>,
    keys: &[String],
    is_final: bool,
) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    for key in keys {
        backend.send_special_keys(std::slice::from_ref(key)).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if is_final {
        analyzer_runtime.write().await.prompt_active = false;
    }
    Ok(())
}

pub(crate) async fn handle_tui_text_input(
    backend: &Arc<dyn SessionBackend>,
    adapter: &Arc<Mutex<CliAdapter>>,
    analyzer_runtime: &Arc<RwLock<AnalyzerRuntime>>,
    keys: &[String],
    text: &str,
) -> Result<()> {
    let nav_keys = if keys.last().map(String::as_str) == Some("Enter") {
        &keys[..keys.len().saturating_sub(1)]
    } else {
        keys
    };
    if !nav_keys.is_empty() {
        for key in nav_keys {
            backend.send_special_keys(std::slice::from_ref(key)).await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    analyzer_runtime.write().await.prompt_active = false;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = adapter
        .lock()
        .await
        .write_input(backend.as_ref(), text)
        .await?;
    Ok(())
}

pub(crate) async fn handle_tui_prompt_override(
    stdout: &Arc<Mutex<tokio::io::Stdout>>,
    analyzer_runtime: &Arc<RwLock<AnalyzerRuntime>>,
) {
    let was_active = {
        let mut runtime = analyzer_runtime.write().await;
        if runtime.prompt_active {
            runtime.prompt_active = false;
            true
        } else {
            false
        }
    };
    if was_active {
        let _ = send_message(
            stdout,
            &WorkerToDaemon::TuiPromptResolved {
                selected_text: Some("user-override".to_string()),
            },
        )
        .await;
    }
}
