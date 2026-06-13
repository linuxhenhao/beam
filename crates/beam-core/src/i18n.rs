use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Locale {
    Zh,
    En,
}

impl Locale {
    pub fn from_str(s: &str) -> Self {
        match s {
            "en" => Self::En,
            _ => Self::Zh,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Zh => "zh",
            Self::En => "en",
        }
    }
}

pub struct I18n {
    locale: Locale,
    messages: HashMap<&'static str, &'static str>,
}

impl I18n {
    pub fn new(locale: Locale) -> Self {
        let messages = match locale {
            Locale::Zh => HashMap::from([
                ("permission_denied", "权限不足"),
                ("session_closed", "会话已关闭"),
                ("session_created", "会话已创建"),
                ("session_not_found", "未找到会话"),
                ("grant_success", "授权成功"),
                ("grant_denied_msg", "已拒绝授权"),
                ("workflow_running", "工作流运行中"),
                ("workflow_complete", "工作流已完成"),
                ("workflow_failed", "工作流失败"),
                ("setup_complete", "设置完成"),
                ("migration_complete", "迁移完成"),
                ("bot_not_found", "未找到机器人配置"),
                ("daemon_started", "守护进程已启动"),
                ("daemon_stopped", "守护进程已停止"),
            ]),
            Locale::En => HashMap::from([
                ("permission_denied", "Permission denied"),
                ("session_closed", "Session closed"),
                ("session_created", "Session created"),
                ("session_not_found", "Session not found"),
                ("grant_success", "Grant successful"),
                ("grant_denied_msg", "Grant denied"),
                ("workflow_running", "Workflow running"),
                ("workflow_complete", "Workflow completed"),
                ("workflow_failed", "Workflow failed"),
                ("setup_complete", "Setup complete"),
                ("migration_complete", "Migration complete"),
                ("bot_not_found", "Bot config not found"),
                ("daemon_started", "Daemon started"),
                ("daemon_stopped", "Daemon stopped"),
            ]),
        };
        Self { locale, messages }
    }

    pub fn t(&self, key: &str) -> &'static str {
        self.messages.get(key).copied().unwrap_or("")
    }

    pub fn locale(&self) -> Locale {
        self.locale
    }
}
