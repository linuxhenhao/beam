use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::{os::unix::process::CommandExt, process::Command as StdCommand};

use anyhow::Result;
use beam_core::{
    ApiHealth, BeamPaths, BotConfig, CreateSessionRequest, DaemonRuntimeState, FinalOutputRequest,
    MentionTarget, RestartSessionRequest, ResumeSessionRequest, Session, SessionInputRequest,
    SessionStatus, SessionSummary,
};
use clap::{Args, Parser, Subcommand};
use reqwest::Client;

mod ask_hook;
mod autostart;
mod global_config;
mod hook_setup;
mod register_app;
mod workflow_cli;

#[derive(Debug, Parser)]
#[command(name = "beam", version, about = "Rust core runtime for beam")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Start,
    Stop,
    Restart,
    Logs,
    Status,
    #[command(name = "list", alias = "ls")]
    List {
        #[arg(long)]
        plain: bool,
    },
    Attach {
        session_id: String,
    },
    Workflow {
        #[command(subcommand)]
        command: workflow_cli::WorkflowCommand,
    },
    Send(SendArgs),
    History(HistoryArgs),
    Quoted(QuotedArgs),
    Bots {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    Setup,
    Migrate {
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    Dashboard,
    Autostart {
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    Schedule {
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    Report {
        content: Option<String>,
    },
    Ask {
        content: Option<String>,
    },
    Hook {
        cli_id: Option<String>,
    },
    Voice {
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    Lang {
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    Session {
        #[command(subcommand)]
        command: SessionCommand,
    },
    Simulate {
        #[command(subcommand)]
        command: SimulateCommand,
    },
    #[command(hide = true, name = "__daemon")]
    InternalDaemon,
    #[command(hide = true, name = "__worker")]
    InternalWorker(WorkerArgs),
}

#[derive(Debug, Subcommand)]
enum SessionCommand {
    Create(SessionCreateArgs),
    List,
    Attach {
        session_id: String,
    },
    Input(SessionInputArgs),
    Refresh {
        session_id: String,
    },
    Restart {
        session_id: String,
        #[arg(long, default_value = "")]
        prompt: String,
    },
    Resume {
        session_id: String,
        #[arg(long, default_value = "")]
        prompt: String,
    },
    Adopt(SessionAdoptArgs),
    Discover,
    Close {
        session_id: String,
    },
    Info {
        session_id: String,
    },
}

#[derive(Debug, Args)]
struct SessionCreateArgs {
    #[arg(long)]
    title: String,
    #[arg(long)]
    cli_id: String,
    #[arg(long)]
    cli_bin: String,
    #[arg(long)]
    working_dir: String,
    #[arg(long, default_value = "")]
    prompt: String,
    #[arg(trailing_var_arg = true)]
    cli_args: Vec<String>,
}

#[derive(Debug, Args)]
struct SessionAdoptArgs {
    #[arg(long)]
    target: String,
    #[arg(long)]
    cli_id: String,
    #[arg(long)]
    cli_bin: String,
    #[arg(long)]
    title: Option<String>,
}

/// beam send — structured message delivery to Feishu.
///
/// Content can come from positional arg, stdin, or --content-file.
/// Exactly one mention policy MUST be chosen: --mention-back, --mention, or --no-mention.
#[derive(Debug, Args)]
struct SendArgs {
    /// Message body (positional). If omitted, reads from stdin.
    content: Option<String>,

    /// Mention someone by open_id[:name]; may be repeated.
    #[arg(long = "mention", value_name = "OPEN_ID[:NAME]")]
    mention: Vec<String>,

    /// Mention the session's triggering sender.
    #[arg(long = "mention-back")]
    mention_back: bool,

    /// Suppress all @-mentions in the message and footer.
    #[arg(long = "no-mention")]
    no_mention: bool,

    /// Read message body from a file path.
    #[arg(long = "content-file", value_name = "PATH")]
    content_file: Option<PathBuf>,

    /// Attach files (repeatable). Alias: --file.
    #[arg(long = "files", visible_alias = "file", value_name = "PATH")]
    files: Vec<String>,

    /// Inline images in an interactive card (repeatable). Alias: --image.
    #[arg(long = "images", visible_alias = "image", value_name = "PATH")]
    images: Vec<String>,

    /// Force sending as a top-level chat message (not a reply).
    #[arg(long = "top-level")]
    top_level: bool,

    /// Target a specific chat by oc_xxx id.
    #[arg(long = "chat-id", value_name = "OC_XXX")]
    chat_id: Option<String>,

    /// Send into a specific thread (message id).
    #[arg(long = "into", value_name = "MESSAGE_ID")]
    into: Option<String>,

    /// Explicitly quote a specific message id.
    #[arg(long = "quote", value_name = "MESSAGE_ID")]
    quote: Option<String>,

    /// Disable automatic quoting in chat scope.
    #[arg(long = "no-quote")]
    no_quote: bool,

    /// (compat no-op) Explicitly send as interactive card.
    #[arg(long = "card")]
    card: bool,

    /// (compat no-op) Explicitly send as text.
    #[arg(long = "text")]
    text: bool,

    /// (compat) Pass through anyway flag.
    #[arg(long = "anyway")]
    anyway: bool,

    /// Request attention with a specific kind (authz|decision|blocked|help).
    /// Defaults to "blocked" when used without a value.
    #[arg(long = "attention", value_name = "KIND", num_args = 0..=1, default_missing_value = "blocked")]
    attention: Option<String>,

    /// Request TTS/voice delivery. NOT YET SUPPORTED — daemon will reject with a clear error.
    #[arg(long = "voice")]
    voice: bool,
}

#[derive(Debug, Args)]
struct SessionInputArgs {
    session_id: String,
    content: String,
    #[arg(long)]
    raw: bool,
}

#[derive(Debug, Args)]
struct HistoryArgs {
    #[arg(long, default_value_t = 50)]
    limit: usize,
    #[arg(long, default_value = "session")]
    scope: String,
    #[arg(long)]
    session_id: Option<String>,
}

#[derive(Debug, Args)]
struct QuotedArgs {
    message_id: String,
    #[arg(long)]
    session_id: Option<String>,
}

#[derive(Debug, Args)]
struct WorkerArgs {
    #[arg(long)]
    init_path: PathBuf,
}

#[derive(Debug, Subcommand)]
enum SimulateCommand {
    #[command(name = "lark-message")]
    LarkMessage(SimulateLarkMessageArgs),
}

#[derive(Debug, Args)]
struct SimulateLarkMessageArgs {
    /// Session ID to simulate the message in.
    #[arg(long, value_name = "SESSION_ID")]
    session: String,
    /// Sender's open ID.
    #[arg(long = "sender", value_name = "OPEN_ID")]
    sender: String,
    /// Message text content.
    text: String,
}

mod cli_commands;
pub(crate) use cli_commands::ask_line;

#[cfg(test)]
#[path = "cli_commands/tests.rs"]
mod tests;

#[tokio::main]
async fn main() -> Result<()> {
    beam_core::logging::init_tracing();

    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let msg = info.to_string();
        if msg.contains("JoinHandle polled after completion") {
            tracing::warn!("JoinHandle dropped after task completion (known tokio 1.52 issue)");
            return;
        }
        default_hook(info);
    }));

    cli_commands::run(Cli::parse().command).await
}
