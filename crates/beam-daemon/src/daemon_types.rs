use super::*;

#[derive(Clone)]
pub struct RunOptions {
    pub worker_exe: PathBuf,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct ZellijAdoptCandidate {
    pub(crate) zellij_session: String,
    pub(crate) zellij_pane_id: String,
    pub(crate) title: String,
    pub(crate) cwd: String,
    pub(crate) cli_id: String,
    pub(crate) cli_pid: Option<i32>,
    pub(crate) pane_cols: Option<u16>,
    pub(crate) pane_rows: Option<u16>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct AdoptZellijSessionRequest {
    pub(crate) zellij_session: String,
    pub(crate) zellij_pane_id: String,
    pub(crate) cli_id: String,
    pub(crate) cli_bin: String,
    pub(crate) title: Option<String>,
    pub(crate) cwd: String,
    pub(crate) pane_cols: Option<u16>,
    pub(crate) pane_rows: Option<u16>,
    #[serde(default)]
    pub(crate) lark_app_id: Option<String>,
    #[serde(default)]
    pub(crate) chat_id: Option<String>,
    #[serde(default)]
    pub(crate) chat_type: Option<String>,
    #[serde(default)]
    pub(crate) root_message_id: Option<String>,
    #[serde(default)]
    pub(crate) scope: Option<SessionScope>,
    #[serde(default)]
    pub(crate) thread_id: Option<String>,
    #[serde(default)]
    pub(crate) owner_open_id: Option<String>,
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) paths: BeamPaths,
    pub(crate) started_at: chrono::DateTime<Utc>,
    pub(crate) sessions: Arc<Mutex<HashMap<String, Session>>>,
    pub(crate) workers: Arc<Mutex<HashMap<String, WorkerHandle>>>,
    pub(crate) attempt_resumes: Arc<Mutex<HashMap<String, AttemptResumeEntry>>>,
    pub(crate) shutdown: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    pub(crate) options: RunOptions,
    pub(crate) http: Client,
    pub(crate) config: Config,
    pub(crate) bots: Arc<HashMap<String, BotConfig>>,
    pub(crate) lark_tokens: Arc<Mutex<HashMap<String, CachedLarkToken>>>,
    pub(crate) chat_mode_cache: Arc<Mutex<HashMap<String, CachedChatMode>>>,
    pub(crate) recent_lark_events: Arc<Mutex<HashMap<String, Instant>>>,
    pub(crate) inflight_final_output_turns: Arc<Mutex<HashSet<String>>>,
    pub(crate) workflow_progress_cards: Arc<Mutex<HashMap<String, String>>>,
    pub(crate) ask_pending: Arc<Mutex<HashMap<String, ask::AskPendingEntry>>>,
    pub(crate) grant_pending: Arc<Mutex<HashMap<String, grant::GrantPendingEntry>>>,
    pub(crate) pending_creates: Arc<Mutex<HashMap<String, dir_select::PendingCreateSession>>>,
    pub(crate) dashboard_token: Arc<Mutex<Option<DashboardAuthToken>>>,
    pub(crate) external_host: String,
}

pub(crate) struct WorkerHandle {
    pub(crate) child: Child,
    pub(crate) stdin: Arc<Mutex<ChildStdin>>,
}

#[derive(Debug, Clone)]
pub(crate) struct CachedLarkToken {
    pub(crate) token: String,
    pub(crate) expires_at: Instant,
}

#[derive(Debug, Clone)]
pub(crate) struct CachedChatMode {
    pub(crate) mode: ChatMode,
    pub(crate) cached_at: Instant,
}

#[derive(Debug, Clone)]
pub(crate) struct DashboardAuthToken {
    pub(crate) token: String,
    pub(crate) expires_at: Instant,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct AttemptResumeSidecar {
    pub(crate) schema_version: u64,
    pub(crate) resume_id: String,
    pub(crate) run_id: String,
    pub(crate) activity_id: String,
    pub(crate) attempt_id: String,
    pub(crate) session_id: String,
    pub(crate) original_session_id: String,
    pub(crate) cli_session_id: Option<String>,
    pub(crate) web_port: Option<u16>,
    pub(crate) write_token: Option<String>,
    pub(crate) status: String,
    pub(crate) lark_app_id: String,
    pub(crate) bot_name: Option<String>,
    pub(crate) cli_id: String,
    pub(crate) working_dir: String,
    pub(crate) log_path: String,
    pub(crate) started_at: u64,
    pub(crate) updated_at: u64,
    pub(crate) closed_at: Option<u64>,
    pub(crate) close_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct AttemptResumeEntry {
    pub(crate) resume_id: String,
    pub(crate) run_id: String,
    pub(crate) activity_id: String,
    pub(crate) attempt_id: String,
    pub(crate) session_id: String,
    pub(crate) original_session_id: String,
    pub(crate) cli_session_id: Option<String>,
    pub(crate) lark_app_id: String,
    pub(crate) bot_name: Option<String>,
    pub(crate) cli_id: String,
    pub(crate) working_dir: String,
    pub(crate) log_path: String,
    pub(crate) sidecar_path: String,
    pub(crate) started_at: u64,
    pub(crate) updated_at: u64,
    pub(crate) web_port: Option<u16>,
    pub(crate) write_token: Option<String>,
    pub(crate) close_reason: Option<String>,
}

#[derive(Debug)]
pub(crate) enum AttemptResumeWaitOutcome {
    Ready(AttemptResumeEntry),
    Failed {
        error: String,
        message: Option<String>,
    },
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct LarkTokenResponse {
    pub(crate) code: i32,
    pub(crate) msg: Option<String>,
    pub(crate) tenant_access_token: Option<String>,
    pub(crate) expire: Option<u64>,
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct LarkMessageResponse {
    pub(crate) code: Option<i32>,
    pub(crate) msg: Option<String>,
    pub(crate) data: Option<LarkMessageResponseData>,
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct LarkMessageResponseData {
    pub(crate) message_id: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct WorkflowRunRequest {
    #[serde(default, rename = "rawParams")]
    pub(crate) raw_params: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) initiator: Option<String>,
    #[serde(default, rename = "chatBinding")]
    pub(crate) chat_binding: Option<RunChatBinding>,
}

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub(crate) struct WorkflowWindowQuery {
    #[serde(default)]
    pub(crate) tail: Option<usize>,
    #[serde(default, rename = "beforeSeq")]
    pub(crate) before_seq: Option<u64>,
    #[serde(default, rename = "afterSeq")]
    pub(crate) after_seq: Option<u64>,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub(crate) struct WorkflowRunsQuery {
    #[serde(default)]
    pub(crate) all: Option<String>,
    #[serde(default)]
    pub(crate) status: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct WorkflowCancelRequest {
    #[serde(default)]
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct WorkflowWaitActionRequest {
    #[serde(default)]
    pub(crate) comment: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct WorkflowResumeRequest {
    #[serde(default)]
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct WorkflowRunTriggerBody {
    #[serde(default, rename = "params")]
    pub(crate) params: BTreeMap<String, Value>,
    #[serde(default, rename = "chatBinding")]
    pub(crate) chat_binding: Option<RunChatBinding>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ApiTriggerSource {
    #[serde(rename = "type")]
    pub(crate) source_type: String,
    #[serde(default)]
    pub(crate) connector_id: Option<String>,
    #[serde(default)]
    pub(crate) request_id: Option<String>,
    #[serde(default)]
    pub(crate) received_at: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ApiTriggerTarget {
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) bot_id: Option<String>,
    #[serde(default)]
    pub(crate) chat_id: Option<String>,
    #[serde(default)]
    pub(crate) session_id: Option<String>,
    #[serde(default)]
    pub(crate) workflow_id: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ApiTriggerEnvelope {
    pub(crate) format: String,
    pub(crate) source_name: String,
    pub(crate) trusted: bool,
    #[serde(default)]
    pub(crate) headers: Option<Value>,
    #[serde(default)]
    pub(crate) payload: Option<Value>,
    #[serde(default)]
    pub(crate) raw_text: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ApiTriggerOptions {
    #[serde(default)]
    pub(crate) dry_run: Option<bool>,
    #[serde(default)]
    pub(crate) dedup_key: Option<String>,
    #[serde(default)]
    pub(crate) status: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ApiTriggerRequest {
    pub(crate) source: ApiTriggerSource,
    pub(crate) target: ApiTriggerTarget,
    pub(crate) envelope: ApiTriggerEnvelope,
    #[serde(default)]
    pub(crate) options: ApiTriggerOptions,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct FeishuResumeInput {
    #[serde(rename = "larkAppId")]
    pub(crate) lark_app_id: String,
    #[serde(rename = "chatId", default)]
    pub(crate) chat_id: Option<String>,
    #[serde(rename = "rootMessageId", default)]
    pub(crate) root_message_id: Option<String>,
    pub(crate) content: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FeishuResumeOutcome {
    pub(crate) activity_id: String,
    pub(crate) attempt_id: String,
    pub(crate) decision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FeishuTransientFailure {
    pub(crate) activity_id: String,
    pub(crate) attempt_id: String,
    pub(crate) provider: String,
    pub(crate) idempotency_key: String,
    pub(crate) error_code: String,
    pub(crate) error_class: String,
    pub(crate) error_message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct FeishuResumeResult {
    pub(crate) reconciled: Vec<FeishuResumeOutcome>,
    pub(crate) fresh_retry: Vec<FeishuResumeOutcome>,
    pub(crate) transient_failures: Vec<FeishuTransientFailure>,
    pub(crate) skipped: Vec<String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub(crate) struct WorkflowFeishuSendInput {
    #[serde(rename = "larkAppId")]
    pub(crate) lark_app_id: String,
    #[serde(rename = "chatId")]
    pub(crate) chat_id: String,
    pub(crate) content: String,
    #[serde(rename = "msgType", default)]
    pub(crate) _msg_type: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub(crate) struct WorkflowFeishuReplyInput {
    #[serde(rename = "larkAppId")]
    pub(crate) lark_app_id: String,
    #[serde(rename = "rootMessageId")]
    pub(crate) root_message_id: String,
    pub(crate) content: String,
    #[serde(rename = "msgType", default)]
    pub(crate) _msg_type: Option<String>,
    #[serde(rename = "replyInThread", default)]
    pub(crate) _reply_in_thread: Option<bool>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct FrozenCard {
    pub(crate) message_id: String,
    pub(crate) content: String,
    pub(crate) title: String,
    #[serde(default)]
    pub(crate) display_mode: Option<DisplayMode>,
    #[serde(default)]
    pub(crate) image_key: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct PendingResponsePatchMarker {
    pub(crate) session_id: String,
    pub(crate) card_id: String,
    pub(crate) state: String,
    pub(crate) created_at: String,
    #[serde(default)]
    pub(crate) patched_at: Option<String>,
}

pub(crate) const FINAL_OUTPUT_RETRY_BACKOFF_MS: [u64; 3] = [0, 5_000, 15_000];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrivateCardDelivery {
    Ephemeral,
    DirectMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CardRenderTarget {
    CallbackRaw,
    PatchMessage(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LarkCardDeliveryPlan {
    NotReady,
    PostNew,
    PatchExisting,
}
