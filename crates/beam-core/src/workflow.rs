use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowActor {
    Scheduler,
    Human,
    System,
    Worker,
    HostExecutor,
    Supervisor,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowEventEnvelope {
    pub event_id: String,
    pub run_id: String,
    pub timestamp: u64,
    pub schema_version: u32,
    pub actor: WorkflowActor,
    #[serde(rename = "type")]
    pub event_type: String,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventDraft {
    pub event_type: String,
    pub actor: WorkflowActor,
    pub payload: Value,
    #[serde(default)]
    pub timestamp: Option<u64>,
    #[serde(default)]
    pub payload_hash: Option<String>,
}

static RUN_MUTEXES: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();

fn run_mutex(run_id: &str) -> Arc<Mutex<()>> {
    let map = RUN_MUTEXES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().expect("workflow mutex map poisoned");
    guard
        .entry(run_id.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

#[derive(Debug, Clone)]
pub struct EventLog {
    pub run_id: String,
    pub run_dir: PathBuf,
    pub events_file: PathBuf,
    pub blob_dir: PathBuf,
    seq: u64,
    seq_loaded: bool,
    cached_len: u64,
}

impl EventLog {
    pub fn new(run_id: impl Into<String>, base_dir: impl AsRef<Path>) -> Result<Self> {
        let run_id = run_id.into();
        if run_id.trim().is_empty() {
            anyhow::bail!("EventLog: runId required");
        }
        let base_dir = base_dir.as_ref();
        if base_dir.as_os_str().is_empty() {
            anyhow::bail!("EventLog: baseDir required");
        }
        let run_dir = base_dir.join(&run_id);
        let events_file = run_dir.join("events.ndjson");
        let blob_dir = run_dir.join("blobs");
        fs::create_dir_all(&blob_dir).context("failed to create workflow run directories")?;
        Ok(Self {
            run_id,
            run_dir,
            events_file,
            blob_dir,
            seq: 0,
            seq_loaded: false,
            cached_len: 0,
        })
    }

    pub fn append(&mut self, draft: EventDraft) -> Result<WorkflowEventEnvelope> {
        let mutex = run_mutex(&self.run_id);
        let _guard = mutex.lock().expect("workflow run mutex poisoned");
        let _lock = FileLock::acquire(&self.events_file)?;
        self.refresh_seq_if_stale()?;

        let next_seq = self.seq + 1;
        let timestamp = draft.timestamp.unwrap_or_else(now_ms);
        let candidate = WorkflowEventEnvelope {
            event_id: format!("{}-{}", self.run_id, next_seq),
            run_id: self.run_id.clone(),
            timestamp,
            schema_version: 1,
            actor: draft.actor,
            event_type: draft.event_type,
            payload: draft.payload,
            payload_hash: draft.payload_hash,
        };

        let line = serde_json::to_string(&candidate)? + "\n";
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.events_file)
            .with_context(|| format!("failed to open {}", self.events_file.display()))?;
        file.write_all(line.as_bytes())?;
        file.flush()?;

        self.seq = next_seq;
        self.seq_loaded = true;
        self.cached_len = fs::metadata(&self.events_file)?.len();
        Ok(candidate)
    }

    pub fn read_all(&self) -> Result<Vec<WorkflowEventEnvelope>> {
        if !self.events_file.exists() {
            return Ok(Vec::new());
        }
        let mut raw = String::new();
        fs::File::open(&self.events_file)?.read_to_string(&mut raw)?;
        let mut events = Vec::new();
        for (idx, line) in raw.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let event = serde_json::from_str::<WorkflowEventEnvelope>(line)
                .with_context(|| format!("corrupt event at line {}", idx + 1))?;
            events.push(event);
        }
        Ok(events)
    }

    pub fn read_blob(&self, reference: &str) -> Result<Vec<u8>> {
        Ok(fs::read(reference)?)
    }

    pub fn current_seq(&mut self) -> Result<u64> {
        self.refresh_seq_if_stale()?;
        Ok(self.seq)
    }

    fn refresh_seq_if_stale(&mut self) -> Result<()> {
        if !self.events_file.exists() {
            self.seq = 0;
            self.cached_len = 0;
            self.seq_loaded = true;
            return Ok(());
        }
        let len = fs::metadata(&self.events_file)?.len();
        if self.seq_loaded && len == self.cached_len {
            return Ok(());
        }
        let events = self.read_all()?;
        self.seq = events
            .iter()
            .filter_map(|event| event.event_id.rsplit_once('-'))
            .filter_map(|(_, seq)| seq.parse::<u64>().ok())
            .max()
            .unwrap_or(0);
        self.cached_len = len;
        self.seq_loaded = true;
        Ok(())
    }
}

struct FileLock {
    path: PathBuf,
}

impl FileLock {
    fn acquire(target: &Path) -> Result<Self> {
        let path = target.with_extension("lock");
        for _ in 0..300 {
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(_) => return Ok(Self { path }),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(err) => return Err(err.into()),
            }
        }
        anyhow::bail!("timed out acquiring workflow file lock: {}", path.display());
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn now_ms() -> u64 {
    let now = std::time::SystemTime::now();
    now.duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
