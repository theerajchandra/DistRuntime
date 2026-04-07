mod storage;
pub use storage::CheckpointStorage;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

// ── Errors ────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    #[error("checkpoint '{0}' not found")]
    NotFound(String),
    #[error("checkpoint '{0}' is not in the preparing phase")]
    InvalidState(String),
    #[error("worker '{0}' has already committed")]
    AlreadyCommitted(String),
}

// ── Internal phase state machine ──────────────────────────────────────────────

enum CheckpointPhase {
    Preparing {
        expected_workers: HashSet<String>,
        committed: HashMap<String, u64>, // worker_id -> bytes_written
    },
    Committed,
}

// ── Session (private) ─────────────────────────────────────────────────────────

struct CheckpointSession {
    job_id: String,
    epoch: u64,
    step: u64,
    storage_path: String,
    phase: CheckpointPhase,
}

// ── Engine (public) ───────────────────────────────────────────────────────────

/// Two-phase checkpoint protocol engine.
///
/// **Phase 1 – Begin**: Locks checkpoint state, assigns a unique checkpoint ID
/// and a storage path for shards.  All participating workers are registered as
/// "expected" committers.
///
/// **Phase 2 – Commit**: Each worker calls `worker_commit` once it finishes
/// writing its shard.  When every expected worker has committed, the engine
/// atomically transitions the session to `Committed`, records the total bytes
/// written, and returns `Ok(true)` to the final committer.
///
/// **Abort**: Any party can call `abort` to cancel a checkpoint that is still
/// in the `Preparing` phase.  The session is removed from memory immediately
/// (in a real system the storage path would also be deleted), guaranteeing
/// that no partial checkpoint data can be mistakenly used.
pub struct CheckpointEngine {
    sessions: Arc<Mutex<HashMap<String, CheckpointSession>>>,
    storage: Option<Arc<storage::CheckpointStorage>>,
}

impl CheckpointEngine {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            storage: None,
        }
    }

    /// Attach a [`CheckpointStorage`] backend.  When set, staging shards are
    /// promoted to committed on a successful checkpoint and GC'd on abort.
    pub fn with_storage(mut self, s: CheckpointStorage) -> Self {
        self.storage = Some(Arc::new(s));
        self
    }

    /// Start a new checkpoint for `job_id` at the given `epoch` and `step`.
    ///
    /// Returns `(checkpoint_id, storage_path)`.
    pub async fn begin(
        &self,
        job_id: &str,
        epoch: u64,
        step: u64,
        expected_workers: Vec<String>,
    ) -> (String, String) {
        let checkpoint_id = format!("ckpt-{job_id}-{epoch}-{step}");
        let storage_path = format!("checkpoints/{checkpoint_id}");

        let session = CheckpointSession {
            job_id: job_id.to_string(),
            epoch,
            step,
            storage_path: storage_path.clone(),
            phase: CheckpointPhase::Preparing {
                expected_workers: expected_workers.into_iter().collect(),
                committed: HashMap::new(),
            },
        };

        self.sessions
            .lock()
            .await
            .insert(checkpoint_id.clone(), session);

        (checkpoint_id, storage_path)
    }

    /// Record that `worker_id` has finished writing `bytes_written` bytes for
    /// the given checkpoint.
    ///
    /// Returns `Ok(true)` when all expected workers have committed (the
    /// checkpoint is now fully committed), `Ok(false)` while commits are still
    /// pending.
    pub async fn worker_commit(
        &self,
        checkpoint_id: &str,
        worker_id: &str,
        bytes_written: u64,
    ) -> Result<bool, CheckpointError> {
        let mut sessions = self.sessions.lock().await;

        let session = sessions
            .get_mut(checkpoint_id)
            .ok_or_else(|| CheckpointError::NotFound(checkpoint_id.to_string()))?;

        // Determine what to do while the borrow of `session.phase` is live,
        // then apply the phase transition (if any) after the borrow ends.
        enum Action {
            Done(u64), // all workers committed; carry total_bytes
            Pending,
            Err(CheckpointError),
        }

        let action = match &mut session.phase {
            CheckpointPhase::Preparing {
                expected_workers,
                committed,
            } => {
                if committed.contains_key(worker_id) {
                    Action::Err(CheckpointError::AlreadyCommitted(worker_id.to_string()))
                } else {
                    committed.insert(worker_id.to_string(), bytes_written);
                    if committed.len() == expected_workers.len() {
                        let total_bytes: u64 = committed.values().sum();
                        Action::Done(total_bytes)
                    } else {
                        Action::Pending
                    }
                }
            }
            _ => Action::Err(CheckpointError::InvalidState(checkpoint_id.to_string())),
        };

        match action {
            Action::Err(e) => Err(e),
            Action::Pending => Ok(false),
            Action::Done(total_bytes) => {
                // Collect committed worker IDs from the Preparing phase BEFORE
                // we overwrite session.phase.
                let worker_ids: Vec<String> =
                    if let CheckpointPhase::Preparing { ref committed, .. } = session.phase {
                        committed.keys().cloned().collect()
                    } else {
                        vec![]
                    };

                tracing::info!(
                    checkpoint_id,
                    job_id = %session.job_id,
                    epoch = session.epoch,
                    step = session.step,
                    total_bytes,
                    "checkpoint fully committed"
                );
                session.phase = CheckpointPhase::Committed;

                if let Some(ref storage) = self.storage {
                    let storage = Arc::clone(storage);
                    let ckpt_id = checkpoint_id.to_string();
                    let wids = worker_ids.clone();
                    tokio::spawn(async move {
                        if let Err(e) = storage.promote_to_committed(&ckpt_id, &wids).await {
                            tracing::error!(
                                checkpoint_id = %ckpt_id,
                                error = %e,
                                "failed to promote checkpoint to committed"
                            );
                        }
                    });
                }

                Ok(true)
            }
        }
    }

    /// Abort a checkpoint that is still in the `Preparing` phase.
    ///
    /// The session is removed from the in-memory map immediately — in a
    /// production system this would also trigger deletion of any partially
    /// written shard files at `storage_path`, ensuring no partial checkpoint
    /// data can be read by other components.
    ///
    /// Returns `true` if the session existed and was in `Preparing`, `false`
    /// otherwise (already committed or not found).
    pub async fn abort(&self, checkpoint_id: &str, reason: &str) -> bool {
        let mut sessions = self.sessions.lock().await;

        match sessions.get(checkpoint_id) {
            None => return false,
            Some(session) => {
                if let CheckpointPhase::Committed = &session.phase {
                    return false;
                }
                tracing::warn!(
                    checkpoint_id,
                    job_id = %session.job_id,
                    storage_path = %session.storage_path,
                    reason,
                    "checkpoint aborted"
                );
            }
        }

        // Remove from map — this is the "no partial data" guarantee.
        sessions.remove(checkpoint_id);

        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let ckpt_id = checkpoint_id.to_string();
            tokio::spawn(async move {
                match storage.gc_staging(&ckpt_id).await {
                    Ok(n) => tracing::info!(
                        checkpoint_id = %ckpt_id,
                        deleted = n,
                        "staging GC complete"
                    ),
                    Err(e) => tracing::error!(
                        checkpoint_id = %ckpt_id,
                        error = %e,
                        "staging GC failed"
                    ),
                }
            });
        }

        true
    }
}

impl Default for CheckpointEngine {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn begin_returns_expected_ids() {
        let engine = CheckpointEngine::new();
        let (checkpoint_id, storage_path) =
            engine.begin("job-42", 1, 5, vec!["w0".to_string()]).await;

        assert_eq!(checkpoint_id, "ckpt-job-42-1-5");
        assert_eq!(storage_path, "checkpoints/ckpt-job-42-1-5");
    }

    #[tokio::test]
    async fn commit_all_workers_transitions_to_committed() {
        let engine = CheckpointEngine::new();
        let (ckpt_id, _) = engine
            .begin("job-1", 0, 0, vec!["w0".to_string(), "w1".to_string()])
            .await;

        let first = engine.worker_commit(&ckpt_id, "w0", 100).await.unwrap();
        assert!(!first, "first commit should not finalize");

        let second = engine.worker_commit(&ckpt_id, "w1", 200).await.unwrap();
        assert!(second, "second commit should finalize the checkpoint");
    }

    #[tokio::test]
    async fn commit_partial_returns_false() {
        let engine = CheckpointEngine::new();
        let (ckpt_id, _) = engine
            .begin("job-2", 0, 0, vec!["w0".to_string(), "w1".to_string()])
            .await;

        let result = engine.worker_commit(&ckpt_id, "w0", 512).await.unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn commit_unknown_checkpoint_returns_not_found() {
        let engine = CheckpointEngine::new();
        let err = engine.worker_commit("bogus-id", "w0", 0).await.unwrap_err();

        assert!(matches!(err, CheckpointError::NotFound(_)));
    }

    #[tokio::test]
    async fn commit_duplicate_worker_returns_already_committed() {
        let engine = CheckpointEngine::new();
        let (ckpt_id, _) = engine
            .begin("job-3", 0, 0, vec!["w0".to_string(), "w1".to_string()])
            .await;

        engine.worker_commit(&ckpt_id, "w0", 64).await.unwrap();

        let err = engine.worker_commit(&ckpt_id, "w0", 64).await.unwrap_err();

        assert!(matches!(err, CheckpointError::AlreadyCommitted(_)));
    }

    #[tokio::test]
    async fn abort_cleans_up_session() {
        let engine = CheckpointEngine::new();
        let (ckpt_id, _) = engine.begin("job-4", 0, 0, vec!["w0".to_string()]).await;

        let aborted = engine.abort(&ckpt_id, "node failure").await;
        assert!(aborted);

        // Session must have been removed — worker_commit should return NotFound
        let err = engine.worker_commit(&ckpt_id, "w0", 0).await.unwrap_err();
        assert!(matches!(err, CheckpointError::NotFound(_)));
    }

    #[tokio::test]
    async fn abort_unknown_returns_false() {
        let engine = CheckpointEngine::new();
        let result = engine.abort("does-not-exist", "test").await;
        assert!(!result);
    }

    #[tokio::test]
    async fn abort_committed_checkpoint_returns_false() {
        let engine = CheckpointEngine::new();
        let (ckpt_id, _) = engine.begin("job-5", 0, 0, vec!["w0".to_string()]).await;

        // Fully commit first
        let committed = engine.worker_commit(&ckpt_id, "w0", 256).await.unwrap();
        assert!(committed);

        // Abort should refuse because the session is already Committed
        let result = engine.abort(&ckpt_id, "too late").await;
        assert!(!result);
    }
}
