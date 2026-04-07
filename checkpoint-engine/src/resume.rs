use crate::registry::CheckpointMetadata;

/// Information needed to resume training from a checkpoint.
#[derive(Debug, Clone)]
pub struct ResumeInfo {
    /// Training step to resume from.
    pub step: u64,
    /// Training epoch to resume from.
    pub epoch: u64,
    /// The checkpoint identifier.
    pub checkpoint_id: String,
    /// S3 prefix where committed shard files live.
    /// Format: `checkpoints/{checkpoint_id}/committed/`
    pub committed_path: String,
    /// Optional loss value recorded at checkpoint time.
    pub loss: Option<f64>,
}

impl ResumeInfo {
    /// Build a `ResumeInfo` from a committed checkpoint's metadata.
    pub fn from_metadata(meta: &CheckpointMetadata) -> Self {
        Self {
            step: meta.step,
            epoch: meta.epoch,
            checkpoint_id: meta.checkpoint_id.clone(),
            committed_path: format!("checkpoints/{}/committed/", meta.checkpoint_id),
            loss: meta.loss,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_metadata(
        checkpoint_id: &str,
        job_id: &str,
        epoch: u64,
        step: u64,
        loss: Option<f64>,
    ) -> CheckpointMetadata {
        CheckpointMetadata {
            version: 1,
            checkpoint_id: checkpoint_id.to_string(),
            job_id: job_id.to_string(),
            epoch,
            step,
            committed_at_secs: 0,
            total_bytes: 1024,
            loss,
            config_hash: None,
        }
    }

    #[test]
    fn from_metadata_sets_correct_fields() {
        let meta = make_metadata("ckpt-job-0-100", "job-0", 2, 100, Some(0.42));
        let info = ResumeInfo::from_metadata(&meta);
        assert_eq!(info.step, 100);
        assert_eq!(info.epoch, 2);
        assert_eq!(info.checkpoint_id, "ckpt-job-0-100");
        assert_eq!(info.committed_path, "checkpoints/ckpt-job-0-100/committed/");
        assert_eq!(info.loss, Some(0.42));
    }

    #[test]
    fn committed_path_format() {
        let meta = make_metadata("ckpt-job-0-500", "job-0", 0, 500, None);
        let info = ResumeInfo::from_metadata(&meta);
        assert_eq!(info.committed_path, "checkpoints/ckpt-job-0-500/committed/");
    }
}
