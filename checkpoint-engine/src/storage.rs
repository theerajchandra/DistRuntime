use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use bytes::Bytes;
use data_loader::s3_writer;

// ── Key helpers (pure functions) ──────────────────────────────────────────────

/// Staging key: `checkpoints/{checkpoint_id}/staging/{worker_id}.shard`
pub fn staging_key(checkpoint_id: &str, worker_id: &str) -> String {
    format!("checkpoints/{checkpoint_id}/staging/{worker_id}.shard")
}

/// Committed key: `checkpoints/{checkpoint_id}/committed/{worker_id}.shard`
pub fn committed_key(checkpoint_id: &str, worker_id: &str) -> String {
    format!("checkpoints/{checkpoint_id}/committed/{worker_id}.shard")
}

/// Staging prefix for a checkpoint: `checkpoints/{checkpoint_id}/staging/`
pub fn staging_prefix(checkpoint_id: &str) -> String {
    format!("checkpoints/{checkpoint_id}/staging/")
}

// ── CheckpointStorage ─────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct CheckpointStorage {
    client: Client,
    bucket: String,
}

impl CheckpointStorage {
    /// Create a new `CheckpointStorage` using environment-based AWS config.
    ///
    /// Respects `STORAGE_ENDPOINT` for local/test S3 endpoints, and forces
    /// path-style addressing (required by MinIO and LocalStack).
    pub async fn new(bucket: impl Into<String>) -> anyhow::Result<Self> {
        let mut config_loader = aws_config::from_env();
        if let Ok(endpoint) = std::env::var("STORAGE_ENDPOINT") {
            config_loader = config_loader.endpoint_url(endpoint);
        }
        let config = config_loader.load().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(true)
            .build();
        let client = Client::from_conf(s3_config);
        Ok(Self {
            client,
            bucket: bucket.into(),
        })
    }

    /// Direct constructor for testing — inject a pre-built client.
    pub fn with_client(client: Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
        }
    }

    /// Write `data` to the staging prefix for `worker_id` within `checkpoint_id`.
    pub async fn write_staging(
        &self,
        checkpoint_id: &str,
        worker_id: &str,
        data: Bytes,
    ) -> anyhow::Result<()> {
        let key = staging_key(checkpoint_id, worker_id);
        let bytes_written = data.len();
        s3_writer::write_object(&self.client, &self.bucket, &key, data).await?;
        tracing::info!(
            checkpoint_id,
            worker_id,
            bytes_written,
            "wrote shard to staging"
        );
        Ok(())
    }

    /// Copy all staging shards to committed, then delete the staging prefix.
    pub async fn promote_to_committed(
        &self,
        checkpoint_id: &str,
        worker_ids: &[String],
    ) -> anyhow::Result<()> {
        // First copy every staging shard to its committed location.
        for worker_id in worker_ids {
            let src = staging_key(checkpoint_id, worker_id);
            let dst = committed_key(checkpoint_id, worker_id);
            s3_writer::copy_object(&self.client, &self.bucket, &src, &dst).await?;
        }

        // Then bulk-delete the entire staging prefix.
        let prefix = staging_prefix(checkpoint_id);
        s3_writer::delete_prefix(&self.client, &self.bucket, &prefix).await?;

        tracing::info!(
            checkpoint_id,
            worker_count = worker_ids.len(),
            "promoted checkpoint to committed"
        );
        Ok(())
    }

    /// Delete all staging objects for `checkpoint_id` (used on abort).
    ///
    /// Returns the number of objects deleted.
    pub async fn gc_staging(&self, checkpoint_id: &str) -> anyhow::Result<usize> {
        let prefix = staging_prefix(checkpoint_id);
        let deleted = s3_writer::delete_prefix(&self.client, &self.bucket, &prefix).await?;
        tracing::warn!(
            checkpoint_id,
            deleted,
            "GC deleted staging objects for aborted checkpoint"
        );
        Ok(deleted)
    }

    /// Delete ALL staging objects across every checkpoint under `checkpoints/`.
    ///
    /// Called on coordinator restart to clean up abandoned staging writes.
    /// Returns the total number of objects deleted.
    pub async fn gc_all_staging(&self) -> anyhow::Result<usize> {
        let mut continuation_token: Option<String> = None;
        let mut staging_keys: Vec<ObjectIdentifier> = Vec::new();

        // Paginate list_objects_v2 under the root checkpoints/ prefix and
        // collect only keys that contain /staging/.
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix("checkpoints/");

            if let Some(ref token) = continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("failed to list objects for gc_all_staging: {e}"))?;

            for obj in resp.contents() {
                if let Some(k) = obj.key() {
                    if k.contains("/staging/") {
                        let oid = ObjectIdentifier::builder().key(k).build().map_err(|e| {
                            anyhow::anyhow!("failed to build ObjectIdentifier: {e}")
                        })?;
                        staging_keys.push(oid);
                    }
                }
            }

            if resp.is_truncated().unwrap_or(false) {
                continuation_token = resp.next_continuation_token().map(str::to_owned);
            } else {
                break;
            }
        }

        let total = staging_keys.len();
        if total == 0 {
            tracing::warn!(
                deleted = 0,
                "gc_all_staging: no abandoned staging objects found"
            );
            return Ok(0);
        }

        // Delete in batches of 1 000 (S3 DeleteObjects limit).
        for batch in staging_keys.chunks(1_000) {
            let delete = Delete::builder()
                .set_objects(Some(batch.to_vec()))
                .quiet(true)
                .build()
                .map_err(|e| anyhow::anyhow!("failed to build Delete request: {e}"))?;

            self.client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete)
                .send()
                .await
                .map_err(|e| {
                    anyhow::anyhow!("failed to delete staging objects in gc_all_staging: {e}")
                })?;
        }

        tracing::warn!(
            deleted = total,
            "gc_all_staging: deleted abandoned staging objects"
        );
        Ok(total)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staging_key_format() {
        assert_eq!(
            staging_key("ckpt-job1-0-0", "worker-1"),
            "checkpoints/ckpt-job1-0-0/staging/worker-1.shard"
        );
    }

    #[test]
    fn committed_key_format() {
        assert_eq!(
            committed_key("ckpt-job1-0-0", "worker-1"),
            "checkpoints/ckpt-job1-0-0/committed/worker-1.shard"
        );
    }

    #[test]
    fn staging_prefix_format() {
        assert_eq!(
            staging_prefix("ckpt-job1-0-0"),
            "checkpoints/ckpt-job1-0-0/staging/"
        );
    }

    #[test]
    fn staging_and_committed_keys_differ() {
        let id = "ckpt-job1-0-0";
        let w = "worker-1";
        assert_ne!(staging_key(id, w), committed_key(id, w));
    }

    #[test]
    fn staging_prefix_is_prefix_of_staging_key() {
        let id = "ckpt-job1-0-0";
        let w = "worker-1";
        assert!(staging_key(id, w).starts_with(&staging_prefix(id)));
    }
}
