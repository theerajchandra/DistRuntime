use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::stream::ByteStream;

/// Wraps a [`ByteStream`] with a shared concurrency limiter.
///
/// Each call to `next_chunk` acquires a permit from the semaphore,
/// ensuring that at most `max_concurrent` reads are in-flight at once
/// across all streams sharing the same semaphore.
pub struct LimitedStream<S> {
    inner: S,
    semaphore: Arc<Semaphore>,
}

impl<S: ByteStream> LimitedStream<S> {
    pub fn new(inner: S, semaphore: Arc<Semaphore>) -> Self {
        Self { inner, semaphore }
    }
}

#[async_trait]
impl<S: ByteStream> ByteStream for LimitedStream<S> {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| anyhow::anyhow!("semaphore closed: {e}"))?;
        self.inner.next_chunk().await
    }
}

/// Create a shared concurrency limiter.
pub fn concurrency_limiter(max_concurrent: usize) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(max_concurrent))
}
