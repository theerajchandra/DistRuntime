use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;

/// A unified trait for reading byte chunks from any storage backend.
///
/// Implementors yield data in backpressure-friendly chunks, allowing
/// callers to control memory usage by consuming chunks incrementally.
#[async_trait]
pub trait ByteStream: Send + Sync {
    /// Read the next chunk of bytes. Returns `Ok(None)` at EOF.
    async fn next_chunk(&mut self) -> Result<Option<Bytes>>;

    /// Read all remaining bytes into a single buffer.
    async fn read_all(&mut self) -> Result<Bytes> {
        let mut buf = Vec::new();
        while let Some(chunk) = self.next_chunk().await? {
            buf.extend_from_slice(&chunk);
        }
        Ok(Bytes::from(buf))
    }
}
