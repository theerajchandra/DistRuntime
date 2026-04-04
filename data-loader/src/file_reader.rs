use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;

use crate::stream::ByteStream;

const DEFAULT_BUF_SIZE: usize = 64 * 1024; // 64 KiB

/// Buffered async file reader that yields chunks via [`ByteStream`].
pub struct FileReader {
    reader: tokio::io::BufReader<tokio::fs::File>,
    buf_size: usize,
}

impl FileReader {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with_buf_size(path, DEFAULT_BUF_SIZE).await
    }

    pub async fn open_with_buf_size(path: impl Into<PathBuf>, buf_size: usize) -> Result<Self> {
        let file = tokio::fs::File::open(path.into()).await?;
        let reader = tokio::io::BufReader::with_capacity(buf_size, file);
        Ok(Self { reader, buf_size })
    }
}

#[async_trait]
impl ByteStream for FileReader {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        let mut buf = vec![0u8; self.buf_size];
        let n = self.reader.read(&mut buf).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.truncate(n);
        Ok(Some(Bytes::from(buf)))
    }
}
