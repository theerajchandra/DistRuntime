use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use bytes::BytesMut;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;

use crate::stream::ByteStream;

const DEFAULT_BUF_SIZE: usize = 64 * 1024; // 64 KiB

/// Buffered async file reader that yields chunks via [`ByteStream`].
pub struct FileReader {
    reader: tokio::io::BufReader<tokio::fs::File>,
    buf_size: usize,
    scratch: BytesMut,
}

impl FileReader {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with_buf_size(path, DEFAULT_BUF_SIZE).await
    }

    pub async fn open_with_buf_size(path: impl Into<PathBuf>, buf_size: usize) -> Result<Self> {
        let file = tokio::fs::File::open(path.into()).await?;
        let reader = tokio::io::BufReader::with_capacity(buf_size, file);
        Ok(Self {
            reader,
            buf_size,
            scratch: BytesMut::with_capacity(buf_size),
        })
    }
}

#[async_trait]
impl ByteStream for FileReader {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        self.scratch.resize(self.buf_size, 0);
        let n = self.reader.read(&mut self.scratch[..]).await?;
        if n == 0 {
            self.scratch.clear();
            return Ok(None);
        }
        self.scratch.truncate(n);
        Ok(Some(self.scratch.split().freeze()))
    }
}
