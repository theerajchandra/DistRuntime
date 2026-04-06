use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::file_reader::FileReader;
use crate::format_plugin::{BuiltinFormat, RecordFormatPlugin};
use crate::record::{prefetch_depth, Record};
use crate::shard_reader::ShardReader;
use crate::stream::ByteStream;

/// Reads multiple shards in parallel, funneling records through a bounded
/// channel whose capacity equals the configurable prefetch depth.
pub struct ParallelShardReader {
    rx: mpsc::Receiver<Result<Record>>,
}

/// Describes the location of shard files on local disk.
/// Shard `i` maps to `{base_dir}/shard-{i}.{ext}`.
pub struct ShardDescriptor {
    pub base_dir: PathBuf,
    pub extension: String,
    pub shard_indices: Vec<u64>,
}

impl ShardDescriptor {
    pub fn shard_path(&self, index: u64) -> PathBuf {
        self.base_dir
            .join(format!("shard-{index}.{}", self.extension))
    }
}

impl ParallelShardReader {
    /// Create a reader that processes local-file shards in parallel using a built-in format.
    ///
    /// Prefetch depth is read from the `PREFETCH_DEPTH` env var (default 64).
    pub fn open(desc: ShardDescriptor, format: BuiltinFormat) -> Self {
        Self::open_plugin(desc, Arc::new(format), prefetch_depth())
    }

    pub fn open_with_prefetch(
        desc: ShardDescriptor,
        format: BuiltinFormat,
        prefetch: usize,
    ) -> Self {
        Self::open_plugin(desc, Arc::new(format), prefetch)
    }

    /// Create a reader with an arbitrary [`RecordFormatPlugin`] (including custom types).
    pub fn open_plugin(
        desc: ShardDescriptor,
        plugin: Arc<dyn RecordFormatPlugin>,
        prefetch: usize,
    ) -> Self {
        let (tx, rx) = mpsc::channel(prefetch.max(1));

        for &idx in &desc.shard_indices {
            let path = desc.shard_path(idx);
            let tx = tx.clone();
            let plugin = plugin.clone();

            tokio::spawn(async move {
                let result = Self::read_shard(path, plugin).await;
                match result {
                    Ok(records) => {
                        for r in records {
                            if tx.send(Ok(r)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                    }
                }
            });
        }

        Self { rx }
    }

    /// Create a reader from an arbitrary list of `ByteStream` sources.
    pub fn from_streams(
        streams: Vec<Box<dyn ByteStream>>,
        plugin: Arc<dyn RecordFormatPlugin>,
        prefetch: usize,
    ) -> Self {
        let (tx, rx) = mpsc::channel(prefetch.max(1));

        for stream in streams {
            let tx = tx.clone();
            let plugin = plugin.clone();

            tokio::spawn(async move {
                match ShardReader::load(stream, plugin).await {
                    Ok(mut reader) => {
                        while let Some(r) = reader.next_record() {
                            if tx.send(Ok(r)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                    }
                }
            });
        }

        Self { rx }
    }

    /// Get the next record. Returns `None` when all shards are exhausted.
    pub async fn next_record(&mut self) -> Option<Result<Record>> {
        self.rx.recv().await
    }

    async fn read_shard(path: PathBuf, plugin: Arc<dyn RecordFormatPlugin>) -> Result<Vec<Record>> {
        let stream: Box<dyn ByteStream> = Box::new(FileReader::open(&path).await?);
        let mut reader = ShardReader::load(stream, plugin).await?;
        let mut records = Vec::new();
        while let Some(r) = reader.next_record() {
            records.push(r);
        }
        Ok(records)
    }
}
