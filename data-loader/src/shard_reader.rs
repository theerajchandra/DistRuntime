use anyhow::Result;
use std::sync::Arc;

use crate::format_plugin::RecordFormatPlugin;
use crate::record::Record;
use crate::stream::ByteStream;

/// Format-aware reader that consumes a [`ByteStream`] and yields [`Record`] items.
pub struct ShardReader {
    records: std::vec::IntoIter<Record>,
}

impl ShardReader {
    /// Read the entire shard from the stream and deserialize into records.
    pub async fn load(
        mut stream: Box<dyn ByteStream>,
        plugin: Arc<dyn RecordFormatPlugin>,
    ) -> Result<Self> {
        let raw = stream.read_all().await?;
        let records = plugin.decode_shard(raw)?;
        Ok(Self {
            records: records.into_iter(),
        })
    }

    /// Convenience for built-in formats.
    pub async fn load_builtin(
        stream: Box<dyn ByteStream>,
        format: crate::format_plugin::BuiltinFormat,
    ) -> Result<Self> {
        Self::load(stream, Arc::new(format)).await
    }

    /// Return the next record, or `None` if the shard is exhausted.
    pub fn next_record(&mut self) -> Option<Record> {
        self.records.next()
    }
}
