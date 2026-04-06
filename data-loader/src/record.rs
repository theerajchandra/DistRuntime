use arrow::record_batch::RecordBatch;
use bytes::Bytes;

const DEFAULT_PREFETCH_DEPTH: usize = 64;

/// A single record yielded by the shard reader.
#[derive(Debug, Clone)]
pub enum Record {
    RawBytes(Bytes),
    /// One JSON value from a newline-delimited JSON shard.
    JsonValue(serde_json::Value),
    ParquetBatch(RecordBatch),
}

/// Read the prefetch depth from the `PREFETCH_DEPTH` environment variable,
/// falling back to 64 if unset or unparseable.
pub fn prefetch_depth() -> usize {
    std::env::var("PREFETCH_DEPTH")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PREFETCH_DEPTH)
}
