use arrow::record_batch::RecordBatch;
use bytes::Bytes;

const DEFAULT_PREFETCH_DEPTH: usize = 64;

/// Supported deserialization formats for shard data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordFormat {
    RawBytes,
    Csv,
    Parquet,
}

impl RecordFormat {
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "raw" | "raw_bytes" | "bytes" => Some(Self::RawBytes),
            "csv" => Some(Self::Csv),
            "parquet" => Some(Self::Parquet),
            _ => None,
        }
    }
}

/// A single record yielded by the shard reader.
#[derive(Debug, Clone)]
pub enum Record {
    RawBytes(Bytes),
    CsvRow(Vec<String>),
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
