//! Pluggable record deserialization for shard bytes.
//!
//! Built-in formats: [`BuiltinFormat`] (raw bytes, Parquet, newline-delimited JSON).
//! Custom logic implements [`RecordFormatPlugin`] and can be composed with
//! [`std::sync::Arc`] for use with [`crate::shard_reader::ShardReader`] and
//! [`crate::parallel_reader::ParallelShardReader`].
//!
//! # Example: custom delimiter-separated records
//!
//! ```
//! use bytes::Bytes;
//! use data_loader::format_plugin::RecordFormatPlugin;
//! use data_loader::record::Record;
//! use std::sync::Arc;
//!
//! /// Treats the shard as UTF-8 and splits on `||` into raw byte records.
//! #[derive(Debug, Default)]
//! struct PipeDelimited;
//!
//! impl RecordFormatPlugin for PipeDelimited {
//!     fn decode_shard(&self, data: Bytes) -> anyhow::Result<Vec<Record>> {
//!         let s = std::str::from_utf8(&data)?;
//!         let records = s
//!             .split("||")
//!             .filter(|p| !p.is_empty())
//!             .map(|p| Record::RawBytes(Bytes::copy_from_slice(p.as_bytes())))
//!             .collect();
//!         Ok(records)
//!     }
//! }
//!
//! let plugin = Arc::new(PipeDelimited);
//! let out = plugin.decode_shard(Bytes::from("a||b||c")).unwrap();
//! assert_eq!(out.len(), 3);
//! ```

use anyhow::Result;
use bytes::Bytes;

use crate::record::Record;

/// Deserializes an entire shard (byte buffer) into a sequence of [`Record`] values.
pub trait RecordFormatPlugin: Send + Sync {
    fn decode_shard(&self, data: Bytes) -> Result<Vec<Record>>;
}

/// Built-in formats required by the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinFormat {
    RawBytes,
    Parquet,
    Ndjson,
}

impl BuiltinFormat {
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "raw" | "raw_bytes" | "bytes" => Some(Self::RawBytes),
            "parquet" => Some(Self::Parquet),
            "ndjson" | "jsonl" | "jsonlines" | "newline_json" => Some(Self::Ndjson),
            _ => None,
        }
    }
}

impl RecordFormatPlugin for BuiltinFormat {
    fn decode_shard(&self, data: Bytes) -> Result<Vec<Record>> {
        match self {
            Self::RawBytes => decode_raw_bytes(data),
            Self::Parquet => decode_parquet(data),
            Self::Ndjson => decode_ndjson(data),
        }
    }
}

fn decode_raw_bytes(data: Bytes) -> Result<Vec<Record>> {
    if data.is_empty() {
        return Ok(vec![]);
    }
    let records = data
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| Record::RawBytes(Bytes::copy_from_slice(line)))
        .collect();
    Ok(records)
}

fn decode_ndjson(data: Bytes) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    for line in data.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_slice(line)?;
        records.push(Record::JsonValue(v));
    }
    Ok(records)
}

fn decode_parquet(data: Bytes) -> Result<Vec<Record>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let reader = ParquetRecordBatchReaderBuilder::try_new(data)?.build()?;
    let mut records = Vec::new();
    for batch in reader {
        records.push(Record::ParquetBatch(batch?));
    }
    Ok(records)
}
