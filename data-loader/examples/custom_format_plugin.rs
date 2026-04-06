//! Working example: register a custom [`data_loader::format_plugin::RecordFormatPlugin`]
//! that splits shard bytes on `||` delimiters.

use bytes::Bytes;
use data_loader::format_plugin::RecordFormatPlugin;
use data_loader::record::Record;
use std::sync::Arc;

#[derive(Debug, Default)]
struct PipeDelimited;

impl RecordFormatPlugin for PipeDelimited {
    fn decode_shard(&self, data: Bytes) -> anyhow::Result<Vec<Record>> {
        let s = std::str::from_utf8(&data)?;
        let records = s
            .split("||")
            .filter(|p| !p.is_empty())
            .map(|p| Record::RawBytes(Bytes::copy_from_slice(p.as_bytes())))
            .collect();
        Ok(records)
    }
}

fn main() -> anyhow::Result<()> {
    let plugin = Arc::new(PipeDelimited);
    let out = plugin.decode_shard(Bytes::from("alpha||beta||gamma"))?;
    assert_eq!(out.len(), 3);
    println!("decoded {} records", out.len());
    Ok(())
}
