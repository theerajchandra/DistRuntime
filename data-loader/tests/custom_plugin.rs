use bytes::Bytes;
use data_loader::format_plugin::RecordFormatPlugin;
use data_loader::record::Record;

/// Example custom plugin: fixed-width records of 4 bytes each.
#[derive(Debug, Default)]
struct FixedWidth4;

impl RecordFormatPlugin for FixedWidth4 {
    fn decode_shard(&self, data: Bytes) -> anyhow::Result<Vec<Record>> {
        let mut out = Vec::new();
        for chunk in data.chunks(4) {
            if !chunk.is_empty() {
                out.push(Record::RawBytes(Bytes::copy_from_slice(chunk)));
            }
        }
        Ok(out)
    }
}

#[test]
fn custom_rust_plugin_fixed_width() {
    let plugin = FixedWidth4;
    let records = plugin.decode_shard(Bytes::from("abcdefgh")).unwrap();
    assert_eq!(records.len(), 2);
    match &records[0] {
        Record::RawBytes(b) => assert_eq!(&b[..], b"abcd"),
        _ => panic!("expected RawBytes"),
    }
    match &records[1] {
        Record::RawBytes(b) => assert_eq!(&b[..], b"efgh"),
        _ => panic!("expected RawBytes"),
    }
}
