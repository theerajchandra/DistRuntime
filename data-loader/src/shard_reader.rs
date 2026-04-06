use anyhow::Result;
use bytes::Bytes;
use std::io::Cursor;

use crate::record::{Record, RecordFormat};
use crate::stream::ByteStream;

/// Format-aware reader that consumes a [`ByteStream`] and yields [`Record`] items.
pub struct ShardReader {
    format: RecordFormat,
    records: Vec<Record>,
    cursor: usize,
}

impl ShardReader {
    /// Read the entire shard from the stream and deserialize into records.
    pub async fn load(mut stream: Box<dyn ByteStream>, format: RecordFormat) -> Result<Self> {
        let raw = stream.read_all().await?;
        let records = match format {
            RecordFormat::RawBytes => decode_raw(raw),
            RecordFormat::Csv => decode_csv(raw)?,
            RecordFormat::Parquet => decode_parquet(raw)?,
        };
        Ok(Self {
            format,
            records,
            cursor: 0,
        })
    }

    pub fn format(&self) -> RecordFormat {
        self.format
    }

    /// Return the next record, or `None` if the shard is exhausted.
    pub fn next_record(&mut self) -> Option<Record> {
        if self.cursor < self.records.len() {
            let r = self.records[self.cursor].clone();
            self.cursor += 1;
            Some(r)
        } else {
            None
        }
    }
}

fn decode_raw(data: Bytes) -> Vec<Record> {
    if data.is_empty() {
        return vec![];
    }
    // Each line (newline-delimited) is one record.
    data.split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| Record::RawBytes(Bytes::copy_from_slice(line)))
        .collect()
}

fn decode_csv(data: Bytes) -> Result<Vec<Record>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(Cursor::new(&data));

    let mut records = Vec::new();
    for result in reader.records() {
        let row = result?;
        records.push(Record::CsvRow(row.iter().map(String::from).collect()));
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
