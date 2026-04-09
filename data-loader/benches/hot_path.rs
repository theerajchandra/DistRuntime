use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use data_loader::format_plugin::RecordFormatPlugin;
use data_loader::{BuiltinFormat, ByteStream, FileReader, Record};
use std::path::Path;
use tokio::io::AsyncReadExt;

#[cfg(not(target_os = "windows"))]
use pprof::criterion::{Output, PProfProfiler};

const RAW_RECORDS_PER_SHARD: usize = 20_000;
const IO_BENCH_BYTES: usize = 16 * 1024 * 1024;
const BUF_SIZE: usize = 64 * 1024;

fn make_raw_shard(lines: usize) -> Bytes {
    let mut text = String::with_capacity(lines * 10);
    for i in 0..lines {
        text.push_str(&i.to_string());
        text.push('\n');
    }
    Bytes::from(text)
}

fn decode_raw_records(shard: &Bytes) -> Vec<Record> {
    BuiltinFormat::RawBytes.decode_shard(shard.clone()).unwrap()
}

fn drain_with_clone(records: &[Record]) -> usize {
    let mut count = 0usize;
    let mut cursor = 0usize;
    while cursor < records.len() {
        black_box(records[cursor].clone());
        cursor += 1;
        count += 1;
    }
    count
}

fn drain_owned(records: Vec<Record>) -> usize {
    let mut count = 0usize;
    for record in records {
        black_box(record);
        count += 1;
    }
    count
}

struct LegacyFileReader {
    reader: tokio::io::BufReader<tokio::fs::File>,
    buf_size: usize,
}

impl LegacyFileReader {
    async fn open(path: impl AsRef<Path>, buf_size: usize) -> Result<Self> {
        let file = tokio::fs::File::open(path.as_ref()).await?;
        Ok(Self {
            reader: tokio::io::BufReader::with_capacity(buf_size, file),
            buf_size,
        })
    }
}

#[async_trait]
impl ByteStream for LegacyFileReader {
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

async fn read_all_legacy(path: &Path) -> usize {
    let mut reader = LegacyFileReader::open(path, BUF_SIZE).await.unwrap();
    let mut total = 0usize;
    while let Some(chunk) = reader.next_chunk().await.unwrap() {
        total += chunk.len();
    }
    total
}

async fn read_all_optimized(path: &Path) -> usize {
    let mut reader = FileReader::open_with_buf_size(path, BUF_SIZE)
        .await
        .unwrap();
    let mut total = 0usize;
    while let Some(chunk) = reader.next_chunk().await.unwrap() {
        total += chunk.len();
    }
    total
}

fn bench_shard_reader(c: &mut Criterion) {
    let shard = make_raw_shard(RAW_RECORDS_PER_SHARD);
    let mut group = c.benchmark_group("shard_reader_record_drain");
    group.throughput(Throughput::Elements(RAW_RECORDS_PER_SHARD as u64));

    group.bench_function("legacy_clone_cursor", |b| {
        b.iter(|| {
            let records = decode_raw_records(black_box(&shard));
            black_box(drain_with_clone(&records))
        })
    });

    group.bench_function("optimized_owned_iter", |b| {
        b.iter(|| {
            let records = decode_raw_records(black_box(&shard));
            black_box(drain_owned(records))
        })
    });

    group.finish();
}

fn bench_file_reader(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bench-shard.dat");
    let payload = vec![b'x'; IO_BENCH_BYTES];
    std::fs::write(&path, payload).unwrap();

    let mut group = c.benchmark_group("file_reader_chunk_overhead");
    group.throughput(Throughput::Bytes(IO_BENCH_BYTES as u64));

    group.bench_function("legacy_alloc_per_chunk", |b| {
        b.iter(|| black_box(runtime.block_on(read_all_legacy(&path))))
    });

    group.bench_function("optimized_scratch_reuse", |b| {
        b.iter(|| black_box(runtime.block_on(read_all_optimized(&path))))
    });

    group.finish();
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(20)
        .measurement_time(std::time::Duration::from_secs(5))
        .without_plots()
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_shard_reader, bench_file_reader
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(20)
        .measurement_time(std::time::Duration::from_secs(5))
        .without_plots();
    targets = bench_shard_reader, bench_file_reader
}

criterion_main!(benches);
