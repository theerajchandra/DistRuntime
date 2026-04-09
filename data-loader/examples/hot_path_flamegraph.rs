use anyhow::Result;
use bytes::Bytes;
use data_loader::format_plugin::RecordFormatPlugin;
use data_loader::{BuiltinFormat, Record};
use std::fs::File;
use std::hint::black_box;

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

fn run_legacy_clone_cursor(shard: &Bytes, iterations: usize) -> usize {
    let mut processed = 0usize;
    for _ in 0..iterations {
        let records = decode_raw_records(shard);
        let mut cursor = 0usize;
        while cursor < records.len() {
            black_box(records[cursor].clone());
            cursor += 1;
            processed += 1;
        }
    }
    processed
}

fn run_optimized_owned_iter(shard: &Bytes, iterations: usize) -> usize {
    let mut processed = 0usize;
    for _ in 0..iterations {
        let records = decode_raw_records(shard);
        for record in records {
            black_box(record);
            processed += 1;
        }
    }
    processed
}

fn main() -> Result<()> {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "optimized".to_string());
    let output = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "hot-path.svg".to_string());
    let lines = std::env::var("HOT_PATH_LINES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(30_000);
    let iterations = std::env::var("HOT_PATH_ITERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(300);

    let guard = pprof::ProfilerGuard::new(100)?;
    let shard = make_raw_shard(lines);

    let processed = match mode.as_str() {
        "legacy" => run_legacy_clone_cursor(&shard, iterations),
        "optimized" => run_optimized_owned_iter(&shard, iterations),
        other => {
            anyhow::bail!("unknown mode '{other}', expected 'legacy' or 'optimized'");
        }
    };

    let report = guard.report().build()?;
    let file = File::create(&output)?;
    report.flamegraph(file)?;

    println!("mode={mode} processed_records={processed} flamegraph={output}");
    Ok(())
}
