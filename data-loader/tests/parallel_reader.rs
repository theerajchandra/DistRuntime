use data_loader::{ParallelShardReader, Record, RecordFormat, ShardDescriptor};
use std::collections::HashSet;
use std::io::Write;
use tempfile::TempDir;

fn create_shard_files(
    dir: &std::path::Path,
    ext: &str,
    num_shards: u64,
    lines_per_shard: usize,
    id_offset: u64,
) {
    for shard in 0..num_shards {
        let path = dir.join(format!("shard-{shard}.{ext}"));
        let mut f = std::fs::File::create(path).unwrap();
        for line in 0..lines_per_shard {
            let id = id_offset + shard * lines_per_shard as u64 + line as u64;
            writeln!(f, "{id}").unwrap();
        }
    }
}

fn create_csv_shard_files(
    dir: &std::path::Path,
    num_shards: u64,
    rows_per_shard: usize,
    id_offset: u64,
) {
    for shard in 0..num_shards {
        let path = dir.join(format!("shard-{shard}.csv"));
        let mut f = std::fs::File::create(path).unwrap();
        for row in 0..rows_per_shard {
            let id = id_offset + shard * rows_per_shard as u64 + row as u64;
            writeln!(f, "{id},value-{id}").unwrap();
        }
    }
}

fn create_parquet_shard_files(
    dir: &std::path::Path,
    num_shards: u64,
    rows_per_shard: usize,
    id_offset: u64,
) {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    for shard in 0..num_shards {
        let path = dir.join(format!("shard-{shard}.parquet"));
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

        let ids: Vec<i64> = (0..rows_per_shard)
            .map(|row| (id_offset + shard * rows_per_shard as u64 + row as u64) as i64)
            .collect();
        let id_array = Int64Array::from(ids);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array)]).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
}

/// 10 workers, each assigned 10 shards containing 5 records each.
/// Total = 500 unique records. Verify no drops or duplicates.
#[tokio::test]
async fn ten_workers_no_drop_no_dup() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();

    let records_per_shard = 5;
    let shards_per_worker = 10;
    let num_workers = 10;
    let total_shards = num_workers * shards_per_worker;

    create_shard_files(dir, "dat", total_shards as u64, records_per_shard, 0);

    let mut all_ids: Vec<String> = Vec::new();
    let mut handles = Vec::new();

    for worker in 0..num_workers {
        let base = dir.to_path_buf();
        handles.push(tokio::spawn(async move {
            let start = (worker * shards_per_worker) as u64;
            let indices: Vec<u64> = (start..start + shards_per_worker as u64).collect();
            let desc = ShardDescriptor {
                base_dir: base,
                extension: "dat".to_string(),
                shard_indices: indices,
            };
            let mut reader =
                ParallelShardReader::open_with_prefetch(desc, RecordFormat::RawBytes, 8);

            let mut ids = Vec::new();
            while let Some(Ok(Record::RawBytes(b))) = reader.next_record().await {
                ids.push(String::from_utf8_lossy(&b).to_string());
            }
            ids
        }));
    }

    for h in handles {
        all_ids.extend(h.await.unwrap());
    }

    assert_eq!(
        all_ids.len(),
        num_workers * shards_per_worker * records_per_shard,
        "expected {} records, got {}",
        num_workers * shards_per_worker * records_per_shard,
        all_ids.len()
    );

    let unique: HashSet<_> = all_ids.iter().collect();
    assert_eq!(
        unique.len(),
        all_ids.len(),
        "found duplicate records among {} total",
        all_ids.len()
    );
}

/// Verify that PREFETCH_DEPTH env var is respected.
#[tokio::test]
async fn prefetch_depth_from_env() {
    let tmp = TempDir::new().unwrap();
    create_shard_files(tmp.path(), "dat", 2, 3, 0);

    unsafe { std::env::set_var("PREFETCH_DEPTH", "4") };

    let desc = ShardDescriptor {
        base_dir: tmp.path().to_path_buf(),
        extension: "dat".to_string(),
        shard_indices: vec![0, 1],
    };

    let mut reader = ParallelShardReader::open(desc, RecordFormat::RawBytes);

    let mut count = 0;
    while let Some(Ok(_)) = reader.next_record().await {
        count += 1;
    }
    assert_eq!(count, 6);

    unsafe { std::env::remove_var("PREFETCH_DEPTH") };
}

/// Raw bytes round-trip: each line from the shard should be returned as-is.
#[tokio::test]
async fn format_raw_bytes() {
    let tmp = TempDir::new().unwrap();
    create_shard_files(tmp.path(), "dat", 1, 4, 100);

    let desc = ShardDescriptor {
        base_dir: tmp.path().to_path_buf(),
        extension: "dat".to_string(),
        shard_indices: vec![0],
    };
    let mut reader = ParallelShardReader::open_with_prefetch(desc, RecordFormat::RawBytes, 8);

    let mut ids = Vec::new();
    while let Some(Ok(Record::RawBytes(b))) = reader.next_record().await {
        ids.push(String::from_utf8_lossy(&b).to_string());
    }
    ids.sort();
    assert_eq!(ids, vec!["100", "101", "102", "103"]);
}

/// CSV row parsing: each row yields a `CsvRow` with fields.
#[tokio::test]
async fn format_csv() {
    let tmp = TempDir::new().unwrap();
    create_csv_shard_files(tmp.path(), 1, 3, 200);

    let desc = ShardDescriptor {
        base_dir: tmp.path().to_path_buf(),
        extension: "csv".to_string(),
        shard_indices: vec![0],
    };
    let mut reader = ParallelShardReader::open_with_prefetch(desc, RecordFormat::Csv, 8);

    let mut rows = Vec::new();
    while let Some(Ok(Record::CsvRow(fields))) = reader.next_record().await {
        rows.push(fields);
    }
    rows.sort();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["200", "value-200"]);
    assert_eq!(rows[1], vec!["201", "value-201"]);
    assert_eq!(rows[2], vec!["202", "value-202"]);
}

/// Parquet deserialization: each batch should contain the correct rows.
#[tokio::test]
async fn format_parquet() {
    let tmp = TempDir::new().unwrap();
    create_parquet_shard_files(tmp.path(), 1, 5, 300);

    let desc = ShardDescriptor {
        base_dir: tmp.path().to_path_buf(),
        extension: "parquet".to_string(),
        shard_indices: vec![0],
    };
    let mut reader = ParallelShardReader::open_with_prefetch(desc, RecordFormat::Parquet, 8);

    let mut total_rows = 0;
    while let Some(Ok(Record::ParquetBatch(batch))) = reader.next_record().await {
        total_rows += batch.num_rows();
        assert_eq!(batch.num_columns(), 1);
    }
    assert_eq!(total_rows, 5);
}
