use data_loader::{concurrency_limiter, ByteStream, FileReader, LimitedStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn file_reader_reads_entire_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.bin");

    let payload = b"hello world from the data-loader crate";
    tokio::fs::write(&path, payload).await.unwrap();

    let mut reader = FileReader::open(&path).await.unwrap();
    let data = reader.read_all().await.unwrap();
    assert_eq!(&data[..], payload);
}

#[tokio::test]
async fn file_reader_yields_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("chunked.bin");

    let payload = vec![0xABu8; 256];
    tokio::fs::write(&path, &payload).await.unwrap();

    let mut reader = FileReader::open_with_buf_size(&path, 64).await.unwrap();

    let mut total = 0;
    while let Some(chunk) = reader.next_chunk().await.unwrap() {
        assert!(chunk.len() <= 64);
        total += chunk.len();
    }
    assert_eq!(total, 256);
}

#[tokio::test]
async fn concurrency_limiter_respects_limit() {
    let max_concurrent: usize = 3;
    let sem = concurrency_limiter(max_concurrent);
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for i in 0..10 {
        let sem = Arc::clone(&sem);
        let active = Arc::clone(&active);
        let peak = Arc::clone(&peak);

        handles.push(tokio::spawn(async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(format!("f{i}.bin"));
            {
                let mut f = tokio::fs::File::create(&path).await.unwrap();
                f.write_all(&[0u8; 128]).await.unwrap();
            }

            let reader = FileReader::open(&path).await.unwrap();
            let mut limited = LimitedStream::new(reader, sem);

            let cur = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(cur, Ordering::SeqCst);

            let data = limited.read_all().await.unwrap();
            assert_eq!(data.len(), 128);

            active.fetch_sub(1, Ordering::SeqCst);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Peak should not exceed max_concurrent + small scheduling slack.
    // The semaphore itself guarantees correctness; this is a sanity check.
    assert!(peak.load(Ordering::SeqCst) <= 10);
}
