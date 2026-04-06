use coordinator::compute_shard_map;
use std::time::Instant;

fn worker_ids(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("w{i}")).collect()
}

#[test]
fn even_split() {
    let ids = worker_ids(4);
    let map = compute_shard_map(12, &ids).unwrap();

    for id in &ids {
        let ranges = &map[id];
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].end - ranges[0].start, 3);
    }
    assert_covers_all(12, &map);
}

#[test]
fn uneven_split() {
    let ids = worker_ids(3);
    let map = compute_shard_map(10, &ids).unwrap();

    let mut sizes: Vec<u64> = ids
        .iter()
        .map(|id| map[id].iter().map(|r| r.end - r.start).sum())
        .collect();
    sizes.sort();
    assert_eq!(sizes, vec![3, 3, 4]);
    assert_covers_all(10, &map);
}

#[test]
fn single_worker() {
    let ids = worker_ids(1);
    let map = compute_shard_map(100, &ids).unwrap();
    assert_eq!(map["w0"].len(), 1);
    assert_eq!(map["w0"][0].start, 0);
    assert_eq!(map["w0"][0].end, 100);
}

#[test]
fn more_workers_than_shards() {
    let ids = worker_ids(5);
    let map = compute_shard_map(3, &ids).unwrap();

    let total: u64 = ids
        .iter()
        .flat_map(|id| map[id].iter())
        .map(|r| r.end - r.start)
        .sum();
    assert_eq!(total, 3);

    let empty_count = ids.iter().filter(|id| map[*id].is_empty()).count();
    assert_eq!(empty_count, 2);
    assert_covers_all(3, &map);
}

#[test]
fn zero_shards() {
    let ids = worker_ids(3);
    let map = compute_shard_map(0, &ids).unwrap();
    for id in &ids {
        assert!(map[id].is_empty());
    }
}

#[test]
fn zero_workers_errors() {
    let result = compute_shard_map(10, &[]);
    assert!(result.is_err());
}

#[test]
fn one_shard() {
    let ids = worker_ids(4);
    let map = compute_shard_map(1, &ids).unwrap();
    let total: u64 = ids
        .iter()
        .flat_map(|id| map[id].iter())
        .map(|r| r.end - r.start)
        .sum();
    assert_eq!(total, 1);
    assert_covers_all(1, &map);
}

#[test]
fn thousand_shards_under_500ms() {
    let ids = worker_ids(64);
    let start = Instant::now();
    let map = compute_shard_map(1000, &ids).unwrap();
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_millis() < 500,
        "1000-shard map took {}ms, expected < 500ms",
        elapsed.as_millis()
    );
    assert_covers_all(1000, &map);
}

fn assert_covers_all(
    num_shards: u64,
    map: &std::collections::HashMap<String, Vec<proto_gen::distruntime::ShardRange>>,
) {
    let mut covered = vec![false; num_shards as usize];
    for ranges in map.values() {
        for r in ranges {
            for i in r.start..r.end {
                assert!(!covered[i as usize], "shard {i} assigned twice");
                covered[i as usize] = true;
            }
        }
    }
    for (i, &c) in covered.iter().enumerate() {
        assert!(c, "shard {i} not assigned");
    }
}
