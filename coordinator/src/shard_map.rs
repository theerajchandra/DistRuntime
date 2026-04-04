use std::collections::HashMap;

use proto_gen::distruntime::ShardRange;

/// Divide `[0, num_shards)` into contiguous, non-overlapping ranges across workers.
///
/// Each worker gets `base` shards, and the first `remainder` workers get one extra.
/// Returns an error if `worker_ids` is empty.
pub fn compute_shard_map(
    num_shards: u64,
    worker_ids: &[String],
) -> Result<HashMap<String, Vec<ShardRange>>, ComputeError> {
    if worker_ids.is_empty() {
        return Err(ComputeError::NoWorkers);
    }

    let n_workers = worker_ids.len() as u64;
    let mut result: HashMap<String, Vec<ShardRange>> = HashMap::new();

    if num_shards == 0 {
        for id in worker_ids {
            result.insert(id.clone(), vec![]);
        }
        return Ok(result);
    }

    let base = num_shards / n_workers;
    let remainder = num_shards % n_workers;
    let mut cursor: u64 = 0;

    for (i, id) in worker_ids.iter().enumerate() {
        let extra = if (i as u64) < remainder { 1 } else { 0 };
        let count = base + extra;

        if count == 0 {
            result.insert(id.clone(), vec![]);
        } else {
            let range = ShardRange {
                start: cursor,
                end: cursor + count,
            };
            cursor += count;
            result.insert(id.clone(), vec![range]);
        }
    }

    Ok(result)
}

#[derive(Debug, thiserror::Error)]
pub enum ComputeError {
    #[error("cannot compute shard map with zero workers")]
    NoWorkers,
}
