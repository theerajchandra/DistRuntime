use std::collections::HashMap;

// ── Metadata ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CheckpointMetadata {
    pub version: u64,
    pub checkpoint_id: String,
    pub job_id: String,
    pub epoch: u64,
    pub step: u64,
    pub committed_at_secs: u64,
    pub total_bytes: u64,
    pub loss: Option<f64>,
    pub config_hash: Option<String>,
}

// ── Registry ──────────────────────────────────────────────────────────────────

pub struct CheckpointRegistry {
    entries: HashMap<u64, CheckpointMetadata>,
    next_version: u64,
    retention: usize, // keep last N per job; 0 = unlimited
}

impl CheckpointRegistry {
    pub fn new(retention: usize) -> Self {
        Self {
            entries: HashMap::new(),
            next_version: 1,
            retention,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &mut self,
        checkpoint_id: &str,
        job_id: &str,
        epoch: u64,
        step: u64,
        total_bytes: u64,
        loss: Option<f64>,
        config_hash: Option<String>,
    ) -> CheckpointMetadata {
        let version = self.next_version;
        self.next_version += 1;

        let committed_at_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let meta = CheckpointMetadata {
            version,
            checkpoint_id: checkpoint_id.to_string(),
            job_id: job_id.to_string(),
            epoch,
            step,
            committed_at_secs,
            total_bytes,
            loss,
            config_hash,
        };

        self.entries.insert(version, meta.clone());
        meta
    }

    pub fn apply_retention(&mut self, job_id: &str) -> Vec<CheckpointMetadata> {
        if self.retention == 0 {
            return vec![];
        }

        let mut versions: Vec<u64> = self
            .entries
            .values()
            .filter(|m| m.job_id == job_id)
            .map(|m| m.version)
            .collect();
        versions.sort_unstable();

        let evict_count = versions.len().saturating_sub(self.retention);
        versions[..evict_count]
            .iter()
            .filter_map(|v| self.entries.remove(v))
            .collect()
    }

    pub fn list(&self, job_id: &str) -> Vec<CheckpointMetadata> {
        let mut result: Vec<CheckpointMetadata> = self
            .entries
            .values()
            .filter(|m| m.job_id == job_id)
            .cloned()
            .collect();
        result.sort_by_key(|m| m.version);
        result
    }

    pub fn get_by_version(&self, version: u64) -> Option<CheckpointMetadata> {
        self.entries.get(&version).cloned()
    }

    pub fn get_by_step(&self, job_id: &str, step: u64) -> Option<CheckpointMetadata> {
        self.entries
            .values()
            .find(|m| m.job_id == job_id && m.step == step)
            .cloned()
    }

    pub fn delete(&mut self, version: u64) -> Option<CheckpointMetadata> {
        self.entries.remove(&version)
    }

    pub fn retention(&self) -> usize {
        self.retention
    }

    pub fn set_retention(&mut self, n: usize) {
        self.retention = n;
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_meta(reg: &mut CheckpointRegistry, job_id: &str, step: u64) -> CheckpointMetadata {
        reg.record(
            &format!("ckpt-{job_id}-{step}"),
            job_id,
            0,
            step,
            1024,
            None,
            None,
        )
    }

    #[test]
    fn record_assigns_monotonic_versions() {
        let mut reg = CheckpointRegistry::new(0);
        let m1 = make_meta(&mut reg, "job-a", 1);
        let m2 = make_meta(&mut reg, "job-a", 2);
        let m3 = make_meta(&mut reg, "job-a", 3);
        assert_eq!(m1.version, 1);
        assert_eq!(m2.version, 2);
        assert_eq!(m3.version, 3);
    }

    #[test]
    fn list_returns_sorted_ascending() {
        let mut reg = CheckpointRegistry::new(0);
        make_meta(&mut reg, "job-a", 10);
        make_meta(&mut reg, "job-a", 30);
        make_meta(&mut reg, "job-a", 20);
        let list = reg.list("job-a");
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].version, 1);
        assert_eq!(list[1].version, 2);
        assert_eq!(list[2].version, 3);
    }

    #[test]
    fn retention_trims_oldest_ten_to_five() {
        let mut reg = CheckpointRegistry::new(5);
        for s in 0..10 {
            make_meta(&mut reg, "job-a", s);
        }
        let evicted = reg.apply_retention("job-a");
        assert_eq!(evicted.len(), 5);
        assert_eq!(reg.list("job-a").len(), 5);
    }

    #[test]
    fn retention_zero_keeps_all() {
        let mut reg = CheckpointRegistry::new(0);
        for s in 0..10 {
            make_meta(&mut reg, "job-a", s);
        }
        let evicted = reg.apply_retention("job-a");
        assert!(evicted.is_empty());
        assert_eq!(reg.list("job-a").len(), 10);
    }

    #[test]
    fn get_by_version_found_and_not_found() {
        let mut reg = CheckpointRegistry::new(0);
        let m = make_meta(&mut reg, "job-a", 5);
        assert!(reg.get_by_version(m.version).is_some());
        assert!(reg.get_by_version(9999).is_none());
    }

    #[test]
    fn get_by_step_returns_correct_entry() {
        let mut reg = CheckpointRegistry::new(0);
        make_meta(&mut reg, "job-a", 10);
        make_meta(&mut reg, "job-a", 20);
        let found = reg.get_by_step("job-a", 20).expect("should find step 20");
        assert_eq!(found.step, 20);
    }

    #[test]
    fn get_by_step_filters_by_job_id() {
        let mut reg = CheckpointRegistry::new(0);
        make_meta(&mut reg, "job-a", 42);
        make_meta(&mut reg, "job-b", 42);
        let a = reg
            .get_by_step("job-a", 42)
            .expect("job-a step 42 should exist");
        assert_eq!(a.job_id, "job-a");
        let b = reg
            .get_by_step("job-b", 42)
            .expect("job-b step 42 should exist");
        assert_eq!(b.job_id, "job-b");
    }

    #[test]
    fn delete_removes_and_returns_entry() {
        let mut reg = CheckpointRegistry::new(0);
        let m = make_meta(&mut reg, "job-a", 1);
        let removed = reg.delete(m.version).expect("should return the entry");
        assert_eq!(removed.version, m.version);
        assert!(reg.get_by_version(m.version).is_none());
    }

    #[test]
    fn list_filters_by_job() {
        let mut reg = CheckpointRegistry::new(0);
        make_meta(&mut reg, "job-a", 1);
        make_meta(&mut reg, "job-b", 2);
        make_meta(&mut reg, "job-a", 3);
        let list_a = reg.list("job-a");
        assert_eq!(list_a.len(), 2);
        assert!(list_a.iter().all(|m| m.job_id == "job-a"));
    }
}
