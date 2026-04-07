use anyhow::Result;
use bytes::Bytes;
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use checkpoint_engine::{CheckpointMetadata, CheckpointRegistry};

use data_loader::{
    BuiltinFormat, ParallelShardReader, Record, RecordFormatPlugin, ShardDescriptor,
};

/// Python callable used as a custom [`RecordFormatPlugin`].
///
/// The callable receives the full shard as `bytes` and must return a `list`.
/// Each element is either `bytes` (becomes [`Record::RawBytes`]) or any JSON-serializable
/// value (encoded with the standard `json` module and stored as [`Record::JsonValue`]).
struct PyFormatPlugin {
    callable: Py<PyAny>,
}

impl RecordFormatPlugin for PyFormatPlugin {
    fn decode_shard(&self, data: Bytes) -> Result<Vec<Record>> {
        Python::attach(|py| {
            let bytes = PyBytes::new(py, &data);
            let out = self.callable.call1(py, (bytes,))?;
            let list = out
                .cast_bound::<PyList>(py)
                .map_err(|e| anyhow::anyhow!("custom format plugin must return a list: {e}"))?;
            let json = py.import("json")?;
            let mut records = Vec::new();
            for item in list.iter() {
                if let Ok(buf) = item.extract::<Vec<u8>>() {
                    records.push(Record::RawBytes(Bytes::from(buf)));
                } else {
                    let dumped = json.call_method1("dumps", (&item,))?;
                    let s: String = dumped.extract()?;
                    let v: serde_json::Value = serde_json::from_str(&s)?;
                    records.push(Record::JsonValue(v));
                }
            }
            Ok(records)
        })
    }
}

/// Synchronous Python iterator over records from parallel shard reading.
///
/// Built-in formats: `"raw"`, `"parquet"`, `"ndjson"` (also `jsonl`).
///
/// Custom format: pass `plugin=my_callable` where `my_callable(shard: bytes) -> list`.
#[pyclass]
struct ShardIterator {
    reader: Option<ParallelShardReader>,
    runtime: tokio::runtime::Runtime,
}

#[pymethods]
impl ShardIterator {
    #[new]
    #[pyo3(signature = (base_dir, shard_indices, format=None, extension="dat", prefetch=None, plugin=None))]
    fn new(
        base_dir: &str,
        shard_indices: Vec<u64>,
        format: Option<&str>,
        extension: &str,
        prefetch: Option<usize>,
        plugin: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let plugin_arc: Arc<dyn RecordFormatPlugin> = if let Some(p) = plugin {
            Arc::new(PyFormatPlugin { callable: p })
        } else {
            let fmt = format.ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err("format is required when plugin is omitted")
            })?;
            let bf = BuiltinFormat::from_str_loose(fmt).ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!("unknown format: {fmt}"))
            })?;
            Arc::new(bf)
        };

        let desc = ShardDescriptor {
            base_dir: PathBuf::from(base_dir),
            extension: extension.to_string(),
            shard_indices,
        };

        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let reader = match prefetch {
            Some(p) => ParallelShardReader::open_plugin(desc, plugin_arc, p),
            None => {
                ParallelShardReader::open_plugin(desc, plugin_arc, data_loader::prefetch_depth())
            }
        };

        Ok(Self {
            reader: Some(reader),
            runtime,
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("iterator exhausted"))?;

        let record = self.runtime.block_on(reader.next_record());

        match record {
            Some(Ok(rec)) => record_to_python(py, rec),
            Some(Err(e)) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
            None => Err(PyStopIteration::new_err(())),
        }
    }
}

fn record_to_python(py: Python<'_>, record: Record) -> PyResult<Py<PyAny>> {
    match record {
        Record::RawBytes(b) => Ok(PyBytes::new(py, &b).into()),
        Record::JsonValue(v) => {
            let s = serde_json::to_string(&v)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            let json = py.import("json")?;
            let obj = json.getattr("loads")?.call1((s,))?;
            Ok(obj.unbind())
        }
        Record::ParquetBatch(batch) => {
            let num_rows = batch.num_rows();
            let num_cols = batch.num_columns();
            let desc = format!("RecordBatch(rows={num_rows}, cols={num_cols})");
            Ok(desc.into_pyobject(py)?.into())
        }
    }
}

fn metadata_to_dict(py: Python<'_>, m: &CheckpointMetadata) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("version", m.version)?;
    d.set_item("checkpoint_id", &m.checkpoint_id)?;
    d.set_item("job_id", &m.job_id)?;
    d.set_item("epoch", m.epoch)?;
    d.set_item("step", m.step)?;
    d.set_item("committed_at_secs", m.committed_at_secs)?;
    d.set_item("total_bytes", m.total_bytes)?;
    d.set_item("loss", m.loss.into_pyobject(py)?)?;
    d.set_item("config_hash", m.config_hash.as_deref().into_pyobject(py)?)?;
    Ok(d.into())
}

/// Python interface for checkpoint versioning and retention.
///
/// Records committed checkpoints, assigns monotonic version IDs,
/// and enforces a configurable retention policy (keep last N per job).
#[pyclass]
struct CheckpointManager {
    job_id: String,
    inner: Arc<Mutex<CheckpointRegistry>>,
}

#[pymethods]
impl CheckpointManager {
    #[new]
    #[pyo3(signature = (job_id, retention=0))]
    fn new(job_id: String, retention: usize) -> Self {
        Self {
            job_id,
            inner: Arc::new(Mutex::new(CheckpointRegistry::new(retention))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (checkpoint_id, step, epoch, total_bytes, loss=None, config_hash=None))]
    fn record_checkpoint(
        &self,
        py: Python<'_>,
        checkpoint_id: String,
        step: u64,
        epoch: u64,
        total_bytes: u64,
        loss: Option<f64>,
        config_hash: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let mut reg = self.inner.lock().unwrap();
        let meta = reg.record(
            &checkpoint_id,
            &self.job_id,
            epoch,
            step,
            total_bytes,
            loss,
            config_hash,
        );
        reg.apply_retention(&self.job_id);
        metadata_to_dict(py, &meta)
    }

    fn list_checkpoints(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let reg = self.inner.lock().unwrap();
        let entries = reg.list(&self.job_id);
        let list = pyo3::types::PyList::empty(py);
        for m in &entries {
            list.append(metadata_to_dict(py, m)?)?;
        }
        Ok(list.into())
    }

    fn delete_checkpoint(&self, version: u64) -> bool {
        let mut reg = self.inner.lock().unwrap();
        reg.delete(version).is_some()
    }

    fn get_checkpoint(&self, py: Python<'_>, version: u64) -> PyResult<Option<Py<PyAny>>> {
        let reg = self.inner.lock().unwrap();
        match reg.get_by_version(version) {
            Some(m) => Ok(Some(metadata_to_dict(py, &m)?)),
            None => Ok(None),
        }
    }

    fn get_checkpoint_by_step(&self, py: Python<'_>, step: u64) -> PyResult<Option<Py<PyAny>>> {
        let reg = self.inner.lock().unwrap();
        match reg.get_by_step(&self.job_id, step) {
            Some(m) => Ok(Some(metadata_to_dict(py, &m)?)),
            None => Ok(None),
        }
    }

    fn set_retention(&self, n: usize) {
        let mut reg = self.inner.lock().unwrap();
        reg.set_retention(n);
    }

    fn __repr__(&self) -> String {
        let reg = self.inner.lock().unwrap();
        format!(
            "CheckpointManager(job_id={:?}, retention={}, count={})",
            self.job_id,
            reg.retention(),
            reg.list(&self.job_id).len()
        )
    }
}

#[pymodule]
fn distruntime(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ShardIterator>()?;
    m.add_class::<CheckpointManager>()?;
    Ok(())
}
