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

use worker_client::WorkerClient;

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

/// Connection to the DistRuntime coordinator.
///
/// Connects to the coordinator gRPC service, registers this process as a
/// worker, and starts a background heartbeat. All blocking network calls
/// release the Python GIL so other threads can run.
///
/// Example
/// -------
/// >>> rt = Runtime("grpc://localhost:50051", "my-training-job")
/// >>> print(rt.worker_id)
#[pyclass]
struct Runtime {
    worker_client: std::sync::Mutex<WorkerClient>,
    tokio_rt: tokio::runtime::Runtime,
    worker_id: String,
    job_id: String,
    heartbeat_abort: Option<tokio::task::AbortHandle>,
}

#[pymethods]
impl Runtime {
    /// Create a new Runtime connected to the coordinator.
    ///
    /// Parameters
    /// ----------
    /// coordinator_addr : str
    ///     gRPC address of the coordinator (e.g. "http://localhost:50051").
    /// job_id : str
    ///     Unique identifier for this training job.
    /// address : str, optional
    ///     Address to advertise for this worker. Defaults to "127.0.0.1".
    /// port : int, optional
    ///     Port to advertise. Defaults to 0.
    #[new]
    #[pyo3(signature = (coordinator_addr, job_id, address="127.0.0.1", port=0))]
    fn new(
        py: Python<'_>,
        coordinator_addr: &str,
        job_id: String,
        address: &str,
        port: u32,
    ) -> PyResult<Self> {
        let tokio_rt = tokio::runtime::Runtime::new()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let coordinator_addr = coordinator_addr.to_string();
        let address = address.to_string();
        let (client, worker_id) = py.detach(|| {
            tokio_rt.block_on(async {
                let mut c = WorkerClient::connect(&coordinator_addr)
                    .await
                    .map_err(|e| pyo3::exceptions::PyConnectionError::new_err(e.to_string()))?;
                let wid = c
                    .register(&address, port)
                    .await
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                Ok::<_, PyErr>((c, wid))
            })
        })?;

        // start_heartbeat uses tokio::spawn, so we need a runtime context.
        let abort = {
            let _guard = tokio_rt.enter();
            let (_handle, abort) = client.start_heartbeat();
            abort
        };

        Ok(Self {
            worker_client: std::sync::Mutex::new(client),
            tokio_rt,
            worker_id,
            job_id,
            heartbeat_abort: Some(abort),
        })
    }

    /// The unique worker ID assigned by the coordinator.
    #[getter]
    fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// The job ID for this runtime.
    #[getter]
    fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Attempt to resume from the latest checkpoint for this job.
    ///
    /// Returns a dict with ``checkpoint_path`` and ``shards`` if a checkpoint
    /// exists, or ``None`` otherwise. Releases the GIL during the RPC call.
    fn recover(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let job_id = self.job_id.clone();
        let worker_client = &self.worker_client;
        let tokio_rt = &self.tokio_rt;

        let result = py.detach(|| {
            let mut client = worker_client.lock().unwrap();
            tokio_rt.block_on(async {
                client
                    .recover(&job_id)
                    .await
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
            })
        })?;

        match result {
            None => Ok(None),
            Some((path, shards)) => {
                let d = PyDict::new(py);
                d.set_item("checkpoint_path", path)?;
                let shard_list = PyList::empty(py);
                for s in &shards {
                    let sd = PyDict::new(py);
                    sd.set_item("start", s.start)?;
                    sd.set_item("end", s.end)?;
                    shard_list.append(sd)?;
                }
                d.set_item("shards", shard_list)?;
                Ok(Some(d.into()))
            }
        }
    }

    /// Stop the background heartbeat and release coordinator resources.
    fn shutdown(&mut self) {
        if let Some(abort) = self.heartbeat_abort.take() {
            abort.abort();
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Runtime(job_id={:?}, worker_id={:?})",
            self.job_id, self.worker_id
        )
    }

    fn __del__(&mut self) {
        self.shutdown();
    }
}

/// Represents a registered dataset with shard assignments.
///
/// Created by ``Runtime.register_dataset()``. Iterate over batches with
/// ``batches(batch_size)`` (implemented in DIST-18).
#[pyclass(skip_from_py_object)]
#[derive(Clone)]
struct Dataset {
    dataset_id: String,
    uri: String,
    format: String,
    num_shards: u64,
}

#[pymethods]
impl Dataset {
    /// The unique dataset identifier assigned by the coordinator.
    #[getter]
    fn dataset_id(&self) -> &str {
        &self.dataset_id
    }

    /// The storage URI for this dataset.
    #[getter]
    fn uri(&self) -> &str {
        &self.uri
    }

    /// The data format (e.g. "parquet", "ndjson").
    #[getter]
    fn format(&self) -> &str {
        &self.format
    }

    /// Total number of shards in this dataset.
    #[getter]
    fn num_shards(&self) -> u64 {
        self.num_shards
    }

    fn __repr__(&self) -> String {
        format!(
            "Dataset(id={:?}, uri={:?}, format={:?}, shards={})",
            self.dataset_id, self.uri, self.format, self.num_shards
        )
    }
}

#[pymodule]
fn distruntime(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ShardIterator>()?;
    m.add_class::<CheckpointManager>()?;
    m.add_class::<Runtime>()?;
    m.add_class::<Dataset>()?;
    Ok(())
}
