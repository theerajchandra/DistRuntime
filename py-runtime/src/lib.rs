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

        let reader = {
            let _guard = runtime.enter();
            match prefetch {
                Some(p) => ParallelShardReader::open_plugin(desc, plugin_arc, p),
                None => ParallelShardReader::open_plugin(
                    desc,
                    plugin_arc,
                    data_loader::prefetch_depth(),
                ),
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

    /// Create a checkpoint manager for this job.
    ///
    /// Parameters
    /// ----------
    /// storage_path : str
    ///     Local directory (or S3 prefix) where checkpoint files are stored.
    /// keep_last : int, optional
    ///     Number of recent checkpoints to keep. Older ones are deleted
    ///     automatically. Defaults to 0 (keep all).
    ///
    /// Returns
    /// -------
    /// Checkpoint
    ///     A checkpoint interface bound to this job.
    #[pyo3(signature = (storage_path, keep_last=0))]
    fn register_checkpoint(&self, storage_path: &str, keep_last: usize) -> PyResult<Checkpoint> {
        let tokio_rt = tokio::runtime::Runtime::new()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Ok(Checkpoint {
            job_id: self.job_id.clone(),
            storage_path: PathBuf::from(storage_path),
            registry: Arc::new(Mutex::new(CheckpointRegistry::new(keep_last))),
            tokio_rt,
            on_save_complete_cb: Arc::new(Mutex::new(None)),
            on_save_failed_cb: Arc::new(Mutex::new(None)),
            pending_saves: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
    }

    /// Register a dataset with the coordinator and receive shard assignments.
    ///
    /// Parameters
    /// ----------
    /// uri : str
    ///     Storage URI for the dataset (e.g. "s3://bucket/data/" or "/local/path/").
    /// num_shards : int
    ///     Total number of shards in the dataset.
    /// format : str
    ///     Data format: "parquet", "ndjson", "raw".
    ///
    /// Returns
    /// -------
    /// Dataset
    ///     A dataset object bound to this worker's assigned shards.
    fn register_dataset(
        &self,
        py: Python<'_>,
        uri: &str,
        num_shards: u64,
        format: &str,
    ) -> PyResult<Dataset> {
        let job_id = self.job_id.clone();
        let uri_owned = uri.to_string();
        let format_owned = format.to_string();
        let worker_client = &self.worker_client;
        let tokio_rt = &self.tokio_rt;

        let (dataset_id, shard_indices) = py.detach(|| {
            let mut client = worker_client.lock().unwrap();
            tokio_rt.block_on(async {
                let ds_id = client
                    .register_dataset(&job_id, &uri_owned, num_shards, &format_owned)
                    .await
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

                // Perform a heartbeat to get our shard assignments
                let (_gen, assignments) = client
                    .heartbeat_once()
                    .await
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

                // Find assignments for this dataset
                let mut indices: Vec<u64> = Vec::new();
                for a in &assignments {
                    if a.dataset_id == ds_id {
                        for range in &a.shards {
                            for i in range.start..range.end {
                                indices.push(i);
                            }
                        }
                    }
                }

                Ok::<_, PyErr>((ds_id, indices))
            })
        })?;

        Ok(Dataset {
            dataset_id,
            uri: uri_owned,
            format: format_owned,
            num_shards,
            shard_indices,
        })
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
    shard_indices: Vec<u64>,
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

    /// Shard indices assigned to this worker.
    #[getter]
    fn shard_indices(&self) -> Vec<u64> {
        self.shard_indices.clone()
    }

    /// Return the number of shards assigned to this worker.
    fn __len__(&self) -> usize {
        self.shard_indices.len()
    }

    /// Iterate over individual records from the assigned shards.
    ///
    /// Compatible with ``torch.utils.data.DataLoader(dataset, num_workers=0)``.
    fn __iter__(&self) -> PyResult<DatasetIterator> {
        let fmt = BuiltinFormat::from_str_loose(&self.format).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!("unknown format: {}", self.format))
        })?;

        let ext = format_to_extension(&self.format);

        let desc = ShardDescriptor {
            base_dir: PathBuf::from(&self.uri),
            extension: ext,
            shard_indices: self.shard_indices.clone(),
        };

        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let reader = {
            let _guard = runtime.enter();
            ParallelShardReader::open_plugin(desc, Arc::new(fmt), data_loader::prefetch_depth())
        };

        Ok(DatasetIterator {
            reader: Some(reader),
            runtime,
        })
    }

    /// Iterate over batches of records.
    ///
    /// Parameters
    /// ----------
    /// batch_size : int
    ///     Number of records per batch.
    ///
    /// Returns
    /// -------
    /// BatchIterator
    ///     An iterator yielding lists of records, each list up to ``batch_size``
    ///     elements.
    fn batches(&self, batch_size: usize) -> PyResult<BatchIterator> {
        let iter = self.__iter__()?;
        Ok(BatchIterator {
            inner: iter,
            batch_size,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Dataset(id={:?}, uri={:?}, format={:?}, shards={})",
            self.dataset_id, self.uri, self.format, self.num_shards
        )
    }
}

/// Map format name to file extension.
fn format_to_extension(format: &str) -> String {
    match format.to_lowercase().as_str() {
        "parquet" => "parquet".to_string(),
        "ndjson" | "jsonl" => "jsonl".to_string(),
        "raw" | "raw_bytes" | "bytes" => "dat".to_string(),
        other => other.to_string(),
    }
}

/// Iterator over individual records from a dataset's assigned shards.
#[pyclass]
struct DatasetIterator {
    reader: Option<ParallelShardReader>,
    runtime: tokio::runtime::Runtime,
}

#[pymethods]
impl DatasetIterator {
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

/// Iterator that yields lists of records grouped into batches.
#[pyclass]
struct BatchIterator {
    inner: DatasetIterator,
    batch_size: usize,
}

#[pymethods]
impl BatchIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut batch = Vec::with_capacity(self.batch_size);

        for _ in 0..self.batch_size {
            match self.inner.__next__(py) {
                Ok(item) => batch.push(item),
                Err(e) if e.is_instance_of::<PyStopIteration>(py) => break,
                Err(e) => return Err(e),
            }
        }

        if batch.is_empty() {
            return Err(PyStopIteration::new_err(()));
        }

        let list = PyList::new(py, &batch)?;
        Ok(list.into())
    }
}

/// Async checkpoint save/load interface with retention and callbacks.
///
/// ``save()`` returns immediately (data is pickled synchronously but the
/// file write runs in a background Rust task). Register callbacks with
/// ``on_save_complete()`` and ``on_save_failed()`` to be notified.
///
/// Example
/// -------
/// >>> ckpt = rt.register_checkpoint("./checkpoints", keep_last=5)
/// >>> ckpt.on_save_complete(lambda meta: print("saved", meta["step"]))
/// >>> ckpt.save(model.state_dict(), step=100)
/// >>> state = ckpt.load()
#[pyclass]
struct Checkpoint {
    job_id: String,
    storage_path: PathBuf,
    registry: Arc<Mutex<CheckpointRegistry>>,
    tokio_rt: tokio::runtime::Runtime,
    on_save_complete_cb: Arc<Mutex<Option<Py<PyAny>>>>,
    on_save_failed_cb: Arc<Mutex<Option<Py<PyAny>>>>,
    pending_saves: Arc<std::sync::atomic::AtomicUsize>,
}

#[pymethods]
impl Checkpoint {
    /// Save a state dict to a checkpoint file.
    ///
    /// The state dict is pickled synchronously, then written to disk in a
    /// background task. Returns immediately (typically under 100 ms).
    ///
    /// Parameters
    /// ----------
    /// state_dict : Any
    ///     The object to checkpoint (typically ``model.state_dict()``).
    /// step : int
    ///     Training step number.
    /// epoch : int, optional
    ///     Training epoch. Defaults to 0.
    /// loss : float, optional
    ///     Loss value to record in metadata.
    /// config_hash : str, optional
    ///     Configuration hash to record in metadata.
    #[pyo3(signature = (state_dict, step, epoch=0, loss=None, config_hash=None))]
    fn save(
        &self,
        py: Python<'_>,
        state_dict: Py<PyAny>,
        step: u64,
        epoch: u64,
        loss: Option<f64>,
        config_hash: Option<String>,
    ) -> PyResult<()> {
        // 1. Pickle the state dict (synchronous, GIL held)
        let pickle = py.import("pickle")?;
        let data_bytes: Vec<u8> = pickle.call_method1("dumps", (&state_dict,))?.extract()?;
        let data = Bytes::from(data_bytes);

        // 2. Prepare paths and identifiers
        let file_name = format!("ckpt-{step}.pkl");
        let file_path = self.storage_path.join(&file_name);
        let checkpoint_id = format!("ckpt-{}-{epoch}-{step}", self.job_id);

        // 3. Clone shared state for the spawned task
        let registry = Arc::clone(&self.registry);
        let job_id = self.job_id.clone();
        let on_complete = Arc::clone(&self.on_save_complete_cb);
        let on_failed = Arc::clone(&self.on_save_failed_cb);
        let pending = Arc::clone(&self.pending_saves);
        let storage_path = self.storage_path.clone();

        // 4. Increment pending counter
        pending.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // 5. Spawn background write
        self.tokio_rt.spawn(async move {
            let total_bytes = data.len() as u64;

            // Ensure parent directory exists, then write
            let result = async {
                if let Some(parent) = file_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::write(&file_path, &data).await
            }
            .await;

            match result {
                Ok(()) => {
                    // Record in registry and apply retention
                    let evicted = {
                        let mut reg = registry.lock().unwrap();
                        reg.record(
                            &checkpoint_id,
                            &job_id,
                            epoch,
                            step,
                            total_bytes,
                            loss,
                            config_hash,
                        );
                        reg.apply_retention(&job_id)
                    };

                    // Delete evicted checkpoint files
                    for meta in &evicted {
                        let old_path = storage_path.join(format!("ckpt-{}.pkl", meta.step));
                        let _ = tokio::fs::remove_file(&old_path).await;
                    }

                    // Fire on_save_complete callback
                    Python::attach(|py| {
                        let guard = on_complete.lock().unwrap();
                        if let Some(ref cb) = *guard {
                            let d = PyDict::new(py);
                            let _ = d.set_item("step", step);
                            let _ = d.set_item("checkpoint_id", &checkpoint_id);
                            let _ = d.set_item("total_bytes", total_bytes);
                            let _: PyResult<Py<PyAny>> = cb.call1(py, (d,));
                        }
                    });
                }
                Err(e) => {
                    // Fire on_save_failed callback
                    let err_msg = e.to_string();
                    Python::attach(|py| {
                        let guard = on_failed.lock().unwrap();
                        if let Some(ref cb) = *guard {
                            let _: PyResult<Py<PyAny>> = cb.call1(py, (err_msg.as_str(),));
                        }
                    });
                }
            }

            pending.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        });

        Ok(())
    }

    /// Load a checkpoint and return the unpickled state dict.
    ///
    /// Parameters
    /// ----------
    /// version : str, optional
    ///     ``"latest"`` (default) loads the most recent checkpoint.
    ///     A numeric string (e.g. ``"3"``) loads that specific version.
    ///
    /// Returns
    /// -------
    /// Any
    ///     The unpickled state dict.
    ///
    /// Raises
    /// ------
    /// FileNotFoundError
    ///     If no checkpoint is found.
    #[pyo3(signature = (version="latest"))]
    fn load(&self, py: Python<'_>, version: &str) -> PyResult<Py<PyAny>> {
        let meta = {
            let reg = self.registry.lock().unwrap();
            if version == "latest" {
                reg.latest_for_job(&self.job_id)
            } else {
                let ver: u64 = version.parse().map_err(|_| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "version must be 'latest' or a number, got: {version}"
                    ))
                })?;
                reg.get_by_version(ver)
            }
        };

        let meta = meta
            .ok_or_else(|| pyo3::exceptions::PyFileNotFoundError::new_err("no checkpoint found"))?;

        let file_path = self.storage_path.join(format!("ckpt-{}.pkl", meta.step));

        // Read file (release GIL during I/O)
        let tokio_rt = &self.tokio_rt;
        let data = py.detach(|| {
            tokio_rt.block_on(async {
                tokio::fs::read(&file_path)
                    .await
                    .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
            })
        })?;

        // Unpickle
        let pickle = py.import("pickle")?;
        let obj = pickle.call_method1("loads", (PyBytes::new(py, &data),))?;
        Ok(obj.unbind())
    }

    /// Register a callback invoked after each successful save.
    ///
    /// The callback receives a single ``dict`` argument with keys
    /// ``step``, ``checkpoint_id``, and ``total_bytes``.
    fn on_save_complete(&self, callback: Py<PyAny>) {
        *self.on_save_complete_cb.lock().unwrap() = Some(callback);
    }

    /// Register a callback invoked when a save fails.
    ///
    /// The callback receives a single ``str`` argument with the error message.
    fn on_save_failed(&self, callback: Py<PyAny>) {
        *self.on_save_failed_cb.lock().unwrap() = Some(callback);
    }

    /// Block until all pending background saves have finished.
    ///
    /// Useful before exiting to ensure all checkpoints are flushed.
    fn wait(&self, py: Python<'_>) {
        let pending = &self.pending_saves;
        py.detach(|| {
            while pending.load(std::sync::atomic::Ordering::SeqCst) > 0 {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        });
    }

    /// List all committed checkpoints, sorted by version.
    fn list_checkpoints(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let reg = self.registry.lock().unwrap();
        let entries = reg.list(&self.job_id);
        let list = PyList::empty(py);
        for m in &entries {
            list.append(metadata_to_dict(py, m)?)?;
        }
        Ok(list.into())
    }

    fn __repr__(&self) -> String {
        let reg = self.registry.lock().unwrap();
        let count = reg.list(&self.job_id).len();
        let pending = self.pending_saves.load(std::sync::atomic::Ordering::SeqCst);
        format!(
            "Checkpoint(job_id={:?}, path={:?}, saved={}, pending={})",
            self.job_id,
            self.storage_path.display(),
            count,
            pending
        )
    }
}

#[pymodule]
fn distruntime(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ShardIterator>()?;
    m.add_class::<CheckpointManager>()?;
    m.add_class::<Runtime>()?;
    m.add_class::<Dataset>()?;
    m.add_class::<DatasetIterator>()?;
    m.add_class::<BatchIterator>()?;
    m.add_class::<Checkpoint>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::exceptions::PyStopIteration;
    use std::collections::HashMap;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn write_sample_jsonl_shards() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::temp_dir().join(format!("distruntime-py-runtime-{unique}"));
        fs::create_dir_all(&base).unwrap();
        fs::write(base.join("shard-0.jsonl"), b"{\"x\":1}\n{\"x\":2}\n").unwrap();
        fs::write(base.join("shard-1.jsonl"), b"{\"x\":3}\n{\"x\":4}\n").unwrap();
        base
    }

    fn ensure_python() {
        Python::initialize();
    }

    fn collect_dataset_values(mut iter: DatasetIterator) -> Vec<i64> {
        Python::attach(|py| {
            let mut values = Vec::new();
            loop {
                match iter.__next__(py) {
                    Ok(obj) => {
                        let row: HashMap<String, i64> = obj.bind(py).extract().unwrap();
                        values.push(*row.get("x").unwrap());
                    }
                    Err(err) if err.is_instance_of::<PyStopIteration>(py) => break,
                    Err(err) => panic!("unexpected python error: {err}"),
                }
            }
            values
        })
    }

    fn collect_shard_values(mut iter: ShardIterator) -> Vec<i64> {
        Python::attach(|py| {
            let mut values = Vec::new();
            loop {
                match iter.__next__(py) {
                    Ok(obj) => {
                        let row: HashMap<String, i64> = obj.bind(py).extract().unwrap();
                        values.push(*row.get("x").unwrap());
                    }
                    Err(err) if err.is_instance_of::<PyStopIteration>(py) => break,
                    Err(err) => panic!("unexpected python error: {err}"),
                }
            }
            values
        })
    }

    #[test]
    fn dataset_iterator_reads_without_external_tokio_context() {
        ensure_python();
        let base = write_sample_jsonl_shards();
        let dataset = Dataset {
            dataset_id: "ds-test".to_string(),
            uri: base.display().to_string(),
            format: "jsonl".to_string(),
            num_shards: 2,
            shard_indices: vec![0, 1],
        };

        let mut values = collect_dataset_values(dataset.__iter__().unwrap());
        values.sort_unstable();
        assert_eq!(values, vec![1, 2, 3, 4]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn shard_iterator_reads_without_external_tokio_context() {
        ensure_python();
        let base = write_sample_jsonl_shards();
        let mut values = collect_shard_values(
            ShardIterator::new(
                base.to_str().unwrap(),
                vec![0, 1],
                Some("jsonl"),
                "jsonl",
                Some(2),
                None,
            )
            .unwrap(),
        );
        values.sort_unstable();
        assert_eq!(values, vec![1, 2, 3, 4]);

        fs::remove_dir_all(base).unwrap();
    }
}
