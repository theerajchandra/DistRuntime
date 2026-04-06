use anyhow::Result;
use bytes::Bytes;
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use std::path::PathBuf;
use std::sync::Arc;

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

#[pymodule]
fn distruntime(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ShardIterator>()?;
    Ok(())
}
