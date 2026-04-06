use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::types::PyList;
use std::path::PathBuf;

use data_loader::{ParallelShardReader, Record, RecordFormat, ShardDescriptor};

/// Synchronous Python iterator over records from parallel shard reading.
///
/// Usage:
///     from distruntime import ShardIterator
///     it = ShardIterator("/data/shards", [0, 1, 2], "csv")
///     for record in it:
///         process(record)
#[pyclass]
struct ShardIterator {
    reader: Option<ParallelShardReader>,
    runtime: tokio::runtime::Runtime,
}

#[pymethods]
impl ShardIterator {
    #[new]
    #[pyo3(signature = (base_dir, shard_indices, format, extension="dat", prefetch=None))]
    fn new(
        base_dir: &str,
        shard_indices: Vec<u64>,
        format: &str,
        extension: &str,
        prefetch: Option<usize>,
    ) -> PyResult<Self> {
        let fmt = RecordFormat::from_str_loose(format).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!("unknown format: {format}"))
        })?;

        let desc = ShardDescriptor {
            base_dir: PathBuf::from(base_dir),
            extension: extension.to_string(),
            shard_indices,
        };

        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let reader = match prefetch {
            Some(p) => {
                runtime.block_on(async { ParallelShardReader::open_with_prefetch(desc, fmt, p) })
            }
            None => runtime.block_on(async { ParallelShardReader::open(desc, fmt) }),
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
        Record::RawBytes(b) => Ok(pyo3::types::PyBytes::new(py, &b).into()),
        Record::CsvRow(fields) => {
            let list = PyList::new(py, fields.iter().map(|s| s.as_str()))?;
            Ok(list.into())
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
