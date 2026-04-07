"""Distributed training runtime – Python bindings."""

from typing import Any, Iterator, Optional

class ShardIterator:
    """Synchronous Python iterator over records from parallel shard reading.

    Built-in formats: ``"raw"``, ``"parquet"``, ``"ndjson"`` (also ``jsonl``).
    Custom format: pass ``plugin=my_callable`` where
    ``my_callable(shard: bytes) -> list``.
    """

    def __init__(
        self,
        base_dir: str,
        shard_indices: list[int],
        format: Optional[str] = None,
        extension: str = "dat",
        prefetch: Optional[int] = None,
        plugin: Optional[Any] = None,
    ) -> None: ...
    def __iter__(self) -> Iterator[Any]: ...
    def __next__(self) -> Any: ...

class CheckpointManager:
    """Checkpoint versioning and retention manager.

    Records committed checkpoints, assigns monotonic version IDs,
    and enforces a configurable retention policy (keep last N per job).
    """

    def __init__(self, job_id: str, retention: int = 0) -> None: ...
    def record_checkpoint(
        self,
        checkpoint_id: str,
        step: int,
        epoch: int,
        total_bytes: int,
        loss: Optional[float] = None,
        config_hash: Optional[str] = None,
    ) -> dict[str, Any]: ...
    def list_checkpoints(self) -> list[dict[str, Any]]: ...
    def delete_checkpoint(self, version: int) -> bool: ...
    def get_checkpoint(self, version: int) -> Optional[dict[str, Any]]: ...
    def get_checkpoint_by_step(self, step: int) -> Optional[dict[str, Any]]: ...
    def set_retention(self, n: int) -> None: ...

class Runtime:
    """Connection to the DistRuntime coordinator.

    Connects to the coordinator gRPC service, registers this process as a
    worker, and starts a background heartbeat. All blocking network calls
    release the Python GIL so other threads can run.
    """

    def __init__(
        self,
        coordinator_addr: str,
        job_id: str,
        address: str = "127.0.0.1",
        port: int = 0,
    ) -> None: ...
    @property
    def worker_id(self) -> str: ...
    @property
    def job_id(self) -> str: ...
    def recover(self) -> Optional[dict[str, Any]]: ...
    def register_dataset(self, uri: str, num_shards: int, format: str) -> Dataset: ...
    def register_checkpoint(self, storage_path: str, keep_last: int = 0) -> Checkpoint: ...
    def shutdown(self) -> None: ...

class Dataset:
    """Represents a registered dataset with shard assignments.

    Created by ``Runtime.register_dataset()``. Compatible with
    ``torch.utils.data.DataLoader(dataset, num_workers=0)`` as an
    iterable-style dataset.
    """

    @property
    def dataset_id(self) -> str: ...
    @property
    def uri(self) -> str: ...
    @property
    def format(self) -> str: ...
    @property
    def num_shards(self) -> int: ...
    @property
    def shard_indices(self) -> list[int]: ...
    def __len__(self) -> int: ...
    def __iter__(self) -> DatasetIterator: ...
    def batches(self, batch_size: int) -> BatchIterator: ...

class Checkpoint:
    """Async checkpoint save/load with retention and callbacks.

    ``save()`` returns immediately -- the file write runs in a background
    Rust task. Register ``on_save_complete`` / ``on_save_failed`` callbacks
    to be notified.
    """

    def save(
        self,
        state_dict: Any,
        step: int,
        epoch: int = 0,
        loss: Optional[float] = None,
        config_hash: Optional[str] = None,
    ) -> None:
        """Save a state dict asynchronously. Returns in under 100 ms."""
        ...
    def load(self, version: str = "latest") -> Any:
        """Load and unpickle a checkpoint. *version* is ``"latest"`` or a version number."""
        ...
    def on_save_complete(self, callback: Any) -> None:
        """Register a success callback: ``callback(meta_dict)``."""
        ...
    def on_save_failed(self, callback: Any) -> None:
        """Register a failure callback: ``callback(error_message)``."""
        ...
    def wait(self) -> None:
        """Block until all pending saves finish."""
        ...
    def list_checkpoints(self) -> list[dict[str, Any]]: ...

class DatasetIterator:
    """Iterator over individual records from a dataset."""

    def __iter__(self) -> DatasetIterator: ...
    def __next__(self) -> Any: ...

class BatchIterator:
    """Iterator yielding lists of records grouped into batches."""

    def __iter__(self) -> BatchIterator: ...
    def __next__(self) -> list[Any]: ...
