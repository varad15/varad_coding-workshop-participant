"""
S3 data lake adapter (works against AWS S3 and LocalStack unchanged).

Why not Spark's s3a:// connector? s3a needs hadoop-aws + AWS SDK JARs
downloaded from Maven at start-up and pinned to the exact Hadoop version
inside PySpark. That is fragile on the VDI and in EKS pods, and the repo's
own job skeleton already shipped a mismatched version. This adapter uses
PyArrow's native S3 client instead; data moves into Spark as Arrow tables
(zero-copy columnar), so Spark still does all transformations.

Trade-off (documented in docs/limitations.md): data passes through the Spark
driver, which suits this ~14 MB dataset. For large volumes, swap this module
for s3a:// reads/writes; no other module needs to change.

Write semantics are idempotent by design:
* unpartitioned writes replace everything under the prefix;
* partitioned writes replace only the partitions present in the new data
  (equivalent to Spark's dynamic partition overwrite).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import boto3
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from common.config import Settings
from common.logger import get_logger
from common.retry import with_retry

log = get_logger("storage")

# Network-level failures worth retrying. 4xx ClientErrors (e.g. AccessDenied)
# are not transient and fail fast.
TRANSIENT_S3_ERRORS: tuple[type[BaseException], ...] = (BotoCoreError, ConnectionError, TimeoutError)


@dataclass(frozen=True)
class ObjectInfo:
    """A single object in the bucket."""

    key: str
    size: int


class DataLake:
    """Thin, retrying wrapper around one S3 bucket."""

    def __init__(self, settings: Settings) -> None:
        """Create boto3 and PyArrow clients for the configured bucket/endpoint."""
        self.bucket = settings.s3.bucket
        self._is_local = settings.is_local
        self._attempts = settings.retry_attempts
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.s3.endpoint_url,
            region_name=settings.s3.region,
            config=Config(
                retries={"max_attempts": 3, "mode": "standard"},
                s3={"addressing_style": "path"},
                max_pool_connections=32,
            ),
        )
        fs_kwargs: dict[str, object] = {"region": settings.s3.region}
        if settings.s3.endpoint_url:
            fs_kwargs["endpoint_override"] = settings.s3.endpoint_url
            fs_kwargs["scheme"] = settings.s3.scheme
        self.fs = pafs.S3FileSystem(**fs_kwargs)

    # ------------------------------------------------------------------ paths
    def uri(self, key: str) -> str:
        """Return the s3:// URI for a key (used in lineage records)."""
        return f"s3://{self.bucket}/{key.lstrip('/')}"

    def _path(self, key: str) -> str:
        """Return the PyArrow path for a key (bucket/key)."""
        return f"{self.bucket}/{key.strip('/')}"

    # ----------------------------------------------------------------- bucket
    def ensure_bucket(self) -> None:
        """
        Make sure the bucket exists.

        Locally (LocalStack) the bucket is created on demand. In the cloud the
        bucket is owned by Terraform, so a missing bucket is a hard error.
        """
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in {"404", "NoSuchBucket", "NotFound"}:
                raise
        if not self._is_local:
            raise RuntimeError(f"Bucket {self.bucket} does not exist; deploy infra/s3.tf first")
        self.client.create_bucket(Bucket=self.bucket)
        log.info("bucket_created", extra={"bucket": self.bucket})

    # ---------------------------------------------------------------- objects
    def list_objects(self, prefix: str) -> list[ObjectInfo]:
        """List all objects under a prefix (handles pagination)."""

        @with_retry(TRANSIENT_S3_ERRORS, attempts=self._attempts)
        def _list() -> list[ObjectInfo]:
            paginator = self.client.get_paginator("list_objects_v2")
            found: list[ObjectInfo] = []
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                found.extend(ObjectInfo(o["Key"], o["Size"]) for o in page.get("Contents", []))
            return found

        return _list()

    def exists(self, prefix: str) -> bool:
        """Return True if at least one object exists under the prefix."""
        resp = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, MaxKeys=1)
        return resp.get("KeyCount", 0) > 0

    def upload_file(self, local_path: Path, key: str, existing: dict[str, int] | None = None) -> bool:
        """
        Upload a local file unless an object of identical size already exists.

        Args:
            local_path: File to upload.
            key: Destination key.
            existing: Optional pre-fetched {key: size} map to avoid a HEAD call.

        Returns:
            True if uploaded, False if skipped (already present).
        """
        size = local_path.stat().st_size
        if existing is not None and existing.get(key) == size:
            return False

        @with_retry(TRANSIENT_S3_ERRORS, attempts=self._attempts)
        def _upload() -> None:
            self.client.upload_file(str(local_path), self.bucket, key)

        _upload()
        return True

    def get_bytes(self, key: str) -> bytes:
        """Download an object's content."""

        @with_retry(TRANSIENT_S3_ERRORS, attempts=self._attempts)
        def _get() -> bytes:
            return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

        return _get()

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        """Write raw bytes to a key (overwrites)."""

        @with_retry(TRANSIENT_S3_ERRORS, attempts=self._attempts)
        def _put() -> None:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)

        _put()

    def delete_prefix(self, prefix: str) -> int:
        """Delete every object under a prefix; returns the number deleted."""
        keys = [o.key for o in self.list_objects(prefix)]
        for start in range(0, len(keys), 1000):
            batch = [{"Key": k} for k in keys[start : start + 1000]]
            self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": batch, "Quiet": True})
        return len(keys)

    # ---------------------------------------------------------------- parquet
    def write_table(
        self,
        table: pa.Table,
        prefix: str,
        partition_cols: Iterable[str] | None = None,
    ) -> int:
        """
        Write an Arrow table as Parquet under a prefix, idempotently.

        Args:
            table: Data to write.
            prefix: Destination prefix, e.g. 'silver/employees'.
            partition_cols: Hive partition columns, e.g. ['year', 'month'].

        Returns:
            Number of rows written.
        """
        partition_cols = list(partition_cols or [])
        prefix = prefix.strip("/")

        @with_retry(TRANSIENT_S3_ERRORS + (OSError,), attempts=self._attempts)
        def _write() -> None:
            if partition_cols:
                ds.write_dataset(
                    table,
                    base_dir=self._path(prefix),
                    filesystem=self.fs,
                    format="parquet",
                    partitioning=partition_cols,
                    partitioning_flavor="hive",
                    existing_data_behavior="delete_matching",
                    basename_template="part-{i}.parquet",
                )
            else:
                self.delete_prefix(prefix + "/")
                pq.write_table(table, self._path(f"{prefix}/part-0.parquet"), filesystem=self.fs)

        _write()
        log.info("parquet_written", extra={"target": self.uri(prefix), "rows": table.num_rows})
        return table.num_rows

    def write_immutable(self, table: pa.Table, prefix: str) -> bool:
        """
        Write a Parquet file to a prefix that must never be modified (Bronze).

        If the prefix already holds data the write is skipped, which keeps
        reruns idempotent without ever overwriting raw data.

        Returns:
            True if written, False if the prefix already existed.
        """
        prefix = prefix.strip("/")
        if self.exists(prefix + "/"):
            log.info("immutable_prefix_exists_skip", extra={"target": self.uri(prefix)})
            return False

        @with_retry(TRANSIENT_S3_ERRORS + (OSError,), attempts=self._attempts)
        def _write() -> None:
            pq.write_table(table, self._path(f"{prefix}/part-0.parquet"), filesystem=self.fs)

        _write()
        log.info("immutable_written", extra={"target": self.uri(prefix), "rows": table.num_rows})
        return True

    def read_table(self, prefix: str) -> pa.Table:
        """Read every Parquet file under a prefix (Hive partitions become columns)."""

        @with_retry(TRANSIENT_S3_ERRORS + (OSError,), attempts=self._attempts)
        def _read() -> pa.Table:
            dataset = ds.dataset(self._path(prefix), filesystem=self.fs, format="parquet", partitioning="hive")
            return dataset.to_table()

        return _read()
