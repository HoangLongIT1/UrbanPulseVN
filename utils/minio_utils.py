"""
UrbanPulse VN — MinIO (S3-compatible) Client Utility.

Provides a wrapper around the MinIO Python SDK for data lake operations.
Supports file upload, DataFrame-to-Parquet upload, and bucket management.

Usage:
    from utils.minio_utils import MinIOClient

    client = MinIOClient()
    client.ensure_buckets()
    client.upload_file("raw", "air_quality/2024/data.parquet", "/path/to/file.parquet")
"""

import io
import os
import json
import logging
from datetime import datetime

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

# Default buckets for the data lake
DEFAULT_BUCKETS = ["raw", "processed", "iceberg", "mlflow-artifacts"]


class MinIOClient:
    """MinIO client for data lake operations."""

    def __init__(
        self,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        secure: bool = False,
    ):
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = access_key or os.getenv("MINIO_ROOT_USER", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")

        self.client = Minio(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=secure,
        )
        logger.info("MinIO client initialized: %s", self.endpoint)

    def ensure_buckets(self, buckets: list[str] | None = None) -> None:
        """Create default buckets if they don't exist."""
        for bucket in buckets or DEFAULT_BUCKETS:
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
                logger.info("Created bucket: %s", bucket)
            else:
                logger.debug("Bucket already exists: %s", bucket)

    def upload_file(
        self, bucket: str, object_name: str, file_path: str
    ) -> str:
        """Upload a file to MinIO. Returns the object name."""
        self.client.fput_object(bucket, object_name, file_path)
        logger.info("Uploaded: %s/%s", bucket, object_name)
        return object_name

    def upload_bytes(
        self,
        bucket: str,
        object_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Upload raw bytes to MinIO."""
        stream = io.BytesIO(data)
        self.client.put_object(
            bucket, object_name, stream, length=len(data), content_type=content_type
        )
        logger.info("Uploaded bytes: %s/%s (%d bytes)", bucket, object_name, len(data))
        return object_name

    def upload_json(self, bucket: str, object_name: str, data: dict | list) -> str:
        """Upload a JSON object to MinIO."""
        json_bytes = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
        return self.upload_bytes(bucket, object_name, json_bytes, "application/json")

    def upload_dataframe(self, bucket: str, object_name: str, df) -> str:
        """
        Upload a pandas DataFrame as Parquet to MinIO.

        Args:
            bucket: Target bucket name
            object_name: Object path (e.g., 'air_quality/2024/01/data.parquet')
            df: pandas DataFrame
        """
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        self.client.put_object(
            bucket,
            object_name,
            buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream",
        )
        logger.info(
            "Uploaded DataFrame: %s/%s (%d rows)",
            bucket, object_name, len(df),
        )
        return object_name

    def download_file(self, bucket: str, object_name: str, file_path: str) -> str:
        """Download a file from MinIO to local path."""
        self.client.fget_object(bucket, object_name, file_path)
        logger.info("Downloaded: %s/%s → %s", bucket, object_name, file_path)
        return file_path

    def download_bytes(self, bucket: str, object_name: str) -> bytes:
        """Download object as bytes."""
        response = self.client.get_object(bucket, object_name)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def list_objects(
        self, bucket: str, prefix: str = "", recursive: bool = True
    ) -> list[str]:
        """List object names in a bucket with optional prefix filter."""
        objects = self.client.list_objects(bucket, prefix=prefix, recursive=recursive)
        return [obj.object_name for obj in objects]

    def object_exists(self, bucket: str, object_name: str) -> bool:
        """Check if an object exists in the bucket."""
        try:
            self.client.stat_object(bucket, object_name)
            return True
        except S3Error:
            return False

    def delete_object(self, bucket: str, object_name: str) -> None:
        """Delete a single object."""
        self.client.remove_object(bucket, object_name)
        logger.info("Deleted: %s/%s", bucket, object_name)

    def generate_object_name(
        self, source: str, file_format: str = "parquet"
    ) -> str:
        """
        Generate a date-partitioned object name.

        Example: air_quality/2024/04/12/air_quality_20240412_153000.parquet
        """
        now = datetime.utcnow()
        return (
            f"{source}/{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{source}_{now.strftime('%Y%m%d_%H%M%S')}.{file_format}"
        )
