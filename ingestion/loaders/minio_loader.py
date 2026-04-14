"""
UrbanPulse VN — MinIO Loader.

Takes Pandas DataFrames and writes them directly to MinIO (S3) 
as Parquet files, landing them safely into the Bronze layer.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from ingestion.config import DEFAULTS
from utils.minio_utils import MinIOClient

logger = logging.getLogger(__name__)


class MinIOLoader:
    """Loads Python DataFrames into MinIO Bronze Layer as Parquet.

    Automatically handles bucket creation, structured object naming
    (date-partitioned), and underlying byte transfers.
    """

    def __init__(self, bucket: str = DEFAULTS.raw_bucket) -> None:
        self.bucket = bucket
        self.client = MinIOClient()
        self.client.ensure_buckets([self.bucket])
        logger.info("MinIOLoader initialised for bucket: %s", self.bucket)

    def load(self, source_name: str, df: pd.DataFrame, suffix: str = "") -> str:
        """Upload a DataFrame to MinIO.

        Args:
            source_name: String identifying the source (e.g. 'air_quality').
                         Used to create Hive-style path prefixes.
            df: Data to upload.
            suffix: Optional filename suffix.

        Returns:
            The object key of the newly uploaded file.
        """
        if df.empty:
            logger.warning("Empty DataFrame provided for %s — skipping load", source_name)
            return ""

        now = datetime.now(tz=timezone.utc)
        object_name = (
            f"{source_name}/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"{source_name}_{now.strftime('%Y%m%d_%H%M%S')}"
        )
        if suffix:
            object_name = f"{object_name}_{suffix}"
        object_name = f"{object_name}.parquet"

        try:
            # MinIOClient handles parquet serialization internally
            obj_path = self.client.upload_dataframe(
                bucket=self.bucket,
                object_name=object_name,
                df=df,
            )
            logger.info("Successfully loaded %s [%d rows] to MinIO: %s", source_name, len(df), obj_path)
            return obj_path
        except Exception as exc:
            logger.error("MinIO upload failed for %s: %s", source_name, exc)
            raise

    def close(self) -> None:
        pass
