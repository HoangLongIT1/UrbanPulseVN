"""
UrbanPulse VN — PostgreSQL Metadata Loader.

Registers ingestion jobs and records metadata logs into PostgreSQL
using raw psycopg2 (lightweight, no SQLAlchemy required).
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

from utils.helper import get_env

logger = logging.getLogger(__name__)


class PostgresLoader:
    """Manages system metadata logging to PostgreSQL.

    Does NOT store row-level extracted data (that goes to MinIO).
    Instead, it records ingestion runs, row counts, and statuses.
    """

    def __init__(self) -> None:
        try:
            self.conn = psycopg2.connect(
                host=get_env("POSTGRES_HOST", "localhost"),
                port=int(get_env("POSTGRES_PORT", "5432")),
                dbname=get_env("POSTGRES_DB", "urbanpulse"),
                user=get_env("POSTGRES_USER", "urbanpulse"),
                password=get_env("POSTGRES_PASSWORD", "urbanpulse_dev_2024"),
            )
            self.conn.autocommit = True
            logger.info("PostgresLoader connected successfully")
            self._init_tables()
        except Exception as exc:
            logger.error("PostgresLoader failed to connect: %s", exc)
            raise

    def _init_tables(self) -> None:
        """Create metadata tracking tables if they don't exist."""
        query = """
        CREATE TABLE IF NOT EXISTS ingestion_logs (
            id SERIAL PRIMARY KEY,
            source_name VARCHAR(100) NOT NULL,
            crawled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            row_count INTEGER NOT NULL,
            object_path VARCHAR(500),
            status VARCHAR(20) DEFAULT 'SUCCESS'
        );
        """
        with self.conn.cursor() as cur:
            cur.execute(query)

    def log_run(
        self, 
        source_name: str, 
        row_count: int, 
        object_path: str, 
        status: str = "SUCCESS"
    ) -> None:
        """Log an extraction/crawl run to the registry."""
        query = """
        INSERT INTO ingestion_logs (source_name, row_count, object_path, status)
        VALUES (%s, %s, %s, %s)
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (source_name, row_count, object_path, status))
            logger.debug("Logged run for %s (%d rows)", source_name, row_count)
        except Exception as exc:
            logger.error("Failed to log run to Postgres: %s", exc)

    def load(self, source_name: str, df: Any) -> None:
        """Load DataFrame rows into the bronze schema.

        Args:
            source_name: Table name (will be created in 'bronze' schema).
            df: Pandas DataFrame to load.
        """
        if df.empty:
            return

        table_name = f"bronze.{source_name}"

        # Quote all column names for safety
        quoted_cols = ", ".join([f'"{c}"' for c in df.columns])

        # 1. Create table if not exists (all TEXT for Bronze raw layer)
        columns_def = ", ".join([f'"{c}" TEXT' for c in df.columns])
        create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def});"

        # 2. Convert DataFrame to list of tuples, handling NaN/NaT → None
        clean_df = df.copy()
        for col in clean_df.columns:
            clean_df[col] = clean_df[col].apply(
                lambda v: None if v is None or (isinstance(v, float) and str(v) == 'nan') else str(v)
            )
        tuples = [tuple(row) for row in clean_df.to_numpy()]

        insert_query = f"INSERT INTO {table_name} ({quoted_cols}) VALUES %s"

        try:
            with self.conn.cursor() as cur:
                cur.execute(create_query)
                cur.execute(f"TRUNCATE TABLE {table_name};")
                execute_values(cur, insert_query, tuples)
            logger.info("Loaded %d rows into %s", len(df), table_name)
        except Exception as exc:
            logger.error("Failed to load data into %s: %s", table_name, exc)
            raise

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            logger.info("PostgresLoader disconnected")

