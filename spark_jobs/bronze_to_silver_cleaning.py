"""
UrbanPulse VN — Spark Job: Bronze → Silver Cleaning.

Reads raw data from the Bronze layer (PostgreSQL or MinIO/Parquet),
applies data cleaning and standardization, then writes to the Silver layer
in Apache Iceberg format on MinIO, partitioned by date and city.

Cleaning rules applied:
    - Drop fully null rows
    - Cast measurement_value to DoubleType; replace out-of-range with null
    - Trim whitespace from all string columns
    - Standardize timestamp columns to UTC
    - Add partition columns: year, month, day

Usage (local):
    spark-submit spark_jobs/bronze_to_silver_cleaning.py

Usage (via Docker):
    docker exec urbanpulse-spark-master \
        spark-submit /opt/spark/jobs/bronze_to_silver_cleaning.py
"""

from __future__ import annotations

import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — read from environment (Docker-injected)
# ---------------------------------------------------------------------------

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "urbanpulse-postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "urbanpulse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "urbanpulse")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "urbanpulse_dev_2024")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://urbanpulse-minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
NESSIE_URI = os.getenv("NESSIE_URI", "http://urbanpulse-nessie:19120/api/v1")

JARS_DIR = os.path.join(os.path.dirname(__file__), "jars")

# ---------------------------------------------------------------------------
# Valid measurement ranges (physical bounds — not AQI standards)
# ---------------------------------------------------------------------------

MEASUREMENT_BOUNDS: dict[str, tuple[float, float]] = {
    "pm2_5":       (0.0, 1000.0),
    "pm10":        (0.0, 1000.0),
    "aqi":         (0.0, 500.0),
    "temperature": (-10.0, 50.0),
    "humidity":    (0.0, 100.0),
    "river_discharge": (0.0, 100_000.0),
}


# ---------------------------------------------------------------------------
# SparkSession factory
# ---------------------------------------------------------------------------

def build_spark_session() -> SparkSession:
    """
    Create a SparkSession pre-configured with:
        - Iceberg catalog pointing at Nessie + MinIO
        - S3A connector for MinIO (path-style access)
        - PostgreSQL JDBC driver
    """
    return (
        SparkSession.builder
        .appName("UrbanPulse-Bronze-to-Silver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.warehouse", "s3a://iceberg/")
        # S3A / MinIO settings
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Memory guardrails (16GB machine)
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Cleaning helpers
# ---------------------------------------------------------------------------

def _read_bronze_air_quality(spark: SparkSession):
    """Read air_quality_raw from PostgreSQL bronze schema."""
    jdbc_url = (
        f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "bronze.air_quality_raw")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def _clean_air_quality(df):
    """
    Apply cleaning rules to air quality DataFrame.

    Rules:
        1. Drop rows where all key measurement columns are null.
        2. Cast measurement_value to Double; values outside physical range → null.
        3. Trim whitespace from string columns.
        4. Parse extracted_at as UTC timestamp.
        5. Add partition columns (year, month, day).
    """
    # 1. Drop fully-null rows
    key_cols = ["station_code", "pollutant", "measurement_value"]
    df = df.dropna(how="all", subset=key_cols)

    # 2. Cast + range validation
    df = df.withColumn(
        "measurement_value",
        F.col("measurement_value").cast(DoubleType())
    )
    for pollutant, (lo, hi) in MEASUREMENT_BOUNDS.items():
        df = df.withColumn(
            "measurement_value",
            F.when(
                (F.col("pollutant") == pollutant) &
                (
                    (F.col("measurement_value") < lo) |
                    (F.col("measurement_value") > hi)
                ),
                None
            ).otherwise(F.col("measurement_value"))
        )

    # 3. Trim strings
    str_cols = ["station_code", "station_name", "pollutant", "city_name", "source_name"]
    for col in str_cols:
        if col in df.columns:
            df = df.withColumn(col, F.trim(F.col(col)))

    # 4. Standardize timestamp
    df = df.withColumn(
        "extracted_at",
        F.to_utc_timestamp(
            F.col("extracted_at").cast(TimestampType()), "Asia/Ho_Chi_Minh"
        )
    )

    # 5. Add partition columns
    df = (
        df
        .withColumn("year",  F.year("extracted_at"))
        .withColumn("month", F.month("extracted_at"))
        .withColumn("day",   F.dayofmonth("extracted_at"))
    )

    return df


def _write_silver_iceberg(df, table_name: str) -> None:
    """
    Write cleaned DataFrame to Iceberg Silver table on MinIO via Nessie.
    Partitioned by year/month/day for efficient time-range queries.
    """
    full_table = f"nessie.silver.{table_name}"

    # Create table if not exists (schema inferred from DataFrame)
    df.writeTo(full_table) \
      .partitionedBy("year", "month", "day") \
      .tableProperty("write.format.default", "parquet") \
      .createOrReplace()

    logger.info("Written %d rows to Iceberg table: %s", df.count(), full_table)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger.info("Starting Bronze → Silver cleaning job")

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Air Quality
        logger.info("Processing: air_quality_raw")
        raw_df = _read_bronze_air_quality(spark)
        logger.info("Rows read from Bronze: %d", raw_df.count())

        cleaned_df = _clean_air_quality(raw_df)
        logger.info("Rows after cleaning: %d", cleaned_df.count())

        _write_silver_iceberg(cleaned_df, "air_quality_cleaned")
        logger.info("Bronze → Silver cleaning completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
