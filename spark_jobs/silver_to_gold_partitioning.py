"""
UrbanPulse VN — Spark Job: Silver → Gold Aggregation & Partitioning.

Reads cleaned data from the Silver Iceberg layer, applies business-level
aggregations (daily AQI summary per city), and writes to the Gold layer,
partitioned by year/month for analytical query performance.

Gold tables produced:
    - gold.daily_city_aqi_summary   — daily avg/max AQI per city
    - gold.monthly_risk_index        — monthly environmental risk score

Usage (local):
    spark-submit spark_jobs/silver_to_gold_partitioning.py

Usage (via Docker):
    docker exec urbanpulse-spark-master \
        spark-submit /opt/spark/jobs/silver_to_gold_partitioning.py
"""

from __future__ import annotations

import os
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://urbanpulse-minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
NESSIE_URI = os.getenv("NESSIE_URI", "http://urbanpulse-nessie:19120/api/v1")

# AQI thresholds for risk scoring (Vietnam QCVN 05:2023/BTNMT)
AQI_THRESHOLDS = {
    "good":      (0, 50),
    "moderate":  (51, 100),
    "sensitive": (101, 150),
    "unhealthy": (151, 200),
    "very_unhealthy": (201, 300),
    "hazardous": (301, 500),
}


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("UrbanPulse-Silver-to-Gold")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.warehouse", "s3a://iceberg/")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Transformation functions
# ---------------------------------------------------------------------------

def _read_silver(spark: SparkSession, table: str) -> DataFrame:
    """Read an Iceberg Silver table from the Nessie catalog."""
    return spark.read.format("iceberg").load(f"nessie.silver.{table}")


def _build_daily_city_aqi_summary(df: DataFrame) -> DataFrame:
    """
    Aggregate air quality data to daily city-level AQI summary.

    Output columns:
        city_name, measurement_date, avg_aqi, max_aqi,
        dominant_pollutant, station_count, year, month
    """
    return (
        df
        .filter(F.col("pollutant") == "aqi")
        .filter(F.col("measurement_value").isNotNull())
        .withColumn("measurement_date", F.to_date("extracted_at"))
        .groupBy("city_name", "measurement_date")
        .agg(
            F.round(F.avg("measurement_value"), 2).alias("avg_aqi"),
            F.round(F.max("measurement_value"), 2).alias("max_aqi"),
            F.countDistinct("station_code").alias("station_count"),
        )
        # Add risk label based on avg_aqi
        .withColumn(
            "risk_label",
            F.when(F.col("avg_aqi") <= 50,  "Good")
             .when(F.col("avg_aqi") <= 100, "Moderate")
             .when(F.col("avg_aqi") <= 150, "Sensitive")
             .when(F.col("avg_aqi") <= 200, "Unhealthy")
             .when(F.col("avg_aqi") <= 300, "Very Unhealthy")
             .otherwise("Hazardous")
        )
        # Partition columns for Iceberg
        .withColumn("year",  F.year("measurement_date"))
        .withColumn("month", F.month("measurement_date"))
    )


def _build_monthly_risk_index(daily_df: DataFrame) -> DataFrame:
    """
    Roll daily AQI summaries up to a monthly risk index per city.

    Risk index = proportion of days with AQI > 100 (Sensitive threshold).
    Output: city_name, year, month, avg_monthly_aqi, risk_index_pct
    """
    return (
        daily_df
        .withColumn("is_unhealthy_day", (F.col("avg_aqi") > 100).cast("int"))
        .groupBy("city_name", "year", "month")
        .agg(
            F.round(F.avg("avg_aqi"), 2).alias("avg_monthly_aqi"),
            F.round(
                F.avg("is_unhealthy_day") * 100, 1
            ).alias("risk_index_pct"),   # % of days exceeding threshold
            F.sum("station_count").alias("total_station_days"),
        )
        .withColumn(
            "risk_tier",
            F.when(F.col("risk_index_pct") >= 50, "High")
             .when(F.col("risk_index_pct") >= 20, "Medium")
             .otherwise("Low")
        )
    )


def _write_gold_iceberg(df: DataFrame, table_name: str, partition_cols: list[str]) -> None:
    """Write a Gold-layer Iceberg table, partitioned as specified."""
    full_table = f"nessie.gold.{table_name}"
    writer = df.writeTo(full_table).tableProperty("write.format.default", "parquet")
    for col in partition_cols:
        writer = writer.partitionedBy(col)
    writer.createOrReplace()
    logger.info("Written Gold table: %s (%d rows)", full_table, df.count())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger.info("Starting Silver → Gold aggregation job")

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Read Silver
        logger.info("Reading silver.air_quality_cleaned")
        silver_df = _read_silver(spark, "air_quality_cleaned")

        # Gold Table 1: Daily city AQI summary
        logger.info("Building: daily_city_aqi_summary")
        daily_df = _build_daily_city_aqi_summary(silver_df)
        _write_gold_iceberg(daily_df, "daily_city_aqi_summary", ["year", "month"])

        # Gold Table 2: Monthly risk index
        logger.info("Building: monthly_risk_index")
        monthly_df = _build_monthly_risk_index(daily_df)
        _write_gold_iceberg(monthly_df, "monthly_risk_index", ["year", "month"])

        logger.info("Silver → Gold aggregation completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
