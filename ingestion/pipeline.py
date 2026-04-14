"""
UrbanPulse VN — Ingestion Pipeline.

Orchestrates the entire data extraction, web crawling, and loading
process. Supports both daily incremental runs and historical seeding.

Usage:
    python -m ingestion.pipeline --mode daily
    python -m ingestion.pipeline --mode seed
"""

from __future__ import annotations

import argparse
import logging
from typing import Any

from ingestion.crawlers.cem_aqi_crawler import CEMAQICrawler
from ingestion.crawlers.nchmf_disaster_crawler import NCHMFDisasterCrawler
from ingestion.extractors.air_quality import AirQualityExtractor
from ingestion.extractors.fire_hotspot import NASAFirmsExtractor
from ingestion.extractors.flood import FloodExtractor
from ingestion.extractors.geo_features import GeoFeaturesExtractor
from ingestion.extractors.weather import WeatherExtractor
from ingestion.loaders.minio_loader import MinIOLoader
from ingestion.loaders.postgres_loader import PostgresLoader
from utils.helper import setup_logging

logger = logging.getLogger(__name__)


class IngestionPipeline:
    """End-to-End Orchestrator for Sprint 1 Ingestion layer."""

    def __init__(self) -> None:
        self.minio = MinIOLoader()
        self.pg = PostgresLoader()

        # Instantiate all our sources
        self.sources = [
            WeatherExtractor(),
            FloodExtractor(),
            AirQualityExtractor(),
            NASAFirmsExtractor(),
            GeoFeaturesExtractor(),
            CEMAQICrawler(),
            NCHMFDisasterCrawler(),
        ]

    def run_all(self, is_seed: bool = False) -> None:
        """Run all extractors and load data to Bronze.

        Args:
            is_seed: If True, this is a historical data backfill.
        """
        run_mode = "seed" if is_seed else "incremental"
        logger.info("Starting ingestion pipeline in %s mode", run_mode.upper())

        for source in self.sources:
            source_name = getattr(source, "SOURCE_NAME", type(source).__name__)
            try:
                # 1. Extract
                logger.info("--- Processing %s ---", source_name)
                df = source.run()

                # 2. Add pipeline metadata
                df["_run_mode"] = run_mode

                # 3. Load to MinIO Bronze Layer
                # Note: MinIOLoader auto-creates Hive partition paths
                object_path = self.minio.load(source_name=source_name, df=df)

                # 4. Log to PostgreSQL registry
                self.pg.log_run(
                    source_name=source_name,
                    row_count=len(df),
                    object_path=object_path,
                    status="SUCCESS",
                )

            except Exception as exc:
                logger.error("Pipeline failed for %s: %s", source_name, exc)
                self.pg.log_run(
                    source_name=source_name,
                    row_count=0,
                    object_path="",
                    status="FAILED",
                )
            finally:
                if hasattr(source, "close"):
                    source.close()
        
        logger.info("Ingestion pipeline completed.")

    def close(self) -> None:
        self.pg.close()


def main() -> None:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Run UrbanPulse Ingestion")
    parser.add_argument(
        "--mode",
        choices=["daily", "seed"],
        default="daily",
        help="Run mode: 'daily' for incremental, 'seed' for historical.",
    )
    args = parser.parse_args()

    setup_logging()
    
    pipeline = IngestionPipeline()
    try:
        pipeline.run_all(is_seed=(args.mode == "seed"))
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
