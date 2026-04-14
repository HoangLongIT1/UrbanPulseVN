"""
UrbanPulse VN — Flood Extractor (Open-Meteo Flood API).

Extracts daily river discharge forecasts for major Vietnamese rivers
from the Open-Meteo Flood API (free, no API key required).

Monitors 12 key river points covering the Red River (Sông Hồng),
Mekong branches (Sông Tiền / Sông Hậu), Đà, Mã, Cả, Hương,
Thu Bồn, Ba, Đồng Nai, and Sài Gòn rivers.

API docs: https://open-meteo.com/en/docs/flood-api
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd

from ingestion.config import OPEN_METEO_FLOOD_URL, VIETNAM_RIVERS
from ingestion.extractors.base import BaseExtractor, ExtractionError

logger = logging.getLogger(__name__)

# Daily variables to request from the Flood API
_DAILY_VARIABLES: str = "river_discharge"

# Number of forecast days to retrieve
_FORECAST_DAYS: int = 7

# Max river points per batch request
_BATCH_SIZE: int = 10


class FloodExtractor(BaseExtractor):
    """Extract daily river discharge forecasts for Vietnamese rivers.

    The Flood API supports multi-coordinate queries similarly to the
    weather endpoint.  We batch river monitoring points to minimise
    the number of HTTP requests.

    Example:
        >>> with FloodExtractor() as ext:
        ...     df = ext.run()
        ...     print(df[["river_name", "time", "river_discharge"]].head())
    """

    SOURCE_NAME: str = "flood"

    # ------------------------------------------------------------------
    # Core extraction
    # ------------------------------------------------------------------

    def extract(self) -> pd.DataFrame:
        """Extract daily river discharge forecasts for VN rivers.

        Returns:
            DataFrame with columns: river_name, latitude, longitude,
            time, river_discharge, _source, _extracted_at.

        Raises:
            ExtractionError: When no data could be fetched.
        """
        all_records: list[dict[str, Any]] = []

        for batch_start in range(0, len(VIETNAM_RIVERS), _BATCH_SIZE):
            batch = VIETNAM_RIVERS[batch_start : batch_start + _BATCH_SIZE]
            records = self._fetch_batch(batch)
            all_records.extend(records)

            if batch_start + _BATCH_SIZE < len(VIETNAM_RIVERS):
                time.sleep(0.5)

        if not all_records:
            raise ExtractionError(
                "Open-Meteo Flood API returned 0 records for VN rivers"
            )

        logger.info(
            "Extracted %d flood forecast records for %d river points",
            len(all_records),
            len(VIETNAM_RIVERS),
        )
        return self._records_to_dataframe(all_records)

    # ------------------------------------------------------------------
    # Batch API call
    # ------------------------------------------------------------------

    def _fetch_batch(
        self, rivers: list[dict[str, float | str]]
    ) -> list[dict[str, Any]]:
        """Fetch flood data for a batch of river points.

        Args:
            rivers: List of river dicts with ``name``, ``lat``, ``lon``.

        Returns:
            Flattened list of daily river discharge records.
        """
        latitudes = ",".join(str(r["lat"]) for r in rivers)
        longitudes = ",".join(str(r["lon"]) for r in rivers)

        params: dict[str, str] = {
            "latitude": latitudes,
            "longitude": longitudes,
            "daily": _DAILY_VARIABLES,
            "forecast_days": str(_FORECAST_DAYS),
        }

        try:
            response = self._get(OPEN_METEO_FLOOD_URL, params=params)
            data = response.json()
        except ExtractionError:
            river_names = [str(r["name"]) for r in rivers]
            logger.warning(
                "Failed to fetch flood data for batch: %s",
                ", ".join(river_names),
            )
            return []

        return self._parse_batch_response(data, rivers)

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_batch_response(
        self,
        data: Any,
        rivers: list[dict[str, float | str]],
    ) -> list[dict[str, Any]]:
        """Parse a multi-coordinate Flood API response.

        Args:
            data: Raw JSON response from the Flood API.
            rivers: The river list that was queried (same order).

        Returns:
            Flattened list of daily river discharge records.
        """
        records: list[dict[str, Any]] = []

        # Normalise: single coordinate returns a dict, multi returns a list
        if isinstance(data, dict):
            data = [data]

        for idx, river_data in enumerate(data):
            if idx >= len(rivers):
                break

            river = rivers[idx]
            daily = river_data.get("daily", {})
            times = daily.get("time", [])
            discharge_values = daily.get("river_discharge", [])

            if not times:
                logger.debug(
                    "No flood data for %s — skipping", river["name"]
                )
                continue

            for i, ts in enumerate(times):
                discharge = (
                    discharge_values[i]
                    if i < len(discharge_values)
                    else None
                )
                record: dict[str, Any] = {
                    "river_name": river["name"],
                    "latitude": river["lat"],
                    "longitude": river["lon"],
                    "time": ts,
                    "river_discharge": discharge,
                }
                records.append(record)

        return records
