"""
UrbanPulse VN — Weather Extractor (Open-Meteo).

Extracts hourly weather forecast data for 63 Vietnamese provinces/cities
from the Open-Meteo Weather API (free, no API key required).

Collects: temperature, humidity, precipitation, wind speed, wind direction,
UV index, and surface pressure.

API docs: https://open-meteo.com/en/docs

Warnings (from warnings.md):
    - Some smaller provinces may not have a nearby weather station,
      Open-Meteo may return empty data → handled via skip + log.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd

from ingestion.config import OPEN_METEO_WEATHER_URL, VIETNAM_CITIES
from ingestion.extractors.base import BaseExtractor, ExtractionError

logger = logging.getLogger(__name__)

# Hourly variables to request from Open-Meteo
_HOURLY_VARIABLES: str = (
    "temperature_2m,"
    "relative_humidity_2m,"
    "precipitation,"
    "wind_speed_10m,"
    "wind_direction_10m,"
    "uv_index,"
    "surface_pressure"
)

# Max cities per batch request (Open-Meteo supports multi-coordinate queries)
_BATCH_SIZE: int = 15


class WeatherExtractor(BaseExtractor):
    """Extract hourly weather data for 63 Vietnamese cities.

    Open-Meteo supports batching multiple coordinates in a single request
    (comma-separated latitudes/longitudes).  We batch in groups of
    ``_BATCH_SIZE`` to avoid oversized query strings.

    Example:
        >>> with WeatherExtractor() as ext:
        ...     df = ext.run()
        ...     print(df.columns.tolist())
    """

    SOURCE_NAME: str = "weather"

    # ------------------------------------------------------------------
    # Core extraction
    # ------------------------------------------------------------------

    def extract(self) -> pd.DataFrame:
        """Extract hourly weather data for all 63 VN cities.

        Returns:
            DataFrame with columns: city, latitude, longitude, time,
            temperature_2m, relative_humidity_2m, precipitation,
            wind_speed_10m, wind_direction_10m, uv_index,
            surface_pressure, _source, _extracted_at.

        Raises:
            ExtractionError: When no data could be fetched for any city.
        """
        all_records: list[dict[str, Any]] = []

        # Process cities in batches
        for batch_start in range(0, len(VIETNAM_CITIES), _BATCH_SIZE):
            batch = VIETNAM_CITIES[batch_start : batch_start + _BATCH_SIZE]
            records = self._fetch_batch(batch)
            all_records.extend(records)

            # Small delay between batches
            if batch_start + _BATCH_SIZE < len(VIETNAM_CITIES):
                time.sleep(0.5)

        if not all_records:
            raise ExtractionError(
                "Open-Meteo returned 0 weather records for VN cities"
            )

        logger.info(
            "Extracted %d weather records for %d cities",
            len(all_records),
            len(VIETNAM_CITIES),
        )
        return self._records_to_dataframe(all_records)

    # ------------------------------------------------------------------
    # Batch API call
    # ------------------------------------------------------------------

    def _fetch_batch(
        self, cities: list[dict[str, float | str]]
    ) -> list[dict[str, Any]]:
        """Fetch weather data for a batch of cities in one API call.

        Args:
            cities: List of city dicts with ``name``, ``lat``, ``lon``.

        Returns:
            Flattened list of hourly weather records.
        """
        latitudes = ",".join(str(c["lat"]) for c in cities)
        longitudes = ",".join(str(c["lon"]) for c in cities)

        params: dict[str, str] = {
            "latitude": latitudes,
            "longitude": longitudes,
            "hourly": _HOURLY_VARIABLES,
            "timezone": "Asia/Ho_Chi_Minh",
            "forecast_days": "1",
        }

        try:
            response = self._get(OPEN_METEO_WEATHER_URL, params=params)
            data = response.json()
        except ExtractionError:
            city_names = [str(c["name"]) for c in cities]
            logger.warning(
                "Failed to fetch weather for batch: %s",
                ", ".join(city_names),
            )
            return []

        return self._parse_batch_response(data, cities)

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_batch_response(
        self,
        data: Any,
        cities: list[dict[str, float | str]],
    ) -> list[dict[str, Any]]:
        """Parse a multi-coordinate Open-Meteo response.

        Open-Meteo returns a list when multiple coordinates are queried,
        or a single dict for one coordinate.

        Args:
            data: Raw JSON response from Open-Meteo.
            cities: The city list that was queried (same order).

        Returns:
            Flattened list of hourly weather records.
        """
        records: list[dict[str, Any]] = []

        # Normalise to list (single city returns a dict, not a list)
        if isinstance(data, dict):
            data = [data]

        for idx, city_data in enumerate(data):
            if idx >= len(cities):
                break

            city = cities[idx]
            hourly = city_data.get("hourly", {})
            times = hourly.get("time", [])

            if not times:
                logger.debug("No hourly data for %s — skipping", city["name"])
                continue

            for i, ts in enumerate(times):
                record: dict[str, Any] = {
                    "city": city["name"],
                    "latitude": city["lat"],
                    "longitude": city["lon"],
                    "time": ts,
                    "temperature_2m": self._safe_index(
                        hourly.get("temperature_2m", []), i
                    ),
                    "relative_humidity_2m": self._safe_index(
                        hourly.get("relative_humidity_2m", []), i
                    ),
                    "precipitation": self._safe_index(
                        hourly.get("precipitation", []), i
                    ),
                    "wind_speed_10m": self._safe_index(
                        hourly.get("wind_speed_10m", []), i
                    ),
                    "wind_direction_10m": self._safe_index(
                        hourly.get("wind_direction_10m", []), i
                    ),
                    "uv_index": self._safe_index(
                        hourly.get("uv_index", []), i
                    ),
                    "surface_pressure": self._safe_index(
                        hourly.get("surface_pressure", []), i
                    ),
                }
                records.append(record)

        return records

    @staticmethod
    def _safe_index(lst: list[Any], idx: int) -> Any | None:
        """Safely access a list by index, returning None on out-of-bounds.

        Args:
            lst: The list to index into.
            idx: The index position.

        Returns:
            The value at ``idx``, or ``None`` if out of range.
        """
        if idx < len(lst):
            return lst[idx]
        return None
