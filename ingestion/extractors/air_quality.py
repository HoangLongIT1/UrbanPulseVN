"""
UrbanPulse VN — Air Quality Extractor (OpenAQ v3).

Extracts air quality measurements from monitoring stations in Vietnam
using the OpenAQ v3 API.  Collects PM2.5, PM10, O3, NO2, SO2, and CO
readings filtered by ``country_id=VN``.

API docs: https://docs.openaq.org/reference/

Warnings (from warnings.md):
    - OpenAQ free tier: 100 requests / minute.  Use batch pagination,
      never loop per-station.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from ingestion.config import OPENAQ_API_KEY, OPENAQ_BASE_URL
from ingestion.extractors.base import BaseExtractor, ExtractionError

logger = logging.getLogger(__name__)

# Parameters supported by OpenAQ v3
_TARGET_PARAMETERS: list[str] = ["pm25", "pm10", "o3", "no2", "so2", "co"]
_COUNTRY_ID: int = 56  # Vietnam's numeric ID on OpenAQ v3
_PAGE_LIMIT: int = 100  # max results per page (OpenAQ default)

# Delay between per-location /latest calls to respect the 100 req/min limit.
# At 2 s/call we stay safely under 92 req/min even with 100+ stations.
_PER_LOCATION_DELAY: float = 2


class AirQualityExtractor(BaseExtractor):
    """Extract air quality data from OpenAQ for Vietnam stations.

    The extractor:
        1. Discovers all active VN locations via ``/v3/locations``.
        2. Fetches latest measurements for each location via
           ``/v3/locations/{id}/latest``.
        3. Flattens the nested API response into a tabular DataFrame.

    Example:
        >>> with AirQualityExtractor() as ext:
        ...     df = ext.run()
        ...     print(df.shape)
    """

    SOURCE_NAME: str = "air_quality"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._headers: dict[str, str] = {}
        if OPENAQ_API_KEY:
            self._headers["X-API-Key"] = OPENAQ_API_KEY

    # ------------------------------------------------------------------
    # Core extraction
    # ------------------------------------------------------------------

    def extract(self) -> pd.DataFrame:
        """Extract latest air quality measurements from VN stations.

        Returns:
            DataFrame with columns: location_id, location_name, city,
            latitude, longitude, parameter, value, unit, last_updated,
            _source, _extracted_at.

        Raises:
            ExtractionError: When the API is unreachable and no
                cache is available.
        """
        locations = self._fetch_locations()
        if not locations:
            raise ExtractionError("No VN locations found on OpenAQ")

        logger.info("Found %d VN locations on OpenAQ", len(locations))

        all_records: list[dict[str, Any]] = []
        for i, loc in enumerate(locations):
            records = self._fetch_latest_for_location(loc)
            all_records.extend(records)

            # FIX: Throttle per-location calls to respect the 100 req/min
            # free-tier limit. Without this delay, 100+ sequential /latest
            # requests are fired in rapid succession and receive 429 errors.
            if i < len(locations) - 1:
                time.sleep(_PER_LOCATION_DELAY)

        if not all_records:
            logger.warning(
                "OpenAQ returned 0 measurements for %d VN locations "
                "(stations may be offline)",
                len(locations),
            )
            return pd.DataFrame()

        logger.info("Extracted %d air quality measurements", len(all_records))
        return self._records_to_dataframe(all_records)

    # ------------------------------------------------------------------
    # API calls
    # ------------------------------------------------------------------

    def _fetch_locations(self) -> list[dict[str, Any]]:
        """Fetch all active monitoring locations in Vietnam.

        Returns:
            List of location dicts from the OpenAQ API.
        """
        all_locations: list[dict[str, Any]] = []
        page = 1

        while True:
            params: dict[str, Any] = {
                "countries_id": _COUNTRY_ID,
                "limit": _PAGE_LIMIT,
                "page": page,
            }
            try:
                response = self._get(
                    f"{OPENAQ_BASE_URL}/locations",
                    params=params,
                    headers=self._headers,
                )
                data = response.json()
                results = data.get("results", [])

                if not results:
                    break

                all_locations.extend(results)
                logger.debug(
                    "Locations page %d: %d results", page, len(results)
                )

                # Stop when we've received fewer than a full page
                if len(results) < _PAGE_LIMIT:
                    break

                page += 1
                # Respect rate limit: small delay between pages
                time.sleep(0.7)

            except ExtractionError:
                logger.warning(
                    "Failed to fetch locations page %d — stopping pagination",
                    page,
                )
                break

        return all_locations

    def _fetch_latest_for_location(
        self, location: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Fetch the latest measurements for a single location.

        OpenAQ v3 /latest returns {value, sensorsId, datetime, coordinates}
        but does NOT include parameter info. We build a sensor→parameter
        mapping from the location's ``sensors`` array to resolve names.

        Args:
            location: A location dict from the /locations endpoint.

        Returns:
            List of flattened measurement records.
        """
        location_id = location.get("id")
        location_name = location.get("name", "Unknown")
        city = location.get("locality", location.get("city", ""))
        coords = location.get("coordinates", {})
        lat = coords.get("latitude")
        lon = coords.get("longitude")

        # Build sensor_id → {param_name, units} mapping from location data
        sensor_map: dict[int, dict[str, str]] = {}
        for sensor in location.get("sensors", []):
            sid = sensor.get("id")
            param = sensor.get("parameter", {})
            if sid and param:
                sensor_map[sid] = {
                    "name": param.get("name", "").lower(),
                    "units": param.get("units", ""),
                }

        try:
            response = self._get(
                f"{OPENAQ_BASE_URL}/locations/{location_id}/latest",
                headers=self._headers,
            )
            data = response.json()
        except ExtractionError:
            logger.warning(
                "Failed to fetch latest for location %s (%s)",
                location_id,
                location_name,
            )
            return []

        records: list[dict[str, Any]] = []
        measurements = data.get("results", [])

        for measurement in measurements:
            # Resolve parameter via sensorsId → sensor_map
            sensors_id = measurement.get("sensorsId")
            sensor_info = sensor_map.get(sensors_id, {})
            param_name = sensor_info.get("name", "")
            param_units = sensor_info.get("units", "")

            # Only collect our target pollutants
            if param_name not in _TARGET_PARAMETERS:
                continue

            # Extract datetime (v3 returns nested {utc, local})
            dt_raw = measurement.get("datetime", "")
            if isinstance(dt_raw, dict):
                dt_raw = dt_raw.get("utc", "")

            record: dict[str, Any] = {
                "location_id": location_id,
                "location_name": location_name,
                "city": city,
                "latitude": lat,
                "longitude": lon,
                "parameter": param_name,
                "value": measurement.get("value"),
                "unit": param_units,
                "last_updated": dt_raw,
            }
            records.append(record)

        return records

