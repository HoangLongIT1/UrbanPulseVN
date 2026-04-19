"""
UrbanPulse VN — NASA FIRMS Extractor.

Extracts active fire hotspots from NASA FIRMS (Fire Information for Resource
Management System) for Vietnam.

API docs: https://earthdata.nasa.gov/firms/api
Format: CSV -> Pandas DataFrame.

Authentication note:
    NASA FIRMS area/CSV endpoint requires the MAP_KEY embedded in the URL
    path — this is the API's own design and cannot be moved to a header.
    The key is therefore NOT logged at INFO level; only a redacted URL is
    emitted for traceability. Ensure NASA_EARTHDATA_TOKEN is stored
    exclusively in environment variables / secrets manager and never
    committed to source control.
"""

from __future__ import annotations

import logging
from io import StringIO
from typing import Any

import pandas as pd

from ingestion.config import NASA_EARTHDATA_TOKEN, NASA_FIRMS_URL
from ingestion.extractors.base import BaseExtractor, ExtractionError

logger = logging.getLogger(__name__)

# NASA FIRMS area endpoint config (country endpoint is currently unavailable)
_FIRMS_SOURCE: str = "VIIRS_NOAA20_NRT"
_FIRMS_DAY_RANGE: int = 1

# Vietnam bounding box (West, South, East, North)
_VN_BBOX: str = "102.14,8.18,109.46,23.39"


class NASAFirmsExtractor(BaseExtractor):
    """Extract active fire hotspots for Vietnam.

    Uses the FIRMS area/csv endpoint with Vietnam's bounding box
    since the /country endpoint is currently unavailable.

    Example:
        >>> with NASAFirmsExtractor() as ext:
        ...     df = ext.run()
        ...     print(df.head())
    """

    SOURCE_NAME: str = "nasa_firms"

    def extract(self) -> pd.DataFrame:
        """Fetch and parse NASA FIRMS fire hotspots.

        Returns:
            DataFrame containing hotspot latitudes, longitudes, confidence,
            and acquisition details.

        Raises:
            ExtractionError: When the token is missing or the API fails
                after retries.
        """
        if not NASA_EARTHDATA_TOKEN:
            logger.warning(
                "NASA_EARTHDATA_TOKEN not set — skipping FIRMS extraction"
            )
            raise ExtractionError("Missing NASA Earthdata Token")

        # NASA FIRMS area/CSV API requires the MAP_KEY in the URL path.
        # Using /area endpoint because /country is currently unavailable.
        firms_url = (
            "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
            f"/{NASA_EARTHDATA_TOKEN}"
            f"/{_FIRMS_SOURCE}/{_VN_BBOX}/{_FIRMS_DAY_RANGE}"
        )

        # Log a redacted URL so the token does not appear in application logs.
        safe_url = (
            "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
            f"/***/{_FIRMS_SOURCE}/{_VN_BBOX}/{_FIRMS_DAY_RANGE}"
        )
        logger.info("Fetching NASA FIRMS data: %s", safe_url)

        try:
            response = self._get(firms_url)
            csv_data = response.text
        except ExtractionError as exc:
            logger.warning("Failed to fetch NASA FIRMS data from %s", safe_url)
            raise ExtractionError("NASA FIRMS API fetch failed") from exc

        # FIRMS returns a CSV string; parse it directly.
        df = pd.read_csv(StringIO(csv_data))

        if df.empty:
            logger.warning("NASA FIRMS returned 0 hotspots for Vietnam bbox")
            raise ExtractionError("0 hotspots returned")

        logger.info("Extracted %d active fire hotspots", len(df))

        records = df.to_dict(orient="records")
        return self._records_to_dataframe(records)
        