"""
UrbanPulse VN — NASA FIRMS Extractor.

Extracts active tire hotspots from NASA FIRMS (Fire Information for Resource 
Management System) for Vietnam.

API docs: https://earthdata.nasa.gov/firms/api
Format: CSV -> Pandas DataFrame.
"""

from __future__ import annotations

import logging
from io import StringIO
from typing import Any

import pandas as pd

from ingestion.config import NASA_EARTHDATA_TOKEN, NASA_FIRMS_URL
from ingestion.extractors.base import BaseExtractor, ExtractionError

logger = logging.getLogger(__name__)


class NASAFirmsExtractor(BaseExtractor):
    """Extract active fire hotspots for Vietnam.

    Uses the FIRMS area/csv endpoint. We bound the query to the
    general coordinates of VNM.

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
        """
        if not NASA_EARTHDATA_TOKEN:
            logger.warning("NASA_EARTHDATA_TOKEN not found — skipping FIRMS extraction")
            raise ExtractionError("Missing NASA Earthdata Token")

        # Vietnam country code in FIRMS is "VNM"
        # We query the last 1 day (1) of VIIRS NOAA-20 data
        url = f"{NASA_FIRMS_URL}/VNM/VIIRS_NOAA20_NRT/1"

        try:
            # FIRMS requires MAP_KEY header instead of Authorization in some endpoints,
            # or directly in the URL path depending on the exact FIRMS API variant.
            # Using standard implementation:
            url_with_key = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{NASA_EARTHDATA_TOKEN}/VNM/VIIRS_NOAA20_NRT/1"
            
            response = self._get(url_with_key)
            csv_data = response.text
        except ExtractionError as exc:
            logger.warning("Failed to fetch NASA FIRMS data")
            raise ExtractionError("NASA FIRMS API fetch failed") from exc

        # FIRMS returns CSV string
        df = pd.read_csv(StringIO(csv_data))
        
        if df.empty:
            logger.warning("NASA FIRMS returned 0 hotspots for VNM")
            raise ExtractionError("0 hotspots returned")

        logger.info("Extracted %d active fire hotspots", len(df))

        records = df.to_dict(orient="records")
        return self._records_to_dataframe(records)
