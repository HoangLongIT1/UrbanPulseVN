"""
UrbanPulse VN — Geo Features Extractor (OSM Overpass).

Extracts critical infrastructural features (e.g. hospitals, shelters,
fire stations) inside Vietnam's bounding box using OSM Overpass API.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from ingestion.config import OSM_OVERPASS_URL, VIETNAM_BBOX
from ingestion.extractors.base import BaseExtractor, ExtractionError

logger = logging.getLogger(__name__)


class GeoFeaturesExtractor(BaseExtractor):
    """Extract OpenStreetMap infrastructural nodes for Vietnam.

    Sends an Overpass QL query to find all hospitals and fire stations
    within the predefined Vietnam bounding box.

    OSM Overpass requires POST with form-encoded ``data`` field —
    the base ``_post_form()`` helper is used here instead of ``_get()``
    to ensure retry + backoff still applies.

    Example:
        >>> with GeoFeaturesExtractor() as ext:
        ...     df = ext.run()
        ...     print(df.head())
    """

    SOURCE_NAME: str = "geo_features"

    def extract(self) -> pd.DataFrame:
        """Fetch geographical features from OSM.

        Returns:
            DataFrame of locations, coords, and tags.

        Raises:
            ExtractionError: When the Overpass API is unreachable after
                retries and no cached fallback is available.
        """
        bbox_str = (
            f"{VIETNAM_BBOX['south']},{VIETNAM_BBOX['west']},"
            f"{VIETNAM_BBOX['north']},{VIETNAM_BBOX['east']}"
        )

        # Overpass QL: fetch hospital and fire station nodes inside Vietnam.
        # Timeout is set conservatively — Overpass can be slow on large bboxes.
        overpass_query = f"""
        [out:json][timeout:25];
        (
          node["amenity"="hospital"]({bbox_str});
          node["amenity"="fire_station"]({bbox_str});
        );
        out body;
        >;
        out skel qt;
        """

        # OSM Overpass accepts POST with application/x-www-form-urlencoded.
        # We use _post_form() so retry + backoff is handled identically to
        # every other extractor in the pipeline.
        try:
            response = self._post_form(
                OSM_OVERPASS_URL,
                form_data={"data": overpass_query},
            )
            data = response.json()
        except ExtractionError:
            logger.warning("Failed to fetch OSM Overpass data after retries")
            raise

        elements = data.get("elements", [])
        if not elements:
            raise ExtractionError("No geo features parsed from Overpass JSON")

        records: list[dict[str, Any]] = []
        for element in elements:
            if element.get("type") != "node":
                continue

            tags = element.get("tags", {})
            record = {
                "node_id": element.get("id"),
                "latitude": element.get("lat"),
                "longitude": element.get("lon"),
                "amenity_type": tags.get("amenity"),
                "name": tags.get("name", "Unknown"),
            }
            records.append(record)

        logger.info("Extracted %d geo infrastructural features", len(records))
        return self._records_to_dataframe(records)