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
        """
        bbox_str = (
            f"{VIETNAM_BBOX['south']},{VIETNAM_BBOX['west']},"
            f"{VIETNAM_BBOX['north']},{VIETNAM_BBOX['east']}"
        )

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

        try:
            response = self._request(
                "POST", 
                OSM_OVERPASS_URL, 
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                json_body=None,
                params=None,
            )
            # httpx allows putting data as string using content instead of json usually,
            # base class requires dict handling, overriding carefully here:
        except Exception as exc:
            pass
        
        # Manually sending the query string via the http client to handle data=query
        try:
            raw_resp = self._http_client.post(
                OSM_OVERPASS_URL, data={"data": overpass_query}
            )
            raw_resp.raise_for_status()
            data = raw_resp.json()
        except Exception as exc:
            logger.warning("Failed to fetch OSM Overpass data: %s", exc)
            raise ExtractionError("OSM Overpass API failed") from exc

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
