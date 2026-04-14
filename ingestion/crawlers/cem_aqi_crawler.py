"""
UrbanPulse VN — CEM AQI Web Crawler.

Scrapes Air Quality Index (AQI) values from cem.gov.vn (Vietnam Environment Administration).
Uses configured CSS selectors from `ingestion.config` to locate tables and extract data.
"""

from __future__ import annotations

import logging
from typing import Any
import re
from datetime import datetime, timezone

from bs4 import BeautifulSoup
import pandas as pd

from ingestion.config import CEM_BASE_URL, CEM_SELECTORS
from ingestion.crawlers.base_crawler import BaseCrawler

logger = logging.getLogger(__name__)


class CEMAQICrawler(BaseCrawler):
    """Crawler for cem.gov.vn AQI data.

    Fetches the main AQI page, parses the table using BS4, and extracts:
    station name, AQI value, category (e.g., 'Tốt', 'Xấu'), and timestamp.

    Example:
        >>> with CEMAQICrawler() as crawler:
        ...     df = crawler.run()
        ...     print(df.head())
    """

    SOURCE_NAME: str = "cem_aqi"

    def get_target_urls(self) -> list[str]:
        """Return the list of URLs to crawl."""
        # Typically CEM has a main portal or specific sub-page for AQI.
        # This will be fetched.
        return [f"{CEM_BASE_URL}/"]

    def parse(self, html: str, url: str) -> list[dict[str, Any]]:
        """Parse the HTML content using CSS selectors.

        Args:
            html: Raw HTML.
            url: Source URL.

        Returns:
            List of parsed dictionaries.
        """
        soup = BeautifulSoup(html, "lxml")
        records: list[dict[str, Any]] = []

        # Find the table containing the data
        table = soup.select_one(CEM_SELECTORS.station_table)
        if not table:
            logger.warning("Could not find station table using selector: %s", CEM_SELECTORS.station_table)
            # Try a fallback to any table if specific selector fails
            table = soup.find("table")
        
        if not table:
            logger.error("No table found in HTML at %s", url)
            return []

        # Extract rows
        rows = soup.select(CEM_SELECTORS.station_rows)
        # Fallback if specific CSS for rows fails
        if not rows and table:
            rows = table.find_all("tr")

        logger.debug("Found %d rows in AQI table", len(rows))

        for row in rows:
            cols = row.find_all(["td", "th"])
            # Usually we need at least 4 columns (station, aqi, category, time)
            if len(cols) < 4:
                continue

            # Extract text safely
            texts = [c.get_text(strip=True) for c in cols]
            
            # Skip header rows often containing 'STT' or 'Trạm'
            if "Trạm" in texts[0] or "Station" in texts[0]:
                continue

            # We use index-based mapping as a best-effort if specific CSS nth-child fails in bs4
            station_name = texts[0]
            try:
                aqi_value_str = texts[1]
                aqi_value = float(aqi_value_str) if aqi_value_str.replace('.', '', 1).isdigit() else None
            except ValueError:
                aqi_value = None

            category = texts[2]
            published_time = texts[3]

            record = {
                "station_name": station_name,
                "aqi_value": aqi_value,
                "category": category,
                "published_time": published_time,
            }
            records.append(record)

        return records
