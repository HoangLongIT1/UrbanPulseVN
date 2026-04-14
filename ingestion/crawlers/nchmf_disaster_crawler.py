"""
UrbanPulse VN — NCHMF Disaster Web Crawler.

Scrapes extreme weather and disaster warnings from nchmf.gov.vn 
(National Centre for Hydro-Meteorological Forecasting).
"""

from __future__ import annotations

import logging
from typing import Any
import re
from datetime import datetime

from bs4 import BeautifulSoup

from ingestion.config import NCHMF_BASE_URL, NCHMF_SELECTORS
from ingestion.crawlers.base_crawler import BaseCrawler

logger = logging.getLogger(__name__)


class NCHMFDisasterCrawler(BaseCrawler):
    """Crawler for nchmf.gov.vn disaster warnings.

    Extracts titles, summaries, publish dates, and URLs of the latest 
    disaster announcements.

    Example:
        >>> with NCHMFDisasterCrawler() as crawler:
        ...     df = crawler.run()
        ...     print(df.head())
    """

    SOURCE_NAME: str = "nchmf_disaster"

    def get_target_urls(self) -> list[str]:
        """Return the list of URLs to crawl."""
        return [
            # Main warning portals, might differ slightly but normally
            # route through their main categories.
            # Example: Tin cảnh báo thiên tai
            f"{NCHMF_BASE_URL}/"
        ]

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

        # Find items
        items = soup.select(NCHMF_SELECTORS.warning_items)
        if not items:
            logger.warning("No warning items found using selector: %s", NCHMF_SELECTORS.warning_items)
            # Fallback search attempting to find news layouts common on NCHMF
            items = soup.find_all("div", class_=re.compile(r"news|item"))

        logger.debug("Found %d items on warning page", len(items))

        for item in items:
            try:
                # Find title and link
                title_elem = item.select_one(NCHMF_SELECTORS.title)
                if not title_elem:
                    title_elem = item.find("a")
                
                title = title_elem.get_text(strip=True) if title_elem else "N/A"
                link = title_elem.get("href") if title_elem else ""

                if link and not link.startswith("http"):
                    link = f"{NCHMF_BASE_URL}{link}"

                # Find summary
                summary_elem = item.select_one(NCHMF_SELECTORS.summary)
                summary = summary_elem.get_text(strip=True) if summary_elem else ""

                # Find date
                date_elem = item.select_one(NCHMF_SELECTORS.publish_date)
                date_str = date_elem.get_text(strip=True) if date_elem else ""

                # Skip blank artifacts
                if title == "N/A" and not summary:
                    continue

                record = {
                    "title": title,
                    "summary": summary,
                    "publish_date": date_str,
                    "detail_url": link,
                }
                records.append(record)
            except Exception as exc:
                logger.debug("Skipping unparseable item chunk: %s", exc)

        return records
