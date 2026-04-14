"""
UrbanPulse VN — Base Crawler (Abstract).

Provides the abstract foundation for all web-scraping crawlers.
Each concrete crawler (CEM AQI, NCHMF Disaster) inherits from this class
and implements ``parse()`` plus ``get_target_urls()``.

Features:
    - Retry with exponential backoff (5s → 10s → 20s)
    - Random delay between requests (2–8s) to respect rate limits
    - Rotating User-Agent headers
    - Redis cache fallback when source website is down
    - Local-file cache as secondary fallback (if Redis unavailable)
    - Configurable CSS selectors loaded from ``ingestion.config``
    - Structured logging (no print statements)

Design contract (from warnings.md):
    MAX_RETRIES = 3
    BACKOFF_FACTOR = 2
    REQUEST_DELAY = (2, 8)
    CACHE_TTL = 3600

Usage:
    class CEMAQICrawler(BaseCrawler):
        SOURCE_NAME = "cem_aqi"

        def get_target_urls(self) -> list[str]:
            return ["https://cem.gov.vn/..."]

        def parse(self, html: str, url: str) -> list[dict]:
            ...
"""

from __future__ import annotations

import json
import logging
import random
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

from ingestion.config import DEFAULTS

logger = logging.getLogger(__name__)

# Rotating User-Agent pool to reduce ban risk
_USER_AGENTS: list[str] = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.4 Safari/605.1.15"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) "
        "Gecko/20100101 Firefox/125.0"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"
    ),
]


class BaseCrawler(ABC):
    """Abstract base class for all web-scraping crawlers.

    Attributes:
        SOURCE_NAME: Unique identifier for the crawler (e.g. ``"cem_aqi"``).
            Must be overridden by subclasses.
    """

    SOURCE_NAME: str = ""

    def __init__(
        self,
        *,
        max_retries: int = DEFAULTS.max_retries,
        backoff_factor: int = DEFAULTS.backoff_factor,
        initial_backoff: float = DEFAULTS.initial_backoff_seconds,
        request_delay_min: float = DEFAULTS.request_delay_min,
        request_delay_max: float = DEFAULTS.request_delay_max,
        http_timeout: int = DEFAULTS.http_timeout_seconds,
        cache_ttl: int = DEFAULTS.cache_ttl_seconds,
        raw_bucket: str = DEFAULTS.raw_bucket,
    ) -> None:
        """Initialise the crawler.

        Args:
            max_retries: Maximum retry attempts per URL.
            backoff_factor: Multiplier for exponential backoff.
            initial_backoff: Seconds before the first retry.
            request_delay_min: Minimum random delay between requests (s).
            request_delay_max: Maximum random delay between requests (s).
            http_timeout: HTTP request timeout in seconds.
            cache_ttl: Time-to-live for Redis cache entries (seconds).
            raw_bucket: MinIO bucket for raw data landing.
        """
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.initial_backoff = initial_backoff
        self.request_delay_min = request_delay_min
        self.request_delay_max = request_delay_max
        self.http_timeout = http_timeout
        self.cache_ttl = cache_ttl
        self.raw_bucket = raw_bucket

        self._http_client = httpx.Client(timeout=self.http_timeout)
        self._cache_dir = Path("data/cache") / self.SOURCE_NAME
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._redis: Any | None = None

        logger.info(
            "Crawler initialised: source=%s retries=%d delay=(%.1f–%.1fs)",
            self.SOURCE_NAME,
            self.max_retries,
            self.request_delay_min,
            self.request_delay_max,
        )

    # ------------------------------------------------------------------
    # Redis cache (optional — gracefully degrades to local file)
    # ------------------------------------------------------------------

    def _init_redis(self) -> None:
        """Attempt to connect to Redis for caching. Non-fatal on failure."""
        if self._redis is not None:
            return
        try:
            from utils.redis_client import RedisClient

            client = RedisClient()
            if client.ping():
                self._redis = client
                logger.info("Redis cache connected for %s", self.SOURCE_NAME)
            else:
                logger.warning(
                    "Redis unreachable — falling back to local file cache"
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Redis init failed (%s) — using file cache", exc)

    def _cache_key(self) -> str:
        """Build a Redis cache key for this crawler."""
        return f"urbanpulse:crawler:{self.SOURCE_NAME}:latest"

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def get_target_urls(self) -> list[str]:
        """Return the list of URLs to crawl.

        Returns:
            List of target page URLs.
        """

    @abstractmethod
    def parse(self, html: str, url: str) -> list[dict[str, Any]]:
        """Parse an HTML page into structured records.

        Args:
            html: Raw HTML content of the page.
            url: The URL the HTML was fetched from (for logging context).

        Returns:
            List of parsed record dicts.
        """

    # ------------------------------------------------------------------
    # HTTP fetch with retry + backoff + rate-limit delay
    # ------------------------------------------------------------------

    def _fetch(self, url: str) -> str | None:
        """Fetch a single URL with retry logic and random delay.

        Args:
            url: Target URL to fetch.

        Returns:
            HTML content as string, or ``None`` after all retries fail.
        """
        backoff = self.initial_backoff

        for attempt in range(1, self.max_retries + 1):
            try:
                headers = {
                    "User-Agent": random.choice(_USER_AGENTS),
                    "Accept": "text/html,application/xhtml+xml",
                    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
                }
                response = self._http_client.get(
                    url, headers=headers, follow_redirects=True
                )
                response.raise_for_status()

                logger.debug(
                    "Fetched %s → %d (attempt %d)",
                    url,
                    response.status_code,
                    attempt,
                )
                return response.text

            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status in (403, 429):
                    logger.warning(
                        "Blocked/rate-limited (%d) on %s — "
                        "backing off %.1fs (attempt %d/%d)",
                        status,
                        url,
                        backoff,
                        attempt,
                        self.max_retries,
                    )
                elif 500 <= status < 600:
                    logger.warning(
                        "Server error (%d) on %s — retrying in %.1fs "
                        "(attempt %d/%d)",
                        status,
                        url,
                        backoff,
                        attempt,
                        self.max_retries,
                    )
                else:
                    logger.error(
                        "HTTP %d on %s — not retryable", status, url
                    )
                    return None

            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                logger.warning(
                    "Connection error on %s (%s) — retrying in %.1fs "
                    "(attempt %d/%d)",
                    url,
                    type(exc).__name__,
                    backoff,
                    attempt,
                    self.max_retries,
                )

            time.sleep(backoff)
            backoff *= self.backoff_factor

        logger.error(
            "All %d retries exhausted for %s", self.max_retries, url
        )
        return None

    def _polite_delay(self) -> None:
        """Sleep a random interval between requests to respect rate limits."""
        delay = random.uniform(self.request_delay_min, self.request_delay_max)
        logger.debug("Polite delay: %.2fs", delay)
        time.sleep(delay)

    # ------------------------------------------------------------------
    # Cache helpers (Redis → local file)
    # ------------------------------------------------------------------

    def _save_cache(self, data: list[dict[str, Any]]) -> None:
        """Save crawled data to Redis (primary) and local file (secondary).

        Args:
            data: List of parsed records.
        """
        serialised = json.dumps(data, ensure_ascii=False, default=str)

        # Redis primary cache
        self._init_redis()
        if self._redis is not None:
            try:
                self._redis.set(self._cache_key(), serialised, ttl=self.cache_ttl)
                logger.info(
                    "Redis cache saved: %s (%d records)",
                    self._cache_key(),
                    len(data),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Redis cache write failed: %s", exc)

        # Local file secondary cache (always)
        cache_file = self._cache_dir / "latest.json"
        cache_file.write_text(serialised, encoding="utf-8")
        logger.info("File cache saved: %s (%d records)", cache_file, len(data))

    def _load_cache(self) -> list[dict[str, Any]] | None:
        """Load data from Redis first, then fall back to local file.

        Returns:
            Cached records, or ``None`` if nothing is available.
        """
        # Try Redis
        self._init_redis()
        if self._redis is not None:
            try:
                raw = self._redis.get(self._cache_key())
                if raw:
                    data = json.loads(raw)
                    logger.info(
                        "Redis cache hit: %s (%d records)",
                        self._cache_key(),
                        len(data),
                    )
                    return data
            except Exception as exc:  # noqa: BLE001
                logger.warning("Redis cache read failed: %s", exc)

        # Fall back to local file
        cache_file = self._cache_dir / "latest.json"
        if cache_file.exists():
            try:
                data = json.loads(cache_file.read_text(encoding="utf-8"))
                logger.info(
                    "File cache hit: %s (%d records)", cache_file, len(data)
                )
                return data
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("File cache read failed: %s", exc)

        logger.debug("No cache available for %s", self.SOURCE_NAME)
        return None

    # ------------------------------------------------------------------
    # Output helpers
    # ------------------------------------------------------------------

    def _generate_object_name(self, suffix: str = "") -> str:
        """Build a date-partitioned MinIO object key.

        Args:
            suffix: Optional suffix before the file extension.

        Returns:
            Object key string.
        """
        now = datetime.now(tz=timezone.utc)
        base = (
            f"{self.SOURCE_NAME}/{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{self.SOURCE_NAME}_{now.strftime('%Y%m%d_%H%M%S')}"
        )
        if suffix:
            base = f"{base}_{suffix}"
        return f"{base}.parquet"

    def _records_to_dataframe(
        self, records: list[dict[str, Any]]
    ) -> pd.DataFrame:
        """Convert parsed records to a DataFrame with metadata columns.

        Args:
            records: Parsed records from ``parse()``.

        Returns:
            DataFrame with ``_source`` and ``_crawled_at`` columns added.
        """
        df = pd.DataFrame(records)
        df["_source"] = self.SOURCE_NAME
        df["_crawled_at"] = datetime.now(tz=timezone.utc).isoformat()
        return df

    # ------------------------------------------------------------------
    # Main crawl orchestration
    # ------------------------------------------------------------------

    def crawl(self) -> pd.DataFrame:
        """Execute the full crawl pipeline with fallback.

        Steps:
            1. Get target URLs from subclass
            2. Fetch each URL with retry + polite delay
            3. Parse HTML into records
            4. Save to cache (Redis + file)
            5. Return as DataFrame

        On failure, falls back to cached data.

        Returns:
            DataFrame of crawled (or cached) records.

        Raises:
            CrawlError: If crawling fails and no cache exists.
        """
        all_records: list[dict[str, Any]] = []
        urls = self.get_target_urls()
        logger.info(
            "Starting crawl: source=%s urls=%d", self.SOURCE_NAME, len(urls)
        )

        for i, url in enumerate(urls):
            html = self._fetch(url)
            if html is not None:
                try:
                    records = self.parse(html, url)
                    all_records.extend(records)
                    logger.info(
                        "Parsed %d records from %s", len(records), url
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "Parse error on %s: %s", url, exc, exc_info=True
                    )
            else:
                logger.warning("Skipped (fetch failed): %s", url)

            # Polite delay between pages (skip after the last one)
            if i < len(urls) - 1:
                self._polite_delay()

        if all_records:
            self._save_cache(all_records)
            df = self._records_to_dataframe(all_records)
            logger.info(
                "Crawl completed: source=%s total_rows=%d",
                self.SOURCE_NAME,
                len(df),
            )
            return df

        # Fallback to cache
        logger.warning(
            "Crawl yielded 0 records for %s — attempting cache fallback",
            self.SOURCE_NAME,
        )
        cached = self._load_cache()
        if cached:
            logger.info(
                "Serving %d cached records for %s",
                len(cached),
                self.SOURCE_NAME,
            )
            return self._records_to_dataframe(cached)

        logger.error(
            "No cache available for %s — crawl fully failed",
            self.SOURCE_NAME,
        )
        raise CrawlError(
            f"Crawl failed for {self.SOURCE_NAME}: "
            f"0 records fetched and no cache available"
        )

    # ------------------------------------------------------------------
    # Alias for consistent interface with BaseExtractor
    # ------------------------------------------------------------------

    def run(self) -> pd.DataFrame:
        """Alias for ``crawl()`` to provide a uniform interface.

        Returns:
            DataFrame of crawled records.
        """
        return self.crawl()

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Release HTTP client and Redis resources."""
        self._http_client.close()
        if self._redis is not None:
            self._redis.close()
        logger.debug("Crawler resources released for %s", self.SOURCE_NAME)

    def __enter__(self) -> "BaseCrawler":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


# ======================================================================
# Custom exceptions
# ======================================================================


class CrawlError(Exception):
    """Raised when crawling fails after retries and cache miss."""
