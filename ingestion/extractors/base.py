"""
UrbanPulse VN — Base Extractor (Abstract).

Provides the abstract foundation for all API-based data extractors.
Each concrete extractor (air_quality, weather, flood, fire, geo) inherits
from this class and implements ``extract()``.

Features:
    - HTTP client with retry + exponential backoff
    - Automatic date-partitioned object naming for MinIO
    - Local-file fallback cache when API is unreachable
    - Cache TTL enforcement (stale cache is logged and discarded)
    - Structured logging (no print statements)

Usage:
    class AirQualityExtractor(BaseExtractor):
        SOURCE_NAME = "air_quality"

        def extract(self) -> pd.DataFrame:
            ...
"""

from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

from ingestion.config import DEFAULTS

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Abstract base class for all API-based data extractors.

    Attributes:
        SOURCE_NAME: Unique identifier for the data source (e.g. ``"air_quality"``).
            Must be overridden by subclasses.
    """

    SOURCE_NAME: str = ""

    def __init__(
        self,
        *,
        max_retries: int = DEFAULTS.max_retries,
        backoff_factor: int = DEFAULTS.backoff_factor,
        initial_backoff: float = DEFAULTS.initial_backoff_seconds,
        http_timeout: int = DEFAULTS.http_timeout_seconds,
        cache_ttl: int = DEFAULTS.cache_ttl_seconds,
        raw_bucket: str = DEFAULTS.raw_bucket,
    ) -> None:
        """Initialise the extractor.

        Args:
            max_retries: Maximum number of retry attempts on failure.
            backoff_factor: Multiplier applied to backoff between retries.
            initial_backoff: Seconds to wait before the first retry.
            http_timeout: HTTP request timeout in seconds.
            cache_ttl: Time-to-live for cached data in seconds. Cache older
                than this threshold will be discarded and not served as fallback.
            raw_bucket: MinIO bucket for raw data landing.
        """
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.initial_backoff = initial_backoff
        self.http_timeout = http_timeout
        self.cache_ttl = cache_ttl
        self.raw_bucket = raw_bucket

        self._http_client = httpx.Client(timeout=self.http_timeout)
        self._cache_dir = Path("data/cache") / self.SOURCE_NAME
        self._cache_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "Extractor initialised: source=%s retries=%d backoff=%ds cache_ttl=%ds",
            self.SOURCE_NAME,
            self.max_retries,
            self.initial_backoff,
            self.cache_ttl,
        )

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extract data from the external API source.

        Returns:
            A pandas DataFrame containing the extracted records.

        Raises:
            ExtractionError: When the source is unreachable after retries
                and no cached fallback is available.
        """

    # ------------------------------------------------------------------
    # HTTP helpers with retry + backoff
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """Execute an HTTP request with automatic retry and backoff.

        Args:
            method: HTTP method (GET, POST, …).
            url: Target URL.
            params: Query parameters.
            headers: Additional HTTP headers.
            json_body: JSON request body (for POST requests).

        Returns:
            The successful ``httpx.Response``.

        Raises:
            ExtractionError: After all retries are exhausted.
        """
        backoff = self.initial_backoff

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self._http_client.request(
                    method,
                    url,
                    params=params,
                    headers=headers,
                    json=json_body,
                )
                response.raise_for_status()
                logger.debug(
                    "%s %s → %d (attempt %d)",
                    method,
                    url,
                    response.status_code,
                    attempt,
                )
                return response

            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status == 429:
                    logger.warning(
                        "Rate-limited (429) on %s — backing off %.1fs "
                        "(attempt %d/%d)",
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
                    raise ExtractionError(
                        f"HTTP {status} from {url}"
                    ) from exc

            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                logger.warning(
                    "Connection error on %s — retrying in %.1fs "
                    "(attempt %d/%d)",
                    url,
                    backoff,
                    attempt,
                    self.max_retries,
                )
                if attempt == self.max_retries:
                    raise ExtractionError(
                        f"Failed to connect to {url} after "
                        f"{self.max_retries} attempts"
                    ) from exc

            time.sleep(backoff)
            backoff *= self.backoff_factor

        raise ExtractionError(f"All {self.max_retries} retries exhausted for {url}")

    def _get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Convenience wrapper for GET requests.

        Args:
            url: Target URL.
            params: Query parameters.
            headers: Additional HTTP headers.

        Returns:
            The successful ``httpx.Response``.
        """
        return self._request("GET", url, params=params, headers=headers)

    def _post_form(
        self,
        url: str,
        *,
        form_data: dict[str, str],
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """POST with form-encoded data (application/x-www-form-urlencoded).

        Unlike ``_request`` which sends JSON, this method sends form data —
        required by APIs such as OSM Overpass that do not accept JSON bodies.
        Applies the same retry + exponential backoff logic as ``_request``.

        Args:
            url: Target URL.
            form_data: Key-value pairs to send as form body.
            headers: Additional HTTP headers.

        Returns:
            The successful ``httpx.Response``.

        Raises:
            ExtractionError: After all retries are exhausted.
        """
        backoff = self.initial_backoff

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self._http_client.post(
                    url, data=form_data, headers=headers
                )
                response.raise_for_status()
                logger.debug(
                    "POST (form) %s → %d (attempt %d)",
                    url,
                    response.status_code,
                    attempt,
                )
                return response

            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status == 429 or 500 <= status < 600:
                    logger.warning(
                        "HTTP %d on POST %s — retrying in %.1fs "
                        "(attempt %d/%d)",
                        status,
                        url,
                        backoff,
                        attempt,
                        self.max_retries,
                    )
                else:
                    logger.error(
                        "HTTP %d on POST %s — not retryable", status, url
                    )
                    raise ExtractionError(
                        f"HTTP {status} from POST {url}"
                    ) from exc

            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                logger.warning(
                    "Connection error on POST %s — retrying in %.1fs "
                    "(attempt %d/%d)",
                    url,
                    backoff,
                    attempt,
                    self.max_retries,
                )
                if attempt == self.max_retries:
                    raise ExtractionError(
                        f"Failed to POST {url} after {self.max_retries} attempts"
                    ) from exc

            time.sleep(backoff)
            backoff *= self.backoff_factor

        raise ExtractionError(
            f"All {self.max_retries} retries exhausted for POST {url}"
        )

    # ------------------------------------------------------------------
    # Cache helpers (local-file fallback)
    # ------------------------------------------------------------------

    def _save_cache(self, data: list[dict[str, Any]]) -> Path:
        """Persist extracted data to a local JSON cache file.

        Args:
            data: List of records to cache.

        Returns:
            Path to the written cache file.
        """
        cache_file = self._cache_dir / "latest.json"
        cache_file.write_text(
            json.dumps(data, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        logger.info("Cache saved: %s (%d records)", cache_file, len(data))
        return cache_file

    def _load_cache(self) -> list[dict[str, Any]] | None:
        """Load data from local cache if it exists and is within TTL.

        Cache files older than ``self.cache_ttl`` seconds are considered
        stale and will not be served — this prevents the pipeline from
        silently returning days-old data during prolonged API outages.

        Returns:
            Cached records if valid, or ``None`` if not available / expired.
        """
        cache_file = self._cache_dir / "latest.json"
        if not cache_file.exists():
            logger.debug("No cache file found for %s", self.SOURCE_NAME)
            return None

        # --- FIX: Enforce cache_ttl ---
        file_age_seconds = time.time() - cache_file.stat().st_mtime
        if file_age_seconds > self.cache_ttl:
            logger.warning(
                "Cache expired for %s (age=%.0fs, ttl=%ds) — "
                "discarding stale data",
                self.SOURCE_NAME,
                file_age_seconds,
                self.cache_ttl,
            )
            return None

        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            logger.info(
                "Cache loaded: %s (%d records, age=%.0fs)",
                cache_file,
                len(data),
                file_age_seconds,
            )
            return data
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Cache read failed for %s: %s", self.SOURCE_NAME, exc)
            return None

    # ------------------------------------------------------------------
    # Output helpers
    # ------------------------------------------------------------------

    def _generate_object_name(self, suffix: str = "") -> str:
        """Build a date-partitioned MinIO object key.

        Args:
            suffix: Optional suffix before the file extension.

        Returns:
            Object key string, e.g.
            ``air_quality/2026/04/14/air_quality_20260414_151200.parquet``.
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
        """Convert a list of dicts to a DataFrame with metadata columns.

        Args:
            records: Raw records from the API.

        Returns:
            DataFrame with added ``_source`` and ``_extracted_at`` columns.
        """
        df = pd.DataFrame(records)
        df["_source"] = self.SOURCE_NAME
        df["_extracted_at"] = datetime.now(tz=timezone.utc).isoformat()
        return df

    # ------------------------------------------------------------------
    # Run orchestration
    # ------------------------------------------------------------------

    def run(self) -> pd.DataFrame:
        """Execute the full extract pipeline with fallback.

        Attempts ``extract()``; on failure falls back to local cache
        (only if cache is within TTL).

        Returns:
            DataFrame of extracted (or cached) records.

        Raises:
            ExtractionError: If extraction fails and no valid cache exists.
        """
        try:
            df = self.extract()
            logger.info(
                "Extraction OK: source=%s rows=%d", self.SOURCE_NAME, len(df)
            )
            # Persist cache for future fallback
            if not df.empty:
                self._save_cache(df.to_dict(orient="records"))
            return df

        except ExtractionError:
            logger.warning(
                "Extraction failed for %s — attempting cache fallback",
                self.SOURCE_NAME,
            )
            cached = self._load_cache()
            if cached:
                logger.info(
                    "Serving %d cached records for %s",
                    len(cached),
                    self.SOURCE_NAME,
                )
                return pd.DataFrame(cached)

            logger.error(
                "No valid cache available for %s — extraction fully failed",
                self.SOURCE_NAME,
            )
            raise

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Release HTTP client resources."""
        self._http_client.close()
        logger.debug("HTTP client closed for %s", self.SOURCE_NAME)

    def __enter__(self) -> "BaseExtractor":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


# ======================================================================
# Custom exceptions
# ======================================================================


class ExtractionError(Exception):
    """Raised when data extraction fails after retries and cache miss."""
    