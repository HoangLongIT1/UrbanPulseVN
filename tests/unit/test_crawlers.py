"""
Unit tests for Extractors and Crawlers.
"""
from unittest.mock import MagicMock

import httpx
import pandas as pd
import pytest

from ingestion.crawlers.base_crawler import BaseCrawler, CrawlError
from ingestion.extractors.base import BaseExtractor, ExtractionError


class MockExtractor(BaseExtractor):
    SOURCE_NAME = "mock_ext"

    def extract(self) -> pd.DataFrame:
        resp = self._get("https://mock-api.local")
        return pd.DataFrame([{"data": resp.text}])


def test_base_extractor_retries_and_raises(mocker) -> None:
    # Use max_retries=2, backoff=0 to make test fast
    ext = MockExtractor(max_retries=2, initial_backoff=0.01)
    
    # Mock httpx to always throw HTTP 500 error
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "500 Server Error", request=MagicMock(), response=mock_response
    )
    mocker.patch.object(ext._http_client, "request", return_value=mock_response)
    
    # We expect extraction to fully fail (ExtractionError) because we have no cache
    with pytest.raises(ExtractionError, match="All 2 retries exhausted"):
        ext.run()

    # It should have called request exactly 2 times (the number of max_retries)
    assert ext._http_client.request.call_count == 2


class MockCrawler(BaseCrawler):
    SOURCE_NAME = "mock_crawler"

    def get_target_urls(self) -> list[str]:
        return ["https://mock-site.local"]

    def parse(self, html: str, url: str) -> list[dict]:
        return [{"parsed": "ok"}]


def test_base_crawler_cache_fallback(mocker, tmp_path) -> None:
    # Use tmp_path to mock the cache directory so it doesn't write to real 'data/cache'
    crawler = MockCrawler(max_retries=1, initial_backoff=0.01)
    crawler._cache_dir = tmp_path
    
    # Create fake cache fallback file
    cache_file = tmp_path / "latest.json"
    cache_file.write_text('[{"parsed": "from_cache", "_crawled_at": "mock"}]', encoding="utf-8")
    
    # Mock network connection error
    mocker.patch.object(crawler._http_client, "get", side_effect=httpx.ConnectError("Network Down"))

    # Should NOT raise an exception, but instead read from the cache file
    df = crawler.run()

    # The returned dataframe should be the one from the cache
    assert len(df) == 1
    assert df.iloc[0]["parsed"] == "from_cache"
