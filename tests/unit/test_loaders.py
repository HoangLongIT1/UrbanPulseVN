"""
Unit tests for MinIO and Postgres Loaders.
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from ingestion.loaders.minio_loader import MinIOLoader
from ingestion.loaders.postgres_loader import PostgresLoader


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})


def test_minio_loader_success(mocker, sample_df) -> None:
    # Mock the internal MinIO client to avoid reaching real network
    mock_minio_client = mocker.patch("ingestion.loaders.minio_loader.MinIOClient")
    # Tell the mock exactly what to return when upload_dataframe is called
    mock_instance = mock_minio_client.return_value
    mock_instance.upload_dataframe.return_value = "fake_path.parquet"

    loader = MinIOLoader(bucket="test_bucket")
    
    # Assert client ensures bucket exists during instantiation
    mock_instance.ensure_buckets.assert_called_once_with(["test_bucket"])

    # Test load
    obj_path = loader.load("test_source", sample_df)
    
    # Assert the mock was called correctly and returned the mock string
    assert obj_path == "fake_path.parquet"
    mock_instance.upload_dataframe.assert_called_once()


def test_minio_loader_empty_df_skipped(mocker) -> None:
    mock_minio_client = mocker.patch("ingestion.loaders.minio_loader.MinIOClient")
    loader = MinIOLoader(bucket="test_bucket")

    empty_df = pd.DataFrame()
    path = loader.load("test_source", empty_df)

    assert path == ""
    mock_minio_client.return_value.upload_dataframe.assert_not_called()


def test_postgres_loader_logging(mocker) -> None:
    mock_connect = mocker.patch("ingestion.loaders.postgres_loader.psycopg2.connect")
    
    # Mock cursor context manager
    mock_conn = mock_connect.return_value
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    loader = PostgresLoader()
    
    loader.log_run("mock_source", 5, "mock/path.parquet", "SUCCESS")
    
    # Assert insert query was called with the right data
    called_args = mock_cursor.execute.call_args_list[-1]
    query, params = called_args[0]
    
    assert "INSERT INTO ingestion_logs" in query
    assert params == ("mock_source", 5, "mock/path.parquet", "SUCCESS")

    loader.close()
    mock_conn.close.assert_called_once()
