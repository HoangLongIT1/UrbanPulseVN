"""
UrbanPulse VN — Pytest Configuration.

Shared fixtures for unit and integration tests.
"""

import pytest


@pytest.fixture
def sample_air_quality_data():
    """Sample air quality data for testing."""
    return {
        "location": "Hanoi",
        "parameter": "pm25",
        "value": 45.2,
        "unit": "µg/m³",
        "date": "2024-04-12T10:00:00+07:00",
        "country": "VN",
    }


@pytest.fixture
def sample_weather_data():
    """Sample weather data for testing."""
    return {
        "city": "Ho Chi Minh City",
        "latitude": 10.8231,
        "longitude": 106.6297,
        "temperature_2m": 32.5,
        "relative_humidity_2m": 75,
        "precipitation": 0.0,
        "wind_speed_10m": 12.3,
    }
