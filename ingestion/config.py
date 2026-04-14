"""
UrbanPulse VN — Ingestion Configuration.

Centralized configuration for all extractors and crawlers.
Contains API endpoints, crawler CSS selectors, Vietnamese city coordinates,
and river monitoring points.

All configurable values are defined here to avoid hardcoding in extractors.
CSS selectors for web crawlers MUST be defined here so they can be updated
without modifying crawler logic when target websites change their HTML.
"""

from dataclasses import dataclass, field

from utils.helper import get_env


# ---------------------------------------------------------------------------
# API Endpoints & Keys
# ---------------------------------------------------------------------------

OPENAQ_BASE_URL = "https://api.openaq.org/v3"
OPENAQ_API_KEY: str = get_env("OPENAQ_API_KEY", default="")

OPEN_METEO_WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_FLOOD_URL = "https://flood-api.open-meteo.com/v1/flood"

NASA_FIRMS_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
NASA_EARTHDATA_TOKEN: str = get_env("NASA_EARTHDATA_TOKEN", default="")

OSM_OVERPASS_URL = "https://overpass-api.de/api/interpreter"


# ---------------------------------------------------------------------------
# Web Crawler Selectors  (⚠️ update here when website HTML changes)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CEMSelectors:
    """CSS selectors for cem.gov.vn AQI data."""

    # Main table containing AQI station data
    station_table: str = "table.table-striped"
    station_rows: str = "table.table-striped tbody tr"
    # Columns within each row (by index or CSS)
    station_name: str = "td:nth-child(1)"
    aqi_value: str = "td:nth-child(2)"
    category: str = "td:nth-child(3)"
    timestamp: str = "td:nth-child(4)"


@dataclass(frozen=True)
class NCHMFSelectors:
    """CSS selectors for nchmf.gov.vn disaster warnings."""

    # Warning articles list
    warning_list: str = "div.news-list"
    warning_items: str = "div.news-list div.item"
    # Individual warning fields
    title: str = "h3 a"
    summary: str = "div.summary"
    publish_date: str = "span.date"
    detail_link: str = "h3 a"


CEM_SELECTORS = CEMSelectors()
NCHMF_SELECTORS = NCHMFSelectors()

CEM_BASE_URL = "https://cem.gov.vn"
NCHMF_BASE_URL = "https://nchmf.gov.vn"


# ---------------------------------------------------------------------------
# Vietnamese Cities — 63 provinces/cities with lat/lon
# ---------------------------------------------------------------------------

VIETNAM_CITIES: list[dict[str, float | str]] = [
    {"name": "Hà Nội", "lat": 21.0285, "lon": 105.8542},
    {"name": "Hồ Chí Minh", "lat": 10.8231, "lon": 106.6297},
    {"name": "Đà Nẵng", "lat": 16.0544, "lon": 108.2022},
    {"name": "Hải Phòng", "lat": 20.8449, "lon": 106.6881},
    {"name": "Cần Thơ", "lat": 10.0452, "lon": 105.7469},
    {"name": "An Giang", "lat": 10.3860, "lon": 105.4358},
    {"name": "Bà Rịa-Vũng Tàu", "lat": 10.5417, "lon": 107.2430},
    {"name": "Bắc Giang", "lat": 21.2731, "lon": 106.1946},
    {"name": "Bắc Kạn", "lat": 22.1473, "lon": 105.8348},
    {"name": "Bạc Liêu", "lat": 9.2940, "lon": 105.7216},
    {"name": "Bắc Ninh", "lat": 21.1861, "lon": 106.0763},
    {"name": "Bến Tre", "lat": 10.2434, "lon": 106.3756},
    {"name": "Bình Định", "lat": 13.7820, "lon": 109.2197},
    {"name": "Bình Dương", "lat": 11.3254, "lon": 106.4770},
    {"name": "Bình Phước", "lat": 11.7512, "lon": 106.7235},
    {"name": "Bình Thuận", "lat": 11.0904, "lon": 108.0721},
    {"name": "Cà Mau", "lat": 9.1527, "lon": 105.1961},
    {"name": "Cao Bằng", "lat": 22.6666, "lon": 106.2640},
    {"name": "Đắk Lắk", "lat": 12.7100, "lon": 108.2378},
    {"name": "Đắk Nông", "lat": 12.2646, "lon": 107.6098},
    {"name": "Điện Biên", "lat": 21.3860, "lon": 103.0163},
    {"name": "Đồng Nai", "lat": 10.9453, "lon": 106.8244},
    {"name": "Đồng Tháp", "lat": 10.4938, "lon": 105.6882},
    {"name": "Gia Lai", "lat": 13.9832, "lon": 108.0000},
    {"name": "Hà Giang", "lat": 22.8026, "lon": 104.9784},
    {"name": "Hà Nam", "lat": 20.5835, "lon": 105.9230},
    {"name": "Hà Tĩnh", "lat": 18.3560, "lon": 105.8876},
    {"name": "Hải Dương", "lat": 20.9373, "lon": 106.3146},
    {"name": "Hậu Giang", "lat": 9.7579, "lon": 105.6413},
    {"name": "Hòa Bình", "lat": 20.8171, "lon": 105.3378},
    {"name": "Hưng Yên", "lat": 20.6464, "lon": 106.0512},
    {"name": "Khánh Hòa", "lat": 12.2585, "lon": 109.0526},
    {"name": "Kiên Giang", "lat": 10.0125, "lon": 105.0809},
    {"name": "Kon Tum", "lat": 14.3498, "lon": 108.0005},
    {"name": "Lai Châu", "lat": 22.3964, "lon": 103.4703},
    {"name": "Lâm Đồng", "lat": 11.5753, "lon": 108.1429},
    {"name": "Lạng Sơn", "lat": 21.8460, "lon": 106.7614},
    {"name": "Lào Cai", "lat": 22.4856, "lon": 103.9707},
    {"name": "Long An", "lat": 10.5360, "lon": 106.4093},
    {"name": "Nam Định", "lat": 20.4389, "lon": 106.1621},
    {"name": "Nghệ An", "lat": 18.6737, "lon": 105.6813},
    {"name": "Ninh Bình", "lat": 20.2539, "lon": 105.9745},
    {"name": "Ninh Thuận", "lat": 11.5833, "lon": 108.9881},
    {"name": "Phú Thọ", "lat": 21.4220, "lon": 105.2298},
    {"name": "Phú Yên", "lat": 13.0882, "lon": 109.0929},
    {"name": "Quảng Bình", "lat": 17.4690, "lon": 106.6222},
    {"name": "Quảng Nam", "lat": 15.5394, "lon": 108.0191},
    {"name": "Quảng Ngãi", "lat": 15.1214, "lon": 108.8044},
    {"name": "Quảng Ninh", "lat": 21.0064, "lon": 107.2925},
    {"name": "Quảng Trị", "lat": 16.7504, "lon": 107.1855},
    {"name": "Sóc Trăng", "lat": 9.6024, "lon": 105.9739},
    {"name": "Sơn La", "lat": 21.3270, "lon": 103.9008},
    {"name": "Tây Ninh", "lat": 11.3352, "lon": 106.0987},
    {"name": "Thái Bình", "lat": 20.4463, "lon": 106.3366},
    {"name": "Thái Nguyên", "lat": 21.5942, "lon": 105.8482},
    {"name": "Thanh Hóa", "lat": 19.8067, "lon": 105.7852},
    {"name": "Thừa Thiên Huế", "lat": 16.4637, "lon": 107.5909},
    {"name": "Tiền Giang", "lat": 10.4493, "lon": 106.3420},
    {"name": "Trà Vinh", "lat": 9.9347, "lon": 106.3455},
    {"name": "Tuyên Quang", "lat": 21.8235, "lon": 105.2180},
    {"name": "Vĩnh Long", "lat": 10.2538, "lon": 105.9722},
    {"name": "Vĩnh Phúc", "lat": 21.3089, "lon": 105.6047},
    {"name": "Yên Bái", "lat": 21.7168, "lon": 104.8986},
]


# ---------------------------------------------------------------------------
# Vietnamese Rivers — Flood monitoring points
# ---------------------------------------------------------------------------

VIETNAM_RIVERS: list[dict[str, float | str]] = [
    {"name": "Sông Hồng (Hà Nội)", "lat": 21.0285, "lon": 105.8542},
    {"name": "Sông Hồng (Lào Cai)", "lat": 22.4856, "lon": 103.9707},
    {"name": "Sông Đà (Hòa Bình)", "lat": 20.8171, "lon": 105.3378},
    {"name": "Sông Mã (Thanh Hóa)", "lat": 19.8067, "lon": 105.7852},
    {"name": "Sông Cả (Nghệ An)", "lat": 18.6737, "lon": 105.6813},
    {"name": "Sông Hương (Huế)", "lat": 16.4637, "lon": 107.5909},
    {"name": "Sông Thu Bồn (Quảng Nam)", "lat": 15.5394, "lon": 108.0191},
    {"name": "Sông Ba (Phú Yên)", "lat": 13.0882, "lon": 109.0929},
    {"name": "Sông Đồng Nai (Đồng Nai)", "lat": 10.9453, "lon": 106.8244},
    {"name": "Sông Tiền (Tiền Giang)", "lat": 10.4493, "lon": 106.3420},
    {"name": "Sông Hậu (Cần Thơ)", "lat": 10.0452, "lon": 105.7469},
    {"name": "Sông Sài Gòn (HCM)", "lat": 10.8231, "lon": 106.6297},
]


# ---------------------------------------------------------------------------
# OSM Overpass — Vietnam bounding box
# ---------------------------------------------------------------------------

VIETNAM_BBOX: dict[str, float] = {
    "south": 8.18,
    "west": 102.14,
    "north": 23.39,
    "east": 109.46,
}


# ---------------------------------------------------------------------------
# Ingestion defaults
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IngestionDefaults:
    """Default parameters for ingestion pipeline."""

    # Retry / backoff
    max_retries: int = 3
    backoff_factor: int = 2
    initial_backoff_seconds: float = 5.0

    # Rate limiting (crawlers)
    request_delay_min: float = 2.0
    request_delay_max: float = 8.0

    # Cache
    cache_ttl_seconds: int = 3600

    # HTTP
    http_timeout_seconds: int = 30

    # Output
    default_file_format: str = "parquet"
    raw_bucket: str = "raw"


DEFAULTS = IngestionDefaults()
