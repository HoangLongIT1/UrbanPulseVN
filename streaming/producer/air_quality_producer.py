"""
UrbanPulse VN — Kafka Producer for Air Quality Data.

Polls OpenAQ API at regular intervals and publishes
air quality readings to the 'raw-air-quality' Kafka topic.

Usage:
    python -m streaming.producer.air_quality_producer
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone

import httpx
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_RAW = "raw-air-quality"
OPENAQ_API_URL = "https://api.openaq.org/v3/locations"
POLL_INTERVAL_SECONDS = 300  # 5 minutes
COUNTRY_CODE = "VN"


def create_producer() -> KafkaProducer:
    """Create a Kafka producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        retry_backoff_ms=500,
    )


def fetch_air_quality() -> list[dict]:
    """Fetch latest air quality data from OpenAQ for Vietnam."""
    try:
        with httpx.Client(timeout=30) as client:
            resp = client.get(
                OPENAQ_API_URL,
                params={
                    "country": COUNTRY_CODE,
                    "limit": 100,
                    "sort": "desc",
                    "order_by": "lastUpdated",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            logger.info("Fetched %d locations from OpenAQ", len(results))
            return results
    except httpx.HTTPError as e:
        logger.error("OpenAQ API error: %s", e)
        return []


def transform_to_messages(locations: list[dict]) -> list[dict]:
    """Transform OpenAQ API response into flat Kafka messages."""
    messages = []
    now = datetime.now(timezone.utc).isoformat()

    for loc in locations:
        location_id = loc.get("id", 0)
        location_name = loc.get("name", "Unknown")
        city = loc.get("city", "Unknown")
        coords = loc.get("coordinates", {})
        lat = coords.get("latitude", 0.0)
        lon = coords.get("longitude", 0.0)

        for param in loc.get("parameters", []):
            msg = {
                "location_id": location_id,
                "location_name": location_name,
                "city": city,
                "country": COUNTRY_CODE,
                "latitude": lat,
                "longitude": lon,
                "parameter": param.get("parameter", "unknown"),
                "value": param.get("lastValue", 0.0),
                "unit": param.get("unit", "µg/m³"),
                "date_utc": param.get("lastUpdated", now),
                "date_local": param.get("lastUpdated", now),
                "source": "openaq",
                "produced_at": now,
            }
            messages.append(msg)

    return messages


def publish_messages(producer: KafkaProducer, messages: list[dict]) -> int:
    """Publish messages to Kafka topic. Returns count of successfully sent."""
    sent = 0
    for msg in messages:
        try:
            key = f"{msg['location_id']}_{msg['parameter']}"
            future = producer.send(TOPIC_RAW, key=key, value=msg)
            future.get(timeout=10)
            sent += 1
        except KafkaError as e:
            logger.error("Failed to send message: %s", e)

    producer.flush()
    return sent


def run() -> None:
    """Main producer loop: poll API → publish to Kafka → sleep → repeat."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger.info("Starting Air Quality Kafka Producer")
    logger.info("  Topic: %s | Poll interval: %ds", TOPIC_RAW, POLL_INTERVAL_SECONDS)

    producer = create_producer()
    logger.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)

    try:
        while True:
            locations = fetch_air_quality()
            if locations:
                messages = transform_to_messages(locations)
                sent = publish_messages(producer, messages)
                logger.info(
                    "Published %d/%d messages to %s", sent, len(messages), TOPIC_RAW
                )
            else:
                logger.warning("No data fetched, will retry in %ds", POLL_INTERVAL_SECONDS)

            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    finally:
        producer.close()
        logger.info("Producer closed")


if __name__ == "__main__":
    run()
