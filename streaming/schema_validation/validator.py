"""
UrbanPulse VN — Kafka Schema Validator Consumer.

Consumes from 'raw-air-quality', validates each message against
the Avro schema, then routes:
  - Valid   → 'processed-air-quality'
  - Invalid → 'dlq-air-quality' (Dead Letter Queue)

Usage:
    python -m streaming.schema_validation.validator
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_RAW = "raw-air-quality"
TOPIC_PROCESSED = "processed-air-quality"
TOPIC_DLQ = "dlq-air-quality"
CONSUMER_GROUP = "urbanpulse-validator"

# ── Schema definition (matches air_quality.avsc) ───
REQUIRED_FIELDS = {
    "location_id": int,
    "location_name": str,
    "city": str,
    "latitude": (int, float),
    "longitude": (int, float),
    "parameter": str,
    "value": (int, float),
    "date_utc": str,
}

VALID_POLLUTANTS = {"pm25", "pm10", "o3", "no2", "so2", "co"}


def validate_message(msg: dict[str, Any]) -> tuple[bool, str]:
    """Validate a message against the air quality schema.

    Returns:
        (is_valid, error_reason)
    """
    # Check required fields exist and have correct types
    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in msg:
            return False, f"Missing required field: {field}"
        if not isinstance(msg[field], expected_type):
            return False, f"Invalid type for {field}: expected {expected_type}, got {type(msg[field])}"

    # Validate pollutant enum
    if msg["parameter"] not in VALID_POLLUTANTS:
        return False, f"Invalid pollutant: {msg['parameter']}. Must be one of {VALID_POLLUTANTS}"

    # Validate value range (non-negative)
    if msg["value"] < 0:
        return False, f"Negative measurement value: {msg['value']}"

    # Validate coordinates (roughly Vietnam bounding box)
    lat, lon = msg["latitude"], msg["longitude"]
    if not (8.0 <= lat <= 24.0 and 102.0 <= lon <= 110.0):
        return False, f"Coordinates outside Vietnam: ({lat}, {lon})"

    return True, ""


def create_consumer() -> KafkaConsumer:
    """Create a Kafka consumer for the raw topic."""
    return KafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        max_poll_interval_ms=300000,
    )


def create_producer() -> KafkaProducer:
    """Create a Kafka producer for routing messages."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )


def run() -> None:
    """Main validator loop: consume → validate → route."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger.info("Starting Schema Validator Consumer")
    logger.info("  Input:  %s", TOPIC_RAW)
    logger.info("  Valid → %s | Invalid → %s", TOPIC_PROCESSED, TOPIC_DLQ)

    consumer = create_consumer()
    producer = create_producer()

    valid_count = 0
    invalid_count = 0

    try:
        for record in consumer:
            msg = record.value

            is_valid, error_reason = validate_message(msg)

            if is_valid:
                # Add validation metadata and forward
                msg["_validated_at"] = datetime.now(timezone.utc).isoformat()
                msg["_validation_status"] = "valid"
                producer.send(TOPIC_PROCESSED, value=msg)
                valid_count += 1
            else:
                # Send to DLQ with error info
                dlq_msg = {
                    "original_message": msg,
                    "error_reason": error_reason,
                    "rejected_at": datetime.now(timezone.utc).isoformat(),
                    "source_topic": TOPIC_RAW,
                    "source_partition": record.partition,
                    "source_offset": record.offset,
                }
                producer.send(TOPIC_DLQ, value=dlq_msg)
                invalid_count += 1
                logger.warning("DLQ: %s (offset=%d)", error_reason, record.offset)

            # Log progress periodically
            total = valid_count + invalid_count
            if total % 100 == 0 and total > 0:
                logger.info(
                    "Processed %d messages: %d valid, %d invalid (%.1f%% pass rate)",
                    total, valid_count, invalid_count,
                    valid_count / total * 100,
                )

            producer.flush()

    except KeyboardInterrupt:
        logger.info("Validator stopped by user")
    finally:
        consumer.close()
        producer.close()
        total = valid_count + invalid_count
        logger.info(
            "Final stats: %d total, %d valid, %d invalid",
            total, valid_count, invalid_count,
        )


if __name__ == "__main__":
    run()
