"""
UrbanPulse VN — Redis Client Utility.

Provides a Redis client for caching operations, primarily used by
web crawlers as a fallback cache when source websites are down.

Usage:
    from utils.redis_client import RedisClient

    cache = RedisClient()
    cache.set_json("cem_aqi_latest", data, ttl=3600)
    cached = cache.get_json("cem_aqi_latest")
"""

import os
import json
import logging
from typing import Any

import redis

logger = logging.getLogger(__name__)

# Default TTL: 1 hour (crawler cache)
DEFAULT_TTL = 3600


class RedisClient:
    """Redis client for caching operations."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        db: int = 0,
        decode_responses: bool = True,
    ):
        self.host = host or os.getenv("REDIS_HOST", "localhost")
        self.port = port or int(os.getenv("REDIS_PORT", "6379"))
        self.db = db

        self._client = redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            decode_responses=decode_responses,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        logger.info("Redis client initialized: %s:%s db=%d", self.host, self.port, self.db)

    def ping(self) -> bool:
        """Check Redis connectivity."""
        try:
            return self._client.ping()
        except redis.ConnectionError:
            logger.error("Redis connection failed: %s:%s", self.host, self.port)
            return False

    def get(self, key: str) -> str | None:
        """Get a string value by key."""
        return self._client.get(key)

    def set(self, key: str, value: str, ttl: int = DEFAULT_TTL) -> bool:
        """Set a string value with TTL (seconds)."""
        return self._client.setex(key, ttl, value)

    def get_json(self, key: str) -> Any | None:
        """Get a JSON-deserialized value by key."""
        raw = self._client.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Failed to decode JSON for key: %s", key)
            return None

    def set_json(self, key: str, value: Any, ttl: int = DEFAULT_TTL) -> bool:
        """Set a JSON-serialized value with TTL."""
        try:
            serialized = json.dumps(value, ensure_ascii=False, default=str)
            return self._client.setex(key, ttl, serialized)
        except (TypeError, json.JSONEncodeError) as e:
            logger.error("Failed to serialize JSON for key %s: %s", key, e)
            return False

    def delete(self, key: str) -> int:
        """Delete a key. Returns number of keys deleted."""
        return self._client.delete(key)

    def exists(self, key: str) -> bool:
        """Check if a key exists."""
        return bool(self._client.exists(key))

    def ttl(self, key: str) -> int:
        """Get remaining TTL for a key in seconds. -1 if no TTL, -2 if not exists."""
        return self._client.ttl(key)

    def flush_db(self) -> None:
        """Flush the current database. ⚠️ Use with caution!"""
        self._client.flushdb()
        logger.warning("Redis DB %d flushed!", self.db)

    def close(self) -> None:
        """Close the Redis connection."""
        self._client.close()
        logger.info("Redis connection closed")
