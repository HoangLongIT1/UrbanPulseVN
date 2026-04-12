"""
UrbanPulse VN — PostgreSQL Client Utility.

Provides a connection-pooled PostgreSQL client for all pipeline stages.
Supports context manager, batch operations, and schema-aware queries.

Usage:
    from utils.postgresql_client import PostgreSQLClient

    with PostgreSQLClient() as db:
        rows = db.fetch_all("SELECT * FROM bronze.air_quality LIMIT 10")
"""

import os
import logging
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool, extras

logger = logging.getLogger(__name__)


class PostgreSQLClient:
    """Thread-safe PostgreSQL client with connection pooling."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        user: str | None = None,
        password: str | None = None,
        min_connections: int = 1,
        max_connections: int = 5,
    ):
        self.host = host or os.getenv("POSTGRES_HOST", "localhost")
        self.port = port or int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = database or os.getenv("POSTGRES_DB", "urbanpulse")
        self.user = user or os.getenv("POSTGRES_USER", "urbanpulse")
        self.password = password or os.getenv("POSTGRES_PASSWORD", "urbanpulse_dev_2024")

        self._pool = pool.ThreadedConnectionPool(
            minconn=min_connections,
            maxconn=max_connections,
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )
        logger.info(
            "PostgreSQL connection pool created: %s@%s:%s/%s",
            self.user, self.host, self.port, self.database,
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close all connections in the pool."""
        if self._pool:
            self._pool.closeall()
            logger.info("PostgreSQL connection pool closed")

    @contextmanager
    def _get_connection(self):
        """Get a connection from the pool (context manager)."""
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    def execute(self, query: str, params: tuple | None = None) -> None:
        """Execute a single SQL statement (INSERT, UPDATE, DELETE, DDL)."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                logger.debug("Executed: %s", query[:100])

    def fetch_one(self, query: str, params: tuple | None = None) -> dict | None:
        """Fetch a single row as a dictionary."""
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute(query, params)
                return cur.fetchone()

    def fetch_all(self, query: str, params: tuple | None = None) -> list[dict]:
        """Fetch all rows as a list of dictionaries."""
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute(query, params)
                return cur.fetchall()

    def execute_batch(
        self, query: str, data: list[tuple], page_size: int = 1000
    ) -> None:
        """Execute a batch INSERT/UPDATE using execute_values for performance."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                extras.execute_batch(cur, query, data, page_size=page_size)
                logger.info("Batch executed: %d rows", len(data))

    def execute_values(
        self, query: str, data: list[tuple], page_size: int = 1000
    ) -> None:
        """Execute INSERT using execute_values (fastest for bulk inserts)."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                extras.execute_values(cur, query, data, page_size=page_size)
                logger.info("Values inserted: %d rows", len(data))

    def table_exists(self, table_name: str, schema: str = "public") -> bool:
        """Check if a table exists in the given schema."""
        result = self.fetch_one(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            ) AS exists
            """,
            (schema, table_name),
        )
        return result["exists"] if result else False

    def get_schemas(self) -> list[str]:
        """List all non-system schemas."""
        rows = self.fetch_all(
            """
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY schema_name
            """
        )
        return [row["schema_name"] for row in rows]
