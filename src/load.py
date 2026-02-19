"""
load.py — Snowflake Data Loading
==================================
TEACHING NOTE:
    The loader is ONLY responsible for writing data to Snowflake.
    It takes clean Python dicts and persists them — no business logic here.

    Key patterns demonstrated:
      1. Context manager pattern — guarantees connection cleanup even on errors
      2. Batch inserts — send N rows per round trip (not 1 at a time)
      3. Idempotent loading — MERGE/INSERT OR REPLACE prevents duplicate rows
      4. Separation of DDL and DML — table creation vs. data insertion are separate
      5. Transaction awareness — Snowflake auto-commits; explicit control shown
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Iterator, Any

import snowflake.connector
from snowflake.connector import DictCursor

from src.config import SnowflakeConfig

logger = logging.getLogger(__name__)


class SnowflakeConnectionError(Exception):
    """Raised when Snowflake connection or authentication fails."""
    pass


class SnowflakeLoader:
    """
    Loads data into Snowflake Bronze landing tables.

    Uses the official snowflake-connector-python library.
    All writes go to the BRONZE schema raw tables as VARIANT (JSON).

    Example:
        with SnowflakeLoader(config.snowflake) as loader:
            loader.load_movies(movie_records, batch_id="abc-123")
    """

    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self.conn = None
        self.batch_id = str(uuid.uuid4())  # Unique ID for this pipeline run
        logger.info(f"SnowflakeLoader created. Batch ID: {self.batch_id}")

    def connect(self) -> None:
        """
        Establish a connection to Snowflake.

        TEACHING NOTE:
            Connection parameters are explicit — never rely on implicit defaults
            for database/schema/warehouse in production code. If those defaults
            change in the account, your pipeline silently writes to the wrong place.
        """
        logger.info(
            f"Connecting to Snowflake: account={self.config.account}, "
            f"user={self.config.user}, db={self.config.database}"
        )
        try:
            self.conn = snowflake.connector.connect(
                account=self.config.account,
                user=self.config.user,
                password=self.config.password,
                role=self.config.role,
                warehouse=self.config.warehouse,
                database=self.config.database,
                schema=self.config.schema,
                # Session parameters for safety and performance
                session_parameters={
                    "QUERY_TAG": f"python_pipeline_{self.batch_id}",
                    "TIMEZONE": "UTC",
                },
            )
            logger.info("Snowflake connection established successfully")
        except snowflake.connector.errors.DatabaseError as e:
            raise SnowflakeConnectionError(f"Failed to connect to Snowflake: {e}") from e

    def disconnect(self) -> None:
        """Close the Snowflake connection and release resources."""
        if self.conn and not self.conn.is_closed():
            self.conn.close()
            logger.info("Snowflake connection closed")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        # Return False to propagate exceptions (don't swallow errors)
        return False

    def _execute(self, sql: str, params: list | None = None) -> None:
        """
        Execute a single SQL statement.
        Creates a fresh cursor per statement (Snowflake best practice).
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, params or [])

    def _executemany(self, sql: str, rows: list[tuple]) -> int:
        """
        Execute a SQL statement for multiple rows in a single round-trip.

        TEACHING NOTE:
            NEVER do this in a loop:
                for row in rows:
                    cursor.execute(INSERT, row)   ← N round trips = slow!

            Do this instead:
                cursor.executemany(INSERT, all_rows)  ← 1 round trip = fast

            For very large loads (millions of rows), use COPY INTO with a stage.
            executemany is perfect for thousands of rows.

        Returns:
            Number of rows inserted
        """
        with self.conn.cursor() as cur:
            cur.executemany(sql, rows)
            return cur.rowcount

    def ensure_tables_exist(self) -> None:
        """
        Create Bronze tables if they don't exist.
        Safe to call on every pipeline run — idempotent due to IF NOT EXISTS.
        """
        logger.info("Ensuring Bronze tables exist")

        self._execute("""
            CREATE TABLE IF NOT EXISTS BRONZE.RAW_MOVIES (
                MOVIE_ID     NUMBER        NOT NULL,
                RAW_DATA     VARIANT       NOT NULL,
                SOURCE_URL   VARCHAR(500),
                INGESTED_AT  TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
                BATCH_ID     VARCHAR(36)
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS BRONZE.RAW_GENRES (
                RAW_DATA     VARIANT       NOT NULL,
                INGESTED_AT  TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
                BATCH_ID     VARCHAR(36)
            )
        """)

        logger.info("Bronze tables verified/created")

    def load_movies(self, movies: list[dict], batch_size: int = 100) -> int:
        """
        Load a list of movie dicts into BRONZE.RAW_MOVIES.

        TEACHING NOTE — Idempotency:
            We use MERGE (upsert) instead of plain INSERT.
            This means if the pipeline crashes and restarts, re-running it
            does NOT create duplicate rows — it just updates existing ones.
            This property is called "idempotency" and is critical in pipelines
            because failures and retries are normal, expected events.

        Args:
            movies:     List of raw movie dicts from TMDb API
            batch_size: Number of rows per Snowflake round-trip

        Returns:
            Total number of rows inserted/updated
        """
        if not movies:
            logger.warning("load_movies called with empty list. Nothing to load.")
            return 0

        ingested_at = datetime.now(timezone.utc).isoformat()
        source_url = "https://api.themoviedb.org/3/movie/{id}"
        total_loaded = 0

        # TEACHING NOTE: PARSE_JSON() converts a JSON string into a VARIANT column.
        # We pass the JSON as a string and let Snowflake parse it — this is more
        # reliable than using Python to build Snowflake VARIANT directly.
        merge_sql = """
            MERGE INTO BRONZE.RAW_MOVIES AS target
            USING (
                SELECT
                    %s::NUMBER        AS movie_id,
                    PARSE_JSON(%s)    AS raw_data,
                    %s::VARCHAR       AS source_url,
                    %s::TIMESTAMP_NTZ AS ingested_at,
                    %s::VARCHAR       AS batch_id
            ) AS source
            ON target.MOVIE_ID = source.movie_id
            WHEN MATCHED THEN
                UPDATE SET
                    target.RAW_DATA     = source.raw_data,
                    target.SOURCE_URL   = source.source_url,
                    target.INGESTED_AT  = source.ingested_at,
                    target.BATCH_ID     = source.batch_id
            WHEN NOT MATCHED THEN
                INSERT (MOVIE_ID, RAW_DATA, SOURCE_URL, INGESTED_AT, BATCH_ID)
                VALUES (source.movie_id, source.raw_data, source.source_url,
                        source.ingested_at, source.batch_id)
        """

        # Process in batches to avoid memory issues with very large datasets
        for batch_start in range(0, len(movies), batch_size):
            batch = movies[batch_start: batch_start + batch_size]

            rows = [
                (
                    movie.get("id"),
                    json.dumps(movie),   # Serialize dict → JSON string
                    source_url,
                    ingested_at,
                    self.batch_id,
                )
                for movie in batch
                if movie.get("id")  # Skip malformed records missing an ID
            ]

            if rows:
                for row in rows:
                    self._execute(merge_sql, list(row))
                    total_loaded += 1

            logger.info(
                f"Loaded batch {batch_start // batch_size + 1}: "
                f"{len(rows)} movies (total so far: {total_loaded})"
            )

        logger.info(f"Movie load complete. Total rows loaded: {total_loaded}")
        return total_loaded

    def load_genres(self, genres: list[dict]) -> int:
        """
        Load the genre reference list into BRONZE.RAW_GENRES.

        Genres rarely change, so we insert a fresh snapshot each run
        (rather than merging) — simple and auditable.
        """
        if not genres:
            return 0

        ingested_at = datetime.now(timezone.utc).isoformat()

        insert_sql = """
            INSERT INTO BRONZE.RAW_GENRES (RAW_DATA, INGESTED_AT, BATCH_ID)
            SELECT PARSE_JSON(%s), %s::TIMESTAMP_NTZ, %s
        """

        # Store all genres as a single JSON array row
        payload = json.dumps(genres)
        self._execute(insert_sql, [payload, ingested_at, self.batch_id])

        logger.info(f"Loaded {len(genres)} genres into BRONZE.RAW_GENRES")
        return 1

    def get_row_count(self, table: str) -> int:
        """
        Return current row count for a table.
        Used for post-load validation — verify data actually arrived.
        """
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            return cur.fetchone()[0]

    def run_post_load_checks(self) -> None:
        """
        Simple sanity checks after loading.

        TEACHING NOTE:
            Always validate after loading! Silent failures (wrong row count,
            NULLs where they shouldn't be) are harder to debug than loud failures.
            In production, these checks feed into a data quality dashboard.
        """
        logger.info("Running post-load data quality checks...")

        movies_count = self.get_row_count("BRONZE.RAW_MOVIES")
        genres_count = self.get_row_count("BRONZE.RAW_GENRES")

        logger.info(f"  BRONZE.RAW_MOVIES row count: {movies_count}")
        logger.info(f"  BRONZE.RAW_GENRES row count: {genres_count}")

        if movies_count == 0:
            logger.error("QUALITY CHECK FAILED: BRONZE.RAW_MOVIES is empty after load!")
        else:
            logger.info(f"QUALITY CHECK PASSED: {movies_count} movies in Bronze")
