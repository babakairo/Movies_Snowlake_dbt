"""
config.py — Centralised configuration management
=================================================
TEACHING NOTE:
    A config module is the ONLY place that reads environment variables.
    All other modules import from here — they never call os.getenv() directly.
    Benefits:
      1. Single point of change if you rename a variable
      2. Validation happens once, not scattered across files
      3. Easy to unit-test by mocking this module
      4. Clear documentation of ALL required settings

    We use python-dotenv so developers can run locally with a .env file
    while production uses real environment variables injected by the scheduler.
"""

import os
import logging
from dataclasses import dataclass, field
from dotenv import load_dotenv

# Load .env file if it exists (does nothing if running in CI/CD with real env vars)
load_dotenv()

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TMDbConfig:
    """Configuration for the TMDb API client."""
    api_key: str
    base_url: str
    pages_to_fetch: int


@dataclass(frozen=True)
class SnowflakeConfig:
    """Configuration for the Snowflake connection."""
    account: str
    user: str
    password: str
    role: str
    warehouse: str
    database: str
    schema: str


@dataclass(frozen=True)
class PipelineConfig:
    """General pipeline behaviour configuration."""
    batch_size: int
    log_level: str
    tmdb: TMDbConfig = field(default=None)
    snowflake: SnowflakeConfig = field(default=None)


def _require(key: str) -> str:
    """
    Fetch a required environment variable.
    Raises a clear error if it is missing — fail fast, fail loudly.
    Never silently use None for a required credential.
    """
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            f"Check your .env file or environment configuration."
        )
    return value


def _optional(key: str, default: str) -> str:
    """Fetch an optional environment variable with a safe default."""
    return os.getenv(key, default)


def load_config() -> PipelineConfig:
    """
    Load and validate all configuration from environment variables.

    Returns a fully-populated PipelineConfig. Raises EnvironmentError
    if any required variable is missing.

    Usage:
        from src.config import load_config
        cfg = load_config()
        print(cfg.tmdb.api_key)
    """
    logger.debug("Loading configuration from environment variables")

    tmdb_config = TMDbConfig(
        api_key=_require("TMDB_API_KEY"),
        base_url=_optional("TMDB_BASE_URL", "https://api.themoviedb.org/3"),
        pages_to_fetch=int(_optional("TMDB_PAGES_TO_FETCH", "20")),
    )

    snowflake_config = SnowflakeConfig(
        account=_require("SNOWFLAKE_ACCOUNT"),
        user=_require("SNOWFLAKE_USER"),
        password=_require("SNOWFLAKE_PASSWORD"),
        role=_optional("SNOWFLAKE_ROLE", "DE_ROLE"),
        warehouse=_optional("SNOWFLAKE_WAREHOUSE", "LECTURE_INGEST_WH"),
        database=_optional("SNOWFLAKE_DATABASE", "LECTURE_DE"),
        schema=_optional("SNOWFLAKE_SCHEMA", "BRONZE"),
    )

    pipeline_config = PipelineConfig(
        batch_size=int(_optional("BATCH_SIZE", "100")),
        log_level=_optional("LOG_LEVEL", "INFO"),
        tmdb=tmdb_config,
        snowflake=snowflake_config,
    )

    logger.info(
        "Configuration loaded successfully. "
        f"TMDb pages: {tmdb_config.pages_to_fetch}, "
        f"Snowflake DB: {snowflake_config.database}"
    )

    return pipeline_config


def setup_logging(log_level: str = "INFO") -> None:
    """
    Configure structured logging for the pipeline.

    TEACHING NOTE:
        Always set up logging before anything else runs.
        Use a consistent format so logs are parseable by log aggregators
        (Datadog, Splunk, CloudWatch). ISO 8601 timestamps are universal.
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    # Silence noisy third-party loggers
    logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
