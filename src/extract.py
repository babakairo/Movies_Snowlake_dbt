"""
extract.py — TMDb API Data Extraction
======================================
TEACHING NOTE:
    The extractor is ONLY responsible for getting data from the source.
    It does NOT transform, clean, or load data — single responsibility principle.

    Key patterns demonstrated:
      1. Rate limiting respect — TMDb allows 40 requests/10 seconds
      2. Exponential backoff with jitter — handles transient 429/5xx errors
      3. Pagination — APIs rarely return all data in one response
      4. Session reuse — one HTTP connection pool for all requests (faster)
      5. Generator pattern — yields pages lazily (memory-efficient for large APIs)
"""

import json
import logging
import time
from typing import Iterator, Any

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from src.config import TMDbConfig

logger = logging.getLogger(__name__)

# TMDb free tier: 40 requests per 10 seconds
RATE_LIMIT_REQUESTS = 40
RATE_LIMIT_WINDOW_SECONDS = 10
# Conservative delay between requests to stay within limits
REQUEST_DELAY_SECONDS = 0.26  # ~38 req/10s — safely below limit


class TMDbAPIError(Exception):
    """Raised when the TMDb API returns an unexpected error response."""
    pass


class TMDbExtractor:
    """
    Extracts data from the TMDb REST API.

    Design decisions:
      - Uses requests.Session() for connection pooling (10x faster than new connections)
      - All API calls go through _get() which handles auth, errors, and retries
      - Data is yielded as raw Python dicts (parsed JSON) — no transformation here

    Example:
        extractor = TMDbExtractor(config.tmdb)
        for movie in extractor.extract_popular_movies(pages=10):
            print(movie['title'])
    """

    def __init__(self, config: TMDbConfig):
        self.config = config
        self.session = self._build_session()
        logger.info(f"TMDbExtractor initialized. Base URL: {config.base_url}")

    def _build_session(self) -> requests.Session:
        """
        Create a reusable HTTP session with default headers.
        TEACHING NOTE: Sessions maintain a connection pool — reusing TCP connections
        is significantly faster than opening a new connection for every request.

        TMDb authentication:
          v3 API Key  (short hex string) → passed as ?api_key=xxx query parameter
          v4 Token    (long JWT)         → passed as Authorization: Bearer header
        We use v3 here since that is what a standard free-tier API key registration gives you.
        """
        session = requests.Session()
        session.headers.update({
            "Content-Type": "application/json;charset=utf-8",
            "Accept": "application/json",
        })
        return session

    @retry(
        # Retry up to 5 times before giving up
        stop=stop_after_attempt(5),
        # Exponential backoff: 1s → 2s → 4s → 8s → 16s (with jitter)
        wait=wait_exponential(multiplier=1, min=1, max=30),
        # Only retry on these transient errors (not on 401 auth errors!)
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError, TMDbAPIError)),
        # Log each retry attempt so we can see what's happening
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict:
        """
        Make a single GET request to the TMDb API with retry logic.

        Args:
            endpoint: API path, e.g. "/movie/popular"
            params:   Query parameters dict (api_key injected automatically)

        Returns:
            Parsed JSON response as a Python dict

        Raises:
            TMDbAPIError: If API returns a non-retryable error (400, 401, 404)
        """
        url = f"{self.config.base_url}{endpoint}"
        # Inject the v3 API key into every request as a query parameter
        params = {"api_key": self.config.api_key, **(params or {})}

        # Respect rate limits — sleep between requests
        time.sleep(REQUEST_DELAY_SECONDS)

        try:
            response = self.session.get(url, params=params, timeout=15)

            # 429 = Too Many Requests — retryable
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 10))
                logger.warning(f"Rate limited by TMDb API. Waiting {retry_after}s")
                time.sleep(retry_after)
                raise TMDbAPIError(f"Rate limited (429). Will retry.")

            # 5xx = Server errors — retryable
            if response.status_code >= 500:
                raise TMDbAPIError(f"TMDb server error {response.status_code}. Will retry.")

            # 4xx (except 429) = Client errors — NOT retryable (bug in our code)
            if response.status_code >= 400:
                raise ValueError(
                    f"TMDb client error {response.status_code} for {url}: {response.text}"
                )

            response.raise_for_status()
            return response.json()

        except requests.Timeout:
            logger.warning(f"Request to {url} timed out. Will retry.")
            raise

    def extract_popular_movies(self, pages: int | None = None) -> Iterator[dict]:
        """
        Extract popular movies from TMDb with pagination.

        TEACHING NOTE:
            Most APIs paginate results (return them in pages).
            We use a generator (yield) instead of returning a list because:
            - Memory efficient: we don't hold all pages in RAM simultaneously
            - Streaming: the caller can start processing page 1 while we fetch page 2
            - Composable: generators chain well with other generators/iterators

        Args:
            pages: Number of pages to fetch (each page = 20 movies).
                   Defaults to config value.

        Yields:
            Raw movie dict (list-level data — limited fields, no budget/revenue)
        """
        pages_to_fetch = pages or self.config.pages_to_fetch
        logger.info(f"Extracting popular movies: {pages_to_fetch} pages × 20 = ~{pages_to_fetch * 20} movies")

        for page in range(1, pages_to_fetch + 1):
            logger.debug(f"Fetching popular movies page {page}/{pages_to_fetch}")

            data = self._get("/movie/popular", params={"page": page, "language": "en-US"})

            movies_on_page = data.get("results", [])
            total_pages = data.get("total_pages", 1)

            for movie in movies_on_page:
                yield movie

            # Respect API total pages — don't request beyond what exists
            if page >= total_pages:
                logger.info(f"Reached last page ({total_pages}). Stopping.")
                break

        logger.info(f"Finished extracting popular movies list.")

    def extract_movie_detail(self, movie_id: int) -> dict:
        """
        Fetch full detail for a single movie — includes budget, revenue, runtime,
        production companies, genres, etc.

        TEACHING NOTE:
            The /movie/popular endpoint returns ~20 fields.
            The /movie/{id} endpoint returns ~40+ fields.
            We fetch detail for each movie to get the rich data needed for analytics.
            This is the "enrichment" pattern: get IDs cheaply, then hydrate them.

        Args:
            movie_id: The TMDb movie ID

        Returns:
            Full movie detail dict
        """
        logger.debug(f"Fetching detail for movie_id={movie_id}")
        return self._get(
            f"/movie/{movie_id}",
            params={"language": "en-US", "append_to_response": "keywords"}
        )

    def extract_genre_list(self) -> list[dict]:
        """
        Fetch the complete genre reference list from TMDb.

        Returns:
            List of genre dicts: [{"id": 28, "name": "Action"}, ...]
        """
        logger.info("Fetching genre reference list")
        data = self._get("/genre/movie/list", params={"language": "en-US"})
        genres = data.get("genres", [])
        logger.info(f"Fetched {len(genres)} genres")
        return genres

    def extract_movies_with_details(self, pages: int | None = None) -> Iterator[dict]:
        """
        Full extraction pipeline: popular movie list → individual detail for each.

        This is the main method called by the pipeline. It:
          1. Gets the list of popular movies (fast — batch endpoint)
          2. For each movie, fetches full details (slower — one request each)
          3. Yields fully-detailed movie dicts

        TEACHING NOTE:
            In production you'd parallelize step 2 using a thread pool or async.
            For learning, sequential is clearer and avoids rate limit complexity.
        """
        seen_ids: set[int] = set()
        total_extracted = 0

        for list_movie in self.extract_popular_movies(pages=pages):
            movie_id = list_movie.get("id")

            # Skip duplicates that may appear across pages
            if movie_id in seen_ids:
                logger.debug(f"Skipping duplicate movie_id={movie_id}")
                continue
            seen_ids.add(movie_id)

            try:
                detail = self.extract_movie_detail(movie_id)
                total_extracted += 1

                if total_extracted % 50 == 0:
                    logger.info(f"Progress: {total_extracted} movies extracted so far")

                yield detail

            except (ValueError, TMDbAPIError) as e:
                # Log and skip — don't let one bad movie kill the entire pipeline
                logger.error(f"Failed to fetch detail for movie_id={movie_id}: {e}")
                continue

        logger.info(f"Extraction complete. Total movies extracted: {total_extracted}")

    def close(self) -> None:
        """Close the HTTP session and release connection pool resources."""
        self.session.close()
        logger.debug("HTTP session closed")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
