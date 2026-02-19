"""
main.py — Pipeline Orchestrator
=================================
TEACHING NOTE:
    main.py is the entry point and orchestrator.
    It knows WHAT to do and in what ORDER, but delegates HOW to the modules.
    It should be thin — just coordination logic, no business rules.

    This follows the "Hollywood Principle": Don't call us, we'll call you.
    main.py calls extract, then load — each module doesn't know about the others.

    Pipeline steps:
        1. Load configuration (fail fast if misconfigured)
        2. Extract genres from TMDb API
        3. Extract movie details from TMDb API (paginated)
        4. Load genres to Snowflake Bronze
        5. Load movies to Snowflake Bronze
        6. Run post-load quality checks
        7. Log summary stats

Run with:
    python -m src.main
    python -m src.main --pages 5   (for a quick test run)
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from src.config import load_config, setup_logging
from src.extract import TMDbExtractor
from src.load import SnowflakeLoader

console = Console()
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for flexible pipeline runs."""
    parser = argparse.ArgumentParser(
        description="TMDb Movie Data Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.main                  # Full run (pages from .env)
  python -m src.main --pages 2        # Quick test: fetch 2 pages (~40 movies)
  python -m src.main --dry-run        # Extract only, do not write to Snowflake
        """
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=None,
        help="Number of TMDb pages to fetch (overrides TMDB_PAGES_TO_FETCH env var)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Extract data but do NOT write to Snowflake. Useful for testing."
    )
    return parser.parse_args()


def run_pipeline(pages: int | None = None, dry_run: bool = False) -> dict:
    """
    Execute the full ingestion pipeline.

    Args:
        pages:   Override number of API pages to fetch
        dry_run: If True, skip the Snowflake load step

    Returns:
        Summary stats dict
    """
    pipeline_start = time.time()

    # ─── STEP 1: Configuration ────────────────────────────────────────────────
    console.print(Panel.fit(
        "[bold cyan]TMDb Movie Data Engineering Pipeline[/bold cyan]\n"
        "[dim]Bronze Layer Ingestion[/dim]",
        border_style="cyan"
    ))

    logger.info("=" * 60)
    logger.info("PIPELINE STARTED")
    logger.info("=" * 60)

    cfg = load_config()
    pages_to_fetch = pages or cfg.tmdb.pages_to_fetch
    logger.info(f"Configuration: pages={pages_to_fetch}, dry_run={dry_run}")

    stats = {
        "start_time": datetime.now(timezone.utc).isoformat(),
        "pages_fetched": 0,
        "movies_extracted": 0,
        "genres_extracted": 0,
        "movies_loaded": 0,
        "genres_loaded": 0,
        "errors": 0,
        "duration_seconds": 0,
    }

    # ─── STEP 2: Extraction ───────────────────────────────────────────────────
    movies: list[dict] = []
    genres: list[dict] = []

    with TMDbExtractor(cfg.tmdb) as extractor:

        # Extract genre reference list
        logger.info("Step 1/3: Extracting genre list")
        genres = extractor.extract_genre_list()
        stats["genres_extracted"] = len(genres)

        # Extract movies with progress bar
        logger.info(f"Step 2/3: Extracting movies ({pages_to_fetch} pages)")
        console.print(f"\n[yellow]Fetching {pages_to_fetch} pages of popular movies...[/yellow]")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Extracting movies...", total=pages_to_fetch * 20)

            for movie in extractor.extract_movies_with_details(pages=pages_to_fetch):
                movies.append(movie)
                progress.advance(task)

                if len(movies) % 100 == 0:
                    logger.info(f"Extracted {len(movies)} movies so far...")

        stats["movies_extracted"] = len(movies)
        stats["pages_fetched"] = pages_to_fetch
        logger.info(f"Extraction complete: {len(movies)} movies, {len(genres)} genres")

    if dry_run:
        logger.info("DRY RUN mode — skipping Snowflake load")
        console.print("\n[yellow]DRY RUN: Extraction complete. Snowflake load skipped.[/yellow]")
        stats["duration_seconds"] = round(time.time() - pipeline_start, 2)
        return stats

    # ─── STEP 3: Loading ──────────────────────────────────────────────────────
    logger.info("Step 3/3: Loading data to Snowflake Bronze")
    console.print("\n[yellow]Loading data to Snowflake Bronze layer...[/yellow]")

    with SnowflakeLoader(cfg.snowflake) as loader:
        # Ensure tables exist before loading
        loader.ensure_tables_exist()

        # Load genres first (smaller, reference data)
        genres_loaded = loader.load_genres(genres)
        stats["genres_loaded"] = genres_loaded

        # Load movies in batches
        movies_loaded = loader.load_movies(movies, batch_size=cfg.batch_size)
        stats["movies_loaded"] = movies_loaded

        # Post-load validation
        loader.run_post_load_checks()

    # ─── STEP 4: Summary ──────────────────────────────────────────────────────
    stats["duration_seconds"] = round(time.time() - pipeline_start, 2)

    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETED SUCCESSFULLY")
    logger.info(f"  Movies extracted : {stats['movies_extracted']}")
    logger.info(f"  Movies loaded    : {stats['movies_loaded']}")
    logger.info(f"  Genres loaded    : {stats['genres_extracted']}")
    logger.info(f"  Duration         : {stats['duration_seconds']}s")
    logger.info("=" * 60)

    console.print(Panel.fit(
        f"[bold green]Pipeline Complete![/bold green]\n\n"
        f"  Movies extracted : [cyan]{stats['movies_extracted']}[/cyan]\n"
        f"  Movies loaded    : [cyan]{stats['movies_loaded']}[/cyan]\n"
        f"  Genres loaded    : [cyan]{stats['genres_extracted']}[/cyan]\n"
        f"  Duration         : [cyan]{stats['duration_seconds']}s[/cyan]\n\n"
        f"[dim]Next step: run   dbt run   to transform Bronze → Silver → Gold[/dim]",
        border_style="green"
    ))

    return stats


def main() -> int:
    """
    Main entry point. Returns exit code (0 = success, 1 = failure).
    """
    args = parse_args()

    # Set up logging before anything else
    try:
        cfg = load_config()
        setup_logging(cfg.log_level)
    except EnvironmentError as e:
        # Can't use logger yet if config failed — print directly
        print(f"CONFIGURATION ERROR: {e}")
        print("Copy .env.example to .env and fill in your credentials.")
        return 1

    try:
        run_pipeline(pages=args.pages, dry_run=args.dry_run)
        return 0
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user (Ctrl+C)")
        return 0
    except Exception as e:
        logger.exception(f"Pipeline failed with unhandled error: {e}")
        console.print(f"\n[bold red]PIPELINE FAILED:[/bold red] {e}")
        console.print("[dim]Check the log output above for details.[/dim]")
        return 1


if __name__ == "__main__":
    sys.exit(main())
