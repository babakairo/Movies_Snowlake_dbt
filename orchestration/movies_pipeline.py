"""
=============================================================================
  Dagster Pipeline: TMDb -> Snowflake -> dbt Medallion Architecture
  FILE: orchestration/movies_pipeline.py
  Compatible with: dagster 1.12.x, dagster-dbt 0.28.x
=============================================================================
TEACHING NOTE -- Why Dagster for Orchestration?

  WHAT IS AN ORCHESTRATOR?
  An orchestrator schedules, sequences, and monitors all pipeline steps.
  Without one, you'd manually run each step in order -- fragile and unscalable.

  TASK-CENTRIC (Airflow) vs ASSET-CENTRIC (Dagster):
  Task-centric: "Run task_A, then task_B" -- implicit data lineage
  Asset-centric: "Produce silver_movies, which depends on bronze_movies_raw"
                 -- EXPLICIT data lineage, visible in UI as a graph

  Dagster is asset-centric. Every table, file, or dataset is a
  "Software-Defined Asset". Dagster tracks:
    - What each asset produces
    - What each asset depends on
    - When it was last materialized and whether it succeeded
    - The full lineage graph (same as dbt's ref() graph, integrated!)

  @dbt_assets is especially powerful: it reads your dbt project's
  manifest.json and auto-creates one Dagster asset per dbt model,
  inferring all dependencies from ref() and source() calls.
  You get the dbt lineage graph inside Dagster for free.

INSTALLATION:
  pip install dagster dagster-webserver dagster-dbt dbt-snowflake

RUN LOCALLY:
  cd orchestration/
  dagster dev -f movies_pipeline.py
  # Open http://localhost:3000 in your browser

  # Or materialize everything from CLI:
  dagster asset materialize --select "*" -f movies_pipeline.py
=============================================================================
"""

import os
import subprocess
import sys
from pathlib import Path

# Load .env file so that dbt parse (called by prepare_if_dev) can read
# Snowflake credentials from environment variables.
# TEACHING NOTE:
#   python-dotenv reads key=value pairs from a .env file and injects them
#   into os.environ before any other code runs.  This is the standard way
#   to supply secrets locally without hard-coding them in source files.
#   In CI/CD (GitHub Actions, Dagster Cloud) you set the same vars as
#   platform secrets instead of shipping a .env file.
#
#   load_dotenv() is a no-op if the file does not exist -- safe for prod.
#   override=False means real env vars (already set in the shell) take
#   precedence over anything in .env, which is the correct precedence.
try:
    from dotenv import load_dotenv
    _ENV_FILE = Path(__file__).parent.parent / ".env"
    load_dotenv(dotenv_path=_ENV_FILE, override=False)
except ImportError:
    pass   # python-dotenv not installed; rely on shell env vars being set

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

# =============================================================================
# PATHS
# =============================================================================
PROJECT_ROOT    = Path(__file__).parent.parent          # Snowflake_DBT/
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"          # Snowflake_DBT/dbt_project/
SRC_DIR         = PROJECT_ROOT / "src"                  # Snowflake_DBT/src/

# =============================================================================
# DbtProject -- tells Dagster where the dbt project lives
# =============================================================================
# TEACHING NOTE:
#   DbtProject parses dbt_project.yml at startup to discover all models.
#   prepare_if_dev() runs "dbt parse --quiet" to compile manifest.json if it
#   does not exist yet. This is REQUIRED before @dbt_assets can read the
#   model graph.
#
#   dbt parse reads profiles.yml which references env vars like SNOWFLAKE_ACCOUNT.
#   That is why we load .env at the top of this file -- without it, dbt parse
#   would abort with "Env var required but not provided: SNOWFLAKE_ACCOUNT".
#
#   ALTERNATIVE (if you do not want prepare_if_dev):
#     Run "dbt parse" manually once from dbt_project/ before starting Dagster:
#       cd dbt_project && dbt parse
#     The manifest.json will already exist and prepare_if_dev() becomes a no-op.
#
#   In production (CI/CD or Dagster Cloud) you pre-compile the manifest and
#   skip prepare_if_dev() entirely -- env vars are injected by the platform.
dbt_project = DbtProject(project_dir=str(DBT_PROJECT_DIR))
dbt_project.prepare_if_dev()

dbt_resource = DbtCliResource(project_dir=dbt_project)


# =============================================================================
# DagsterDbtTranslator -- customize how dbt models appear in Dagster UI
# =============================================================================
# TEACHING NOTE:
#   @dbt_assets auto-creates one Dagster asset per dbt model.
#   DagsterDbtTranslator lets you control how those assets are labeled
#   in the Dagster UI -- group names, descriptions, tags, etc.
#
#   dbt_resource_props is the raw dict from the dbt manifest for each node.
#   It contains: name, tags, config, description, schema, database, etc.
#
#   We use the dbt tags we already set in dbt_project.yml ("bronze", "silver",
#   "gold", "marts") to put each model into the matching Dagster group.
#   This makes the asset graph in the UI mirror the medallion architecture.
class MoviesDbtTranslator(DagsterDbtTranslator):
    """
    Maps dbt model tags -> Dagster asset group names.

    Result in Dagster UI:
      Group "bronze"  -> bronze_movies_raw
      Group "silver"  -> silver_movies, silver_genres, silver_movie_genres, ...
      Group "gold"    -> dim_dates, dim_genres, dim_production_companies, fact_movies
      Group "marts"   -> kpi_genre_performance, kpi_yearly_trends, kpi_top_movies
    """

    def get_group_name(self, dbt_resource_props):
        tags = dbt_resource_props.get("tags", [])
        if "marts" in tags:
            return "marts"
        if "gold" in tags:
            return "gold"
        if "silver" in tags:
            return "silver"
        if "bronze" in tags:
            return "bronze"
        return "transformation"


# =============================================================================
# ASSET 1: TMDb API Ingest -> Snowflake Bronze
# =============================================================================
# TEACHING NOTE:
#   @asset creates a single Software-Defined Asset.
#   group_name groups this asset visually in the Dagster UI.
#   compute_kind shows as a badge (python, dbt, sql, etc.) on the asset node.
@asset(
    name="raw_movies_bronze",
    group_name="ingestion",
    compute_kind="python",
    description="Ingest latest movies from TMDb API into Snowflake BRONZE.RAW_MOVIES",
)
def ingest_tmdb_to_bronze(context: AssetExecutionContext) -> MaterializeResult:
    """
    Run the Python TMDb ingestion script (src/main.py).

    This is the Extract + Load step:
      1. Calls TMDb API (/movie/popular, /genre/movie/list, etc.)
      2. Writes raw JSON to Snowflake BRONZE.RAW_MOVIES via snowflake-connector-python
    """
    # TEACHING NOTE -- why "-m src.main" instead of "python src/main.py":
    #
    #   src/main.py uses package-relative imports:
    #     from src.config import load_config
    #     from src.extract import TMDbExtractor
    #
    #   When Python runs a file as a plain script (python path/to/main.py) it
    #   adds the *script's directory* (src/) to sys.path.  From inside src/,
    #   "from src.config import ..." looks for src/src/config.py -- which does
    #   not exist.  The script exits with ImportError (exit code 1).
    #
    #   Running as a module (python -m src.main) adds the *project root* to
    #   sys.path, so "from src.config import ..." correctly resolves to
    #   src/config.py.  Always use -m for packages with relative imports.
    #
    #   sys.executable: guarantees we use the same Python interpreter (and
    #   therefore the same venv) as the Dagster process itself, rather than
    #   whatever "python" resolves to on the system PATH.
    context.log.info(f"Running ingestion module: src.main (cwd={PROJECT_ROOT})")

    result = subprocess.run(
        [sys.executable, "-m", "src.main"],
        capture_output=True,
        text=True,
        encoding="utf-8",   # decode captured stdout/stderr as UTF-8, not cp1252
        cwd=str(PROJECT_ROOT),
        env={
            **os.environ,
            # TEACHING NOTE -- subprocess encoding on Windows:
            #
            #   capture_output=True redirects stdout/stderr to pipes.
            #   On Windows, pipes default to the system code page (cp1252).
            #   The 'rich' library uses Unicode braille characters for its
            #   progress spinner (e.g. U+2819 = braille dot 3).  cp1252
            #   cannot encode those, causing UnicodeEncodeError on teardown.
            #
            #   PYTHONUTF8=1  -- Python UTF-8 Mode: forces all text I/O in
            #     the subprocess (including rich's internal writes) to use
            #     UTF-8 instead of the system code page.
            #
            #   NO_COLOR=1    -- Standard env var (no-color.org) respected by
            #     rich, click, and most modern CLI tools.  Disables all ANSI
            #     colour codes and switches rich to plain-text output -- no
            #     spinner characters, no colour markup.  Safe for non-TTY
            #     capture (like Dagster's capture_output=True).
            "PYTHONUTF8":     "1",
            "NO_COLOR":       "1",
            # Snowflake / TMDb credentials forwarded from parent environment
            "SNOWFLAKE_ACCOUNT":  os.environ.get("SNOWFLAKE_ACCOUNT", ""),
            "SNOWFLAKE_USER":     os.environ.get("SNOWFLAKE_USER", ""),
            "SNOWFLAKE_PASSWORD": os.environ.get("SNOWFLAKE_PASSWORD", ""),
            "TMDB_API_KEY":       os.environ.get("TMDB_API_KEY", ""),
        },
    )

    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError(f"Ingestion script failed (exit {result.returncode})")

    context.log.info(result.stdout)
    return MaterializeResult(
        metadata={
            "module":       "src.main",
            "target_table": "LECTURE_DE.BRONZE.RAW_MOVIES",
        }
    )


# =============================================================================
# ASSET 2: dbt Models (Bronze -> Silver -> Gold -> Marts)
# =============================================================================
# TEACHING NOTE:
#   @dbt_assets is different from @asset. It produces MULTIPLE assets --
#   one per dbt model -- by reading the compiled manifest.json.
#
#   Key parameters:
#     manifest             -- path to dbt's compiled manifest.json
#     project              -- DbtProject instance (provides profiles, target)
#     name                 -- name for the op/function, not individual asset names
#     dagster_dbt_translator -- our custom class to control UI labels/groups
#
#   The function body is simple: just yield from dbt.cli(["run"], ...).stream()
#   Dagster handles logging, progress tracking, and asset materialization events.
#
#   Layer execution order is auto-determined by dbt's ref() graph:
#     bronze_movies_raw -> silver_* -> dim_* -> fact_movies -> kpi_*
@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    name="dbt_movie_models",
    dagster_dbt_translator=MoviesDbtTranslator(),
)
def dbt_movie_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Run all dbt models in dependency order (bronze -> silver -> gold -> marts)."""
    yield from dbt.cli(["run"], context=context).stream()


# =============================================================================
# ASSET 3: dbt Snapshots (SCD2)
# =============================================================================
# TEACHING NOTE:
#   This asset runs "dbt snapshot" AFTER silver + gold models are built.
#   We declare deps on specific dbt asset keys so Dagster knows the order:
#     silver_movies must finish before snap_silver_movies runs.
#     dim_genres must finish before snap_dim_genres runs.
#
#   AssetKey("silver_movies") matches the dbt model named "silver_movies".
#   @dbt_assets creates assets with keys matching the dbt model names exactly.
@asset(
    name="snapshot_scd2",
    group_name="transformation",
    compute_kind="dbt",
    deps=[
        AssetKey("silver_movies"),   # snap_silver_movies tracks this
        AssetKey("dim_genres"),      # snap_dim_genres tracks this
    ],
    description=(
        "SCD Type 2 snapshots: snap_silver_movies (revenue/rating history) "
        "and snap_dim_genres (genre reclassification history)."
    ),
)
def run_dbt_snapshots(context: AssetExecutionContext) -> MaterializeResult:
    """
    Run dbt snapshot command.

    TEACHING: dbt snapshot compares current data against the last snapshot.
    If any check_cols changed, it:
      1. Marks the old row as expired (sets dbt_valid_to = now)
      2. Inserts a new row with the updated values (dbt_valid_to = NULL)
    This creates a full audit trail of every attribute change over time.
    See scripts/incremental_demo.sql for a step-by-step walkthrough.
    """
    context.log.info("Running dbt snapshots (SCD2)...")

    result = subprocess.run(
        ["dbt", "snapshot", "--profiles-dir", str(DBT_PROJECT_DIR)],
        capture_output=True,
        text=True,
        cwd=str(DBT_PROJECT_DIR),
        env=os.environ,
    )

    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError("dbt snapshot failed")

    context.log.info(result.stdout)
    return MaterializeResult(
        metadata={"snapshots": ["snap_silver_movies", "snap_dim_genres"]}
    )


# =============================================================================
# ASSET 4: dbt Tests (Data Quality Validation)
# =============================================================================
# TEACHING NOTE:
#   dbt test runs three test categories:
#     1. Schema tests    -- not_null, unique, accepted_values, relationships
#     2. dbt_expectations -- statistical range + count checks
#     3. Custom SQL tests -- business rule assertions (tests/ directory)
#
#   --store-failures writes failing rows to Snowflake so you can inspect them:
#     SELECT * FROM DBT_DEV.dbt_test__audit.<test_name>_failures
#
#   deps=[AssetKey("snapshot_scd2")] ensures tests run after snapshots.
@asset(
    name="data_quality_tests",
    group_name="validation",
    compute_kind="dbt",
    deps=[AssetKey("snapshot_scd2")],
    description="Run all dbt tests across bronze/silver/gold/marts layers",
)
def run_dbt_tests(context: AssetExecutionContext) -> MaterializeResult:
    """Run dbt test with --store-failures for post-run investigation."""
    context.log.info("Running dbt tests...")

    result = subprocess.run(
        [
            "dbt", "test",
            "--profiles-dir", str(DBT_PROJECT_DIR),
            "--store-failures",
        ],
        capture_output=True,
        text=True,
        cwd=str(DBT_PROJECT_DIR),
        env=os.environ,
    )

    passed  = result.stdout.count("PASS")
    failed  = result.stdout.count("FAIL")
    warned  = result.stdout.count("WARN")
    errored = result.stdout.count("ERROR")

    context.log.info(result.stdout)

    if result.returncode != 0:
        context.log.warning(
            f"Tests finished with failures: "
            f"{passed} passed | {failed} failed | {warned} warned | {errored} errored"
        )
        # TEACHING: raise here to fail the pipeline on test failure (production pattern)
        # Comment out the raise for teaching demos where you want to see all results
        # raise RuntimeError("dbt tests failed -- check Dagster UI for details")

    return MaterializeResult(
        metadata={
            "tests_passed":  passed,
            "tests_failed":  failed,
            "tests_warned":  warned,
        }
    )


# =============================================================================
# JOBS -- Named selections of assets that run together
# =============================================================================
# TEACHING NOTE:
#   AssetSelection lets you target assets by group, key, or tag.
#   AssetSelection.all()          -> every asset in Definitions
#   AssetSelection.groups("gold") -> only assets in the "gold" group
#   AssetSelection.keys("fact_movies") -> one specific asset
#
#   Combining with | (union) and & (intersection) lets you build
#   precise selections without hardcoding individual asset names.

full_pipeline_job = define_asset_job(
    name="full_movies_pipeline",
    # Run everything: ingest -> dbt models -> snapshots -> tests
    selection=AssetSelection.all(),
    description="Full pipeline: TMDb API ingest -> dbt bronze/silver/gold/marts -> SCD2 snapshots -> tests",
)

dbt_only_job = define_asset_job(
    name="dbt_refresh_only",
    # Skip ingestion; just run dbt models + snapshots + tests
    selection=(
        AssetSelection.groups("bronze")
        | AssetSelection.groups("silver")
        | AssetSelection.groups("gold")
        | AssetSelection.groups("marts")
        | AssetSelection.groups("transformation")
        | AssetSelection.groups("validation")
    ),
    description="Refresh dbt models only (no API ingest). Use when testing transformations.",
)


# =============================================================================
# SCHEDULE -- Automate the pipeline
# =============================================================================
# TEACHING NOTE:
#   Cron syntax: minute  hour  day  month  weekday
#   "0 6 * * *"   -> 6:00 AM every day
#   "0 */6 * * *" -> every 6 hours
#   "30 5 * * 1"  -> every Monday at 5:30 AM
daily_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 6 * * *",
    name="daily_movies_pipeline",
    description="Run the full TMDb pipeline every morning at 6:00 AM UTC",
)


# =============================================================================
# DEFINITIONS -- Top-level Dagster entry point
# =============================================================================
# TEACHING NOTE:
#   Definitions is what Dagster reads when you run "dagster dev -f movies_pipeline.py".
#   It registers all assets, jobs, schedules, and resources in one place.
#   Think of it as the "main()" of your Dagster deployment.
defs = Definitions(
    assets=[
        ingest_tmdb_to_bronze,  # Python asset: API -> BRONZE.RAW_MOVIES
        dbt_movie_models,       # dbt assets: bronze -> silver -> gold -> marts
        run_dbt_snapshots,      # dbt snapshot: SCD2 history tables
        run_dbt_tests,          # dbt test: data quality validation
    ],
    jobs=[
        full_pipeline_job,
        dbt_only_job,
    ],
    schedules=[
        daily_schedule,
    ],
    resources={
        "dbt": dbt_resource,    # injected into dbt_movie_models at runtime
    },
)
