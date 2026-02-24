# Modern Data Engineering Platform
### TMDb Movie Analytics — Medallion Architecture with Snowflake + dbt + Dagster

> **Teaching-ready project** demonstrating a production-grade Bronze → Silver → Gold → Marts
> data pipeline using industry best practices.
> Every architectural decision is documented with teaching notes embedded directly in the code.

---

## Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Ingestion** | Python 3.10+ | Extract from TMDb API, load to Snowflake Bronze |
| **Storage** | Snowflake | Cloud data warehouse |
| **Transformation** | dbt Core | SQL models, tests, macros, snapshots, hooks |
| **Orchestration** | Dagster | Asset graph, scheduling, run observability |
| **Data Quality** | dbt_expectations | Statistical + schema tests |

---

## Dataset: The Movie Database (TMDb)

**Why TMDb?**

| Criterion | Why It Works |
|---|---|
| Free | API key in 2 minutes — no billing required |
| No auth complexity | Bearer token, no OAuth dance |
| Realistic schema | Budgets, revenues, genres, companies, ratings |
| High volume | 500K+ movies; we extract ~400 to keep it teachable |
| Intuitive domain | Every student understands movies — business questions are obvious |
| Star-schema-ready | Naturally produces Fact + Date + Genre + Company dimensions |

**Register for a free API key:** https://www.themoviedb.org/settings/api

---

## Architecture at a Glance

```
TMDb API --> Python (src/) --> Snowflake BRONZE (raw VARIANT JSON)
                                      |
                               dbt run (bronze)
                                      |
                               Snowflake SILVER (typed, cleaned, incremental)
                                      |
                          dbt snapshot (SCD2 history)
                                      |
                               dbt run (gold + marts)
                                      |
                        Snowflake GOLD (star schema + KPI marts)
                                      |
                               dbt test (schema + dbt_expectations + custom SQL)
                                      |
                          BI Tools / Analyst SQL Queries
```

**Dagster wraps all of the above** into a Software-Defined Asset graph with scheduling,
dependency tracking, and observability at http://localhost:3000.

See [architecture.txt](architecture.txt) for the full ASCII diagram.

---

## Project Structure

```
Snowflake_DBT/
|
|-- .env.example                  <- Copy to .env and fill credentials
|-- requirements.txt              <- Python ingestion dependencies
|-- architecture.txt              <- Full ASCII architecture diagram
|
|-- snowflake_setup/
|   |-- 01_setup.sql              <- Database, schemas, warehouses, Bronze tables
|   `-- 02_roles_and_permissions.sql  <- RBAC: roles, users, grants
|
|-- src/                          <- Python ingestion pipeline
|   |-- config.py                 <- Environment configuration manager
|   |-- extract.py                <- TMDb API client (pagination, retry)
|   |-- load.py                   <- Snowflake loader (bulk insert, upsert)
|   `-- main.py                   <- Pipeline orchestrator
|
|-- scripts/
|   `-- incremental_demo.sql      <- Step-by-step SCD2 + incremental walkthrough
|
|-- orchestration/                <- Dagster pipeline definition
|   |-- movies_pipeline.py        <- Software-Defined Assets + schedule
|   |-- dagster.yaml              <- Local storage / instance config
|   `-- requirements.txt          <- Dagster-specific dependencies
|
`-- dbt_project/                  <- All dbt transformation code
    |-- dbt_project.yml           <- dbt config, materialisation, hooks
    |-- profiles.yml              <- Snowflake connection profile
    |-- packages.yml              <- dbt_utils, dbt_expectations, dbt_date
    |
    |-- models/
    |   |-- sources.yml           <- Source table definitions + freshness checks
    |   |
    |   |-- bronze/               <- Raw JSON landing (TABLE)
    |   |   |-- bronze_movies_raw.sql
    |   |   `-- schema.yml
    |   |
    |   |-- silver/               <- Parsed, typed, cleaned (INCREMENTAL + MERGE)
    |   |   |-- silver_movies.sql
    |   |   |-- silver_genres.sql
    |   |   |-- silver_movie_genres.sql
    |   |   |-- silver_movie_companies.sql
    |   |   |-- silver_production_companies.sql
    |   |   `-- schema.yml
    |   |
    |   |-- gold/                 <- Star schema dimensions + fact (TABLE)
    |   |   |-- dim_dates.sql
    |   |   |-- dim_genres.sql
    |   |   |-- dim_production_companies.sql
    |   |   |-- fact_movies.sql
    |   |   `-- schema.yml
    |   |
    |   `-- marts/                <- Pre-aggregated KPI tables (TABLE)
    |       |-- kpi_genre_performance.sql
    |       |-- kpi_yearly_trends.sql
    |       |-- kpi_top_movies.sql
    |       `-- schema.yml
    |
    |-- snapshots/                <- SCD Type 2 history tables (DBT_STAGING schema)
    |   |-- snap_silver_movies.sql    <- Tracks financial/quality attribute changes
    |   `-- snap_dim_genres.sql       <- Tracks genre reclassification history
    |
    |-- macros/
    |   |-- generate_schema_name.sql  <- Dev vs prod schema routing
    |   |-- generate_surrogate_key.sql <- SK generation + date_sk helper
    |   |-- standardize_date.sql       <- safe_to_date, extract_date_parts, seasons
    |   `-- audit_logging.sql          <- Hook macros: run log, model log, GRANT
    |
    |-- tests/                    <- Custom SQL data quality tests
    |   |-- assert_revenue_not_negative.sql
    |   |-- assert_profit_equals_revenue_minus_budget.sql
    |   `-- assert_silver_movies_no_future_releases.sql
    |
    `-- seeds/
        `-- budget_tiers.csv      <- Budget tier classification reference data
```

---

## Step-by-Step Setup & Run Guide

### Prerequisites
- Python 3.10 or higher
- A [Snowflake trial account](https://signup.snowflake.com/) (free — no credit card)
- A [TMDb API key](https://www.themoviedb.org/settings/api) (free registration)

---

### Step 1: Clone & Install Python Dependencies

```bash
cd Snowflake_DBT

# Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate           # Windows
source .venv/bin/activate        # Mac/Linux

# Install core ingestion dependencies
pip install -r requirements.txt
```

---

### Step 2: Configure Credentials

```bash
# Copy the example file
cp .env.example .env

# Edit .env — required values:
#   TMDB_API_KEY        -> from https://www.themoviedb.org/settings/api
#   SNOWFLAKE_ACCOUNT   -> e.g., xy12345.us-east-1
#   SNOWFLAKE_USER      -> your username
#   SNOWFLAKE_PASSWORD  -> your password
```

---

### Step 3: Set Up Snowflake Infrastructure

In Snowflake's web UI (or SnowSQL CLI), run these files **in order**:

```sql
-- Run as SYSADMIN
-- File: snowflake_setup/01_setup.sql
-- Creates: LECTURE_DE database, BRONZE/SILVER/GOLD/DBT_STAGING schemas,
--          LECTURE_INGEST_WH, LECTURE_TRANSFORM_WH, Bronze tables

-- Run as SECURITYADMIN
-- File: snowflake_setup/02_roles_and_permissions.sql
-- Creates: DE_ROLE, ANALYST_ROLE, grants
```

> **Teaching tip:** Walk students through each SQL block and explain WHY
> each step exists before running it.

---

### Step 4: Run the Python Ingestion Pipeline

```bash
# Quick test: 2 pages (~40 movies) -- fast, good for verifying setup
python -m src.main --pages 2

# Dry run: extract from API but skip Snowflake write
python -m src.main --pages 5 --dry-run

# Full run: 20 pages (~400 movies)
python -m src.main

# Larger dataset
python -m src.main --pages 50    # ~1000 movies
```

**Verify in Snowflake:**
```sql
SELECT COUNT(*) FROM LECTURE_DE.BRONZE.RAW_MOVIES;   -- ~400
SELECT COUNT(*) FROM LECTURE_DE.BRONZE.RAW_GENRES;   -- 1 row (JSON array)
SELECT MOVIE_ID, RAW_DATA:title::VARCHAR AS title
FROM LECTURE_DE.BRONZE.RAW_MOVIES LIMIT 5;
```

---

### Step 5: Configure dbt

```bash
# Copy the connection profile to dbt's expected location
cp dbt_project/profiles.yml ~/.dbt/profiles.yml

cd dbt_project

# Install dbt packages (dbt_utils, dbt_expectations, dbt_date)
dbt deps

# Test the Snowflake connection
dbt debug
```

---

### Step 6: Load Seed Data

```bash
cd dbt_project

# Load budget_tiers.csv reference table to GOLD schema
dbt seed
```

---

### Step 7: Run dbt Transformations

```bash
cd dbt_project

# Build ALL models in dependency order (Bronze -> Silver -> Gold -> Marts)
dbt run

# Build only one layer
dbt run --select bronze
dbt run --select silver
dbt run --select gold
dbt run --select marts

# Build a specific model + all upstreams
dbt run --select +fact_movies

# Build a specific model + all downstreams
dbt run --select silver_movies+
```

**Expected output:**
```
Running with dbt=1.x.x
Found 14 models, 3 seeds, N tests, 1 source
Completed successfully
PASS=14 WARN=0 ERROR=0 SKIP=0 TOTAL=14
```

---

### Step 8: Run SCD2 Snapshots

```bash
cd dbt_project

# Run both snapshots (creates/updates DBT_STAGING.snap_silver_movies
#                                 and DBT_STAGING.snap_dim_genres)
dbt snapshot

# Run a specific snapshot
dbt snapshot --select snap_silver_movies
```

**What snapshots do:**
SCD (Slowly Changing Dimension) Type 2 snapshots track attribute changes over time.
When a movie's revenue or rating is updated in TMDb, the snapshot:
1. Marks the old row as expired (`dbt_valid_to = NOW()`)
2. Inserts a new current row (`dbt_valid_to = NULL`)

This gives you a full audit trail. See [scripts/incremental_demo.sql](scripts/incremental_demo.sql)
for a step-by-step walkthrough.

---

### Step 9: Run Data Quality Tests

```bash
cd dbt_project

# Run all tests (schema tests + dbt_expectations + custom SQL)
dbt test

# Test a specific model
dbt test --select silver_movies

# Save failing rows to Snowflake tables for inspection
dbt test --store-failures
# Then: SELECT * FROM LECTURE_DE.DBT_STAGING.dbt_test__audit.<test_name>

# Check source freshness
dbt source freshness
```

**Test categories in this project:**

| Type | Location | Examples |
|---|---|---|
| Schema tests | schema.yml | `not_null`, `unique`, `relationships`, `accepted_values` |
| dbt_expectations | schema.yml | `expect_column_values_to_be_between`, `expect_table_row_count_to_be_between` |
| Custom SQL tests | tests/*.sql | Revenue not negative, profit = revenue - budget, no future releases |

---

### Step 10: Query the Gold & Marts Layers

```sql
-- Top 10 highest-grossing movies ever
SELECT title, release_year, revenue_usd / 1000000 AS revenue_millions, vote_average
FROM LECTURE_DE.GOLD.KPI_TOP_MOVIES
WHERE revenue_rank_all_time <= 10
ORDER BY revenue_rank_all_time;

-- Genre performance comparison
SELECT genre_name, total_movies, avg_revenue_millions, avg_roi_pct, roi_rank
FROM LECTURE_DE.GOLD.KPI_GENRE_PERFORMANCE
ORDER BY avg_roi_pct DESC NULLS LAST
LIMIT 10;

-- Year-over-year industry trends
SELECT release_year, total_movies_released, avg_budget_millions,
       avg_revenue_millions, yoy_budget_change_millions, avg_rating
FROM LECTURE_DE.GOLD.KPI_YEARLY_TRENDS
WHERE release_year >= 2000
ORDER BY release_year;

-- Action movies with highest ROI
SELECT m.title, m.release_year,
       m.budget_usd / 1e6 AS budget_m,
       m.revenue_usd / 1e6 AS revenue_m,
       m.roi_pct
FROM LECTURE_DE.GOLD.FACT_MOVIES m
JOIN LECTURE_DE.GOLD.DIM_GENRES g ON m.genre_sk = g.genre_sk
WHERE g.genre_name = 'ACTION'
  AND m.roi_pct IS NOT NULL
ORDER BY m.roi_pct DESC
LIMIT 20;

-- Audit: which dbt runs have completed?
SELECT * FROM LECTURE_DE.DBT_STAGING.dbt_run_log ORDER BY run_start_at DESC;

-- Audit: which models ran in the last run?
SELECT * FROM LECTURE_DE.DBT_STAGING.dbt_model_log
WHERE invocation_id = (SELECT invocation_id FROM LECTURE_DE.DBT_STAGING.dbt_run_log
                       ORDER BY run_start_at DESC LIMIT 1);
```

---

### Step 11 (Optional): Orchestrate with Dagster

Dagster provides a visual asset graph, scheduling, and run history for the full pipeline.

```bash
# Install Dagster dependencies
pip install -r orchestration/requirements.txt

# Start the Dagster UI (from project root)
cd orchestration
dagster dev -f movies_pipeline.py

# Open http://localhost:3000 in your browser
```

**What you see in the Dagster UI:**
- **ingestion** group: `raw_movies_bronze` (Python asset — TMDb -> Bronze)
- **bronze/silver/gold/marts** groups: one asset per dbt model, wired by ref()
- **transformation** group: `snapshot_scd2` (runs dbt snapshot)
- **validation** group: `data_quality_tests` (runs dbt test --store-failures)

**Run the full pipeline:**
1. Click "Jobs" in the left sidebar
2. Select `full_movies_pipeline`
3. Click "Materialize All"

**Schedule:** The pipeline is pre-configured to run daily at 6:00 AM UTC.

---

## Layer-by-Layer Teaching Guide

### Bronze Layer — The Raw Vault

**Materialization:** `table`

**What it is:** An immutable landing zone for raw API data stored as VARIANT (JSON).

**Why it exists:**
- **Reprocessability:** If Silver has a bug, rerun from Bronze — no re-call to the API
- **Auditability:** Regulators can see exactly what data arrived and when
- **Schema-on-read:** VARIANT absorbs API schema changes without breaking ingestion
- **Speed:** No parsing during ingestion = maximum throughput

**Common mistakes:**
- Cleaning or transforming data in Bronze — Bronze is append-only
- Joining multiple sources in Bronze — one source per Bronze table

---

### Silver Layer — The Conformed Layer

**Materialization:** `incremental` (strategy: `merge`, unique_key: `movie_id`)

**What it is:** Type-safe, deduplicated, cleaned relational tables. One row per business entity.

**Why incremental?**
Over weeks and months you accumulate millions of movies. Re-processing all history
on every dbt run is wasteful. Incremental models only merge rows where
`ingested_at > last_run_timestamp`.

**Key patterns shown:**
- VARIANT parsing with `raw_data:key::TYPE` syntax
- FLATTEN() for nested arrays into bridge tables
- TRY_TO_DATE(), NULLIF() for safe type conversion
- QUALIFY clause for window function filtering
- Metadata columns (`_loaded_at`, `_source`) for lineage

---

### Gold Layer — The Star Schema

**Materialization:** `table`

**What it is:** A Kimball-style star schema — one fact table + four dimension tables.

**Why star schema?**
- BI tools (Tableau, Power BI) work best with simple JOIN patterns: fact JOIN dim
- Pre-materialized tables = fast analytical queries without re-reading raw data
- Surrogate keys decouple the warehouse from source system IDs

**Key concepts:**
- `dim_dates`: Generated by `dbt_utils.date_spine()` — spans 2000-01-01 to 2030-12-31
- `dim_genres`: Enriched with `genre_group` and `is_blockbuster_genre` business attributes
- `fact_movies`: Central grain — one row per released movie (primary genre only)
- `post-hook`: Every Gold model logs its execution to `DBT_STAGING.dbt_model_log`

---

### Marts Layer — Business-Facing KPIs

**Materialization:** `table` (schema: `GOLD`)

**What it is:** Pre-aggregated answer tables for specific business questions.
Sits above the star schema. One mart = one dashboard section.

| Model | Question Answered |
|---|---|
| `kpi_genre_performance` | Which genre has the highest ROI? |
| `kpi_yearly_trends` | How has the industry changed year-over-year? |
| `kpi_top_movies` | What are the top movies by revenue / ROI / rating? |

**Why separate from Gold?**
Gold star schema is optimized for analytical flexibility (any join/pivot/filter).
Marts are optimized for specific, repeated business questions — dashboards query
one table with no joins required.

---

### SCD Type 2 Snapshots

**Target schema:** `DBT_STAGING`

dbt snapshots capture the history of slowly-changing attributes using the
`check` strategy. When any tracked column changes:
1. The old row is expired: `dbt_valid_to = NOW()`
2. A new current row is inserted: `dbt_valid_to = NULL`

| Snapshot | Tracks Changes To |
|---|---|
| `snap_silver_movies` | `budget_usd`, `revenue_usd`, `vote_average`, `popularity_score`, `status`, `runtime_minutes` |
| `snap_dim_genres` | `genre_name`, `genre_group`, `is_blockbuster_genre` |

See [scripts/incremental_demo.sql](scripts/incremental_demo.sql) for a step-by-step demo.

---

### dbt Hooks & Audit Logging

Hooks run automatically at specific pipeline points — no manual intervention needed.

| Hook | When | What It Does |
|---|---|---|
| `on-run-start` | Once before any model | Creates `dbt_run_log` + `dbt_model_log` tables; inserts run start record |
| `on-run-end` | Once after all models | Updates run log with SUCCESS/FAILED status and model counts |
| `+post-hook` (Gold/Marts) | After each Gold/Marts model | Inserts per-model execution record into `dbt_model_log` |
| `on-run-end` | Once after all models | `GRANT SELECT ON ALL TABLES IN SCHEMA GOLD TO ROLE ANALYST_ROLE` |

**Query the audit log:**
```sql
SELECT * FROM LECTURE_DE.DBT_STAGING.dbt_run_log ORDER BY run_start_at DESC LIMIT 10;
```

---

### Custom Macros

| Macro File | Macros Provided | Purpose |
|---|---|---|
| `generate_surrogate_key.sql` | `generate_surrogate_key(cols)`, `generate_date_sk(date_col)` | Consistent SK generation across all Gold models |
| `standardize_date.sql` | `safe_to_date()`, `clean_date()`, `extract_date_parts()`, `release_season()`, `fiscal_year()` | Centralized date logic — change once, applies everywhere |
| `audit_logging.sql` | `create_audit_log_tables()`, `log_run_start()`, `log_run_end()`, `log_model_run()`, `grant_analyst_role_access()` | Full operational observability |
| `generate_schema_name.sql` | `generate_schema_name()` | Routes dev runs to dev schemas, prod runs to real schemas |

---

## Key dbt Commands Reference

```bash
# Connection
dbt debug              # Test Snowflake connection

# Dependencies
dbt deps               # Install packages from packages.yml

# Data loading
dbt seed               # Load CSV seeds to Snowflake

# Transformations
dbt run                # Build all models
dbt run --select +fact_movies  # Build specific model + upstreams
dbt run --select silver+       # Build specific model + downstreams

# SCD2 snapshots
dbt snapshot           # Run all snapshots

# Data quality
dbt test               # Run all tests
dbt test --store-failures      # Save failing rows to Snowflake
dbt source freshness   # Check source data freshness

# Combined (recommended for CI/CD)
dbt build              # dbt run + dbt test in one command

# Documentation
dbt docs generate      # Generate documentation site
dbt docs serve         # Open docs at http://localhost:8080
```

---

## Key Dagster Commands Reference

```bash
# Start Dagster UI (from orchestration/)
dagster dev -f movies_pipeline.py

# Materialize specific asset from CLI
dagster asset materialize --select raw_movies_bronze -f movies_pipeline.py

# Materialize all assets
dagster asset materialize --select "*" -f movies_pipeline.py
```

---

## Environment Variables Reference

| Variable | Required | Description |
|---|---|---|
| `TMDB_API_KEY` | Yes | TMDb API key |
| `TMDB_PAGES_TO_FETCH` | No | Pages to fetch (default: 20, ~400 movies) |
| `SNOWFLAKE_ACCOUNT` | Yes | Account identifier e.g., `xy12345.us-east-1` |
| `SNOWFLAKE_USER` | Yes | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Yes | Snowflake password |
| `SNOWFLAKE_ROLE` | No | Role (default: DE_ROLE) |
| `SNOWFLAKE_WAREHOUSE` | No | Warehouse (default: LECTURE_INGEST_WH) |
| `SNOWFLAKE_DATABASE` | No | Database (default: LECTURE_DE) |
| `BATCH_SIZE` | No | Rows per Snowflake insert batch (default: 100) |
| `LOG_LEVEL` | No | Logging verbosity: DEBUG, INFO, WARNING (default: INFO) |

---

## Business Questions This Platform Answers

1. **"Which movie genre has the highest average ROI?"**
   `SELECT * FROM GOLD.KPI_GENRE_PERFORMANCE ORDER BY avg_roi_pct DESC`

2. **"How has average movie budget changed from 2000 to today?"**
   `SELECT release_year, avg_budget_millions FROM GOLD.KPI_YEARLY_TRENDS WHERE release_year >= 2000`

3. **"What percentage of movies are profitable?"**
   `SELECT profitability_flag, COUNT(*) FROM GOLD.FACT_MOVIES GROUP BY 1`

4. **"Which studio produces the most movies per year?"**
   Join `fact_movies` -> `dim_production_companies`, GROUP BY company, year

5. **"Are summer blockbusters more profitable than winter releases?"**
   `SELECT release_season, AVG(roi_pct) FROM GOLD.FACT_MOVIES GROUP BY 1`

6. **"How has a movie's revenue changed since it was first recorded?"** (SCD2)
   `SELECT * FROM DBT_STAGING.SNAP_SILVER_MOVIES WHERE movie_id = 123 ORDER BY dbt_valid_from`

---

## Troubleshooting

**"Connection failed" in Python:**
- `SNOWFLAKE_ACCOUNT` should NOT include `.snowflakecomputing.com`
- Correct: `xy12345.us-east-1` | Wrong: `xy12345.us-east-1.snowflakecomputing.com`

**"Table BRONZE.RAW_MOVIES does not exist":**
- Run `snowflake_setup/01_setup.sql` first
- Verify your user has `DE_ROLE` granted

**"dbt debug failed":**
- Ensure `~/.dbt/profiles.yml` exists (not just `dbt_project/profiles.yml`)
- Run `dbt debug` from inside the `dbt_project/` directory

**"Env var required but not provided: SNOWFLAKE_ACCOUNT" (Dagster):**
- Ensure your `.env` file exists at the project root (not inside `orchestration/`)
- The pipeline loads `.env` automatically via `python-dotenv`

**"Rate limited by TMDb API":**
- TMDb allows 40 requests/10s on the free tier
- The extractor has built-in throttling; if still hitting limits, reduce `--pages`

**"dbt test failed":**
- Run `dbt test --store-failures`
- Inspect: `SELECT * FROM LECTURE_DE.DBT_STAGING.dbt_test__audit.<test_name>`

**"Insufficient privileges" on GRANT in on-run-end hook:**
- DE_ROLE needs GRANT OPTION to run `GRANT SELECT TO ANALYST_ROLE`
- Run as SYSADMIN: `GRANT GRANT OPTION FOR PRIVILEGE SELECT ON ALL TABLES IN SCHEMA LECTURE_DE.GOLD TO ROLE DE_ROLE;`

**"dbt snapshot: no changes detected":**
- This is expected if source data hasn't changed since the last snapshot
- Use `scripts/incremental_demo.sql` to manually insert test rows that will trigger SCD2

---

## Next Steps for Students

After completing this project, explore these advanced topics:

1. **dbt Semantic Layer:** Define metrics once, query them from any BI tool using MetricFlow
2. **Data Contracts:** Use `dbt_contracts` for enforcing schema agreements between teams
3. **CI/CD Pipeline:** Add GitHub Actions to run `dbt build` on every pull request
4. **Production Monitoring:** Integrate Monte Carlo or Elementary for anomaly detection
5. **Streaming Ingestion:** Replace batch Python ingestion with Kafka + Snowpipe
6. **dbt Exposures:** Document how Gold tables connect to downstream dashboards
