# Modern Data Engineering Platform
### TMDb Movie Analytics — Medallion Architecture with Snowflake + dbt + Python

---

## Dataset: The Movie Database (TMDb)

**Why TMDb?**
| Criterion | Why It Works |
|---|---|
| Free | API key in 2 minutes — no billing required |
| No auth complexity | Bearer token, no OAuth dance |
| Realistic schema | Budgets, revenues, genres, companies, ratings |
| High volume | 500K+ movies; we extract ~400 to keep it teachable |
| Intuitive domain | Every student understands movies → business questions are obvious |
| Star-schema-ready | Naturally produces Fact + Date + Genre + Company dimensions |

**Register for a free API key:** https://www.themoviedb.org/settings/api

---

## Architecture at a Glance

```
TMDb API → Python (extract.py) → Snowflake Bronze (VARIANT/JSON)
                                       ↓ dbt run
                              Snowflake Silver (typed, cleaned)
                                       ↓ dbt run
                              Snowflake Gold (fact/dim + KPI marts)
                                       ↓
                              BI Tools / Analyst SQL Queries
```

See [architecture.txt](architecture.txt) for the full ASCII diagram.

---

## Project Structure

```
Snowflake_DBT/
├── .env.example                  ← Copy to .env and fill credentials
├── requirements.txt              ← Python dependencies
├── architecture.txt              ← Full architecture diagram
│
├── snowflake_setup/
│   ├── 01_setup.sql              ← Database, schemas, warehouses, tables
│   └── 02_roles_and_permissions.sql  ← RBAC: roles, users, grants
│
├── src/                          ← Python ingestion pipeline
│   ├── config.py                 ← Environment configuration manager
│   ├── extract.py                ← TMDb API client (pagination, retry)
│   ├── load.py                   ← Snowflake loader (bulk insert, upsert)
│   └── main.py                   ← Pipeline orchestrator
│
└── dbt_project/                  ← All dbt transformation code
    ├── dbt_project.yml           ← dbt configuration & materialization settings
    ├── profiles.yml              ← Snowflake connection (copy to ~/.dbt/)
    ├── packages.yml              ← dbt package dependencies
    │
    ├── models/
    │   ├── sources.yml           ← Source table definitions + freshness checks
    │   ├── bronze/
    │   │   ├── bronze_movies_raw.sql   ← Deduplicated raw JSON landing
    │   │   └── schema.yml
    │   ├── silver/
    │   │   ├── silver_movies.sql       ← Parsed, typed, cleaned movies
    │   │   ├── silver_genres.sql       ← Genre reference (flattened array)
    │   │   ├── silver_movie_genres.sql ← Movie↔Genre bridge table
    │   │   ├── silver_production_companies.sql
    │   │   └── schema.yml
    │   └── gold/
    │       ├── dim_dates.sql           ← Calendar date dimension
    │       ├── dim_genres.sql          ← Genre dimension + enrichment
    │       ├── dim_production_companies.sql
    │       ├── fact_movies.sql         ← Central fact table (star schema)
    │       ├── kpi_genre_performance.sql ← Genre analytics KPI mart
    │       ├── kpi_yearly_trends.sql   ← Year-over-year trend mart
    │       ├── kpi_top_movies.sql      ← Multi-metric movie rankings
    │       └── schema.yml
    │
    ├── tests/
    │   ├── assert_revenue_not_negative.sql
    │   ├── assert_profit_equals_revenue_minus_budget.sql
    │   └── assert_silver_movies_no_future_releases.sql
    │
    ├── macros/
    │   └── generate_schema_name.sql    ← Schema naming strategy (dev vs prod)
    │
    └── seeds/
        └── budget_tiers.csv            ← Budget classification reference data
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
# Clone or navigate to the project directory
cd Snowflake_DBT

# Create a virtual environment (best practice — isolates dependencies)
python -m venv .venv
source .venv/bin/activate        # Mac/Linux
.venv\Scripts\activate           # Windows

# Install dependencies
pip install -r requirements.txt
```

---

### Step 2: Configure Credentials

```bash
# Copy the example file
cp .env.example .env

# Edit .env with your actual credentials
# REQUIRED values to fill in:
#   TMDB_API_KEY        → from https://www.themoviedb.org/settings/api
#   SNOWFLAKE_ACCOUNT   → e.g., xy12345.us-east-1
#   SNOWFLAKE_USER      → your username
#   SNOWFLAKE_PASSWORD  → your password
```

---

### Step 3: Set Up Snowflake Infrastructure

In Snowflake's web UI (or SnowSQL CLI), run these files in order:

```sql
-- Run as SYSADMIN
-- File: snowflake_setup/01_setup.sql
-- Creates: database, schemas, warehouses, Bronze tables

-- Run as SECURITYADMIN
-- File: snowflake_setup/02_roles_and_permissions.sql
-- Creates: roles, users, grants
```


---

### Step 4: Run the Python Ingestion Pipeline

```bash
# Quick test: fetch only 2 pages (40 movies) — fast, for verifying setup
python -m src.main --pages 2

# Dry run: extract from API but don't write to Snowflake
python -m src.main --pages 5 --dry-run

# Full run: fetch 20 pages (~400 movies)
python -m src.main

# Override pages count
python -m src.main --pages 50    # 50 pages = ~1000 movies
```

**After a successful run, verify in Snowflake:**
```sql
SELECT COUNT(*) FROM LECTURE_DE.BRONZE.RAW_MOVIES;   -- Should be ~400
SELECT COUNT(*) FROM LECTURE_DE.BRONZE.RAW_GENRES;   -- Should be 1
SELECT MOVIE_ID, RAW_DATA:title::VARCHAR AS title FROM LECTURE_DE.BRONZE.RAW_MOVIES LIMIT 5;
```

---

### Step 5: Configure dbt

```bash
# Copy profiles.yml to dbt's expected location
cp dbt_project/profiles.yml ~/.dbt/profiles.yml

# Edit ~/.dbt/profiles.yml — dbt reads SNOWFLAKE_ACCOUNT etc. from your .env
# (or set the environment variables directly)

cd dbt_project

# Install dbt packages (dbt_utils, dbt_expectations)
dbt deps

# Test the connection
dbt debug
```

---

### Step 6: Run dbt Transformations

```bash
cd dbt_project

# Build ALL models (Bronze → Silver → Gold) in dependency order
dbt run

# Build only the Bronze layer
dbt run --select bronze

# Build Silver and everything downstream
dbt run --select silver+

# Build a specific model and all models it depends on
dbt run --select +fact_movies

# Build all Gold models
dbt run --select gold

# Run with production target (writes to real BRONZE/SILVER/GOLD schemas)
dbt run --target prod
```

**Expected output:**
```
15:43:02  Running with dbt=1.7.4
15:43:04  Found 10 models, 3 seeds, 5 tests, 1 source
15:43:06  Completed successfully
15:43:06
15:43:06  Done. PASS=10 WARN=0 ERROR=0 SKIP=0 TOTAL=10
```

---

### Step 7: Load Seed Data

```bash
cd dbt_project

# Load the budget_tiers.csv reference table to Snowflake
dbt seed
```

---

### Step 8: Run Data Quality Tests

```bash
cd dbt_project

# Run all tests (schema tests + custom SQL tests)
dbt test

# Run tests for a specific model
dbt test --select silver_movies

# Run tests and save failing rows to Snowflake tables for inspection
dbt test --store-failures

# Check source freshness (was data loaded recently?)
dbt source freshness
```

---

### Step 9: Query the Gold Layer

Connect to Snowflake and explore the analytical layer:

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

-- Year-over-year trends (2000 to present)
SELECT release_year, total_movies_released, avg_budget_millions,
       avg_revenue_millions, yoy_budget_change_millions, avg_rating
FROM LECTURE_DE.GOLD.KPI_YEARLY_TRENDS
WHERE release_year >= 2000
ORDER BY release_year;

-- Action movies with highest ROI
SELECT m.title, m.release_year, m.budget_usd / 1e6 AS budget_m,
       m.revenue_usd / 1e6 AS revenue_m, m.roi_pct
FROM LECTURE_DE.GOLD.FACT_MOVIES m
JOIN LECTURE_DE.GOLD.DIM_GENRES g ON m.genre_sk = g.genre_sk
WHERE g.genre_name = 'ACTION'
  AND m.roi_pct IS NOT NULL
ORDER BY m.roi_pct DESC
LIMIT 20;
```

---

## Layer-by-Layer Teaching Guide

### 🥉 Bronze Layer — The Raw Vault

**What it is:** An immutable landing zone for raw API data.

**Why it exists:**
Every modern data platform has a raw layer because:
- **Reprocessability:** If we find a bug in Silver, we can rerun Silver from Bronze — we don't need to re-call the API
- **Auditability:** Regulators and auditors may want to see exactly what data arrived
- **Schema-on-read:** VARIANT/JSON storage means API schema changes don't break ingestion
- **Speed:** No parsing during ingestion = maximum throughput

**Common mistakes students make:**
- ❌ Cleaning data in Bronze → Bronze is append-only, never updated
- ❌ Joining multiple sources in Bronze → one source per Bronze table
- ❌ Using VARCHAR columns in Bronze → VARIANT preserves the original structure

**Key Snowflake concepts:** VARIANT, PARSE_JSON(), FLATTEN(), stages

---

### 🥈 Silver Layer — The Conformed Layer

**What it is:** Type-safe, deduplicated, cleaned relational tables.

**Why it exists:**
Bronze's VARIANT is great for storage but terrible for analytics. Silver:
- **Converts VARIANT to typed columns** — analysts write `WHERE budget > 1000000`, not `WHERE raw_data:budget::NUMBER > 1000000`
- **Deduplicates** — multiple pipeline runs don't create duplicate rows
- **Normalises** — extracts arrays into proper relational tables (bridge tables)
- **Validates** — NULL handling, TRY_TO_DATE(), NULLIF() for zeros-as-nulls

**Key patterns shown:**
- Incremental materialisation with MERGE strategy
- VARIANT parsing with `raw_data:key::TYPE` syntax
- FLATTEN() for nested arrays
- QUALIFY clause for elegant window function filtering
- Metadata columns (_loaded_at, _source) for lineage

**Common mistakes:**
- ❌ Aggregating in Silver → that's Gold's job
- ❌ Joining unrelated sources in Silver → each Silver model has one lineage
- ❌ Not including lineage columns → makes debugging painful

---

### 🥇 Gold Layer — The Business Layer

**What it is:** Star-schema dimensional model + pre-aggregated KPI marts.

**Why it exists:**
Silver is normalised for storage efficiency. Gold is optimised for query performance:
- **Star schema** enables BI tools to work efficiently with simple JOIN patterns
- **Pre-aggregated marts** make dashboards fast (no re-aggregation on every refresh)
- **Surrogate keys** decouple our warehouse from source system IDs
- **Business enrichment** (genre groups, studio tiers) adds organisational knowledge

**Dimensional modelling concepts:**
- **Fact tables** contain measures (numbers) and foreign keys to dimensions
- **Dimension tables** contain context (labels, attributes, classifications)
- **Surrogate keys** (warehouse-generated) vs **natural keys** (source IDs)
- **Date dimension** — the universal join dimension for all time-based analysis
- **Slowly Changing Dimensions** — how to handle attribute changes over time
- **KPI marts** — pre-aggregated answer tables for common business questions

**Common mistakes:**
- ❌ Storing text labels in fact tables → put labels in dimensions, IDs in facts
- ❌ Multiple grains in one fact table → one grain per fact table
- ❌ No surrogate keys → tightly coupled to source system

---

## Testing Strategy

dbt provides three levels of testing:

| Type | Location | Example |
|------|----------|---------|
| **Schema tests** | schema.yml | `not_null`, `unique`, `accepted_values`, `relationships` |
| **Source tests** | sources.yml | Freshness checks, source column validation |
| **Custom SQL tests** | tests/*.sql | Business rules, cross-model consistency |
| **Package tests** | schema.yml | `dbt_expectations.expect_column_values_to_be_between` |

**Why testing matters in data engineering:**
- Silent data bugs kill business trust — a wrong number on a dashboard may be believed for months
- Tests catch upstream API changes before they corrupt your analytics
- `dbt test --store-failures` saves failing rows to Snowflake so you can inspect exactly what broke
- Source freshness tests detect pipeline failures before analysts notice stale data

---

## Key dbt Commands Reference

```bash
dbt debug              # Test connection to Snowflake
dbt deps               # Install packages from packages.yml
dbt seed               # Load CSV seeds to Snowflake
dbt run                # Build all models
dbt test               # Run all tests
dbt docs generate      # Generate documentation site
dbt docs serve         # Open documentation in browser (http://localhost:8080)
dbt source freshness   # Check if source data is up to date
dbt build              # Run + Test in one command (recommended for CI/CD)
```

---

## Environment Variables Reference

| Variable | Required | Description |
|----------|----------|-------------|
| `TMDB_API_KEY` | Yes | TMDb API key from https://www.themoviedb.org/settings/api |
| `TMDB_PAGES_TO_FETCH` | No | Pages to fetch (default: 20) |
| `SNOWFLAKE_ACCOUNT` | Yes | Account identifier (e.g., xy12345.us-east-1) |
| `SNOWFLAKE_USER` | Yes | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Yes | Snowflake password |
| `SNOWFLAKE_ROLE` | No | Role (default: DE_ROLE) |
| `SNOWFLAKE_WAREHOUSE` | No | Warehouse (default: LECTURE_INGEST_WH) |
| `SNOWFLAKE_DATABASE` | No | Database (default: LECTURE_DE) |
| `BATCH_SIZE` | No | Rows per Snowflake insert batch (default: 100) |
| `LOG_LEVEL` | No | Logging verbosity: DEBUG, INFO, WARNING (default: INFO) |

---

## Business Questions This Platform Answers

Once Gold is built, students can answer real analytical questions:

1. **"Which movie genre has the highest average ROI?"**
   → `SELECT * FROM GOLD.KPI_GENRE_PERFORMANCE ORDER BY avg_roi_pct DESC`

2. **"How has the average movie budget changed from 2000 to today?"**
   → `SELECT release_year, avg_budget_millions FROM GOLD.KPI_YEARLY_TRENDS WHERE release_year >= 2000`

3. **"What percentage of movies are profitable?"**
   → `SELECT profitability_flag, COUNT(*) FROM GOLD.FACT_MOVIES GROUP BY 1`

4. **"Which studio produces the most movies in a year?"**
   → Join `fact_movies` → `dim_production_companies`, GROUP BY company, year

5. **"Are summer blockbusters actually more profitable than winter releases?"**
   → `SELECT release_season, AVG(roi_pct) FROM GOLD.FACT_MOVIES GROUP BY 1`

6. **"What is the correlation between budget and rating quality?"**
   → `SELECT budget_tier, AVG(vote_average) FROM GOLD.FACT_MOVIES GROUP BY 1 ORDER BY 1`

---

## Troubleshooting

**"Connection failed" in Python:**
- Verify `SNOWFLAKE_ACCOUNT` format — should NOT include `.snowflakecomputing.com`
- Example correct value: `xy12345.us-east-1` (not `xy12345.us-east-1.snowflakecomputing.com`)

**"Table BRONZE.RAW_MOVIES does not exist":**
- Run `snowflake_setup/01_setup.sql` first
- Verify your user has the `DE_ROLE` role granted

**"dbt debug failed":**
- Ensure `~/.dbt/profiles.yml` exists (not just `dbt_project/profiles.yml`)
- Run `dbt debug` from inside the `dbt_project/` directory

**"Rate limited by TMDb API":**
- TMDb allows 40 requests/10 seconds on free tier
- The extractor has built-in throttling — if you hit limits, increase `REQUEST_DELAY_SECONDS` in `extract.py`

**"dbt test failed":**
- Run `dbt test --store-failures` to save failing rows to Snowflake
- Query the generated failure tables: `SELECT * FROM DBT_TEST__AUDIT.NOT_NULL_*`

---

## Next Steps 

After completing this project, explore these advanced topics:

1. **Orchestration:** Schedule the Python pipeline + dbt run with Apache Airflow or Prefect
2. **Incremental loading:** Modify the pipeline to only fetch movies newer than the last run
3. **SCD Type 2:** Track historical changes to movie attributes (title changes, status changes)
4. **dbt Snapshots:** Automatically capture SCD Type 2 history
5. **Data Contracts:** Add `dbt_contracts` for enforcing schema agreements between teams
6. **CI/CD:** Add GitHub Actions to run `dbt build` on every pull request
7. **Monitoring:** Add Great Expectations or Monte Carlo for production data quality monitoring
