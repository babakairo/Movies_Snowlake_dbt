-- =============================================================================
-- INCREMENTAL DATA DEMO SCRIPT
-- File: scripts/incremental_demo.sql
-- =============================================================================
-- TEACHING PURPOSE:
--   This script demonstrates two core data engineering concepts:
--     1. INCREMENTAL MODELS — only processing NEW or CHANGED rows
--     2. SCD TYPE 2 — keeping full history of attribute changes
--
-- Since the TMDb API returns the same data each time (no true "delta" feed),
-- we manually insert rows into RAW_MOVIES to simulate new data arriving.
-- This is a REALISTIC scenario: in real pipelines, you often need to test
-- incremental logic without waiting for live API changes.
--
-- PREREQUISITES:
--   - Snowflake setup complete (run snowflake_setup/ scripts)
--   - At least one dbt run completed (so silver_movies + fact_movies exist)
--   - Snapshots have been run once (dbt snapshot) to establish baseline
--
-- INSTRUCTIONS:
--   Run sections 1 through 6 in order.
--   After each dbt command, query the results to observe the changes.
--
-- SNOWFLAKE ROLE: Use DE_ROLE (has INSERT on BRONZE schema)
-- =============================================================================

USE ROLE DE_ROLE;
USE DATABASE LECTURE_DE;
USE WAREHOUSE LECTURE_INGEST_WH;


-- =============================================================================
-- SECTION 1: Check the current state BEFORE making any changes
-- =============================================================================
-- TEACHING: Establish a baseline — know what exists before you insert.

-- Count current movies in each layer
SELECT 'raw_movies'    as layer, count(*) as row_count FROM BRONZE.RAW_MOVIES
UNION ALL
SELECT 'silver_movies' as layer, count(*) as row_count FROM SILVER.SILVER_MOVIES
UNION ALL
SELECT 'fact_movies'   as layer, count(*) as row_count FROM GOLD.FACT_MOVIES;

-- Check the most recent ingestion timestamps
SELECT
    max(ingested_at)    as latest_ingestion,
    min(ingested_at)    as earliest_ingestion,
    count(*)            as total_raw_rows
FROM BRONZE.RAW_MOVIES;


-- =============================================================================
-- SECTION 2: Insert NEW movies into BRONZE.RAW_MOVIES
-- =============================================================================
-- TEACHING: We simulate two new movies arriving from the TMDb API.
-- movie_id 99999991 and 99999992 are fake IDs that don't exist in TMDb.
-- The raw_data column holds a JSON structure that matches the real TMDb format.
--
-- WHY INSERT HERE (not directly into silver)?
--   The pipeline always starts at Bronze. Silver is derived from Bronze via dbt.
--   Inserting into Bronze and then running `dbt run --models silver_movies`
--   simulates the real end-to-end incremental flow.
--
-- AFTER running this section, run:
--   dbt run --models bronze_movies_raw silver_movies fact_movies
--   dbt run --models marts

INSERT INTO BRONZE.RAW_MOVIES (movie_id, raw_data, source_url, ingested_at, batch_id)
SELECT
    99999991 AS movie_id,
    PARSE_JSON('{
        "id": 99999991,
        "title": "The Incremental Demo Film",
        "original_title": "The Incremental Demo Film",
        "overview": "A teaching movie inserted to demonstrate incremental dbt models.",
        "release_date": "2024-03-15",
        "budget": 5000000,
        "revenue": 22000000,
        "runtime": 105,
        "status": "Released",
        "vote_average": 7.3,
        "vote_count": 150,
        "popularity": 42.5,
        "original_language": "en",
        "imdb_id": "tt9999991",
        "genres": [
            {"id": 28, "name": "Action"},
            {"id": 12, "name": "Adventure"}
        ],
        "production_companies": [
            {"id": 420, "name": "Marvel Studios", "origin_country": "US"}
        ]
    }') AS raw_data,
    'https://api.themoviedb.org/3/movie/99999991' AS source_url,
    CURRENT_TIMESTAMP() AS ingested_at,
    'DEMO_BATCH_001'  AS batch_id

UNION ALL

SELECT
    99999992 AS movie_id,
    PARSE_JSON('{
        "id": 99999992,
        "title": "SCD2 History Film",
        "original_title": "SCD2 History Film",
        "overview": "A movie that will have its revenue updated to demonstrate SCD2.",
        "release_date": "2024-01-20",
        "budget": 80000000,
        "revenue": 0,
        "runtime": 132,
        "status": "Released",
        "vote_average": 6.8,
        "vote_count": 234,
        "popularity": 67.2,
        "original_language": "en",
        "imdb_id": "tt9999992",
        "genres": [
            {"id": 18, "name": "Drama"}
        ],
        "production_companies": [
            {"id": 33, "name": "Universal Pictures", "origin_country": "US"}
        ]
    }') AS raw_data,
    'https://api.themoviedb.org/3/movie/99999992' AS source_url,
    CURRENT_TIMESTAMP() AS ingested_at,
    'DEMO_BATCH_001' AS batch_id;

-- Confirm insertion
SELECT movie_id, raw_data:title::varchar as title, ingested_at
FROM BRONZE.RAW_MOVIES
WHERE movie_id IN (99999991, 99999992);


-- =============================================================================
-- SECTION 3: Run dbt to process the new rows (run in your terminal)
-- =============================================================================
-- TEACHING: Run ONLY the affected models. No need to rebuild everything.
-- dbt's incremental logic: only rows where ingested_at > last_run_max are processed.

/*
  Open your terminal and run:

  cd c:\Users\maung\Desktop\Snowflake_DBT\dbt_project

  # Option A: Run the full downstream chain from bronze
  dbt run --models bronze_movies_raw+

  # Option B: Run just silver (if bronze already has the new rows)
  dbt run --models silver_movies silver_genres silver_movie_genres silver_production_companies

  # Option C: Run everything
  dbt run

  After running, check silver_movies for the new rows (Section 4 below).
*/


-- =============================================================================
-- SECTION 4: Verify new rows appear in SILVER and GOLD
-- =============================================================================
-- TEACHING: After `dbt run`, the incremental models should have MERGEd the new rows.

-- Check silver_movies
SELECT
    movie_id,
    title,
    release_date,
    budget_usd,
    revenue_usd,
    vote_average,
    _silver_loaded_at
FROM SILVER.SILVER_MOVIES
WHERE movie_id IN (99999991, 99999992);

-- Check fact_movies (gold layer)
SELECT
    movie_id,
    title,
    budget_usd,
    revenue_usd,
    profit_usd,
    profitability_flag,
    _gold_loaded_at
FROM GOLD.FACT_MOVIES
WHERE movie_id IN (99999991, 99999992);

-- Check KPI marts updated (year 2024 row should include our new movies)
SELECT release_year, total_movies_released
FROM GOLD.KPI_YEARLY_TRENDS
WHERE release_year = 2024;


-- =============================================================================
-- SECTION 5: Run dbt SNAPSHOT (to capture the initial SCD2 state)
-- =============================================================================
-- TEACHING: FIRST run of `dbt snapshot` creates the baseline snapshot records.
-- Both inserted movies get a row with dbt_valid_to = NULL (currently active).

/*
  Run in terminal:
    dbt snapshot
*/

-- Verify snapshot captured our test movies
SELECT
    movie_id,
    title,
    budget_usd,
    revenue_usd,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at
FROM DBT_STAGING.SNAP_SILVER_MOVIES
WHERE movie_id IN (99999991, 99999992)
ORDER BY movie_id, dbt_valid_from;

-- Expected result: 2 rows, both with dbt_valid_to = NULL (active)
-- ┌──────────┬─────────────────────────┬────────────┬─────────────┬────────────────────┬──────────────┐
-- │ movie_id │ title                   │ budget_usd │ revenue_usd │ dbt_valid_from     │ dbt_valid_to │
-- ├──────────┼─────────────────────────┼────────────┼─────────────┼────────────────────┼──────────────┤
-- │ 99999991 │ The Incremental Demo... │ 5000000    │ 22000000    │ 2024-xx-xx ...     │ NULL ← active│
-- │ 99999992 │ SCD2 History Film       │ 80000000   │ NULL        │ 2024-xx-xx ...     │ NULL ← active│
-- └──────────┴─────────────────────────┴────────────┴─────────────┴────────────────────┴──────────────┘


-- =============================================================================
-- SECTION 6: Simulate a DATA CHANGE (revenue reported for movie 99999992)
-- =============================================================================
-- TEACHING: Movie 99999992 had revenue=0 (unknown). Now the studio reports
-- the actual worldwide box office. This simulates a real TMDb data update.
--
-- In a real pipeline, TMDb would return updated data in the next API poll.
-- Here, we manually update the Bronze row to simulate that.

-- Simulate "re-ingestion" with updated revenue
INSERT INTO BRONZE.RAW_MOVIES (movie_id, raw_data, source_url, ingested_at, batch_id)
SELECT
    99999992 AS movie_id,
    PARSE_JSON('{
        "id": 99999992,
        "title": "SCD2 History Film",
        "original_title": "SCD2 History Film",
        "overview": "A movie that will have its revenue updated to demonstrate SCD2.",
        "release_date": "2024-01-20",
        "budget": 80000000,
        "revenue": 145000000,
        "runtime": 132,
        "status": "Released",
        "vote_average": 7.1,
        "vote_count": 892,
        "popularity": 89.4,
        "original_language": "en",
        "imdb_id": "tt9999992",
        "genres": [
            {"id": 18, "name": "Drama"}
        ],
        "production_companies": [
            {"id": 33, "name": "Universal Pictures", "origin_country": "US"}
        ]
    }') AS raw_data,
    'https://api.themoviedb.org/3/movie/99999992' AS source_url,
    CURRENT_TIMESTAMP()  AS ingested_at,   -- NEW timestamp → incremental will pick this up
    'DEMO_BATCH_002'     AS batch_id;      -- New batch_id

/*
  Now run dbt again to process the updated row:
    dbt run --models bronze_movies_raw silver_movies

  Then run the snapshot again:
    dbt snapshot

  The snapshot will detect that revenue_usd changed for movie 99999992.
  It will:
    1. Mark the OLD row as EXPIRED (set dbt_valid_to = current_timestamp)
    2. Insert a NEW row with revenue_usd = 145000000 (dbt_valid_to = NULL)
*/


-- =============================================================================
-- SECTION 7: Observe the SCD2 history after the data change
-- =============================================================================
-- TEACHING: After running `dbt snapshot` again, movie 99999992 should now
-- have TWO rows — the old version (expired) and the new version (active).

SELECT
    movie_id,
    title,
    revenue_usd,
    vote_average,
    vote_count,
    dbt_valid_from,
    dbt_valid_to,
    CASE WHEN dbt_valid_to IS NULL THEN '✓ ACTIVE (current version)'
         ELSE '✗ EXPIRED (historical version)'
    END as version_status
FROM DBT_STAGING.SNAP_SILVER_MOVIES
WHERE movie_id = 99999992
ORDER BY dbt_valid_from;

-- Expected result:
-- ┌──────────┬───────────────────┬─────────────┬──────────────┬─────────────────────┬─────────────────────┬───────────────────────────────┐
-- │ movie_id │ title             │ revenue_usd │ vote_average │ dbt_valid_from      │ dbt_valid_to        │ version_status                │
-- ├──────────┼───────────────────┼─────────────┼──────────────┼─────────────────────┼─────────────────────┼───────────────────────────────┤
-- │ 99999992 │ SCD2 History Film │ NULL        │ 6.8          │ 2024-03-01 10:00:00 │ 2024-03-01 10:05:00 │ ✗ EXPIRED (historical version)│
-- │ 99999992 │ SCD2 History Film │ 145000000   │ 7.1          │ 2024-03-01 10:05:00 │ NULL                │ ✓ ACTIVE (current version)    │
-- └──────────┴───────────────────┴─────────────┴──────────────┴─────────────────────┴─────────────────────┴───────────────────────────────┘


-- =============================================================================
-- SECTION 8: Point-in-time query (the KEY value of SCD2)
-- =============================================================================
-- TEACHING: The whole point of SCD2 is being able to ask:
--   "What did movie 99999992 look like at a specific date in the past?"
--
-- This is impossible with a simple UPDATE overwrite.
-- With SCD2, you just filter by the dbt_valid_from/dbt_valid_to window.

-- "What was the revenue for movie 99999992 AS OF its first ingestion?"
SELECT
    movie_id,
    title,
    revenue_usd,
    vote_average,
    dbt_valid_from   AS version_start,
    dbt_valid_to     AS version_end
FROM DBT_STAGING.SNAP_SILVER_MOVIES
WHERE movie_id = 99999992
  AND dbt_valid_from <= '2024-03-01 10:02:00'::timestamp   -- some time in the past
  AND (dbt_valid_to > '2024-03-01 10:02:00'::timestamp OR dbt_valid_to IS NULL)
ORDER BY dbt_valid_from;

-- → Returns: revenue_usd = NULL (the "first version" was active at 10:02:00)
-- Change the timestamp to AFTER the update to get revenue_usd = 145000000


-- =============================================================================
-- SECTION 9: Cleanup test data (run after the demo)
-- =============================================================================
-- TEACHING: Clean up fake movie_ids so they don't corrupt real data.
-- In production, you'd use a proper DELETE job or a "is_test_data" flag.

/*
  -- Uncomment and run after the demo is complete:

  DELETE FROM BRONZE.RAW_MOVIES
  WHERE movie_id IN (99999991, 99999992);

  DELETE FROM DBT_STAGING.SNAP_SILVER_MOVIES
  WHERE movie_id IN (99999991, 99999992);

  -- Then rebuild silver and gold to remove the test rows:
  dbt run --full-refresh --models silver_movies
  dbt run --models fact_movies kpi_yearly_trends kpi_genre_performance kpi_top_movies
*/
