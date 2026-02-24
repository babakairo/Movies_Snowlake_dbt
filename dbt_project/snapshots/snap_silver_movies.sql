
{% snapshot snap_silver_movies %}

{{
    config(
        target_database         = 'LECTURE_DE',
        target_schema           = 'DBT_STAGING',
        unique_key              = 'movie_id',
        strategy                = 'check',
        check_cols              = [
            'budget_usd',
            'revenue_usd',
            'vote_average',
            'popularity_score',
            'status',
            'runtime_minutes'
        ],
        invalidate_hard_deletes = true,
        tags                    = ['snapshot', 'scd2', 'silver']
    )
}}

/*
=============================================================================
  SNAPSHOT: snap_silver_movies
  STRATEGY: check (Slowly Changing Dimension Type 2)
  SOURCE:   {{ ref('silver_movies') }}
  TARGET:   LECTURE_DE.DBT_STAGING.snap_silver_movies
=============================================================================
TEACHING NOTE — Slowly Changing Dimensions (SCD Type 2)

  THE BUSINESS PROBLEM:
  ─────────────────────────────────────────────────────────────────────────
  A movie's attributes CHANGE over time on TMDb:
    - revenue_usd:      Box office numbers updated weekly as films roll out
    - budget_usd:       Studios sometimes disclose actual budget months later
    - vote_average:     Audience ratings accumulate over years (number shifts)
    - status:           'Post Production' → 'Released' → changes over time
    - popularity_score: Spikes when a sequel releases or a film goes viral
    - runtime_minutes:  Sometimes corrected after initial data entry errors

  NAIVE APPROACH (simple overwrite):
    UPDATE silver_movies SET revenue_usd = 500000000 WHERE movie_id = 123
    ❌ You LOSE the history. You can't answer:
       "What was the revenue figure when we ran the Q1 analysis?"

  SCD TYPE 2 SOLUTION (keep every version):
    Never update — add a new row for each change, mark old row as expired.
    ✅ Full history preserved; point-in-time queries possible.

HOW dbt SNAPSHOTS ADD SCD2 COLUMNS AUTOMATICALLY:
  ─────────────────────────────────────────────────────────────────────────
  dbt automatically adds 4 columns to the snapshot table:

    dbt_scd_id     → Hash unique to THIS VERSION of the record
    dbt_updated_at → Timestamp this version was written by dbt
    dbt_valid_from → When this version became active
    dbt_valid_to   → When this version expired (NULL = currently active)

  Example — movie 123 revenue updated on March 1:

  BEFORE update (first run):
  ┌──────────┬─────────────┬────────────────────┬──────────────┐
  │ movie_id │ revenue_usd │ dbt_valid_from     │ dbt_valid_to │
  ├──────────┼─────────────┼────────────────────┼──────────────┤
  │ 123      │ NULL        │ 2024-01-15 08:00   │ NULL ← active│
  └──────────┴─────────────┴────────────────────┴──────────────┘

  AFTER update (second run):
  ┌──────────┬─────────────┬────────────────────┬────────────────────┐
  │ movie_id │ revenue_usd │ dbt_valid_from     │ dbt_valid_to       │
  ├──────────┼─────────────┼────────────────────┼────────────────────┤
  │ 123      │ NULL        │ 2024-01-15 08:00   │ 2024-03-01 08:00 ←expired│
  │ 123      │ 500000000   │ 2024-03-01 08:00   │ NULL ← active now  │
  └──────────┴─────────────┴────────────────────┴────────────────────┘

  Point-in-time query — "What was the revenue on Feb 1, 2024?"
    WHERE movie_id = 123
      AND dbt_valid_from  <= '2024-02-01'
      AND (dbt_valid_to    > '2024-02-01' OR dbt_valid_to IS NULL)
    → Returns: revenue_usd = NULL (still unknown at that date)

SNAPSHOT STRATEGIES:
  ─────────────────────────────────────────────────────────────────────────
  strategy = 'timestamp'
    dbt looks at ONE column (e.g., updated_at) to detect changes.
    Faster — compares one timestamp vs. last run.
    Requires: a reliable updated_at field in the source.

  strategy = 'check'  ← WE USE THIS
    dbt compares the ACTUAL VALUES of specified columns.
    Works when there is no reliable updated_at column.
    More thorough — detects any value change regardless of timestamps.

WHEN TO RUN SNAPSHOTS:
  ─────────────────────────────────────────────────────────────────────────
  Command: dbt snapshot

  Run AFTER silver is built (so silver data is fresh when snapshot runs).
  Run BEFORE gold (so gold can optionally join to snapshot for history).

  Dagster asset order:
    ingest → bronze → silver → SNAPSHOT → gold → marts → tests

INCREMENTAL DEMO — HOW TO TRIGGER AN SCD2 CHANGE:
  ─────────────────────────────────────────────────────────────────────────
  See scripts/incremental_demo.sql for step-by-step instructions.
  High-level steps:
    1. Run first snapshot → establishes baseline
    2. Update revenue in RAW_MOVIES (simulates re-ingest)
    3. dbt run --models bronze_movies_raw silver_movies
    4. dbt snapshot
    5. Query snap_silver_movies WHERE movie_id = <your test movie>
    → You'll see 2 rows: the old version (expired) + new version (active)

=============================================================================
*/

select
    movie_id,
    title,
    status,

    -- Financial attributes — most likely to change after initial release
    budget_usd,
    revenue_usd,
    profit_usd,
    roi_pct,

    -- Quality metrics — shift as more people vote over time
    vote_average,
    vote_count,
    popularity_score,

    -- Technical attributes — sometimes corrected post-publication
    runtime_minutes,
    original_language,

    -- Release metadata
    release_date,
    release_year,
    release_month,
    release_season,

    -- Pipeline lineage columns
    _source_ingested_at,
    _source_batch_id,
    _silver_loaded_at

from {{ ref('silver_movies') }}

{% endsnapshot %}
