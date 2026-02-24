
{% snapshot snap_dim_genres %}

{{
    config(
        target_database = 'LECTURE_DE',
        target_schema   = 'DBT_STAGING',
        unique_key      = 'genre_id',
        strategy        = 'check',
        check_cols      = [
            'genre_name',
            'genre_group',
            'is_blockbuster_genre'
        ],
        invalidate_hard_deletes = true,
        tags = ['snapshot', 'scd2', 'gold', 'dimension']
    )
}}

/*
=============================================================================
  SNAPSHOT: snap_dim_genres
  STRATEGY: check (Slowly Changing Dimension Type 2)
  SOURCE:   {{ ref('dim_genres') }}
  TARGET:   LECTURE_DE.DBT_STAGING.snap_dim_genres
=============================================================================
TEACHING NOTE — SCD2 on a Dimension Table

  WHY SNAPSHOT A DIMENSION (not just facts)?
  ─────────────────────────────────────────────────────────────────────────
  Genres are relatively stable, but business enrichment CAN change:

  Scenario 1 — Business logic update:
    Your analyst decides 'THRILLER' should move from NARRATIVE → HIGH_OCTANE
    Without SCD2: genre_group for ALL thriller movies changes retroactively
    With SCD2: Historical data keeps the old classification; only new runs
               use the updated grouping. Enables "before vs after" comparison.

  Scenario 2 — TMDb adds a new genre:
    TMDb adds "Superhero" as genre 10770
    You add it to dim_genres → SCD2 captures the "birth" of this category
    Analysts can filter: WHERE genre added AFTER '2023-01-01'

  Scenario 3 — Reclassification project:
    Marketing reclassifies DOCUMENTARY from ARTS → NICHE
    SCD2 lets you analyze "pre-reclassification" vs "post-reclassification"
    financial performance

  THE SLOWLY CHANGING DIMENSION TYPES:
  ─────────────────────────────────────────────────────────────────────────
  SCD Type 1: Overwrite
    Simple UPDATE — no history
    Use: Low-value attributes (typo fixes, abbreviation standardization)

  SCD Type 2: New Row (this snapshot!)
    New row per change — full history via dbt_valid_from/dbt_valid_to
    Use: Attributes with business analytical value (genre_group, studio_tier)

  SCD Type 3: Add Column
    Keep "current_value" and "previous_value" in same row
    Use: Only track ONE previous state; simple but limited

  SCD Type 6: Hybrid (1+2+3)
    Current value column + full history rows
    Use: When you need fast "current value" lookup AND full history

  For teaching simplicity, we implement SCD Type 2 (the industry standard).

CONNECTING SNAPSHOT TO GOLD MODELS:
  ─────────────────────────────────────────────────────────────────────────
  To do point-in-time genre analysis in gold/marts:

    SELECT f.*, sg.genre_name, sg.genre_group
    FROM fact_movies f
    JOIN snap_dim_genres sg
      ON f.primary_genre_id = sg.genre_id
     AND f.release_date BETWEEN sg.dbt_valid_from AND coalesce(sg.dbt_valid_to, current_date())

  This ensures each movie's genre classification uses the genre definition
  that was ACTIVE at the time the movie was released.

=============================================================================
*/

select
    genre_sk,
    genre_id,
    genre_name,
    genre_group,
    is_blockbuster_genre,

    -- Metadata for tracking when this version was created
    _silver_loaded_at,
    _gold_loaded_at

from {{ ref('dim_genres') }}

{% endsnapshot %}
