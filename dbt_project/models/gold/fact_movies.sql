{{
    config(
        materialized = 'table',
        schema       = 'GOLD',
        tags         = ['gold', 'fact']
    )
}}

/*
=============================================================================
  MODEL: fact_movies
  LAYER: Gold (Fact Table)
  SOURCE: {{ ref('silver_movies') }}, {{ ref('silver_movie_genres') }},
          {{ ref('dim_dates') }}, {{ ref('dim_genres') }},
          {{ ref('dim_production_companies') }}
=============================================================================
TEACHING NOTE — The Fact Table: Core of a Star Schema

  In dimensional modelling (Kimball methodology), a FACT TABLE:
    - Contains MEASURES (quantifiable business metrics): revenue, budget, rating
    - Contains FOREIGN KEYS to DIMENSION tables: date_sk, genre_sk, company_sk
    - Has one row per "event" or "observation" — here, one row per movie-genre

  The GRAIN of this fact table is: ONE ROW PER MOVIE PER PRIMARY GENRE.

  WHY per genre (not just per movie)?
    - Analysts want to slice revenue BY genre
    - A movie with 3 genres needs to "appear" under each genre for slicing
    - We use the PRIMARY (first) genre for the fact row to avoid double-counting
      revenue — but the bridge table silver_movie_genres handles full genre analysis

  STAR SCHEMA STRUCTURE:
                    dim_dates
                        │
    dim_genres ──── fact_movies ──── dim_production_companies
                        │
                    dim_genres

  KEY DESIGN DECISIONS:
    1. ALL metric columns in the fact (no metrics in dimensions)
    2. Surrogate keys (not natural keys) in foreign key columns
    3. Pre-computed derived metrics (profit_usd, roi_pct) to avoid re-computation
    4. business_date is the "event date" used to join dim_dates

  COMMON MISTAKES (teach these):
    ❌ Storing text labels in the fact table — put them in dimensions
    ❌ Storing multiple grains in one fact table (mixing movie-level and genre-level)
    ❌ Null foreign keys — use "Unknown" dimension rows instead
=============================================================================
*/

with movies as (

    select * from {{ ref('silver_movies') }}

),

-- Get the PRIMARY genre for each movie (lowest genre_id = first listed)
-- This establishes the fact table grain: one row per movie
primary_genre as (

    select
        movie_id,
        genre_id,
        row_number() over (
            partition by movie_id
            order by     genre_id asc   -- First genre listed is treated as primary
        ) as genre_rank
    from {{ ref('silver_movie_genres') }}

),

primary_genre_only as (

    select movie_id, genre_id
    from primary_genre
    where genre_rank = 1

),

-- Get primary production company for each movie from the Silver bridge table
-- (avoids a direct Gold → Bronze reference which would create a graph cycle)
primary_company as (

    select movie_id, company_id
    from {{ ref('silver_movie_companies') }}
    where is_primary_company = true

),

dim_dates as (

    select date_sk, full_date
    from {{ ref('dim_dates') }}

),

dim_genres as (

    select genre_sk, genre_id
    from {{ ref('dim_genres') }}

),

dim_companies as (

    select company_sk, company_id
    from {{ ref('dim_production_companies') }}

),

joined as (

    select
        -- ── Surrogate Key ──────────────────────────────────────────────────
        -- Each fact row gets a unique key based on its movie_id
        {{ dbt_utils.generate_surrogate_key(['m.movie_id']) }}  as movie_sk,

        -- ── Foreign Keys (surrogate keys to dimension tables) ───────────────
        -- Coalesce to -1 when no match — "-1" rows are "Unknown" in dimensions
        -- This prevents NULL foreign keys which break most BI tool aggregations
        coalesce(dd.date_sk,  -1)                               as date_sk,
        coalesce(dg.genre_sk, 'UNKNOWN')                        as genre_sk,
        coalesce(dc.company_sk, 'UNKNOWN')                      as company_sk,

        -- ── Natural Keys (keep for debugging and source system tracing) ──────
        m.movie_id,
        pg.genre_id                                             as primary_genre_id,
        pc.company_id                                           as primary_company_id,

        -- ── Degenerate Dimension Attributes ─────────────────────────────────
        -- Small attributes that don't warrant their own dimension table
        m.title,
        m.original_language,
        m.status,
        m.release_year,
        m.release_month,
        m.release_season,     -- Derived in dim_dates, replicated here for convenience
        m.imdb_id,

        -- ── Measures (quantitative business metrics) ─────────────────────────
        m.budget_usd,
        m.revenue_usd,
        m.profit_usd,
        m.roi_pct,
        m.runtime_minutes,
        m.popularity_score,
        m.vote_average,
        m.vote_count,

        -- ── Derived Measures (computed from other measures) ──────────────────
        -- TEACHING: Derived measures should be PRE-COMPUTED in the fact table
        -- so every dashboard uses the same formula — no inconsistency risk
        case
            when m.budget_usd is not null and m.revenue_usd is not null
            then case when m.revenue_usd > m.budget_usd then 'PROFITABLE'
                      when m.revenue_usd = m.budget_usd then 'BREAK_EVEN'
                      else 'LOSS'
                 end
            else 'UNKNOWN'
        end                                                     as profitability_flag,

        -- Budget tier classification via seed lookup
        bt.tier_name                                            as budget_tier,

        -- ── Metadata ─────────────────────────────────────────────────────────
        m._source_ingested_at,
        current_timestamp()                                     as _gold_loaded_at

    from movies m

    -- Join to get primary genre (LEFT so movies with no genre still appear)
    left join primary_genre_only pg
        on m.movie_id = pg.movie_id

    -- Join to get primary production company
    left join primary_company pc
        on m.movie_id = pc.movie_id

    -- Resolve release_date → date_sk
    left join dim_dates dd
        on m.release_date = dd.full_date

    -- Resolve genre_id → genre_sk
    left join dim_genres dg
        on pg.genre_id = dg.genre_id

    -- Resolve company_id → company_sk
    left join dim_companies dc
        on pc.company_id = dc.company_id

    -- Budget tier enrichment from seed
    left join {{ ref('budget_tiers') }} bt
        on m.budget_usd >= bt.min_budget_usd
       and (m.budget_usd < bt.max_budget_usd or bt.max_budget_usd is null)

)

-- Final filter: only include released movies (excludes cancelled/rumoured)
select * from joined
where status = 'Released'
  or  status is null   -- Some movies have missing status — include them conservatively
