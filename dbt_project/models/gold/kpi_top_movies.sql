{{
    config(
        materialized = 'table',
        schema       = 'GOLD',
        tags         = ['gold', 'kpi', 'mart']
    )
}}

/*
=============================================================================
  MODEL: kpi_top_movies
  LAYER: Gold (KPI Mart)
  SOURCE: {{ ref('fact_movies') }}, {{ ref('dim_genres') }},
          {{ ref('dim_production_companies') }}
=============================================================================
TEACHING NOTE — Multi-Metric Rankings:

  Different stakeholders define "top" differently:
    CFO:      Top by total revenue (raw box office performance)
    Investor: Top by ROI (return on investment — efficiency)
    Critic:   Top by vote average (quality)
    Marketer: Top by popularity score (audience engagement)

  This mart provides ALL rankings in one place using:
    DENSE_RANK() — like RANK() but no gaps (1,2,3,3,4 vs 1,2,3,3,5)
    PERCENT_RANK() — relative rank as a percentile (0.0 to 1.0)
    NTILE(10) — divides into deciles (1=bottom 10%, 10=top 10%)

  The movie joins to genre and company for display attributes —
  a common pattern: fact contains FKs, dims contain labels.

  TEACHING: Never store display labels (genre_name) in the fact table.
  Always join to the dimension at query time.
  This ensures consistency — one change in dim_genres propagates everywhere.
=============================================================================
*/

with fact as (

    select *
    from {{ ref('fact_movies') }}
    -- Only include movies with enough votes to be statistically meaningful
    where vote_count >= 100

),

genres as (

    select genre_sk, genre_name
    from {{ ref('dim_genres') }}

),

companies as (

    select company_sk, company_name, studio_tier
    from {{ ref('dim_production_companies') }}

),

enriched as (

    select
        -- ── Identifiers ───────────────────────────────────────────────────────
        f.movie_id,
        f.movie_sk,
        f.title,
        f.imdb_id,
        f.release_year,
        f.original_language,

        -- ── Dimension Attributes (joined from dims) ────────────────────────────
        coalesce(g.genre_name, 'UNKNOWN')       as primary_genre,
        coalesce(c.company_name, 'UNKNOWN')     as primary_company,
        coalesce(c.studio_tier, 'UNKNOWN')      as studio_tier,

        -- ── Financial Metrics ─────────────────────────────────────────────────
        f.budget_usd,
        f.revenue_usd,
        f.profit_usd,
        f.roi_pct,
        f.budget_tier,
        f.profitability_flag,

        -- ── Quality & Popularity ──────────────────────────────────────────────
        f.vote_average,
        f.vote_count,
        f.popularity_score,
        f.runtime_minutes,

        -- ── Revenue Rankings (among all movies with known revenue) ───────────
        dense_rank() over (
            order by f.revenue_usd desc nulls last
        )                                       as revenue_rank_all_time,

        dense_rank() over (
            partition by f.release_year
            order by f.revenue_usd desc nulls last
        )                                       as revenue_rank_in_year,

        -- ── ROI Rankings (among financially complete movies) ─────────────────
        dense_rank() over (
            order by f.roi_pct desc nulls last
        )                                       as roi_rank_all_time,

        -- ── Rating Rankings ───────────────────────────────────────────────────
        dense_rank() over (
            order by f.vote_average desc, f.vote_count desc
        )                                       as rating_rank_all_time,

        -- ── Popularity Rankings ───────────────────────────────────────────────
        dense_rank() over (
            order by f.popularity_score desc
        )                                       as popularity_rank_all_time,

        -- ── Percentile Scores (0=bottom, 1=top) ──────────────────────────────
        -- Useful for "this movie is in the top X%" type statements
        round(percent_rank() over (order by f.revenue_usd     asc nulls first), 4)  as revenue_percentile,
        round(percent_rank() over (order by f.vote_average     asc nulls first), 4)  as rating_percentile,
        round(percent_rank() over (order by f.popularity_score asc nulls first), 4)  as popularity_percentile,

        -- ── Decile Buckets (1=bottom 10%, 10=top 10%) ────────────────────────
        -- Useful for "decile analysis" — segment movies into quality buckets
        ntile(10) over (order by f.vote_average     asc nulls first) as rating_decile,
        ntile(10) over (order by f.revenue_usd      asc nulls first) as revenue_decile,

        -- ── Metadata ─────────────────────────────────────────────────────────
        f._gold_loaded_at

    from fact f
    left join genres g   on f.genre_sk   = g.genre_sk
    left join companies c on f.company_sk = c.company_sk

)

select * from enriched
order by revenue_rank_all_time asc nulls last
