
{{
    config(
        materialized = 'table',
        schema       = 'GOLD',
        tags         = ['gold', 'marts', 'kpi']
    )
}}

/*
=============================================================================
  MODEL: kpi_top_movies
  LAYER: Marts (Business-Facing KPI Layer)
  SOURCE: {{ ref('fact_movies') }}, {{ ref('dim_genres') }},
          {{ ref('dim_production_companies') }}
=============================================================================
TEACHING NOTE — Multi-Metric Rankings and Window Functions

  FOUR RANKING FUNCTIONS IN SQL:
  ─────────────────────────────────────────────────────────────────────────
  RANK()         → Gaps after ties:          1, 2, 2, 4, 5   (no #3)
  DENSE_RANK()   → No gaps after ties:       1, 2, 2, 3, 4   (we use this)
  ROW_NUMBER()   → Unique sequential rank:   1, 2, 3, 4, 5   (arbitrary tiebreak)
  NTILE(n)       → Divides into n buckets:   10 buckets → deciles

  PERCENT_RANK() → Relative rank 0.0–1.0
    = (rank - 1) / (total_rows - 1)
    Score of 0.90 means "better than 90% of movies"

  WHY DENSE_RANK FOR MOVIES?
    If Avengers and Titanic BOTH have the highest revenue:
      RANK()       → Both get rank 1, next movie gets rank 3 (skips 2)
      DENSE_RANK() → Both get rank 1, next movie gets rank 2 (no gap)
    Dense rank is more intuitive for "Top 10 movies" use cases.

  THE VOTE_COUNT >= 100 FILTER:
  ─────────────────────────────────────────────────────────────────────────
  A movie with 5 votes and a 10/10 rating is NOT "the top rated movie".
  The 100-vote minimum ensures statistical meaningfulness.

  This is the "Bayesian average" concept — ratings from many voters
  are more trustworthy than ratings from few voters.

  In production, a weighted rating (like IMDb's formula) would be even better:
    Weighted = (v/(v+m)) × R + (m/(v+m)) × C
    Where: v=vote_count, m=min_votes, R=avg_rating, C=mean_rating_overall

  This model answers (by stakeholder):
    CFO:      Top by revenue (raw box office performance)
    Investor: Top by ROI (return on investment efficiency)
    Critic:   Top by vote_average (quality consensus)
    Marketer: Top by popularity_score (audience engagement momentum)
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
        f._gold_loaded_at,
        current_timestamp()                     as _marts_loaded_at

    from fact f
    left join genres g   on f.genre_sk   = g.genre_sk
    left join companies c on f.company_sk = c.company_sk

)

select * from enriched
order by revenue_rank_all_time asc nulls last
