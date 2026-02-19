{{
    config(
        materialized = 'table',
        schema       = 'GOLD',
        tags         = ['gold', 'kpi', 'mart']
    )
}}

/*
=============================================================================
  MODEL: kpi_genre_performance
  LAYER: Gold (KPI Mart)
  SOURCE: {{ ref('fact_movies') }}, {{ ref('dim_genres') }}
=============================================================================
TEACHING NOTE — KPI Marts vs Raw Fact Tables:

  Analysts COULD query fact_movies directly for every report.
  But:
    - Complex aggregations run on every dashboard refresh (expensive)
    - Different analysts may write the same aggregation DIFFERENTLY (inconsistency)
    - BI tools are faster when reading pre-aggregated tables (less computation)

  KPI Mart solution:
    - Pre-aggregate ONCE during the dbt run
    - Dashboards SELECT from this mart — simple, fast, consistent
    - Single source of truth for "What is the revenue by genre?" answer

  This mart answers business questions like:
    Q: "Which genre generates the highest average ROI?"
    Q: "How many Action movies were released vs Drama?"
    Q: "Which genre has the most consistent quality (lowest rating std dev)?"
    Q: "Which genre dominates blockbuster productions ($100M+ budget)?"

AGGREGATION RULES:
  We use ONLY movies with known budget AND revenue for ROI metrics
  (otherwise we'd compare movies with data against movies without — unfair).
  But for count/rating metrics, we include ALL movies.
=============================================================================
*/

with fact as (

    select * from {{ ref('fact_movies') }}

),

genres as (

    select * from {{ ref('dim_genres') }}

),

-- Movies with full financial data (for revenue/ROI aggregations)
financially_complete as (

    select *
    from fact
    where budget_usd  is not null
      and revenue_usd is not null
      and budget_usd  > 0

),

genre_stats as (

    select
        g.genre_sk,
        g.genre_id,
        g.genre_name,
        g.genre_group,
        g.is_blockbuster_genre,

        -- ── Volume Metrics (all movies) ─────────────────────────────────────
        count(distinct f.movie_id)                              as total_movies,
        count(distinct case when f.release_year >= year(current_date()) - 5
                           then f.movie_id end)                 as movies_last_5_years,

        -- ── Financial Metrics (financially complete movies only) ─────────────
        count(distinct fc.movie_id)                             as movies_with_financial_data,

        round(sum(fc.revenue_usd) / 1000000, 2)                as total_revenue_millions,
        round(avg(fc.revenue_usd) / 1000000, 2)                as avg_revenue_millions,
        round(median(fc.revenue_usd) / 1000000, 2)             as median_revenue_millions,
        round(max(fc.revenue_usd) / 1000000, 2)                as max_revenue_millions,

        round(avg(fc.budget_usd) / 1000000, 2)                 as avg_budget_millions,

        round(avg(fc.profit_usd) / 1000000, 2)                 as avg_profit_millions,
        round(sum(fc.profit_usd) / 1000000, 2)                 as total_profit_millions,

        round(avg(fc.roi_pct), 2)                               as avg_roi_pct,
        round(median(fc.roi_pct), 2)                            as median_roi_pct,

        -- Percentage of movies in this genre that were profitable
        round(
            100.0 * count(distinct case when fc.profit_usd > 0 then fc.movie_id end)
            / nullif(count(distinct fc.movie_id), 0),
            1
        )                                                       as pct_movies_profitable,

        -- ── Quality Metrics (all movies with ratings) ────────────────────────
        round(avg(f.vote_average), 2)                           as avg_rating,
        round(stddev(f.vote_average), 2)                        as rating_stddev,   -- Consistency
        round(max(f.vote_average), 2)                           as highest_rating,
        round(avg(f.vote_count), 0)                             as avg_vote_count,

        -- ── Popularity Metrics ───────────────────────────────────────────────
        round(avg(f.popularity_score), 2)                       as avg_popularity,
        round(sum(f.popularity_score), 2)                       as total_popularity,

        -- ── Runtime Metrics ──────────────────────────────────────────────────
        round(avg(f.runtime_minutes), 0)                        as avg_runtime_minutes,

        -- ── Metadata ─────────────────────────────────────────────────────────
        max(f._gold_loaded_at)                                  as data_as_of

    from genres g
    -- LEFT JOIN ensures all genres appear even if no movies are tagged with them
    left join fact f
        on g.genre_sk = f.genre_sk
    left join financially_complete fc
        on g.genre_id = fc.primary_genre_id

    group by
        g.genre_sk,
        g.genre_id,
        g.genre_name,
        g.genre_group,
        g.is_blockbuster_genre

),

-- Add rankings so analysts can answer "what is genre X's rank?" instantly
ranked as (

    select
        *,
        rank() over (order by avg_roi_pct          desc nulls last) as roi_rank,
        rank() over (order by total_revenue_millions desc nulls last) as revenue_rank,
        rank() over (order by avg_rating            desc nulls last) as rating_rank,
        rank() over (order by total_movies          desc nulls last) as volume_rank
    from genre_stats

)

select * from ranked
order by total_revenue_millions desc nulls last
