
{{
    config(
        materialized = 'table',
        schema       = 'GOLD',
        tags         = ['gold', 'marts', 'kpi']
    )
}}

/*
=============================================================================
  MODEL: kpi_genre_performance
  LAYER: Marts (Business-Facing KPI Layer)
  SOURCE: {{ ref('fact_movies') }}, {{ ref('dim_genres') }}
=============================================================================
TEACHING NOTE — Marts Layer vs Gold Layer

  WHY HAVE A SEPARATE MARTS LAYER?
  ─────────────────────────────────────────────────────────────────────────
  Gold Layer (dim_* + fact_*) = Star Schema
    - Normalized for analytical flexibility
    - Built for joining — one fact table joins to any dimension
    - Optimized for OLAP: any slice/dice/pivot by analysts

  Marts Layer (kpi_*) = Pre-Aggregated Business Views
    - Denormalized for specific business questions
    - Built for dashboards — one table = one dashboard panel
    - Optimized for BI: minimal SQL in the BI tool, just SELECT

  The marts layer sits ABOVE gold and answers specific business questions:
    "What is genre performance this year?" → kpi_genre_performance
    "What are yearly industry trends?"     → kpi_yearly_trends
    "What are the top movies?"             → kpi_top_movies

  MATERIALIZATION OPTIONS FOR MARTS:
  ─────────────────────────────────────────────────────────────────────────
  Current: materialized='table'
    - Full rebuild every dbt run
    - Best for: teaching, small marts (<1M rows), infrequent BI queries

  Alternative 1: materialized='view'
    - No data stored; SQL re-runs on every dashboard refresh
    - Best for: real-time data, small fact tables, <10 BI users
    - Warning: complex aggregations are SLOW on views for large fact tables

  Alternative 2: Snowflake Dynamic Table (Snowflake Enterprise+)
    ────────────────────────────────────────────────────────────────────
    TEACHING: {%- raw %} and {% endraw -%} tell dbt's Jinja engine to treat
    the block as plain text, so example config() calls inside comments
    are not parsed as real dbt directives.
{%- raw %}
    {{ config(
        materialized = 'dynamic_table',
        target_lag   = '1 day',               -- Max staleness
        snowflake_warehouse = 'LECTURE_TRANSFORM_WH'
    ) }}
{%- endraw %}
    ────────────────────────────────────────────────────────────────────
    - Snowflake automatically refreshes when source data changes
    - Best for: production marts needing freshness without scheduled runs
    - Supports full SQL including JOINs, subqueries, window functions

  Alternative 3: Snowflake Materialized View
    ────────────────────────────────────────────────────────────────────
{%- raw %}
    {{ config(materialized = 'materialized_view') }}
{%- endraw %}
    ────────────────────────────────────────────────────────────────────
    - Auto-refreshed by Snowflake, but LIMITED SQL support:
      ❌ No GROUP BY + aggregate
      ❌ No JOINs
      ❌ No Window functions (RANK, LAG, etc.)
    - Only useful for simple projections/filters — NOT these KPI models

  RECOMMENDATION FOR PRODUCTION:
    Use 'dynamic_table' for KPIs. It gives you:
    ✅ Auto-refresh   ✅ Full SQL   ✅ Snowflake manages compute cost

  This model answers:
    Q: "Which genre generates the highest average ROI?"
    Q: "How many Action movies were released vs Drama?"
    Q: "Which genre has the most consistent quality (lowest rating std dev)?"
    Q: "Which genre dominates blockbuster productions ($100M+ budget)?"

AGGREGATION RULES:
  Financial metrics: only movies with known budget AND revenue (non-zero)
  Volume/quality metrics: all movies in that genre
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
