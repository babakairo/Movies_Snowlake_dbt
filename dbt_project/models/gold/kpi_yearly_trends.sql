{{
    config(
        materialized = 'table',
        schema       = 'GOLD',
        tags         = ['gold', 'kpi', 'mart']
    )
}}

/*
=============================================================================
  MODEL: kpi_yearly_trends
  LAYER: Gold (KPI Mart)
  SOURCE: {{ ref('fact_movies') }}
=============================================================================
TEACHING NOTE — Time Series Analysis in Data Warehousing:

  Year-over-year trend analysis is one of the most common business queries.
  This mart pre-computes it so BI tools get instant response.

  Key techniques demonstrated:
    1. LAG() window function — compare current year to previous year
    2. Conditional aggregation with FILTER — "count only summer releases"
    3. PERCENT_RANK() — ranking within the time series
    4. Division-by-zero protection with NULLIF()

  LAG() explained:
    lag(total_movies, 1) OVER (ORDER BY release_year)
    Returns the total_movies value from ONE year ago.
    This lets us compute YoY change without self-joins.

  Business questions answered:
    Q: "Is the movie industry releasing more films per year than 10 years ago?"
    Q: "How has average budget changed with streaming (2015–present)?"
    Q: "Which year had the highest average ROI?"
    Q: "Is ratings quality improving or declining?"
=============================================================================
*/

with fact as (

    select *
    from {{ ref('fact_movies') }}
    where release_year is not null
      and release_year >= 1980   -- Pre-1980 data is sparse and skews analytics

),

yearly as (

    select
        release_year,

        -- ── Volume ────────────────────────────────────────────────────────────
        count(distinct movie_id)                                    as total_movies_released,
        count(distinct case when original_language = 'en'          then movie_id end) as english_movies,
        count(distinct case when original_language != 'en'         then movie_id end) as non_english_movies,
        count(distinct case when release_month in (6,7,8)          then movie_id end) as summer_releases,
        count(distinct case when release_month in (11,12)          then movie_id end) as holiday_releases,

        -- ── Financial Aggregates (movies with known financials) ───────────────
        count(distinct case when budget_usd is not null            then movie_id end) as movies_with_budget,
        round(sum(revenue_usd) / 1000000, 1)                       as total_revenue_millions,
        round(avg(revenue_usd) / 1000000, 1)                       as avg_revenue_millions,
        round(avg(budget_usd)  / 1000000, 1)                       as avg_budget_millions,
        round(avg(roi_pct), 1)                                      as avg_roi_pct,
        round(
            100.0 * count(distinct case when profitability_flag = 'PROFITABLE' then movie_id end)
            / nullif(count(distinct case when budget_usd is not null then movie_id end), 0),
            1
        )                                                           as pct_profitable,

        -- ── Quality Metrics ───────────────────────────────────────────────────
        round(avg(vote_average), 2)                                 as avg_rating,
        round(avg(vote_count),   0)                                 as avg_vote_count,
        round(avg(popularity_score), 2)                             as avg_popularity,
        round(avg(runtime_minutes), 0)                              as avg_runtime_minutes,

        -- ── Budget Tier Distribution ──────────────────────────────────────────
        count(distinct case when budget_tier = 'BLOCKBUSTER'       then movie_id end) as blockbuster_count,
        count(distinct case when budget_tier = 'MAJOR'             then movie_id end) as major_count,
        count(distinct case when budget_tier = 'MID'               then movie_id end) as mid_count,
        count(distinct case when budget_tier = 'INDIE'             then movie_id end) as indie_count

    from fact
    group by release_year

),

with_yoy as (

    select
        release_year,

        -- All base metrics
        total_movies_released,
        english_movies,
        non_english_movies,
        summer_releases,
        holiday_releases,
        movies_with_budget,
        total_revenue_millions,
        avg_revenue_millions,
        avg_budget_millions,
        avg_roi_pct,
        pct_profitable,
        avg_rating,
        avg_vote_count,
        avg_popularity,
        avg_runtime_minutes,
        blockbuster_count,
        major_count,
        mid_count,
        indie_count,

        -- ── Year-over-Year Changes (LAG window function) ──────────────────────
        -- LAG(col, 1) gets the value from 1 row back (previous year)
        total_movies_released - lag(total_movies_released, 1) over (order by release_year)
                                                                    as yoy_movie_count_change,

        round(
            100.0 * (total_movies_released - lag(total_movies_released, 1) over (order by release_year))
            / nullif(lag(total_movies_released, 1) over (order by release_year), 0),
            1
        )                                                           as yoy_movie_count_pct,

        round(
            avg_budget_millions - lag(avg_budget_millions, 1) over (order by release_year),
            1
        )                                                           as yoy_budget_change_millions,

        round(
            avg_revenue_millions - lag(avg_revenue_millions, 1) over (order by release_year),
            1
        )                                                           as yoy_revenue_change_millions,

        round(
            avg_rating - lag(avg_rating, 1) over (order by release_year),
            2
        )                                                           as yoy_rating_change,

        -- ── Rankings ──────────────────────────────────────────────────────────
        rank() over (order by total_revenue_millions desc)          as revenue_rank,
        rank() over (order by total_movies_released  desc)          as volume_rank,
        rank() over (order by avg_roi_pct            desc nulls last) as roi_rank,

        current_timestamp()                                         as _gold_loaded_at

    from yearly

)

select * from with_yoy
order by release_year desc
