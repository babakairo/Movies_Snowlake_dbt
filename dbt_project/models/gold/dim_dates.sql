{{
    config(
        materialized = 'table',
        schema       = 'GOLD',
        tags         = ['gold', 'dimension']
    )
}}

/*
=============================================================================
  MODEL: dim_dates
  LAYER: Gold (Dimension)
  TYPE: Date/Calendar Dimension
=============================================================================
TEACHING NOTE — Why Build a Date Dimension?

  Every analytical query filters and groups by time.
  Without dim_dates, analysts write functions directly in fact table queries:
    WHERE YEAR(release_date) = 2023

  WITH dim_dates, analysts write readable, consistent SQL:
    JOIN dim_dates ON fact_movies.date_sk = dim_dates.date_sk
    WHERE dim_dates.release_year = 2023
    AND   dim_dates.is_summer_release = TRUE

  Benefits of a proper date dimension:
    1. Business calendar attributes (is_holiday, fiscal_quarter) — hard to compute on the fly
    2. Consistent date logic — one place to fix errors, not scattered queries
    3. BI tool integration — Tableau/Power BI use date dimensions for time intelligence
    4. Performance — pre-compute year/quarter/month avoids repeated function calls

  This uses dbt_utils.date_spine to generate one row per day
  between date_spine_start and date_spine_end (configured in dbt_project.yml).

  dbt_utils.date_spine is a macro that generates a series of dates.
  It's much better than a recursive CTE for portability.
=============================================================================
*/

with date_spine as (

    {{
        dbt_utils.date_spine(
            datepart = "day",
            start_date = "cast('" ~ var('date_spine_start') ~ "' as date)",
            end_date   = "cast('" ~ var('date_spine_end')   ~ "' as date)"
        )
    }}

),

enriched as (

    select
        -- Surrogate key — use integer YYYYMMDD format for efficiency
        -- BI tools join on integers faster than date types
        to_number(to_char(date_day, 'YYYYMMDD'))    as date_sk,

        -- Natural key
        date_day                                     as full_date,

        -- Standard calendar attributes
        year(date_day)                               as calendar_year,
        quarter(date_day)                            as calendar_quarter,
        month(date_day)                              as calendar_month,
        day(date_day)                                as day_of_month,
        dayofweek(date_day)                          as day_of_week,      -- 0=Sun, 6=Sat
        dayofyear(date_day)                          as day_of_year,
        weekofyear(date_day)                         as week_of_year,

        -- Human-readable labels (useful for dashboard axis labels)
        to_char(date_day, 'MMMM')                   as month_name,       -- "January"
        to_char(date_day, 'MON')                     as month_name_short, -- "JAN"
        to_char(date_day, 'Day')                     as day_name,         -- "Monday   "
        'Q' || quarter(date_day)                     as quarter_label,    -- "Q1"
        to_char(date_day, 'YYYY-MM')                 as year_month,       -- "2023-07"

        -- Boolean flags
        case when dayofweek(date_day) in (0, 6)
             then true else false end                as is_weekend,

        case when dayofweek(date_day) not in (0, 6)
             then true else false end                as is_weekday,

        -- Release season (useful for movie analytics — summer blockbuster season!)
        case
            when month(date_day) in (6, 7, 8)   then 'SUMMER'
            when month(date_day) in (11, 12)     then 'HOLIDAY'
            when month(date_day) in (1, 2, 3)    then 'WINTER'
            when month(date_day) in (4, 5)       then 'SPRING'
            when month(date_day) in (9, 10)      then 'FALL'
        end                                          as release_season,

        -- Fiscal year (January-start = same as calendar year)
        year(date_day)                               as fiscal_year,
        quarter(date_day)                            as fiscal_quarter

    from date_spine

)

select * from enriched
order by full_date
