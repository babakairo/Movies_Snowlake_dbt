{{
    config(
        materialized        = 'incremental',
        schema              = 'SILVER',
        unique_key          = 'company_id',
        incremental_strategy = 'merge',
        tags                = ['silver', 'reference']
    )
}}

/*
=============================================================================
  MODEL: silver_production_companies
  LAYER: Silver (Reference Dimension)
  SOURCE: {{ ref('bronze_movies_raw') }}
=============================================================================
TEACHING NOTE — Extracting Embedded Reference Data:

  Production company data is nested inside each movie's JSON:
    "production_companies": [
      {"id": 4, "name": "Paramount Pictures", "origin_country": "US"},
      {"id": 12, "name": "Village Roadshow Pictures", "origin_country": "AU"}
    ]

  We extract these into a DISTINCT reference table.
  Using QUALIFY + ROW_NUMBER deduplicates across many movies that share
  the same company — we keep the most recently seen version of each company.

  Note the QUALIFY clause — this is Snowflake-specific SQL and is much cleaner
  than wrapping ROW_NUMBER in a subquery:
    QUALIFY row_number() OVER (...) = 1
  is equivalent to:
    SELECT * FROM (SELECT ..., ROW_NUMBER() OVER (...) AS rn) WHERE rn = 1
=============================================================================
*/

with bronze as (

    select
        movie_id,
        raw_data,
        ingested_at
    from {{ ref('bronze_movies_raw') }}

    {% if is_incremental() %}
        where ingested_at >= (
            select dateadd(day, -{{ var('incremental_lookback_days') }}, max(ingested_at))
            from {{ this }}
        )
    {% endif %}

),

exploded_companies as (

    select
        company_elem.value:id::number           as company_id,
        trim(company_elem.value:name::varchar)  as company_name,
        upper(
            trim(company_elem.value:origin_country::varchar)
        )                                       as origin_country,
        b.ingested_at
    from bronze b,
        lateral flatten(input => b.raw_data:production_companies) as company_elem

),

deduplicated as (

    /*
    QUALIFY is Snowflake's elegant way to filter on window function results
    without a subquery. It is equivalent to wrapping in an outer SELECT
    but is far more readable.

    We partition by company_id and take the most recently seen record
    in case a company's name or country was updated between pipeline runs.
    */
    select
        company_id,
        company_name,
        nullif(origin_country, '')  as origin_country,
        ingested_at,
        current_timestamp()         as _silver_loaded_at
    from exploded_companies
    where company_id is not null
      and company_name is not null

    qualify row_number() over (
        partition by company_id
        order by     ingested_at desc
    ) = 1

)

select * from deduplicated
