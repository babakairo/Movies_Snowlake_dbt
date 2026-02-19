{{
    config(
        materialized        = 'incremental',
        schema              = 'SILVER',
        unique_key          = 'movie_id',
        incremental_strategy = 'merge',
        tags                = ['silver', 'cleansed']
    )
}}

/*
=============================================================================
  MODEL: silver_movies
  LAYER: Silver (Cleansed & Typed)
  SOURCE: {{ ref('bronze_movies_raw') }}
=============================================================================
TEACHING NOTE — The Silver Layer's Job:

  Silver transforms raw VARIANT data into proper relational columns.
  It is the "typing, cleaning, and conforming" layer.

  Key Silver operations demonstrated here:
    1. VARIANT PARSING: Use the :: operator to extract JSON fields
       Syntax: raw_data:field_name::TARGET_TYPE
       Example: raw_data:budget::NUMBER    extracts "budget" as a number

    2. NULL HANDLING:
       - NULLIF(value, 0) converts zeros to NULL when 0 means "unknown"
       - COALESCE(value, default) provides fallback values
       - TRY_TO_DATE() safely parses date strings — returns NULL on failure
         instead of crashing (safer than TO_DATE())

    3. DEDUPLICATION (incremental):
       In incremental mode, dbt MERGEs on unique_key = 'movie_id'.
       Only rows newer than the last run are processed.

    4. CLEANING:
       - TRIM() removes whitespace from strings
       - Standardise empty strings to NULL
       - Derive computed columns (release_year, is_profitable, etc.)

COMMON MISTAKES:
  ❌ Doing aggregations in Silver (that's Gold's job)
  ❌ Creating business logic in Silver (revenue quartile = Gold)
  ✓ One-to-one row mapping from Bronze (1 Bronze row → 1 Silver row)
  ✓ Type casting and null handling
  ✓ Derived columns that are direct functions of source fields
=============================================================================
*/

with source as (

    select * from {{ ref('bronze_movies_raw') }}

    /*
    INCREMENTAL LOGIC:
    In incremental mode, dbt automatically wraps this in:
        WHERE ingested_at > (SELECT MAX(ingested_at) FROM silver_movies)

    This processes only NEW Bronze rows on each dbt run, avoiding
    a full table scan every time the pipeline runs.

    We add a buffer of {{ var('incremental_lookback_days') }} days to handle
    late-arriving records (records that land in Bronze after the Silver run).
    */
    {% if is_incremental() %}
        where ingested_at >= (
            select dateadd(day, -{{ var('incremental_lookback_days') }}, max(_source_ingested_at))
            from {{ this }}
        )
    {% endif %}

),

parsed as (

    /*
    ─────────────────────────────────────────────────────────────────────────
    VARIANT PARSING — Core Silver skill

    Snowflake VARIANT syntax:
      raw_data:key              → extracts the value (still VARIANT type)
      raw_data:key::VARCHAR     → extracts and casts to VARCHAR
      raw_data:key::NUMBER      → extracts and casts to NUMBER
      raw_data:nested:sub_key   → navigate nested JSON
      raw_data['key']           → alternative bracket notation for special chars

    TRY_TO_DATE vs TO_DATE:
      TRY_TO_DATE returns NULL on invalid dates (safe for dirty data)
      TO_DATE raises an error and fails the entire query (unsafe for raw data)
    ─────────────────────────────────────────────────────────────────────────
    */
    select
        -- Primary key
        raw_data:id::number                                     as movie_id,

        -- Core text fields — TRIM removes leading/trailing whitespace
        trim(raw_data:title::varchar)                           as title,
        trim(raw_data:original_title::varchar)                  as original_title,
        trim(raw_data:tagline::varchar)                         as tagline,
        trim(raw_data:overview::varchar)                        as overview,
        trim(raw_data:status::varchar)                          as status,
        trim(raw_data:homepage::varchar)                        as homepage,
        trim(raw_data:imdb_id::varchar)                         as imdb_id,
        trim(raw_data:original_language::varchar)               as original_language,

        -- Date fields — safe parsing with TRY_TO_DATE
        try_to_date(raw_data:release_date::varchar, 'YYYY-MM-DD') as release_date,

        -- Derived date parts — pre-computed for analytical convenience
        -- Rather than making every analyst call YEAR(release_date)
        year(try_to_date(raw_data:release_date::varchar, 'YYYY-MM-DD'))     as release_year,
        month(try_to_date(raw_data:release_date::varchar, 'YYYY-MM-DD'))    as release_month,
        quarter(try_to_date(raw_data:release_date::varchar, 'YYYY-MM-DD'))  as release_quarter,

        -- Seasonal classification — pre-computed so fact_movies can inherit it
        case month(try_to_date(raw_data:release_date::varchar, 'YYYY-MM-DD'))
            when 6  then 'SUMMER' when 7  then 'SUMMER' when 8  then 'SUMMER'
            when 11 then 'HOLIDAY' when 12 then 'HOLIDAY'
            when 1  then 'WINTER' when 2  then 'WINTER' when 3  then 'WINTER'
            when 4  then 'SPRING' when 5  then 'SPRING'
            when 9  then 'FALL'   when 10 then 'FALL'
        end                                                             as release_season,

        -- Numeric fields — NULLIF(value, 0) treats 0 as "unknown"
        -- TMDb uses 0 to mean "not reported" for budget and revenue
        nullif(raw_data:budget::number, 0)                      as budget_usd,
        nullif(raw_data:revenue::number, 0)                     as revenue_usd,
        nullif(raw_data:runtime::number, 0)                     as runtime_minutes,

        -- Float metrics from the TMDb community
        raw_data:popularity::float                              as popularity_score,
        raw_data:vote_average::float                            as vote_average,
        nullif(raw_data:vote_count::number, 0)                  as vote_count,

        -- Boolean flag — stored as 0/1 in JSON, cast to proper BOOLEAN
        raw_data:adult::boolean                                 as is_adult_content,

        -- Computed business metrics (based purely on source fields → OK for Silver)
        case
            when nullif(raw_data:budget::number, 0) is not null
             and nullif(raw_data:revenue::number, 0) is not null
            then raw_data:revenue::number - raw_data:budget::number
            else null
        end                                                     as profit_usd,

        case
            when nullif(raw_data:budget::number, 0) > 0
             and nullif(raw_data:revenue::number, 0) is not null
            then round(
                (raw_data:revenue::number - raw_data:budget::number)
                / raw_data:budget::number * 100,
                2
            )
            else null
        end                                                     as roi_pct,

        -- Data lineage columns (always include these in Silver)
        ingested_at                                             as _source_ingested_at,
        batch_id                                                as _source_batch_id,
        current_timestamp()                                     as _silver_loaded_at,
        'TMDb API v3'                                           as _data_source

    from source

),

cleaned as (

    /*
    Final cleaning pass — standardise NULLs and remove clearly invalid rows.

    TEACHING NOTE:
        Data quality rules belong here:
          - Revenue of $0 might be valid (free movies) or unknown — we treat it as unknown
          - Movies with no release date are kept (future releases have no date yet)
          - Empty string titles are set to NULL (they failed upstream)
    */
    select
        movie_id,
        nullif(title, '')               as title,
        nullif(original_title, '')      as original_title,
        nullif(tagline, '')             as tagline,
        nullif(overview, '')            as overview,
        nullif(status, '')              as status,
        nullif(homepage, '')            as homepage,
        nullif(imdb_id, '')             as imdb_id,
        nullif(original_language, '')   as original_language,
        release_date,
        release_year,
        release_month,
        release_quarter,
        release_season,
        budget_usd,
        revenue_usd,
        runtime_minutes,
        popularity_score,
        vote_average,
        vote_count,
        is_adult_content,
        profit_usd,
        roi_pct,
        _source_ingested_at,
        _source_batch_id,
        _silver_loaded_at,
        _data_source

    from parsed
    where movie_id is not null   -- Drop malformed records with no ID

)

select * from cleaned
