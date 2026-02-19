{{
    config(
        materialized = 'table',
        schema       = 'BRONZE',
        tags         = ['bronze', 'raw']
    )
}}

/*
=============================================================================
  MODEL: bronze_movies_raw
  LAYER: Bronze (Raw Ingestion Zone)
  SOURCE: {{ source('raw_ingestion', 'RAW_MOVIES') }}
=============================================================================
TEACHING NOTE — Why does the Bronze dbt model exist if data is already loaded?

  Great question! The Bronze dbt model serves several important purposes:

  1. SINGLE SOURCE OF TRUTH FOR LINEAGE:
     By pointing ref('bronze_movies_raw') from Silver models,
     dbt knows the full lineage: Source → Bronze → Silver → Gold.
     Without this, Silver would ref a source(), breaking the DAG.

  2. LIGHT ENRICHMENT (not transformation):
     We add derived columns like `ingestion_date` and `json_size_bytes`
     that are useful for data quality monitoring and partitioning.
     These do not change the data — they're operational metadata.

  3. DEDUPLICATION AT THE GATE:
     If the pipeline runs twice with the same data, the source table may
     have duplicate MOVIE_IDs from different batches. We keep only the
     LATEST version of each movie here, so Silver never sees duplicates.

  4. STANDARDISATION:
     Ensure column names are consistent CASING and naming conventions.

COMMON MISTAKES (teach these explicitly):
  ❌ Transforming/cleaning in Bronze — Bronze is immutable
  ❌ JOINing in Bronze — Bronze models have a single source
  ❌ Casting to target types in Bronze — that's Silver's job
  ✓ Only add operational metadata columns (ingestion_date, row_hash, etc.)
=============================================================================
*/

with source as (

    /*
    Reference the source table registered in sources.yml.
    dbt tracks this as a dependency edge in the DAG.
    */
    select * from {{ source('raw_ingestion', 'RAW_MOVIES') }}

),

deduplicated as (

    /*
    TEACHING NOTE — Deduplication using window functions:

    ROW_NUMBER() OVER (PARTITION BY movie_id ORDER BY ingested_at DESC)
    assigns rank 1 to the MOST RECENT version of each movie_id.

    We keep rank = 1 to ensure downstream Silver models always see
    exactly one row per movie — the freshest one.

    This is the "keep latest" deduplication strategy.
    Alternative: "keep first" — use ASC instead of DESC.
    */
    select
        movie_id,
        raw_data,
        source_url,
        ingested_at,
        batch_id,

        -- Operational metadata: extract date for partitioning
        ingested_at::date                                     as ingestion_date,

        -- Unique row hash — useful for change detection in incremental loads
        md5(raw_data::varchar)                                as row_hash,

        -- Assign recency rank: 1 = most recently ingested version
        row_number() over (
            partition by movie_id
            order by     ingested_at desc
        )                                                     as recency_rank

    from source

),

final as (

    select
        movie_id,
        raw_data,
        source_url,
        ingested_at,
        ingestion_date,
        batch_id,
        row_hash
    from deduplicated
    where recency_rank = 1   -- Keep only the most recent version

)

select * from final
