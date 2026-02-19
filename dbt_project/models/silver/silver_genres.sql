{{
    config(
        materialized = 'table',
        schema       = 'SILVER',
        tags         = ['silver', 'reference']
    )
}}

/*
=============================================================================
  MODEL: silver_genres
  LAYER: Silver (Reference / Dimension)
  SOURCE: {{ source('raw_ingestion', 'RAW_GENRES') }}
=============================================================================
TEACHING NOTE — Flattening Arrays from VARIANT:

  The TMDb genre list comes as a JSON array:
    [{"id": 28, "name": "Action"}, {"id": 12, "name": "Adventure"}, ...]

  In Snowflake, we use the FLATTEN() table function to "explode" a JSON array
  into individual rows — one row per array element.

  Syntax:
    LATERAL FLATTEN(input => column_name)
    or
    TABLE(FLATTEN(input => column_name))

  The FLATTEN output gives you:
    VALUE   → the array element as a VARIANT object
    INDEX   → the position in the array (0, 1, 2, ...)
    SEQ     → sequence number of the source row
    KEY     → null for arrays (only populated for objects)
    PATH    → path within the document

  This is one of Snowflake's most powerful features for semi-structured data.
=============================================================================
*/

with raw_genres as (

    select
        raw_data,
        ingested_at,
        batch_id,
        -- Keep only the most recent genre list per batch
        row_number() over (order by ingested_at desc) as recency_rank
    from {{ source('raw_ingestion', 'RAW_GENRES') }}

),

latest_genre_batch as (

    -- Genres don't change often — take the most recent snapshot
    select raw_data
    from raw_genres
    where recency_rank = 1

),

flattened as (

    /*
    FLATTEN explodes the JSON array into individual rows.

    OUTER => TRUE means "include rows even if the array is empty"
    (like a LEFT JOIN — prevents data loss on edge cases)

    We use LATERAL FLATTEN which is the SQL standard way to call table functions.
    */
    select
        genre_element.value:id::number    as genre_id,
        genre_element.value:name::varchar as genre_name
    from latest_genre_batch,
        lateral flatten(input => raw_data) as genre_element

)

select
    genre_id,
    upper(trim(genre_name)) as genre_name,    -- Standardise casing
    current_timestamp()     as _silver_loaded_at
from flattened
where genre_id is not null
order by genre_name
