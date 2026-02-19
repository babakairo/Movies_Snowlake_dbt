{{
    config(
        materialized = 'table',
        schema       = 'GOLD',
        tags         = ['gold', 'dimension']
    )
}}

/*
=============================================================================
  MODEL: dim_genres
  LAYER: Gold (Dimension)
  SOURCE: {{ ref('silver_genres') }}
=============================================================================
TEACHING NOTE — Surrogate Keys vs Natural Keys:

  Natural key: the original ID from the source system (TMDb's genre_id = 28)
  Surrogate key: a new integer ID we generate in our warehouse (genre_sk)

  WHY create a surrogate key if we already have genre_id?
    1. Decoupling: if TMDb changes their IDs, our warehouse is unaffected
       (we never change genre_sk — it's stable forever)
    2. Performance: integer joins are faster than string/UUID joins
    3. Convention: star schema best practice — every dimension has a surrogate key
    4. History: if we add SCD Type 2 (slowly changing dimensions) later,
       the natural key may repeat across versions — surrogate key stays unique

  We generate surrogate keys using {{ dbt_utils.generate_surrogate_key() }}
  which hashes the natural key(s) into a deterministic integer.
  Same input always produces the same output — idempotent.

  ENRICHMENT:
  We also add attributes not in the source:
    - genre_group: our business categorisation of genres
    - is_premium_genre: genres that historically command higher box office
  These enrich the dimension with "business knowledge" beyond the raw source.
=============================================================================
*/

with silver_genres as (

    select * from {{ ref('silver_genres') }}

),

enriched as (

    select
        -- Surrogate key — stable, system-generated identifier
        {{ dbt_utils.generate_surrogate_key(['genre_id']) }}   as genre_sk,

        -- Natural key — preserved for joining back to source systems
        genre_id,

        -- Business-friendly name (already UPPER CASE from Silver)
        genre_name,

        -- Business enrichment: our own genre grouping
        -- This is "business knowledge" that TMDb doesn't provide
        case genre_name
            when 'ACTION'          then 'HIGH_OCTANE'
            when 'ADVENTURE'       then 'HIGH_OCTANE'
            when 'THRILLER'        then 'HIGH_OCTANE'
            when 'DRAMA'           then 'NARRATIVE'
            when 'ROMANCE'         then 'NARRATIVE'
            when 'HISTORY'         then 'NARRATIVE'
            when 'COMEDY'          then 'LIGHT'
            when 'FAMILY'          then 'LIGHT'
            when 'ANIMATION'       then 'LIGHT'
            when 'HORROR'          then 'NICHE'
            when 'SCIENCE FICTION' then 'NICHE'
            when 'FANTASY'         then 'NICHE'
            when 'MYSTERY'         then 'NICHE'
            when 'CRIME'           then 'NICHE'
            when 'DOCUMENTARY'     then 'ARTS'
            when 'MUSIC'           then 'ARTS'
            when 'WESTERN'         then 'ARTS'
            else                        'OTHER'
        end                                                    as genre_group,

        -- Flag: genres that historically appear in high-budget blockbusters
        case genre_name
            when 'ACTION'          then true
            when 'ADVENTURE'       then true
            when 'SCIENCE FICTION' then true
            when 'FANTASY'         then true
            when 'ANIMATION'       then true
            else                        false
        end                                                    as is_blockbuster_genre,

        _silver_loaded_at,
        current_timestamp()                                    as _gold_loaded_at

    from silver_genres

)

select * from enriched
