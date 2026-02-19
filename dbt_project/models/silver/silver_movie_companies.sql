{{
    config(
        materialized        = 'incremental',
        schema              = 'SILVER',
        unique_key          = ['movie_id', 'company_id'],
        incremental_strategy = 'merge',
        tags                = ['silver', 'bridge']
    )
}}

/*
  Bridge table: one row per movie-company association.
  Preserves company_position (0-based array index) so fact_movies
  can identify the primary (first-listed) production company
  without reaching back into the Bronze layer.
*/

with bronze as (

    select movie_id, raw_data, ingested_at
    from {{ ref('bronze_movies_raw') }}

    {% if is_incremental() %}
        where ingested_at >= (
            select dateadd(day, -{{ var('incremental_lookback_days') }}, max(ingested_at))
            from {{ this }}
        )
    {% endif %}

),

exploded as (

    select
        b.movie_id,
        company_elem.value:id::number           as company_id,
        trim(company_elem.value:name::varchar)  as company_name,
        company_elem.index::number              as company_position,
        b.ingested_at
    from bronze b,
        lateral flatten(input => b.raw_data:production_companies) as company_elem

),

validated as (

    select
        movie_id,
        company_id,
        company_name,
        company_position,
        case when company_position = 0 then true else false end  as is_primary_company,
        ingested_at,
        current_timestamp()                                      as _silver_loaded_at
    from exploded
    where movie_id   is not null
      and company_id is not null

)

select * from validated
