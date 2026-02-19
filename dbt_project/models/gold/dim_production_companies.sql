{{
    config(
        materialized = 'table',
        schema       = 'GOLD',
        tags         = ['gold', 'dimension']
    )
}}

/*
=============================================================================
  MODEL: dim_production_companies
  LAYER: Gold (Dimension)
  SOURCE: {{ ref('silver_production_companies') }}
=============================================================================
TEACHING NOTE — Enriching Dimensions with Business Knowledge:

  Dimensions are not just "lookup tables" — they carry CONTEXT.
  An analyst asking "Which studio type produces the highest ROI?" needs
  to know that Disney is a "MAJOR STUDIO" and A24 is an "INDIE" studio.

  This classification doesn't exist in TMDb — we add it here.
  This is called "conformed dimension enrichment" — adding meaning to raw IDs.

  The seed file budget_tiers.csv shows another enrichment pattern:
  using CSV files to store business classifications that are too small
  for an API call but too important to hardcode in SQL.

  IMPORTANT: Keep enrichment logic DRY (Don't Repeat Yourself).
  This mapping lives in ONE place. If the business definition of "MAJOR STUDIO"
  changes, you edit this model — not 20 different dashboards.
=============================================================================
*/

with silver_companies as (

    select * from {{ ref('silver_production_companies') }}

),

enriched as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['company_id']) }}  as company_sk,

        -- Natural key
        company_id,

        -- Cleaned name
        company_name,

        -- Country info
        coalesce(origin_country, 'UNKNOWN')                     as origin_country,

        -- Region grouping (useful for "Domestic vs International" analysis)
        case origin_country
            when 'US' then 'NORTH_AMERICA'
            when 'CA' then 'NORTH_AMERICA'
            when 'MX' then 'NORTH_AMERICA'
            when 'GB' then 'EUROPE'
            when 'FR' then 'EUROPE'
            when 'DE' then 'EUROPE'
            when 'IT' then 'EUROPE'
            when 'ES' then 'EUROPE'
            when 'JP' then 'ASIA_PACIFIC'
            when 'CN' then 'ASIA_PACIFIC'
            when 'KR' then 'ASIA_PACIFIC'
            when 'IN' then 'ASIA_PACIFIC'
            when 'AU' then 'ASIA_PACIFIC'
            else           'OTHER'
        end                                                     as region,

        -- Studio tier classification (business knowledge)
        case company_name
            when 'Universal Pictures'       then 'MAJOR_STUDIO'
            when 'Paramount Pictures'       then 'MAJOR_STUDIO'
            when 'Warner Bros. Pictures'    then 'MAJOR_STUDIO'
            when 'Columbia Pictures'        then 'MAJOR_STUDIO'
            when 'Walt Disney Pictures'     then 'MAJOR_STUDIO'
            when '20th Century Fox'         then 'MAJOR_STUDIO'
            when '20th Century Studios'     then 'MAJOR_STUDIO'
            when 'New Line Cinema'          then 'MAJOR_STUDIO'
            when 'Metro-Goldwyn-Mayer'      then 'MAJOR_STUDIO'
            when 'DreamWorks Pictures'      then 'MAJOR_STUDIO'
            when 'Lionsgate'                then 'MINI_MAJOR'
            when 'Focus Features'           then 'MINI_MAJOR'
            when 'Miramax'                  then 'MINI_MAJOR'
            when 'A24'                      then 'INDIE'
            when 'Blumhouse Productions'    then 'INDIE'
            when 'Bad Robot'                then 'INDIE'
            else                                 'UNKNOWN'
        end                                                     as studio_tier,

        _silver_loaded_at,
        current_timestamp()                                     as _gold_loaded_at

    from silver_companies

)

select * from enriched
