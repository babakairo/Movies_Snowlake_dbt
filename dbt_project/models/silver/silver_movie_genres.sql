{{
    config(
        materialized        = 'incremental',
        schema              = 'SILVER',
        unique_key          = ['movie_id', 'genre_id'],
        incremental_strategy = 'merge',
        tags                = ['silver', 'bridge']
    )
}}

/*
=============================================================================
  MODEL: silver_movie_genres
  LAYER: Silver (Bridge / Junction Table)
  SOURCE: {{ ref('bronze_movies_raw') }}
=============================================================================
TEACHING NOTE — Many-to-Many Relationships:

  A movie can belong to multiple genres (Action + Thriller + Drama).
  A genre can apply to thousands of movies.
  This is a MANY-TO-MANY relationship.

  The standard relational solution is a BRIDGE TABLE (also called a junction
  or associative table) with:
    - movie_id (FK → silver_movies)
    - genre_id (FK → silver_genres)
    - Primary key on (movie_id, genre_id) to prevent duplicates

  This enables queries like:
    "Show me all Action movies" → JOIN fact_movies → silver_movie_genres → WHERE genre_id = 28
    "Show me all genres a movie belongs to" → JOIN on movie_id

  In dimensional modelling (Gold layer), this becomes the bridge between
  fact_movies and dim_genres.

VARIANT ARRAY FLATTENING:
  The TMDb API returns genres as a nested array within each movie:
    {
      "id": 550,
      "title": "Fight Club",
      "genres": [
        {"id": 18, "name": "Drama"},
        {"id": 53, "name": "Thriller"}
      ]
    }

  We flatten raw_data:genres to get one row per movie-genre combination.
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

exploded as (

    /*
    Two-level FLATTEN:
      Level 1: Each movie row has a raw_data VARIANT column
      Level 2: raw_data:genres is itself an array — we flatten it

    The LATERAL FLATTEN gives us one row per genre per movie.
    */
    select
        b.movie_id,
        genre_element.value:id::number      as genre_id,
        genre_element.value:name::varchar   as genre_name_raw,
        b.ingested_at
    from bronze b,
        lateral flatten(input => b.raw_data:genres) as genre_element

),

validated as (

    select
        movie_id,
        genre_id,
        genre_name_raw,
        ingested_at,
        current_timestamp() as _silver_loaded_at
    from exploded
    where movie_id is not null
      and genre_id is not null   -- Skip genres that failed to parse

)

select * from validated
