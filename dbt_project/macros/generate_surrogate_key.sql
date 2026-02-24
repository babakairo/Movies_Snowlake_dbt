
{#
=============================================================================
  MACRO: generate_surrogate_key
  FILE:  macros/generate_surrogate_key.sql
=============================================================================
TEACHING NOTE — What is a Surrogate Key?

  A SURROGATE KEY is a system-generated, meaningless identifier used as the
  primary key in a dimension table. It has NO business meaning.

  WHY NOT USE NATURAL KEYS (like movie_id, genre_id)?
  ────────────────────────────────────────────────────
  Problem 1: Source IDs change
    - TMDb might renumber movies if you switch API versions
    - Your genre_id=28 (Action) might become 100 in a new API version
    - Every join across your warehouse breaks instantly

  Problem 2: Composite natural keys are messy in fact tables
    - "movie-genre" bridge has 2 NKs (movie_id + genre_id)
    - A fact table FK must also be 2 columns → slow joins, messy schema

  Problem 3: No history for SCD2 (Slowly Changing Dimensions)
    - When a genre name changes, you want a NEW SK for the new version
    - The natural key (genre_id) stays the same — it can't distinguish history

  SURROGATE KEY SOLUTION:
  ────────────────────────────────────────────────────
  - Always a single column integer or hash
  - Generated deterministically from natural key(s)
  - Stable: same inputs → same SK forever
  - Fast joins: integer/hash is cheaper than multi-column NKs

HOW THIS MACRO WORKS:
────────────────────────────────────────────────────
  Uses MD5 hashing (128-bit) to generate a consistent 32-char hex string.

  Inputs:   ['genre_id']           → genre_id = 28
  Process:  MD5(CAST(28 AS TEXT))  → 'a87ff679a2f3e71d9181a67b7542122c'
  Output:   'a87ff679a2f3e71d9181a67b7542122c'  (genre_sk)

  Multi-column example:
  Inputs:   ['movie_id', 'genre_id']
  Process:  MD5(CONCAT(movie_id, '-', genre_id))
  Output:   one stable hash for the combination

  This is exactly what dbt_utils.generate_surrogate_key() does internally.
  We wrap it here to add this documentation and a consistent project standard.

USAGE:
────────────────────────────────────────────────────
  Single column:
    {{ generate_surrogate_key(['genre_id']) }}

  Multi-column (composite key):
    {{ generate_surrogate_key(['movie_id', 'genre_id']) }}

  In a SELECT:
    {{ generate_surrogate_key(['genre_id']) }} as genre_sk,

STANDARD NAMING CONVENTION IN THIS PROJECT:
────────────────────────────────────────────────────
  dim_genres         → genre_sk
  dim_production_companies → company_sk
  fact_movies        → movie_sk
  dim_dates          → date_sk  (integer YYYYMMDD — NOT a hash, special case)

=============================================================================
#}

{% macro generate_surrogate_key(field_list) %}

    {# -------------------------------------------------------------------
       STEP 1: Validate input — must be a list of at least one column
    ------------------------------------------------------------------- #}
    {%- if field_list is not iterable or field_list is string -%}
        {{ exceptions.raise_compiler_error(
            "generate_surrogate_key() expects a list of column names. "
            ~ "Got: " ~ field_list
        ) }}
    {%- endif -%}

    {# -------------------------------------------------------------------
       STEP 2: Delegate to dbt_utils implementation
       dbt_utils handles:
         - NULL coalescing (NULLs become empty string before hashing)
         - Type casting (all columns cast to VARCHAR before concat)
         - Separator (uses '-' between columns)
         - Hash function (MD5 → 32-char hex string)

       TEACHING: We call dbt_utils here so we don't duplicate their logic.
       This is the "composition over reimplementation" pattern.
    ------------------------------------------------------------------- #}
    {{ dbt_utils.generate_surrogate_key(field_list) }}

{% endmacro %}


{#
=============================================================================
  MACRO: generate_date_sk
=============================================================================
TEACHING NOTE — Why a Special Date Surrogate Key?

  For date dimensions, we use an INTEGER in YYYYMMDD format rather than
  a hash. Example: July 15, 2023 → 20230715

  Why integer instead of MD5 hash?
    1. Readable: you can scan dim_dates and understand the key instantly
    2. Sortable: ORDER BY date_sk = ORDER BY date (same as ORDER BY full_date)
    3. Range queries: WHERE date_sk BETWEEN 20200101 AND 20221231
    4. No join needed for "rough" date filters (though you should join properly)

  This is a well-known industry exception to the "SK is meaningless" rule.
  Data warehousing practitioners universally use YYYYMMDD integer for date_sk.

USAGE:
  {{ generate_date_sk('release_date') }}   → 20231107 (for 2023-11-07)
  {{ generate_date_sk('full_date') }}      → used in dim_dates itself

=============================================================================
#}

{% macro generate_date_sk(date_column) %}

    {# -------------------------------------------------------------------
       Convert date to integer YYYYMMDD format
       TO_NUMBER removes decimal, TO_CHAR formats the date string
    ------------------------------------------------------------------- #}
    to_number(to_char({{ date_column }}, 'YYYYMMDD'))

{% endmacro %}
