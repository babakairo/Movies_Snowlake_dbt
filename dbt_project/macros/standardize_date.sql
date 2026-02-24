
{#
=============================================================================
  MACRO: standardize_date.sql
  FILE:  macros/standardize_date.sql
=============================================================================
TEACHING NOTE — Why Macro-ize Date Logic?

  Date parsing appears in EVERY layer of the pipeline:
    - Bronze → Silver:  parsing raw_data:release_date::varchar
    - Silver → Gold:    joining on release_date to dim_dates
    - Gold → Marts:     filtering by release_year ranges

  Without a macro, every model has its own slightly different TRY_TO_DATE()
  call. When the date format changes (e.g., TMDb switches from YYYY-MM-DD
  to MM/DD/YYYY), you'd need to find and fix EVERY occurrence.

  With a macro, you fix ONE place → all models update automatically.

  This is the DRY principle: Don't Repeat Yourself.

DATE PARSING CHALLENGES IN REAL DATA:
  1. Mixed formats:  '2023-07-15' vs '07/15/2023' vs '20230715'
  2. Timezone offsets: '2023-07-15T00:00:00+00:00' (ISO 8601)
  3. Invalid dates: '0000-00-00', '9999-12-31', '2023-02-30'
  4. NULL vs empty string: '' vs NULL behave differently in Snowflake
  5. Future dates: release dates set to 2099 as placeholders

SNOWFLAKE DATE FUNCTIONS:
  TRY_TO_DATE()   → returns NULL on failure (safe, won't fail your pipeline)
  TO_DATE()       → throws ERROR on failure (strict, good for validation)
  TRY_TO_DATE() is almost always the right choice in ETL pipelines.

=============================================================================
#}


{#
-----------------------------------------------------------------------------
  safe_to_date(value, fmt)
  → Safely parse any string or timestamp to a DATE
  → Returns NULL on failure (never raises an error)
-----------------------------------------------------------------------------
PARAMETERS:
  value  — the column or expression to parse (string, timestamp, or date)
  fmt    — optional Snowflake date format string (default: auto-detect)

EXAMPLES:
  {{ safe_to_date('raw_data:release_date::varchar') }}
  {{ safe_to_date('ingested_at', 'YYYY-MM-DD"T"HH24:MI:SS') }}
  {{ safe_to_date("'2023-07-15'") }}

TEACHING: TRY_TO_DATE vs TRY_CAST vs CAST
  TRY_TO_DATE('2023-07-15')  → DATE 2023-07-15
  TRY_TO_DATE('not-a-date')  → NULL     ← safe
  TO_DATE('not-a-date')      → ERROR    ← strict
  CAST('2023-07-15' as date) → same as TO_DATE — also strict
#}

{% macro safe_to_date(value, fmt=none) %}

    {%- if fmt is not none -%}
        {# User specified an explicit format string — use it for strict parsing #}
        try_to_date(cast({{ value }} as varchar), '{{ fmt }}')
    {%- else -%}
        {# No format specified — let Snowflake auto-detect (handles most ISO 8601 formats) #}
        try_to_date(cast({{ value }} as varchar))
    {%- endif -%}

{% endmacro %}


{#
-----------------------------------------------------------------------------
  clean_date(value, fmt)
  → Like safe_to_date, but also handles the common '0001-01-01' placeholder
    that APIs use for "unknown date", returning NULL instead.
-----------------------------------------------------------------------------
TEACHING NOTE: Data Cleaning vs Data Validation
  Cleaning:    Transform bad values into something useful (NULL, default)
  Validation:  Flag or reject bad values in tests

  clean_date does CLEANING — converts sentinel values to NULL.
  The schema.yml dbt_expectations tests do VALIDATION — fail the pipeline
  if release_year is outside a valid range.

EXAMPLES:
  {{ clean_date('raw_data:release_date::varchar') }}
  → '2023-07-15' → DATE 2023-07-15
  → '0001-01-01' → NULL
  → ''           → NULL
  → NULL         → NULL
#}

{% macro clean_date(value, fmt=none) %}

    nullif(
        {{ safe_to_date(value, fmt) }},
        '0001-01-01'::date   -- Common API sentinel for "unknown date"
    )

{% endmacro %}


{#
-----------------------------------------------------------------------------
  extract_date_parts(date_col)
  → Returns a block of derived date columns — call in a SELECT statement
  → Produces: _year, _month, _quarter, _day, _day_of_week columns

  USAGE (in a model):
    {{ extract_date_parts('release_date') }}

  PRODUCES:
    year(release_date)          as release_date_year,
    month(release_date)         as release_date_month,
    quarter(release_date)       as release_date_quarter,
    day(release_date)           as release_date_day,
    dayofweek(release_date)     as release_date_dow,
    dayname(release_date)       as release_date_day_name,
    to_char(release_date, 'YYYY-MM')  as release_date_month_label

TEACHING: Caller provides the column name as a string.
  The macro concatenates SQL strings to produce the correct SELECT expressions.
#}

{% macro extract_date_parts(date_col, prefix=none) %}

    {%- set col_prefix = prefix if prefix is not none else date_col -%}

    year({{ date_col }})                               as {{ col_prefix }}_year,
    month({{ date_col }})                              as {{ col_prefix }}_month,
    quarter({{ date_col }})                            as {{ col_prefix }}_quarter,
    day({{ date_col }})                                as {{ col_prefix }}_day,
    dayofweek({{ date_col }})                          as {{ col_prefix }}_dow,
    dayname({{ date_col }})                            as {{ col_prefix }}_day_name,
    to_char({{ date_col }}, 'YYYY-MM')                 as {{ col_prefix }}_month_label

{% endmacro %}


{#
-----------------------------------------------------------------------------
  release_season(month_col)
  → Map a month number to a seasonal release window label.
  → Same logic used in dim_dates and silver_movies, centralized here.

  USAGE:
    {{ release_season('release_month') }}   as release_season,

  PRODUCES:
    case
        when release_month in (6, 7, 8)   then 'SUMMER'
        when release_month in (11, 12)    then 'HOLIDAY'
        when release_month in (1, 2, 3)   then 'WINTER'
        when release_month in (4, 5)      then 'SPRING'
        when release_month in (9, 10)     then 'FALL'
        else null
    end

TEACHING NOTE: Why seasons matter for movie analytics?
  Hollywood releases blockbusters in summer (Jun-Aug) and holiday season
  (Nov-Dec) because those are peak moviegoing periods (school breaks, holidays).
  Categorizing by season lets analysts confirm or deny this assumption.
#}

{% macro release_season(month_col) %}

    case
        when {{ month_col }} in (6, 7, 8)   then 'SUMMER'
        when {{ month_col }} in (11, 12)     then 'HOLIDAY'
        when {{ month_col }} in (1, 2, 3)    then 'WINTER'
        when {{ month_col }} in (4, 5)       then 'SPRING'
        when {{ month_col }} in (9, 10)      then 'FALL'
        else null
    end

{% endmacro %}


{#
-----------------------------------------------------------------------------
  fiscal_year(date_col, fiscal_start_month)
  → Compute the fiscal year for a date given a fiscal year start month.
  → Default: fiscal year starts in October (common in media/entertainment).

  USAGE:
    {{ fiscal_year('release_date') }}              as fiscal_year,   -- Oct start
    {{ fiscal_year('release_date', 7) }}           as fiscal_year,   -- Jul start
    {{ fiscal_year('release_date', 1) }}           as fiscal_year,   -- Jan = calendar year

  TEACHING: Fiscal years shift the calendar year boundary.
    A fiscal year starting Oct 1:
      Oct 2022 → Sep 2023 = Fiscal Year 2023
    Use case: Studio annual reports often use Oct–Sep fiscal years.
#}

{% macro fiscal_year(date_col, fiscal_start_month=10) %}

    {%- if fiscal_start_month == 1 -%}
        {# Fiscal year same as calendar year — simple case #}
        year({{ date_col }})
    {%- else -%}
        {# Shift calendar months so fiscal year boundaries align #}
        case
            when month({{ date_col }}) >= {{ fiscal_start_month }}
                then year({{ date_col }}) + 1
            else
                year({{ date_col }})
        end
    {%- endif -%}

{% endmacro %}
