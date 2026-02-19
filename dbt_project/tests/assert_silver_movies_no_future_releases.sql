/*
=============================================================================
  CUSTOM TEST: assert_silver_movies_no_future_releases
=============================================================================
TEACHING NOTE — Temporal Data Quality:

  This test checks: "No movie should have a release date more than 2 years
  in the future."

  Why 2 years? Studios announce movies years in advance.
  A release date 1-2 years away is legitimate. But 10 years? That's a bug.

  This catches common ingestion bugs:
    - Wrong year format parsed (2-digit year → 2099 instead of 1999)
    - Epoch timestamp division error (milliseconds vs seconds)
    - Test data accidentally loaded to production

  CURRENT_DATE() in Snowflake returns today's date in the session timezone.
  DATEADD() adds an interval to a date.
=============================================================================
*/

select
    movie_id,
    title,
    release_date,
    datediff(day, current_date(), release_date) as days_in_future
from {{ ref('silver_movies') }}
where release_date > dateadd(year, 2, current_date())
  and release_date is not null
