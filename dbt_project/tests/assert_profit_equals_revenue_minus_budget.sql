/*
=============================================================================
  CUSTOM TEST: assert_profit_equals_revenue_minus_budget
=============================================================================
TEACHING NOTE — Testing Derived Metric Consistency:

  This test validates a BUSINESS RULE:
    profit_usd = revenue_usd - budget_usd

  Why test this? We compute profit_usd in silver_movies.
  If the formula was ever changed incorrectly (e.g., +  instead of -),
  this test would catch it immediately.

  The ROUND(..., -2) handles floating point rounding errors that appear
  when Snowflake computes differences in large USD amounts.

  TOLERANCE: We allow ±$100 discrepancy to handle floating point precision.
  In production financial systems, you'd tighten this to ±$0.01.
=============================================================================
*/

select
    movie_id,
    title,
    budget_usd,
    revenue_usd,
    profit_usd,
    (revenue_usd - budget_usd)                      as expected_profit,
    abs(profit_usd - (revenue_usd - budget_usd))    as discrepancy_usd
from {{ ref('fact_movies') }}
where budget_usd  is not null
  and revenue_usd is not null
  and profit_usd  is not null
  -- Allow ±$100 tolerance for floating point
  and abs(profit_usd - (revenue_usd - budget_usd)) > 100
