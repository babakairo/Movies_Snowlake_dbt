/*
=============================================================================
  CUSTOM TEST: assert_revenue_not_negative
=============================================================================
TEACHING NOTE — Custom dbt Tests vs Schema Tests:

  dbt has two types of tests:
    1. Schema tests (defined in schema.yml): not_null, unique, accepted_values, relationships
       These cover ~80% of common data quality checks.

    2. Custom SQL tests (stored in tests/): arbitrary SQL queries
       These cover business-specific rules that schema tests can't express.

  A custom test FAILS if the query returns ANY rows.
  A custom test PASSES if the query returns ZERO rows.

  Think of it as: "Find me any data that violates this rule."
  If you find violations, the test fails. No violations = test passes.

  This test checks: "No movie should have negative revenue."
  Logic: revenue can be NULL (not reported), but never negative.
         A negative revenue value indicates a data bug in ingestion.

  Run tests with:
    dbt test                        # Run all tests
    dbt test --select fact_movies   # Run tests for one model
    dbt test --store-failures       # Save failing rows to a table for inspection
=============================================================================
*/

-- This query returns rows that VIOLATE the rule.
-- dbt test FAILS if any rows are returned.
select
    movie_id,
    title,
    revenue_usd,
    budget_usd,
    _source_ingested_at
from {{ ref('fact_movies') }}
where revenue_usd < 0   -- Revenue should never be negative
