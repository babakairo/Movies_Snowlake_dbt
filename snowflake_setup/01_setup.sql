-- =============================================================================
-- FILE: snowflake_setup/01_setup.sql
-- PURPOSE: One-time Snowflake infrastructure setup
-- RUN AS: SYSADMIN (or ACCOUNTADMIN for warehouse creation)
-- =============================================================================
-- TEACHING NOTE:
--   In production, this file is run once by a DBA or platform team.
--   It creates the "skeleton" that all pipelines and transformations live in.
--   Never mix setup DDL with pipeline logic — keep them separate.
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 1: Compute Resources (Virtual Warehouses)
-- ─────────────────────────────────────────────────────────────────────────────
-- TEACHING NOTE:
--   Snowflake separates storage from compute. A "warehouse" is a cluster of
--   compute nodes you start/stop on demand. You pay per second of usage.
--   Best practice: use separate warehouses for ingestion vs. transformation
--   so they don't compete for resources and you can track costs separately.

USE ROLE SYSADMIN;

-- Warehouse for Python ingestion jobs (XS = 1 node, cheap)
CREATE WAREHOUSE IF NOT EXISTS LECTURE_INGEST_WH
    WAREHOUSE_SIZE   = 'XSMALL'
    AUTO_SUSPEND     = 60          -- suspend after 60 seconds idle (saves cost)
    AUTO_RESUME      = TRUE        -- auto-starts when a query arrives
    INITIALLY_SUSPENDED = TRUE
    COMMENT          = 'Used by Python ingestion pipeline';

-- Warehouse for dbt transformation runs (S = 2 nodes, handles JOINs faster)
CREATE WAREHOUSE IF NOT EXISTS LECTURE_TRANSFORM_WH
    WAREHOUSE_SIZE   = 'SMALL'
    AUTO_SUSPEND     = 120
    AUTO_RESUME      = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT          = 'Used by dbt transformation runs';

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2: Database
-- ─────────────────────────────────────────────────────────────────────────────
-- TEACHING NOTE:
--   One database per project is typical for smaller teams.
--   Larger orgs often have separate databases per domain (e.g., MARKETING, FINANCE).
--   Data retention = 7 days means Snowflake keeps 7 days of time-travel history
--   so you can query data "as of" a past point in time — powerful for debugging.

CREATE DATABASE IF NOT EXISTS LECTURE_DE
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Teaching project: TMDb Movie Data Platform';

USE DATABASE LECTURE_DE;

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 3: Medallion Schemas
-- ─────────────────────────────────────────────────────────────────────────────
-- TEACHING NOTE:
--   Each schema represents a maturity tier of data.
--   Physical separation via schemas gives us:
--     1. Clear access control boundaries (analysts only touch GOLD)
--     2. Visual organization in Snowflake's UI
--     3. Separate billing tracking per schema in future (with resource monitors)

CREATE SCHEMA IF NOT EXISTS LECTURE_DE.BRONZE
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Raw ingestion zone — immutable landing area for source data';

CREATE SCHEMA IF NOT EXISTS LECTURE_DE.SILVER
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Cleansed and typed data — deduplicated, cast to proper types';

CREATE SCHEMA IF NOT EXISTS LECTURE_DE.GOLD
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Business analytics layer — fact/dim model, KPI marts';

-- Also create a STAGING schema for dbt internal use
CREATE SCHEMA IF NOT EXISTS LECTURE_DE.DBT_STAGING
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = 'dbt internal staging schema — ephemeral and temp models';

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 4: Bronze Tables — Raw Landing Zone
-- ─────────────────────────────────────────────────────────────────────────────
-- TEACHING NOTE:
--   Bronze tables use the VARIANT data type — Snowflake's native semi-structured
--   storage. This means we store the raw JSON blob exactly as the API returned it.
--   WHY? Because:
--     1. Schema-on-read: if the API adds a field, we don't break the pipeline
--     2. Auditability: we can always re-derive Silver from Bronze if we find a bug
--     3. Speed: ingestion is fast because we don't parse during load
--   This is the MOST IMPORTANT concept in the Bronze layer.

USE SCHEMA LECTURE_DE.BRONZE;

CREATE TABLE IF NOT EXISTS LECTURE_DE.BRONZE.RAW_MOVIES (
    -- Surrogate key extracted for partitioning/lookups without parsing JSON
    MOVIE_ID        NUMBER          NOT NULL COMMENT 'TMDb movie ID extracted for indexing',
    -- The full API response stored as-is — no parsing, no transformations
    RAW_DATA        VARIANT         NOT NULL COMMENT 'Complete JSON response from TMDb /movie/{id}',
    -- Provenance metadata — always know WHERE data came from
    SOURCE_URL      VARCHAR(500)             COMMENT 'API endpoint URL that produced this record',
    -- Lineage metadata — always know WHEN data arrived
    INGESTED_AT     TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp of ingestion',
    -- Batch tracking — group records from the same pipeline run
    BATCH_ID        VARCHAR(36)              COMMENT 'UUID identifying the pipeline run that loaded this row'
);

CREATE TABLE IF NOT EXISTS LECTURE_DE.BRONZE.RAW_GENRES (
    RAW_DATA        VARIANT         NOT NULL COMMENT 'Genre list JSON from TMDb /genre/movie/list',
    INGESTED_AT     TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    BATCH_ID        VARCHAR(36)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 5: File Format & Stage (optional — for bulk loading via COPY INTO)
-- ─────────────────────────────────────────────────────────────────────────────
-- TEACHING NOTE:
--   Snowflake has two ways to load data:
--     1. INSERT/EXECUTE: small batches, connector-based (what we use here)
--     2. COPY INTO: bulk loading from staged files (S3/GCS/Azure Blob) — faster
--          for millions of rows. We define the stage here so students see it.
--   For learning, INSERT is simpler. In production you'd use COPY INTO with
--   an S3 stage for large volumes.

CREATE OR REPLACE FILE FORMAT LECTURE_DE.BRONZE.JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    ENABLE_OCTAL     = FALSE
    ALLOW_DUPLICATE  = FALSE
    STRIP_NULL_VALUES = FALSE
    IGNORE_UTF8_ERRORS = FALSE
    COMMENT = 'Standard JSON file format for loading raw API responses';

-- Internal stage (stored in Snowflake, no external cloud bucket needed)
CREATE STAGE IF NOT EXISTS LECTURE_DE.BRONZE.TMDB_STAGE
    FILE_FORMAT = LECTURE_DE.BRONZE.JSON_FORMAT
    COMMENT     = 'Internal stage for loading TMDb JSON files via COPY INTO';

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 6: Verify Setup
-- ─────────────────────────────────────────────────────────────────────────────

SHOW DATABASES LIKE 'LECTURE_DE';
SHOW SCHEMAS IN DATABASE LECTURE_DE;
SHOW WAREHOUSES LIKE 'LECTURE%';
SHOW TABLES IN SCHEMA LECTURE_DE.BRONZE;
