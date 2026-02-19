-- =============================================================================
-- FILE: snowflake_setup/02_roles_and_permissions.sql
-- PURPOSE: Role-based access control (RBAC) setup
-- RUN AS: SECURITYADMIN
-- =============================================================================
-- TEACHING NOTE:
--   Security is not optional. The principle of least privilege means:
--   "Give each user only the permissions they need to do their job — nothing more."
--
--   Snowflake uses a role hierarchy:
--     ACCOUNTADMIN  (god mode — use rarely)
--       └── SYSADMIN  (infrastructure management)
--             └── DE_ROLE  (data engineers — read/write all layers)
--                   └── ANALYST_ROLE  (read-only on Gold)
--
--   Best practice: No humans should use ACCOUNTADMIN for daily work.
--   Create named roles, grant roles to users, not permissions directly.
-- =============================================================================

USE ROLE SECURITYADMIN;

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 1: Create Roles
-- ─────────────────────────────────────────────────────────────────────────────

-- Data Engineering role — full pipeline access
CREATE ROLE IF NOT EXISTS DE_ROLE
    COMMENT = 'Data Engineers: can read/write Bronze, Silver, Gold and run dbt';

-- Data Analyst role — read-only on Gold (business users, BI tools)
CREATE ROLE IF NOT EXISTS ANALYST_ROLE
    COMMENT = 'Analysts: read-only access to Gold schema for reporting';

-- Role hierarchy: DE_ROLE inherits nothing special; ANALYST_ROLE is a subset
GRANT ROLE ANALYST_ROLE TO ROLE DE_ROLE;  -- DEs can do everything analysts can

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2: Warehouse Grants
-- ─────────────────────────────────────────────────────────────────────────────

USE ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE LECTURE_INGEST_WH    TO ROLE DE_ROLE;
GRANT USAGE ON WAREHOUSE LECTURE_TRANSFORM_WH TO ROLE DE_ROLE;
GRANT USAGE ON WAREHOUSE LECTURE_TRANSFORM_WH TO ROLE ANALYST_ROLE;

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 3: Database and Schema Grants
-- ─────────────────────────────────────────────────────────────────────────────

-- Database-level USAGE (required before any schema-level grants work)
GRANT USAGE ON DATABASE LECTURE_DE TO ROLE DE_ROLE;
GRANT USAGE ON DATABASE LECTURE_DE TO ROLE ANALYST_ROLE;

-- BRONZE schema — Data Engineers only (no analysts touching raw data)
GRANT USAGE ON SCHEMA LECTURE_DE.BRONZE TO ROLE DE_ROLE;
GRANT CREATE TABLE ON SCHEMA LECTURE_DE.BRONZE TO ROLE DE_ROLE;
GRANT CREATE STAGE ON SCHEMA LECTURE_DE.BRONZE TO ROLE DE_ROLE;
GRANT CREATE FILE FORMAT ON SCHEMA LECTURE_DE.BRONZE TO ROLE DE_ROLE;

-- SILVER schema — Data Engineers read/write
GRANT USAGE ON SCHEMA LECTURE_DE.SILVER TO ROLE DE_ROLE;
GRANT CREATE TABLE ON SCHEMA LECTURE_DE.SILVER TO ROLE DE_ROLE;
GRANT CREATE VIEW  ON SCHEMA LECTURE_DE.SILVER TO ROLE DE_ROLE;

-- GOLD schema — Data Engineers write, Analysts read
GRANT USAGE ON SCHEMA LECTURE_DE.GOLD TO ROLE DE_ROLE;
GRANT USAGE ON SCHEMA LECTURE_DE.GOLD TO ROLE ANALYST_ROLE;
GRANT CREATE TABLE ON SCHEMA LECTURE_DE.GOLD TO ROLE DE_ROLE;
GRANT CREATE VIEW  ON SCHEMA LECTURE_DE.GOLD TO ROLE DE_ROLE;

-- Future grants ensure new tables auto-inherit permissions
-- This is critical! Without this, every new dbt model needs manual grants.
GRANT SELECT ON FUTURE TABLES IN SCHEMA LECTURE_DE.BRONZE TO ROLE DE_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA LECTURE_DE.SILVER TO ROLE DE_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA LECTURE_DE.SILVER TO ROLE ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA LECTURE_DE.GOLD   TO ROLE DE_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA LECTURE_DE.GOLD   TO ROLE ANALYST_ROLE;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA LECTURE_DE.GOLD   TO ROLE DE_ROLE;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA LECTURE_DE.GOLD   TO ROLE ANALYST_ROLE;

-- DBT staging schema
GRANT USAGE  ON SCHEMA LECTURE_DE.DBT_STAGING TO ROLE DE_ROLE;
GRANT CREATE TABLE ON SCHEMA LECTURE_DE.DBT_STAGING TO ROLE DE_ROLE;
GRANT CREATE VIEW  ON SCHEMA LECTURE_DE.DBT_STAGING TO ROLE DE_ROLE;

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 4: Create Users and Assign Roles
-- ─────────────────────────────────────────────────────────────────────────────
-- TEACHING NOTE: Replace placeholder passwords with secure ones.
--   In production, use SSO / key-pair authentication instead of passwords.

USE ROLE SECURITYADMIN;

-- Service account for the Python pipeline (non-human user)
CREATE USER IF NOT EXISTS SVC_PIPELINE
    PASSWORD        = 'ReplaceMe_Pipeline123!'
    DEFAULT_ROLE    = DE_ROLE
    DEFAULT_WAREHOUSE = LECTURE_INGEST_WH
    DEFAULT_NAMESPACE = LECTURE_DE.BRONZE
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Service account for Python ingestion pipeline';

GRANT ROLE DE_ROLE TO USER SVC_PIPELINE;

-- Student / teaching user
CREATE USER IF NOT EXISTS STUDENT_USER
    PASSWORD        = 'ReplaceMe_Student123!'
    DEFAULT_ROLE    = DE_ROLE
    DEFAULT_WAREHOUSE = LECTURE_TRANSFORM_WH
    DEFAULT_NAMESPACE = LECTURE_DE.GOLD
    MUST_CHANGE_PASSWORD = TRUE
    COMMENT = 'Student user for hands-on exercises';

GRANT ROLE DE_ROLE TO USER STUDENT_USER;

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 5: Verify Permissions
-- ─────────────────────────────────────────────────────────────────────────────

SHOW GRANTS TO ROLE DE_ROLE;
SHOW GRANTS TO ROLE ANALYST_ROLE;
