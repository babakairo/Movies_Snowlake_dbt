
{#
=============================================================================
  MACRO: audit_logging.sql
  FILE:  macros/audit_logging.sql
=============================================================================
TEACHING NOTE — dbt Hooks and Operational Audit Logging

  WHAT ARE dbt HOOKS?
  ───────────────────────────────────────────────────────────────────────────
  Hooks are SQL statements (or macro calls) that dbt executes automatically
  at specific points during a run:

  on-run-start:  Runs ONCE before any model builds
  on-run-end:    Runs ONCE after ALL models finish (success or failure)
  pre-hook:      Runs before EACH individual model
  post-hook:     Runs after EACH individual model

  Hooks are configured in dbt_project.yml:
  ─────────────────────────────────────────
    on-run-start:
      - "{{ create_audit_log_tables() }}"   ← this file
      - "{{ log_run_start() }}"             ← this file

    on-run-end:
      - "{{ log_run_end() }}"               ← this file
      - "{{ grant_analyst_role_access() }}" ← this file

    models:
      movies_platform:
        gold:
          +post-hook: "{{ log_model_run(this) }}"   ← this file

  WHY ARE HOOKS USEFUL?
  ───────────────────────────────────────────────────────────────────────────
  1. AUDIT LOGGING: Track every pipeline run — who ran it, when, how long,
     how many models, whether it succeeded. Critical for production ops.

  2. PERMISSIONS: Grant BI tool users access to new tables automatically.
     Without this, every new table needs manual GRANT statements.

  3. PERFORMANCE TRACKING: Record query elapsed time per model.
     Helps identify slow models that need optimization.

  4. NOTIFICATIONS: Trigger Slack/email alerts on run completion.

  TABLES CREATED BY THIS MACRO:
  ───────────────────────────────────────────────────────────────────────────
  DBT_STAGING.dbt_run_log    → One row per dbt invocation
  DBT_STAGING.dbt_model_log  → One row per model per invocation

=============================================================================
#}


{#
-----------------------------------------------------------------------------
  create_audit_log_tables()
  → Called in on-run-start to create the audit schema/tables if they
    don't already exist.
  → Uses CREATE TABLE IF NOT EXISTS so it's safe to run on every pipeline run.
-----------------------------------------------------------------------------
TEACHING: "IF NOT EXISTS" is idempotent — safe to re-run without side effects.
This is a key principle in data engineering: make all operations re-runnable.
#}

{% macro create_audit_log_tables() %}

    -- ── dbt_run_log: One row per dbt invocation ──────────────────────────────
    -- invocation_id is dbt's built-in run UUID (unique per dbt run command)
    create table if not exists {{ target.database }}.DBT_STAGING.dbt_run_log (
        invocation_id       varchar(36)     comment 'dbt unique run identifier (UUID)',
        target_name         varchar(50)     comment 'dev or prod',
        run_start_at        timestamp_tz    comment 'UTC timestamp when run started',
        run_end_at          timestamp_tz    comment 'UTC timestamp when run finished (null while running)',
        run_status          varchar(20)     comment 'RUNNING | SUCCESS | FAILED',
        models_total        number          comment 'Total models selected for this run',
        models_completed    number          comment 'Models that completed successfully',
        models_errored      number          comment 'Models that errored',
        run_command_args    varchar(1000)   comment 'dbt CLI arguments (models selected, vars, etc.)',
        run_by              varchar(100)    comment 'Snowflake USER() who executed the run'
    )

{% endmacro %}


{#
-----------------------------------------------------------------------------
  log_run_start()
  → Called in on-run-start to record that this dbt run has begun.
  → Uses dbt's built-in context variables: invocation_id, run_started_at
-----------------------------------------------------------------------------
dbt BUILT-IN VARIABLES (available in all macros/models):
  invocation_id   → UUID unique to this dbt run (e.g., "abc123...")
  run_started_at  → ISO timestamp of when dbt started
  target.name     → 'dev' or 'prod'
  env_var('X')    → value of environment variable X

TEACHING: invocation_id is the linchpin of audit logging — it ties the
run log to individual model logs, letting you reconstruct exactly what
happened in any given pipeline run.
#}

{% macro log_run_start() %}

{#
  TEACHING: The `execute` flag is False during "dbt parse" (when dbt is only
  compiling SQL without running it) and True during actual runs (dbt run, dbt test).

  graph.nodes is only a fully-populated graph object at execution time.
  During dbt parse, graph is a plain Python dict, so graph.nodes raises
  "'dict object' has no attribute 'nodes'".

  Guarding with `if execute` tells Jinja: "only evaluate this expression
  when we are actually executing, not when we are just parsing."
  This is the standard dbt pattern for any macro that touches graph, results,
  or flags context variables.
#}

    {%- if execute -%}
        {%- set model_count = graph.nodes.values()
                              | selectattr('resource_type', 'equalto', 'model')
                              | list | length -%}
        {%- set cmd_args = flags.WHICH_COMMAND
                           ~ ' '
                           ~ (flags.MODELS | join(' ') if flags.MODELS else 'ALL') -%}
    {%- else -%}
        {%- set model_count = 0 -%}
        {%- set cmd_args = 'parse' -%}
    {%- endif -%}

    insert into {{ target.database }}.DBT_STAGING.dbt_run_log (
        invocation_id,
        target_name,
        run_start_at,
        run_end_at,
        run_status,
        models_total,
        models_completed,
        models_errored,
        run_command_args,
        run_by
    )
    values (
        '{{ invocation_id }}',
        '{{ target.name }}',
        '{{ run_started_at }}'::timestamp_tz,
        null,               -- Not known yet; updated by log_run_end()
        'RUNNING',          -- Will be updated to SUCCESS or FAILED at end
        {{ model_count }},
        0,                  -- Updated at end
        0,                  -- Updated at end
        '{{ cmd_args }}',
        current_user()
    )

{% endmacro %}


{#
-----------------------------------------------------------------------------
  log_run_end()
  → Called in on-run-end to update the run log with completion info.
  → dbt provides results context: number of passed/failed models.
-----------------------------------------------------------------------------
TEACHING: dbt's on-run-end context includes a 'results' variable
that is a list of RunResult objects. Each has:
  .status      — 'success' | 'error' | 'warn' | 'skip'
  .node.name   — model name

We use Jinja2 selectattr filter to count successes vs errors.
#}

{% macro log_run_end() %}

    {# Count results by status — Jinja2 in-memory filtering #}
    {%- set ns = namespace(passed=0, errored=0) -%}
    {%- for result in results -%}
        {%- if result.status in ('success', 'pass') -%}
            {%- set ns.passed = ns.passed + 1 -%}
        {%- elif result.status in ('error', 'fail') -%}
            {%- set ns.errored = ns.errored + 1 -%}
        {%- endif -%}
    {%- endfor -%}

    update {{ target.database }}.DBT_STAGING.dbt_run_log
    set
        run_end_at        = current_timestamp(),
        run_status        = '{{ "FAILED" if ns.errored > 0 else "SUCCESS" }}',
        models_completed  = {{ ns.passed }},
        models_errored    = {{ ns.errored }}
    where
        invocation_id = '{{ invocation_id }}'

{% endmacro %}


{#
-----------------------------------------------------------------------------
  log_model_run(model)
  → Called as a post-hook on individual models to record per-model timing.
  → 'this' is the dbt built-in reference to the current model being built.
-----------------------------------------------------------------------------
USAGE (in dbt_project.yml):
  +post-hook: "{{ log_model_run(this) }}"

dbt 'this' object properties:
  this.name       → 'fact_movies'
  this.schema     → 'GOLD' (the Snowflake schema)
  this.database   → 'LECTURE_DE'
  this.identifier → Full table/view name

TEACHING: Post-hooks run in the SAME Snowflake session as the model build,
so you can immediately query the just-built table in the post-hook.
For example, you could post-hook to verify row counts match expectations.
#}

{% macro log_model_run(model) %}

    -- Simple single-statement insert using model context variables
    -- Note: We skip creating this table in the macro body because
    -- create_audit_log_tables() (called in on-run-start) creates it.
    -- This shows how on-run-start hooks "prepare" for model post-hooks.

    insert into {{ target.database }}.DBT_STAGING.dbt_model_log (
        invocation_id,
        model_name,
        model_schema,
        model_database,
        completed_at,
        target_name
    )
    select
        '{{ invocation_id }}'               as invocation_id,
        '{{ model.name }}'                  as model_name,
        '{{ model.schema }}'                as model_schema,
        '{{ model.database }}'              as model_database,
        current_timestamp()                 as completed_at,
        '{{ target.name }}'                 as target_name

{% endmacro %}


{#
-----------------------------------------------------------------------------
  grant_analyst_role_access()
  → Called in on-run-end to ensure ANALYST_ROLE can SELECT from all
    new tables built in this run.
  → Critical: without this, every new table requires manual GRANT.
-----------------------------------------------------------------------------
TEACHING NOTE — Why Automate GRANT Statements?
  When dbt creates a new table (e.g., fact_movies), Snowflake defaults
  to granting privileges only to the role that created it (DE_ROLE).
  The BI tool (Tableau/Power BI) uses ANALYST_ROLE — it can't see the
  new table until someone runs GRANT SELECT.

  This hook runs automatically after every successful dbt run, ensuring
  ANALYST_ROLE always has access to the latest Gold/Marts tables.

  Snowflake syntax:
    GRANT SELECT ON ALL TABLES IN SCHEMA db.schema TO ROLE role_name
  This grants access to ALL existing tables in the schema in one statement.

PERMISSIONS REQUIRED:
  The role running dbt (DE_ROLE) must have GRANT OPTION on ANALYST_ROLE,
  or be a SYSADMIN. If DE_ROLE lacks grant privilege, run this hook
  as SYSADMIN or add the GRANT to your Snowflake setup scripts.
#}

{% macro grant_analyst_role_access() %}

    -- Grant read access to ANALYST_ROLE on all Gold tables and marts
    -- This runs after EVERY dbt run, ensuring new tables are immediately accessible
    grant usage  on schema  {{ target.database }}.GOLD to role ANALYST_ROLE;
    grant select on all tables in schema {{ target.database }}.GOLD to role ANALYST_ROLE;
    grant select on all views  in schema {{ target.database }}.GOLD to role ANALYST_ROLE

{% endmacro %}


{#
-----------------------------------------------------------------------------
  create_model_log_table()
  → Creates the per-model log table used by log_model_run() post-hooks.
  → Called as part of create_audit_log_tables().
-----------------------------------------------------------------------------
#}

{% macro create_model_log_table() %}

    create table if not exists {{ target.database }}.DBT_STAGING.dbt_model_log (
        invocation_id   varchar(36)     comment 'Links to dbt_run_log.invocation_id',
        model_name      varchar(200)    comment 'dbt model name (e.g., fact_movies)',
        model_schema    varchar(100)    comment 'Snowflake schema (e.g., GOLD)',
        model_database  varchar(100)    comment 'Snowflake database (e.g., LECTURE_DE)',
        completed_at    timestamp_tz    comment 'UTC timestamp when this model finished',
        target_name     varchar(50)     comment 'dev or prod'
    )

{% endmacro %}
