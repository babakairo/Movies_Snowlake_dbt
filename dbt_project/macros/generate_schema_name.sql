/*
=============================================================================
  MACRO: generate_schema_name
=============================================================================
TEACHING NOTE — Why Override This Macro?

  By default, dbt names schemas as:
    {target_schema}_{custom_schema}
    e.g., DBT_DEV_BRONZE  or  DBT_PROD_BRONZE

  This default prevents dev and prod from colliding, which is good.
  BUT for production, we want clean schema names: BRONZE, SILVER, GOLD.

  This custom macro overrides the default behaviour:
    - In DEV: prepend developer name → DEV_BRONZE, DEV_SILVER, DEV_GOLD
    - In PROD: use schema name exactly → BRONZE, SILVER, GOLD

  The macro receives:
    custom_schema_name: The schema set in dbt_project.yml (e.g., "BRONZE")
    node:              The current model node object

  IMPORTANT: This macro must be named EXACTLY generate_schema_name
  and placed in the macros/ directory. dbt will automatically find and use it.

  HOW IT WORKS:
    1. If running with target=prod and a custom schema is set → use exactly that schema
    2. If running with target=dev and custom schema is set → prepend "DEV_"
    3. If no custom schema is set → use the default schema from profiles.yml

  This is one of the most important macros to understand in any dbt project.
=============================================================================
*/

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {# No custom schema set — use the target's default schema #}
        {{ default_schema }}

    {%- elif target.name == 'prod' -%}

        {# Production: use the custom schema exactly (BRONZE, SILVER, GOLD) #}
        {# Do NOT prefix with target schema in prod — keep names clean #}
        {{ custom_schema_name | trim | upper }}

    {%- else -%}

        {# Development: prefix with DEV_ to isolate from prod schemas #}
        {# Result: DEV_BRONZE, DEV_SILVER, DEV_GOLD #}
        DEV_{{ custom_schema_name | trim | upper }}

    {%- endif -%}

{%- endmacro %}
