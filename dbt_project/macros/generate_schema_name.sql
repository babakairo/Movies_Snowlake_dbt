
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
