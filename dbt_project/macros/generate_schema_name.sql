{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        Custom schema name generation for consistent naming across environments.

        In production (target = 'prod'), uses the custom schema name directly.
        In development, prefixes with the target schema.

        Examples:
        - dev + staging -> dev_staging
        - prod + staging -> staging
    -#}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
