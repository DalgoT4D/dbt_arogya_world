{% macro generate_schema_name(
        custom_schema_name,
        node
    ) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {% if node.fqn [1:-2] | length == 0 %}
            {{ default_schema }}
        {% else %}
            {# Concat the subfolder(s) name #}
            {% set prefix = node.fqn [1:-2] | join('_') %}
            {{ prefix | trim }}
        {% endif %}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
