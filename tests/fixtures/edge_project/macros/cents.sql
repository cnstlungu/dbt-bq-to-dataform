{% macro cents(column) %}
  {{ return("CAST(" ~ column ~ " * 100 AS INT64)") }}
{% endmacro %}

{% macro _private_helper() %}1{% endmacro %}

{# Overrides edge_pkg's greeting through the project's dispatch search order. #}
{% macro default__greeting() %}'hello from edge'{% endmacro %}

{# A project macro with an adapter-specific implementation: dispatch picks bigquery__ over default__. #}
{% macro variant() %}{{ return(adapter.dispatch('variant')()) }}{% endmacro %}

{% macro default__variant() %}'default'{% endmacro %}

{% macro bigquery__variant() %}'bigquery'{% endmacro %}
