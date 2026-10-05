{% macro cents(column) %}
  {{ return("CAST(" ~ column ~ " * 100 AS INT64)") }}
{% endmacro %}

{% macro _private_helper() %}1{% endmacro %}

{# Overrides edge_pkg's greeting through the project's dispatch search order. #}
{% macro default__greeting() %}'hello from edge'{% endmacro %}
