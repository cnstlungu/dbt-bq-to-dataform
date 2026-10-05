{% macro shout(col) %}UPPER({{ col }}){% endmacro %}

{% macro greeting() %}
  {{ return(adapter.dispatch('greeting', 'edge_pkg')()) }}
{% endmacro %}

{% macro default__greeting() %}'hello from edge_pkg'{% endmacro %}
