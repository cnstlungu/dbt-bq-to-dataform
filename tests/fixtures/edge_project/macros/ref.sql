{# The common "ref override" pattern: wrap dbt's own ref, reached via builtins. #}
{% macro ref(model_name) %}
  {% do return(builtins.ref(model_name)) %}
{% endmacro %}
