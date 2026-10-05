{% for r in var('regions') %}
SELECT '{{ r }}' AS region {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
