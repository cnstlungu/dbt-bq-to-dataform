SELECT id, COUNT(*) AS n
FROM {{ ref('fct_orders') }}
GROUP BY id
HAVING COUNT(*) > 1
