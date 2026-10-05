{%- set maybe = var('not_set_anywhere', none) -%}
SELECT
    {{ edge_pkg.shout("'quiet'") }} AS loud,
    {{ edge.cents('2') }} AS own_namespace,
    {{ edge_pkg.greeting() }} AS greeting,
    {{ _private_helper() }} AS private_value,
    {{ 'NULL' if maybe is none else maybe }} AS maybe,
    TIMESTAMP('{{ run_started_at }}') AS run_started_at
FROM {{ ref('calendar') }}
