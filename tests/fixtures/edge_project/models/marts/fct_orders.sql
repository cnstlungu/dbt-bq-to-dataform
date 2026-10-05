{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='append_new_columns',
    partition_by={'field': 'order_date', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['id'],
    tags=['nightly']
) }}

SELECT
    id,
    order_date,
    {{ dbt.dateadd('day', 1, 'order_date') }} AS next_day,
    {{ cents('amount') }} AS amount_cents,
    {{ dbt.current_timestamp() }} AS loaded_at
FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
