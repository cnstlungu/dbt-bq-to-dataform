{{ config(materialized='view', schema='staging') }}

SELECT
    id,
    CAST(order_date AS DATE) AS order_date,
    amount
FROM {{ source('shop', 'orders') }}
WHERE amount >= {{ var('min_amount') }}
