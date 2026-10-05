{% snapshot orders_snapshot %}
{{ config(target_schema='snapshots', unique_key='id', strategy='check', check_cols=['amount']) }}
SELECT * FROM {{ source('shop', 'orders') }}
{% endsnapshot %}
