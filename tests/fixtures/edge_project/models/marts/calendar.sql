{{ config(materialized='ephemeral') }}

{{ dbt.date_spine('day', "DATE '2024-01-01'", "DATE '2024-01-08'") }}
