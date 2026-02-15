{{ config(materialized='table') }}

Select distinct customer_id,
customer_unique_id,
customer_city,
customer_state
From {{ ref('stg_customers') }}