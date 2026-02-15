{{ config(materialized = 'table') }}

Select distinct seller_id,
seller_zip_code_prefix,
seller_city,
seller_state
From {{ ref('stg_sellers') }}