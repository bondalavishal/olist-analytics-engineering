{{ config(
    materialized = 'table'
) }}

with base as (

    select *
    from {{ ref('fct_order_items') }}

)

select
    order_id,
    min(order_date) as order_date,
    sum(price) as total_order_value,
    sum(freight_value) as total_freight_value,
    count(*) as total_items

from base
group by order_id