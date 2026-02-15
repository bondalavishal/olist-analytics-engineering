{{ config(
    materialized = 'incremental',
    unique_key = 'order_item_sk'
) }}

with order_items as (

    select *
    from {{ ref('stg_order_items') }}

),

orders as (

    select *
    from {{ ref('stg_orders') }}

),

final as (

    select
        -- Deterministic surrogate key
        md5(oi.order_id || '-' || oi.order_item_id) as order_item_sk,

        oi.order_id,
        oi.order_item_id,
        o.customer_id,
        oi.product_id,
        oi.seller_id,

        oi.shipping_limit_date,
        oi.price,
        oi.freight_value,

        o.order_purchase_timestamp::date as order_date

    from order_items oi
    join orders o
        on oi.order_id = o.order_id

)

select *
from final

{% if is_incremental() %}

where order_item_sk not in (
    select order_item_sk from {{ this }}
)

{% endif %}