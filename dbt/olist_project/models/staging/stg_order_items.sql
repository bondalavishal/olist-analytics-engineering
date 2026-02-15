Select

order_id,
order_item_id,
product_id,
seller_id,
shipping_limit_date,
Price,
freight_value
From {{ source('raw','order_items') }}