{{ config(materialized='table') }}

Select distinct order_date as date_day,
extract(year from order_date)::int as year,
extract(month from order_date)::int as month,
extract(day from order_date)::int as day,
extract(quarter from order_date)::int as quarter,
extract(isodow from order_date)::int as iso_day_of_week

From {{ ref('fct_orders') }}