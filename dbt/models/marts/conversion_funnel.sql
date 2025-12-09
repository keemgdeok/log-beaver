{{ config(materialized='table') }}

with daily as (
    select
        toDate(timestamp) as event_date,
        countIf(event_type = 'view') as views,
        countIf(event_type = 'cart') as carts,
        countIf(event_type = 'purchase') as purchases
    from {{ ref('stg_user_logs') }}
    group by event_date
)

select
    event_date,
    views,
    carts,
    purchases,
    if(views = 0, 0, carts / views) as view_to_cart_rate,
    if(carts = 0, 0, purchases / carts) as cart_to_purchase_rate,
    if(views = 0, 0, purchases / views) as view_to_purchase_rate
from daily
order by event_date
