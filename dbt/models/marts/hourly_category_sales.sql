{{ config(materialized='table') }}

select
    toStartOfHour(timestamp) as hour,
    item_category,
    sumIf(item_price, event_type = 'purchase') as total_sales,
    countIf(event_type = 'purchase') as purchase_events
from {{ ref('stg_user_logs') }}
group by hour, item_category
order by hour, item_category
