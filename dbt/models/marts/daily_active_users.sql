{{ config(materialized='table') }}

select
    toDate(timestamp) as event_date,
    uniqExact(user_id) as daily_active_users
from {{ ref('stg_user_logs') }}
group by event_date
order by event_date
