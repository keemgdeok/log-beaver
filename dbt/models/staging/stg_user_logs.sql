{{ config(materialized='view') }}

with source as (
    select * from {{ source('log_beaver', 'raw_user_logs') }}
),

casted as (
    select
        event_id,
        timestamp,
        lower(event_type) as event_type,
        user_id,
        lower(user_gender) as user_gender,
        user_age_group,
        lower(device_type) as device_type,
        item_id,
        item_category,
        toFloat64(item_price) as item_price,
        session_id,
        page_url,
        referrer
    from source
)

select * from casted
