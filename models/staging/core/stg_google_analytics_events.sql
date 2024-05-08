{{ config(materialized="view") }}


-- cast to correct types
with
    source as (
        select 
            FORMAT_TIMESTAMP('%F %H:%M', TIMESTAMP_MICROS(event_timestamp)) AS event_timestamp,
            event_name,
            event_param_key,
            event_string_value,
            event_int_value,
            item_id,
            item_name,
            item_brand,
            item_variant,
            item_category,
            price,
            quantity,
            item_revenue,
            promotion_id,
            promotion_name,
            creative_name,
            creative_slot,
            transaction_id,
            total_item_quantity,
            unique_items,
            user_pseudo_id,
            device_category,
            mobile_brand_name,
            operating_system,
            language,
            country,
            city,
            traffic_medium,
            traffic_name,
            traffic_source
        from {{ source("google_analytics", "google_events") }}
    )

select *
from source
