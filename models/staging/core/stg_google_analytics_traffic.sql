{{
  config(
    materialized="view"
  )
}}

with source as (
    
    select 
    
        user_pseudo_id,
        event_name,
        event_string_value
        device_category,
        mobile_brand_name,
        operating_system,
        language,
        traffic_medium,
        traffic_name,
        traffic_source,
        country,
        city,

    from {{ source('google_analytics', 'google_events') }}
    
)

select * from source