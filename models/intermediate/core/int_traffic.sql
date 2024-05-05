{{ config(materialized="view") }}

with
    sessions as (

        select

            count(user_pseudo_id) as n_users,
            count(event_name) as n_events,
            count(event_string_value) as n_events_strings,
            device_category,
            mobile_brand_name,
            operating_system,
            language,
            traffic_medium,
            traffic_name,
            traffic_source,
            country,
            city,

        from {{ ref("stg_google_analytics_events") }}
        group by
            device_category,
            mobile_brand_name,
            operating_system,
            language,
            traffic_medium,
            traffic_name,
            traffic_source,
            country,
            city
    )

select *
from sessions
