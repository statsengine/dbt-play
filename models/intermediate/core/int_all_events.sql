{{
    config (
        materialized = 'view'
    )
}}

with sessions as (

    select * from {{ ref('stg_google_analytics_events') }}
)

select * from sessions