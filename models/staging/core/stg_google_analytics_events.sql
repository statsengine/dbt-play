{{
  config(
    materialized="view"
  )
}}

with source as (
    
    select 
        *
    from {{ source('google_analytics', 'google_events') }}
    
)

select * from source