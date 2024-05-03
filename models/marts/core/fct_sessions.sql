{{
    config (
        materialized = 'table'
    )
}}

with sessions as (

    select * from {{ ref('int_all_events') }}
)

select * from sessions