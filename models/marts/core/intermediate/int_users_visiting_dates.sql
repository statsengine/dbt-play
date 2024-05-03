
{{
    config(
        dataset = 'int',
        materialized = 'view'
    )
}}

-- ga4 sessions
with sessions as (

    select

        user_id,
        event_date,
    
    from {{ ref('stg_google_analytics_events') }}

),

-- group sessions to a user identity graph (customer_id = unique)
final as (

    -- select all identifiers and group on customer-id.
    select
        *
    from sessions

)

select * from final

