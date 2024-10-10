{{ config(
    materialized='incremental',
    unique_key='user_id'
) }}

with new_or_updated_users as (

    select
        c.user_id,
        c.user_name,
        c.email,
        c.age,
        c.sex,
        c.city,
        c.address
    from {{ ref('stg_customers') }} as c
    {% if is_incremental() %}
        left join {{ this }} as existing on c.user_id = existing.user_id
        where existing.user_id is null
    {% endif %}
),

order_summary as (

    select
        o.user_id,
        cast(min(o.order_timestamp) as timestamp) as first_order_date,  --
        cast(max(o.order_timestamp) as timestamp) as last_order_date
    from {{ ref('stg_orders') }} as o
    group by o.user_id
),

final_users as (

    select
        nu.user_id,
        nu.user_name,
        nu.email,
        nu.age,
        nu.sex,
        nu.city,
        nu.address,
        os.first_order_date,
        os.last_order_date
    from new_or_updated_users as nu
    left join order_summary as os on nu.user_id = os.user_id

    {% if is_incremental() %}

        union all

        -- Include existing users who have new orders affecting their order dates
        select
            old.user_id,
            old.user_name,
            old.email,
            old.age,
            old.sex,
            old.city,
            old.address,
            os.first_order_date,
            os.last_order_date
        from {{ this }} as old
        inner join order_summary as os on old.user_id = os.user_id
        -- Cast both columns to TIMESTAMP for comparison
        where
            cast(os.last_order_date as timestamp)
            > cast(old.last_order_date as timestamp)

    {% endif %}
)

-- Select the final users to be inserted into the table
select * from final_users
