{{ config(
    materialized='incremental',
    unique_key='user_id'
) }}

-- Step 1: Fetch all new or updated users
with new_or_updated_users as (
    select
        c.user_id,
        c.user_name,
        c.email,
        c.age,
        c.sex,
        c.address,
        c.city
    from {{ ref('stg_customers') }} c
    {% if is_incremental() %}
        -- Include users who are either new or have updated information
        where c.user_id not in (select distinct user_id from {{ this }})
    {% endif %}
),

-- Step 2: Summarize orders for all users (both new and existing)
order_summary as (
    select
        o.user_id,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date
    from {{ ref('stg_orders') }} o
    group by o.user_id
),

-- Step 3: Final result, combining user info with their order summaries
final_users as (
    select
        nu.user_id,
        nu.user_name,
        nu.email,
        nu.age,
        nu.sex,
        nu.address,
        nu.city,
        os.first_order_date,
        os.last_order_date
    from new_or_updated_users nu
    left join order_summary os on nu.user_id = os.user_id

    {% if is_incremental() %}
        union all
        -- Include existing users who are not in new_or_updated_users
        -- but have new orders that affect first_order_date or last_order_date
        select
            old.user_id,
            old.user_name,
            old.email,
            old.age,
            old.sex,
            old.address,
            old.city,
            os.first_order_date,
            os.last_order_date
        from {{ this }} old
        join order_summary os on old.user_id = os.user_id
        where os.last_order_date > old.last_order_date
    {% endif %}
)

-- Select the final users to be inserted into the table
select * from final_users