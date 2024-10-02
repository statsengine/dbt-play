select
    c.user_id,
    c.user_name,
    c.email,
    c.age,
    c.sex,
    c.address,
    c.city,
    ca.first_order_date,
    ca.last_order_date
from {{ ref('stg_customers') }} c
left join (
    select 
        user_id,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from {{ ref('stg_orders') }}
    group by user_id
) ca on c.user_id = ca.user_id